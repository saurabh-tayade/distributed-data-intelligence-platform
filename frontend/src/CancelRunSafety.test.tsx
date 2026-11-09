import { StrictMode } from 'react';
import { act, fireEvent, render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { App } from './App';
import { CANCEL_BODY, parseRunDetail, sendCancelCommand, validateCancelCommand } from './managementApi';
import { RunDetail } from './operations/RunDetail';
import { CancelRunPanel } from './operations/CancelRunPanel';
import { CANCEL_INTENT_KEY, readCancelIntent, reviewCancelRun } from './operations/cancelIntent';
import { deferred, fixture, jsonResponse } from './test/fixtures';
import { managementFailure, sourcePage } from './test/managementFixtures';
import { attemptsResponse, controlReceipt, CONTROL_ID, detailResponse, eventsResponse, recordEnvelope, runDetail } from './test/runInspectionFixtures';
const run = runDetail(), id = run.runId;
function review() { reviewCancelRun(parseRunDetail(recordEnvelope(run), id, `"${run.rowVersion}"`)); }
function accepted() {
  return new Response(JSON.stringify(recordEnvelope({ ...controlReceipt('APPLIED'), type: 'CANCEL', reason: 'CANCELLED' })), { status: 202,
    headers: { 'Content-Type': 'application/json', ETag: '"12"', Location: `/api/management/v1/runs/${id}/controls/${CONTROL_ID}` } });
}
function mount() { return render(<StrictMode><CancelRunPanel visible /></StrictMode>); }
beforeEach(() => { sessionStorage.clear(); window.history.replaceState(null, '', `/#/operations/executions/${id}`); });
describe('queued-run cancellation safety', () => {
  it('freezes exact cancellation body and run ETag without sending during review', () => {
    const fetch = vi.fn(); vi.stubGlobal('fetch', fetch); review(); mount();
    expect(fetch).not.toHaveBeenCalled(); expect(readCancelIntent()).toMatchObject({ version: 1, phase: 'review', command: { runId: id, body: CANCEL_BODY, ifMatch: `"${run.rowVersion}"` } });
    expect(() => review()).toThrow();
  });
  it.each(['RUNNING', 'SUCCEEDED', 'FAILED', 'CANCELLED', 'PAUSED', 'RECOVERY_REQUIRED'] as const)('refuses a review for %s', (state) => {
    const record = runDetail(1, state);
    expect(() => reviewCancelRun(parseRunDetail(recordEnvelope(record), id, `"${record.rowVersion}"`))).toThrow();
    expect(readCancelIntent()).toBeNull();
  });
  it('refuses queued runs with any attempt', () => {
    const record = { ...run, attempts: { count: '1', currentAttemptId: null } };
    expect(() => reviewCancelRun(parseRunDetail(recordEnvelope(record), id, `"${record.rowVersion}"`))).toThrow();
  });
  it('sends once for duplicate clicks, retains control receipt and reads current run independently', async () => {
    const late = deferred<Response>(), fetch = vi.fn().mockReturnValueOnce(late.promise).mockImplementation(() => Promise.resolve(detailResponse(runDetail(1, 'CANCELLED'))));
    vi.stubGlobal('fetch', fetch); review(); const original = readCancelIntent()!.command; mount();
    const button = screen.getByRole('button', { name: 'Confirm queued-run cancellation' }); fireEvent.click(button); fireEvent.click(button);
    expect(fetch).toHaveBeenCalledTimes(1); await act(async () => late.resolve(accepted()));
    await screen.findByText(/Fresh run read: CANCELLED/);
    expect(readCancelIntent()).toMatchObject({ phase: 'confirmed', command: original, receipt: { etag: '"12"', data: { type: 'CANCEL', state: 'APPLIED' } } });
    expect(fetch.mock.calls[0]).toEqual([`/api/management/v1/runs/${id}/controls`, expect.objectContaining({ method: 'POST', body: CANCEL_BODY,
      headers: expect.objectContaining({ 'Idempotency-Key': original.key, 'If-Match': original.ifMatch, 'X-DDIP-Local-Request': '1' }) })]);
  });
  it('requires explicit same-key, body and ETag replay after uncertainty and reload', async () => {
    const fetch = vi.fn().mockRejectedValueOnce(new TypeError('lost')).mockResolvedValueOnce(accepted()).mockImplementation(() => Promise.resolve(detailResponse()));
    vi.stubGlobal('fetch', fetch); review(); const original = readCancelIntent()!.command, view = mount();
    fireEvent.click(screen.getByRole('button', { name: 'Confirm queued-run cancellation' })); await screen.findByText(/Outcome unknown/);
    view.unmount(); mount(); expect(fetch).toHaveBeenCalledTimes(1); expect(readCancelIntent()!.command).toEqual(original);
    expect(screen.queryByRole('button', { name: 'Discard cancellation review' })).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'Resolve same cancellation request' })); await screen.findByText(/Persisted control/);
    expect(fetch.mock.calls[1]![1]).toMatchObject({ body: original.body, headers: { 'Idempotency-Key': original.key, 'If-Match': original.ifMatch } });
  });
  it('retains uncertainty after unmount during transport and does not publish a late success', async () => {
    const late = deferred<Response>(), fetch = vi.fn().mockReturnValue(late.promise); vi.stubGlobal('fetch', fetch); review(); const view = mount();
    fireEvent.click(screen.getByRole('button', { name: 'Confirm queued-run cancellation' })); view.unmount();
    expect(readCancelIntent()!.phase).toBe('uncertain'); await act(async () => late.resolve(accepted()));
    expect(readCancelIntent()!.phase).toBe('uncertain'); mount(); expect(fetch).toHaveBeenCalledTimes(1);
  });
  it.each([[409, 'STATE_CONFLICT'], [412, 'PRECONDITION_FAILED'], [429, 'MANAGEMENT_CAPACITY_EXCEEDED']])('retains a definitive %s rejection without replay', async (status, code) => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(jsonResponse(managementFailure(String(code)), Number(status)))); review(); mount();
    fireEvent.click(screen.getByRole('button', { name: 'Confirm queued-run cancellation' })); await screen.findByText(String(code));
    expect(readCancelIntent()!.phase).toBe('rejected'); expect(screen.queryByRole('button', { name: 'Resolve same cancellation request' })).not.toBeInTheDocument();
  });
  it('does not send when saving pending fails', async () => {
    const fetch = vi.fn(); vi.stubGlobal('fetch', fetch); review(); mount();
    const spy = vi.spyOn(Storage.prototype, 'setItem').mockImplementation(() => { throw new Error('full'); });
    fireEvent.click(screen.getByRole('button', { name: 'Confirm queued-run cancellation' }));
    await screen.findByText(/Tab storage could not be updated/); expect(fetch).not.toHaveBeenCalled(); expect(readCancelIntent()!.phase).toBe('review'); spy.mockRestore();
  });
  it.each(['broken-json', '{"version":2}', '{"version":1,"phase":"pending","command":{}}'])('fails closed on corrupted storage %s', (raw) => {
    const fetch = vi.fn(); vi.stubGlobal('fetch', fetch); sessionStorage.setItem(CANCEL_INTENT_KEY, raw); mount();
    expect(screen.getByText('Run cancellation unavailable')).toBeInTheDocument(); expect(fetch).not.toHaveBeenCalled(); expect(() => review()).toThrow();
  });
  it('allows discarding only unsent review and generates a new key for a new deliberate review', async () => {
    vi.stubGlobal('fetch', vi.fn()); review(); const key = readCancelIntent()!.command.key; mount();
    await userEvent.setup().click(screen.getByRole('button', { name: 'Discard cancellation review' })); act(() => review());
    expect(readCancelIntent()!.command.key).not.toBe(key); expect(fetch).not.toHaveBeenCalled();
  });
  it('starts review from an eligible inspected run without sending a mutation', async () => {
    const fetch = vi.fn<(path: string, init?: RequestInit) => Promise<Response>>((path) => Promise.resolve(path.includes('/attempts') ? attemptsResponse([])
      : path.includes('/events') ? eventsResponse([]) : detailResponse()));
    vi.stubGlobal('fetch', fetch); render(<><CancelRunPanel visible /><RunDetail runId={id} returnQuery={{}} /></>);
    const button = await screen.findByRole('button', { name: 'Review queued-run cancellation' });
    await userEvent.setup().click(button);
    await screen.findByRole('button', { name: 'Confirm queued-run cancellation' });
    expect(readCancelIntent()!.phase).toBe('review');
    expect(fetch.mock.calls.every(([, init]) => (init as RequestInit | undefined)?.method !== 'POST')).toBe(true);
  });
  it('retains confirmed receipt when the subsequent run read fails', async () => {
    const fetch = vi.fn().mockResolvedValueOnce(accepted()).mockRejectedValue(new TypeError('lost'));
    vi.stubGlobal('fetch', fetch); review(); mount();
    fireEvent.click(screen.getByRole('button', { name: 'Confirm queued-run cancellation' }));
    await screen.findByText(/Current run read failed/); expect(readCancelIntent()!.phase).toBe('confirmed');
    expect(screen.getByText(/Persisted control/)).toBeVisible();
  });
  it('retains unavailable coordinator outcome for explicit same-request resolution', async () => {
    const fetch = vi.fn().mockResolvedValue(jsonResponse(managementFailure('COORDINATOR_RECOVERY_REQUIRED'), 503));
    vi.stubGlobal('fetch', fetch); review(); mount();
    fireEvent.click(screen.getByRole('button', { name: 'Confirm queued-run cancellation' }));
    await screen.findByText(/Outcome unknown/); expect(readCancelIntent()!.phase).toBe('uncertain');
    expect(fetch).toHaveBeenCalledTimes(1);
  });
  it('keeps a pending cancellation across workspace navigation without redirecting', async () => {
    const late = deferred<Response>();
    vi.stubGlobal('fetch', vi.fn((path: string, init?: RequestInit) => {
      if (init?.method === 'POST') return late.promise;
      if (path === `/api/management/v1/runs/${id}`) return Promise.resolve(detailResponse());
      if (path.includes('/attempts')) return Promise.resolve(attemptsResponse([]));
      if (path.includes('/events')) return Promise.resolve(eventsResponse([]));
      if (path.includes('/sources')) return Promise.resolve(jsonResponse(sourcePage([])));
      return Promise.resolve(jsonResponse(fixture));
    }));
    review(); render(<App />); const user = userEvent.setup();
    await user.click(screen.getByRole('button', { name: 'Confirm queued-run cancellation' }));
    await user.click(screen.getByRole('link', { name: 'Analytics' }));
    await act(async () => late.resolve(accepted())); expect(window.location.hash).toBe('#/analytics');
    expect(readCancelIntent()!.phase).toBe('confirmed');
    await user.click(screen.getByRole('link', { name: 'Operations' })); await screen.findByText(/Persisted control/);
  });
});

describe('cancellation API response validation', () => {
  it.each(['body', 'ifMatch', 'runId', 'key'])('rejects invalid %s before transport', (field) => {
    review(); const command = { ...readCancelIntent()!.command, [field]: 'invalid' };
    expect(() => validateCancelCommand(command)).toThrow();
  });
  it.each(['location', 'etag', 'type', 'identity', 'json', 'state', 'reason'])('treats malformed accepted %s as uncertain', async (field) => {
    review(); const receipt = { ...controlReceipt('APPLIED'), reason: field === 'reason' ? 'RECOVERED_CHECKPOINT' : 'CANCELLED', state: field === 'state' ? 'REJECTED' : 'APPLIED', type: field === 'type' ? 'RETRY' : 'CANCEL', ...(field === 'identity' ? { runId: 'bad' } : {}) };
    const response = new Response(field === 'json' ? 'broken' : JSON.stringify(recordEnvelope(receipt)), { status: 202, headers: {
      'Content-Type': 'application/json', ETag: field === 'etag' ? '"13"' : '"12"',
      Location: field === 'location' ? 'https://other.example/receipt' : `/api/management/v1/runs/${id}/controls/${CONTROL_ID}` } });
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(response));
    await expect(sendCancelCommand(readCancelIntent()!.command, new AbortController().signal)).rejects.toMatchObject({ ambiguous: true, status: 202 });
  });
});
