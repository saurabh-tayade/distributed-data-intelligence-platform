import { StrictMode } from 'react';
import { act, fireEvent, render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { App } from './App';
import type { RunDetail as RunRecord } from './managementApi';
import { RETRY_BODY, parseRunDetail } from './managementApi';
import { CancelRunPanel } from './operations/CancelRunPanel';
import { RunDetail } from './operations/RunDetail';
import { canCancelRun, readCancelIntent, reviewCancelRun } from './operations/cancelIntent';
import { RETRY_INTENT_KEY, saveRetryIntent } from './operations/retryIntent';
import { deferred, fixture, jsonResponse } from './test/fixtures';
import { managementFailure, sourcePage } from './test/managementFixtures';
import { ATTEMPT_ID, CONTROL_ID, attemptsResponse, controlReceipt, detailResponse, eventsResponse, latestControl, recordEnvelope, runDetail } from './test/runInspectionFixtures';
const retryControl = { ...latestControl('APPLIED'), type: 'RETRY' as const, reason: 'RETRY_QUEUED' as const };
const queued: RunRecord = { ...runDetail(1, 'QUEUED', 'application-reliability'), outcomeCode: 'RETRY_QUEUED',
  attempts: { count: '2', currentAttemptId: null }, latestControl: retryControl };
const running: RunRecord = { ...queued, state: 'RUNNING', outcomeCode: 'CLAIMED', attempts: { count: '3', currentAttemptId: ATTEMPT_ID } };
const id = queued.runId;
function parsed(record = queued) { return parseRunDetail(recordEnvelope(record), id, `"${record.rowVersion}"`); }
function review(record = queued) { reviewCancelRun(parsed(record)); }
function receipt(state: 'REQUESTED' | 'APPLIED' | 'TOO_LATE') {
  const reason = state === 'REQUESTED' ? null : state === 'APPLIED' ? 'CANCELLED' : 'SUCCEEDED';
  return new Response(JSON.stringify(recordEnvelope({ ...controlReceipt(state), type: 'CANCEL', reason })), {
    status: 202, headers: { 'Content-Type': 'application/json', ETag: '"12"', Location: `/api/management/v1/runs/${id}/controls/${CONTROL_ID}` },
  });
}
function mount() { return render(<StrictMode><CancelRunPanel visible /></StrictMode>); }
beforeEach(() => { sessionStorage.clear(); window.history.replaceState(null, '', `/#/operations/executions/${id}`); });
describe('Reliability retry cancellation review', () => {
  it.each([queued, running])('offers review for displayed %s retry facts without sending', (record) => {
    vi.stubGlobal('fetch', vi.fn()); expect(canCancelRun(parsed(record))).toBe(true); review(record); mount();
    expect(readCancelIntent()).toMatchObject({ mode: record.state === 'RUNNING' ? 'running' : 'queued', phase: 'review' });
    expect(fetch).not.toHaveBeenCalled();
  });
  it.each([
    { ...queued, latestControl: null },
    { ...queued, outcomeCode: 'FAILED' as const },
    { ...queued, attempts: { count: '2', currentAttemptId: ATTEMPT_ID } },
    { ...queued, latestControl: { ...retryControl, type: 'RECONCILE' as const } },
    { ...queued, latestControl: { ...retryControl, reason: 'RECOVERED_CHECKPOINT' as const } },
    { ...queued, latestControl: { ...retryControl, state: 'REQUESTED' as const } },
    { ...queued, pipeline: 'CONTENT_RSS_V1' as const, productId: 'content-intelligence' as const },
    { ...running, latestControl: null },
    { ...running, attempts: { count: '3', currentAttemptId: null } },
    { ...running, latestControl: { ...retryControl, type: 'CANCEL' as const } },
    { ...running, latestControl: { ...retryControl, state: 'REQUESTED' as const } },
    { ...running, pipeline: 'CONTENT_RSS_V1' as const, productId: 'content-intelligence' as const },
    { ...running, state: 'RECOVERY_REQUIRED' as const },
    { ...running, state: 'CANCEL_REQUESTED' as const },
    { ...running, state: 'SUCCEEDED' as const },
  ])('does not infer authority from attempts alone for %j', (record) => {
    expect(canCancelRun({ ...parsed(), data: record })).toBe(false);
    expect(() => reviewCancelRun({ ...parsed(), data: record })).toThrow(); expect(readCancelIntent()).toBeNull();
  });
  it('explains that queued cancellation preserves history and the prior retry receipt', () => {
    vi.stubGlobal('fetch', vi.fn()); review(); mount();
    expect(screen.getByText(/Queued cancellation prevents the next execution/)).toHaveTextContent('It does not erase earlier attempts or artifacts, or change the historical retry receipt.');
    expect(screen.getByText(/The server checks the exact current lifecycle/)).toBeVisible();
  });
  it.each([queued, running])('retains exact command after response loss and resolves only explicitly', async (record) => {
    const state = record.state === 'RUNNING' ? 'TOO_LATE' : 'APPLIED';
    const fetch = vi.fn().mockRejectedValueOnce(new TypeError('lost')).mockResolvedValueOnce(receipt(state)).mockImplementation(() => Promise.resolve(detailResponse(record)));
    vi.stubGlobal('fetch', fetch); review(record); const original = readCancelIntent()!.command, view = mount();
    fireEvent.click(screen.getByRole('button', { name: `Confirm ${record.state === 'RUNNING' ? 'running' : 'queued'}-run cancellation` }));
    await screen.findByText(/Outcome unknown/); view.unmount(); mount(); expect(fetch).toHaveBeenCalledTimes(1);
    fireEvent.click(screen.getByRole('button', { name: 'Resolve same cancellation request' })); await screen.findByText(/Persisted control/);
    expect(readCancelIntent()!.command).toEqual(original);
    expect(fetch.mock.calls[1]![1]).toMatchObject({ body: original.body, headers: { 'Idempotency-Key': original.key, 'If-Match': original.ifMatch } });
  });
  it('does not publish a late running retry cancellation receipt after unmount', async () => {
    const late = deferred<Response>(); vi.stubGlobal('fetch', vi.fn().mockReturnValue(late.promise)); review(running); const view = mount();
    fireEvent.click(screen.getByRole('button', { name: 'Confirm running-run cancellation' })); view.unmount(); await act(async () => late.resolve(receipt('REQUESTED')));
    expect(readCancelIntent()).toMatchObject({ mode: 'running', phase: 'uncertain' }); mount(); expect(fetch).toHaveBeenCalledTimes(1);
  });
  it('treats stale retry lineage rejection as definitive without automatic replay', async () => {
    const fetch = vi.fn().mockResolvedValue(jsonResponse(managementFailure('PRECONDITION_FAILED'), 412)); vi.stubGlobal('fetch', fetch); review(); mount();
    fireEvent.click(screen.getByRole('button', { name: 'Confirm queued-run cancellation' })); await screen.findByText('PRECONDITION_FAILED');
    expect(readCancelIntent()!.phase).toBe('rejected'); expect(fetch).toHaveBeenCalledTimes(1);
  });
  it('starts retry cancellation from inspected queue facts but never sends during review', async () => {
    const fetch = vi.fn((path: string) => Promise.resolve(path.includes('/attempts') ? attemptsResponse([]) : path.includes('/events') ? eventsResponse([])
      : path.includes('/controls') ? new Response(JSON.stringify(recordEnvelope({ ...retryControl, runId: id, runVersion: queued.rowVersion })), { headers: { 'Content-Type': 'application/json', ETag: '"12"' } }) : detailResponse(queued)));
    vi.stubGlobal('fetch', fetch); render(<><CancelRunPanel visible /><RunDetail runId={id} returnQuery={{}} /></>);
    const button = await screen.findByRole('button', { name: 'Review queued-run cancellation' }); await userEvent.setup().click(button);
    await screen.findByRole('button', { name: 'Confirm queued-run cancellation' }); expect(readCancelIntent()!.phase).toBe('review');
  });
  it('retains a pending retry cancellation across navigation and leaves historical retry intent unchanged', async () => {
    const late = deferred<Response>();
    saveRetryIntent({ version: 1, phase: 'confirmed', command: { runId: id, key: crypto.randomUUID(), body: RETRY_BODY, ifMatch: '"5"' },
      receipt: { ...recordEnvelope({ ...controlReceipt('APPLIED'), type: 'RETRY', reason: 'RETRY_QUEUED' }), etag: '"12"', location: `/api/management/v1/runs/${id}/controls/${CONTROL_ID}` } });
    const historical = sessionStorage.getItem(RETRY_INTENT_KEY);
    vi.stubGlobal('fetch', vi.fn((path: string, init?: RequestInit) => {
      if (init?.method === 'POST') return late.promise;
      if (path.includes('/attempts')) return Promise.resolve(attemptsResponse([]));
      if (path.includes('/events')) return Promise.resolve(eventsResponse([]));
      if (path.includes('/sources')) return Promise.resolve(jsonResponse(sourcePage([])));
      if (path === `/api/management/v1/runs/${id}`) return Promise.resolve(detailResponse(queued));
      return Promise.resolve(jsonResponse(fixture));
    }));
    review(); render(<App />); const user = userEvent.setup();
    await user.click(screen.getByRole('button', { name: 'Confirm queued-run cancellation' })); await user.click(screen.getByRole('link', { name: 'Analytics' }));
    await act(async () => late.resolve(receipt('APPLIED'))); expect(window.location.hash).toBe('#/analytics'); expect(readCancelIntent()!.phase).toBe('confirmed');
    expect(sessionStorage.getItem(RETRY_INTENT_KEY)).toBe(historical);
    await user.click(screen.getByRole('link', { name: 'Operations' })); await screen.findByText(/Cancellation was applied/);
  });
});
