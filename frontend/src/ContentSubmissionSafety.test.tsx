import { act, render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { App } from './App';
import { RunSubmissionPanel } from './operations/RunSubmissionPanel';
import { readRunIntent, reviewRunSubmission, RUN_INTENT_KEY, saveRunIntent } from './operations/runIntent';
import { contentCommand, contentReceipt, contentRequest, contentRun, historyPage } from './test/contentSubmissionFixtures';
import { deferred, fixture, jsonResponse } from './test/fixtures';
import { managementFailure } from './test/managementFixtures';
import { request as reliabilityRequest, submissionCommand } from './test/submissionFixtures';

const mount = () => render(<RunSubmissionPanel visible />);
const review = () => reviewRunSubmission(JSON.stringify(contentRequest(), null, 2));
async function confirm(user: ReturnType<typeof userEvent.setup>) {
  await user.click(screen.getByRole('checkbox', { name: /I acknowledge the Content queue limitations/ }));
  await user.click(screen.getByRole('button', { name: 'Confirm Content submission' }));
}
beforeEach(() => { sessionStorage.clear(); window.history.replaceState(null, '', '/#/operations/submit/content'); });

describe('Content retained intent and uncertain admission safety', () => {
  it.each(['review', 'pending', 'uncertain', 'rejected', 'confirmed'] as const)('retains legacy Reliability %s bytes without migration on mount', (phase) => {
    const value = { version: 1, phase, command: submissionCommand(), ...(phase === 'confirmed' ? { receipt: { runId: contentRun().runId, state: 'QUEUED', rowVersion: '0', etag: '"0"', correlationId: '10000000-0000-4000-8000-000000000001', location: `/api/management/v1/runs/${contentRun().runId}` } } : {}) };
    const raw = JSON.stringify(value, null, 2); sessionStorage.setItem(RUN_INTENT_KEY, raw); const fetch = vi.fn(); vi.stubGlobal('fetch', fetch); mount();
    expect(readRunIntent()).toEqual(value); expect(sessionStorage.getItem(RUN_INTENT_KEY)).toBe(raw); expect(fetch).not.toHaveBeenCalled();
  });
  it.each(['pending', 'uncertain'] as const)('prevents cross-product replacement of %s requests in either direction', (phase) => {
    for (const [original, next] of [[contentCommand(), submissionCommand()], [submissionCommand(), contentCommand()]]) {
      saveRunIntent({ version: 1, phase, command: original! }); const raw = sessionStorage.getItem(RUN_INTENT_KEY);
      expect(() => reviewRunSubmission(next!.body)).toThrow('Resolve the existing run intent first'); expect(sessionStorage.getItem(RUN_INTENT_KEY)).toBe(raw);
    }
  });
  it('requires fresh acknowledgment after refresh of an unsubmitted Content review', async () => {
    vi.stubGlobal('fetch', vi.fn()); review(); const original = readRunIntent()!.command; const user = userEvent.setup(), first = mount();
    await user.click(screen.getByRole('checkbox')); expect(screen.getByRole('button', { name: 'Confirm Content submission' })).toBeEnabled(); first.unmount(); mount();
    expect(screen.getByRole('checkbox')).not.toBeChecked(); expect(screen.getByRole('button', { name: 'Confirm Content submission' })).toBeDisabled(); expect(readRunIntent()!.command).toEqual(original);
  });
  it('does not transfer acknowledgment to a different deliberate request', async () => {
    const fetch = vi.fn(); vi.stubGlobal('fetch', fetch); review(); const user = userEvent.setup(); mount(); await user.click(screen.getByRole('checkbox'));
    await user.click(screen.getByRole('button', { name: 'Cancel run review' })); act(() => review());
    expect(screen.getByRole('checkbox')).not.toBeChecked(); expect(screen.getByRole('button', { name: 'Confirm Content submission' })).toBeDisabled(); expect(fetch).not.toHaveBeenCalled();
  });
  it('cancels an unsubmitted review without mutating the server', async () => {
    const fetch = vi.fn(); vi.stubGlobal('fetch', fetch); review(); const user = userEvent.setup(); mount();
    await user.click(screen.getByRole('button', { name: 'Cancel run review' })); expect(readRunIntent()).toBeNull(); expect(fetch).not.toHaveBeenCalled();
  });
  it.each(['lost', 'malformed', 'identity', 'unavailable'])('preserves exact key/body after %s acknowledgment with no automatic retry', async (fault) => {
    const fetch = vi.fn().mockImplementationOnce(() => fault === 'lost' ? Promise.reject(new Error('lost response')) : Promise.resolve(fault === 'malformed'
      ? new Response('invalid', { status: 202 }) : fault === 'identity' ? contentReceipt({ ...contentRun(), inputArtifactIds: [] }) : jsonResponse(managementFailure(), 503)))
      .mockResolvedValueOnce(contentReceipt()); vi.stubGlobal('fetch', fetch); review(); const original = readRunIntent()!.command; const user = userEvent.setup();
    const first = mount(); await confirm(user); await screen.findByText(/Outcome unknown/); expect(readRunIntent()).toMatchObject({ phase: 'uncertain', command: original });
    expect(fetch).toHaveBeenCalledTimes(1); expect(screen.queryByRole('button', { name: 'Cancel run review' })).not.toBeInTheDocument(); expect(screen.queryByRole('checkbox')).not.toBeInTheDocument();
    first.unmount(); const second = mount(); expect(fetch).toHaveBeenCalledTimes(1); expect(readRunIntent()!.command).toEqual(original);
    await user.click(screen.getByRole('button', { name: 'Resolve same run request' })); await screen.findByText(/Admission confirmed/);
    expect(fetch.mock.calls.map(([, init]) => ({ body: init.body, key: init.headers['Idempotency-Key'] }))).toEqual([{ body: original.body, key: original.key }, { body: original.body, key: original.key }]);
    expect(readRunIntent()!.receipt!.state).toBe('QUEUED'); second.unmount();
  });
  it('resolves retained pending requests without acknowledgment or discovery', async () => {
    const fetch = vi.fn().mockResolvedValue(contentReceipt()); vi.stubGlobal('fetch', fetch); const command = contentCommand();
    saveRunIntent({ version: 1, phase: 'pending', command }); const user = userEvent.setup(); mount();
    expect(fetch).not.toHaveBeenCalled(); expect(screen.queryByRole('checkbox')).not.toBeInTheDocument(); expect(screen.getByText(/Outcome unknown/)).toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'Resolve same run request' })); await screen.findByRole('link', { name: 'Open admitted run' });
    expect(readRunIntent()).toMatchObject({ phase: 'confirmed', command }); expect(fetch).toHaveBeenCalledTimes(1);
  });
  it.each([[400, 'INVALID_ARGUMENT'], [403, 'LOCAL_BOUNDARY_REJECTED'], [404, 'NOT_FOUND'], [409, 'SOURCE_DISABLED'], [429, 'MANAGEMENT_CAPACITY_EXCEEDED']])('handles definitive HTTP %s rejection without replaying', async (status, code) => {
    const fetch = vi.fn().mockResolvedValue(jsonResponse(managementFailure(String(code)), Number(status))); vi.stubGlobal('fetch', fetch); review(); const original = readRunIntent()!.command;
    const user = userEvent.setup(); mount(); await confirm(user); await screen.findByText(String(code)); expect(readRunIntent()).toMatchObject({ phase: 'rejected', command: original });
    expect(screen.queryByRole('button', { name: 'Resolve same run request' })).not.toBeInTheDocument(); expect(fetch).toHaveBeenCalledTimes(1);
    await user.click(screen.getByRole('button', { name: 'Return to selection and deliberate review' })); expect(readRunIntent()).toBeNull();
  });
  it('retains uncertainty when an app is unmounted during POST and ignores a late 202', async () => {
    const late = deferred<Response>(), fetch = vi.fn().mockReturnValue(late.promise); vi.stubGlobal('fetch', fetch); review(); const user = userEvent.setup(), first = mount();
    await confirm(user); const command = readRunIntent()!.command; first.unmount(); expect((fetch.mock.calls[0]![1].signal as AbortSignal).aborted).toBe(true);
    expect(readRunIntent()).toMatchObject({ phase: 'uncertain', command }); await act(async () => late.resolve(contentReceipt())); expect(readRunIntent()).toMatchObject({ phase: 'uncertain', command });
    mount(); expect(fetch).toHaveBeenCalledTimes(1);
  });
  it('keeps a single mounted mutation through cross-product and Analytics navigation', async () => {
    const late = deferred<Response>(); const fetch = vi.fn().mockImplementation((path: string, init: RequestInit) => init.method === 'POST' ? late.promise : Promise.resolve(jsonResponse(path.includes('/sources?') ? historyPage([]) : fixture())));
    vi.stubGlobal('fetch', fetch); review(); render(<App />); const user = userEvent.setup(); await confirm(user);
    const original = readRunIntent()!.command; await user.click(screen.getByRole('link', { name: 'Submit Reliability Run' }));
    expect(screen.getAllByRole('heading', { name: 'Content submission receipt' })).toHaveLength(1); expect(screen.queryByRole('radio')).not.toBeInTheDocument();
    await user.click(screen.getByRole('link', { name: 'Analytics' })); expect(screen.queryByRole('heading', { name: 'Content submission receipt' })).not.toBeInTheDocument();
    expect((fetch.mock.calls.find(([, init]) => init.method === 'POST')![1].signal as AbortSignal).aborted).toBe(false);
    await act(async () => late.resolve(contentReceipt())); expect(readRunIntent()).toMatchObject({ phase: 'confirmed', command: original });
    await user.click(screen.getByRole('link', { name: 'Operations' })); await screen.findByRole('link', { name: 'Open admitted run' });
    expect(fetch.mock.calls.filter(([, init]) => init.method === 'POST')).toHaveLength(1); expect(fetch.mock.calls.some(([path]) => path.includes('/execution-profiles'))).toBe(false);
  });
  it('does not submit if retaining the pending phase fails', async () => {
    const fetch = vi.fn(); vi.stubGlobal('fetch', fetch); review(); const original = sessionStorage.getItem(RUN_INTENT_KEY); const user = userEvent.setup(); mount();
    vi.spyOn(Storage.prototype, 'setItem').mockImplementation(() => { throw new Error('quota'); }); await confirm(user);
    await screen.findByText(/Tab storage could not be updated/); expect(sessionStorage.getItem(RUN_INTENT_KEY)).toBe(original); expect(fetch).not.toHaveBeenCalled();
  });
  it('retains the original pending body if storing a successful receipt fails', async () => {
    const late = deferred<Response>(), fetch = vi.fn().mockReturnValue(late.promise); vi.stubGlobal('fetch', fetch); review(); const user = userEvent.setup(); mount(); await confirm(user);
    const original = readRunIntent()!.command; vi.spyOn(Storage.prototype, 'setItem').mockImplementation(() => { throw new Error('quota'); });
    await act(async () => late.resolve(contentReceipt())); await screen.findByText(/Tab storage could not be updated/);
    expect(readRunIntent()).toMatchObject({ phase: 'pending', command: original }); expect(screen.queryByText(/Admission confirmed/)).not.toBeInTheDocument(); expect(fetch).toHaveBeenCalledTimes(1);
  });
  it('fails closed when tab storage cannot be read', () => {
    vi.spyOn(Storage.prototype, 'getItem').mockImplementation(() => { throw new Error('denied'); }); const fetch = vi.fn(); vi.stubGlobal('fetch', fetch); render(<App />);
    expect(screen.getByRole('heading', { name: 'Run submission unavailable' })).toBeInTheDocument(); expect(screen.queryByRole('radio')).not.toBeInTheDocument(); expect(fetch).not.toHaveBeenCalled();
  });
  it.each(['{"version":2}', '{"version":1,"phase":"pending","command":{"key":"x","body":"{\\"pipeline\\":\\"OTHER\\"}"}}'])('preserves unrecognized stored bytes and blocks both journeys: %s', async (raw) => {
    sessionStorage.setItem(RUN_INTENT_KEY, raw); const fetch = vi.fn().mockResolvedValue(jsonResponse(historyPage([]))); vi.stubGlobal('fetch', fetch); render(<App />); const user = userEvent.setup();
    expect(screen.getByRole('heading', { name: 'Run submission unavailable' })).toBeInTheDocument(); await user.click(screen.getByRole('link', { name: 'Submit Reliability Run' }));
    await waitFor(() => expect(window.location.hash).toBe('#/operations/submit')); expect(sessionStorage.getItem(RUN_INTENT_KEY)).toBe(raw); expect(fetch).not.toHaveBeenCalled();
  });
  it('blocks a late source-review attempt from replacing a newly retained Reliability intent', () => {
    reviewRunSubmission(JSON.stringify(reliabilityRequest())); const original = sessionStorage.getItem(RUN_INTENT_KEY);
    expect(() => review()).toThrow(); expect(sessionStorage.getItem(RUN_INTENT_KEY)).toBe(original);
  });
});
