import { StrictMode } from 'react';
import { act, fireEvent, render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { App } from './App';
import { RunSubmissionPanel } from './operations/RunSubmissionPanel';
import { readRunIntent, reviewRunSubmission, RUN_INTENT_KEY, saveRunIntent } from './operations/runIntent';
import { SOURCE_INTENT_KEY } from './operations/sourceIntent';
import { deferred, fixture, jsonResponse } from './test/fixtures';
import { managementFailure } from './test/managementFixtures';
import { attemptsResponse, detailResponse, eventsResponse } from './test/runInspectionFixtures';
import { historyPage, profile, request, submissionCommand, submissionResponse, submittedRun } from './test/submissionFixtures';

function review() { reviewRunSubmission(JSON.stringify(request())); }
function mount() { return render(<StrictMode><RunSubmissionPanel visible /></StrictMode>); }
beforeEach(() => { sessionStorage.clear(); window.history.replaceState(null, '', '/#/operations/submit'); });

describe('guarded run submission checkpoint B', () => {
  it('does not submit during review and stores its own namespaced, versioned frozen intent', () => {
    const fetch = vi.fn(); vi.stubGlobal('fetch', fetch); sessionStorage.setItem(SOURCE_INTENT_KEY, 'unrelated source intent');
    review(); mount(); expect(fetch).not.toHaveBeenCalled();
    expect(readRunIntent()).toMatchObject({ version: 1, phase: 'review', command: { body: JSON.stringify(request()) } });
    expect(readRunIntent()!.command.key).toMatch(/^[0-9a-f-]{36}$/);
    expect(sessionStorage.getItem(SOURCE_INTENT_KEY)).toBe('unrelated source intent');
    expect(() => review()).toThrow();
  });
  it('confirms one exact request, suppresses duplicate clicks and presents accepted versus processing semantics', async () => {
    const late = deferred<Response>(), fetch = vi.fn().mockReturnValue(late.promise); vi.stubGlobal('fetch', fetch);
    review(); const original = readRunIntent()!.command; mount();
    const confirm = screen.getByRole('button', { name: 'Confirm Reliability submission' });
    fireEvent.click(confirm); fireEvent.click(confirm); expect(fetch).toHaveBeenCalledTimes(1);
    expect(screen.getByRole('button', { name: 'Request pending' })).toBeDisabled();
    await act(async () => late.resolve(submissionResponse()));
    await screen.findByRole('link', { name: 'Open admitted run' });
    expect(readRunIntent()).toMatchObject({ phase: 'confirmed', command: original, receipt: { state: 'QUEUED', runId: submittedRun().runId, etag: `"${submittedRun().rowVersion}"` } });
    expect(screen.getByText(/QUEUED means accepted/)).toBeInTheDocument();
    expect(screen.getByText(/depends on the configured Reliability worker/)).toBeInTheDocument();
    expect(fetch.mock.calls[0]![1]).toMatchObject({ body: original.body, headers: { 'Idempotency-Key': original.key } });
  });
  it('retains an ambiguous outcome across same-tab reload and resolves only by explicit exact replay', async () => {
    const fetch = vi.fn().mockRejectedValueOnce(new TypeError('connection lost')).mockResolvedValueOnce(submissionResponse(submittedRun('SUCCEEDED'))); vi.stubGlobal('fetch', fetch);
    review(); const original = readRunIntent()!.command, user = userEvent.setup(); const first = mount();
    await user.click(screen.getByRole('button', { name: 'Confirm Reliability submission' }));
    await screen.findByText(/Outcome unknown/); expect(fetch).toHaveBeenCalledTimes(1); first.unmount();
    mount(); expect(readRunIntent()!.command).toEqual(original); expect(fetch).toHaveBeenCalledTimes(1);
    expect(screen.queryByRole('button', { name: /Cancel|Return to selection/ })).not.toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'Resolve same run request' }));
    await screen.findByRole('link', { name: 'Open admitted run' });
    const calls = fetch.mock.calls.map(([, init]) => ({ body: init.body, key: init.headers['Idempotency-Key'] }));
    expect(calls).toEqual([{ body: original.body, key: original.key }, { body: original.body, key: original.key }]);
    expect(readRunIntent()!.receipt!.state).toBe('SUCCEEDED'); expect(screen.getByText(/advanced or terminal persisted run/)).toBeInTheDocument();
  });
  it.each([409, 429])('requires deliberate new review after definitive HTTP %s rejection', async (status) => {
    const code = status === 409 ? 'STATE_CONFLICT' : 'MANAGEMENT_CAPACITY_EXCEEDED';
    const fetch = vi.fn().mockResolvedValue(jsonResponse(managementFailure(code), status)); vi.stubGlobal('fetch', fetch);
    review(); const original = readRunIntent()!.command, user = userEvent.setup(); mount();
    await user.click(screen.getByRole('button', { name: 'Confirm Reliability submission' }));
    await screen.findByText(code); expect(readRunIntent()!.phase).toBe('rejected'); expect(fetch).toHaveBeenCalledTimes(1);
    expect(screen.queryByRole('button', { name: 'Resolve same run request' })).not.toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'Return to selection and deliberate review' }));
    expect(readRunIntent()).toBeNull(); act(() => review());
    expect(readRunIntent()!.command.key).not.toBe(original.key); expect(fetch).toHaveBeenCalledTimes(1);
  });
  it('retains coordinator unavailability as an unresolved original intent', async () => {
    const fetch = vi.fn().mockResolvedValue(jsonResponse(managementFailure('COORDINATOR_RECOVERY_REQUIRED'), 503)); vi.stubGlobal('fetch', fetch);
    review(); mount(); const user = userEvent.setup(); await user.click(screen.getByRole('button', { name: 'Confirm Reliability submission' }));
    await screen.findByText('COORDINATOR_RECOVERY_REQUIRED'); expect(readRunIntent()!.phase).toBe('uncertain');
    expect(fetch).toHaveBeenCalledTimes(1); expect(screen.getByRole('button', { name: 'Resolve same run request' })).toBeEnabled();
    expect(screen.queryByText('/private/secret traceback')).not.toBeInTheDocument();
  });
  it('allows cancelling only an unsubmitted review and creates a fresh key for a deliberately changed intent', async () => {
    const fetch = vi.fn(); vi.stubGlobal('fetch', fetch); review(); const original = readRunIntent()!.command; const user = userEvent.setup(); mount();
    await user.click(screen.getByRole('button', { name: 'Cancel run review' }));
    act(() => reviewRunSubmission(JSON.stringify({ ...request(), sources: [{ sourceId: 'beta', revision: 3 }] })));
    expect(readRunIntent()!.command.key).not.toBe(original.key); expect(readRunIntent()!.command.body).not.toBe(original.body); expect(fetch).not.toHaveBeenCalled();
  });
  it('a new unsubmitted review never inherits a prior confirmed receipt', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(submissionResponse())); review(); mount(); const user = userEvent.setup();
    await user.click(screen.getByRole('button', { name: 'Confirm Reliability submission' })); await screen.findByText(/Admission confirmed for/);
    await user.click(screen.getByRole('button', { name: 'Dismiss confirmed receipt' })); act(() => review());
    expect(screen.getByRole('heading', { name: 'Review Reliability run' })).toBeInTheDocument();
    expect(screen.queryByText(/Admission confirmed for/)).not.toBeInTheDocument(); expect(readRunIntent()!.receipt).toBeUndefined();
  });
  it('tracks a pending result across route changes without redirecting or overwriting the current route', async () => {
    const late = deferred<Response>();
    const fetch = vi.fn().mockImplementation((path: string, init: RequestInit) => {
      if (init.method === 'POST') return late.promise;
      if (path.startsWith('/api/management/v1/runs?')) return Promise.resolve(jsonResponse(historyPage([])));
      if (path.includes('/attempts?')) return Promise.resolve(attemptsResponse([]));
      if (path.includes('/events?')) return Promise.resolve(eventsResponse([]));
      if (path === `/api/management/v1/runs/${submittedRun().runId}`) return Promise.resolve(detailResponse(submittedRun()));
      if (path.startsWith('/api/management/v1/execution-profiles')) return Promise.resolve(jsonResponse(historyPage([profile()])));
      return Promise.resolve(jsonResponse(fixture()));
    }); vi.stubGlobal('fetch', fetch); review(); render(<App />); const user = userEvent.setup();
    await user.click(screen.getByRole('button', { name: 'Confirm Reliability submission' }));
    await user.click(screen.getByRole('link', { name: 'Executions' }));
    const signal = fetch.mock.calls.find(([, init]) => init.method === 'POST')![1].signal as AbortSignal;
    expect(signal.aborted).toBe(false); await act(async () => late.resolve(submissionResponse()));
    expect(window.location.hash).toBe('#/operations/executions'); await screen.findByRole('link', { name: 'Open admitted run' });
    await user.click(screen.getByRole('link', { name: 'Open admitted run' }));
    await waitFor(() => expect(window.location.hash).toBe(`#/operations/executions/${submittedRun().runId}`));
    await screen.findByRole('heading', { name: /^Run detail/ });
    expect(fetch.mock.calls.filter(([, init]) => init.method === 'POST')).toHaveLength(1);
  });
  it('invalidates a late response after app unmount, retaining the original pending request for reload', async () => {
    const late = deferred<Response>(), fetch = vi.fn().mockReturnValue(late.promise); vi.stubGlobal('fetch', fetch); review();
    const first = mount(), user = userEvent.setup(); await user.click(screen.getByRole('button', { name: 'Confirm Reliability submission' }));
    const command = readRunIntent()!.command; first.unmount();
    expect((fetch.mock.calls[0]![1].signal as AbortSignal).aborted).toBe(true); expect(readRunIntent()!.phase).toBe('uncertain');
    await act(async () => late.resolve(submissionResponse())); expect(readRunIntent()).toMatchObject({ phase: 'uncertain', command });
    mount(); expect(fetch).toHaveBeenCalledTimes(1);
  });
  it('keeps mutation outcomes tracked while Analytics hides Operations', async () => {
    const late = deferred<Response>();
    const fetch = vi.fn().mockImplementation((path: string, init: RequestInit) => init.method === 'POST' ? late.promise
      : Promise.resolve(path.includes('/sources?') ? jsonResponse(historyPage([])) : jsonResponse(fixture())));
    vi.stubGlobal('fetch', fetch); review(); render(<App />); const user = userEvent.setup();
    await user.click(screen.getByRole('button', { name: 'Confirm Reliability submission' }));
    await user.click(screen.getByRole('link', { name: 'Analytics' }));
    expect(screen.queryByRole('heading', { name: 'Reliability submission receipt' })).not.toBeInTheDocument();
    await act(async () => late.resolve(submissionResponse())); expect(readRunIntent()!.phase).toBe('confirmed');
    expect(window.location.hash).toBe('#/analytics'); await user.click(screen.getByRole('link', { name: 'Operations' }));
    await screen.findByRole('link', { name: 'Open admitted run' }); expect(fetch.mock.calls.filter(([, init]) => init.method === 'POST')).toHaveLength(1);
  });
  it('shows retained pending requests as unknown after a reload without sending them', () => {
    const fetch = vi.fn(); vi.stubGlobal('fetch', fetch);
    saveRunIntent({ version: 1, phase: 'pending', command: submissionCommand() }); mount();
    expect(screen.getByText(/Outcome unknown/)).toBeInTheDocument(); expect(fetch).not.toHaveBeenCalled();
  });
  it.each(['broken-json', '{"version":2}', '{"version":1,"phase":"pending","command":{"key":"a","body":"{}"}}', 'x'.repeat(24577)])('fails closed on invalid stored run intent', (raw) => {
    sessionStorage.setItem(RUN_INTENT_KEY, raw); const fetch = vi.fn(); vi.stubGlobal('fetch', fetch); mount();
    expect(screen.getByRole('heading', { name: 'Run submission unavailable' })).toBeInTheDocument(); expect(fetch).not.toHaveBeenCalled();
  });
});
