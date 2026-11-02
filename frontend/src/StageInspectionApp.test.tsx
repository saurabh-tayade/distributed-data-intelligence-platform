import { StrictMode } from 'react';
import { act, cleanup, fireEvent, render, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { App } from './App';
import { STAGE_STATES } from './managementApi';
import type { StageRecord } from './managementApi';
import { runHref } from './navigation';
import { deferred, fixture, jsonResponse } from './test/fixtures';
import { CORRELATION, FIRST_CURSOR, managementFailure } from './test/managementFixtures';
import { attempt, attemptsResponse, detailResponse, eventsResponse, runDetail, stageRecord, stagesResponse } from './test/runInspectionFixtures';

const id = runDetail().runId, attempts = [attempt(), attempt(2, 'RECONCILE'), attempt(3)];
const scope = () => within(screen.getByRole('region', { name: 'Stage Records' }));
const attemptsPanel = () => within(screen.getByRole('region', { name: 'Attempts' }));
const stageCalls = (fetch: ReturnType<typeof vi.fn>) => fetch.mock.calls.filter((call) => String(call[0]).includes('/stages?'));
const select = async (number = 1, user = userEvent.setup()) => user.click(await attemptsPanel().findByRole('button', { name: `Inspect stages for attempt ${number}` }));
const ready = () => scope().findByRole('list', { name: 'Stage records in server identity order' });
const url = (path: string) => new URL(path, 'http://localhost');
function respond(path: string): Response {
  if (path.includes('/attempts?')) return attemptsResponse(attempts);
  if (path.includes('/events?')) return eventsResponse();
  if (path.includes('/stages?')) return stagesResponse([stageRecord(0, 'CHECK_INPUTS', path.split('/')[7]!)]);
  if (path.startsWith('/api/management/')) return detailResponse(runDetail(Number.parseInt(path.split('/')[5]!.slice(-12), 16), 'FAILED'));
  return jsonResponse(fixture());
}
beforeEach(() => {
  window.history.replaceState(null, '', '/#/operations/executions/' + id);
  vi.spyOn(document, 'visibilityState', 'get').mockReturnValue('visible');
});
afterEach(() => { cleanup(); vi.useRealTimers(); });

describe('selected-attempt stage records', () => {
  it('loads nothing until selected, keeps parent panels and switches original/reconciliation/resumed ownership', async () => {
    const fetch = vi.fn().mockImplementation(async (path: string) => respond(path)); vi.stubGlobal('fetch', fetch); render(<App />);
    await attemptsPanel().findByText('RECONCILE'); expect(screen.queryByRole('region', { name: 'Stage Records' })).not.toBeInTheDocument(); expect(stageCalls(fetch)).toHaveLength(0);
    for (const number of [1, 2, 3, 1]) {
      await select(number); await ready(); expect(scope().getByText(`Selected attempt ${number} · ${attempts[number - 1]!.kind}`)).toBeVisible();
      expect(scope().getByText(attempts[number - 1]!.attemptId)).toBeVisible();
      expect(attemptsPanel().getByRole('button', { name: `Inspect stages for attempt ${number}` })).toHaveAttribute('aria-pressed', 'true');
      expect(stageCalls(fetch).at(-1)![0]).toContain(`/runs/${id}/attempts/${attempts[number - 1]!.attemptId}/stages?limit=20`);
    }
    expect(stageCalls(fetch)).toHaveLength(4); expect(within(screen.getByRole('region', { name: 'Run record' })).getByText('Run row version')).toBeVisible();
    expect(screen.getByRole('region', { name: 'Audit events' })).toBeVisible(); expect(screen.getByRole('region', { name: 'Latest control receipt' })).toBeVisible();
    expect(fetch.mock.calls.every((call) => (call[1] as RequestInit).method === 'GET')).toBe(true);
    expect(screen.queryByRole('button', { name: /recover|resume|reconcile|cancel|retry run|start stage|verify stage/i })).not.toBeInTheDocument();
  });

  it('preserves server identity order, item ordinals, stage states, nulls, counts and checkpoints', async () => {
    const rows: StageRecord[] = STAGE_STATES.map((state, index) => ({ ...stageRecord(index, 'VERIFY_RESULT'), state,
      startedAt: index === 0 ? '2026-11-02T10:00:00.123456Z' : null, finishedAt: ['FAILED', 'VERIFIED'].includes(state) ? '2026-11-01T10:00:00.654321Z' : null,
      outcomeCode: index === 2 ? 'PUBLICATION_INVALID' : null, counts: index === 0 ? { rows: '9007199254740993', gold_articles: '2' } : index === 1 ? {} : null }));
    vi.stubGlobal('fetch', vi.fn().mockImplementation(async (path: string) => path.includes('/stages?') ? stagesResponse(rows) : respond(path)));
    render(<App />); await select(); await ready();
    const cards = scope().getAllByRole('listitem'); expect(cards).toHaveLength(5);
    for (let index = 0; index < rows.length; index++) {
      const card = within(cards[index]!); expect(card.getByRole('heading', { level: 3 })).toHaveTextContent(`VERIFY_RESULT · Item ${index}`);
      expect(card.getByText(rows[index]!.state, { selector: 'dd' })).toBeVisible(); expect(card.getByText(rows[index]!.checkpointId)).toBeVisible();
    }
    expect(scope().getByText('9007199254740993')).toBeVisible(); expect(scope().getByText('gold_articles')).toBeVisible(); expect(scope().getByText('PUBLICATION_INVALID')).toBeVisible();
    expect(scope().getAllByText('No counts recorded.')).toHaveLength(4); expect(scope().getAllByText('Not available').length).toBeGreaterThan(0);
    expect(scope().getByText('10:00:00.123456 UTC').closest('time')).toHaveAttribute('datetime', rows[0]!.startedAt);
    expect(scope().getByText(/not execution time/)).toBeVisible(); expect(scope().getByText(/not a fresh integrity check/)).toBeVisible();
    expect(scope().queryByText(/duration|percent|inherited from|reused from/i)).not.toBeInTheDocument();
  });

  it('shows an empty stage collection without claiming complete history', async () => {
    vi.stubGlobal('fetch', vi.fn().mockImplementation(async (path: string) => path.includes('/stages?') ? stagesResponse([]) : respond(path)));
    render(<App />); await select(); expect(await scope().findByText('No stages returned on this page.')).toBeVisible();
    expect(scope().getByRole('button', { name: 'Next stages' })).toBeDisabled(); expect(scope().getByRole('button', { name: 'Previous stages' })).toBeDisabled();
    expect(scope().getByText(/This page is not a snapshot/)).toBeVisible();
  });

  it.each([[403, 'LOCAL_BOUNDARY_REJECTED'], [404, 'NOT_FOUND'], [503, 'MANAGEMENT_UNAVAILABLE'], [200, 'MALFORMED']] as const)('isolates safe stage HTTP %s failures and retry', async (status, code) => {
    let fail = true; const user = userEvent.setup(), fetch = vi.fn().mockImplementation(async (path: string) => path.includes('/stages?')
      ? fail ? jsonResponse(managementFailure(code), status) : stagesResponse() : respond(path));
    vi.stubGlobal('fetch', fetch); render(<App />); await select(1, user);
    const error = await scope().findByRole('alert'); expect(error).not.toHaveTextContent(/private|traceback/); if (status !== 200) expect(error).toHaveTextContent(CORRELATION);
    expect(screen.getByRole('region', { name: 'Run record' })).toHaveTextContent('Run row version'); expect(attemptsPanel().getByText('RECONCILE')).toBeVisible();
    expect(screen.getByRole('region', { name: 'Audit events' })).toHaveTextContent('STAGE_STARTED');
    fail = false; await user.click(scope().getByRole('button', { name: 'Try again' })); await ready();
    expect(fetch.mock.calls.filter((call) => String(call[0]).includes('/attempts?'))).toHaveLength(1); expect(scope().queryByRole('alert')).not.toBeInTheDocument();
  });

  it('removes stale stages after a failed explicit refresh and keeps selected identity visible', async () => {
    let reads = 0; const user = userEvent.setup();
    vi.stubGlobal('fetch', vi.fn().mockImplementation(async (path: string) => path.includes('/stages?')
      ? ++reads === 1 ? stagesResponse() : jsonResponse(managementFailure(), 503) : respond(path)));
    render(<App />); await select(1, user); await ready(); await user.click(scope().getByRole('button', { name: 'Refresh stages' }));
    expect(await scope().findByRole('alert')).toHaveTextContent('MANAGEMENT_UNAVAILABLE'); expect(scope().queryByRole('list')).not.toBeInTheDocument();
    expect(scope().getByText(attempts[0]!.attemptId)).toBeVisible(); expect(scope().queryByText('9007199254740993')).not.toBeInTheDocument();
  });
});

describe('explicit stage pagination', () => {
  it('pages next/previous, changes size and refreshes independently while selection resets cursors', async () => {
    const user = userEvent.setup(), fetch = vi.fn().mockImplementation(async (path: string) => {
      if (!path.includes('/stages?')) return respond(path);
      const query = url(path).searchParams, limit = Number(query.get('limit')), after = query.has('cursor'), owner = path.split('/')[7]!;
      return stagesResponse(Array.from({ length: after ? 1 : limit }, (_, i) => stageRecord(i + (after ? limit : 0), 'VERIFY_RESULT', owner)), limit, after ? null : FIRST_CURSOR);
    });
    vi.stubGlobal('fetch', fetch); render(<App />); await select(1, user); await ready(); expect(stageCalls(fetch)).toHaveLength(1);
    await user.click(scope().getByRole('button', { name: 'Next stages' })); await ready(); expect(scope().getByText(/Page 2/)).toBeVisible(); expect(scope().getByRole('heading', { name: 'VERIFY_RESULT · Item 20' })).toBeVisible();
    await user.click(scope().getByRole('button', { name: 'Previous stages' })); await ready(); expect(scope().getByText(/Page 1/)).toBeVisible();
    await user.selectOptions(scope().getByRole('combobox', { name: 'Stage Records per page' }), '5'); await ready(); expect(stageCalls(fetch).at(-1)![0]).toContain('limit=5');
    await user.click(scope().getByRole('button', { name: 'Next stages' })); await ready();
    await user.click(scope().getByRole('button', { name: 'Refresh stages' })); await ready(); expect(url(stageCalls(fetch).at(-1)![0] as string).searchParams.has('cursor')).toBe(false);
    await user.click(scope().getByRole('button', { name: 'Next stages' })); await ready(); await select(2, user); await ready();
    expect(scope().getByText(/Page 1/)).toBeVisible(); expect(stageCalls(fetch).at(-1)![0]).toContain(`/attempts/${attempts[1]!.attemptId}/stages?limit=20`);
    expect(fetch.mock.calls.filter((call) => String(call[0]).includes('/events?'))).toHaveLength(1);
  });

  it('offers an explicit first-page restart for invalid/expired cursors', async () => {
    const user = userEvent.setup(), fetch = vi.fn().mockImplementation(async (path: string) => !path.includes('/stages?') ? respond(path)
      : url(path).searchParams.has('cursor') ? jsonResponse(managementFailure('INVALID_ARGUMENT'), 400)
        : stagesResponse(Array.from({ length: 20 }, (_, i) => stageRecord(i)), 20, FIRST_CURSOR));
    vi.stubGlobal('fetch', fetch); render(<App />); await select(1, user); await ready(); await user.click(scope().getByRole('button', { name: 'Next stages' }));
    expect(await scope().findByRole('alert')).toHaveTextContent('expire after 24 hours'); expect(scope().queryByRole('list')).not.toBeInTheDocument();
    await user.click(scope().getByRole('button', { name: 'Start from first page' })); await ready(); expect(scope().getByText(/Page 1/)).toBeVisible();
    expect(url(stageCalls(fetch).at(-1)![0] as string).searchParams.has('cursor')).toBe(false);
  });
});

describe('stage request invalidation', () => {
  it('ignores late success and failure during rapid attempt selection', async () => {
    const one = deferred<Response>(), two = deferred<Response>(); const fetch = vi.fn().mockImplementation(async (path: string) => !path.includes('/stages?') ? respond(path)
      : path.includes('/' + attempts[0]!.attemptId + '/') ? one.promise : path.includes('/' + attempts[1]!.attemptId + '/') ? two.promise : respond(path));
    vi.stubGlobal('fetch', fetch); render(<App />); await select(1); expect(scope().getByText('Loading stages')).toBeVisible(); await select(2); await select(3); await ready();
    for (const call of stageCalls(fetch).slice(0, 2)) expect((call[1] as RequestInit).signal?.aborted).toBe(true);
    await act(async () => { one.resolve(stagesResponse()); two.reject(new Error('PRIVATE')); });
    expect(scope().getByText(attempts[2]!.attemptId)).toBeVisible(); expect(scope().queryByRole('alert')).not.toBeInTheDocument(); expect(stageCalls(fetch)).toHaveLength(3);
  });
  it.each(['run', 'Analytics'] as const)('aborts stage reads after navigation to another %s and rejects late responses', async (target) => {
    const late = deferred<Response>(), fetch = vi.fn().mockImplementation(async (path: string) => path.includes('/stages?') ? late.promise : respond(path));
    vi.stubGlobal('fetch', fetch); render(<App />); await select();
    act(() => { window.location.hash = target === 'run' ? runHref(runDetail(2).runId) : '#/analytics'; });
    await waitFor(() => expect(screen.queryByRole('region', { name: 'Stage Records' })).not.toBeInTheDocument());
    expect((stageCalls(fetch)[0]![1] as RequestInit).signal?.aborted).toBe(true); await act(async () => late.resolve(stagesResponse()));
    expect(screen.queryByRole('region', { name: 'Stage Records' })).not.toBeInTheDocument(); expect(stageCalls(fetch)).toHaveLength(1);
  });
  it('refresh invalidates a pending second page and ignores its late response', async () => {
    const user = userEvent.setup(), late = deferred<Response>(); let firsts = 0;
    const fetch = vi.fn().mockImplementation(async (path: string) => !path.includes('/stages?') ? respond(path)
      : url(path).searchParams.has('cursor') ? late.promise : ++firsts === 1
        ? stagesResponse(Array.from({ length: 20 }, (_, i) => stageRecord(i)), 20, FIRST_CURSOR) : stagesResponse([]));
    vi.stubGlobal('fetch', fetch); render(<App />); await select(1, user); await ready(); await user.click(scope().getByRole('button', { name: 'Next stages' }));
    expect(scope().getByText('Loading stages')).toBeVisible(); await user.click(scope().getByRole('button', { name: 'Refresh stages' }));
    expect(await scope().findByText('No stages returned on this page.')).toBeVisible(); expect((stageCalls(fetch)[1]![1] as RequestInit).signal?.aborted).toBe(true);
    await act(async () => late.resolve(stagesResponse([stageRecord(20)]))); expect(scope().queryByRole('list')).not.toBeInTheDocument(); expect(scope().getByText(/Page 1/)).toBeVisible();
  });
  it('cancels hidden document reads, resumes only the selected page, and ignores stale results', async () => {
    const late = deferred<Response>(); let reads = 0;
    const visibility = vi.spyOn(document, 'visibilityState', 'get').mockReturnValue('visible');
    const fetch = vi.fn().mockImplementation(async (path: string) => !path.includes('/stages?') ? respond(path) : ++reads === 1 ? late.promise : stagesResponse([]));
    vi.stubGlobal('fetch', fetch); render(<App />); await select();
    act(() => { visibility.mockReturnValue('hidden'); document.dispatchEvent(new Event('visibilitychange')); });
    expect(scope().getByText(/Stage reads are suspended/)).toBeVisible(); expect((stageCalls(fetch)[0]![1] as RequestInit).signal?.aborted).toBe(true);
    await act(async () => late.resolve(stagesResponse())); expect(scope().queryByRole('list')).not.toBeInTheDocument(); expect(stageCalls(fetch)).toHaveLength(1);
    act(() => { visibility.mockReturnValue('visible'); document.dispatchEvent(new Event('visibilitychange')); });
    expect(await scope().findByText('No stages returned on this page.')).toBeVisible(); expect(stageCalls(fetch)).toHaveLength(2);
  });
  it('does not poll stages or refresh them with active run polling', async () => {
    vi.useFakeTimers();
    const fetch = vi.fn().mockImplementation(async (path: string) => /\/runs\/[^/?]+$/.test(path) ? detailResponse(runDetail(1, 'RUNNING')) : respond(path));
    vi.stubGlobal('fetch', fetch); render(<App />); await act(async () => { await vi.advanceTimersByTimeAsync(0); });
    fireEvent.click(attemptsPanel().getByRole('button', { name: 'Inspect stages for attempt 1' })); await act(async () => { await vi.advanceTimersByTimeAsync(0); });
    await act(async () => { await vi.advanceTimersByTimeAsync(20000); }); expect(stageCalls(fetch)).toHaveLength(1);
    expect(fetch.mock.calls.filter((call) => String(call[0]).endsWith('/' + id)).length).toBeGreaterThan(1);
    fireEvent.click(screen.getByRole('button', { name: 'Refresh run detail' })); await act(async () => { await vi.advanceTimersByTimeAsync(0); }); expect(stageCalls(fetch)).toHaveLength(1);
    expect(fetch.mock.calls.every((call) => (call[1] as RequestInit).method === 'GET')).toBe(true);
  });
  it('cleans up pending stage transports under StrictMode unmount', async () => {
    const late = deferred<Response>(), fetch = vi.fn().mockImplementation(async (path: string) => path.includes('/stages?') ? late.promise : respond(path));
    vi.stubGlobal('fetch', fetch); const view = render(<StrictMode><App /></StrictMode>); await select();
    expect(stageCalls(fetch).length).toBeGreaterThanOrEqual(1); view.unmount();
    for (const call of stageCalls(fetch)) expect((call[1] as RequestInit).signal?.aborted).toBe(true);
    await act(async () => late.reject(new DOMException('aborted', 'AbortError'))); expect(screen.queryByRole('region', { name: 'Stage Records' })).not.toBeInTheDocument();
  });
});
