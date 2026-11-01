import { StrictMode } from 'react';
import { act, render, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { App } from './App';
import { CONTROL_STATES } from './managementApi';
import { parseWorkspaceRoute, runHref } from './navigation';
import { deferred, fixture, FINGERPRINT, jsonResponse } from './test/fixtures';
import { FIRST_CURSOR, CORRELATION, managementFailure, revisionPage, sourcePage, sourceResponse } from './test/managementFixtures';
import { run, runPage } from './test/runFixtures';
import { ATTEMPT_ID, CONTROL_ID, attempt, attemptsResponse, auditEvent, controlReceipt, controlResponse, detailResponse, eventsResponse, historyPage, latestControl, runDetail } from './test/runInspectionFixtures';

const id = run().runId, route = '#/operations/executions/' + id;
const panel = (name: string) => within(screen.getByRole('region', { name }));
const record = () => panel('Run record');
const ready = async () => within(await screen.findByRole('region', { name: 'Run record' })).findByText('Run row version');
const urlOf = (path: string) => new URL(path, 'http://localhost');
const workspace = (name: string) => within(screen.getByRole('navigation', { name: 'Workspaces' })).getByRole('link', { name });
const operation = (name: string) => within(screen.getByRole('navigation', { name: 'Operations views' })).getByRole('link', { name });
function respond(path: string): Response {
  if (path.startsWith('/api/management/v1/runs?')) return jsonResponse(runPage([run(1, 'FAILED')]));
  if (path.startsWith('/api/management/v1/runs/')) {
    if (path.includes('/attempts?')) return attemptsResponse([attempt(), attempt(2, 'RECONCILE')]);
    if (path.includes('/events?')) return eventsResponse();
    const runId = path.split('/')[5]!;
    if (path.includes('/controls/')) return controlResponse({ ...controlReceipt('APPLIED', runId), controlId: path.split('/').at(-1)! });
    return detailResponse(runDetail(Number.parseInt(runId.slice(-12), 16), 'FAILED'));
  }
  if (path.startsWith('/api/management/v1/sources?')) return jsonResponse(sourcePage());
  if (path.includes('/revisions?')) return jsonResponse(revisionPage());
  if (path.startsWith('/api/management/')) return sourceResponse();
  const body = fixture(), selected = urlOf(path).searchParams.get('sourceId');
  if (selected) { body.data = body.data.filter((item) => item.sourceId === selected); body.meta.returned = body.data.length; }
  return jsonResponse(body);
}
beforeEach(() => { window.history.replaceState(null, '', '/' + route); });

describe('read-only run inspection and panel domains', () => {
  it('opens directly with independent loading, exact captured references/counts and audit context', async () => {
    const pending = deferred<Response>(), pendingAttempts = deferred<Response>(), pendingEvents = deferred<Response>();
    const fetch = vi.fn().mockImplementation(async (path: string) => path.includes('/attempts?') ? pendingAttempts.promise
      : path.includes('/events?') ? pendingEvents.promise : path.includes('/controls/') ? respond(path) : pending.promise);
    vi.stubGlobal('fetch', fetch); render(<App />);
    expect(record().getByText('Loading run detail')).toBeVisible(); expect(panel('Attempts').getByText('Loading attempts')).toBeVisible();
    expect(panel('Audit events').getByText('Loading audit events')).toBeVisible();
    const data = runDetail(1, 'FAILED'); data.counts = { rows: '9007199254740993', bytes: '9223372036854775807' };
    data.latestControl = latestControl(); data.attempts = { count: '2', currentAttemptId: ATTEMPT_ID };
    await act(async () => { pending.resolve(detailResponse(data)); pendingAttempts.resolve(attemptsResponse([attempt(), attempt(2, 'RECONCILE')]));
      pendingEvents.resolve(eventsResponse([{ ...auditEvent(), context: { attemptId: ATTEMPT_ID, controlId: CONTROL_ID } }])); });
    await ready();
    expect(record().getByText('9223372036854775807')).toBeVisible(); expect(record().getByText('Captured revision 1', { exact: false })).toBeVisible();
    expect(record().getByText('alpha')).toBeVisible(); expect(record().getByText(data.inputArtifactIds[0]!)).toBeVisible();
    expect(record().queryByRole('link')).not.toBeInTheDocument();
    expect(panel('Attempts').getByText('RECONCILE')).toBeVisible(); expect(panel('Attempts').getByText(/Stage count does not identify stages/)).toBeVisible();
    expect(panel('Audit events').getByRole('heading', { name: /Event 9007199254740993 STAGE_STARTED/ })).toBeVisible();
    expect(panel('Audit events').getByText('9007199254740994')).toBeVisible(); expect(panel('Audit events').getByText(CONTROL_ID)).toBeVisible();
    expect(panel('Audit events').getByText(/These are not named stage records/)).toBeVisible();
    expect(await panel('Latest control receipt').findByText('APPLIED', { selector: 'dd' })).toBeVisible();
    expect(record().getByText('REQUESTED', { selector: 'dd' })).toBeVisible(); // Independent observations do not rewrite the parent snapshot.
    expect(fetch.mock.calls.every((call) => (call[1] as RequestInit).method === 'GET')).toBe(true);
    expect(fetch.mock.calls.some((call) => String(call[0]).includes('/sources'))).toBe(false);
    expect(screen.queryByRole('button', { name: /reconcile|resume|cancel|submit|retry run/i })).not.toBeInTheDocument();
  });

  it.each(['content-intelligence', 'application-reliability'] as const)('shows historical %s publication verification without activation or artifact links', async (product) => {
    const user = userEvent.setup(), data = runDetail(1, 'SUCCEEDED', product);
    vi.stubGlobal('fetch', vi.fn().mockImplementation(async (path: string) => /\/(attempts|events)\?/.test(path) ? respond(path) : detailResponse(data)));
    render(<App />); await ready(); expect(record().getByText('VERIFIED_AT_EXECUTION')).toBeVisible();
    await user.click(record().getByText('Recorded result metadata')); expect(record().getByText(data.result!.nativeFingerprint)).toBeVisible();
    expect(record().getByText('12:39:59.987654 UTC').closest('time')).toHaveAttribute('datetime', data.result!.verifiedAt);
    expect(record().queryByRole('link')).not.toBeInTheDocument(); expect(screen.getByText(/does not requalify an artifact or activate Analytics/)).toBeVisible();
    if (product === 'application-reliability') expect(record().getByText('synthetic')).toBeVisible();
  });

  it.each(['PAUSED', 'RECOVERY_REQUIRED'] as const)('displays %s as an operator boundary without automatic recovery', async (state) => {
    vi.stubGlobal('fetch', vi.fn().mockImplementation(async (path: string) => /\/(attempts|events)\?/.test(path) ? respond(path) : detailResponse(runDetail(1, state))));
    render(<App />); await ready(); expect(record().getByText(/Operator attention is required/)).toBeVisible();
    expect(record().getByText(/Refresh manually; no automatic recovery/)).toBeVisible();
    expect(screen.getByRole('button', { name: 'Refresh run detail' })).toBeEnabled();
  });

  it.each([[404, 'NOT_FOUND'], [403, 'LOCAL_BOUNDARY_REJECTED'], [503, 'MANAGEMENT_UNAVAILABLE'], [200, 'MALFORMED']] as const)(
    'shows safe run HTTP %s errors while histories remain independently available', async (status, code) => {
      const user = userEvent.setup(); let fail = true;
      const fetch = vi.fn().mockImplementation(async (path: string) => /\/(attempts|events)\?/.test(path) ? respond(path)
        : fail ? jsonResponse(managementFailure(code), status) : detailResponse(runDetail(1, 'FAILED')));
      vi.stubGlobal('fetch', fetch); render(<App />); const error = await record().findByRole('alert');
      expect(error).not.toHaveTextContent(/private|traceback/); if (status !== 200) expect(error).toHaveTextContent(CORRELATION);
      expect(await panel('Attempts').findByText('RECONCILE')).toBeVisible();
      expect(await panel('Audit events').findByRole('heading', { name: /Event 9007199254740993/ })).toBeVisible();
      expect(record().queryByText('Run row version')).not.toBeInTheDocument();
      fail = false; await user.click(record().getByRole('button', { name: 'Try again' })); await ready();
      expect(fetch.mock.calls.filter((call) => String(call[0]).includes('/attempts?'))).toHaveLength(1);
    });

  it.each(['attempts', 'events'] as const)('isolates %s failure/retry and successful empty pages from run detail', async (kind) => {
    const user = userEvent.setup(); let fail = true;
    const fetch = vi.fn().mockImplementation(async (path: string) => path.includes('/' + kind + '?')
      ? fail ? jsonResponse(managementFailure(), 503) : jsonResponse(historyPage([])) : respond(path));
    vi.stubGlobal('fetch', fetch); render(<App />); await ready(); const scope = panel(kind === 'attempts' ? 'Attempts' : 'Audit events');
    expect(await scope.findByRole('alert')).toHaveTextContent('MANAGEMENT_UNAVAILABLE'); expect(record().getByText('Run row version')).toBeVisible();
    fail = false; await user.click(scope.getByRole('button', { name: 'Try again' }));
    expect(await scope.findByText(/No .* returned on this page/)).toBeVisible(); expect(record().getByText('Run row version')).toBeVisible();
    expect(scope.getByRole('button', { name: /^Next/ })).toBeDisabled();
  });

  it('marks a failed same-run refresh stale with the last successful read timestamp', async () => {
    const user = userEvent.setup(); let reads = 0;
    vi.stubGlobal('fetch', vi.fn().mockImplementation(async (path: string) => /\/(attempts|events)\?/.test(path) ? respond(path)
      : ++reads === 1 ? detailResponse(runDetail(1, 'FAILED')) : jsonResponse(managementFailure(), 503)));
    render(<App />); await ready(); const timestamp = record().getByText(/Last successful read/).querySelector('time')?.getAttribute('datetime');
    await user.click(screen.getByRole('button', { name: 'Refresh run detail' }));
    expect(await record().findByRole('alert')).toHaveTextContent('Unable to refresh run detail');
    expect(record().getByText(/Stale run record/)).toBeVisible();
    expect(record().getByText(/Last successful read/).querySelector('time')).toHaveAttribute('datetime', timestamp);
    expect(record().getByText('Run row version')).toBeVisible();
  });

  it.each(CONTROL_STATES)('inspects control %s with separate run version metadata', async (state) => {
    const data = runDetail(1, 'FAILED'); data.latestControl = latestControl(state);
    vi.stubGlobal('fetch', vi.fn().mockImplementation(async (path: string) => path.includes('/controls/') ? controlResponse(controlReceipt(state))
      : /\/(attempts|events)\?/.test(path) ? respond(path) : detailResponse(data)));
    render(<App />); await ready(); const receipt = panel('Latest control receipt');
    expect(await receipt.findByText(state, { selector: 'dd' })).toBeVisible(); expect(receipt.getByText('9007199254740994')).toBeVisible();
    expect(receipt.getByText(/REQUESTED records intent, not completed recovery/)).toBeVisible();
    expect(receipt.queryByRole('button', { name: /reconcile|resume|retry|cancel/i })).not.toBeInTheDocument();
  });

  it('isolates a missing control and reads it again explicitly without erasing the run', async () => {
    const user = userEvent.setup(), data = runDetail(1, 'FAILED'); data.latestControl = latestControl(); let missing = true;
    const fetch = vi.fn().mockImplementation(async (path: string) => path.includes('/controls/')
      ? missing ? jsonResponse(managementFailure('NOT_FOUND'), 404) : controlResponse() : /\/(attempts|events)\?/.test(path) ? respond(path) : detailResponse(data));
    vi.stubGlobal('fetch', fetch); render(<App />); await ready(); expect(await panel('Latest control receipt').findByRole('alert')).toHaveTextContent('NOT_FOUND');
    expect(record().getByText('Run row version')).toBeVisible(); missing = false;
    await user.click(panel('Latest control receipt').getByRole('button', { name: 'Try again' }));
    expect(await panel('Latest control receipt').findByText('REQUESTED', { selector: 'dd' })).toBeVisible();
    expect(fetch.mock.calls.filter((call) => !/\/(attempts|events|controls)\//.test(String(call[0])) && !String(call[0]).includes('?'))).toHaveLength(1);
  });
});

describe('independent high-water history pages', () => {
  it('pages attempts/events independently, resets on page size/refresh and preserves parent metadata', async () => {
    const user = userEvent.setup();
    const fetch = vi.fn().mockImplementation(async (path: string) => {
      const query = urlOf(path).searchParams, limit = Number(query.get('limit')), cursor = query.get('cursor');
      if (path.includes('/attempts?')) {
        const all = Array.from({ length: 6 }, (_, i) => attempt(i + 1, i === 5 ? 'RECONCILE' : 'EXECUTE'));
        return jsonResponse(historyPage(cursor ? all.slice(5) : all.slice(0, limit), limit, !cursor && limit === 5 ? FIRST_CURSOR : null));
      }
      if (path.includes('/events?')) return eventsResponse(Array.from({ length: cursor ? 1 : 20 }, (_, i) => auditEvent((9007199254740993n + BigInt(i + (cursor ? 20 : 0))).toString())), limit, cursor ? null : FIRST_CURSOR);
      return respond(path);
    });
    vi.stubGlobal('fetch', fetch); render(<App />); await ready(); await panel('Attempts').findByText('RECONCILE');
    await user.selectOptions(panel('Attempts').getByRole('combobox', { name: 'Attempts per page' }), '5');
    await waitFor(() => expect(panel('Attempts').getByRole('button', { name: 'Next attempts' })).toBeEnabled());
    await user.click(panel('Attempts').getByRole('button', { name: 'Next attempts' }));
    expect(await panel('Attempts').findByRole('rowheader', { name: /Attempt 6/ })).toBeVisible();
    await user.click(panel('Audit events').getByRole('button', { name: 'Next audit events' }));
    expect(await panel('Audit events').findByRole('heading', { name: /Event 9007199254741013/ })).toBeVisible();
    expect(panel('Attempts').getByText(/Page 2/)).toBeVisible();
    await user.click(panel('Audit events').getByRole('button', { name: 'Previous audit events' }));
    await panel('Audit events').findByRole('heading', { name: /Event 9007199254740993 / });
    await user.click(panel('Attempts').getByRole('button', { name: 'Refresh attempts' }));
    await panel('Attempts').findByRole('rowheader', { name: /Attempt 1/ }); expect(panel('Attempts').getByText(/Page 1/)).toBeVisible();
    expect(urlOf(fetch.mock.calls.at(-1)![0]).searchParams.has('cursor')).toBe(false);
    await user.selectOptions(panel('Attempts').getByRole('combobox', { name: 'Attempts per page' }), '100');
    await panel('Attempts').findByText('RECONCILE'); expect(fetch.mock.calls.at(-1)?.[0]).toContain('limit=100');
    expect(fetch.mock.calls.filter((call) => String(call[0]) === '/api/management/v1/runs/' + id)).toHaveLength(1);
  });

  it.each(['attempts', 'events'] as const)('offers explicit restart for expired %s cursors without erasing other panels', async (kind) => {
    const user = userEvent.setup(), fetch = vi.fn().mockImplementation(async (path: string) => {
      if (path.includes('/' + kind + '?')) {
        const query = urlOf(path).searchParams;
        if (query.has('cursor')) return jsonResponse(managementFailure('INVALID_ARGUMENT'), 400);
        const rows = kind === 'attempts' ? Array.from({ length: 20 }, (_, i) => attempt(i + 1)) : Array.from({ length: 20 }, (_, i) => auditEvent(String(i + 1)));
        // Attempts have at most 20 records; use a 5-row page to exercise a valid continuation.
        const limit = Number(query.get('limit')); return jsonResponse(historyPage<unknown>(rows.slice(0, limit), limit, kind === 'events' || limit === 5 ? FIRST_CURSOR : null));
      }
      return respond(path);
    });
    vi.stubGlobal('fetch', fetch); render(<App />); await ready(); const scope = panel(kind === 'attempts' ? 'Attempts' : 'Audit events');
    if (kind === 'attempts') await user.selectOptions(scope.getByRole('combobox', { name: 'Attempts per page' }), '5');
    await waitFor(() => expect(scope.getByRole('button', { name: /^Next/ })).toBeEnabled()); await user.click(scope.getByRole('button', { name: /^Next/ }));
    expect(await scope.findByRole('alert')).toHaveTextContent('expire after 24 hours'); expect(record().getByText('Run row version')).toBeVisible();
    await user.click(scope.getByRole('button', { name: 'Start from first page' }));
    await waitFor(() => expect(scope.queryByRole('alert')).not.toBeInTheDocument()); expect(scope.getByText(/Page 1/)).toBeVisible();
  });
});

describe('inspection navigation and request races', () => {
  it('preserves execution filters and the current cursor page through link, Back/Forward and return navigation', async () => {
    window.history.replaceState(null, '', '/#/operations/executions?productId=content-intelligence&sourceId=alpha&state=FAILED&limit=20');
    const user = userEvent.setup(), fetch = vi.fn().mockImplementation(async (path: string) => path.startsWith('/api/management/v1/runs?')
      ? jsonResponse(urlOf(path).searchParams.has('cursor') ? runPage([run(1, 'FAILED')]) : runPage(Array.from({ length: 20 }, (_, i) => run(i + 2, 'FAILED')), 20, FIRST_CURSOR)) : respond(path));
    vi.stubGlobal('fetch', fetch); render(<App />); await screen.findByRole('heading', { name: 'Execution history' });
    await user.click(screen.getByRole('button', { name: 'Next executions' })); await user.click(await screen.findByRole('link', { name: id })); await ready();
    expect(screen.getByRole('main')).toHaveFocus(); expect(screen.getByRole('link', { name: 'Back to Executions' })).toHaveAttribute('href', '#/operations/executions?productId=content-intelligence&sourceId=alpha&state=FAILED');
    const listCalls = fetch.mock.calls.filter((call) => String(call[0]).startsWith('/api/management/v1/runs?')).length;
    act(() => window.history.back()); await screen.findByRole('heading', { name: 'Execution history' });
    expect(screen.getByText(/Execution page 2/)).toBeVisible(); expect(screen.getByRole('textbox', { name: 'Exact source ID' })).toHaveValue('alpha');
    act(() => window.history.forward()); await ready(); await user.click(screen.getByRole('link', { name: 'Back to Executions' }));
    await screen.findByRole('heading', { name: 'Execution history' }); expect(screen.getByText(/Execution page 2/)).toBeVisible();
    expect(fetch.mock.calls.filter((call) => String(call[0]).startsWith('/api/management/v1/runs?'))).toHaveLength(listCalls + 2);
    expect(urlOf(fetch.mock.calls.at(-1)![0]).searchParams.get('cursor')).toBe(FIRST_CURSOR);
  });

  it('preserves Analytics selection and Sources/revision navigation', async () => {
    window.history.replaceState(null, '', '/#/analytics'); const user = userEvent.setup(); vi.stubGlobal('fetch', vi.fn().mockImplementation(async (path: string) => respond(path)));
    render(<App />); await screen.findByRole('heading', { name: 'Publisher breakdown' });
    await user.selectOptions(screen.getByRole('combobox', { name: 'Publisher scope' }), 'beta');
    await waitFor(() => expect(screen.queryByRole('rowheader', { name: 'alpha' })).not.toBeInTheDocument());
    await user.click(workspace('Operations')); await user.click(operation('Executions')); await user.click(await screen.findByRole('link', { name: id })); await ready();
    await user.click(operation('Sources')); await user.click(await screen.findByRole('link', { name: /^Source alpha/ }));
    await screen.findByRole('heading', { name: 'Source alpha' }); expect(await screen.findByText('Current in displayed source record')).toBeVisible();
    await user.click(workspace('Analytics')); await screen.findByRole('heading', { name: 'Publisher breakdown' });
    expect(screen.getByRole('combobox', { name: 'Publisher scope' })).toHaveValue('beta'); expect(screen.getByLabelText('Publication fingerprint')).toHaveTextContent(FINGERPRINT);
  });

  it.each(['not-a-uuid', '40000000-0000-4000-8000-00000000000F', id + '/attempts', id + '?cursor=invented', id + '/commands'])('rejects invalid/unknown inspection route %s', (suffix) => {
    window.history.replaceState(null, '', '/#/operations/executions/' + suffix); const fetch = vi.fn(); vi.stubGlobal('fetch', fetch); render(<App />);
    expect(screen.getByRole('heading', { name: 'Page not found' })).toBeVisible(); expect(fetch).not.toHaveBeenCalled();
  });

  it('roundtrips only the current run and supported return filters', () => {
    const query = { productId: 'application-reliability' as const, sourceId: 'exact-source', state: 'RUNNING' as const, limit: 100 };
    expect(parseWorkspaceRoute(runHref(id, query))).toEqual({ kind: 'run-detail', runId: id, query });
  });

  it('aborts all panels on rapid run switches and ignores late successes/failures', async () => {
    const detail = deferred<Response>(), attempts = deferred<Response>(), events = deferred<Response>();
    const fetch = vi.fn().mockImplementation(async (path: string) => path.includes('/' + id)
      ? path.includes('/attempts?') ? attempts.promise : path.includes('/events?') ? events.promise : detail.promise : respond(path));
    vi.stubGlobal('fetch', fetch); render(<App />);
    act(() => { window.location.hash = runHref(run(2).runId); }); await waitFor(() => expect(fetch).toHaveBeenCalledTimes(6)); await ready();
    for (const call of fetch.mock.calls.slice(0, 3)) expect((call[1] as RequestInit).signal?.aborted).toBe(true);
    await act(async () => { detail.resolve(detailResponse()); attempts.reject(new Error('private')); events.resolve(eventsResponse([auditEvent('123')])); });
    expect(record().getByText(run(2).runId)).toBeVisible(); expect(panel('Audit events').queryByRole('heading', { name: /Event 123 / })).not.toBeInTheDocument();
    expect(screen.queryByRole('alert')).not.toBeInTheDocument(); expect(panel('Attempts').getByText(/Page 1/)).toBeVisible();
  });

  it('aborts pending control lookups when the parent exposes a different latest control', async () => {
    const user = userEvent.setup(), late = deferred<Response>(); let reads = 0;
    const data = runDetail(1, 'FAILED'); data.latestControl = latestControl(); const nextId = '70000000-0000-4000-8000-000000000002';
    const fetch = vi.fn().mockImplementation(async (path: string) => path.includes('/controls/')
      ? path.endsWith(CONTROL_ID) ? late.promise : controlResponse({ ...controlReceipt('APPLIED'), controlId: nextId })
      : /\/(attempts|events)\?/.test(path) ? respond(path) : detailResponse(++reads === 1 ? data : { ...data, latestControl: { ...latestControl(), controlId: nextId } }));
    vi.stubGlobal('fetch', fetch); render(<App />); await ready(); await waitFor(() => expect(fetch.mock.calls.some((call) => String(call[0]).endsWith(CONTROL_ID))).toBe(true));
    const signal = (fetch.mock.calls.find((call) => String(call[0]).endsWith(CONTROL_ID))![1] as RequestInit).signal;
    await user.click(screen.getByRole('button', { name: 'Refresh run detail' }));
    expect(await panel('Latest control receipt').findByText(nextId)).toBeVisible(); expect(signal?.aborted).toBe(true);
    await act(async () => late.resolve(controlResponse()));
    expect(panel('Latest control receipt').queryByText(CONTROL_ID)).not.toBeInTheDocument();
  });

  it('cancels every pending panel on navigation/unmount, including StrictMode replay', async () => {
    const late = deferred<Response>(), fetch = vi.fn().mockReturnValue(late.promise); vi.stubGlobal('fetch', fetch);
    const view = render(<StrictMode><App /></StrictMode>); expect(fetch).toHaveBeenCalledTimes(6);
    view.unmount(); for (const call of fetch.mock.calls) expect((call[1] as RequestInit).signal?.aborted).toBe(true);
    await act(async () => late.resolve(detailResponse())); expect(screen.queryByRole('region', { name: 'Run record' })).not.toBeInTheDocument();
  });
});
