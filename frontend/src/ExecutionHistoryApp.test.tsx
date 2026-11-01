import { StrictMode } from 'react';
import { act, render, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { App } from './App';
import { RUN_STATES } from './managementApi';
import { executionsHref, parseWorkspaceRoute } from './navigation';
import { deferred, fixture, FINGERPRINT, jsonResponse } from './test/fixtures';
import { CORRELATION, FIRST_CURSOR, SECOND_CURSOR, managementFailure, revisionPage, sourcePage, sourceResponse } from './test/managementFixtures';
import { fullRunPage, run, runPage } from './test/runFixtures';

const ready = () => screen.findByRole('heading', { name: 'Execution history' });
const workspace = (name: string) => within(screen.getByRole('navigation', { name: 'Workspaces' })).getByRole('link', { name });
const operation = (name: string) => within(screen.getByRole('navigation', { name: 'Operations views' })).getByRole('link', { name });
const urlOf = (path: string) => new URL(path, 'http://localhost');
const historyRow = (id = run().runId) => within(screen.getByRole('rowheader', { name: new RegExp(id) }).closest('tr')!);
function respond(path: string) {
  if (path.startsWith('/api/management/v1/runs?')) return jsonResponse(runPage());
  if (path.startsWith('/api/management/v1/sources?')) return jsonResponse(sourcePage());
  if (path.includes('/revisions?')) return jsonResponse(revisionPage());
  if (path.startsWith('/api/management/')) return sourceResponse();
  const body = fixture(), selected = urlOf(path).searchParams.get('sourceId');
  if (selected) { body.data = body.data.filter((item) => item.sourceId === selected); body.meta.returned = body.data.length; }
  return jsonResponse(body);
}
beforeEach(() => { window.history.replaceState(null, '', '/#/operations/executions'); });

describe('read-only execution history', () => {
  it('loads directly, requests one page only and presents all persisted states without actions', async () => {
    const late = deferred<Response>(), fetch = vi.fn().mockReturnValue(late.promise); vi.stubGlobal('fetch', fetch); render(<App />);
    expect(screen.getByRole('heading', { name: 'Loading executions' })).toBeVisible();
    expect(screen.queryByRole('table')).not.toBeInTheDocument(); expect(screen.getByRole('button', { name: 'Next executions' })).toBeDisabled();
    await act(async () => late.resolve(jsonResponse(runPage(RUN_STATES.map((state, index) => run(index + 1, state)))))); await ready();
    expect(fetch).toHaveBeenCalledTimes(1); expect(fetch.mock.calls[0]?.[0]).toBe('/api/management/v1/runs?limit=20');
    for (const [index, state] of RUN_STATES.entries()) expect(historyRow(run(index + 1).runId).getByText(state, { selector: '.run-state' })).toBeVisible();
    expect(workspace('Operations')).toHaveAttribute('aria-current', 'page'); expect(operation('Executions')).toHaveAttribute('aria-current', 'page');
    expect(historyRow().getByText('9007199254740993')).toBeVisible();
    expect(historyRow().getByText('Current stage: Not recorded')).toBeVisible();
    expect(historyRow().getByText('No result recorded')).toBeVisible();
    expect(historyRow().getByText('2026-11-01').closest('time')).toHaveAttribute('datetime', '2026-11-01T12:34:56.654321Z');
    expect(historyRow().getByText('12:34:56.654321 UTC')).toBeVisible();
    expect(historyRow().getByText('Not available')).toBeVisible();
    expect(screen.queryByRole('button', { name: /run now|submit run|cancel|retry run|resume|recover/i })).not.toBeInTheDocument();
    expect(screen.getByRole('region', { name: /Execution history table/ })).toHaveAttribute('tabindex', '0');
    expect(screen.queryByRole('link', { name: new RegExp(run().runId) })).not.toBeInTheDocument();
    expect(screen.queryByRole('alert')).not.toBeInTheDocument();
  });

  it('distinguishes historical result verification, synthetic origin and persisted outcomes from request errors', async () => {
    const user = userEvent.setup(), succeeded = run(1, 'SUCCEEDED', 'application-reliability'), failed = { ...run(2, 'FAILED'), retryEligibility: true };
    vi.stubGlobal('fetch', vi.fn().mockImplementation(async () => jsonResponse(runPage([succeeded, failed])))); render(<App />); await ready();
    const row = historyRow(succeeded.runId); expect(row.getByText('Synthetic data origin')).toBeVisible();
    expect(row.getByText('VERIFIED_AT_EXECUTION')).toBeVisible(); expect(row.getByText('Historical verification only')).toBeVisible();
    await user.click(row.getByText('Recorded result metadata'));
    expect(row.getByText(succeeded.result!.artifactId)).toBeVisible(); expect(row.getByText(succeeded.result!.nativeFingerprint)).toBeVisible();
    expect(row.getByText('12:39:59.987654 UTC').closest('time')).toHaveAttribute('datetime', succeeded.result!.verifiedAt);
    expect(row.getByText('12:40:00.123456 UTC').closest('time')).toHaveAttribute('datetime', succeeded.finishedAt);
    expect(row.queryByRole('link')).not.toBeInTheDocument();
    expect(historyRow(failed.runId).getByText('INPUT_INVALID')).toBeVisible();
    expect(historyRow(failed.runId).getByText('Retry eligibility (recorded): true')).toBeVisible();
    expect(historyRow(failed.runId).getByText('No result recorded')).toBeVisible(); expect(screen.queryByRole('alert')).not.toBeInTheDocument();
    await user.click(screen.getByText('How to interpret execution records'));
    expect(screen.getByText(/It does not establish current verification or activate an Analytics publication/)).toBeVisible();
    expect(screen.getByText(/QUEUED means waiting, not active processing/)).toBeVisible();
    expect(screen.getByText(/Retry eligibility is recorded metadata, not permission to retry/)).toBeVisible();
    expect(screen.getByText(/Ordinary Content worker dispatch remains disabled/)).toBeVisible();
  });

  it('separates empty pages from unavailable history and filtered-empty results', async () => {
    const user = userEvent.setup(); vi.stubGlobal('fetch', vi.fn().mockImplementation(async () => jsonResponse(runPage([])))); render(<App />);
    expect(await screen.findByRole('heading', { name: 'No executions returned' })).toBeVisible();
    expect(screen.queryByRole('alert')).not.toBeInTheDocument();
    await user.selectOptions(screen.getByRole('combobox', { name: 'Run state' }), 'FAILED');
    await user.click(screen.getByRole('button', { name: 'Apply filters' }));
    expect(await screen.findByRole('heading', { name: 'No executions match these filters' })).toBeVisible();
    expect(screen.getByRole('button', { name: 'Next executions' })).toBeDisabled();
    await user.click(screen.getByRole('button', { name: 'Reset filters' })); await screen.findByRole('heading', { name: 'No executions returned' });
    expect(screen.getByRole('combobox', { name: 'Run state' })).toHaveValue('');
  });

  it.each([[403, 'LOCAL_BOUNDARY_REJECTED', /Local access was rejected/], [503, 'MANAGEMENT_UNAVAILABLE', /Execution history is unavailable/],
    [404, 'NOT_FOUND', /running in API mode/], [500, 'INTERNAL_FAILURE', /could not complete/], [200, 'BAD_DATA', /does not match the execution history contract/]] as const)(
    'shows safe HTTP %s errors with user-triggered retry', async (status, code, expected) => {
      const user = userEvent.setup(), fetch = vi.fn().mockResolvedValueOnce(jsonResponse(managementFailure(code), status)).mockResolvedValueOnce(jsonResponse(runPage()));
      vi.stubGlobal('fetch', fetch); render(<App />); const error = await screen.findByRole('alert'); expect(error).toHaveTextContent(expected);
      expect(error).not.toHaveTextContent(/private|traceback|Gold/); expect(screen.queryByRole('heading', { name: 'No executions returned' })).not.toBeInTheDocument();
      if (status !== 200) { expect(error).toHaveTextContent(CORRELATION); expect(error).toHaveTextContent(code); expect(error).toHaveTextContent('Retry guidance'); }
      expect(fetch).toHaveBeenCalledTimes(1); await user.click(screen.getByRole('button', { name: 'Try again' })); await ready();
      expect(fetch).toHaveBeenCalledTimes(2);
    });

  it('handles network failure and discards old results after a failed explicit refresh', async () => {
    const user = userEvent.setup(), fetch = vi.fn().mockResolvedValueOnce(jsonResponse(runPage())).mockRejectedValueOnce(new Error('private/secret'));
    vi.stubGlobal('fetch', fetch); render(<App />); await ready(); await user.click(screen.getByRole('button', { name: 'Refresh executions' }));
    expect(await screen.findByRole('alert')).toHaveTextContent('Cannot reach the local management service');
    expect(screen.queryByRole('table')).not.toBeInTheDocument(); expect(screen.queryByRole('rowheader', { name: new RegExp(run().runId) })).not.toBeInTheDocument();
  });
});

describe('execution filtering and bounded traversal', () => {
  it('applies exact server product/source/state filters, resets paging and provides explicit reset', async () => {
    const user = userEvent.setup(), fetch = vi.fn().mockImplementation(async (path: string) => {
      const params = urlOf(path).searchParams;
      if (params.get('productId') === 'application-reliability') return jsonResponse(runPage([run(50, 'FAILED', 'application-reliability')], Number(params.get('limit'))));
      return jsonResponse(params.has('cursor') ? runPage([run(1)]) : fullRunPage());
    });
    vi.stubGlobal('fetch', fetch); render(<App />); await ready();
    await user.click(screen.getByRole('button', { name: 'Next executions' })); await waitFor(() => expect(historyRow().getByText('QUEUED')).toBeVisible());
    expect(screen.getByText(/Execution page 2/)).toBeVisible();
    await user.selectOptions(screen.getByRole('combobox', { name: 'Product' }), 'application-reliability');
    await user.type(screen.getByRole('textbox', { name: 'Exact source ID' }), 'exact_source-1');
    await user.selectOptions(screen.getByRole('combobox', { name: 'Run state' }), 'FAILED');
    await user.selectOptions(screen.getByRole('combobox', { name: 'Executions per page' }), '100');
    expect(fetch).toHaveBeenCalledTimes(2); // Draft filters do not cause per-keystroke HTTP requests.
    await user.click(screen.getByRole('button', { name: 'Apply filters' })); await ready();
    expect(Object.fromEntries(urlOf(fetch.mock.calls.at(-1)![0]).searchParams)).toEqual({ limit: '100', productId: 'application-reliability', sourceId: 'exact_source-1', state: 'FAILED' });
    expect(screen.getByText(/Execution page 1/)).toBeVisible(); expect(historyRow(run(50).runId).getByText('Synthetic data origin')).toBeVisible();
    expect(window.location.hash).toContain('sourceId=exact_source-1');
    await user.click(screen.getByRole('button', { name: 'Reset filters' })); await ready();
    expect(fetch.mock.calls.at(-1)?.[0]).toBe('/api/management/v1/runs?limit=20'); expect(window.location.hash).toBe('#/operations/executions');
    expect(screen.getByRole('textbox', { name: 'Exact source ID' })).toHaveValue('');
    expect(screen.getByRole('combobox', { name: 'Product' })).toHaveValue('');
  });

  it.each([' Alpha ', 'a'.repeat(129), '../private', 'UPPER'])('rejects invalid source draft without normalizing %s', async (sourceId) => {
    const user = userEvent.setup(), fetch = vi.fn().mockImplementation(async () => jsonResponse(runPage()));
    vi.stubGlobal('fetch', fetch); render(<App />); await ready();
    await user.type(screen.getByRole('textbox', { name: 'Exact source ID' }), sourceId); await user.click(screen.getByRole('button', { name: 'Apply filters' }));
    expect(screen.getByRole('textbox', { name: 'Exact source ID' })).toHaveValue(sourceId);
    expect(screen.getByRole('textbox', { name: 'Exact source ID' })).toHaveAttribute('aria-invalid', 'true');
    expect(screen.getByRole('alert')).toHaveTextContent('Whitespace is not removed'); expect(fetch).toHaveBeenCalledTimes(1);
    expect(historyRow().getByText('QUEUED')).toBeVisible();
  });

  it('presents exactly the supported state filter options', async () => {
    vi.stubGlobal('fetch', vi.fn().mockImplementation(async () => jsonResponse(runPage()))); render(<App />); await ready();
    const options = within(screen.getByRole('combobox', { name: 'Run state' })).getAllByRole('option').map((option) => option.getAttribute('value'));
    expect(options).toEqual(['', ...RUN_STATES]);
  });

  it('follows opaque Next/Previous cursors and starts a new traversal on Refresh or same-filter Apply', async () => {
    const user = userEvent.setup(), fetch = vi.fn().mockImplementation(async (path: string) => jsonResponse(urlOf(path).searchParams.has('cursor') ? runPage([run(1)]) : fullRunPage()));
    vi.stubGlobal('fetch', fetch); render(<App />); await ready();
    await user.click(screen.getByRole('button', { name: 'Next executions' })); await ready();
    expect(fetch.mock.calls.at(-1)?.[0]).toBe(`/api/management/v1/runs?limit=20&cursor=${FIRST_CURSOR}`);
    expect(screen.queryByRole('rowheader', { name: new RegExp(run(20).runId) })).not.toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'Previous executions' })); await ready();
    expect(fetch.mock.calls.at(-1)?.[0]).toBe('/api/management/v1/runs?limit=20');
    await user.click(screen.getByRole('button', { name: 'Next executions' })); await ready();
    await user.click(screen.getByRole('button', { name: 'Refresh executions' })); await ready();
    expect(screen.getByText(/Execution page 1/)).toBeVisible(); expect(fetch.mock.calls.at(-1)?.[0]).not.toContain('cursor');
    await user.click(screen.getByRole('button', { name: 'Next executions' })); await ready();
    await user.click(screen.getByRole('button', { name: 'Apply filters' })); await ready();
    expect(screen.getByText(/Execution page 1/)).toBeVisible(); expect(fetch.mock.calls.at(-1)?.[0]).not.toContain('cursor');
  });

  it('offers explicit invalid/expired cursor restart without silently restarting', async () => {
    const user = userEvent.setup(), fetch = vi.fn().mockImplementation(async (path: string) => urlOf(path).searchParams.has('cursor')
      ? jsonResponse(managementFailure('INVALID_ARGUMENT'), 400) : jsonResponse(fullRunPage()));
    vi.stubGlobal('fetch', fetch); render(<App />); await ready(); await user.click(screen.getByRole('button', { name: 'Next executions' }));
    expect(await screen.findByRole('alert')).toHaveTextContent('Cursors expire after 24 hours'); expect(fetch).toHaveBeenCalledTimes(2);
    await user.click(screen.getByRole('button', { name: 'Start from first execution page' })); await ready();
    expect(fetch.mock.calls.at(-1)?.[0]).not.toContain('cursor'); expect(screen.getByText(/Execution page 1/)).toBeVisible();
  });

  it('rejects cursor cycles across pages', async () => {
    const user = userEvent.setup(), fetch = vi.fn().mockResolvedValueOnce(jsonResponse(fullRunPage(80, FIRST_CURSOR)))
      .mockResolvedValueOnce(jsonResponse(fullRunPage(60, SECOND_CURSOR))).mockResolvedValueOnce(jsonResponse(fullRunPage(40, FIRST_CURSOR)));
    vi.stubGlobal('fetch', fetch); render(<App />); await ready(); await user.click(screen.getByRole('button', { name: 'Next executions' })); await ready();
    await user.click(screen.getByRole('button', { name: 'Next executions' }));
    expect(await screen.findByRole('alert')).toHaveTextContent('does not match the execution history contract');
  });
});

describe('Operations navigation and filter URLs', () => {
  it('preserves Analytics filters/publication and Sources details/revisions through execution navigation and history', async () => {
    window.history.replaceState(null, '', '/#/analytics'); const user = userEvent.setup(), fetch = vi.fn().mockImplementation(async (path: string) => respond(path));
    vi.stubGlobal('fetch', fetch); render(<App />); await screen.findByRole('heading', { name: 'Publisher breakdown' });
    await user.selectOptions(screen.getByRole('combobox', { name: 'Publisher scope' }), 'beta');
    await waitFor(() => expect(screen.queryByRole('rowheader', { name: 'alpha' })).not.toBeInTheDocument());
    await user.click(workspace('Operations')); await screen.findByRole('heading', { name: 'Registered sources' });
    await user.click(operation('Executions')); await ready(); expect(screen.getByRole('main')).toHaveFocus();
    act(() => window.history.back()); await screen.findByRole('heading', { name: 'Registered sources' });
    act(() => window.history.forward()); await ready();
    await user.click(operation('Sources')); await user.click(await screen.findByRole('link', { name: /^Source alpha/ }));
    await screen.findByRole('heading', { name: 'Source alpha' });
    expect(await screen.findByText('Current in displayed source record')).toBeVisible();
    await user.click(workspace('Analytics')); await screen.findByRole('heading', { name: 'Publisher breakdown' });
    expect(screen.getByRole('combobox', { name: 'Publisher scope' })).toHaveValue('beta');
    expect(screen.getByLabelText('Publication fingerprint')).toHaveTextContent(FINGERPRINT);
    expect(screen.queryByRole('button', { name: 'Reload dashboard' })).not.toBeInTheDocument();
  });

  it('opens source-related history with composite filters and a fresh traversal', async () => {
    const user = userEvent.setup(), fetch = vi.fn().mockImplementation(async (path: string) => {
      if (path.startsWith('/api/management/v1/runs?')) return jsonResponse(urlOf(path).searchParams.has('cursor') ? runPage([run(1)]) : fullRunPage());
      return respond(path);
    });
    vi.stubGlobal('fetch', fetch); render(<App />); await ready();
    await user.click(screen.getByRole('button', { name: 'Next executions' })); await ready();
    await user.click(operation('Sources')); await user.click(await screen.findByRole('link', { name: /^Source alpha/ }));
    await screen.findByRole('heading', { name: 'Source alpha' });
    await user.click(screen.getByRole('link', { name: 'View executions for this source' })); await ready();
    expect(screen.getByRole('combobox', { name: 'Product' })).toHaveValue('content-intelligence');
    expect(screen.getByRole('textbox', { name: 'Exact source ID' })).toHaveValue('alpha');
    expect(screen.getByText(/Execution page 1/)).toBeVisible();
    expect(Object.fromEntries(urlOf(fetch.mock.calls.at(-1)![0]).searchParams)).toEqual({ limit: '20', productId: 'content-intelligence', sourceId: 'alpha' });
  });

  it('keeps applied filters consistent with direct entry and Back/Forward', async () => {
    window.history.replaceState(null, '', '/#/operations/executions?productId=application-reliability&sourceId=exact-id&state=FAILED&limit=7');
    const user = userEvent.setup(), fetch = vi.fn().mockImplementation(async (path: string) => jsonResponse(runPage([], Number(urlOf(path).searchParams.get('limit')))));
    vi.stubGlobal('fetch', fetch); render(<App />); await screen.findByRole('heading', { name: 'No executions match these filters' });
    expect(screen.getByRole('combobox', { name: 'Executions per page' })).toHaveValue('7'); expect(screen.getByRole('textbox', { name: 'Exact source ID' })).toHaveValue('exact-id');
    await user.selectOptions(screen.getByRole('combobox', { name: 'Run state' }), 'RUNNING'); await user.click(screen.getByRole('button', { name: 'Apply filters' }));
    await waitFor(() => expect(fetch.mock.calls.at(-1)?.[0]).toContain('state=RUNNING'));
    act(() => window.history.back()); await waitFor(() => expect(screen.getByRole('combobox', { name: 'Run state' })).toHaveValue('FAILED'));
    expect(screen.getByRole('textbox', { name: 'Exact source ID' })).toHaveValue('exact-id');
    act(() => window.history.forward()); await waitFor(() => expect(screen.getByRole('combobox', { name: 'Run state' })).toHaveValue('RUNNING'));
  });

  it.each(['?productId=unknown', '?sourceId=UPPER', '?sourceId=%20alpha%20', '?state=waiting', '?limit=101', '?limit=01',
    '?sourceId=a&sourceId=b', '?cursor=invented', '?pipeline=RELIABILITY_V2', '/40000000-0000-4000-8000-000000000001'])('rejects unsupported or malformed route %s safely', (suffix) => {
    window.history.replaceState(null, '', '/#/operations/executions' + suffix); const fetch = vi.fn(); vi.stubGlobal('fetch', fetch); render(<App />);
    expect(screen.getByRole('heading', { name: 'Page not found' })).toBeVisible(); expect(fetch).not.toHaveBeenCalled();
  });

  it('roundtrips only supported filters and exposes keyboard-accessible Operations navigation', async () => {
    const query = { productId: 'application-reliability' as const, sourceId: 'a'.repeat(128), state: 'PAUSED' as const, limit: 100 };
    expect(parseWorkspaceRoute(executionsHref(query))).toEqual({ kind: 'executions', query });
    const user = userEvent.setup(); vi.stubGlobal('fetch', vi.fn().mockImplementation(async () => jsonResponse(runPage()))); render(<App />); await ready();
    await user.tab(); expect(screen.getByRole('link', { name: 'Skip to operations' })).toHaveFocus();
    await user.keyboard('{Enter}'); expect(screen.getByRole('main')).toHaveFocus(); expect(window.location.hash).toBe('#/operations/executions');
    operation('Sources').focus(); await user.tab(); expect(operation('Executions')).toHaveFocus();
    await user.keyboard('{Enter}'); expect(window.location.hash).toBe('#/operations/executions');
  });
});

describe('execution request isolation', () => {
  it('aborts and ignores late filter responses, including rapid repeated changes and failures', async () => {
    const user = userEvent.setup(), first = deferred<Response>(), second = deferred<Response>(), third = deferred<Response>();
    const fetch = vi.fn().mockReturnValueOnce(first.promise).mockReturnValueOnce(second.promise).mockReturnValueOnce(third.promise);
    vi.stubGlobal('fetch', fetch); render(<App />);
    await user.selectOptions(screen.getByRole('combobox', { name: 'Product' }), 'application-reliability'); await user.click(screen.getByRole('button', { name: 'Apply filters' }));
    await waitFor(() => expect(fetch).toHaveBeenCalledTimes(2));
    await user.selectOptions(screen.getByRole('combobox', { name: 'Run state' }), 'FAILED'); await user.click(screen.getByRole('button', { name: 'Apply filters' }));
    await waitFor(() => expect(fetch).toHaveBeenCalledTimes(3));
    expect(screen.queryByRole('table')).not.toBeInTheDocument();
    await act(async () => third.resolve(jsonResponse(runPage([run(3, 'FAILED', 'application-reliability')])))); await ready();
    await act(async () => { first.resolve(jsonResponse(runPage([run(99)]))); second.reject(new Error('private')); });
    expect(historyRow(run(3).runId).getByText('FAILED', { selector: '.run-state' })).toBeVisible();
    expect(screen.queryByRole('rowheader', { name: new RegExp(run(99).runId) })).not.toBeInTheDocument();
    expect(screen.queryByRole('alert')).not.toBeInTheDocument();
    for (const call of fetch.mock.calls.slice(0, 2)) expect((call[1] as RequestInit).signal?.aborted).toBe(true);
  });

  it('ignores pending page data after refresh and never mixes incompatible pages', async () => {
    const user = userEvent.setup(), pending = deferred<Response>(), fetch = vi.fn().mockResolvedValueOnce(jsonResponse(fullRunPage())).mockReturnValueOnce(pending.promise)
      .mockResolvedValueOnce(jsonResponse(runPage([run(100)])));
    vi.stubGlobal('fetch', fetch); render(<App />); await ready(); await user.click(screen.getByRole('button', { name: 'Next executions' }));
    expect(screen.queryByRole('table')).not.toBeInTheDocument(); await user.click(screen.getByRole('button', { name: 'Refresh executions' })); await ready();
    await act(async () => pending.resolve(jsonResponse(runPage([run(1)]))));
    expect(historyRow(run(100).runId).getByText('QUEUED')).toBeVisible();
    expect(screen.queryByRole('rowheader', { name: new RegExp(run(1).runId) })).not.toBeInTheDocument();
    expect((fetch.mock.calls[1]?.[1] as RequestInit).signal?.aborted).toBe(true);
  });

  it('ignores a late response after navigation and reenters with a fresh first page', async () => {
    const user = userEvent.setup(), late = deferred<Response>(), fetch = vi.fn().mockReturnValueOnce(late.promise).mockImplementation(async (path: string) => respond(path));
    vi.stubGlobal('fetch', fetch); render(<App />); await user.click(operation('Sources')); await screen.findByRole('heading', { name: 'Registered sources' });
    expect((fetch.mock.calls[0]?.[1] as RequestInit).signal?.aborted).toBe(true);
    await act(async () => late.resolve(jsonResponse(runPage([run(999)])))); expect(screen.queryByRole('heading', { name: 'Execution history' })).not.toBeInTheDocument();
    await user.click(operation('Executions')); await ready();
    expect(screen.queryByRole('rowheader', { name: new RegExp(run(999).runId) })).not.toBeInTheDocument();
    expect(fetch.mock.calls.at(-1)?.[0]).toBe('/api/management/v1/runs?limit=20');
  });

  it('does not let an aborted transport rejection appear as an operational error', async () => {
    const user = userEvent.setup(), late = deferred<Response>(), fetch = vi.fn().mockReturnValueOnce(late.promise).mockImplementation(async (path: string) => respond(path));
    vi.stubGlobal('fetch', fetch); render(<App />); await user.click(workspace('Analytics')); await screen.findByRole('heading', { name: 'Publisher breakdown' });
    await act(async () => late.reject(new DOMException('aborted', 'AbortError')));
    expect(screen.queryByRole('alert')).not.toBeInTheDocument(); expect(screen.getByLabelText('Publication fingerprint')).toHaveTextContent(FINGERPRINT);
  });

  it('remains safe under StrictMode and cancels on unmount without polling', async () => {
    const late = deferred<Response>(), fetch = vi.fn().mockReturnValue(late.promise); vi.stubGlobal('fetch', fetch);
    const view = render(<StrictMode><App /></StrictMode>); expect(fetch).toHaveBeenCalledTimes(2);
    expect((fetch.mock.calls[0]?.[1] as RequestInit).signal?.aborted).toBe(true);
    view.unmount(); expect((fetch.mock.calls[1]?.[1] as RequestInit).signal?.aborted).toBe(true);
    await act(async () => late.resolve(jsonResponse(runPage()))); expect(fetch).toHaveBeenCalledTimes(2);
  });
});
