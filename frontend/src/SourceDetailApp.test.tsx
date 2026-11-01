import { StrictMode } from 'react';
import { act, render, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { App } from './App';
import { parseWorkspaceRoute, sourceHref } from './navigation';
import { deferred, fixture, FINGERPRINT, jsonResponse } from './test/fixtures';
import { CORRELATION, FIRST_CURSOR, SECOND_CURSOR, fullRevisionPage, fullSourcePage, managementFailure,
  revision, revisionPage, source, sourcePage, sourceRecord, sourceResponse } from './test/managementFixtures';

const route = '#/operations/sources/content-intelligence/alpha';
const detail = () => within(screen.getByRole('region', { name: 'Current source record' }));
const history = () => within(screen.getByRole('region', { name: 'Revision history' }));
const ready = async () => within(await screen.findByRole('region', { name: 'Current source record' })).findByRole('heading', { name: 'Source alpha' });
const workspace = (name: string) => within(screen.getByRole('navigation', { name: 'Workspaces' })).getByRole('link', { name });
function respond(path: string): Response {
  if (!path.startsWith('/api/management/')) {
    const body = fixture(), selected = new URL(path, 'http://localhost').searchParams.get('sourceId');
    if (selected) { body.data = body.data.filter((row) => row.sourceId === selected); body.meta.returned = body.data.length; }
    return jsonResponse(body);
  }
  if (path.startsWith('/api/management/v1/sources?')) return jsonResponse(sourcePage());
  const parts = path.split('?')[0]!.split('/'), record = sourceRecord(parts[6]!, parts[5] === 'application-reliability' ? 'application-reliability' : 'content-intelligence');
  return path.includes('/revisions?') ? jsonResponse(revisionPage([revision(1, record), revision(2, record)])) : sourceResponse(record);
}
beforeEach(() => { window.history.replaceState(null, '', `/${route}`); });

// Navigation is exercised through real hash links and browser history, without a router or API.
describe('source detail navigation', () => {
  it('opens directly with no hidden Sources or Analytics requests and presents read-only semantics', async () => {
    const pendingDetail = deferred<Response>(), pendingHistory = deferred<Response>();
    const fetch = vi.fn().mockReturnValueOnce(pendingDetail.promise).mockReturnValueOnce(pendingHistory.promise);
    vi.stubGlobal('fetch', fetch); render(<App />);
    expect(detail().getByRole('heading', { name: 'Loading source detail' })).toBeVisible();
    expect(history().getByRole('heading', { name: 'Loading revision history' })).toBeVisible();
    await act(async () => { pendingDetail.resolve(sourceResponse()); pendingHistory.resolve(jsonResponse(revisionPage())); });
    await ready();
    expect(fetch.mock.calls.map((call) => call[0])).toEqual([
      '/api/management/v1/sources/content-intelligence/alpha', '/api/management/v1/sources/content-intelligence/alpha/revisions?limit=20',
    ]);
    expect(workspace('Operations')).toHaveAttribute('aria-current', 'page');
    expect(detail().getByText('9007199254740993')).toBeVisible();
    expect(detail().getByText('2026-11-01').closest('time')).toHaveAttribute('datetime', '2026-11-01T12:34:56.654321Z');
    expect(screen.getByText(/Enabled is an admission gate, not health or revision approval/)).toBeVisible();
    expect(screen.queryByRole('button', { name: /enable|approve|ingest|submit|recover|cancel|resume/i })).not.toBeInTheDocument();
    expect(screen.getByRole('link', { name: 'View executions for this source' })).toHaveAttribute('href', '#/operations/executions?productId=content-intelligence&sourceId=alpha');
    expect(screen.queryByRole('link', { name: /artifacts|run detail/i })).not.toBeInTheDocument();
    expect(history().getByText('Current in displayed source record')).toBeVisible();
    expect(history().getByText('Historical revision')).toBeVisible();
  });

  it('keeps list filters and cursor context through selection, Back/Forward and return link', async () => {
    window.history.replaceState(null, '', '/#/operations/sources'); const user = userEvent.setup();
    const fetch = vi.fn().mockImplementation(async (path: string) => {
      if (path.startsWith('/api/management/v1/sources?')) {
        const query = new URL(path, 'http://localhost').searchParams;
        const page = query.has('cursor') ? sourcePage([source('alpha')]) : fullSourcePage();
        return jsonResponse(page);
      }
      return respond(path);
    });
    vi.stubGlobal('fetch', fetch); render(<App />); await screen.findByRole('heading', { name: 'Registered sources' });
    await user.selectOptions(screen.getByRole('combobox', { name: 'Product' }), 'content-intelligence');
    await user.selectOptions(screen.getByRole('combobox', { name: 'Admission status' }), 'true');
    await user.click(screen.getByRole('button', { name: 'Next sources' }));
    await user.click(await screen.findByRole('link', { name: /^Source alpha/ })); await ready();
    expect(window.location.hash).toBe(route); expect(screen.getByRole('main')).toHaveFocus();
    act(() => window.history.back()); await screen.findByRole('heading', { name: 'Registered sources' });
    expect(screen.getByRole('combobox', { name: 'Product' })).toHaveValue('content-intelligence');
    expect(screen.getByRole('combobox', { name: 'Admission status' })).toHaveValue('true');
    expect(screen.getByText(/Page 2/)).toBeVisible();
    act(() => window.history.forward()); await ready();
    await user.click(screen.getByRole('link', { name: 'Back to Sources' }));
    await screen.findByRole('heading', { name: 'Registered sources' }); expect(screen.getByText(/Page 2/)).toBeVisible();
    expect(fetch.mock.calls.at(-1)?.[0]).toBe(`/api/management/v1/sources?limit=20&productId=content-intelligence&enabled=true&cursor=${FIRST_CURSOR}`);
  });

  it('preserves Analytics selection and publication pin through list and detail navigation', async () => {
    window.history.replaceState(null, '', '/#/analytics'); const user = userEvent.setup();
    vi.stubGlobal('fetch', vi.fn().mockImplementation(async (path: string) => respond(path))); render(<App />);
    await screen.findByRole('heading', { name: 'Publisher breakdown' });
    await user.selectOptions(screen.getByRole('combobox', { name: 'Publisher scope' }), 'beta');
    await waitFor(() => expect(screen.queryByRole('rowheader', { name: 'alpha' })).not.toBeInTheDocument());
    await user.click(workspace('Operations')); await user.click(await screen.findByRole('link', { name: /^Source alpha/ }));
    await ready(); await user.click(workspace('Analytics')); await screen.findByRole('heading', { name: 'Publisher breakdown' });
    expect(screen.getByRole('combobox', { name: 'Publisher scope' })).toHaveValue('beta');
    expect(screen.getByLabelText('Publication fingerprint')).toHaveTextContent(FINGERPRINT);
    expect(screen.queryByRole('button', { name: 'Reload dashboard' })).not.toBeInTheDocument();
  });

  it.each(['unknown/alpha', 'content-intelligence/Upper', 'content-intelligence/a%2Fb', 'content-intelligence/%ZZ',
    'content-intelligence/alpha/revisions/2', 'content-intelligence/', 'content-intelligence/' + 'a'.repeat(129)])('fails safely for invalid detail identity %s', (suffix) => {
    window.history.replaceState(null, '', '/#/operations/sources/' + suffix); const fetch = vi.fn(); vi.stubGlobal('fetch', fetch); render(<App />);
    expect(screen.getByRole('heading', { name: 'Page not found' })).toBeVisible(); expect(fetch).not.toHaveBeenCalled();
  });

  it('supports composite identity, maximum IDs and encoded safe characters', () => {
    const id = { productId: 'application-reliability' as const, sourceId: 'a'.repeat(128) };
    expect(parseWorkspaceRoute(sourceHref(id))).toEqual({ kind: 'source-detail', ...id });
    expect(parseWorkspaceRoute('#/operations/sources/content-intelligence/%61lpha')).toEqual({ kind: 'source-detail', productId: 'content-intelligence', sourceId: 'alpha' });
  });
});

describe('current source and independent immutable history', () => {
  it.each(['content-intelligence', 'application-reliability'] as const)('shows disabled %s configuration and immutable historical references', async (product) => {
    window.history.replaceState(null, '', '/' + sourceHref({ productId: product, sourceId: 'alpha' }));
    const user = userEvent.setup(), current = sourceRecord('alpha', product); current.enabled = false;
    const old = sourceRecord('alpha', product); old.config.inputStorageProfileId = '20000000-0000-4000-8000-000000000002'; old.configSha256 = 'c'.repeat(64);
    vi.stubGlobal('fetch', vi.fn().mockImplementation(async (path: string) => path.includes('/revisions?')
      ? jsonResponse(revisionPage([revision(1, old), revision(2, current)])) : sourceResponse(current)));
    render(<App />); await ready();
    expect(detail().getByText('Disabled')).toBeVisible(); expect(detail().getByText(current.config.profile)).toBeVisible();
    expect(detail().getByText('Not applicable')).toBeVisible();
    await user.click(history().getByText('Configuration references for revision 1'));
    expect(history().getByText(old.config.inputStorageProfileId)).toBeVisible();
    expect(detail().getByText(current.config.inputStorageProfileId)).toBeVisible();
    expect(history().getByText(old.configSha256)).toBeVisible();
  });

  it.each([[404, 'NOT_FOUND', /Source not found/], [403, 'LOCAL_BOUNDARY_REJECTED', /Local access was rejected/],
    [503, 'MANAGEMENT_UNAVAILABLE', /unavailable/], [200, 'MALFORMED', /does not match the source contract/]] as const)(
    'shows safe detail HTTP %s failure while history succeeds, then retries only detail', async (status, code, expected) => {
      const user = userEvent.setup(); let fail = true;
      const fetch = vi.fn().mockImplementation(async (path: string) => path.includes('/revisions?') ? jsonResponse(revisionPage([revision(1)]))
        : fail ? jsonResponse(managementFailure(code), status) : sourceResponse());
      vi.stubGlobal('fetch', fetch); render(<App />);
      const alert = await detail().findByRole('alert'); expect(alert).toHaveTextContent(expected);
      expect(alert).not.toHaveTextContent(/secret|traceback/);
      if (status !== 200) { expect(alert).toHaveTextContent(CORRELATION); expect(alert).toHaveTextContent('Retry guidance'); }
      expect(await history().findByText('Current revision not verified')).toBeVisible();
      fail = false; await user.click(detail().getByRole('button', { name: 'Try again' })); await ready();
      expect(fetch.mock.calls.filter((call) => String(call[0]).includes('/revisions?'))).toHaveLength(1);
    });

  it('keeps valid detail after a revision failure and retries history independently', async () => {
    const user = userEvent.setup(); let fail = true;
    const fetch = vi.fn().mockImplementation(async (path: string) => path.includes('/revisions?')
      ? fail ? jsonResponse(managementFailure(), 503) : jsonResponse(revisionPage([revision(2)])) : sourceResponse());
    vi.stubGlobal('fetch', fetch); render(<App />); await ready();
    expect(await history().findByRole('alert')).toHaveTextContent('MANAGEMENT_UNAVAILABLE');
    expect(detail().getByRole('heading', { name: 'Source alpha' })).toBeVisible();
    fail = false; await user.click(history().getByRole('button', { name: 'Try again' }));
    expect(await history().findByText('Current in displayed source record')).toBeVisible();
    expect(fetch.mock.calls.filter((call) => !String(call[0]).includes('/revisions?'))).toHaveLength(1);
  });

  it('presents an empty revision page explicitly without erasing detail', async () => {
    vi.stubGlobal('fetch', vi.fn().mockImplementation(async (path: string) => path.includes('/revisions?') ? jsonResponse(revisionPage([])) : sourceResponse()));
    render(<App />); await ready(); expect(await history().findByRole('heading', { name: 'No revisions returned' })).toBeVisible();
    expect(history().getByRole('button', { name: 'Next revisions' })).toBeDisabled();
  });

  it('pages explicit 20-row high-water history forward/back and restarts on refresh', async () => {
    const user = userEvent.setup(), record = sourceRecord(); record.currentRevision = 21;
    const fetch = vi.fn().mockImplementation(async (path: string) => path.includes('/revisions?')
      ? jsonResponse(new URL(path, 'http://localhost').searchParams.has('cursor') ? revisionPage([revision(21)]) : fullRevisionPage()) : sourceResponse(record));
    vi.stubGlobal('fetch', fetch); render(<App />); await ready();
    await user.click(history().getByRole('button', { name: 'Next revisions' }));
    expect(await history().findByRole('heading', { name: /Revision 21/ })).toBeVisible(); expect(history().queryByRole('heading', { name: /^Revision 1 / })).not.toBeInTheDocument();
    expect(fetch.mock.calls.at(-1)?.[0]).toContain(`limit=20&cursor=${FIRST_CURSOR}`);
    await user.click(history().getByRole('button', { name: 'Previous revisions' }));
    await history().findByRole('heading', { name: /^Revision 1 / }); expect(fetch.mock.calls.at(-1)?.[0]).toMatch(/revisions\?limit=20$/);
    await user.click(history().getByRole('button', { name: 'Next revisions' })); await history().findByRole('heading', { name: /Revision 21/ });
    await user.click(history().getByRole('button', { name: 'Refresh revisions' }));
    await history().findByRole('heading', { name: /^Revision 1 / });
    expect(history().getByText(/Revision page 1/)).toBeVisible(); expect(fetch.mock.calls.at(-1)?.[0]).toMatch(/revisions\?limit=20$/);
    expect(fetch.mock.calls.filter((call) => !String(call[0]).includes('/revisions?'))).toHaveLength(1);
  });

  it('offers explicit invalid-cursor restart and never silently restarts', async () => {
    const user = userEvent.setup(); const fetch = vi.fn().mockImplementation(async (path: string) => path.includes('/revisions?')
      ? new URL(path, 'http://localhost').searchParams.has('cursor') ? jsonResponse(managementFailure('INVALID_ARGUMENT'), 400) : jsonResponse(fullRevisionPage()) : sourceResponse());
    vi.stubGlobal('fetch', fetch); render(<App />); await ready();
    await user.click(history().getByRole('button', { name: 'Next revisions' })); await history().findByRole('alert'); expect(fetch).toHaveBeenCalledTimes(3);
    expect(detail().getByRole('heading', { name: 'Source alpha' })).toBeVisible();
    await user.click(history().getByRole('button', { name: 'Start from first revision page' })); await history().findByRole('heading', { name: /^Revision 1 / });
    expect(history().getByText(/Revision page 1/)).toBeVisible();
  });

  it.each(['cycle', 'regressing'] as const)('fails closed on %s revision traversal', async (mode) => {
    const user = userEvent.setup(); let page = 0;
    vi.stubGlobal('fetch', vi.fn().mockImplementation(async (path: string) => {
      if (!path.includes('/revisions?')) return sourceResponse();
      page++;
      return jsonResponse(page === 1 ? fullRevisionPage() : page === 2 ? fullRevisionPage(21, SECOND_CURSOR)
        : mode === 'cycle' ? fullRevisionPage(41, FIRST_CURSOR) : revisionPage([revision(2)]));
    }));
    render(<App />); await ready();
    await user.click(history().getByRole('button', { name: 'Next revisions' })); await history().findByRole('heading', { name: /^Revision 21 / });
    await user.click(history().getByRole('button', { name: 'Next revisions' }));
    expect(await history().findByRole('alert')).toHaveTextContent('does not match the source contract');
  });

  it('updates current markers only from refreshed source metadata while preserving historical configuration', async () => {
    const user = userEvent.setup();
    const old = sourceRecord(); old.config.inputStorageProfileId = '20000000-0000-4000-8000-000000000002';
    old.configSha256 = 'c'.repeat(64); let current = old;
    vi.stubGlobal('fetch', vi.fn().mockImplementation(async (path: string) => path.includes('/revisions?')
      ? jsonResponse(revisionPage([revision(2, old), revision(3)])) : sourceResponse(current)));
    render(<App />); await ready(); expect(await history().findByText('Newer than displayed source record')).toBeVisible();
    await user.click(history().getByText('Configuration references for revision 2'));
    current = { ...sourceRecord(), currentRevision: 3, rowVersion: '9007199254740994' };
    await user.click(history().getByRole('button', { name: 'Refresh revisions' })); await history().findByText('Newer than displayed source record');
    await user.click(screen.getByRole('button', { name: 'Refresh source and revisions' }));
    await waitFor(() => expect(detail().getByText('9007199254740994')).toBeVisible());
    expect(history().getByRole('heading', { name: /^Revision 2 / })).toHaveTextContent('Historical revision');
    expect(history().getByRole('heading', { name: /^Revision 3 / })).toHaveTextContent('Current in displayed source record');
    await user.click(history().getByText('Configuration references for revision 2'));
    expect(history().getByText(old.config.inputStorageProfileId)).toBeVisible();
    expect(detail().getByText(current.config.inputStorageProfileId)).toBeVisible();
  });
});

describe('source request lifecycle', () => {
  it('resets revision paging when the source changes and aborts a pending history retry', async () => {
    const user = userEvent.setup(), late = deferred<Response>(); let alphaPages = 0;
    const fetch = vi.fn().mockImplementation(async (path: string) => {
      if (path.includes('/content-intelligence/alpha/revisions?')) {
        alphaPages++;
        return alphaPages === 1 ? jsonResponse(fullRevisionPage()) : alphaPages === 2 ? jsonResponse(revisionPage([revision(21)])) : late.promise;
      }
      return respond(path);
    });
    vi.stubGlobal('fetch', fetch); render(<App />); await ready();
    await user.click(history().getByRole('button', { name: 'Next revisions' }));
    await history().findByRole('heading', { name: /Revision 21/ });
    await user.click(history().getByRole('button', { name: 'Previous revisions' }));
    const obsolete = (fetch.mock.calls.at(-1)?.[1] as RequestInit).signal;
    act(() => { window.location.hash = sourceHref({ productId: 'application-reliability', sourceId: 'alpha' }); });
    await waitFor(() => expect(fetch).toHaveBeenCalledTimes(6)); await ready();
    expect(obsolete?.aborted).toBe(true);
    expect(history().getByText(/Revision page 1/)).toBeVisible();
    expect(fetch.mock.calls.at(-1)?.[0]).toBe('/api/management/v1/sources/application-reliability/alpha/revisions?limit=20');
    await act(async () => late.reject(new DOMException('aborted', 'AbortError')));
    expect(history().queryByRole('alert')).not.toBeInTheDocument();
    expect(detail().getByText('Application Reliability')).toBeVisible();
  });

  it('aborts rapid source switches and ignores out-of-order detail and revision successes/failures', async () => {
    const a = deferred<Response>(), ar = deferred<Response>(), b = deferred<Response>(), br = deferred<Response>();
    const fetch = vi.fn().mockReturnValueOnce(a.promise).mockReturnValueOnce(ar.promise).mockReturnValueOnce(b.promise).mockReturnValueOnce(br.promise)
      .mockImplementation(async (path: string) => respond(path));
    vi.stubGlobal('fetch', fetch); render(<App />);
    act(() => { window.location.hash = sourceHref({ productId: 'content-intelligence', sourceId: 'beta' }); });
    await waitFor(() => expect(fetch).toHaveBeenCalledTimes(4));
    act(() => { window.location.hash = sourceHref({ productId: 'application-reliability', sourceId: 'alpha' }); });
    await waitFor(() => expect(fetch).toHaveBeenCalledTimes(6));
    await ready(); expect(detail().getByText('Application Reliability')).toBeVisible();
    for (const call of fetch.mock.calls.slice(0, 4)) expect((call[1] as RequestInit).signal?.aborted).toBe(true);
    await act(async () => { a.resolve(sourceResponse()); ar.resolve(jsonResponse(revisionPage())); b.reject(new Error('private')); br.resolve(jsonResponse(revisionPage([revision(500)]))); });
    expect(detail().getByText('Application Reliability')).toBeVisible();
    expect(history().queryByRole('heading', { name: /Revision 500/ })).not.toBeInTheDocument(); expect(screen.queryByRole('alert')).not.toBeInTheDocument();
    expect(history().getByText(/Revision page 1/)).toBeVisible();
  });

  it('ignores late page data after refresh and clears old successful panels when refresh fails', async () => {
    const user = userEvent.setup(), late = deferred<Response>(); let detailCalls = 0, revisionCalls = 0;
    const fetch = vi.fn().mockImplementation(async (path: string) => {
      if (!path.includes('/revisions?')) return ++detailCalls === 1 ? sourceResponse() : jsonResponse(managementFailure(), 503);
      revisionCalls++; return revisionCalls === 1 ? jsonResponse(fullRevisionPage()) : revisionCalls === 2 ? late.promise : jsonResponse(managementFailure(), 503);
    });
    vi.stubGlobal('fetch', fetch); render(<App />); await ready();
    await user.click(history().getByRole('button', { name: 'Next revisions' }));
    const signal = (fetch.mock.calls.at(-1)?.[1] as RequestInit).signal;
    await user.click(screen.getByRole('button', { name: 'Refresh source and revisions' }));
    await detail().findByRole('alert'); await history().findByRole('alert'); expect(signal?.aborted).toBe(true);
    expect(detail().queryByRole('heading', { name: 'Source alpha' })).not.toBeInTheDocument();
    await act(async () => late.resolve(jsonResponse(revisionPage([revision(21)]))));
    expect(history().queryByRole('heading', { name: /Revision 21/ })).not.toBeInTheDocument();
    expect(history().getByText(/Revision page 1/)).toBeVisible();
  });

  it('aborts both panels on route departure and ignores late responses after navigation', async () => {
    const user = userEvent.setup(), late = deferred<Response>(), lateHistory = deferred<Response>();
    const fetch = vi.fn().mockReturnValueOnce(late.promise).mockReturnValueOnce(lateHistory.promise).mockImplementation(async (path: string) => respond(path));
    vi.stubGlobal('fetch', fetch); render(<App />); await user.click(workspace('Analytics'));
    await screen.findByRole('heading', { name: 'Publisher breakdown' });
    for (const call of fetch.mock.calls.slice(0, 2)) expect((call[1] as RequestInit).signal?.aborted).toBe(true);
    await act(async () => { late.resolve(sourceResponse()); lateHistory.resolve(jsonResponse(revisionPage())); });
    expect(screen.queryByRole('region', { name: 'Current source record' })).not.toBeInTheDocument();
    expect(screen.getByLabelText('Publication fingerprint')).toHaveTextContent(FINGERPRINT);
  });

  it('aborts detail and revision requests on unmount, including StrictMode replay', async () => {
    const late = deferred<Response>(), fetch = vi.fn().mockReturnValue(late.promise); vi.stubGlobal('fetch', fetch);
    const view = render(<StrictMode><App /></StrictMode>);
    expect(fetch).toHaveBeenCalledTimes(4);
    expect((fetch.mock.calls[0]?.[1] as RequestInit).signal?.aborted).toBe(true);
    expect((fetch.mock.calls[1]?.[1] as RequestInit).signal?.aborted).toBe(true);
    view.unmount(); for (const call of fetch.mock.calls) expect((call[1] as RequestInit).signal?.aborted).toBe(true);
    await act(async () => late.resolve(sourceResponse()));
  });
});
