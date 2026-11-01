import { StrictMode } from 'react';
import { act, render, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { App } from './App';
import { deferred, fixture, FINGERPRINT, jsonResponse } from './test/fixtures';
import { dailyFixture, qualityFixture } from './test/analyticsFixtures';
import { CORRELATION, FIRST_CURSOR, SECOND_CURSOR, fullSourcePage, managementFailure, source, sourcePage } from './test/managementFixtures';
import { parseWorkspaceRoute } from './navigation';

beforeEach(() => { window.history.replaceState(null, '', '/#/operations/sources'); });
const ready = () => screen.findByRole('heading', { name: 'Registered sources' });
const workspace = (name: string) => within(screen.getByRole('navigation', { name: 'Workspaces' })).getByRole('link', { name });
const row = (text: string) => within(screen.getByRole('table')).getByRole('rowheader', { name: new RegExp(text) });
const urlOf = (path: string) => new URL(path, 'http://localhost');

describe('Operations foundation and source browsing', () => {
  it('opens Sources directly, shows loading, then exact read-only records without analytics fetches', async () => {
    const pending = deferred<Response>(), fetch = vi.fn().mockReturnValue(pending.promise);
    vi.stubGlobal('fetch', fetch); render(<App />);
    expect(screen.getByRole('heading', { name: 'Loading sources' })).toBeInTheDocument();
    expect(screen.queryByRole('table')).not.toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Next sources' })).toBeDisabled();
    await act(async () => pending.resolve(jsonResponse(sourcePage()))); await ready();
    expect(fetch).toHaveBeenCalledTimes(1);
    expect(fetch.mock.calls[0]?.[0]).toBe('/api/management/v1/sources?limit=20');
    expect(workspace('Operations')).toHaveAttribute('aria-current', 'page');
    expect(row('Source alpha')).toHaveTextContent('alpha');
    expect(screen.getByText('9007199254740993')).toBeVisible();
    expect(screen.getByText('2026-11-01').closest('time')).toHaveAttribute('datetime', '2026-11-01T12:34:56.654321Z');
    expect(screen.getByText('12:34:56.654321 UTC')).toBeVisible();
    expect(screen.getByRole('region', { name: /Registered sources table/ })).toHaveAttribute('tabindex', '0');
    expect(screen.getByRole('button', { name: 'Previous sources' })).toBeDisabled();
    expect(screen.getByRole('button', { name: 'Next sources' })).toBeDisabled();
    expect(screen.queryByRole('button', { name: /create|enable|run now|cancel|resume|retry run|import/i })).not.toBeInTheDocument();
  });

  it('separates an empty catalog from filtered empty results', async () => {
    const user = userEvent.setup(), fetch = vi.fn().mockImplementation(async () => jsonResponse(sourcePage([])));
    vi.stubGlobal('fetch', fetch); render(<App />);
    expect(await screen.findByRole('heading', { name: 'No registered sources' })).toBeVisible();
    await user.selectOptions(screen.getByRole('combobox', { name: 'Admission status' }), 'true');
    expect(await screen.findByRole('heading', { name: 'No sources match these filters' })).toBeVisible();
    expect(fetch.mock.calls.at(-1)?.[0]).toContain('enabled=true');
    await user.selectOptions(screen.getByRole('combobox', { name: 'Admission status' }), 'false');
    expect(await screen.findByRole('heading', { name: 'No sources match these filters' })).toBeVisible();
    expect(fetch.mock.calls.at(-1)?.[0]).toContain('enabled=false');
    expect(screen.queryByRole('table')).not.toBeInTheDocument();
  });

  it.each([
    [403, 'LOCAL_BOUNDARY_REJECTED', /Local access was rejected/],
    [404, 'NOT_FOUND', /running in API mode/],
    [503, 'MANAGEMENT_UNAVAILABLE', /source catalog is unavailable/],
  ])('shows safe HTTP %s errors and explicit successful read retry', async (status, code, message) => {
    const user = userEvent.setup(), fetch = vi.fn().mockResolvedValueOnce(jsonResponse(managementFailure(code), status))
      .mockResolvedValueOnce(jsonResponse(sourcePage()));
    vi.stubGlobal('fetch', fetch); render(<App />);
    const alert = await screen.findByRole('alert');
    expect(alert).toHaveTextContent(message); expect(alert).toHaveTextContent(CORRELATION);
    expect(alert).toHaveTextContent(status === 503 ? 'The service marked this read as retryable.' : 'Resolve the reported condition before trying again.');
    expect(alert).not.toHaveTextContent(/secret|traceback/);
    expect(screen.queryByRole('heading', { name: 'No registered sources' })).not.toBeInTheDocument();
    expect(fetch).toHaveBeenCalledTimes(1);
    await user.click(screen.getByRole('button', { name: 'Try again' })); await ready();
    expect(fetch).toHaveBeenCalledTimes(2);
  });

  it('shows network and invalid-envelope errors without leaving old records visible', async () => {
    const user = userEvent.setup(), fetch = vi.fn().mockResolvedValueOnce(jsonResponse(sourcePage()))
      .mockRejectedValueOnce(new Error('/private/secret')).mockResolvedValueOnce(jsonResponse({ data: 'secret' }));
    vi.stubGlobal('fetch', fetch); render(<App />); await ready();
    await user.click(screen.getByRole('button', { name: 'Refresh sources' }));
    expect(await screen.findByRole('alert')).toHaveTextContent('Cannot reach the local management service');
    expect(screen.queryByRole('table')).not.toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'Try again' }));
    await waitFor(() => expect(screen.getByRole('alert')).toHaveTextContent('does not match the source contract'));
    expect(screen.getByRole('alert')).not.toHaveTextContent('secret');
  });

  it('pages only on demand, goes back, and resets cursors for filters, page size and refresh', async () => {
    const user = userEvent.setup();
    const fetch = vi.fn().mockImplementation(async (path: string) => {
      const url = urlOf(path), limit = Number(url.searchParams.get('limit'));
      if (url.searchParams.has('productId') || url.searchParams.has('enabled') || limit !== 20) return jsonResponse(sourcePage([], limit));
      return jsonResponse(url.searchParams.has('cursor') ? sourcePage([source('tail')]) : fullSourcePage());
    });
    vi.stubGlobal('fetch', fetch); render(<App />); await ready();
    expect(fetch).toHaveBeenCalledTimes(1);
    expect(screen.getByText(/More sources available/)).toBeVisible();
    await user.click(screen.getByRole('button', { name: 'Next sources' })); await waitFor(() => expect(row('tail')).toBeVisible());
    expect(fetch.mock.calls.at(-1)?.[0]).toContain(`cursor=${FIRST_CURSOR}`);
    expect(screen.getByText(/Page 2.*End of results/)).toBeVisible();
    await user.click(screen.getByRole('button', { name: 'Previous sources' })); await waitFor(() => expect(row('source-000')).toBeVisible());
    expect(urlOf(fetch.mock.calls.at(-1)![0]).searchParams.has('cursor')).toBe(false);
    await user.click(screen.getByRole('button', { name: 'Next sources' })); await waitFor(() => expect(row('tail')).toBeVisible());
    await user.selectOptions(screen.getByRole('combobox', { name: 'Admission status' }), 'false');
    await screen.findByRole('heading', { name: 'No sources match these filters' });
    expect(urlOf(fetch.mock.calls.at(-1)![0]).searchParams.has('cursor')).toBe(false);
    expect(screen.getByText(/Page 1/)).toBeVisible();
    await user.selectOptions(screen.getByRole('combobox', { name: 'Admission status' }), '');
    await waitFor(() => expect(row('source-000')).toBeVisible());
    await user.click(screen.getByRole('button', { name: 'Next sources' })); await waitFor(() => expect(row('tail')).toBeVisible());
    await user.selectOptions(screen.getByRole('combobox', { name: 'Product' }), 'application-reliability');
    await screen.findByRole('heading', { name: 'No sources match these filters' });
    expect(screen.getByText(/Page 1/)).toBeVisible();
    await user.selectOptions(screen.getByRole('combobox', { name: 'Admission status' }), 'false');
    await user.selectOptions(screen.getByRole('combobox', { name: 'Sources per page' }), '100');
    await waitFor(() => expect(fetch.mock.calls.at(-1)?.[0]).toContain('limit=100'));
    await user.click(screen.getByRole('button', { name: 'Refresh sources' }));
    await waitFor(() => expect(screen.getByText(/Page 1.*End of results/)).toBeVisible());
    const last = urlOf(fetch.mock.calls.at(-1)![0]);
    expect(last.searchParams.get('productId')).toBe('application-reliability');
    expect(last.searchParams.get('enabled')).toBe('false'); expect(last.searchParams.has('cursor')).toBe(false);
  });

  it('abandons a rejected cursor without silently restarting or retrying it', async () => {
    const user = userEvent.setup(), fetch = vi.fn().mockResolvedValueOnce(jsonResponse(fullSourcePage()))
      .mockResolvedValueOnce(jsonResponse(managementFailure('INVALID_ARGUMENT'), 400)).mockResolvedValueOnce(jsonResponse(sourcePage()));
    vi.stubGlobal('fetch', fetch); render(<App />); await ready();
    await user.click(screen.getByRole('button', { name: 'Next sources' })); await screen.findByRole('alert');
    expect(fetch).toHaveBeenCalledTimes(2);
    await user.click(screen.getByRole('button', { name: 'Start from first page' })); await ready();
    expect(fetch.mock.calls.at(-1)?.[0]).toBe('/api/management/v1/sources?limit=20');
  });

  it('hides old rows immediately and ignores late responses even if fetch ignores abort', async () => {
    const user = userEvent.setup(), oldPage = deferred<Response>();
    const fetch = vi.fn().mockResolvedValueOnce(jsonResponse(fullSourcePage())).mockReturnValueOnce(oldPage.promise)
      .mockResolvedValueOnce(jsonResponse(sourcePage([source('reliability', 'application-reliability')])));
    vi.stubGlobal('fetch', fetch); render(<App />); await ready();
    await user.click(screen.getByRole('button', { name: 'Next sources' }));
    expect(screen.queryByRole('table')).not.toBeInTheDocument();
    const oldSignal = (fetch.mock.calls[1]?.[1] as RequestInit).signal;
    await user.selectOptions(screen.getByRole('combobox', { name: 'Product' }), 'application-reliability');
    await ready(); expect(oldSignal?.aborted).toBe(true);
    await act(async () => oldPage.resolve(jsonResponse(sourcePage([source('obsolete')]))));
    expect(row('reliability')).toBeVisible(); expect(screen.queryByRole('rowheader', { name: /obsolete/ })).not.toBeInTheDocument();
    expect(screen.queryByRole('alert')).not.toBeInTheDocument();
  });

  it('refuses cursor cycles across multiple pages', async () => {
    const user = userEvent.setup(), fetch = vi.fn().mockResolvedValueOnce(jsonResponse(fullSourcePage(FIRST_CURSOR)))
      .mockResolvedValueOnce(jsonResponse(fullSourcePage(SECOND_CURSOR))).mockResolvedValueOnce(jsonResponse(fullSourcePage(FIRST_CURSOR)));
    vi.stubGlobal('fetch', fetch); render(<App />); await ready();
    await user.click(screen.getByRole('button', { name: 'Next sources' }));
    await waitFor(() => expect(screen.getByText(/Page 2/)).toBeVisible()); await ready();
    await user.click(screen.getByRole('button', { name: 'Next sources' }));
    expect(await screen.findByRole('alert')).toHaveTextContent('does not match the source contract');
    expect(screen.getByRole('button', { name: 'Next sources' })).toBeDisabled(); expect(fetch).toHaveBeenCalledTimes(3);
  });

  it('keeps only the latest rapid product/status selection despite out-of-order success and failure', async () => {
    const initial = deferred<Response>(), intermediate = deferred<Response>(), latest = deferred<Response>();
    const fetch = vi.fn().mockReturnValueOnce(initial.promise).mockReturnValueOnce(intermediate.promise).mockReturnValueOnce(latest.promise);
    const user = userEvent.setup(); vi.stubGlobal('fetch', fetch); render(<App />);
    await user.selectOptions(screen.getByRole('combobox', { name: 'Product' }), 'application-reliability');
    await user.selectOptions(screen.getByRole('combobox', { name: 'Admission status' }), 'false');
    expect(fetch).toHaveBeenCalledTimes(3);
    expect((fetch.mock.calls[0]?.[1] as RequestInit).signal?.aborted).toBe(true);
    expect((fetch.mock.calls[1]?.[1] as RequestInit).signal?.aborted).toBe(true);
    expect((fetch.mock.calls[2]?.[1] as RequestInit).signal?.aborted).toBe(false);
    await act(async () => latest.resolve(jsonResponse(sourcePage([{ ...source('current', 'application-reliability'), enabled: false }]))));
    await ready();
    await act(async () => {
      initial.resolve(jsonResponse(sourcePage([source('obsolete')])));
      intermediate.reject(new Error('/private/late-error'));
    });
    expect(row('current')).toBeVisible();
    expect(screen.queryByRole('rowheader', { name: /obsolete/ })).not.toBeInTheDocument();
    expect(screen.queryByRole('alert')).not.toBeInTheDocument();
    expect(screen.getByRole('combobox', { name: 'Product' })).toHaveValue('application-reliability');
    expect(screen.getByRole('combobox', { name: 'Admission status' })).toHaveValue('false');
    const last = urlOf(fetch.mock.calls[2]![0]);
    expect(last.searchParams.get('enabled')).toBe('false'); expect(last.searchParams.has('cursor')).toBe(false);
  });

  it('renders untrusted labels as text and distinguishes identical IDs in different products', async () => {
    const rows = [source('same', 'application-reliability'), { ...source('same'), displayName: '<img src=x onerror=alert(1)>', enabled: false }];
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(jsonResponse(sourcePage(rows))));
    const { container } = render(<App />); await ready();
    expect(screen.getByText('<img src=x onerror=alert(1)>')).toBeVisible(); expect(container.querySelector('img')).toBeNull();
    expect(within(screen.getByRole('table')).getAllByRole('rowheader')).toHaveLength(2);
    expect(screen.getByText('Disabled', { selector: 'span' })).toBeVisible();
  });

  it('remains safe under StrictMode and aborts on workspace changes and unmount', async () => {
    const user = userEvent.setup(), pending = deferred<Response>(), fetch = vi.fn().mockReturnValue(pending.promise);
    vi.stubGlobal('fetch', fetch); const view = render(<StrictMode><App /></StrictMode>);
    expect((fetch.mock.calls[0]?.[1] as RequestInit).signal?.aborted).toBe(true);
    const second = (fetch.mock.calls.at(-1)?.[1] as RequestInit).signal;
    await user.click(workspace('Analytics')); expect(second?.aborted).toBe(true);
    await act(async () => pending.resolve(jsonResponse(sourcePage())));
    expect(screen.queryByRole('heading', { name: 'Registered sources' })).not.toBeInTheDocument();
    view.unmount(); expect((fetch.mock.calls.at(-1)?.[1] as RequestInit).signal?.aborted).toBe(true);
  });
});

describe('workspace navigation and independent Analytics session', () => {
  it('ignores late Analytics identity changes after navigating to Operations', async () => {
    window.history.replaceState(null, '', '/#/analytics'); const user = userEvent.setup(), late = deferred<Response>();
    const filtered = fixture(); filtered.data = filtered.data.filter((item) => item.sourceId === 'beta'); filtered.meta.returned = 1;
    const fetch = vi.fn().mockResolvedValueOnce(jsonResponse(fixture())).mockReturnValueOnce(late.promise)
      .mockResolvedValueOnce(jsonResponse(sourcePage())).mockResolvedValueOnce(jsonResponse(filtered));
    vi.stubGlobal('fetch', fetch); render(<App />); await screen.findByRole('heading', { name: 'Publisher breakdown' });
    await user.selectOptions(screen.getByRole('combobox', { name: 'Publisher scope' }), 'beta');
    await user.click(workspace('Operations')); await ready();
    expect((fetch.mock.calls[1]?.[1] as RequestInit).signal?.aborted).toBe(true);
    await act(async () => late.resolve(jsonResponse({ ...filtered, publication: 'b'.repeat(64) })));
    expect(row('alpha')).toBeVisible(); expect(screen.queryByRole('alert')).not.toBeInTheDocument();
    await user.click(workspace('Analytics')); await screen.findByRole('heading', { name: 'Publisher breakdown' });
    expect(screen.getByLabelText('Publication fingerprint')).toHaveTextContent(FINGERPRINT);
    expect(screen.getByRole('combobox', { name: 'Publisher scope' })).toHaveValue('beta');
    expect(screen.queryByRole('button', { name: 'Reload dashboard' })).not.toBeInTheDocument();
    expect(fetch).toHaveBeenCalledTimes(4);
  });

  it('ignores a late Sources page while Analytics is visible and revalidates on returning', async () => {
    const user = userEvent.setup(), late = deferred<Response>();
    const fetch = vi.fn().mockReturnValueOnce(late.promise).mockResolvedValueOnce(jsonResponse(fixture()))
      .mockResolvedValueOnce(jsonResponse(sourcePage([source('fresh')])));
    vi.stubGlobal('fetch', fetch); render(<App />);
    await user.click(workspace('Analytics')); await screen.findByRole('heading', { name: 'Publisher breakdown' });
    expect((fetch.mock.calls[0]?.[1] as RequestInit).signal?.aborted).toBe(true);
    await act(async () => late.resolve(jsonResponse(sourcePage([source('obsolete')]))));
    expect(screen.getByLabelText('Publication fingerprint')).toHaveTextContent(FINGERPRINT);
    expect(screen.queryByRole('heading', { name: 'Registered sources' })).not.toBeInTheDocument();
    expect(screen.queryByRole('alert')).not.toBeInTheDocument();
    await user.click(workspace('Operations')); await ready(); expect(row('fresh')).toBeVisible();
    expect(screen.queryByRole('rowheader', { name: /obsolete/ })).not.toBeInTheDocument();
    expect(fetch).toHaveBeenCalledTimes(3);
  });

  it.each([
    ['Publisher Overview', 'View exact values', 'Publisher breakdown'],
    ['Daily Activity', 'View exact daily values', 'Daily breakdown'],
    ['Data Quality', 'View quality values', 'Quality breakdown'],
  ])('preserves existing %s in-page links without starting another request', async (view, link, heading) => {
    window.history.replaceState(null, '', '/#/analytics'); const user = userEvent.setup();
    const fetch = vi.fn().mockImplementation(async (path: string) => jsonResponse(path.includes('/daily?') ? dailyFixture()
      : path.includes('/quality?') ? qualityFixture() : fixture()));
    vi.stubGlobal('fetch', fetch); render(<App />); await screen.findByRole('heading', { name: 'Publisher breakdown' });
    await user.click(within(screen.getByRole('navigation', { name: 'Analytics views' })).getByRole('button', { name: view }));
    await screen.findByRole('heading', { name: heading }); const count = fetch.mock.calls.length;
    await user.click(screen.getByRole('link', { name: new RegExp(`^${link}`) }));
    await waitFor(() => expect(window.location.hash).toMatch(/-details$/));
    expect(screen.getByRole('heading', { name: heading })).toBeVisible();
    expect(screen.queryByRole('heading', { name: 'Page not found' })).not.toBeInTheDocument();
    expect(fetch).toHaveBeenCalledTimes(count);
  });

  it('preserves Analytics filters and pin through Operations, browser Back and Forward', async () => {
    window.history.replaceState(null, '', '/'); const user = userEvent.setup();
    const fetch = vi.fn().mockImplementation(async (path: string) => {
      if (path.startsWith('/api/management/')) return jsonResponse(sourcePage());
      const body = fixture(), selected = urlOf(path).searchParams.get('sourceId');
      if (selected) { body.data = body.data.filter((r) => r.sourceId === selected); body.meta.returned = body.data.length; }
      return jsonResponse(body);
    });
    vi.stubGlobal('fetch', fetch); render(<App />); await screen.findByRole('heading', { name: 'Publisher breakdown' });
    await user.selectOptions(screen.getByRole('combobox', { name: 'Publisher scope' }), 'beta');
    await waitFor(() => expect(screen.queryByRole('rowheader', { name: 'alpha' })).not.toBeInTheDocument());
    await user.click(workspace('Operations')); await ready(); expect(screen.getByRole('main')).toHaveFocus();
    act(() => window.history.back());
    await screen.findByRole('heading', { name: 'Publisher breakdown' });
    expect(screen.getByRole('combobox', { name: 'Publisher scope' })).toHaveValue('beta');
    expect(screen.getByLabelText('Publication fingerprint')).toHaveTextContent(FINGERPRINT);
    act(() => window.history.forward()); await ready(); expect(workspace('Operations')).toHaveAttribute('aria-current', 'page');
  });

  it('does not let a blocked Analytics publication prevent source browsing', async () => {
    window.history.replaceState(null, '', '/'); const user = userEvent.setup();
    const changed = { ...fixture(), publication: 'b'.repeat(64) };
    changed.data = changed.data.filter((item) => item.sourceId === 'beta'); changed.meta.returned = 1;
    vi.stubGlobal('fetch', vi.fn().mockResolvedValueOnce(jsonResponse(fixture())).mockResolvedValueOnce(jsonResponse(changed))
      .mockResolvedValueOnce(jsonResponse(sourcePage())));
    render(<App />); await screen.findByRole('heading', { name: 'Publisher breakdown' });
    await user.selectOptions(screen.getByRole('combobox', { name: 'Publisher scope' }), 'beta');
    await screen.findByRole('button', { name: 'Reload dashboard' });
    await user.click(workspace('Operations')); await ready();
    expect(screen.queryByRole('alert')).not.toBeInTheDocument();
    await user.click(workspace('Analytics')); expect(await screen.findByRole('button', { name: 'Reload dashboard' })).toBeVisible();
  });

  it('shows Operations-specific unavailability even when Analytics is unavailable too', async () => {
    window.history.replaceState(null, '', '/#/analytics'); const user = userEvent.setup();
    const fetch = vi.fn().mockResolvedValueOnce(jsonResponse({ code: 'INPUT_UNAVAILABLE' }, 503))
      .mockResolvedValueOnce(jsonResponse(managementFailure(), 503));
    vi.stubGlobal('fetch', fetch); render(<App />);
    expect(await screen.findByRole('alert')).toHaveTextContent('Publisher analytics are unavailable');
    await user.click(workspace('Operations'));
    await waitFor(() => expect(screen.getByRole('alert')).toHaveTextContent('The source catalog is unavailable'));
    expect(screen.getByRole('alert')).not.toHaveTextContent(/Gold|publication|secret/);
    expect(screen.queryByRole('heading', { name: 'No registered sources' })).not.toBeInTheDocument();
    expect(fetch).toHaveBeenCalledTimes(2);
  });

  it('supports keyboard workspace navigation and skip links without replacing the route', async () => {
    const user = userEvent.setup(); vi.stubGlobal('fetch', vi.fn().mockResolvedValue(jsonResponse(sourcePage())));
    render(<App />); await ready();
    await user.tab(); expect(screen.getByRole('link', { name: 'Skip to operations' })).toHaveFocus();
    await user.keyboard('{Enter}'); expect(screen.getByRole('main')).toHaveFocus();
    expect(window.location.hash).toBe('#/operations/sources');
    workspace('Operations').focus(); await user.tab();
    expect(within(screen.getByRole('navigation', { name: 'Operations views' })).getByRole('link', { name: 'Sources' })).toHaveFocus();
    await user.tab(); expect(screen.getByRole('link', { name: 'Executions' })).toHaveFocus();
    await user.tab(); expect(screen.getByRole('combobox', { name: 'Product' })).toHaveFocus();
  });

  it('handles unknown routes without fetching data and offers supported navigation', async () => {
    window.history.replaceState(null, '', '/#/operations/unimplemented');
    const fetch = vi.fn().mockResolvedValue(jsonResponse(sourcePage())), user = userEvent.setup();
    vi.stubGlobal('fetch', fetch); render(<App />);
    expect(screen.getByRole('heading', { name: 'Page not found' })).toBeVisible(); expect(fetch).not.toHaveBeenCalled();
    expect(workspace('Operations')).not.toHaveAttribute('aria-current'); expect(workspace('Analytics')).not.toHaveAttribute('aria-current');
    await user.click(workspace('Operations')); await ready(); expect(fetch).toHaveBeenCalledTimes(1);
    expect(parseWorkspaceRoute('#/operations')).toBe('sources'); expect(parseWorkspaceRoute('#/analytics')).toBe('analytics');
  });
});
