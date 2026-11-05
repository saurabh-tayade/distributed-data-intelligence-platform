import { StrictMode } from 'react';
import { act, fireEvent, render, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { App } from './App';
import { ReliabilityTable, ReliabilityWorkspace } from './ReliabilityWorkspace';
import { RELIABILITY_LABEL } from './reliabilityApi';
import type { ReliabilityQuery, ReliabilityResponse, ReliabilityRow } from './reliabilityApi';
import type { useReliability } from './useReliability';
import { parseWorkspaceRoute } from './navigation';
import { deferred, fixture, FINGERPRINT, jsonResponse } from './test/fixtures';
import { sourcePage } from './test/managementFixtures';

const START = '2026-10-12T00:00:00.000000Z', END = '2026-10-13T00:00:00.000000Z';
const PIN = 'b'.repeat(64);
const query: ReliabilityQuery = { start: START, end: END, limit: 100, offset: 0 };
function row(index = 0): ReliabilityRow {
  return { windowStart: START, serviceId: `service-${String(index).padStart(3, '0')}`, environment: 'test', requestCount: '20',
    serverErrorCount: '2', serverErrorRate: 0.1, durationSumMs: '210', meanDurationMs: 10.5, p95DurationMsApprox: '19',
    serviceName: 'Synthetic checkout', ownerTeam: 'example-platform', serviceTier: 'standard', referenceStatus: 'MATCHED' };
}
function page(path?: string, count = 1, hasMore = false): ReliabilityResponse {
  const search = new URL(path ?? `/api/v1/reliability/request-windows?${new URLSearchParams({ start: START, end: END })}`, 'http://localhost').searchParams;
  const offset = Number(search.get('offset') ?? 0), limit = Number(search.get('limit') ?? 100);
  const environment = search.get('environment') as ReliabilityRow['environment'] | null;
  return { publication: PIN, productId: 'application-reliability', productContractVersion: 2, processorRevision: 2,
    inputFormat: 'JSONL', origin: 'synthetic', label: RELIABILITY_LABEL,
    reference: { referenceId: 'services', revision: 'v1', snapshotFingerprint: 'c'.repeat(64) },
    filters: { start: search.get('start') ?? START, end: search.get('end') ?? END, serviceId: search.get('serviceId'), environment },
    data: Array.from({ length: count }, (_, index) => ({ ...row(offset + index), windowStart: search.get('start') ?? START,
      ...(search.has('serviceId') ? { serviceId: search.get('serviceId')! } : {}), ...(environment ? { environment } : {}) })),
    meta: { returned: count, limit, offset, hasMore } };
}
function mockApi() {
  const fetch = vi.fn(async (path: string) => path.startsWith('/api/v1/reliability/') ? jsonResponse(page(path))
    : path.startsWith('/api/management/') ? jsonResponse(sourcePage()) : jsonResponse(fixture()));
  vi.stubGlobal('fetch', fetch); return fetch;
}
const ready = () => screen.findByRole('heading', { name: 'Request windows' });
const productLink = (name: string) => within(screen.getByRole('navigation', { name: 'Analytics products' })).getByRole('link', { name });
const workspace = (name: string) => within(screen.getByRole('navigation', { name: 'Workspaces' })).getByRole('link', { name });
function input(name: string, value: string) { fireEvent.change(screen.getByRole('textbox', { name }), { target: { value } }); }
function apply(start = START, end = END) {
  input('Start inclusive (UTC)', start); input('End exclusive (UTC)', end);
  fireEvent.click(screen.getByRole('button', { name: 'Apply window filters' }));
}
beforeEach(() => { window.history.replaceState(null, '', '/#/analytics/reliability'); });

describe('Reliability analytics route and presentation', () => {
  it('opens separately without fetching an assumed interval or Content Gold data', () => {
    const fetch = mockApi(); render(<App />);
    expect(screen.getByRole('heading', { name: 'Choose a UTC window interval' })).toBeVisible();
    expect(screen.getByText(RELIABILITY_LABEL)).toBeVisible();
    expect(screen.getByRole('region', { name: 'Reliability publication' })).not.toHaveTextContent('Gold');
    expect(productLink('Application Reliability')).toHaveAttribute('aria-current', 'page');
    expect(workspace('Analytics')).toHaveAttribute('aria-current', 'page');
    expect(screen.queryByRole('table')).not.toBeInTheDocument();
    expect(fetch).not.toHaveBeenCalled();
    expect(document.title).toBe('Reliability request windows | DDIP');
  });

  it('shows native windows, exact values, ratios and explicitly approximate p95', async () => {
    const user = userEvent.setup(); const fetch = mockApi(); render(<App />); apply(); await ready();
    const search = new URL(fetch.mock.calls[0]![0], 'http://localhost').searchParams;
    expect(Object.fromEntries(search)).toEqual({ start: START, end: END, limit: '100', offset: '0' });
    expect(screen.getByLabelText('Reliability publication fingerprint')).toHaveTextContent(PIN);
    const table = screen.getByRole('table');
    expect(table.querySelector('time')).toHaveAttribute('datetime', START);
    expect(table).toHaveTextContent('00:00:00.000000 UTC');
    expect(within(table).getByRole('columnheader', { name: 'Approximate p95 (ms)' })).toBeVisible();
    expect(within(table).getByRole('cell', { name: '0.1' })).toBeVisible();
    expect(within(table).getByRole('cell', { name: '210' })).toBeVisible();
    expect(within(table).getByRole('cell', { name: '10.5' })).toBeVisible();
    expect(screen.getByText(/not a percentage or availability score/)).toBeVisible();
    expect(screen.queryByText(/grand total/i)).not.toBeInTheDocument();
    await user.click(screen.getByText('Publication and service-reference details'));
    expect(screen.getByText('services')).toBeVisible();
    expect(screen.getByText('JSONL')).toBeVisible();
    expect(screen.getByText('c'.repeat(64))).toBeVisible();
    expect(fetch).toHaveBeenCalledTimes(1);
  });

  it('keeps missing metadata explicit and renders metadata as text', () => {
    const response = page(); Object.assign(response.data[0]!, { serviceName: null, ownerTeam: null, serviceTier: null, referenceStatus: 'MISSING' });
    const { rerender } = render(<ReliabilityTable response={response} />);
    expect(screen.getByText('Missing reference')).toBeVisible();
    expect(screen.getAllByText('Not available')).toHaveLength(3);
    response.data[0] = { ...row(), serviceName: '<img src=x onerror=alert(1)>' };
    rerender(<ReliabilityTable response={response} />);
    expect(screen.getByText('<img src=x onerror=alert(1)>')).toBeVisible();
    expect(document.querySelector('img,script,iframe')).toBeNull();
  });

  it('renders large exact integral values without Number conversion or aggregation', () => {
    const response = page(); Object.assign(response.data[0]!, { requestCount: '2000000', serverErrorRate: 0.000001, durationSumMs: '7200000000000', meanDurationMs: 3600000, p95DurationMsApprox: '3600000' });
    render(<ReliabilityTable response={response} />);
    expect(screen.getByRole('cell', { name: '7,200,000,000,000' })).toBeVisible();
    expect(screen.getByRole('cell', { name: '2,000,000' })).toBeVisible();
  });

  it.each(['#/analytics/reliability?limit=20', '#/analytics/reliability/other', '#/analytics/reliability?publication=anything'])('rejects unsupported Reliability route %s', (hash) => {
    expect(parseWorkspaceRoute(hash)).toBe('not-found');
  });
});

describe('Reliability filtering and bounded pages', () => {
  it.each([
    ['2026-02-30T00:00:00.000000Z', END], [START, START], [END, START],
    ['2026-10-12T00:01:00.000000Z', END], ['2026-10-12T00:00:00.000001Z', END],
    ['2026-10-12', END], [START, '2026-11-13T00:00:00.000000Z'], [START, '2026-10-13T00:00:00.000000+00:00'],
  ])('rejects invalid interval %s to %s before fetching', (start, end) => {
    const fetch = mockApi(); render(<App />); apply(start, end);
    expect(screen.getByRole('alert')).toBeVisible(); expect(fetch).not.toHaveBeenCalled();
    expect(screen.getByRole('textbox', { name: 'Start inclusive (UTC)' })).toHaveValue(start);
  });

  it('applies exact filters on submit without per-keystroke requests or silent normalization', async () => {
    const fetch = mockApi(); render(<App />);
    input('Exact service ID', ' Service '); apply();
    expect(screen.getByRole('alert')).toBeVisible(); expect(fetch).not.toHaveBeenCalled();
    input('Exact service ID', 'service-000');
    fireEvent.change(screen.getByRole('combobox', { name: 'Environment' }), { target: { value: 'production' } });
    expect(fetch).not.toHaveBeenCalled(); apply(); await ready();
    const search = new URL(fetch.mock.calls[0]![0], 'http://localhost').searchParams;
    expect(search.get('serviceId')).toBe('service-000'); expect(search.get('environment')).toBe('production');
    expect(screen.getByRole('region', { name: 'Applied Reliability filters' })).toHaveTextContent('production');
  });

  it('preserves the exclusive native year-10000 end sentinel', async () => {
    const fetch = mockApi(); render(<App />);
    apply('9999-12-31T23:55:00.000000Z', '10000-01-01T00:00:00.000000Z'); await ready();
    expect(new URL(fetch.mock.calls[0]![0], 'http://localhost').searchParams.get('end')).toBe('10000-01-01T00:00:00.000000Z');
    expect(screen.getByRole('region', { name: 'Applied Reliability filters' })).toHaveTextContent('10000-01-01T00:00:00.000000Z');
  });

  it('pages explicitly and resets to offset zero for new service, environment, interval or page size', async () => {
    const user = userEvent.setup(); const fetch = mockApi();
    fetch.mockImplementation(async (path) => {
      const search = new URL(path, 'http://localhost').searchParams;
      const offset = Number(search.get('offset') ?? 0), limit = Number(search.get('limit') ?? 100);
      return jsonResponse(page(path, offset === 0 ? limit : 2, offset === 0));
    });
    render(<App />); apply(); await ready();
    expect(screen.getByRole('button', { name: 'Previous windows' })).toBeDisabled();
    await user.click(screen.getByRole('button', { name: 'Next windows' })); await ready();
    expect(screen.getByText('Rows 101–102 in this scope')).toBeVisible();
    expect(screen.getByRole('button', { name: 'Next windows' })).toBeDisabled();
    await user.click(screen.getByRole('button', { name: 'Previous windows' })); await ready();
    expect(screen.getByText('Rows 1–100 in this scope')).toBeVisible();
    await user.click(screen.getByRole('button', { name: 'Next windows' })); await ready();
    input('Exact service ID', 'service-000');
    // A single exact-service filter still permits many native window starts; use one valid row here.
    fetch.mockImplementation(async (path) => jsonResponse(page(path)));
    fireEvent.change(screen.getByRole('combobox', { name: 'Environment' }), { target: { value: 'dev' } });
    fireEvent.change(screen.getByRole('combobox', { name: 'Windows per page' }), { target: { value: '20' } });
    apply('2026-10-12T00:05:00.000000Z'); await ready();
    const search = new URL(fetch.mock.calls.at(-1)![0], 'http://localhost').searchParams;
    expect(Object.fromEntries(search)).toMatchObject({ offset: '0', limit: '20', serviceId: 'service-000', environment: 'dev', start: '2026-10-12T00:05:00.000000Z' });
    expect(screen.getByRole('button', { name: 'Previous windows' })).toBeDisabled();
  });

  it('keeps empty scope empty instead of zero-filling', async () => {
    const fetch = mockApi(); fetch.mockImplementation(async (path) => jsonResponse(page(path, 0)));
    render(<App />); apply();
    expect(await screen.findByRole('heading', { name: 'No request windows in this scope' })).toBeVisible();
    expect(screen.queryByRole('table')).not.toBeInTheDocument();
    expect(screen.getByText(RELIABILITY_LABEL)).toBeVisible();
    expect(screen.getByRole('button', { name: 'Next windows' })).toBeDisabled();
    expect(screen.queryByText('0 ms')).not.toBeInTheDocument();
  });

  it('stops Next at the API maximum offset without inventing a total', () => {
    const response = page(); response.meta.offset = 1_000_000; response.meta.hasMore = true;
    const movePage = vi.fn();
    const model: ReturnType<typeof useReliability> = { query: { ...query, offset: 1_000_000 }, offset: 1_000_000, limit: 100,
      current: { status: 'ready', response }, publication: PIN, blocked: false, movePage, apply: vi.fn(), retry: vi.fn(),
      report: 'request-windows', selectReport: vi.fn(), coverageQuery: {}, coverageCurrent: null, applyCoverage: vi.fn() };
    render(<ReliabilityWorkspace model={model} />);
    expect(screen.getByRole('button', { name: 'Next windows' })).toBeDisabled();
    expect(screen.getByText(/API maximum offset has been reached/)).toBeVisible();
    fireEvent.click(screen.getByRole('button', { name: 'Previous windows' }));
    expect(movePage).toHaveBeenCalledWith(999900);
  });
});

describe('Reliability safe failures and session isolation', () => {
  it('shows only local safe errors, retries manually and preserves the synthetic label', async () => {
    const fetch = mockApi(); fetch.mockResolvedValueOnce(jsonResponse({ code: 'ANALYTICS_CAPACITY_EXCEEDED', message: '/private/secret' }, 503));
    render(<App />); apply(); expect(await screen.findByRole('alert')).toHaveTextContent(/busy|capacity/);
    expect(screen.getByRole('alert')).not.toHaveTextContent('/private');
    expect(screen.getByText(RELIABILITY_LABEL)).toBeVisible(); expect(screen.queryByRole('table')).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'Try again' })); await ready();
    expect(fetch).toHaveBeenCalledTimes(2);
  });

  it('fails closed for malformed rows and echoed filters without rendering server metadata', async () => {
    const fetch = mockApi(); const malformed = page(); malformed.filters.start = '2026-10-12T00:05:00.000000Z';
    malformed.data[0]!.serviceName = '/secret/path'; fetch.mockResolvedValueOnce(jsonResponse(malformed));
    render(<App />); apply(); expect(await screen.findByRole('alert')).toHaveTextContent(/contract|invalid/);
    expect(screen.queryByText('/secret/path')).not.toBeInTheDocument();
    expect(screen.queryByRole('table')).not.toBeInTheDocument();
    expect(screen.queryByLabelText('Reliability publication fingerprint')).not.toBeInTheDocument();
  });

  it('rejects owner teams outside the captured native vocabulary before pinning or showing rows', async () => {
    const fetch = mockApi(); const invalid = page(); invalid.data[0]!.ownerTeam = 'not-in-vocabulary';
    fetch.mockResolvedValueOnce(jsonResponse(invalid)); render(<App />); apply();
    expect(await screen.findByRole('alert')).toHaveTextContent(/contract/);
    expect(screen.queryByRole('table')).not.toBeInTheDocument();
    expect(screen.queryByText('not-in-vocabulary')).not.toBeInTheDocument();
    expect(screen.queryByLabelText('Reliability publication fingerprint')).not.toBeInTheDocument();
  });

  it('ignores a stale reversed response and abort after a newer filter succeeds', async () => {
    const late = deferred<Response>(); const fetch = mockApi(); fetch.mockReturnValueOnce(late.promise);
    render(<App />); apply(); const first = fetch.mock.calls[0]![0];
    const signal = (fetch.mock.calls[0] as unknown as [string, RequestInit])[1].signal;
    input('Exact service ID', 'service-002'); apply(); await ready();
    expect(signal?.aborted).toBe(true);
    const old = page(first); old.publication = 'd'.repeat(64);
    await act(async () => late.resolve(jsonResponse(old)));
    expect(screen.getByLabelText('Reliability publication fingerprint')).toHaveTextContent(PIN);
    expect(screen.queryByRole('alert')).not.toBeInTheDocument();
    expect(screen.getByRole('table')).toHaveTextContent('service-002');
    expect(screen.getByRole('table')).not.toHaveTextContent('service-000');
  });

  it('hides old rows during paging and ignores a late page after a filter interruption', async () => {
    const late = deferred<Response>(); const fetch = mockApi(); fetch.mockImplementation(async (path) => jsonResponse(page(path, 100, true)));
    render(<App />); apply(); await ready();
    fetch.mockReturnValueOnce(late.promise); fireEvent.click(screen.getByRole('button', { name: 'Next windows' }));
    const oldPath = fetch.mock.calls.at(-1)![0];
    expect(screen.queryByRole('table')).not.toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Next windows' })).toBeDisabled();
    expect(screen.getByRole('button', { name: 'Previous windows' })).toBeDisabled();
    fetch.mockImplementation(async (path) => jsonResponse(page(path)));
    input('Exact service ID', 'service-003'); apply(); await ready();
    const obsolete = page(oldPath); obsolete.publication = 'd'.repeat(64);
    await act(async () => late.resolve(jsonResponse(obsolete)));
    expect(screen.queryByRole('alert')).not.toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Previous windows' })).toBeDisabled();
  });

  it('isolates Content and Reliability pins and retains applied filters across navigation', async () => {
    const user = userEvent.setup(); const fetch = mockApi(); render(<App />); apply(); await ready();
    await user.click(productLink('Content Intelligence'));
    await screen.findByRole('heading', { name: 'Publisher breakdown' });
    expect(screen.getByLabelText('Publication fingerprint')).toHaveTextContent(FINGERPRINT);
    await user.click(productLink('Application Reliability')); await ready();
    expect(screen.getByLabelText('Reliability publication fingerprint')).toHaveTextContent(PIN);
    expect(screen.getByRole('textbox', { name: 'Start inclusive (UTC)' })).toHaveValue(START);
    expect(screen.queryByRole('alert')).not.toBeInTheDocument();
    expect(fetch.mock.calls.filter(([path]) => path.startsWith('/api/v1/reliability/'))).toHaveLength(2);
  });

  it('latches publication mismatch until reload, including Operations and Content navigation', async () => {
    const user = userEvent.setup(); const fetch = mockApi(); render(<App />); apply(); await ready();
    const changed = page(); changed.publication = 'd'.repeat(64); fetch.mockResolvedValueOnce(jsonResponse(changed));
    await user.click(screen.getByRole('button', { name: 'Refresh page' }));
    expect(await screen.findByRole('heading', { name: 'Publication changed' })).toBeVisible();
    expect(screen.queryByRole('table')).not.toBeInTheDocument();
    expect(screen.queryByRole('form', { name: 'Reliability window filters' })).not.toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Reload dashboard' })).toBeVisible();
    expect(screen.getByText(RELIABILITY_LABEL)).toBeVisible();
    const reliabilityCount = fetch.mock.calls.filter(([path]) => path.startsWith('/api/v1/reliability/')).length;
    await user.click(workspace('Operations')); await screen.findByRole('heading', { name: 'Registered sources' });
    await user.click(workspace('Analytics')); await screen.findByRole('heading', { name: 'Publisher breakdown' });
    await user.click(productLink('Application Reliability'));
    expect(await screen.findByRole('heading', { name: 'Publication changed' })).toBeVisible();
    expect(screen.queryByLabelText('Reliability publication fingerprint')).not.toBeInTheDocument();
    expect(fetch.mock.calls.filter(([path]) => path.startsWith('/api/v1/reliability/'))).toHaveLength(reliabilityCount);
  });

  it('does not let a latched Content publication change block Reliability', async () => {
    const user = userEvent.setup(); const fetch = mockApi();
    window.history.replaceState(null, '', '/#/analytics'); render(<App />);
    await screen.findByRole('heading', { name: 'Publisher breakdown' });
    fetch.mockResolvedValueOnce(jsonResponse({ ...fixture(), publication: 'd'.repeat(64), data: [fixture().data[0]], meta: { returned: 1, limit: 100 } }));
    await user.selectOptions(screen.getByRole('combobox', { name: 'Publisher scope' }), 'alpha');
    expect(await screen.findByRole('heading', { name: 'Publication changed' })).toBeVisible();
    await user.click(productLink('Application Reliability')); apply(); await ready();
    expect(screen.queryByRole('alert')).not.toBeInTheDocument();
    expect(screen.getByLabelText('Reliability publication fingerprint')).toHaveTextContent(PIN);
    const count = fetch.mock.calls.length;
    await user.click(productLink('Content Intelligence'));
    expect(await screen.findByRole('heading', { name: 'Publication changed' })).toBeVisible();
    expect(fetch).toHaveBeenCalledTimes(count);
  });

  it('cancels a route-interrupted read and ignores it after returning through history events', async () => {
    const late = deferred<Response>(); const fetch = mockApi(); fetch.mockReturnValueOnce(late.promise);
    render(<App />); apply(); const path = fetch.mock.calls[0]![0];
    const signal = (fetch.mock.calls[0] as unknown as [string, RequestInit])[1].signal;
    act(() => { window.history.pushState(null, '', '/#/unknown'); window.dispatchEvent(new PopStateEvent('popstate')); });
    expect(screen.getByRole('heading', { name: 'Page not found' })).toBeVisible();
    expect(signal?.aborted).toBe(true);
    act(() => { window.history.replaceState(null, '', '/#/analytics/reliability'); window.dispatchEvent(new PopStateEvent('popstate')); });
    await ready();
    const obsolete = page(path); obsolete.publication = 'd'.repeat(64);
    await act(async () => late.resolve(jsonResponse(obsolete)));
    expect(screen.queryByRole('alert')).not.toBeInTheDocument();
    expect(screen.getByLabelText('Reliability publication fingerprint')).toHaveTextContent(PIN);
    expect(document.getElementById('main')).toHaveFocus();
  });

  it('retains isolation under StrictMode and supports keyboard form submission', async () => {
    const user = userEvent.setup(); const fetch = mockApi(); render(<StrictMode><App /></StrictMode>);
    input('Start inclusive (UTC)', START); input('End exclusive (UTC)', END);
    screen.getByRole('button', { name: 'Apply window filters' }).focus(); await user.keyboard('{Enter}'); await ready();
    expect(fetch).toHaveBeenCalledTimes(1);
    expect(screen.getByRole('region', { name: /scroll horizontally/ })).toHaveAttribute('tabindex', '0');
    await user.click(productLink('Content Intelligence'));
    await waitFor(() => expect(document.getElementById('main')).toHaveFocus());
  });
});
