import { StrictMode } from 'react';
import { act, render, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, expect, it, vi } from 'vitest';
import { App } from './App';
import { fixture, FINGERPRINT, MICROSECOND_TIME, jsonResponse, deferred } from './test/fixtures';

const ready = () => screen.findByRole('heading', { name: 'Publisher breakdown' });

function summaryValue(label: string) {
  return within(screen.getByRole('region', { name: 'Returned publisher scope' })).getByText(label, { selector: 'dt' }).parentElement?.querySelector('dd')?.textContent;
}

describe('publisher dashboard', () => {
  it('shows loading, then the fingerprint, scoped KPIs, chart, exact table and metric definitions', async () => {
    const pending = deferred<Response>();
    vi.stubGlobal('fetch', vi.fn().mockReturnValue(pending.promise));
    render(<App />);
    expect(screen.getByRole('heading', { name: 'Loading publisher analytics' })).toBeInTheDocument();
    expect(screen.queryByText('Publishers represented')).not.toBeInTheDocument();
    await act(async () => pending.resolve(jsonResponse(fixture())));
    await ready();
    expect(screen.getByLabelText('Publication fingerprint')).toHaveTextContent(FINGERPRINT);
    expect(summaryValue('Publishers represented')).toBe('2');
    expect(summaryValue('Articles')).toBe('8');
    expect(summaryValue('Observations')).toBe('15');
    expect(summaryValue('Without publication date')).toBe('1');
    expect(screen.getByText('Totals for the displayed results')).toBeInTheDocument();
    expect(screen.getByRole('heading', { name: 'Content & observations' })).toBeInTheDocument();
    expect(screen.getByText('Observations are not views, clicks, or user engagement.')).toBeInTheDocument();
    const table = screen.getByRole('table');
    expect(within(table).getByRole('rowheader', { name: 'alpha' })).toBeInTheDocument();
    expect(table.querySelector(`time[datetime="${MICROSECOND_TIME}"]`)).toHaveTextContent('23:59:59.123456 UTC');
    expect(within(table).getAllByText('Not available')).toHaveLength(2);
    expect(screen.getByText('Missing timestamps are not warnings.')).toBeInTheDocument();
  });

  it('filters with sourceId and resets through new API requests, retaining selector choices', async () => {
    const user = userEvent.setup();
    const full = fixture();
    const filtered = { ...full, data: full.data.slice(0, 1), meta: { returned: 1, limit: 100 } };
    const fetch = vi.fn().mockResolvedValueOnce(jsonResponse(full)).mockResolvedValueOnce(jsonResponse(filtered)).mockResolvedValueOnce(jsonResponse(full));
    vi.stubGlobal('fetch', fetch);
    render(<App />);
    await ready();
    await user.selectOptions(screen.getByRole('combobox', { name: 'Publisher scope' }), 'alpha');
    await waitFor(() => expect(summaryValue('Articles')).toBe('3'));
    expect(fetch.mock.calls[1]?.[0]).toBe('/api/v1/publishers?limit=100&sourceId=alpha');
    expect(screen.getByRole('option', { name: 'beta' })).toBeInTheDocument();
    expect(within(screen.getByRole('table')).queryByRole('rowheader', { name: 'beta' })).not.toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'Reset filter' }));
    await waitFor(() => expect(summaryValue('Articles')).toBe('8'));
    expect(fetch.mock.calls[2]?.[0]).toBe('/api/v1/publishers?limit=100');
  });

  it('cancels stale requests and ignores late responses even when a transport ignores abort', async () => {
    const user = userEvent.setup();
    const late = deferred<Response>();
    const full = fixture();
    const beta = { ...full, data: full.data.slice(1), meta: { returned: 1, limit: 100 } };
    const fetch = vi.fn().mockResolvedValueOnce(jsonResponse(full)).mockReturnValueOnce(late.promise).mockResolvedValueOnce(jsonResponse(beta));
    vi.stubGlobal('fetch', fetch);
    render(<App />);
    await ready();
    const selector = screen.getByRole('combobox', { name: 'Publisher scope' });
    await user.selectOptions(selector, 'alpha');
    expect(screen.queryByRole('table')).not.toBeInTheDocument();
    const oldSignal = (fetch.mock.calls[1]?.[1] as RequestInit).signal;
    await user.selectOptions(selector, 'beta');
    expect(oldSignal?.aborted).toBe(true);
    await waitFor(() => expect(summaryValue('Articles')).toBe('5'));
    // A stale different fingerprint must not poison the session or replace beta's results.
    await act(async () => late.resolve(jsonResponse({ ...full, publication: 'b'.repeat(64), data: full.data.slice(0, 1), meta: { returned: 1, limit: 100 } })));
    expect(summaryValue('Articles')).toBe('5');
    expect(screen.getByLabelText('Publication fingerprint')).toHaveTextContent(FINGERPRINT);
    expect(screen.queryByRole('alert')).not.toBeInTheDocument();
  });

  it('aborts on unmount and remains safe under StrictMode effect replay', async () => {
    const pending = deferred<Response>();
    const fetch = vi.fn().mockReturnValue(pending.promise);
    vi.stubGlobal('fetch', fetch);
    const { unmount } = render(<StrictMode><App /></StrictMode>);
    expect(fetch).toHaveBeenCalledTimes(2);
    expect((fetch.mock.calls[0]?.[1] as RequestInit).signal?.aborted).toBe(true);
    unmount();
    expect((fetch.mock.calls[1]?.[1] as RequestInit).signal?.aborted).toBe(true);
    await act(async () => pending.reject(new DOMException('aborted', 'AbortError')));
  });

  it('refuses publication changes without combining or switching data', async () => {
    const user = userEvent.setup();
    const changed = fixture();
    changed.publication = 'b'.repeat(64);
    changed.data = changed.data.slice(0, 1);
    changed.meta.returned = 1;
    vi.stubGlobal('fetch', vi.fn().mockResolvedValueOnce(jsonResponse(fixture())).mockResolvedValueOnce(jsonResponse(changed)));
    render(<App />);
    await ready();
    await user.selectOptions(screen.getByLabelText('Publisher scope'), 'alpha');
    expect(await screen.findByRole('heading', { name: 'Publication changed' })).toBeInTheDocument();
    expect(screen.queryByRole('table')).not.toBeInTheDocument();
    expect(screen.queryByText('Publishers represented')).not.toBeInTheDocument();
    expect(screen.getByLabelText('Publication fingerprint')).toHaveTextContent(FINGERPRINT);
    expect(screen.getByRole('button', { name: 'Reload dashboard' })).toBeInTheDocument();
  });

  it.each([400, 500, 503, 504])('shows a safe HTTP %s error and supports explicit retry', async (status) => {
    const user = userEvent.setup();
    const fetch = vi.fn().mockResolvedValueOnce(jsonResponse({ message: '/private/secret' }, status)).mockResolvedValueOnce(jsonResponse(fixture()));
    vi.stubGlobal('fetch', fetch);
    render(<App />);
    expect(await screen.findByRole('alert')).not.toHaveTextContent('secret');
    expect(screen.queryByRole('table')).not.toBeInTheDocument();
    expect(fetch).toHaveBeenCalledTimes(1);
    await user.click(screen.getByRole('button', { name: 'Try again' }));
    await ready();
    expect(fetch).toHaveBeenCalledTimes(2);
  });

  it('shows network and invalid-response errors without fabricated data', async () => {
    const user = userEvent.setup();
    vi.stubGlobal('fetch', vi.fn().mockRejectedValueOnce(new Error('secret')).mockResolvedValueOnce(jsonResponse({ bad: 'secret' })));
    render(<App />);
    expect(await screen.findByRole('alert')).toHaveTextContent('Cannot reach the content API');
    await user.click(screen.getByRole('button', { name: 'Try again' }));
    await waitFor(() => expect(screen.getByRole('alert')).toHaveTextContent('does not match the publisher contract'));
    expect(screen.queryByText('Publishers represented')).not.toBeInTheDocument();
  });

  it('shows an empty publication, exact zero scoped totals and no chart', async () => {
    const empty = fixture();
    empty.data = [];
    empty.meta.returned = 0;
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(jsonResponse(empty)));
    render(<App />);
    await screen.findByRole('heading', { name: 'No publishers in this scope' });
    expect(summaryValue('Articles')).toBe('0');
    expect(screen.getByLabelText('Publication fingerprint')).toHaveTextContent(FINGERPRINT);
    expect(screen.queryByRole('table')).not.toBeInTheDocument();
    expect(screen.queryByRole('heading', { name: 'Content & observations' })).not.toBeInTheDocument();
  });

  it('shows an empty filtered response and allows reset', async () => {
    const user = userEvent.setup();
    const full = fixture();
    vi.stubGlobal('fetch', vi.fn().mockResolvedValueOnce(jsonResponse(full)).mockResolvedValueOnce(jsonResponse({ ...full, data: [], meta: { returned: 0, limit: 100 } })));
    render(<App />);
    await ready();
    await user.selectOptions(screen.getByLabelText('Publisher scope'), 'alpha');
    await screen.findByRole('heading', { name: 'No publishers in this scope' });
    expect(screen.getByText(/Reset the filter to explore/)).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Reset filter' })).toBeEnabled();
  });

  it('preserves huge exact values and replaces an unsafe chart with an explanation', async () => {
    const large = fixture();
    large.data = large.data.map((row) => ({ ...row, articleCount: '9223372036854775807' }));
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(jsonResponse(large)));
    render(<App />);
    await ready();
    expect(summaryValue('Articles')).toBe('18,446,744,073,709,551,614');
    expect(screen.getByText('Exact values exceed the chart’s safe range')).toBeInTheDocument();
    expect(within(screen.getByRole('table')).getAllByText('9,223,372,036,854,775,807')).toHaveLength(2);
    expect(screen.queryByRole('application')).not.toBeInTheDocument();
  });

  it('labels potentially truncated results and limits the chart without hiding table rows', async () => {
    const bounded = fixture();
    bounded.data = Array.from({ length: 100 }, (_, i) => ({ ...bounded.data[0]!, sourceId: `publisher-${String(i).padStart(3, '0')}` }));
    bounded.meta.returned = 100;
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(jsonResponse(bounded)));
    render(<App />);
    await ready();
    expect(screen.getByText(/Additional publishers may exist/)).toBeInTheDocument();
    expect(screen.getByText(/Showing the first 12 of 100 returned publishers/)).toBeInTheDocument();
    expect(screen.getByText(/Selector includes the first 100/)).toBeInTheDocument();
    expect(within(screen.getByRole('table')).getAllByRole('row')).toHaveLength(101);
    expect(summaryValue('Publishers represented')).toBe('100');
  });

  it('offers labeled keyboard controls, a skip link and a scrollable table region', async () => {
    const user = userEvent.setup();
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(jsonResponse(fixture())));
    render(<App />);
    await ready();
    await user.tab();
    expect(screen.getByRole('link', { name: 'Skip to analytics' })).toHaveFocus();
    await user.tab();
    expect(screen.getByRole('link', { name: 'Analytics' })).toHaveFocus();
    await user.tab();
    expect(screen.getByRole('link', { name: 'Operations' })).toHaveFocus();
    await user.tab();
    expect(screen.getByRole('combobox', { name: 'Publisher scope' })).toHaveFocus();
    expect(screen.getByRole('region', { name: /Publisher analytics table/ })).toHaveAttribute('tabindex', '0');
    expect(screen.getAllByRole('columnheader')).toHaveLength(6);
  });

  it('renders untrusted identifiers as text, never as HTML or links', async () => {
    const value = fixture();
    value.data[0]!.sourceId = '<img src=x onerror=alert(1)>';
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(jsonResponse(value)));
    render(<App />);
    await ready();
    expect(within(screen.getByRole('table')).getByRole('rowheader', { name: '<img src=x onerror=alert(1)>' })).toBeInTheDocument();
    expect(document.querySelector('img')).toBeNull();
    expect(screen.getByRole('option', { name: '<img src=x onerror=alert(1)>' })).toBeDisabled();
  });
});
