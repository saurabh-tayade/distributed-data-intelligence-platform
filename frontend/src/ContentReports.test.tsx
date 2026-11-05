import { act, fireEvent, render, screen, waitFor, within } from '@testing-library/react';
import { beforeEach, expect, it, vi } from 'vitest';
import { App } from './App';
import { contentFixture } from './test/contentFixtures';
import { deferred, jsonResponse } from './test/fixtures';

beforeEach(() => { window.history.replaceState(null, '', '/#/content'); });
function daily() {
  const base = contentFixture();
  return { ...base, filters: { sourceId: null, from: null, to: null }, data: [{ sourceId: 'alpha', discoveryDate: '2026-10-20', discoveredArticleCount: '9007199254740993', articlesMissingReferenceCount: '0' }], meta: { returned: 1, limit: 100, offset: 0, hasMore: false } };
}
function coverage() {
  const base = contentFixture();
  return { ...base, filters: { sourceId: null }, data: [
    { sourceId: 'alpha', referenceStatus: 'MATCHED', referenceUsage: 'USED', publisherName: '<svg onload=alert(1)>', publisherCategory: null, declaredLanguage: null, countryCode: null, articleCount: '1', articlesMissingReferenceCount: '0', missingOptionalAttributes: ['publisher_category', 'declared_language', 'country_code'] },
    { sourceId: 'beta', referenceStatus: 'MATCHED', referenceUsage: 'UNUSED', publisherName: 'Beta', publisherCategory: 'science', declaredLanguage: 'en', countryCode: 'US', articleCount: '0', articlesMissingReferenceCount: '0', missingOptionalAttributes: [] },
    { sourceId: 'gamma', referenceStatus: 'MISSING', referenceUsage: 'ABSENT', publisherName: null, publisherCategory: null, declaredLanguage: null, countryCode: null, articleCount: '1', articlesMissingReferenceCount: '1', missingOptionalAttributes: [] },
  ], meta: { returned: 3, limit: 100, offset: 0, hasMore: false }, totals: { articleCount: '2', matchedArticleCount: '1', missingArticleCount: '1', catalogSourceCount: '2', matchedSourceCount: '1', missingSourceCount: '1', matchedSourcesMissingOptionalAttributesCount: '1', referenceSourceCount: '2', unusedReferenceSourceCount: '1' } };
}
const select = (name: string) => fireEvent.click(screen.getByRole('button', { name }));
const ready = () => screen.findByRole('heading', { name: 'An enriched article' });
function mockReports() {
  const fetch = vi.fn(async (path: string) => jsonResponse(path.includes('/source-discovery-daily?') ? daily() : path.includes('/reference-coverage?') ? coverage() : contentFixture()));
  vi.stubGlobal('fetch', fetch); return fetch;
}
it('offers fixed reports, resets filters, shows exact discovery counts and all coverage semantics', async () => {
  const fetch = mockReports(); render(<App />); await ready();
  select('Daily discovery'); await screen.findByRole('table', { name: 'Daily source discovery' });
  expect(fetch.mock.calls.at(-1)![0]).toBe('/api/v1/content/source-discovery-daily?limit=100&offset=0');
  expect(screen.getByText('9,007,199,254,740,993')).toBeVisible();
  expect(screen.queryByLabelText('Publisher category')).not.toBeInTheDocument();
  const form = screen.getByRole('form', { name: 'Content daily discovery filters' });
  fireEvent.change(within(form).getByLabelText('Discovery from (inclusive UTC)'), { target: { value: '2026-02-30' } }); select('Apply daily discovery filters');
  expect(screen.getByRole('alert')).toHaveTextContent('valid discovery dates'); expect(fetch).toHaveBeenCalledTimes(2);
  select('Reference coverage'); await screen.findByRole('table', { name: 'Publisher reference coverage' });
  expect(fetch.mock.calls.at(-1)![0]).toBe('/api/v1/content/reference-coverage?limit=100&offset=0');
  expect(screen.queryByLabelText('Discovery from (inclusive UTC)')).not.toBeInTheDocument();
  expect(screen.getByText('<svg onload=alert(1)>')).toBeVisible(); expect(screen.getByRole('table').querySelector('svg')).toBeNull();
  expect(screen.getByText('publisher_category, declared_language, country_code')).toBeVisible();
  expect(screen.getByText('Not applicable (reference absent)')).toBeVisible();
  expect(screen.getByRole('region', { name: 'Reference coverage totals' }).querySelectorAll('dt')).toHaveLength(9);
  select('Enriched catalog'); await ready(); expect(fetch.mock.calls.at(-1)![0]).toBe('/api/v1/content/catalog?limit=10&offset=0');
});
it('discards old rows and aborted report responses without replacing or poisoning the current pin', async () => {
  const pending = deferred<Response>();
  const fetch = vi.fn().mockResolvedValueOnce(jsonResponse(contentFixture())).mockReturnValueOnce(pending.promise).mockResolvedValueOnce(jsonResponse(coverage()));
  vi.stubGlobal('fetch', fetch); render(<App />); await ready(); select('Daily discovery');
  expect(screen.queryByRole('heading', { name: 'An enriched article' })).not.toBeInTheDocument();
  select('Reference coverage'); await screen.findByRole('table');
  const stale = daily(); stale.goldPublication = 'e'.repeat(64); await act(async () => pending.resolve(jsonResponse(stale)));
  expect(screen.getByRole('table', { name: 'Publisher reference coverage' })).toBeVisible(); expect(screen.queryByRole('alert')).not.toBeInTheDocument();
});
it.each(['publication', 'goldPublication', 'referenceId', 'revision', 'snapshotFingerprint'])('permanently blocks all reports for changed %s across workspace navigation', async (field) => {
  const changed = daily();
  if (field === 'publication' || field === 'goldPublication') changed[field] = 'e'.repeat(64);
  else changed.reference[field as keyof typeof changed.reference] = field === 'snapshotFingerprint' ? 'e'.repeat(64) : 'other';
  const fetch = vi.fn().mockResolvedValueOnce(jsonResponse(contentFixture())).mockResolvedValueOnce(jsonResponse(changed)); vi.stubGlobal('fetch', fetch);
  render(<App />); await ready(); select('Daily discovery'); await screen.findByRole('heading', { name: 'Publication changed' });
  expect(screen.queryByRole('table')).not.toBeInTheDocument(); expect(screen.getByRole('button', { name: 'Reference coverage' })).toBeDisabled();
  act(() => { window.location.hash = '#/analytics/reliability'; window.dispatchEvent(new HashChangeEvent('hashchange')); });
  act(() => { window.location.hash = '#/content'; window.dispatchEvent(new HashChangeEvent('hashchange')); });
  expect(screen.getByRole('heading', { name: 'Publication changed' })).toBeVisible(); expect(fetch).toHaveBeenCalledTimes(2);
});
it('handles report failures safely, retries, and preserves the pin', async () => {
  const fetch = vi.fn().mockResolvedValueOnce(jsonResponse(contentFixture())).mockResolvedValueOnce(new Response('private stack trace', { status: 503 })).mockResolvedValueOnce(jsonResponse(daily()));
  vi.stubGlobal('fetch', fetch); render(<App />); await ready(); select('Daily discovery'); await screen.findByRole('alert');
  expect(screen.getByRole('alert')).not.toHaveTextContent('private stack'); expect(screen.queryByRole('table')).not.toBeInTheDocument();
  select('Try again'); await screen.findByRole('table');
});
it('pages discovery, clears stale rows, resets offset on filter changes, and renders empty coverage totals', async () => {
  const page = daily(); page.data = Array.from({ length: 100 }, (_, i) => ({ ...page.data[0]!, sourceId: `s${String(i).padStart(3, '0')}` })); page.meta.returned = 100; page.meta.hasMore = true;
  const pending = deferred<Response>(); const empty = coverage(); empty.data = []; empty.meta.returned = 0; empty.totals = Object.fromEntries(Object.keys(empty.totals).map((key) => [key, '0'])) as typeof empty.totals;
  const fetch = vi.fn().mockResolvedValueOnce(jsonResponse(contentFixture())).mockResolvedValueOnce(jsonResponse(page)).mockReturnValueOnce(pending.promise).mockResolvedValueOnce(jsonResponse(empty));
  vi.stubGlobal('fetch', fetch); render(<App />); await ready(); select('Daily discovery'); await screen.findByRole('table'); select('Next daily discovery page');
  await waitFor(() => expect(fetch).toHaveBeenCalledTimes(3)); expect(fetch.mock.calls[2]![0]).toContain('offset=100'); expect(screen.queryByRole('table')).not.toBeInTheDocument();
  select('Reference coverage'); await screen.findByRole('heading', { name: 'No rows in this reference coverage page' });
  expect(screen.getByRole('region', { name: 'Reference coverage totals' })).toBeVisible(); expect(screen.getByRole('button', { name: 'Next reference coverage page' })).toBeDisabled();
  await act(async () => pending.resolve(jsonResponse({ invalid: true })));
  expect(screen.queryByRole('alert')).not.toBeInTheDocument();
});
it('sends only daily filters with inclusive UTC bounds and clears them for coverage', async () => {
  const fetch = vi.fn(async (path: string) => {
    if (path.includes('/reference-coverage?')) return jsonResponse(coverage());
    if (!path.includes('/source-discovery-daily?')) return jsonResponse(contentFixture());
    const result = daily(), params = new URL(path, 'http://localhost').searchParams;
    result.filters = { sourceId: params.get('sourceId'), from: params.get('from'), to: params.get('to') } as typeof result.filters;
    return jsonResponse(result);
  });
  vi.stubGlobal('fetch', fetch); render(<App />); await ready(); select('Daily discovery'); await screen.findByRole('table');
  const form = screen.getByRole('form', { name: 'Content daily discovery filters' });
  fireEvent.change(within(form).getByLabelText('Exact source ID'), { target: { value: 'alpha' } });
  for (const label of ['Discovery from (inclusive UTC)', 'Discovery to (inclusive UTC)']) fireEvent.change(within(form).getByLabelText(label), { target: { value: '2026-10-20' } });
  select('Apply daily discovery filters'); await screen.findByRole('table');
  expect(fetch.mock.calls.at(-1)![0]).toBe('/api/v1/content/source-discovery-daily?limit=100&offset=0&sourceId=alpha&from=2026-10-20&to=2026-10-20');
  select('Reference coverage'); await screen.findByRole('table'); expect(fetch.mock.calls.at(-1)![0]).toBe('/api/v1/content/reference-coverage?limit=100&offset=0');
});
it('keeps whole-domain coverage totals on later pages and revalidates on workspace return', async () => {
  const fetch = vi.fn(async (path: string) => {
    if (!path.includes('/reference-coverage?')) return jsonResponse(contentFixture());
    const params = new URL(path, 'http://localhost').searchParams, limit = Number(params.get('limit')), offset = Number(params.get('offset'));
    const result = coverage(); result.data = Array.from({ length: 11 }, (_, i) => ({ ...result.data[1]!, sourceId: `unused${String(i).padStart(2, '0')}` })).slice(offset, offset + limit);
    result.totals = { articleCount: '0', matchedArticleCount: '0', missingArticleCount: '0', catalogSourceCount: '0', matchedSourceCount: '0', missingSourceCount: '0', matchedSourcesMissingOptionalAttributesCount: '0', referenceSourceCount: '11', unusedReferenceSourceCount: '11' };
    result.meta = { returned: result.data.length, limit, offset, hasMore: offset + limit < 11 }; return jsonResponse(result);
  });
  vi.stubGlobal('fetch', fetch); render(<App />); await ready(); select('Reference coverage'); await screen.findByRole('table');
  fireEvent.change(screen.getByLabelText('Rows per page'), { target: { value: '10' } }); select('Apply reference coverage filters'); await screen.findByRole('table'); select('Next reference coverage page'); await screen.findByRole('table');
  expect(screen.getByRole('table')).toHaveTextContent('unused10'); expect(screen.getByRole('table')).not.toHaveTextContent('unused00');
  const totals = screen.getByRole('region', { name: 'Reference coverage totals' }); expect(within(totals).getAllByText('11')).toHaveLength(2);
  expect(fetch.mock.calls.at(-1)![0]).toContain('offset=10'); expect(screen.getByRole('button', { name: 'Next reference coverage page' })).toBeDisabled();
  const before = fetch.mock.calls.length;
  act(() => { window.location.hash = '#/analytics/reliability'; window.dispatchEvent(new HashChangeEvent('hashchange')); });
  act(() => { window.location.hash = '#/content'; window.dispatchEvent(new HashChangeEvent('hashchange')); });
  await screen.findByRole('table'); expect(fetch).toHaveBeenCalledTimes(before + 1); expect(fetch.mock.calls.at(-1)![0]).toContain('offset=10');
});
