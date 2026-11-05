import { StrictMode } from 'react';
import { act, fireEvent, render, screen, waitFor, within } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { App } from './App';
import { ContentCatalogRows } from './ContentWorkspace';
import { parseWorkspaceRoute } from './navigation';
import type { ContentQuery, ContentResponse } from './contentApi';
import { contentFixture, contentRow } from './test/contentFixtures';
import { deferred, fixture, jsonResponse } from './test/fixtures';

beforeEach(() => { window.history.replaceState(null, '', '/#/content'); });
function navigate(hash: string) { act(() => { window.location.hash = hash; window.dispatchEvent(new HashChangeEvent('hashchange')); }); }
function fromPath(path: string): ContentResponse {
  const search = new URL(path, 'http://localhost').searchParams;
  const query = Object.fromEntries(search) as ContentQuery;
  query.limit = Number(search.get('limit')); query.offset = Number(search.get('offset'));
  return contentFixture(query);
}
function mockApi() {
  const fetch = vi.fn(async (path: string) => jsonResponse(path.startsWith('/api/v1/content/') ? fromPath(path) : fixture()));
  vi.stubGlobal('fetch', fetch); return fetch;
}
const ready = () => screen.findByRole('heading', { name: 'An enriched article' });
const filters = () => screen.getByRole('form', { name: 'Content catalog filters' });
const apply = () => fireEvent.click(screen.getByRole('button', { name: 'Apply catalog filters' }));

describe('Enriched Content catalog route', () => {
  it('fetches only Content, exposes all identities and explicit scope', async () => {
    const fetch = mockApi(); render(<App />); await ready();
    expect(fetch).toHaveBeenCalledTimes(1); expect(fetch.mock.calls[0]![0]).toBe('/api/v1/content/catalog?limit=10&offset=0');
    expect(parseWorkspaceRoute('#/content')).toBe('content'); expect(parseWorkspaceRoute('#/content?extra=1')).toBe('not-found');
    expect(screen.getByRole('navigation', { name: 'Analytics products' })).toHaveTextContent('Content Intelligence');
    expect(screen.getByRole('link', { name: 'Enriched Content catalog' })).toHaveAttribute('aria-current', 'page');
    const identities = screen.getByRole('region', { name: 'Content selection identities' });
    for (const value of ['b'.repeat(64), 'c'.repeat(64), 'd'.repeat(64), 'publishers', 'r1']) expect(identities).toHaveTextContent(value);
    expect(screen.getByText(/not readership/)).toBeVisible(); expect(screen.getByText(/selected history, not original publication/)).toBeVisible();
    expect(screen.getByText('9,007,199,254,740,993')).toBeVisible(); expect(document.title).toBe('Enriched Content catalog | DDIP');
  });
  it('does not load hidden Content from Gold or Reliability routes', async () => {
    navigate('#/analytics'); const fetch = mockApi(); render(<App />); await screen.findByRole('heading', { name: 'Publisher analytics.' });
    expect(fetch.mock.calls.every(([path]) => !path.startsWith('/api/v1/content/'))).toBe(true);
    navigate('#/analytics/reliability'); expect(screen.getByRole('heading', { name: 'Choose a UTC window interval' })).toBeVisible();
    expect(fetch.mock.calls.every(([path]) => !path.startsWith('/api/v1/content/'))).toBe(true);
  });
  it('validates filters before fetching and uses fixed native vocabularies', async () => {
    const fetch = mockApi(); render(<App />); await ready();
    fireEvent.change(within(filters()).getByLabelText('Exact source ID'), { target: { value: 'Bad Source' } }); apply();
    expect(screen.getByRole('alert')).toHaveTextContent('Source ID'); expect(fetch).toHaveBeenCalledTimes(1);
    fireEvent.change(within(filters()).getByLabelText('Exact source ID'), { target: { value: 'beta' } }); apply();
    await waitFor(() => expect(fetch).toHaveBeenCalledTimes(2)); await ready();
    expect(fetch.mock.calls[1]![0]).toContain('sourceId=beta'); expect(screen.getByText('Applied filters: sourceId: beta')).toBeVisible();
    expect(within(filters()).getByLabelText('Publisher category')).toHaveTextContent('general-news');
    expect(within(filters()).getByLabelText('Articles per page').querySelectorAll('option')).toHaveLength(3);
  });
  it('clears old rows on filter and page changes and resets offset', async () => {
    const next = deferred<Response>();
    const fetch = vi.fn().mockImplementationOnce(async () => {
      const response = contentFixture(); response.data = Array.from({ length: 10 }, (_, i) => contentRow({ articleId: String(i).repeat(64), title: `Page one ${i}` })); response.meta.returned = 10; response.meta.hasMore = true; return jsonResponse(response);
    }).mockReturnValueOnce(next.promise).mockImplementation(async (path: string) => jsonResponse(fromPath(path)));
    vi.stubGlobal('fetch', fetch); render(<App />); await screen.findByRole('heading', { name: 'Page one 0' });
    fireEvent.click(screen.getByRole('button', { name: 'Next catalog page' }));
    expect(screen.queryByRole('heading', { name: 'Page one 0' })).not.toBeInTheDocument(); expect(fetch.mock.calls[1]![0]).toContain('offset=10');
    fireEvent.change(within(filters()).getByLabelText('Exact source ID'), { target: { value: 'beta' } }); apply(); await ready();
    expect(fetch.mock.calls[2]![0]).toContain('offset=0');
    const obsolete = contentFixture({ offset: 10 }); obsolete.publication = 'e'.repeat(64);
    await act(async () => next.resolve(jsonResponse(obsolete)));
    expect(screen.queryByRole('heading', { name: 'Publication changed' })).not.toBeInTheDocument(); expect(screen.getByRole('heading', { name: 'An enriched article' })).toBeVisible();
  });
  it.each(['publication', 'goldPublication', 'referenceId', 'revision', 'snapshotFingerprint'])('latches changed %s until reload, including navigation away and back', async (field) => {
    const changed = contentFixture();
    if (field === 'publication' || field === 'goldPublication') changed[field] = 'e'.repeat(64);
    else {
      const key = field as 'referenceId' | 'revision' | 'snapshotFingerprint'; changed.reference[key] = key === 'snapshotFingerprint' ? 'e'.repeat(64) : 'other';
      Object.assign(changed.data[0]!, { referenceId: changed.reference.referenceId, referenceRevision: changed.reference.revision, referenceFingerprint: changed.reference.snapshotFingerprint });
    }
    const fetch = vi.fn().mockResolvedValueOnce(jsonResponse(contentFixture())).mockResolvedValueOnce(jsonResponse(changed)); vi.stubGlobal('fetch', fetch);
    render(<App />); await ready(); fireEvent.click(screen.getByRole('button', { name: 'Refresh catalog page' }));
    await screen.findByRole('heading', { name: 'Publication changed' }); expect(screen.queryByRole('heading', { name: 'An enriched article' })).not.toBeInTheDocument();
    expect(screen.queryByRole('form', { name: 'Content catalog filters' })).not.toBeInTheDocument();
    navigate('#/analytics/reliability'); navigate('#/content'); expect(screen.getByRole('heading', { name: 'Publication changed' })).toBeVisible(); expect(fetch).toHaveBeenCalledTimes(2);
  });
  it('hides prior rows on navigation re-entry until fresh verification', async () => {
    const pending = deferred<Response>(); const fetch = vi.fn().mockResolvedValueOnce(jsonResponse(contentFixture())).mockReturnValueOnce(pending.promise); vi.stubGlobal('fetch', fetch);
    render(<App />); await ready(); navigate('#/analytics/reliability'); navigate('#/content');
    expect(screen.queryByRole('heading', { name: 'An enriched article' })).not.toBeInTheDocument();
    expect(screen.getByRole('heading', { name: 'Loading Content catalog' })).toBeVisible();
    expect(screen.getByRole('region', { name: 'Content selection identities' })).toHaveTextContent('b'.repeat(64));
    await act(async () => pending.resolve(jsonResponse(contentFixture()))); await ready();
  });
  it('ignores an aborted hidden completion, including an invalid response', async () => {
    const first = deferred<Response>(); const fetch = vi.fn().mockReturnValueOnce(first.promise).mockResolvedValueOnce(jsonResponse(contentFixture())); vi.stubGlobal('fetch', fetch);
    render(<App />); navigate('#/analytics/reliability'); await act(async () => first.resolve(jsonResponse({ secret: 'invalid' })));
    navigate('#/content'); await ready(); expect(screen.queryByRole('alert')).not.toBeInTheDocument();
  });
  it('ignores StrictMode abandoned requests and permits retry after an error', async () => {
    const abandoned = deferred<Response>(); const fetch = vi.fn().mockReturnValueOnce(abandoned.promise).mockResolvedValueOnce(new Response('private', { status: 503 })).mockResolvedValueOnce(jsonResponse(contentFixture())); vi.stubGlobal('fetch', fetch);
    render(<StrictMode><App /></StrictMode>); await screen.findByRole('alert'); expect(screen.getByRole('alert')).not.toHaveTextContent('private');
    fireEvent.click(screen.getByRole('button', { name: 'Try again' })); await ready();
    const obsolete = contentFixture(); obsolete.goldPublication = 'e'.repeat(64); await act(async () => abandoned.resolve(jsonResponse(obsolete)));
    expect(screen.queryByRole('alert')).not.toBeInTheDocument();
  });
  it('shows empty pages without invented totals or articles', async () => {
    const response = contentFixture(); response.data = []; response.meta.returned = 0;
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(jsonResponse(response))); render(<App />);
    await screen.findByRole('heading', { name: 'No articles in this catalog scope' }); expect(screen.getByRole('button', { name: 'Next catalog page' })).toBeDisabled();
  });
  it('renders untrusted values only as text and rejects unsafe navigation', () => {
    const response = contentFixture(); Object.assign(response.data[0]!, { title: '<img src=x>', summary: '<script>alert(1)</script>', publisherName: '<svg onload=alert(1)>', url: 'javascript:alert(1)' });
    const { rerender } = render(<ContentCatalogRows response={response} />);
    expect(screen.getByText('<img src=x>')).toBeVisible(); expect(screen.getByText('<script>alert(1)</script>')).toBeVisible();
    expect(document.querySelector('img,script,iframe,svg')).toBeNull(); expect(screen.queryByRole('link')).not.toBeInTheDocument();
    response.data[0]!.url = 'https://example.com/article'; rerender(<ContentCatalogRows response={response} />);
    expect(screen.getByRole('link')).toHaveAttribute('rel', 'noopener noreferrer');
  });
});
