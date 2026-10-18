import { act, fireEvent, render, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, expect, it, vi } from 'vitest';
import { App } from './App';
import ArticleExplorer from './views/ArticleExplorer';
import { MAX_OFFSET } from './api';
import { article, articlesFixture, dailyFixture, qualityFixture } from './test/analyticsFixtures';
import { deferred, fixture, FINGERPRINT, jsonResponse } from './test/fixtures';

const publisherReady = () => screen.findByRole('heading', { name: 'Publisher breakdown' });
const articleReady = () => screen.findByRole('list', { name: 'Articles in API order' });
const nav = (name: string) => within(screen.getByRole('navigation', { name: 'Analytics views' })).getByRole('button', { name });

function apiResponse(path: string): Response {
  const url = new URL(path, 'http://localhost');
  const source = url.searchParams.get('sourceId');
  const endpoint = url.pathname.split('/').at(-1);
  if (endpoint === 'articles') return jsonResponse(articlesFixture(Number(url.searchParams.get('offset') || 0), source === 'beta' ? 4 : 23, source || 'alpha'));
  const body = endpoint === 'daily' ? dailyFixture() : endpoint === 'quality' ? qualityFixture() : fixture();
  if (source) body.data = body.data.filter((row) => row.sourceId === source) as typeof body.data;
  if (endpoint === 'daily') {
    const daily = body as ReturnType<typeof dailyFixture>;
    daily.data = daily.data.filter((row) => (!url.searchParams.get('from') || row.ingestionDate >= url.searchParams.get('from')!)
      && (!url.searchParams.get('to') || row.ingestionDate <= url.searchParams.get('to')!));
  }
  body.meta.returned = body.data.length;
  return jsonResponse(body);
}
function mockApi() {
  const fetch = vi.fn().mockImplementation((path: string) => Promise.resolve(apiResponse(path)));
  vi.stubGlobal('fetch', fetch);
  return fetch;
}
function summaryValue(region: string, name: string) {
  const term = within(screen.getByRole('region', { name: region })).getByText(name, { selector: 'dt' });
  return term.parentElement?.querySelector('dd')?.textContent;
}

describe('complete analytics application', () => {
  it('loads one view at a time with a shared identity and preserves publisher navigation', async () => {
    const user = userEvent.setup(); const fetch = mockApi(); render(<App />);
    await publisherReady();
    expect(fetch).toHaveBeenCalledTimes(1);
    expect(nav('Publisher Overview')).toHaveAttribute('aria-current', 'page');
    await user.click(nav('Daily Activity'));
    await screen.findByRole('heading', { name: 'Daily breakdown' });
    expect(fetch).toHaveBeenCalledTimes(2);
    expect(summaryValue('Returned daily scope', 'Observations')).toBe('10');
    expect(summaryValue('Returned daily scope', 'Warning codes')).toBe('9');
    expect(screen.getByText(/They are not summed into global distinct totals/)).toBeInTheDocument();
    expect(within(screen.getByRole('table')).queryByText('2026-10-13')).not.toBeInTheDocument();
    await user.click(nav('Data Quality'));
    await screen.findByRole('heading', { name: 'Quality breakdown' });
    expect(fetch).toHaveBeenCalledTimes(3);
    expect(within(screen.getByRole('table')).getByText('2')).toBeInTheDocument();
    expect(screen.getByText(/This is not a percentage and can exceed 1.0/)).toBeInTheDocument();
    expect(summaryValue('Returned quality scope', 'Warning codes')).toBe('9');
    expect(screen.getByLabelText('Publication fingerprint')).toHaveTextContent(FINGERPRINT);
    await user.click(nav('Publisher Overview'));
    await publisherReady();
    expect(fetch).toHaveBeenCalledTimes(4);
  });

  it('applies inclusive daily date/source filters on submission and shares dates with quality only', async () => {
    const user = userEvent.setup(); const fetch = mockApi(); render(<App />); await publisherReady();
    await user.click(nav('Daily Activity')); await screen.findByRole('heading', { name: 'Daily breakdown' });
    const before = fetch.mock.calls.length;
    await user.type(screen.getByLabelText('From date (UTC)'), '2026-10-12');
    await user.type(screen.getByLabelText('To date (UTC)'), '2026-10-12');
    expect(fetch).toHaveBeenCalledTimes(before);
    await user.click(screen.getByRole('button', { name: 'Apply dates' }));
    await waitFor(() => expect(screen.getAllByRole('row')).toHaveLength(3));
    expect(fetch.mock.calls.at(-1)?.[0]).toBe('/api/v1/daily?limit=100&from=2026-10-12&to=2026-10-12');
    await user.selectOptions(screen.getByLabelText('Publisher scope'), 'beta');
    await waitFor(() => expect(screen.getAllByRole('row')).toHaveLength(2));
    expect(fetch.mock.calls.at(-1)?.[0]).toContain('sourceId=beta&from=2026-10-12&to=2026-10-12');
    await user.click(nav('Data Quality')); await screen.findByRole('heading', { name: 'Quality breakdown' });
    expect(fetch.mock.calls.at(-1)?.[0]).toBe('/api/v1/quality?limit=100&sourceId=beta&from=2026-10-12&to=2026-10-12');
    await user.click(screen.getByRole('button', { name: 'Clear dates' }));
    await waitFor(() => expect(fetch.mock.calls.at(-1)?.[0]).toBe('/api/v1/quality?limit=100&sourceId=beta'));
    await user.type(screen.getByLabelText('From date (UTC)'), '2026-10-14');
    await user.click(screen.getByRole('button', { name: 'Apply dates' }));
    await screen.findByRole('heading', { name: 'Quality breakdown' });
    await user.click(nav('Article Explorer')); await articleReady();
    expect(fetch.mock.calls.at(-1)?.[0]).toBe('/api/v1/articles?limit=10&sourceId=beta&offset=0');
    expect(screen.queryByLabelText('From date (UTC)')).not.toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Next' })).toBeDisabled();
  });

  it.each(['Daily Activity', 'Data Quality'])('rejects invalid or reversed dates in %s without requesting new data', async (view) => {
    const user = userEvent.setup(); const fetch = mockApi(); render(<App />); await publisherReady();
    await user.click(nav(view)); await screen.findByRole('table');
    const count = fetch.mock.calls.length;
    await user.type(screen.getByLabelText('From date (UTC)'), '2026-02-30');
    await user.click(screen.getByRole('button', { name: 'Apply dates' }));
    expect(screen.getByRole('alert')).toHaveTextContent('Use valid UTC dates');
    expect(fetch).toHaveBeenCalledTimes(count);
    await user.clear(screen.getByLabelText('From date (UTC)'));
    await user.type(screen.getByLabelText('From date (UTC)'), '2026-10-14');
    await user.type(screen.getByLabelText('To date (UTC)'), '2026-10-12');
    await user.click(screen.getByRole('button', { name: 'Apply dates' }));
    expect(screen.getByRole('alert')).toHaveTextContent('on or before');
    expect(fetch).toHaveBeenCalledTimes(count);
    expect(screen.getByText('No start bound', { selector: 'strong' })).toBeInTheDocument();
  });

  it('traverses article first, middle and final pages through the API and resets offset on source change', async () => {
    const user = userEvent.setup(); const fetch = mockApi(); render(<App />); await publisherReady();
    await user.click(nav('Article Explorer')); await articleReady();
    expect(within(await articleReady()).getAllByRole('listitem')).toHaveLength(10);
    expect(screen.getByRole('button', { name: 'Previous' })).toBeDisabled();
    expect(screen.getByRole('heading', { name: /Article 1 \(/ })).toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'Next' }));
    await screen.findByRole('heading', { name: /Article 11 \(/ });
    expect(fetch.mock.calls.at(-1)?.[0]).toBe('/api/v1/articles?limit=10&offset=10');
    await user.click(screen.getByRole('button', { name: 'Next' }));
    await screen.findByRole('heading', { name: /Article 21 \(/ });
    expect(within(await articleReady()).getAllByRole('listitem')).toHaveLength(3);
    expect(screen.getByRole('button', { name: 'Next' })).toBeDisabled();
    expect(screen.queryByText(/total pages/i)).not.toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'Previous' }));
    await screen.findByRole('heading', { name: /Article 11 \(/ });
    await user.selectOptions(screen.getByLabelText('Publisher scope'), 'beta');
    await screen.findByRole('heading', { name: /Article 1 \(/ });
    expect(fetch.mock.calls.at(-1)?.[0]).toBe('/api/v1/articles?limit=10&sourceId=beta&offset=0');
    expect(screen.getByRole('button', { name: 'Previous' })).toBeDisabled();
    await user.click(screen.getByRole('button', { name: 'Reset filter' })); await articleReady();
    expect(fetch.mock.calls.at(-1)?.[0]).toBe('/api/v1/articles?limit=10&offset=0');
  });

  it('hides obsolete article pages and cancels late responses even across source changes', async () => {
    const user = userEvent.setup(); const late = deferred<Response>();
    const fetch = mockApi(); fetch.mockImplementation((path: string) => path.endsWith('offset=10') ? late.promise : Promise.resolve(apiResponse(path)));
    render(<App />); await publisherReady(); await user.click(nav('Article Explorer')); await articleReady();
    await user.click(screen.getByRole('button', { name: 'Next' }));
    expect(screen.queryByRole('list', { name: 'Articles in API order' })).not.toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Next' })).toBeDisabled();
    expect(screen.getByRole('button', { name: 'Previous' })).toBeDisabled();
    const signal = (fetch.mock.calls.at(-1)?.[1] as RequestInit).signal;
    await user.selectOptions(screen.getByLabelText('Publisher scope'), 'beta'); await articleReady();
    expect(signal?.aborted).toBe(true);
    await act(async () => late.resolve(jsonResponse({ ...articlesFixture(10), publication: 'b'.repeat(64) })));
    expect(screen.queryByRole('alert')).not.toBeInTheDocument();
    expect(screen.getByLabelText('Publication fingerprint')).toHaveTextContent(FINGERPRINT);
    expect(screen.getByRole('button', { name: 'Previous' })).toBeDisabled();
  });

  it('ignores a stale cross-view identity change and pins the successful current response', async () => {
    const user = userEvent.setup(); const late = deferred<Response>(); const fetch = mockApi();
    fetch.mockImplementation((path: string) => path.includes('/daily?') ? late.promise : Promise.resolve(apiResponse(path)));
    render(<App />); await publisherReady(); await user.click(nav('Daily Activity'));
    const signal = (fetch.mock.calls.at(-1)?.[1] as RequestInit).signal;
    await user.click(nav('Data Quality')); await screen.findByRole('heading', { name: 'Quality breakdown' });
    expect(signal?.aborted).toBe(true);
    await act(async () => late.resolve(jsonResponse({ ...dailyFixture(), publication: 'b'.repeat(64) })));
    expect(screen.queryByRole('alert')).not.toBeInTheDocument();
    expect(screen.getByLabelText('Publication fingerprint')).toHaveTextContent(FINGERPRINT);
    expect(screen.getByRole('heading', { name: 'Quality breakdown' })).toBeInTheDocument();
  });

  it.each(['Daily Activity', 'Data Quality', 'Article Explorer'])('latches a current identity change in %s across every subsequent navigation', async (view) => {
    const user = userEvent.setup(); const fetch = mockApi();
    render(<App />); await publisherReady();
    fetch.mockImplementation(async (path: string) => {
      const body: unknown = await apiResponse(path).json();
      return jsonResponse({ ...(body as object), publication: 'b'.repeat(64) });
    });
    await user.click(nav(view)); await screen.findByRole('heading', { name: 'Publication changed' });
    const count = fetch.mock.calls.length;
    expect(screen.queryByRole('table')).not.toBeInTheDocument();
    expect(screen.queryByRole('list', { name: 'Articles in API order' })).not.toBeInTheDocument();
    await user.click(nav('Publisher Overview'));
    expect(screen.getByRole('heading', { name: 'Publication changed' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Reload dashboard' })).toBeInTheDocument();
    expect(screen.getByLabelText('Publication fingerprint')).toHaveTextContent(FINGERPRINT);
    expect(fetch).toHaveBeenCalledTimes(count);
    expect(screen.queryByText('Publishers represented')).not.toBeInTheDocument();
  });

  it('can establish the pin from a new view when the initial publisher request was cancelled', async () => {
    const user = userEvent.setup(); const late = deferred<Response>(); const fetch = mockApi();
    fetch.mockImplementation((path: string) => path.includes('/publishers?') ? late.promise : Promise.resolve(apiResponse(path)));
    render(<App />); await user.click(nav('Daily Activity')); await screen.findByRole('heading', { name: 'Daily breakdown' });
    expect(screen.getByLabelText('Publication fingerprint')).toHaveTextContent(FINGERPRINT);
    expect(screen.getByRole('option', { name: 'alpha' })).toBeInTheDocument();
    await act(async () => late.resolve(jsonResponse({ ...fixture(), publication: 'b'.repeat(64) })));
    expect(screen.queryByRole('alert')).not.toBeInTheDocument();
  });

  it.each(['Daily Activity', 'Data Quality', 'Article Explorer'])('provides a safe error and explicit retry for %s', async (view) => {
    const user = userEvent.setup(); const fetch = mockApi(); render(<App />); await publisherReady();
    fetch.mockResolvedValueOnce(jsonResponse({ code: 'ANALYTICS_CAPACITY_EXCEEDED', message: '/secret' }, 503));
    await user.click(nav(view));
    expect(await screen.findByRole('alert')).toHaveTextContent('busy');
    expect(screen.getByRole('alert')).not.toHaveTextContent('secret');
    expect(screen.queryByRole('table')).not.toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'Try again' }));
    if (view === 'Article Explorer') await articleReady(); else await screen.findByRole('table');
    expect(screen.queryByRole('alert')).not.toBeInTheDocument();
  });

  it.each(['Daily Activity', 'Data Quality', 'Article Explorer'])('shows a genuine empty result in %s', async (view) => {
    const user = userEvent.setup(); const fetch = mockApi(); render(<App />); await publisherReady();
    const body = view === 'Daily Activity' ? dailyFixture() : view === 'Data Quality' ? qualityFixture() : articlesFixture(0, 0);
    body.data = []; body.meta.returned = 0;
    fetch.mockResolvedValueOnce(jsonResponse(body));
    await user.click(nav(view)); await screen.findByRole('heading', { name: /No (daily rows|quality rows|articles) in this scope/ });
    expect(screen.queryByRole('table')).not.toBeInTheDocument();
    expect(screen.queryByRole('application')).not.toBeInTheDocument();
  });

  it('represents a single date faithfully and hides unsafe grouped chart numbers without losing exact counts', async () => {
    const user = userEvent.setup(); const fetch = mockApi(); render(<App />); await publisherReady();
    const body = dailyFixture(); body.data = [body.data[0]!]; body.meta.returned = 1;
    fetch.mockResolvedValueOnce(jsonResponse(body));
    await user.click(nav('Daily Activity')); await screen.findByRole('table');
    expect(screen.getByText(/One UTC ingestion date is represented/)).toBeInTheDocument();
    await user.click(nav('Publisher Overview')); await publisherReady();
    body.data[0]!.observationCount = '9223372036854775807';
    fetch.mockResolvedValueOnce(jsonResponse(body));
    await user.click(nav('Daily Activity')); await screen.findByRole('table');
    expect(screen.getByText('Exact values exceed the chart’s safe range')).toBeInTheDocument();
    expect(summaryValue('Returned daily scope', 'Observations')).toBe('9,223,372,036,854,775,807');
    expect(within(screen.getByRole('table')).getByText('9,223,372,036,854,775,807')).toBeInTheDocument();
  });

  it('labels capped daily results and keeps every row in the text alternative', async () => {
    const user = userEvent.setup(); const fetch = mockApi(); render(<App />); await publisherReady();
    const body = dailyFixture();
    body.data = Array.from({ length: 100 }, (_, i) => {
      const date = new Date(Date.UTC(2026, 0, i + 1)).toISOString().slice(0, 10);
      return { ...body.data[0]!, ingestionDate: date, latestIngestedAt: `${date}T00:00:00.000001Z` };
    }); body.meta.returned = 100;
    fetch.mockResolvedValueOnce(jsonResponse(body));
    await user.click(nav('Daily Activity')); await screen.findByRole('table');
    expect(screen.getByText(/Additional records may exist/)).toBeInTheDocument();
    expect(screen.getByText(/Showing the first 30 of 100 returned dates/)).toBeInTheDocument();
    expect(screen.getAllByRole('row')).toHaveLength(101);
  });

  it('supports keyboard navigation and native expandable article details', async () => {
    const user = userEvent.setup(); mockApi(); render(<App />); await publisherReady();
    nav('Article Explorer').focus(); await user.keyboard('{Enter}'); await articleReady();
    const detail = screen.getAllByText('Article details', { selector: 'summary' })[0]!;
    await user.click(detail);
    expect(detail.closest('details')).toHaveAttribute('open');
    expect(screen.getByText('Plain summary 1')).toBeVisible();
    expect(screen.getByRole('link', { name: 'Article 1 (opens in a new tab)' })).toHaveAttribute('rel', 'noopener noreferrer');
  });
});

describe('article rendering and page boundaries', () => {
  it('renders unsafe/null URLs and HTML-shaped or very long metadata as text', async () => {
    const user = userEvent.setup(); const response = articlesFixture(0, 4);
    const title = '<img src=x onerror=alert(1)>';
    Object.assign(response.data[0]!, { title, summary: '<script>not trusted</script>', url: 'javascript:alert(1)' });
    Object.assign(response.data[1]!, { url: 'data:text/html,unsafe', title: 'Data URL title' });
    Object.assign(response.data[2]!, { url: null, title: 'Null URL title' });
    const long = 'long-unbroken-text'.repeat(1000);
    Object.assign(response.data[3]!, { url: 'not a URL', title: long, summary: long });
    render(<ArticleExplorer response={response} offset={0} limit={10} loading={false} onPage={vi.fn()} />);
    expect(screen.queryByRole('link')).not.toBeInTheDocument();
    expect(screen.getByRole('heading', { name: title })).toBeInTheDocument();
    expect(document.querySelector('img, script, iframe')).toBeNull();
    await user.click(screen.getAllByText('Article details', { selector: 'summary' })[0]!);
    expect(screen.getByText('<script>not trusted</script>')).toBeVisible();
    await user.click(screen.getAllByText('Article details', { selector: 'summary' })[3]!);
    expect(screen.getByRole('heading', { name: long })).toBeVisible();
    expect(screen.getByText(long, { selector: '.article-summary' })).toBeVisible();
    expect(screen.getAllByText('Not available').length).toBeGreaterThan(0);
  });

  it('allows explicit HTTP/HTTPS navigation only, preserving original href text', () => {
    const response = articlesFixture(0, 2);
    response.data[0]!.url = 'http://example.test/article'; response.data[1]!.url = 'https://example.test/article?x=1';
    render(<ArticleExplorer response={response} offset={0} limit={10} loading={false} onPage={vi.fn()} />);
    for (const [i, link] of screen.getAllByRole('link').entries()) {
      expect(link).toHaveAttribute('href', response.data[i]!.url);
      expect(link).toHaveAttribute('target', '_blank'); expect(link).toHaveAttribute('rel', 'noopener noreferrer');
    }
  });

  it('respects the maximum offset, empty pages, loading and recovery to Previous', () => {
    const onPage = vi.fn();
    const response = { publication: FINGERPRINT, data: Array.from({ length: 10 }, (_, i) => article(i + 1)),
      meta: { returned: 10, limit: 10, offset: MAX_OFFSET, hasMore: true } };
    const { rerender } = render(<ArticleExplorer response={response} offset={MAX_OFFSET} limit={10} loading={false} onPage={onPage} />);
    expect(screen.getByRole('button', { name: 'Next' })).toBeDisabled();
    expect(screen.getByText(/maximum offset has been reached/)).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'Previous' })); expect(onPage).toHaveBeenCalledWith(MAX_OFFSET - 10);
    rerender(<ArticleExplorer response={articlesFixture(30)} offset={30} limit={10} loading={false} onPage={onPage} />);
    expect(screen.getByRole('heading', { name: 'No articles in this scope' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Next' })).toBeDisabled();
    expect(screen.getByRole('button', { name: 'Previous' })).toBeEnabled();
    rerender(<ArticleExplorer response={null} offset={30} limit={10} loading onPage={onPage} />);
    expect(screen.getByRole('button', { name: 'Previous' })).toBeDisabled();
  });
});
