import { describe, expect, it, vi } from 'vitest';
import { DashboardError, fetchArticles, fetchDaily, fetchQuality, parseArticles, parseDaily, parseQuality, parsePublishers } from './api';
import { articlesFixture, dailyFixture, qualityFixture } from './test/analyticsFixtures';
import { fixture, jsonResponse, MICROSECOND_TIME } from './test/fixtures';

describe('M6.4 response contracts', () => {
  it('preserves article nulls, microseconds, selected warning codes and exact BIGINT strings', () => {
    const body = articlesFixture();
    const row = body.data[0]!;
    Object.assign(row, { url: null, summary: null, observationCount: '9223372036854775807', distinctIngestionCount: '9007199254740993',
      qualityWarnings: ['INVALID_PUBLISHED_AT', 'INVALID_UPDATED_AT'] });
    expect(parseArticles(body, 10, 0)).toEqual(body);
    expect(parseArticles(body, 10, 0).data[0]!.firstObservedAt).toBe(MICROSECOND_TIME);
    expect(parseDaily(dailyFixture(), 100)).toEqual(dailyFixture());
    expect(parseQuality(qualityFixture(), 100).data[0]!.warningCodesPerObservation).toBe(2);
  });

  it('accepts empty, final, maximum-offset and middle pages with matching metadata', () => {
    for (const offset of [0, 10, 20, 23, 1_000_000]) {
      const body = articlesFixture(offset);
      expect(parseArticles(body, 10, offset)).toEqual(body);
    }
  });

  const invalidArticle: [string, (v: ReturnType<typeof articlesFixture>) => unknown][] = [
    ['unknown field', (v) => ({ ...v, data: [{ ...v.data[0], provenance: '/secret' }, ...v.data.slice(1)] })],
    ['missing title', (v) => { const row = Object.fromEntries(Object.entries(v.data[0]!).filter(([key]) => key !== 'title')); return { ...v, data: [row, ...v.data.slice(1)] }; }],
    ['blank title', (v) => { v.data[0]!.title = ' '; return v; }],
    ['invalid identity', (v) => { v.data[0]!.articleId = 'guessed-id'; return v; }],
    ['duplicate identity', (v) => { v.data[1]!.articleId = v.data[0]!.articleId; return v; }],
    ['wrong identity order', (v) => { v.data.reverse(); return v; }],
    ['wrong time order', (v) => { v.data[1]!.lastObservedAt = '2026-10-13T00:00:00.000001Z'; return v; }],
    ['first time after last', (v) => { v.data[0]!.firstObservedAt = '2026-10-13T00:00:00.000001Z'; return v; }],
    ['null required timestamp', (v) => ({ ...v, data: [{ ...v.data[0], lastObservedAt: null }, ...v.data.slice(1)] })],
    ['invalid calendar timestamp', (v) => { v.data[0]!.updatedAt = '2026-02-30T00:00:00.000000Z'; return v; }],
    ['millisecond timestamp', (v) => { v.data[0]!.publishedAt = '2026-10-12T00:00:00.123Z'; return v; }],
    ['non-string summary', (v) => ({ ...v, data: [{ ...v.data[0], summary: 4 }, ...v.data.slice(1)] })],
    ['negative count', (v) => { v.data[0]!.observationCount = '-1'; return v; }],
    ['zero observations', (v) => { v.data[0]!.observationCount = '0'; return v; }],
    ['count overflow', (v) => { v.data[0]!.observationCount = '9223372036854775808'; return v; }],
    ['numeric count', (v) => ({ ...v, data: [{ ...v.data[0], observationCount: 9007199254740992 }, ...v.data.slice(1)] })],
    ['too many distinct ingestions', (v) => { v.data[0]!.distinctIngestionCount = '3'; return v; }],
    ['unsupported warning', (v) => { v.data[0]!.qualityWarnings = ['UNKNOWN']; return v; }],
    ['warning order', (v) => { v.data[0]!.qualityWarnings = ['INVALID_UPDATED_AT', 'INVALID_PUBLISHED_AT']; return v; }],
    ['warning duplicates', (v) => { v.data[0]!.qualityWarnings = ['INVALID_UPDATED_AT', 'INVALID_UPDATED_AT']; return v; }],
    ['null warning list', (v) => ({ ...v, data: [{ ...v.data[0], qualityWarnings: null }, ...v.data.slice(1)] })],
  ];
  it.each(invalidArticle)('rejects article %s', (_, mutate) => {
    expect(() => parseArticles(mutate(articlesFixture()), 10, 0)).toThrow(DashboardError);
  });

  it.each([
    { returned: 9, limit: 10, offset: 0, hasMore: true }, { returned: 10, limit: 20, offset: 0, hasMore: true },
    { returned: 10, limit: 10, offset: 1, hasMore: true }, { returned: 10, limit: 10, offset: '0', hasMore: true },
    { returned: 10, limit: 10, offset: 0, hasMore: 'true' }, { returned: 10, limit: 10, offset: 0 },
    { returned: 10, limit: 10, offset: 0, hasMore: true, total: 23 },
  ])('rejects incorrect page metadata %j', (meta) => {
    expect(() => parseArticles({ ...articlesFixture(), meta }, 10, 0)).toThrow(DashboardError);
  });

  it('rejects short and empty pages claiming more rows, and wrong source filters', () => {
    const empty = articlesFixture(0, 0);
    empty.meta.hasMore = true;
    expect(() => parseArticles(empty, 10, 0)).toThrow();
    const short = articlesFixture(20);
    short.meta.hasMore = true;
    expect(() => parseArticles(short, 10, 20)).toThrow();
    expect(() => parseArticles(articlesFixture(), 10, 0, 'beta')).toThrow();
    expect(() => parseDaily(dailyFixture(), 100, 'alpha')).toThrow();
    expect(() => parseQuality(qualityFixture(), 100, 'alpha')).toThrow();
  });

  it.each([
    ['date', { ingestionDate: '2026-02-30' }], ['non-UTC day', { ingestionDate: '2026-10-11' }],
    ['zero distinct', { distinctArticleCount: '0' }], ['excessive distinct', { distinctIngestionCount: '6' }],
    ['negative warnings', { qualityWarningCount: '-1' }], ['null timestamp', { latestIngestedAt: null }],
    ['extra field', { unknown: 1 }], ['float counter', { observationCount: 5.5 }],
  ])('rejects daily %s', (_, change) => {
    const body = dailyFixture();
    expect(() => parseDaily({ ...body, data: [{ ...body.data[0], ...change }, ...body.data.slice(1)] }, 100)).toThrow();
  });

  it('validates source/day ordering, inclusive dates and nonblank IDs', () => {
    const body = dailyFixture();
    expect(() => parseDaily({ ...body, data: [...body.data].reverse() }, 100)).toThrow();
    expect(() => parseDaily({ ...body, data: [body.data[0], body.data[0], body.data[2]] }, 100)).toThrow();
    expect(() => parseDaily(body, 100, '', { from: '2026-10-13', to: '' })).toThrow();
    expect(() => parseDaily(body, 100, '', { from: '', to: '2026-10-13' })).toThrow();
    expect(parseDaily(body, 100, '', { from: '2026-10-12', to: '2026-10-14' })).toEqual(body);
    const publishers = fixture();
    expect(() => parsePublishers({ ...publishers, data: [...publishers.data].reverse() }, 100)).toThrow();
    expect(() => parseQuality({ ...qualityFixture(), data: [{ ...qualityFixture().data[0], sourceId: ' ' }] }, 100)).toThrow();
  });

  it.each([-1, Number.NaN, Number.POSITIVE_INFINITY, null, '2', true])('rejects invalid quality rate %s', (rate) => {
    const body = qualityFixture();
    expect(() => parseQuality({ ...body, data: [{ ...body.data[0], warningCodesPerObservation: rate }, body.data[1]] }, 100)).toThrow();
  });

  it('preserves huge quality counters without altering supplied rate and rejects wrong source order', () => {
    const body = qualityFixture();
    Object.assign(body.data[0]!, { observationCount: '9223372036854775807', qualityWarningCount: '9007199254740993', warningCodesPerObservation: .0009765625 });
    expect(parseQuality(body, 100)).toEqual(body);
    expect(() => parseQuality({ ...body, data: [...body.data].reverse() }, 100)).toThrow();
    body.data[1]!.sourceId = 'alpha';
    expect(() => parseQuality(body, 100)).toThrow();
  });

  it('rejects wrong envelope shapes for every new report', () => {
    for (const [body, parse] of [
      [articlesFixture(), (v: unknown) => parseArticles(v, 10, 0)],
      [dailyFixture(), (v: unknown) => parseDaily(v, 100)],
      [qualityFixture(), (v: unknown) => parseQuality(v, 100)],
    ] as const) {
      for (const invalid of [null, [], {}, { ...body, publication: 'latest' }, { ...body, data: null },
        { ...body, extra: true }, { ...body, meta: { ...body.meta, returned: '1' } }]) expect(() => parse(invalid)).toThrow(DashboardError);
    }
  });
});

describe('new fetch clients', () => {
  it('constructs only supported same-origin parameters with no-store and cancellation', async () => {
    const fetch = vi.fn().mockResolvedValueOnce(jsonResponse(articlesFixture(10))).mockResolvedValueOnce(jsonResponse(dailyFixture()))
      .mockResolvedValueOnce(jsonResponse(qualityFixture()));
    vi.stubGlobal('fetch', fetch);
    const signal = new AbortController().signal;
    await fetchArticles('alpha', signal, 10);
    await fetchDaily('', signal, { from: '2026-10-12', to: '2026-10-14' });
    await fetchQuality('', signal, { from: '', to: '2026-10-14' });
    expect(fetch.mock.calls.map(([url]) => url)).toEqual(['/api/v1/articles?limit=10&sourceId=alpha&offset=10',
      '/api/v1/daily?limit=100&from=2026-10-12&to=2026-10-14', '/api/v1/quality?limit=100&to=2026-10-14']);
    expect(fetch.mock.calls[0]?.[1]).toEqual({ signal, cache: 'no-store', credentials: 'omit', headers: { Accept: 'application/json' } });
  });

  it.each([-1, 1_000_001, Number.MAX_VALUE, 1.5, Number.NaN])('rejects offset %s before fetch', async (offset) => {
    const fetch = vi.fn(); vi.stubGlobal('fetch', fetch);
    await expect(fetchArticles('', new AbortController().signal, offset)).rejects.toHaveProperty('kind', 'invalid-argument');
    expect(fetch).not.toHaveBeenCalled();
  });
  it.each([{ from: '2026-02-30', to: '' }, { from: '2026-10-14', to: '2026-10-12' }, { from: '', to: '0000-01-01' },
    { from: '2026-1-01', to: '' }])('rejects invalid date scope %j before fetching', async (dates) => {
    const fetch = vi.fn(); vi.stubGlobal('fetch', fetch);
    for (const load of [fetchDaily, fetchQuality]) await expect(load('', new AbortController().signal, dates)).rejects.toHaveProperty('kind', 'invalid-argument');
    expect(fetch).not.toHaveBeenCalled();
  });

  it.each([400, 500, 503, 504])('shares safe HTTP %s handling across reports', async (status) => {
    vi.stubGlobal('fetch', vi.fn().mockImplementation(() => Promise.resolve(jsonResponse({ message: '/secret traceback' }, status))));
    for (const load of [fetchArticles, fetchDaily, fetchQuality]) {
      const error = await load('', new AbortController().signal).catch((e: unknown) => e);
      expect(error).toBeInstanceOf(DashboardError);
      expect((error as Error).message).not.toMatch(/secret|traceback/);
    }
  });
});
