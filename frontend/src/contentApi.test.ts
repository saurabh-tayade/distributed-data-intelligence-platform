import { describe, expect, it, vi } from 'vitest';
import { ContentError, contentIdentityKey, contentQueryError, contentQueryParameters, fetchContentCatalog, parseContentCatalog } from './contentApi';
import type { ContentQuery, ContentResponse } from './contentApi';
import { contentFixture, contentRow } from './test/contentFixtures';
import { jsonResponse } from './test/fixtures';

describe('Content catalog query boundary', () => {
  it('uses only fixed filters and bounded pagination', () => {
    expect(contentQueryParameters({}).toString()).toBe('limit=10&offset=0');
    const query: ContentQuery = { sourceId: 'alpha', from: '2024-02-29', to: '2024-02-29', referenceStatus: 'MATCHED', publisherCategory: 'science', declaredLanguage: 'en', countryCode: 'GB', limit: 1000, offset: 1000000 };
    expect(contentQueryError(query)).toBeNull(); expect(Object.fromEntries(contentQueryParameters(query))).toEqual(Object.fromEntries(Object.entries(query).map(([k,v]) => [k,String(v)])));
  });
  it.each([null, [], { extra: 1 }, { sourceId: '' }, { sourceId: 'alpha\n' }, { sourceId: 'Alpha' }, { sourceId: undefined }, { from: '2025-02-29' }, { from: '0000-01-01' }, { to: '2024-01-01\n' }, { from: '2026-02-02', to: '2026-02-01' }, { referenceStatus: 'UNKNOWN' }, { publisherCategory: 'news' }, { declaredLanguage: 'EN' }, { countryCode: 'UK' }, { limit: 0 }, { limit: 1001 }, { limit: '10' }, { limit: 1.5 }, { offset: -1 }, { offset: 1000001 }, { offset: undefined }, { offset: NaN }])('rejects invalid query %j', (query) => expect(contentQueryError(query)).not.toBeNull());
  it('rejects symbol fields and inherited optional values supplied as own undefined', () => { expect(contentQueryError({ [Symbol()]: 1 })).not.toBeNull(); expect(contentQueryError({ from: undefined })).not.toBeNull(); });
});
describe('Content catalog response boundary', () => {
  it('preserves exact large counts, microseconds and null semantics', () => {
    const response = contentFixture(); expect(parseContentCatalog(response, {})).toEqual(response);
    response.data[0]!.publisherCategory = null; response.data[0]!.summary = '';
    expect(parseContentCatalog(response, {}).data[0]).toMatchObject({ observationCount: '9007199254740993', summary: '', publisherCategory: null });
  });
  it('preserves the native reader warning and source identifier domain', () => {
    const response = contentFixture();
    // The independently verified native Content fixture uses both warning codes on RSS.
    // Consumer projection must not invent additional per-format warning or URL rules.
    Object.assign(response.data[0]!, { qualityWarnings: ['INVALID_PUBLISHED_AT', 'INVALID_UPDATED_AT'],
      identifierKind: 'rss_link', sourceArticleId: 'retained-native-id', url: null,
      publishedAt: response.data[0]!.lastObservedAt, updatedAt: response.data[0]!.lastObservedAt });
    expect(parseContentCatalog(response, {}).data[0]).toEqual(response.data[0]);
  });
  it('accepts empty pages and inclusive discovery endpoints', () => {
    const query = { from: '2026-10-20', to: '2026-10-20' }; expect(parseContentCatalog(contentFixture(query), query).data).toHaveLength(1);
    const empty = contentFixture(); empty.data = []; empty.meta.returned = 0; expect(parseContentCatalog(empty, {}).data).toEqual([]);
  });
  it('keeps reference selection identity even for missing reference rows', () => {
    const response = contentFixture({ referenceStatus: 'MISSING' }); Object.assign(response.data[0]!, { referenceStatus: 'MISSING', publisherName: null, publisherCategory: null, declaredLanguage: null, countryCode: null });
    expect(parseContentCatalog(response, { referenceStatus: 'MISSING' }).data[0]!.referenceId).toBe('publishers');
  });
  const corruptions: [string, (r: ContentResponse) => void][] = [
    ['top-level extra', r => Object.assign(r, { path: '/private' })], ['meta extra', r => Object.assign(r.meta, { total: 1 })],
    ['reference extra', r => Object.assign(r.reference, { extra: 1 })], ['filter extra', r => Object.assign(r.filters, { extra: 1 })],
    ['row extra', r => Object.assign(r.data[0]!, { sourceUri: 'hidden' })], ['product', r => Object.assign(r, { productId: 'wrong' })],
    ['publication', r => { r.publication += '\n'; }], ['gold selection', r => { r.goldPublication = 'bad'; }],
    ['reference identity', r => { r.data[0]!.referenceRevision = 'r2'; }], ['reference fingerprint', r => { r.data[0]!.referenceFingerprint = 'e'.repeat(64); }],
    ['reference name', r => { r.reference.referenceId = 'Bad'; }], ['counter precision', r => Object.assign(r.data[0]!, { observationCount: Number('9007199254740993') })],
    ['counter overflow', r => { r.data[0]!.observationCount = '9223372036854775808'; }], ['counter newline', r => { r.data[0]!.observationCount = '2\n'; }],
    ['zero count', r => { r.data[0]!.observationCount = '0'; }], ['count relation', r => { r.data[0]!.distinctIngestionCount = '9223372036854775807'; }],
    ['timestamp precision', r => { r.data[0]!.lastObservedAt = '2026-10-21T12:00:00.123Z'; }], ['timestamp calendar', r => { r.data[0]!.lastObservedAt = '2026-02-30T12:00:00.123456Z'; }],
    ['discovery mismatch', r => { r.data[0]!.discoveredAt = r.data[0]!.lastObservedAt; }], ['date mismatch', r => { r.data[0]!.discoveryDate = '2026-10-21'; }],
    ['source', r => { r.data[0]!.sourceId = 'alpha\n'; }], ['identifier kind', r => { r.data[0]!.identifierKind = 'atom_id'; }],
    ['empty source article', r => { r.data[0]!.sourceArticleId = ''; }], ['missing attributes', r => { r.data[0]!.referenceStatus = 'MISSING'; }],
    ['matched null name', r => { r.data[0]!.publisherName = null; }], ['name whitespace', r => { r.data[0]!.publisherName = ' Alpha'; }],
    ['name surrogate', r => { r.data[0]!.publisherName = '\ud800'; }], ['name too long', r => { r.data[0]!.publisherName = 'x'.repeat(201); }],
    ['native vocabulary', r => Object.assign(r.data[0]!, { countryCode: 'UK' })], ['warning', r => { r.data[0]!.qualityWarnings = ['UNKNOWN']; }],
    ['filter echo', r => { r.filters.sourceId = 'another'; }], ['returned count', r => { r.meta.returned = 2; }],
    ['offset echo', r => { r.meta.offset = 1; }], ['false more', r => { r.meta.hasMore = true; }],
  ];
  it.each(corruptions)('rejects %s', (_label, mutate) => { const response = contentFixture(); mutate(response); expect(() => parseContentCatalog(response, {})).toThrow(ContentError); });
  it('rejects row filter disagreement separately from echo', () => {
    for (const query of [{ sourceId: 'beta' }, { from: '2026-10-21' }, { to: '2026-10-19' }, { referenceStatus: 'MISSING' }, { publisherCategory: 'culture' }, { declaredLanguage: 'fr' }, { countryCode: 'US' }] as ContentQuery[]) {
      const r = contentFixture(query); r.data = [contentRow()]; expect(() => parseContentCatalog(r, query)).toThrow(ContentError);
    }
  });
  it('requires unique IDs and deterministic descending timestamp/ascending ID order', () => {
    const r = contentFixture(); r.data.push(contentRow({ articleId: 'b'.repeat(64) })); r.meta.returned = 2;
    expect(parseContentCatalog(r, {}).data).toHaveLength(2); r.data.reverse(); expect(() => parseContentCatalog(r, {})).toThrow();
    r.data = [contentRow(), contentRow()]; expect(() => parseContentCatalog(r, {})).toThrow();
  });
  it('includes every dependency identity in the session key', () => {
    const r = contentFixture(), baseline = contentIdentityKey(r);
    for (const mutate of [(v: ContentResponse) => { v.publication = 'e'.repeat(64); }, (v: ContentResponse) => { v.goldPublication = 'e'.repeat(64); }, (v: ContentResponse) => { v.reference.referenceId = 'other'; }, (v: ContentResponse) => { v.reference.revision = 'r2'; }, (v: ContentResponse) => { v.reference.snapshotFingerprint = 'e'.repeat(64); }]) {
      const copy = structuredClone(r); mutate(copy); expect(contentIdentityKey(copy)).not.toBe(baseline);
    }
  });
});
describe('Content transport', () => {
  it('uses read-only no-store transport without credentials', async () => {
    const fetch = vi.fn().mockResolvedValue(jsonResponse(contentFixture())); vi.stubGlobal('fetch', fetch);
    const signal = new AbortController().signal; await fetchContentCatalog({}, signal);
    expect(fetch).toHaveBeenCalledWith('/api/v1/content/catalog?limit=10&offset=0', { signal, cache: 'no-store', credentials: 'omit', headers: { Accept: 'application/json' } });
  });
  it.each([[400,'invalid-argument'],[503,'unavailable'],[504,'timeout'],[500,'execution'],[201,'execution']])('maps %s without exposing bodies', async (status, kind) => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response('secret server path', { status: Number(status) })));
    await expect(fetchContentCatalog({}, new AbortController().signal)).rejects.toMatchObject({ kind });
  });
  it('distinguishes capacity, network, invalid JSON and wrong MIME', async () => {
    const signal = new AbortController().signal;
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(jsonResponse({ code: 'ANALYTICS_CAPACITY_EXCEEDED' }, 503)));
    await expect(fetchContentCatalog({}, signal)).rejects.toMatchObject({ kind: 'capacity' });
    vi.stubGlobal('fetch', vi.fn().mockRejectedValue(new Error('secret'))); await expect(fetchContentCatalog({}, signal)).rejects.toMatchObject({ kind: 'network' });
    for (const response of [new Response('{', { headers: { 'content-type': 'application/json' } }), new Response(JSON.stringify(contentFixture()), { headers: { 'content-type': 'text/html' } })]) {
      vi.stubGlobal('fetch', vi.fn().mockResolvedValue(response)); await expect(fetchContentCatalog({}, signal)).rejects.toMatchObject({ kind: 'invalid-response' });
    }
  });
});
