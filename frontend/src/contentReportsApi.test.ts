import { describe, expect, it, vi } from 'vitest';
import { CONTENT_COVERAGE_TOTALS, ContentError, contentReportQueryError, contentReportQueryParameters, fetchContentReport, parseContentDiscoveryDaily, parseContentReferenceCoverage } from './contentApi';
import type { ContentCoverageResponse, ContentCoverageRow, ContentDailyResponse } from './contentApi';
import { jsonResponse } from './test/fixtures';
const identity = { publication: 'a'.repeat(64), goldPublication: 'b'.repeat(64), productId: 'content-intelligence' as const, reference: { referenceId: 'publishers', revision: 'r1', snapshotFingerprint: 'c'.repeat(64) } };
function daily(): ContentDailyResponse {
  return { ...structuredClone(identity), filters: { sourceId: null, from: null, to: null }, data: [{ sourceId: 'alpha', discoveryDate: '2026-10-20', discoveredArticleCount: '9007199254740993', articlesMissingReferenceCount: '0' }], meta: { limit: 100, offset: 0, returned: 1, hasMore: false } };
}
function coverage(): ContentCoverageResponse {
  const row = (sourceId: string, overrides: Partial<ContentCoverageRow>): ContentCoverageRow => ({ sourceId, referenceStatus: 'MATCHED', referenceUsage: 'USED', publisherName: 'Publisher', publisherCategory: 'science', declaredLanguage: 'en', countryCode: 'US', articleCount: '1', articlesMissingReferenceCount: '0', missingOptionalAttributes: [], ...overrides });
  return { ...structuredClone(identity), filters: { sourceId: null }, data: [
    row('alpha', { articleCount: '2' }),
    row('beta', { publisherCategory: null, declaredLanguage: null, countryCode: null, missingOptionalAttributes: ['publisher_category', 'declared_language', 'country_code'] }),
    row('delta', { referenceUsage: 'UNUSED', articleCount: '0' }),
    row('gamma', { referenceStatus: 'MISSING', referenceUsage: 'ABSENT', publisherName: null, publisherCategory: null, declaredLanguage: null, countryCode: null, articlesMissingReferenceCount: '1' }),
  ], totals: { articleCount: '4', matchedArticleCount: '3', missingArticleCount: '1', catalogSourceCount: '3', matchedSourceCount: '2', missingSourceCount: '1', matchedSourcesMissingOptionalAttributesCount: '1', referenceSourceCount: '3', unusedReferenceSourceCount: '1' }, meta: { limit: 100, offset: 0, returned: 4, hasMore: false } };
}
describe('Content report queries', () => {
  it('defaults aggregate reports to 100 with fixed supported filters', () => {
    expect(contentReportQueryParameters('source_discovery_daily', {}).toString()).toBe('limit=100&offset=0');
    expect(contentReportQueryParameters('reference_coverage', { sourceId: 'alpha' }).get('sourceId')).toBe('alpha');
    expect(contentReportQueryParameters('catalog', {}).get('limit')).toBe('10');
    expect(contentReportQueryError('source_discovery_daily', { from: '2024-02-29', to: '2024-02-29' })).toBeNull();
  });
  it.each([{ referenceStatus: 'MATCHED' }, { publisherCategory: 'science' }, { report: 'catalog' }, { sourceId: undefined }, { limit: '100' }])('rejects unsupported daily filters %j', query => expect(contentReportQueryError('source_discovery_daily', query)).not.toBeNull());
  it.each([{ from: '2026-01-01' }, { to: '2026-01-01' }, { countryCode: 'US' }])('rejects unsupported coverage filters %j', query => expect(contentReportQueryError('reference_coverage', query)).not.toBeNull());
});
describe('Daily report strict parsing', () => {
  it('keeps exact metric strings and accepts inclusive dates', () => {
    const response = daily(); response.filters.from = response.filters.to = '2026-10-20';
    expect(parseContentDiscoveryDaily(response, { from: '2026-10-20', to: '2026-10-20' }).data[0]!.discoveredArticleCount).toBe('9007199254740993');
  });
  it.each(['01', '-1', '1.0', '9223372036854775808', '1\n', '0'])('rejects invalid discovery count %s', count => {
    const response = daily(); response.data[0]!.discoveredArticleCount = count; expect(() => parseContentDiscoveryDaily(response, {})).toThrow(ContentError);
  });
  it.each([
    (r: ContentDailyResponse) => { r.data[0]!.articlesMissingReferenceCount = '1'; },
    (r: ContentDailyResponse) => { r.data[0]!.discoveryDate = '2026-02-30'; },
    (r: ContentDailyResponse) => { r.filters.sourceId = 'beta'; },
    (r: ContentDailyResponse) => { r.meta.hasMore = true; },
    (r: ContentDailyResponse) => { r.meta.returned = 0; },
    (r: ContentDailyResponse) => { r.reference.revision = 'INVALID'; },
    (r: ContentDailyResponse) => { Object.assign(r.data[0]!, { path: 'secret' }); },
  ])('rejects malformed daily response %#', mutate => { const response = daily(); mutate(response); expect(() => parseContentDiscoveryDaily(response, {})).toThrow(ContentError); });
  it('rejects duplicate and reversed source/date keys', () => {
    const r = daily(); r.data.push({ ...r.data[0]! }); r.meta.returned = 2;
    expect(() => parseContentDiscoveryDaily(r, {})).toThrow(); r.data[1]!.discoveryDate = '2026-10-21';
    expect(parseContentDiscoveryDaily(r, {}).data).toHaveLength(2);
    r.data[1]!.articlesMissingReferenceCount = r.data[1]!.discoveredArticleCount; expect(() => parseContentDiscoveryDaily(r, {})).toThrow();
    r.data[1]!.articlesMissingReferenceCount = '0'; r.data.reverse(); expect(() => parseContentDiscoveryDaily(r, {})).toThrow();
  });
});
describe('Coverage strict parsing and global totals', () => {
  it('accepts used, unused, absent and nullable optional attributes', () => expect(parseContentReferenceCoverage(coverage(), {})).toEqual(coverage()));
  it('preserves totals across pages including beyond end', () => {
    const r = coverage(); r.data = [r.data[1]!]; r.meta = { limit: 1, offset: 1, returned: 1, hasMore: true };
    expect(parseContentReferenceCoverage(r, { limit: 1, offset: 1 }).totals.articleCount).toBe('4');
    r.data = []; r.meta = { limit: 1, offset: 4, returned: 0, hasMore: false };
    expect(parseContentReferenceCoverage(r, { limit: 1, offset: 4 }).totals.articleCount).toBe('4');
  });
  it('allows unknown sources with zero totals and empty selected publications', () => {
    const r = coverage(); r.data = []; r.meta.returned = 0;
    for (const key of CONTENT_COVERAGE_TOTALS) r.totals[key] = '0';
    expect(parseContentReferenceCoverage(r, {}).data).toEqual([]);
    r.filters.sourceId = 'unknown'; expect(parseContentReferenceCoverage(r, { sourceId: 'unknown' }).data).toEqual([]);
  });
  it('checks source-filtered totals rather than global totals', () => {
    const r = coverage(); r.filters.sourceId = 'delta'; r.data = [r.data[2]!]; r.meta.returned = 1;
    expect(() => parseContentReferenceCoverage(r, { sourceId: 'delta' })).toThrow();
    for (const key of CONTENT_COVERAGE_TOTALS) r.totals[key] = '0'; r.totals.referenceSourceCount = r.totals.unusedReferenceSourceCount = '1';
    expect(parseContentReferenceCoverage(r, { sourceId: 'delta' }).data[0]!.referenceUsage).toBe('UNUSED');
  });
  it.each([
    (r: ContentCoverageResponse) => { r.totals.articleCount = '5'; },
    (r: ContentCoverageResponse) => { r.totals.catalogSourceCount = '4'; },
    (r: ContentCoverageResponse) => { r.totals.referenceSourceCount = '4'; },
    (r: ContentCoverageResponse) => { r.totals.matchedSourcesMissingOptionalAttributesCount = '3'; },
    (r: ContentCoverageResponse) => { Object.assign(r.totals, { extra: '0' }); },
    (r: ContentCoverageResponse) => { r.totals.matchedArticleCount = '03'; },
    (r: ContentCoverageResponse) => { r.data[0]!.referenceUsage = 'UNUSED'; },
    (r: ContentCoverageResponse) => { r.data[0]!.referenceStatus = 'MISSING'; },
    (r: ContentCoverageResponse) => { r.data[0]!.articlesMissingReferenceCount = '1'; },
    (r: ContentCoverageResponse) => { r.data[1]!.missingOptionalAttributes.reverse(); },
    (r: ContentCoverageResponse) => { r.data[1]!.missingOptionalAttributes = []; },
    (r: ContentCoverageResponse) => { r.data[3]!.missingOptionalAttributes = ['publisher_category']; },
    (r: ContentCoverageResponse) => { r.data[3]!.publisherName = 'Unexpected'; },
    (r: ContentCoverageResponse) => { r.data[0]!.publisherName = null; },
    (r: ContentCoverageResponse) => { r.data.reverse(); },
    (r: ContentCoverageResponse) => { r.data[1]!.sourceId = 'alpha'; },
    (r: ContentCoverageResponse) => { r.data.pop(); r.meta.returned--; },
    (r: ContentCoverageResponse) => { Object.assign(r.filters, { from: null }); },
  ])('rejects coverage corruption %#', mutate => { const r = coverage(); mutate(r); expect(() => parseContentReferenceCoverage(r, {})).toThrow(ContentError); });
});
describe('Aggregate report transport', () => {
  it.each(['source_discovery_daily', 'reference_coverage'] as const)('uses fixed route for %s', async report => {
    const fetch = vi.fn().mockResolvedValue(jsonResponse(report === 'source_discovery_daily' ? daily() : coverage())); vi.stubGlobal('fetch', fetch);
    const signal = new AbortController().signal; await fetchContentReport(report, {}, signal);
    expect(fetch).toHaveBeenCalledWith(`/api/v1/content/${report.replaceAll('_', '-')}?limit=100&offset=0`, { signal, cache: 'no-store', credentials: 'omit', headers: { Accept: 'application/json' } });
  });
  it('rejects incompatible queries before fetch', async () => {
    const fetch = vi.fn(); vi.stubGlobal('fetch', fetch); await expect(fetchContentReport('reference_coverage', { from: '2026-01-01' }, new AbortController().signal)).rejects.toMatchObject({ kind: 'invalid-argument' }); expect(fetch).not.toHaveBeenCalled();
  });
  it.each([[400, 'invalid-argument'], [503, 'unavailable'], [504, 'timeout'], [500, 'execution']])('maps aggregate status %s', async (status, kind) => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response('secret', { status: Number(status) })));
    await expect(fetchContentReport('source_discovery_daily', {}, new AbortController().signal)).rejects.toMatchObject({ kind });
  });
});
