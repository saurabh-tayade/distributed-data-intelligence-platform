import type { ContentQuery, ContentResponse, ContentRow } from '../contentApi';
export function contentRow(overrides: Partial<ContentRow> = {}): ContentRow {
  return { articleId: 'a'.repeat(64), sourceId: 'alpha', identifierKind: 'rss_guid', sourceArticleId: 'item-1', feedFormat: 'rss',
    title: 'An enriched article', url: 'https://example.com/article', summary: 'Summary', publishedAt: null, updatedAt: null,
    qualityWarnings: [], firstObservedAt: '2026-10-20T12:00:00.123456Z', lastObservedAt: '2026-10-21T12:00:00.123456Z',
    observationCount: '9007199254740993', distinctIngestionCount: '2', discoveredAt: '2026-10-20T12:00:00.123456Z', discoveryDate: '2026-10-20',
    publisherName: 'Alpha News', publisherCategory: 'science', declaredLanguage: 'en', countryCode: 'GB', referenceStatus: 'MATCHED',
    referenceId: 'publishers', referenceRevision: 'r1', referenceFingerprint: 'd'.repeat(64), ...overrides };
}
export function contentFixture(query: ContentQuery = {}): ContentResponse {
  return { publication: 'b'.repeat(64), productId: 'content-intelligence', goldPublication: 'c'.repeat(64),
    reference: { referenceId: 'publishers', revision: 'r1', snapshotFingerprint: 'd'.repeat(64) },
    filters: { sourceId: query.sourceId ?? null, from: query.from ?? null, to: query.to ?? null, referenceStatus: query.referenceStatus ?? null,
      publisherCategory: query.publisherCategory ?? null, declaredLanguage: query.declaredLanguage ?? null, countryCode: query.countryCode ?? null },
    data: [contentRow({ sourceId: query.sourceId ?? 'alpha' })], meta: { returned: 1, limit: query.limit ?? 10, offset: query.offset ?? 0, hasMore: false } };
}
