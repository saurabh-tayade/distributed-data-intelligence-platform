import type { ProductId, RevisionsPage, SourceRecord, SourceRevision, SourcesPage, SourceSummary } from '../managementApi';

export const CORRELATION = '10000000-0000-4000-8000-000000000001';
export const FIRST_CURSOR = 'eyJwYWdlIjoxfQ.' + 's'.repeat(43);
export const SECOND_CURSOR = 'eyJwYWdlIjoyfQ.' + 's'.repeat(43);
export function source(sourceId = 'alpha', productId: ProductId = 'content-intelligence'): SourceSummary {
  return { sourceId, productId, displayName: `Source ${sourceId}`, enabled: true, currentRevision: 2,
    rowVersion: '9007199254740993', configSha256: 'a'.repeat(64),
    createdAt: '2026-10-12T00:00:00.123456Z', updatedAt: '2026-11-01T12:34:56.654321Z' };
}
export function sourcePage(rows: SourceSummary[] = [source()], limit = 20, nextCursor: string | null = null): SourcesPage {
  return { apiVersion: 1, data: rows, meta: { correlationId: CORRELATION, limit, hasMore: nextCursor !== null, nextCursor } };
}
export function fullSourcePage(nextCursor = FIRST_CURSOR): SourcesPage {
  return sourcePage(Array.from({ length: 20 }, (_, index) => source(`source-${String(index).padStart(3, '0')}`)), 20, nextCursor);
}
export function managementFailure(code = 'MANAGEMENT_UNAVAILABLE') {
  return { apiVersion: 1, code, message: '/private/secret traceback', correlationId: CORRELATION, retryable: code === 'MANAGEMENT_UNAVAILABLE' };
}

export function sourceRecord(sourceId = 'alpha', productId: ProductId = 'content-intelligence'): SourceRecord {
  const shared = { configVersion: 1 as const, inputStorageProfileId: '20000000-0000-4000-8000-000000000001' };
  return { ...source(sourceId, productId), config: productId === 'content-intelligence'
    ? { ...shared, profile: 'rss-atom-v1', dataOrigin: 'publisher', feedEndpointId: '30000000-0000-4000-8000-000000000001', registeredProfileSha256: null }
    : { ...shared, profile: 'request-jsonl-v2', dataOrigin: 'synthetic', feedEndpointId: null, registeredProfileSha256: 'b'.repeat(64) } };
}
export function sourceEnvelope(record = sourceRecord()) {
  return { apiVersion: 1 as const, data: record, meta: { correlationId: CORRELATION } };
}
export function sourceResponse(record = sourceRecord()): Response {
  return new Response(JSON.stringify(sourceEnvelope(record)), { headers: { 'Content-Type': 'application/json', ETag: `"${record.rowVersion}"` } });
}
export function revision(revision = 1, record = sourceRecord()): SourceRevision {
  return { productId: record.productId, sourceId: record.sourceId, revision, displayName: record.displayName,
    configSha256: record.configSha256, createdAt: record.createdAt, config: record.config };
}
export function revisionPage(rows = [revision(1), revision(2)], limit = 20, nextCursor: string | null = null): RevisionsPage {
  return { apiVersion: 1, data: rows, meta: { correlationId: CORRELATION, limit, hasMore: nextCursor !== null, nextCursor } };
}
export function fullRevisionPage(start = 1, nextCursor: string | null = FIRST_CURSOR) {
  return revisionPage(Array.from({ length: 20 }, (_, index) => revision(start + index)), 20, nextCursor);
}
