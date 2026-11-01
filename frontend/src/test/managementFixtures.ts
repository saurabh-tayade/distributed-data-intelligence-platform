import type { ProductId, SourcesPage, SourceSummary } from '../managementApi';

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
  return { apiVersion: 1, code, message: '/private/secret traceback', correlationId: CORRELATION, retryable: true };
}
