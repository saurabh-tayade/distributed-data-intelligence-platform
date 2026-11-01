import type { SourceCapabilities, SourceCommand, SourceResponse } from '../managementApi';
import { CORRELATION, sourceEnvelope, sourceRecord } from './managementFixtures';
export const ENDPOINT = '30000000-0000-4000-8000-000000000001';
export const STORAGE = '20000000-0000-4000-8000-000000000001';
export const REL_STORAGE = '20000000-0000-4000-8000-000000000002';
export function capabilities(): { apiVersion: 1; data: SourceCapabilities; meta: { correlationId: string } } {
  return { apiVersion: 1, meta: { correlationId: CORRELATION }, data: { scope: 'SOURCE_MANAGEMENT_ONLY',
    profiles: ['rss-atom-v1', 'request-jsonl-v2', 'request-parquet-v1'], maxSources: 128, maxRevisionsPerSource: 1000,
    endpoints: [{ endpointId: ENDPOINT, sourceId: 'alpha' }],
    reliabilityProfiles: [
      { sourceId: 'reliability', profile: 'request-jsonl-v2', dataOrigin: 'synthetic', registeredProfileSha256: 'b'.repeat(64) },
      { sourceId: 'reliability', profile: 'request-parquet-v1', dataOrigin: 'synthetic', registeredProfileSha256: 'c'.repeat(64) },
    ], storageProfiles: [{ profileId: STORAGE, productId: 'content-intelligence', role: 'BRONZE' },
      { profileId: REL_STORAGE, productId: 'application-reliability', role: 'BATCH' }],
  } };
}
export function baseline(): SourceResponse { const body = sourceEnvelope(); return { ...body, etag: `"${body.data.rowVersion}"` }; }
export function command(action: SourceCommand['action'] = 'create'): SourceCommand {
  const source = sourceRecord(), { productId, sourceId, displayName, enabled, config } = source;
  return { action, identity: { productId, sourceId }, ifMatch: action === 'create' ? null : `"${source.rowVersion}"`, key: 'test-intent-1',
    body: JSON.stringify(action === 'create' ? { productId, sourceId, displayName, enabled, config }
      : action === 'revise' ? { displayName, config } : { enabled: false }) };
}
