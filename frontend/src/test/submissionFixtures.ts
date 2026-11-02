import type { ReliabilityBatch, ReliabilityProfile, ReliabilityRequest, RunDetail, ServiceReference } from '../managementApi';
import { sourceRecord } from './managementFixtures';
import { historyPage, recordEnvelope, runDetail } from './runInspectionFixtures';
export { historyPage } from './runInspectionFixtures';
export const PROFILE_ID = '80000000-0000-4000-8000-000000000001';
export const PROFILE_2 = '80000000-0000-4000-8000-000000000002';
export const BATCH_ID = '90000000-0000-4000-8000-000000000001';
export const REFERENCE_ID = 'a0000000-0000-4000-8000-000000000001';
export function profile(id = PROFILE_ID, parquet = false): ReliabilityProfile {
  return { executionProfileId: id, productId: 'application-reliability', pipeline: 'RELIABILITY_V2', runProfileVersion: 1, pipelineVersion: 1,
    sourceProfile: parquet ? 'request-parquet-v1' : 'request-jsonl-v2', inputFormat: parquet ? 'PARQUET' : 'JSONL', configurationSha256: 'c'.repeat(64), createdAt: '2026-11-01T12:34:56.654321Z' };
}
export function selectedSource(id = 'alpha') { return sourceRecord(id, 'application-reliability'); }
export function batch(id = BATCH_ID, sourceId = 'alpha'): ReliabilityBatch {
  return { artifactId: id, productId: 'application-reliability', kind: 'RELIABILITY_BATCH_V2', role: 'RELIABILITY_BATCH', contractVersion: 2, processorRevision: 2,
    nativeFormat: 'JSONL', storageBindingId: selectedSource().config.inputStorageProfileId, nativeFingerprint: 'd'.repeat(64), verifiedAt: '2026-11-01T12:34:56.654321Z',
    identity: { sourceId, sourceRevision: 2, batchId: `batch-${id.slice(-3)}`, inputFormat: 'JSONL', registeredProfileSha256: 'b'.repeat(64) } };
}
export function reference(id = REFERENCE_ID): ServiceReference {
  return { artifactId: id, productId: 'application-reliability', kind: 'SERVICE_REFERENCE', role: 'SERVICE_REFERENCE', contractVersion: 1, processorRevision: 1,
    nativeFormat: null, storageBindingId: 'registered-reference', nativeFingerprint: 'e'.repeat(64), verifiedAt: '2026-11-01T12:34:56.654321Z', identity: { referenceId: 'services', revision: 'r1' } };
}
export function request(): ReliabilityRequest {
  return { pipeline: 'RELIABILITY_V2', executionProfileId: PROFILE_ID, sources: [{ sourceId: 'alpha', revision: 2 }], batchArtifactIds: [BATCH_ID], serviceReferenceArtifactId: REFERENCE_ID };
}
export const submissionCommand = () => ({ key: 'intent-1', body: JSON.stringify(request()) });
export function submittedRun(state: RunDetail['state'] = 'QUEUED') {
  return { ...runDetail(1, state, 'application-reliability'), sourceRevisions: request().sources, inputArtifactIds: [BATCH_ID, REFERENCE_ID] };
}
export function submissionResponse(record = submittedRun(), location = `/api/management/v1/runs/${record.runId}`) {
  return new Response(JSON.stringify(recordEnvelope(record)), { status: 202,
    headers: { 'Content-Type': 'application/json', ETag: `"${record.rowVersion}"`, Location: location } });
}
export function fullProfiles(nextCursor: string) {
  return historyPage(Array.from({ length: 20 }, (_, i) => profile(`80000000-0000-4000-8000-${(i + 1).toString(16).padStart(12, '0')}`)), 20, nextCursor);
}
