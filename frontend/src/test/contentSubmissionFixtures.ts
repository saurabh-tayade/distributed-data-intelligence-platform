import type { ContentMetadata, ContentProfile, ContentRequest, ContentSource, PublisherReference } from '../managementApi';
import { sourceRecord } from './managementFixtures';
import { runDetail } from './runInspectionFixtures';
import { submissionResponse } from './submissionFixtures';
export { historyPage } from './runInspectionFixtures';
export const CONTENT_PROFILE = '81000000-0000-4000-8000-000000000001';
export const PUBLISHER_REFERENCE = 'a1000000-0000-4000-8000-000000000001';
export const metadataOnly: ContentMetadata = { dispatchDisabled: true, qualificationScope: 'REGISTRATION_METADATA_ONLY', runtimeQualification: 'NOT_CHECKED' };
export function contentProfile(id = CONTENT_PROFILE): ContentProfile {
  return { ...metadataOnly, executionProfileId: id, productId: 'content-intelligence', pipeline: 'CONTENT_RSS_V1', runProfileVersion: 2, pipelineVersion: 1,
    sourceProfile: 'rss-atom-v1', inputFormat: 'RSS_ATOM', configurationSha256: 'c'.repeat(64), createdAt: '2026-11-01T12:34:56.654321Z',
    supportedSelection: { freshSourceCount: 1, retainedSilverCount: 0, publisherReferenceCount: 1 } };
}
export function contentSource(sourceId = 'alpha', profileId = CONTENT_PROFILE): ContentSource {
  const current = sourceRecord(sourceId);
  return { ...metadataOnly, executionProfileId: profileId, productId: 'content-intelligence', pipeline: 'CONTENT_RSS_V1', sourceId,
    revision: current.currentRevision, sourceProfile: 'rss-atom-v1', configurationSha256: current.configSha256 };
}
export function publisherReference(id = PUBLISHER_REFERENCE): PublisherReference {
  return { ...metadataOnly, artifactId: id, productId: 'content-intelligence', kind: 'PUBLISHER_REFERENCE', role: 'PUBLISHER_REFERENCE',
    contractVersion: 1, processorRevision: 1, nativeFormat: null, storageBindingId: 'registered-reference', nativeFingerprint: 'e'.repeat(64),
    verifiedAt: '2026-11-01T12:34:56.654321Z', identity: { referenceId: 'publishers', revision: 'r1' } };
}
export function contentRequest(): ContentRequest {
  return { pipeline: 'CONTENT_RSS_V1', executionProfileId: CONTENT_PROFILE, fetchSources: [{ sourceId: 'alpha', revision: 2 }], retainedSilverArtifactIds: [], publisherReferenceArtifactId: PUBLISHER_REFERENCE };
}
export const contentCommand = () => ({ key: 'content-intent-1', body: JSON.stringify(contentRequest()) });
export function contentRun(state: ReturnType<typeof runDetail>['state'] = 'QUEUED') {
  return { ...runDetail(1, state), sourceRevisions: contentRequest().fetchSources, inputArtifactIds: [PUBLISHER_REFERENCE] };
}
export const contentReceipt = (record = contentRun()) => submissionResponse(record);
