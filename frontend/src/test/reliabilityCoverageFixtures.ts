import { reliabilityFixture } from './reliabilityFixtures';
import { REASONS } from '../reliabilityCoverageApi';
import type { CoverageQuery, InputCoverageResponse, ReferenceCoverageResponse } from '../reliabilityCoverageApi';
export function inputCoverageFixture(query: CoverageQuery = {}): InputCoverageResponse {
 const base = reliabilityFixture();
 return { ...base, filters: { sourceId: query.sourceId ?? null, batchId: query.batchId ?? null },
  meta: { returned: 1, limit: query.limit ?? 100, offset: query.offset ?? 0, hasMore: false },
  totals: { inputBatchCount:'1',inputFileCount:'1',inputByteLength:'1000',physicalOccurrenceCount:'4',validOccurrenceCount:'2',acceptedRepresentativeCount:'1',duplicateOccurrenceCount:'1',rejectedOccurrenceCount:'2',distinctValidIdentityCount:'1',duplicateExcessInScope:'1' },
  data: [{ sourceId: query.sourceId ?? 'fixture', batchId: query.batchId ?? 'batch', batchFingerprint:'c'.repeat(64), ingestionId:'12345678-1234-1234-1234-123456789abc', receivedAt:'2026-10-20T12:00:00.000001Z', inputFileCount:'1', inputByteLength:'1000', physicalOccurrenceCount:'4', validOccurrenceCount:'2', acceptedRepresentativeCount:'1', duplicateOccurrenceCount:'1', rejectedOccurrenceCount:'2', firstEventTime:'2026-10-20T12:00:00.000002Z', lastEventTime:'2026-10-20T12:00:00.000002Z', futureOccurrenceCount:'2',minReceiptDelayUs:'-1',maxReceiptDelayUs:'-1',rejectedReasonCounts:Object.fromEntries(REASONS.map(reason => [reason, ['FIELD_NULL','FIELD_VALUE'].includes(reason) ? '2' : '0'])) as InputCoverageResponse['data'][number]['rejectedReasonCounts'] }] };
}
export function referenceCoverageFixture(query: CoverageQuery = {}): ReferenceCoverageResponse {
 const base = reliabilityFixture();
 return { ...base, filters: { serviceId: query.serviceId ?? null }, meta: { returned:1,limit:query.limit ?? 100,offset:query.offset ?? 0,hasMore:false },
  totals: { eventCount:'2',matchedEventCount:'2',missingEventCount:'0',eventServiceCount:'1',matchedServiceCount:'1',missingServiceCount:'0',matchedServicesMissingOptionalAttributesCount:'1',referenceServiceCount:'1',unusedReferenceServiceCount:'0' },
  data:[{ serviceId:query.serviceId ?? 'alpha',referenceStatus:'MATCHED',referenceUsage:'USED',serviceName:'Alpha Service',ownerTeam:null,serviceTier:null,eventCount:'2',missingReferenceEventCount:'0',missingOptionalAttributes:['owner_team','service_tier'] }] };
}
