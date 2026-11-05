/** Native coverage projections. Integral quantities remain exact decimal strings. */
import { ReliabilityError, RELIABILITY_LABEL, SERVICE_ID, REFERENCE_ID, REFERENCE_REVISION, HEX,
  matches, isRecord, timestampMs, requireValid, record, fields, decimal, nullableText, serviceName } from './reliabilityApi';
import type { ReliabilityResponse } from './reliabilityApi';
export type CoverageReport = 'input-coverage' | 'reference-coverage';
export interface CoverageQuery { sourceId?: string; batchId?: string; serviceId?: string; limit?: number; offset?: number }
export type ReliabilityIdentity = Pick<ReliabilityResponse, 'publication' | 'productId' | 'productContractVersion' | 'processorRevision' | 'inputFormat' | 'origin' | 'label' | 'reference'>;
export const REASONS = ['BLANK_LINE', 'JSON_SYNTAX', 'JSON_NOT_OBJECT', 'JSON_DUPLICATE_KEY', 'JSON_NONFINITE', 'FIELD_MISSING', 'FIELD_NULL', 'FIELD_TYPE', 'FIELD_VALUE', 'TIMESTAMP_INVALID'] as const;
export const INPUT_TOTALS = ['inputBatchCount', 'inputFileCount', 'inputByteLength', 'physicalOccurrenceCount', 'validOccurrenceCount', 'acceptedRepresentativeCount', 'duplicateOccurrenceCount', 'rejectedOccurrenceCount', 'distinctValidIdentityCount', 'duplicateExcessInScope'] as const;
export const REFERENCE_TOTALS = ['eventCount', 'matchedEventCount', 'missingEventCount', 'eventServiceCount', 'matchedServiceCount', 'missingServiceCount', 'matchedServicesMissingOptionalAttributesCount', 'referenceServiceCount', 'unusedReferenceServiceCount'] as const;
export type InputTotals = Record<typeof INPUT_TOTALS[number], string>;
export type ReferenceTotals = Record<typeof REFERENCE_TOTALS[number], string>;
export interface InputCoverageRow {
 sourceId: string; batchId: string; batchFingerprint: string; ingestionId: string; receivedAt: string;
 inputFileCount: string; inputByteLength: string; physicalOccurrenceCount: string; validOccurrenceCount: string;
 acceptedRepresentativeCount: string; duplicateOccurrenceCount: string; rejectedOccurrenceCount: string;
 firstEventTime: string | null; lastEventTime: string | null; futureOccurrenceCount: string;
 minReceiptDelayUs: string | null; maxReceiptDelayUs: string | null; rejectedReasonCounts: Record<typeof REASONS[number], string>;
}
export interface ReferenceCoverageRow {
 serviceId: string; referenceStatus: 'MATCHED' | 'MISSING'; referenceUsage: 'USED' | 'UNUSED' | 'ABSENT';
 serviceName: string | null; ownerTeam: string | null; serviceTier: string | null;
 eventCount: string; missingReferenceEventCount: string; missingOptionalAttributes: ('owner_team' | 'service_tier')[];
}
interface CoverageResponse<R, T, F> extends ReliabilityIdentity { data: R[]; totals: T; filters: F; meta: ReliabilityResponse['meta'] }
export type InputCoverageResponse = CoverageResponse<InputCoverageRow, InputTotals, { sourceId: string | null; batchId: string | null }>;
export type ReferenceCoverageResponse = CoverageResponse<ReferenceCoverageRow, ReferenceTotals, { serviceId: string | null }>;
export type AnyCoverageResponse = InputCoverageResponse | ReferenceCoverageResponse;
const count = (v: unknown) => decimal(v, 2_000_000n);
const integer = (v: string) => BigInt(v);
export function reliabilityIdentity(value: ReliabilityIdentity): string {
 return JSON.stringify([value.publication, value.productId, value.productContractVersion, value.processorRevision, value.inputFormat,
  value.reference.referenceId, value.reference.revision, value.reference.snapshotFingerprint]);
}
export function coverageQueryError(report: CoverageReport, query: unknown): string | null {
 if (report !== 'input-coverage' && report !== 'reference-coverage') return 'Choose a supported coverage report.';
 const allowed = report === 'input-coverage' ? ['sourceId', 'batchId'] : ['serviceId'];
 if (!isRecord(query) || Reflect.ownKeys(query).some(k => typeof k !== 'string' || ![...allowed, 'limit', 'offset'].includes(k))) return 'Use only the supported exact filters for this report.';
 for (const key of allowed) if (Object.hasOwn(query, key) && !matches(query[key], key === 'batchId' ? REFERENCE_ID : SERVICE_ID)) return `${key} must be an exact lowercase identifier, without whitespace (maximum ${key === 'batchId' ? 64 : 128} characters).`;
 if (Object.hasOwn(query, 'limit') && (typeof query.limit !== 'number' || !Number.isInteger(query.limit) || query.limit < 1 || query.limit > 1000)) return 'Limit must be 1–1000.';
 if (Object.hasOwn(query, 'offset') && (typeof query.offset !== 'number' || !Number.isInteger(query.offset) || query.offset < 0 || query.offset > 1_000_000)) return 'Offset must be 0–1000000.';
 return null;
}
function envelope(value: unknown, query: CoverageQuery, report: CoverageReport) {
 if (coverageQueryError(report, query)) throw new ReliabilityError('invalid-argument');
 record(value); fields(value, ['publication','productId','productContractVersion','processorRevision','inputFormat','origin','label','reference','filters','data','meta','totals']);
 requireValid(matches(value.publication, HEX) && value.productId === 'application-reliability' && value.productContractVersion === 2 && value.processorRevision === 2
  && (value.inputFormat === 'JSONL' || value.inputFormat === 'PARQUET') && value.origin === 'synthetic' && value.label === RELIABILITY_LABEL);
 record(value.reference); fields(value.reference, ['referenceId','revision','snapshotFingerprint']);
 requireValid(matches(value.reference.referenceId, REFERENCE_ID) && matches(value.reference.revision, REFERENCE_REVISION) && matches(value.reference.snapshotFingerprint, HEX));
 record(value.filters); const keys = report === 'input-coverage' ? ['sourceId','batchId'] as const : ['serviceId'] as const;
 fields(value.filters, [...keys]); for (const k of keys) requireValid(value.filters[k] === (query[k] ?? null));
 record(value.meta); fields(value.meta, ['returned','limit','offset','hasMore']);
 requireValid(Array.isArray(value.data) && value.data.length <= (query.limit ?? 100) && value.meta.returned === value.data.length
  && value.meta.limit === (query.limit ?? 100) && value.meta.offset === (query.offset ?? 0) && typeof value.meta.hasMore === 'boolean'
  && (!value.meta.hasMore || value.data.length === value.meta.limit));
 record(value.totals); return value;
}
function occurrenceEquations(row: Record<string, unknown>) {
 const p = integer(count(row.physicalOccurrenceCount)), v = integer(count(row.validOccurrenceCount));
 const a = integer(count(row.acceptedRepresentativeCount)), d = integer(count(row.duplicateOccurrenceCount)), r = integer(count(row.rejectedOccurrenceCount));
 requireValid(p === v + r && v === a + d);
}
function timeUs(value: unknown): bigint { const ms = timestampMs(value); requireValid(ms !== null && typeof value === 'string'); return BigInt(ms) * 1000n + BigInt(value.slice(-4,-1)); }
function delay(value: unknown): bigint {
 requireValid(matches(value, /^(0|[1-9][0-9]*|-[1-9][0-9]*)$/) && value.length <= 19);
 const n = BigInt(value); requireValid(n >= -315537897599999999n && n <= 315537897599999999n); return n;
}
export function parseInputCoverage(raw: unknown, query: CoverageQuery = {}): InputCoverageResponse {
 const value = envelope(raw, query, 'input-coverage'), totals = value.totals as Record<string, unknown>;
 fields(totals, [...INPUT_TOTALS]); for (const k of INPUT_TOTALS) decimal(totals[k], k === 'inputBatchCount' ? 32n : k === 'inputFileCount' ? 128n : k === 'inputByteLength' ? 1073741824n : 2_000_000n);
 occurrenceEquations(totals);
 requireValid(BigInt(totals.validOccurrenceCount as string) === BigInt(totals.distinctValidIdentityCount as string) + BigInt(totals.duplicateExcessInScope as string)
  && BigInt(totals.acceptedRepresentativeCount as string) <= BigInt(totals.distinctValidIdentityCount as string));
 requireValid((totals.validOccurrenceCount === '0') === (totals.distinctValidIdentityCount === '0'));
 if (totals.inputBatchCount === '0') requireValid(INPUT_TOTALS.every(k => totals[k] === '0'));
 if (query.sourceId && query.batchId) requireValid(BigInt(totals.inputBatchCount as string) <= 1n);
 if (!query.sourceId && !query.batchId) requireValid(totals.distinctValidIdentityCount === totals.acceptedRepresentativeCount && totals.duplicateExcessInScope === totals.duplicateOccurrenceCount);
 let previous = ''; const rows = value.data as unknown[];
 for (const item of rows) {
  record(item); fields(item, ['sourceId','batchId','batchFingerprint','ingestionId','receivedAt','inputFileCount','inputByteLength','physicalOccurrenceCount','validOccurrenceCount','acceptedRepresentativeCount','duplicateOccurrenceCount','rejectedOccurrenceCount','firstEventTime','lastEventTime','futureOccurrenceCount','minReceiptDelayUs','maxReceiptDelayUs','rejectedReasonCounts']);
  requireValid(matches(item.sourceId, SERVICE_ID) && matches(item.batchId, REFERENCE_ID) && matches(item.batchFingerprint, HEX)
   && matches(item.ingestionId, /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/)
   && (!query.sourceId || query.sourceId === item.sourceId) && (!query.batchId || query.batchId === item.batchId));
  const key = `${item.sourceId}\0${item.batchId}\0${item.batchFingerprint}`; requireValid(key > previous); previous = key;
  const received = timeUs(item.receivedAt); requireValid(BigInt(decimal(item.inputFileCount, 16n)) >= 1n); decimal(item.inputByteLength, 1073741824n); occurrenceEquations(item);
  const valid = BigInt(item.validOccurrenceCount as string), future = BigInt(count(item.futureOccurrenceCount)); requireValid(future <= valid);
  if (valid === 0n) requireValid(item.firstEventTime === null && item.lastEventTime === null && item.minReceiptDelayUs === null && item.maxReceiptDelayUs === null && future === 0n);
  else { const first = timeUs(item.firstEventTime), last = timeUs(item.lastEventTime); requireValid(first <= last);
   const min = delay(item.minReceiptDelayUs), max = delay(item.maxReceiptDelayUs);
   requireValid(min === received - last && max === received - first && (future === 0n) === (min >= 0n) && (future === valid) === (max < 0n)); }
  record(item.rejectedReasonCounts); fields(item.rejectedReasonCounts, [...REASONS]);
  const rejected = BigInt(item.rejectedOccurrenceCount as string); let sum = 0n;
  for (const reason of REASONS) { const n = BigInt(count(item.rejectedReasonCounts[reason])); requireValid(n <= rejected); sum += n; } requireValid(sum >= rejected);
 }
 const batchCount = Number(totals.inputBatchCount); checkPage(value, batchCount);
 for (const k of INPUT_TOTALS.filter(k => !['inputBatchCount','distinctValidIdentityCount','duplicateExcessInScope'].includes(k))) {
  const sum = rows.reduce<bigint>((n, r) => n + BigInt((r as Record<string,string>)[k]!), 0n); requireValid(sum <= BigInt(totals[k] as string));
  if ((query.offset ?? 0) === 0 && !((value.meta as Record<string,unknown>).hasMore)) requireValid(sum === BigInt(totals[k] as string));
 }
 requireValid(BigInt(totals.inputFileCount as string) >= BigInt(totals.inputBatchCount as string) && BigInt(totals.inputFileCount as string) <= BigInt(totals.inputBatchCount as string)*16n);
 return value as unknown as InputCoverageResponse;
}
function checkPage(value: Record<string, unknown>, total: number) {
 const meta = value.meta as ReliabilityResponse['meta']; const expected = Math.max(0, Math.min(meta.limit, total - meta.offset));
 requireValid(meta.returned === expected && meta.hasMore === (meta.offset + meta.returned < total));
}
export function parseReferenceCoverage(raw: unknown, query: CoverageQuery = {}): ReferenceCoverageResponse {
 const value = envelope(raw, query, 'reference-coverage'), totals = value.totals as Record<string, unknown>;
 fields(totals, [...REFERENCE_TOTALS]); for (const k of REFERENCE_TOTALS) decimal(totals[k], ['referenceServiceCount','unusedReferenceServiceCount','matchedServiceCount','matchedServicesMissingOptionalAttributesCount'].includes(k) ? 10000n : 2_000_000n);
 const t = (k: typeof REFERENCE_TOTALS[number]) => BigInt(totals[k] as string);
 requireValid(t('eventCount') === t('matchedEventCount')+t('missingEventCount') && t('eventServiceCount') === t('matchedServiceCount')+t('missingServiceCount')
  && t('referenceServiceCount') === t('matchedServiceCount')+t('unusedReferenceServiceCount') && t('matchedServicesMissingOptionalAttributesCount') <= t('matchedServiceCount')
  && t('matchedServiceCount') <= t('matchedEventCount') && t('missingServiceCount') <= t('missingEventCount'));
 requireValid((t('matchedServiceCount') === 0n) === (t('matchedEventCount') === 0n) && (t('missingServiceCount') === 0n) === (t('missingEventCount') === 0n));
 const sums = Object.fromEntries(REFERENCE_TOTALS.map(k => [k, 0n])) as Record<typeof REFERENCE_TOTALS[number], bigint>;
 let previous = ''; for (const item of value.data as unknown[]) {
  record(item); fields(item, ['serviceId','referenceStatus','referenceUsage','serviceName','ownerTeam','serviceTier','eventCount','missingReferenceEventCount','missingOptionalAttributes']);
  requireValid(matches(item.serviceId, SERVICE_ID) && item.serviceId > previous && (!query.serviceId || item.serviceId === query.serviceId)); previous = item.serviceId;
  const events = BigInt(count(item.eventCount)), missing = BigInt(count(item.missingReferenceEventCount));
  requireValid(Array.isArray(item.missingOptionalAttributes)); sums.eventCount += events;
  if (item.referenceStatus === 'MISSING') {
   requireValid(item.referenceUsage === 'ABSENT' && events > 0n && missing === events && item.serviceName === null && item.ownerTeam === null && item.serviceTier === null && item.missingOptionalAttributes.length === 0);
   sums.missingEventCount += events; sums.missingServiceCount++; sums.eventServiceCount++;
  } else {
   requireValid(item.referenceStatus === 'MATCHED' && missing === 0n); serviceName(nullableText(item.serviceName));
   requireValid(item.ownerTeam === null || ['example-platform','example-service'].includes(item.ownerTeam as string));
   requireValid(item.serviceTier === null || ['critical','standard','experimental'].includes(item.serviceTier as string));
   const optional = [...(item.ownerTeam === null ? ['owner_team'] : []), ...(item.serviceTier === null ? ['service_tier'] : [])];
   requireValid(JSON.stringify(item.missingOptionalAttributes) === JSON.stringify(optional)); sums.referenceServiceCount++;
   if (events === 0n) { requireValid(item.referenceUsage === 'UNUSED'); sums.unusedReferenceServiceCount++; }
   else { requireValid(item.referenceUsage === 'USED'); sums.matchedEventCount += events; sums.matchedServiceCount++; sums.eventServiceCount++; if (optional.length) sums.matchedServicesMissingOptionalAttributesCount++; }
  }
 }
 checkPage(value, Number(t('referenceServiceCount')+t('missingServiceCount')));
 if (query.serviceId) requireValid(t('referenceServiceCount')+t('missingServiceCount') <= 1n);
 for (const k of REFERENCE_TOTALS) { requireValid(sums[k] <= t(k)); if ((query.offset ?? 0) === 0 && !(value.meta as ReliabilityResponse['meta']).hasMore) requireValid(sums[k] === t(k)); }
 return value as unknown as ReferenceCoverageResponse;
}
export async function fetchCoverage(report: CoverageReport, query: CoverageQuery, signal: AbortSignal): Promise<AnyCoverageResponse> {
 if (coverageQueryError(report, query)) throw new ReliabilityError('invalid-argument');
 const params = new URLSearchParams({ limit: String(query.limit ?? 100), offset: String(query.offset ?? 0) });
 for (const k of ['sourceId','batchId','serviceId'] as const) if (query[k] !== undefined) params.set(k, query[k]);
 let response: Response;
 try { response = await fetch(`/api/v1/reliability/${report}?${params}`, {signal, cache:'no-store',credentials:'omit',headers:{Accept:'application/json'}}); }
 catch (error) { if (signal.aborted) throw error; throw new ReliabilityError('network'); }
 if (response.status !== 200) {
  if (response.status === 503) { let body: unknown; try { body = await response.json(); } catch { /* safe fixed message */ }
   throw new ReliabilityError(isRecord(body) && body.code === 'ANALYTICS_CAPACITY_EXCEEDED' ? 'capacity' : 'unavailable'); }
  throw new ReliabilityError(response.status === 400 ? 'invalid-argument' : response.status === 504 ? 'timeout' : 'execution');
 }
 try { requireValid(response.headers.get('content-type')?.split(';',1)[0]?.trim().toLowerCase() === 'application/json');
  const body: unknown = await response.json(); return report === 'input-coverage' ? parseInputCoverage(body,query) : parseReferenceCoverage(body,query);
 } catch (error) { if (signal.aborted) throw error; throw new ReliabilityError('invalid-response'); }
}
