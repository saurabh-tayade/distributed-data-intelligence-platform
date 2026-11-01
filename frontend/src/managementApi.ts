/** Management v1 wire contracts. Analytics publication envelopes remain independent. */
export type ProductId = 'content-intelligence' | 'application-reliability';
export interface SourceIdentity { productId: ProductId; sourceId: string }
export type SourceConfig = { configVersion: 1; inputStorageProfileId: string } & (
  { profile: 'rss-atom-v1'; dataOrigin: 'publisher'; feedEndpointId: string; registeredProfileSha256: null }
  | { profile: 'request-jsonl-v2' | 'request-parquet-v1'; dataOrigin: 'synthetic'; feedEndpointId: null; registeredProfileSha256: string }
);
export interface ManagementEnvelope<T> { apiVersion: 1; data: T; meta: { correlationId: string } }
export interface ManagementPage<T> extends ManagementEnvelope<T[]> {
  meta: { correlationId: string; limit: number; hasMore: boolean; nextCursor: string | null };
}
export interface SourceSummary {
  productId: ProductId; sourceId: string; displayName: string; enabled: boolean;
  currentRevision: number; rowVersion: string; configSha256: string; createdAt: string; updatedAt: string;
}
export interface SourcesQuery { productId?: ProductId; enabled?: boolean; limit?: number; cursor?: string }
export type RevisionQuery = Pick<SourcesQuery, 'limit' | 'cursor'>;
export interface SourceRecord extends SourceSummary { config: SourceConfig }
export interface SourceRevision extends SourceIdentity {
  revision: number; displayName: string; configSha256: string; createdAt: string; config: SourceConfig;
}
export interface SourceResponse extends ManagementEnvelope<SourceRecord> { etag: string }
export type RevisionsPage = ManagementPage<SourceRevision>;
export type SourcesPage = ManagementPage<SourceSummary>;
export const SOURCE_PAGE_LIMIT = 20;
export const RUN_STATES = ['QUEUED', 'RUNNING', 'PAUSED', 'RECOVERY_REQUIRED', 'SUCCEEDED', 'FAILED', 'CANCEL_REQUESTED', 'CANCELLED'] as const;
export type RunState = typeof RUN_STATES[number];
export const RUN_STAGES = ['CHECK_INPUTS', 'FETCH_SOURCE', 'STORE_BRONZE', 'SILVER', 'PREPARE_GOLD', 'PUBLISH_GOLD', 'PUBLISH_CONTENT', 'PUBLISH_RELIABILITY', 'VERIFY_RESULT'] as const;
// RunInspectionValues.CODES: public classifications, never arbitrary private diagnostics.
export const RUN_OUTCOMES = ['RESUME_QUEUED', 'RESUME_CHECKPOINT_UNSUPPORTED', 'RESUME_EVIDENCE_INVALID', 'RESUME_EVIDENCE_UNAVAILABLE', 'RESUME_NAMESPACE_UNSAFE',
  'ACCEPTED', 'CLAIMED', 'STARTED', 'VERIFIED', 'SUCCEEDED', 'FAILED', 'CANCELLED', 'INTERRUPTED', 'UNKNOWN', 'UPDATED', 'UNCHANGED',
  'INPUT_UNAVAILABLE', 'INPUT_INVALID', 'INPUT_CHANGED', 'PUBLICATION_UNAVAILABLE', 'PUBLICATION_INVALID', 'PUBLICATION_CHANGED', 'INCOMPLETE_OUTPUT',
  'FETCH_FAILED', 'FETCH_EVIDENCE_LOST', 'BRONZE_CONFLICT', 'BRONZE_ABSENT', 'BRONZE_PAYLOAD_ONLY', 'STAGE_TIMEOUT', 'STAGE_OUTPUT_LIMIT',
  'PROCESS_START_FAILED', 'TERMINATION_UNCONFIRMED', 'RUNTIME_MISMATCH', 'RESOURCE_LIMIT', 'SOURCE_DISABLED', 'RETRY_NOT_SAFE', 'TOO_LATE',
  'RECOVERY_REQUIRED', 'RECOVERY_HANDOFF', 'RECOVERY_CLOSED', 'STATE_CONFLICT', 'RECOVERED_PUBLICATION', 'PUBLICATION_ABSENT', 'DEPENDENCY_CHANGED',
  'DEPENDENCY_INVALID', 'RECONCILED_SUCCEEDED', 'RECONCILED_FAILED', 'RECONCILED_PAUSED', 'RECONCILED_CANCELLED', 'RECOVERED_CHECKPOINT',
  'VERIFIED_PUBLICATION', 'OUTPUT_ABSENT', 'OUTPUT_INCOMPLETE', 'OUTPUT_CHANGED', 'OUTPUT_INVALID', 'OUTPUT_UNAVAILABLE', 'DEPENDENCY_UNAVAILABLE',
  'RUNTIME_INCOMPATIBLE', 'EVIDENCE_INVALID', 'EVIDENCE_UNAVAILABLE'] as const;
export interface RunResult {
  artifactId: string; nativeFingerprint: string; productContractVersion: 1 | 2; processorRevision: 1 | 2;
  verificationStatus: 'VERIFIED_AT_EXECUTION'; verifiedAt: string;
}
export interface RunSummary {
  runId: string; productId: ProductId; pipeline: 'CONTENT_RSS_V1' | 'RELIABILITY_V2'; state: RunState;
  currentStage: typeof RUN_STAGES[number] | null; rowVersion: string; requestedAt: string; finishedAt: string | null;
  outcomeCode: typeof RUN_OUTCOMES[number] | null; retryEligibility: boolean; result: RunResult | null; dataOrigin?: 'synthetic';
}
export interface RunsQuery { productId?: ProductId; sourceId?: string; state?: RunState; limit?: number; cursor?: string }
export type RunsPage = ManagementPage<RunSummary>;
export const ATTEMPT_STATES = ['CLAIMED', 'RUNNING', 'SUCCEEDED', 'FAILED', 'CANCELLED', 'INTERRUPTED'] as const;
export const CONTROL_TYPES = ['CANCEL', 'RETRY', 'RECONCILE', 'RESUME'] as const;
export const CONTROL_STATES = ['REQUESTED', 'APPLIED', 'TOO_LATE', 'REJECTED'] as const;
export const AUDIT_STATES = [...RUN_STATES, 'CLAIMED', 'INTERRUPTED', 'INTENT', 'VERIFIED', 'UNKNOWN', 'REQUESTED', 'APPLIED', 'TOO_LATE', 'REJECTED', 'RECOVERY_ONLY', 'READY'] as const;
export const AUDIT_ACTIONS = ['CONTENT_RESUME_QUEUED', 'CONTENT_RESUME_CLAIMED', 'RUN_SUBMITTED', 'RUN_CLAIMED', 'RUN_STARTED', 'RUN_CANCEL_REQUESTED', 'RUN_CANCELLED', 'RUN_FAILED', 'RUN_SUCCEEDED', 'RUN_RECOVERY_REQUIRED', 'RUN_REQUEUED',
  'ATTEMPT_CLAIMED', 'ATTEMPT_STARTED', 'ATTEMPT_FINISHED', 'ATTEMPT_INTERRUPTED', 'STAGE_INTENT', 'STAGE_LAUNCH_AUTHORIZED', 'STAGE_STARTED', 'STAGE_VERIFIED', 'STAGE_FAILED', 'STAGE_UNKNOWN',
  'CONTROL_REQUESTED', 'CONTROL_APPLIED', 'CONTROL_TOO_LATE', 'CONTROL_REJECTED', 'RECOVERY_HANDOFF', 'RECOVERY_CLOSED', 'SOURCE_DISABLED_AT_DISPATCH',
  'RECOVERY_ATTEMPT_RUNNING', 'RECOVERY_STAGE_INTENT', 'RECOVERY_LAUNCH_AUTHORIZED', 'RECOVERY_CHILD_STARTED', 'RECOVERY_STAGE_OBSERVED',
  'RECOVERY_ATTEMPT_FINISHED', 'RECOVERY_RUN_FINISHED', 'RECOVERY_CONTROL_APPLIED', 'RECOVERY_SLOT_RELEASED', 'RECOVERY_FINALIZED'] as const;
export const RUN_COUNT_KEYS = ['accepted', 'rejected', 'articles', 'occurrences', 'events', 'windows', 'rows', 'files', 'bytes'] as const;
export interface LatestControl {
  controlId: string; type: typeof CONTROL_TYPES[number]; state: typeof CONTROL_STATES[number]; reason: typeof RUN_OUTCOMES[number] | null;
  requestedAt: string; appliedAt: string | null; rowVersion: string;
}
export interface ControlReceipt extends LatestControl { runId: string; runVersion: string }
export interface RunDetail extends RunSummary {
  counts: Partial<Record<typeof RUN_COUNT_KEYS[number], string>> | null;
  attempts: { count: string; currentAttemptId: string | null }; latestControl: LatestControl | null;
  sourceRevisions: { sourceId: string; revision: number }[]; inputArtifactIds: string[];
}
export interface RunDetailResponse extends ManagementEnvelope<RunDetail> { etag: string }
export interface ControlResponse extends ManagementEnvelope<ControlReceipt> { etag: string }
export interface RunAttempt {
  attemptId: string; attemptNo: number; kind: 'EXECUTE' | 'RECONCILE'; state: typeof ATTEMPT_STATES[number];
  startedAt: string; finishedAt: string | null; outcomeCode: typeof RUN_OUTCOMES[number] | null; stageCount: string;
}
export interface AuditEvent {
  eventSeq: string; timestamp: string; action: typeof AUDIT_ACTIONS[number]; beforeState: typeof AUDIT_STATES[number] | null;
  afterState: typeof AUDIT_STATES[number] | null; code: typeof RUN_OUTCOMES[number] | null; beforeVersion: string | null; afterVersion: string | null;
  context: { attemptId: string | null; controlId: string | null };
}

export type ManagementErrorKind = 'invalid-request' | 'local-boundary' | 'not-found' | 'capacity'
  | 'unavailable' | 'server' | 'network' | 'invalid-response';
const messages: Record<ManagementErrorKind, string> = {
  'invalid-request': 'The source request or page cursor was not accepted. Review the filters or start from the first page.',
  'local-boundary': 'Local access was rejected. Check the approved UI origin and API host.',
  'not-found': 'Source management is unavailable at this address. Check that the local service is running in API mode.',
  capacity: 'The management service is at capacity. Refresh when capacity is available.',
  unavailable: 'The source catalog is unavailable. Check the local management service, then try again.',
  server: 'The management service could not complete the source request. Try again after checking the service.',
  network: 'Cannot reach the local management service. Check the API and development proxy, then try again.',
  'invalid-response': 'The API returned data that does not match the source contract. No source records are displayed.',
};
export class ManagementError extends Error {
  constructor(public readonly kind: ManagementErrorKind, public readonly code: string | null = null,
    public readonly correlationId: string | null = null, public readonly retryable: boolean | null = null) {
    super(messages[kind]); this.name = 'ManagementError';
  }
}

const uuid = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
export function isManagementId(value: unknown): value is string { return typeof value === 'string' && uuid.test(value); }
const cursorPattern = /^[A-Za-z0-9_-]+\.[A-Za-z0-9_-]{43}$/;
const isProduct = (value: unknown): value is ProductId => value === 'content-intelligence' || value === 'application-reliability';
export function isSourceIdentity(value: { productId: unknown; sourceId: unknown }): value is SourceIdentity {
  return isProduct(value.productId) && isSourceId(value.sourceId);
}
export function isSourceId(value: unknown): value is string { return typeof value === 'string' && /^[a-z0-9][a-z0-9_-]{0,127}$/.test(value); }
function member<T extends string>(values: readonly T[], value: unknown): value is T { return typeof value === 'string' && values.includes(value as T); }
function need(value: unknown): asserts value { if (!value) throw new ManagementError('invalid-response'); }
function object(value: unknown): Record<string, unknown> {
  need(value !== null && typeof value === 'object' && !Array.isArray(value));
  return value as Record<string, unknown>;
}
function fields(value: Record<string, unknown>, names: string[]) {
  need(Object.keys(value).length === names.length && names.every((name) => Object.hasOwn(value, name)));
}
function timestamp(value: unknown): string {
  need(typeof value === 'string' && /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z$/.test(value) && !value.startsWith('0000'));
  const milliseconds = value.slice(0, 23) + 'Z', date = new Date(milliseconds);
  need(Number.isFinite(date.getTime()) && date.toISOString() === milliseconds);
  return value;
}
function version(value: unknown): string {
  need(typeof value === 'string' && /^(0|[1-9][0-9]{0,18})$/.test(value) && BigInt(value) <= 9223372036854775807n);
  return value;
}
function cursor(value: unknown): value is string {
  return typeof value === 'string' && value.length <= 512 && cursorPattern.test(value);
}
function label(value: string): boolean {
  const characters = Array.from(value);
  // Java String.isBlank permits these nonbreaking spaces, unlike JavaScript trim().
  const nonblank = characters.some((character) => !/\p{White_Space}/u.test(character) || '\u00a0\u2007\u202f'.includes(character));
  return nonblank && new TextEncoder().encode(value).length <= 128 && characters.every((character) => {
    const point = character.codePointAt(0)!;
    return point >= 32 && (point < 127 || point > 159) && (point < 0xd800 || point > 0xdfff);
  });
}
function parameters(query: SourcesQuery): URLSearchParams {
  const limit = query.limit ?? SOURCE_PAGE_LIMIT;
  if (!Number.isInteger(limit) || limit < 1 || limit > 100
    || (query.productId !== undefined && !isProduct(query.productId))
    || (query.enabled !== undefined && typeof query.enabled !== 'boolean')
    || (query.cursor !== undefined && !cursor(query.cursor))) throw new ManagementError('invalid-request');
  const result = new URLSearchParams({ limit: String(limit) });
  if (query.productId !== undefined) result.set('productId', query.productId);
  if (query.enabled !== undefined) result.set('enabled', String(query.enabled));
  if (query.cursor !== undefined) result.set('cursor', query.cursor);
  return result;
}

export function runsParameters(query: RunsQuery): URLSearchParams {
  // Reuse page/product validation; no source admission-status filter belongs on runs.
  const page: SourcesQuery = {};
  if (query.limit !== undefined) page.limit = query.limit;
  if (query.cursor !== undefined) page.cursor = query.cursor;
  if (query.productId !== undefined) page.productId = query.productId;
  const result = parameters(page);
  if ((query.sourceId !== undefined && !isSourceId(query.sourceId))
    || (query.state !== undefined && !member(RUN_STATES, query.state))) throw new ManagementError('invalid-request');
  if (query.sourceId !== undefined) result.set('sourceId', query.sourceId);
  if (query.state !== undefined) result.set('state', query.state);
  return result;
}

function parsePage<T>(value: unknown, query: RevisionQuery, parseRow: (value: unknown) => T): ManagementPage<T> {
  parameters(query);
  const root = object(value); fields(root, ['apiVersion', 'data', 'meta']); need(root.apiVersion === 1);
  const meta = object(root.meta); fields(meta, ['correlationId', 'limit', 'hasMore', 'nextCursor']);
  need(typeof meta.correlationId === 'string' && uuid.test(meta.correlationId));
  const limit = query.limit ?? SOURCE_PAGE_LIMIT;
  need(meta.limit === limit && typeof meta.hasMore === 'boolean');
  need(meta.nextCursor === null || cursor(meta.nextCursor));
  need(meta.hasMore === (meta.nextCursor !== null) && (meta.nextCursor === null || meta.nextCursor !== query.cursor));
  need(Array.isArray(root.data) && root.data.length <= limit && (!meta.hasMore || root.data.length === limit));
  const data = root.data.map(parseRow);
  return { apiVersion: 1, data, meta: { correlationId: meta.correlationId, limit, hasMore: meta.hasMore, nextCursor: meta.nextCursor } };
}

const summaryFields = ['productId', 'sourceId', 'displayName', 'enabled', 'currentRevision', 'rowVersion', 'configSha256', 'createdAt', 'updatedAt'];
function summary(row: Record<string, unknown>, query: SourcesQuery = {}): SourceSummary {
  need(isProduct(row.productId) && (query.productId === undefined || row.productId === query.productId));
  need(typeof row.sourceId === 'string' && /^[a-z0-9][a-z0-9_-]{0,127}$/.test(row.sourceId));
  need(typeof row.displayName === 'string' && label(row.displayName));
  need(typeof row.enabled === 'boolean' && (query.enabled === undefined || row.enabled === query.enabled));
  need(typeof row.currentRevision === 'number' && Number.isInteger(row.currentRevision) && row.currentRevision >= 1 && row.currentRevision <= 1000);
  need(typeof row.configSha256 === 'string' && /^[0-9a-f]{64}$/.test(row.configSha256));
  return { productId: row.productId, sourceId: row.sourceId, displayName: row.displayName, enabled: row.enabled,
    currentRevision: row.currentRevision, rowVersion: version(row.rowVersion), configSha256: row.configSha256,
    createdAt: timestamp(row.createdAt), updatedAt: timestamp(row.updatedAt) };
}

export function parseSources(value: unknown, query: SourcesQuery = {}): SourcesPage {
  parameters(query);
  let previous = '';
  return parsePage(value, query, (item) => {
    const row = object(item); fields(row, summaryFields);
    const source = summary(row, query), key = `${source.productId}:${source.sourceId}`;
    need(previous < key); previous = key; // ASCII, matching the API's binary ordering.
    return source;
  });
}

function configuration(value: unknown, product: ProductId): SourceConfig {
  const config = object(value);
  fields(config, ['configVersion', 'profile', 'dataOrigin', 'feedEndpointId', 'registeredProfileSha256', 'inputStorageProfileId']);
  need(config.configVersion === 1 && typeof config.inputStorageProfileId === 'string' && uuid.test(config.inputStorageProfileId));
  if (product === 'content-intelligence') {
    need(config.profile === 'rss-atom-v1' && config.dataOrigin === 'publisher' && config.registeredProfileSha256 === null
      && typeof config.feedEndpointId === 'string' && uuid.test(config.feedEndpointId));
    return { configVersion: 1, profile: config.profile, dataOrigin: config.dataOrigin, feedEndpointId: config.feedEndpointId,
      registeredProfileSha256: null, inputStorageProfileId: config.inputStorageProfileId };
  }
  need((config.profile === 'request-jsonl-v2' || config.profile === 'request-parquet-v1') && config.dataOrigin === 'synthetic'
    && config.feedEndpointId === null && typeof config.registeredProfileSha256 === 'string' && /^[0-9a-f]{64}$/.test(config.registeredProfileSha256));
  return { configVersion: 1, profile: config.profile, dataOrigin: config.dataOrigin, feedEndpointId: null,
    registeredProfileSha256: config.registeredProfileSha256, inputStorageProfileId: config.inputStorageProfileId };
}

function matches(row: SourceIdentity, identity: SourceIdentity) {
  need(row.productId === identity.productId && row.sourceId === identity.sourceId);
}
export function parseSource(value: unknown, identity: SourceIdentity, etag: string | null): SourceResponse {
  const root = object(value); fields(root, ['apiVersion', 'data', 'meta']); need(root.apiVersion === 1);
  const meta = object(root.meta); fields(meta, ['correlationId']);
  need(typeof meta.correlationId === 'string' && uuid.test(meta.correlationId));
  const row = object(root.data); fields(row, [...summaryFields, 'config']);
  const source = summary(row); matches(source, identity);
  need(etag === `"${source.rowVersion}"`);
  return { apiVersion: 1, data: { ...source, config: configuration(row.config, source.productId) },
    meta: { correlationId: meta.correlationId }, etag };
}

export function parseRevisions(value: unknown, identity: SourceIdentity, query: RevisionQuery = {}): RevisionsPage {
  let previous = 0;
  return parsePage(value, query, (item) => {
    const row = object(item); fields(row, ['productId', 'sourceId', 'revision', 'displayName', 'configSha256', 'createdAt', 'config']);
    need(isSourceIdentity({ productId: row.productId, sourceId: row.sourceId }));
    need(row.productId === identity.productId && row.sourceId === identity.sourceId);
    need(typeof row.revision === 'number' && Number.isInteger(row.revision) && row.revision > previous && row.revision <= 1000);
    need(typeof row.displayName === 'string' && label(row.displayName));
    need(typeof row.configSha256 === 'string' && /^[0-9a-f]{64}$/.test(row.configSha256));
    previous = row.revision;
    return { ...identity, revision: row.revision, displayName: row.displayName, configSha256: row.configSha256,
      createdAt: timestamp(row.createdAt), config: configuration(row.config, identity.productId) };
  });
}

function runSummary(item: unknown, query: RunsQuery = {}): RunSummary {
  const row = object(item);
  const reliability = row.pipeline === 'RELIABILITY_V2';
  fields(row, ['runId', 'productId', 'pipeline', 'state', 'currentStage', 'rowVersion', 'requestedAt', 'finishedAt',
    'outcomeCode', 'retryEligibility', 'result', ...(reliability ? ['dataOrigin'] : [])]);
  need(isManagementId(row.runId));
  need(isProduct(row.productId) && (query.productId === undefined || row.productId === query.productId));
  need(reliability ? row.productId === 'application-reliability' && row.dataOrigin === 'synthetic'
    : row.pipeline === 'CONTENT_RSS_V1' && row.productId === 'content-intelligence');
  need(member(RUN_STATES, row.state) && (query.state === undefined || row.state === query.state));
  need(row.currentStage === null || member(RUN_STAGES, row.currentStage));
  need(row.outcomeCode === null || member(RUN_OUTCOMES, row.outcomeCode));
  need(typeof row.retryEligibility === 'boolean');
  let result: RunResult | null = null;
  if (row.result !== null) {
    const record = object(row.result); fields(record, ['artifactId', 'nativeFingerprint', 'productContractVersion', 'processorRevision', 'verificationStatus', 'verifiedAt']);
    const revision = reliability ? 2 : 1;
    need(row.state === 'SUCCEEDED' && typeof record.artifactId === 'string' && uuid.test(record.artifactId));
    need(typeof record.nativeFingerprint === 'string' && /^[0-9a-f]{64}$/.test(record.nativeFingerprint));
    need(record.productContractVersion === revision && record.processorRevision === revision && record.verificationStatus === 'VERIFIED_AT_EXECUTION');
    result = { artifactId: record.artifactId, nativeFingerprint: record.nativeFingerprint, productContractVersion: revision,
      processorRevision: revision, verificationStatus: record.verificationStatus, verifiedAt: timestamp(record.verifiedAt) };
  }
  // Persisted run constraints require terminal timestamps and a verified result for SUCCEEDED.
  need(row.state !== 'SUCCEEDED' || result !== null);
  need(!['SUCCEEDED', 'FAILED', 'CANCELLED'].includes(row.state) || row.finishedAt !== null);
  return { runId: row.runId, productId: row.productId, pipeline: reliability ? 'RELIABILITY_V2' : 'CONTENT_RSS_V1', state: row.state,
    currentStage: row.currentStage, rowVersion: version(row.rowVersion), requestedAt: timestamp(row.requestedAt),
    finishedAt: row.finishedAt === null ? null : timestamp(row.finishedAt), outcomeCode: row.outcomeCode, retryEligibility: row.retryEligibility,
    result, ...(reliability ? { dataOrigin: 'synthetic' as const } : {}) };
}
export function parseRuns(value: unknown, query: RunsQuery = {}): RunsPage {
  runsParameters(query);
  const ids = new Set<string>();
  return parsePage(value, query, (item) => {
    const row = runSummary(item, query); need(!ids.has(row.runId)); ids.add(row.runId); return row;
  });
}

function recordEnvelope(value: unknown) {
  const root = object(value); fields(root, ['apiVersion', 'data', 'meta']); need(root.apiVersion === 1);
  const meta = object(root.meta); fields(meta, ['correlationId']); need(isManagementId(meta.correlationId));
  return { row: object(root.data), meta: { correlationId: meta.correlationId } };
}
const nullableTime = (value: unknown) => value === null ? null : timestamp(value);
function nullableId(value: unknown): string | null { need(value === null || isManagementId(value)); return value; }
function publicCode(value: unknown) { need(value === null || member(RUN_OUTCOMES, value)); return value; }
function latestControl(value: unknown): LatestControl {
  const row = object(value); fields(row, ['controlId', 'type', 'state', 'reason', 'requestedAt', 'appliedAt', 'rowVersion']);
  need(isManagementId(row.controlId) && member(CONTROL_TYPES, row.type) && member(CONTROL_STATES, row.state));
  need(row.state === 'REQUESTED' || (row.reason !== null && row.appliedAt !== null));
  return { controlId: row.controlId, type: row.type, state: row.state, reason: publicCode(row.reason), requestedAt: timestamp(row.requestedAt),
    appliedAt: nullableTime(row.appliedAt), rowVersion: version(row.rowVersion) };
}
function tag(value: string | null, rowVersion: string): string { need(value === `"${rowVersion}"`); return value; }

export function parseRunDetail(value: unknown, runId: string, etag: string | null): RunDetailResponse {
  const { row, meta } = recordEnvelope(value);
  const extra = ['counts', 'attempts', 'latestControl', 'sourceRevisions', 'inputArtifactIds'];
  need(extra.every((key) => Object.hasOwn(row, key)));
  const summary = runSummary(Object.fromEntries(Object.entries(row).filter(([key]) => !extra.includes(key))));
  need(summary.runId === runId);
  let counts: RunDetail['counts'] = null;
  if (row.counts !== null) {
    const entries = object(row.counts); need(Object.keys(entries).every((key) => member(RUN_COUNT_KEYS, key)));
    counts = Object.fromEntries(Object.entries(entries).map(([key, count]) => [key, version(count)]));
  }
  const attempts = object(row.attempts); fields(attempts, ['count', 'currentAttemptId']);
  const count = version(attempts.count); need(BigInt(count) <= 20n);
  need(Array.isArray(row.sourceRevisions) && row.sourceRevisions.length <= (summary.productId === 'content-intelligence' ? 8 : 32));
  let previous = '';
  const sourceRevisions = row.sourceRevisions.map((item) => {
    const source = object(item); fields(source, ['sourceId', 'revision']);
    need(isSourceId(source.sourceId) && previous < source.sourceId); previous = source.sourceId;
    need(typeof source.revision === 'number' && Number.isInteger(source.revision) && source.revision >= 1 && source.revision <= 1000);
    return { sourceId: source.sourceId, revision: source.revision };
  });
  need(Array.isArray(row.inputArtifactIds) && row.inputArtifactIds.length <= 129 && row.inputArtifactIds.every(isManagementId));
  return { apiVersion: 1, meta, etag: tag(etag, summary.rowVersion), data: { ...summary, counts,
    attempts: { count, currentAttemptId: nullableId(attempts.currentAttemptId) }, sourceRevisions, inputArtifactIds: row.inputArtifactIds,
    latestControl: row.latestControl === null ? null : latestControl(row.latestControl) } };
}
export function parseAttempts(value: unknown, query: RevisionQuery = {}): ManagementPage<RunAttempt> {
  let previous = 0; const ids = new Set<string>();
  return parsePage(value, query, (item) => {
    const row = object(item); fields(row, ['attemptId', 'attemptNo', 'kind', 'state', 'startedAt', 'finishedAt', 'outcomeCode', 'stageCount']);
    need(isManagementId(row.attemptId) && !ids.has(row.attemptId)); ids.add(row.attemptId);
    need(typeof row.attemptNo === 'number' && Number.isInteger(row.attemptNo) && row.attemptNo > previous && row.attemptNo <= 20); previous = row.attemptNo;
    need((row.kind === 'EXECUTE' || row.kind === 'RECONCILE') && member(ATTEMPT_STATES, row.state));
    need(['CLAIMED', 'RUNNING'].includes(row.state) || row.finishedAt !== null);
    return { attemptId: row.attemptId, attemptNo: row.attemptNo, kind: row.kind, state: row.state, startedAt: timestamp(row.startedAt),
      finishedAt: nullableTime(row.finishedAt), outcomeCode: publicCode(row.outcomeCode), stageCount: version(row.stageCount) };
  });
}
export function parseEvents(value: unknown, query: RevisionQuery = {}): ManagementPage<AuditEvent> {
  let previous = 0n;
  return parsePage(value, query, (item) => {
    const row = object(item); fields(row, ['eventSeq', 'timestamp', 'action', 'beforeState', 'afterState', 'code', 'beforeVersion', 'afterVersion', 'context']);
    const eventSeq = version(row.eventSeq); need(BigInt(eventSeq) > previous); previous = BigInt(eventSeq);
    need(member(AUDIT_ACTIONS, row.action));
    need(row.beforeState === null || member(AUDIT_STATES, row.beforeState)); need(row.afterState === null || member(AUDIT_STATES, row.afterState));
    const context = object(row.context); fields(context, ['attemptId', 'controlId']);
    return { eventSeq, timestamp: timestamp(row.timestamp), action: row.action, beforeState: row.beforeState, afterState: row.afterState,
      code: publicCode(row.code), beforeVersion: row.beforeVersion === null ? null : version(row.beforeVersion), afterVersion: row.afterVersion === null ? null : version(row.afterVersion),
      context: { attemptId: nullableId(context.attemptId), controlId: nullableId(context.controlId) } };
  });
}
export function parseControl(value: unknown, runId: string, controlId: string, etag: string | null): ControlResponse {
  const { row, meta } = recordEnvelope(value);
  need(Object.hasOwn(row, 'runId') && row.runId === runId && Object.hasOwn(row, 'runVersion'));
  const summary = latestControl(Object.fromEntries(Object.entries(row).filter(([key]) => !['runId', 'runVersion'].includes(key))));
  need(summary.controlId === controlId);
  return { apiVersion: 1, meta, etag: tag(etag, summary.rowVersion), data: { ...summary, runId, runVersion: version(row.runVersion) } };
}

const failures: Record<number, { kind: ManagementErrorKind; code: string }> = {
  400: { kind: 'invalid-request', code: 'INVALID_ARGUMENT' },
  403: { kind: 'local-boundary', code: 'LOCAL_BOUNDARY_REJECTED' },
  404: { kind: 'not-found', code: 'NOT_FOUND' },
  429: { kind: 'capacity', code: 'MANAGEMENT_CAPACITY_EXCEEDED' },
  500: { kind: 'server', code: 'INTERNAL_FAILURE' },
  503: { kind: 'unavailable', code: 'MANAGEMENT_UNAVAILABLE' },
};
async function read<T>(path: string, signal: AbortSignal, parse: (body: unknown, response: Response) => T): Promise<T> {
  let response: Response;
  try {
    response = await fetch(path, {
      method: 'GET', signal, cache: 'no-store', credentials: 'omit', headers: { Accept: 'application/json' },
    });
  } catch (error) {
    if (signal.aborted) throw error;
    throw new ManagementError('network');
  }
  if (response.status !== 200) {
    const failure = failures[response.status];
    let code: string | null = null, correlationId: string | null = null, retryable: boolean | null = null;
    try {
      const body = object(await response.json());
      fields(body, ['apiVersion', 'code', 'message', 'correlationId', 'retryable']);
      if (failure && body.apiVersion === 1 && body.code === failure.code && typeof body.message === 'string'
        && typeof body.retryable === 'boolean' && typeof body.correlationId === 'string' && uuid.test(body.correlationId)) {
        code = failure.code; correlationId = body.correlationId; retryable = body.retryable;
      }
    } catch { /* Proxy errors can be non-JSON. No server text crosses into the UI. */ }
    throw new ManagementError(failure?.kind ?? 'server', code, correlationId, retryable);
  }
  try {
    need(response.headers.get('content-type')?.split(';', 1)[0]?.trim().toLowerCase() === 'application/json');
    return parse(await response.json(), response);
  } catch (error) {
    if (signal.aborted) throw error;
    throw new ManagementError('invalid-response');
  }
}

export async function fetchSources(query: SourcesQuery, signal: AbortSignal): Promise<SourcesPage> {
  const search = parameters(query);
  return read(`/api/management/v1/sources?${search}`, signal, (body) => parseSources(body, query));
}
function sourcePath(identity: SourceIdentity): string {
  if (!isSourceIdentity(identity)) throw new ManagementError('invalid-request');
  return `/api/management/v1/sources/${encodeURIComponent(identity.productId)}/${encodeURIComponent(identity.sourceId)}`;
}
export async function fetchSource(identity: SourceIdentity, signal: AbortSignal): Promise<SourceResponse> {
  return read(sourcePath(identity), signal, (body, response) => parseSource(body, identity, response.headers.get('ETag')));
}
export async function fetchRevisions(identity: SourceIdentity, query: RevisionQuery, signal: AbortSignal): Promise<RevisionsPage> {
  return read(`${sourcePath(identity)}/revisions?${parameters(query)}`, signal, (body) => parseRevisions(body, identity, query));
}
export async function fetchRuns(query: RunsQuery, signal: AbortSignal): Promise<RunsPage> {
  return read(`/api/management/v1/runs?${runsParameters(query)}`, signal, (body) => parseRuns(body, query));
}
function runPath(runId: string): string {
  if (!isManagementId(runId)) throw new ManagementError('invalid-request');
  return `/api/management/v1/runs/${runId}`;
}
export async function fetchRunDetail(runId: string, signal: AbortSignal): Promise<RunDetailResponse> {
  return read(runPath(runId), signal, (body, response) => parseRunDetail(body, runId, response.headers.get('ETag')));
}
export async function fetchAttempts(runId: string, query: RevisionQuery, signal: AbortSignal): Promise<ManagementPage<RunAttempt>> {
  return read(`${runPath(runId)}/attempts?${parameters(query)}`, signal, (body) => parseAttempts(body, query));
}
export async function fetchEvents(runId: string, query: RevisionQuery, signal: AbortSignal): Promise<ManagementPage<AuditEvent>> {
  return read(`${runPath(runId)}/events?${parameters(query)}`, signal, (body) => parseEvents(body, query));
}
export async function fetchControl(runId: string, controlId: string, signal: AbortSignal): Promise<ControlResponse> {
  if (!isManagementId(controlId)) throw new ManagementError('invalid-request');
  return read(`${runPath(runId)}/controls/${controlId}`, signal, (body, response) => parseControl(body, runId, controlId, response.headers.get('ETag')));
}
