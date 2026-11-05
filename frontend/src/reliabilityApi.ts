/** Independent synthetic-reliability REST boundary. Exact decimals stay strings. */
export type ReliabilityEnvironment = 'dev' | 'test' | 'production';
export interface ReliabilityQuery {
  start: string;
  end: string;
  serviceId?: string;
  environment?: ReliabilityEnvironment;
  limit?: number;
  offset?: number;
}
export interface ReliabilityRow {
  windowStart: string;
  serviceId: string;
  environment: ReliabilityEnvironment;
  requestCount: string;
  serverErrorCount: string;
  serverErrorRate: number;
  durationSumMs: string;
  meanDurationMs: number;
  p95DurationMsApprox: string;
  serviceName: string | null;
  ownerTeam: string | null;
  serviceTier: string | null;
  referenceStatus: 'MATCHED' | 'MISSING';
}
export const RELIABILITY_LABEL = 'Synthetic request events — not production reliability.';
export const RELIABILITY_MAX_OFFSET = 1_000_000;
export interface ReliabilityResponse {
  publication: string;
  productId: 'application-reliability';
  productContractVersion: 2;
  processorRevision: 2;
  inputFormat: 'JSONL' | 'PARQUET';
  origin: 'synthetic';
  label: typeof RELIABILITY_LABEL;
  reference: { referenceId: string; revision: string; snapshotFingerprint: string };
  filters: { serviceId: string | null; environment: ReliabilityEnvironment | null; start: string; end: string };
  data: ReliabilityRow[];
  meta: { returned: number; limit: number; offset: number; hasMore: boolean };
}

export type ReliabilityErrorKind = 'publication-changed' | 'invalid-argument' | 'network' | 'unavailable'
  | 'capacity' | 'timeout' | 'execution' | 'invalid-response';
const messages: Record<ReliabilityErrorKind, string> = {
  'publication-changed': 'The reliability publication has changed. Reload reliability analytics to start a new session. Results from different publications are never combined.',
  'invalid-argument': 'The reliability request was not accepted. Check the filters and try again.',
  network: 'Cannot reach the reliability API. Check the local API and development proxy, then try again.',
  unavailable: 'Reliability analytics are unavailable. Check that the API has a configured, completed reliability publication.',
  capacity: 'The reliability service is busy. Wait a moment, then try again.',
  timeout: 'The reliability request took too long. Try again when the service is ready.',
  execution: 'The reliability service could not complete this request. Please try again.',
  'invalid-response': 'The API returned data that does not match the reliability contract. No results are displayed.',
};
export class ReliabilityError extends Error {
  constructor(public readonly kind: ReliabilityErrorKind) {
    super(messages[kind]);
    this.name = 'ReliabilityError';
  }
}

export const SERVICE_ID = /^[a-z0-9][a-z0-9_-]{0,127}$/;
export const REFERENCE_ID = /^[a-z0-9][a-z0-9_-]{0,63}$/;
export const REFERENCE_REVISION = /^[a-z0-9][a-z0-9_-]{0,31}$/;
export const HEX = /^[0-9a-f]{64}$/;
const UTC = /^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{6}Z$/;
const END_OF_TIME = '10000-01-01T00:00:00.000000Z';
const END_OF_TIME_MS = Date.parse('+010000-01-01T00:00:00.000Z');
const MAX_INTERVAL_MS = 31 * 24 * 60 * 60 * 1000;
const QUERY_FIELDS = ['start', 'end', 'serviceId', 'environment', 'limit', 'offset'];
const BOUNDARY_WHITESPACE = new Set([9, 10, 11, 12, 13, 32, 133, 160, 5760,
  8192, 8193, 8194, 8195, 8196, 8197, 8198, 8199, 8200, 8201, 8202, 8232, 8233, 8239, 8287, 12288]);

// Comparing the entire match also rejects the trailing newline accepted by JS's $ anchor.
export function matches(value: unknown, pattern: RegExp): value is string {
  return typeof value === 'string' && pattern.exec(value)?.[0] === value;
}
export function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}
function isEnvironment(value: unknown): value is ReliabilityEnvironment {
  return value === 'dev' || value === 'test' || value === 'production';
}
export function timestampMs(value: unknown, allowEnd = false): number | null {
  if (allowEnd && value === END_OF_TIME) return END_OF_TIME_MS;
  if (!matches(value, UTC) || value.startsWith('0000')) return null;
  // Date checks the calendar only. The original six-digit timestamp is never reformatted.
  const milliseconds = value.slice(0, 23) + 'Z';
  const parsed = new Date(milliseconds);
  return Number.isFinite(parsed.getTime()) && parsed.toISOString() === milliseconds ? parsed.getTime() : null;
}
function isAligned(value: string): boolean {
  return value.endsWith(':00.000000Z') && Number(value.slice(-13, -11)) % 5 === 0;
}

/** Validate at runtime too: callers cannot bypass the wire contract through a cast. */
export function reliabilityQueryError(query: unknown): string | null {
  if (!isRecord(query) || Reflect.ownKeys(query).some((key) => typeof key !== 'string' || !QUERY_FIELDS.includes(key))
    || !Object.hasOwn(query, 'start') || !Object.hasOwn(query, 'end')) {
    return 'Provide start and end with only supported reliability filters.';
  }
  const start = timestampMs(query.start), end = timestampMs(query.end, true);
  if (start === null || end === null) return 'Use valid UTC timestamps in YYYY-MM-DDTHH:mm:ss.ffffffZ format.';
  if (!isAligned(query.start as string) || !isAligned(query.end as string)) return 'Start and end must fall on five-minute UTC boundaries.';
  if (end <= start || end - start > MAX_INTERVAL_MS) return 'Choose an end after start and a range of at most 31 days.';
  if (Object.hasOwn(query, 'serviceId') && !matches(query.serviceId, SERVICE_ID)) {
    return 'Service ID must be 1–128 lowercase letters, digits, underscores or hyphens, starting with a letter or digit.';
  }
  if (Object.hasOwn(query, 'environment') && !isEnvironment(query.environment)) return 'Choose dev, test or production as the environment.';
  if (Object.hasOwn(query, 'limit') && (typeof query.limit !== 'number' || !Number.isInteger(query.limit) || query.limit < 1 || query.limit > 1000)) {
    return 'Limit must be a whole number from 1 to 1000.';
  }
  if (Object.hasOwn(query, 'offset') && (typeof query.offset !== 'number' || !Number.isInteger(query.offset) || query.offset < 0 || query.offset > RELIABILITY_MAX_OFFSET)) {
    return 'Offset must be a whole number from 0 to 1000000.';
  }
  return null;
}

type CheckedQuery = ReliabilityQuery & { limit: number; offset: number };
function checkedQuery(query: ReliabilityQuery): CheckedQuery {
  if (reliabilityQueryError(query)) throw new ReliabilityError('invalid-argument');
  return { ...query, limit: query.limit ?? 100, offset: query.offset ?? 0 };
}
export function reliabilityQueryParameters(query: ReliabilityQuery): URLSearchParams {
  const checked = checkedQuery(query);
  const result = new URLSearchParams({ start: checked.start, end: checked.end, limit: String(checked.limit), offset: String(checked.offset) });
  if (checked.serviceId !== undefined) result.set('serviceId', checked.serviceId);
  if (checked.environment !== undefined) result.set('environment', checked.environment);
  return result;
}

export function requireValid(condition: unknown): asserts condition {
  if (!condition) throw new ReliabilityError('invalid-response');
}
export function record(value: unknown): asserts value is Record<string, unknown> {
  requireValid(isRecord(value));
}
export function fields(value: Record<string, unknown>, expected: string[]) {
  requireValid(Reflect.ownKeys(value).length === expected.length && expected.every((key) => Object.hasOwn(value, key)));
}
export function decimal(value: unknown, maximum: bigint): string {
  requireValid(matches(value, /^(0|[1-9][0-9]{0,37})$/) && BigInt(value) <= maximum);
  return value;
}
function boundedNumber(value: unknown, maximum: number): number {
  requireValid(typeof value === 'number' && Number.isFinite(value) && value >= 0 && value <= maximum);
  return value;
}
function agreesWithRatio(actual: number, expected: number): boolean {
  return Math.abs(actual - expected) <= Math.max(1e-12, Math.abs(expected) * 1e-12);
}
export function nullableText(value: unknown): string | null {
  requireValid(value === null || typeof value === 'string');
  return value;
}
export function serviceName(value: string | null) {
  requireValid(value !== null);
  const scalars = Array.from(value);
  requireValid(scalars.length >= 1 && scalars.length <= 200);
  // Same scalar-text-v1 rules as the selected service reference. Do not trim or normalize.
  requireValid(!BOUNDARY_WHITESPACE.has(scalars[0]!.codePointAt(0)!)
    && !BOUNDARY_WHITESPACE.has(scalars[scalars.length - 1]!.codePointAt(0)!));
  requireValid(scalars.every((character) => {
    const code = character.codePointAt(0)!;
    return code >= 32 && !(code >= 127 && code <= 159) && code !== 8232 && code !== 8233
      && code !== 65279 && !(code >= 0xd800 && code <= 0xdfff);
  }));
}

export function parseRequestWindows(value: unknown, query: ReliabilityQuery): ReliabilityResponse {
  const checked = checkedQuery(query);
  record(value);
  fields(value, ['publication', 'productId', 'productContractVersion', 'processorRevision', 'inputFormat', 'origin', 'label', 'reference', 'filters', 'data', 'meta']);
  requireValid(matches(value.publication, HEX) && value.productId === 'application-reliability'
    && value.productContractVersion === 2 && value.processorRevision === 2
    && (value.inputFormat === 'JSONL' || value.inputFormat === 'PARQUET')
    && value.origin === 'synthetic' && value.label === RELIABILITY_LABEL);
  record(value.reference);
  fields(value.reference, ['referenceId', 'revision', 'snapshotFingerprint']);
  requireValid(matches(value.reference.referenceId, REFERENCE_ID) && matches(value.reference.revision, REFERENCE_REVISION)
    && matches(value.reference.snapshotFingerprint, HEX));
  record(value.filters);
  fields(value.filters, ['serviceId', 'environment', 'start', 'end']);
  requireValid(value.filters.serviceId === (checked.serviceId ?? null) && value.filters.environment === (checked.environment ?? null)
    && value.filters.start === checked.start && value.filters.end === checked.end);
  record(value.meta);
  fields(value.meta, ['returned', 'limit', 'offset', 'hasMore']);
  requireValid(Array.isArray(value.data) && value.data.length <= checked.limit
    && Number.isInteger(value.meta.returned) && value.meta.returned === value.data.length
    && value.meta.limit === checked.limit && value.meta.offset === checked.offset
    && typeof value.meta.hasMore === 'boolean' && (!value.meta.hasMore || value.data.length === checked.limit));
  const start = timestampMs(checked.start)!, end = timestampMs(checked.end, true)!;
  let previous: ReliabilityRow | null = null;
  const data = value.data.map((item: unknown): ReliabilityRow => {
    record(item);
    fields(item, ['windowStart', 'serviceId', 'environment', 'requestCount', 'serverErrorCount', 'serverErrorRate', 'durationSumMs',
      'meanDurationMs', 'p95DurationMsApprox', 'serviceName', 'ownerTeam', 'serviceTier', 'referenceStatus']);
    const time = timestampMs(item.windowStart);
    requireValid(typeof item.windowStart === 'string' && time !== null && isAligned(item.windowStart) && time >= start && time < end);
    requireValid(matches(item.serviceId, SERVICE_ID) && (!checked.serviceId || item.serviceId === checked.serviceId));
    requireValid(isEnvironment(item.environment) && (!checked.environment || item.environment === checked.environment));
    requireValid(item.referenceStatus === 'MATCHED' || item.referenceStatus === 'MISSING');
    const row: ReliabilityRow = {
      windowStart: item.windowStart, serviceId: item.serviceId, environment: item.environment,
      requestCount: decimal(item.requestCount, 2_000_000n), serverErrorCount: decimal(item.serverErrorCount, 2_000_000n),
      serverErrorRate: boundedNumber(item.serverErrorRate, 1), durationSumMs: decimal(item.durationSumMs, 10n ** 38n - 1n),
      meanDurationMs: boundedNumber(item.meanDurationMs, 3_600_000), p95DurationMsApprox: decimal(item.p95DurationMsApprox, 3_600_000n),
      serviceName: nullableText(item.serviceName), ownerTeam: nullableText(item.ownerTeam), serviceTier: nullableText(item.serviceTier),
      referenceStatus: item.referenceStatus,
    };
    requireValid(BigInt(row.requestCount) >= 1n && BigInt(row.serverErrorCount) <= BigInt(row.requestCount));
    requireValid(BigInt(row.durationSumMs) <= BigInt(row.requestCount) * 3_600_000n);
    // These operands are exact safe integers after the native count/duration caps above.
    // Keep the original decimal strings; number conversion is only for ratio validation.
    requireValid(agreesWithRatio(row.serverErrorRate, Number(row.serverErrorCount) / Number(row.requestCount))
      && agreesWithRatio(row.meanDurationMs, Number(row.durationSumMs) / Number(row.requestCount)));
    if (row.referenceStatus === 'MISSING') {
      requireValid(row.serviceName === null && row.ownerTeam === null && row.serviceTier === null);
    } else {
      serviceName(row.serviceName);
      requireValid(row.ownerTeam === null || row.ownerTeam === 'example-platform' || row.ownerTeam === 'example-service');
      requireValid(row.serviceTier === null || ['critical', 'standard', 'experimental'].includes(row.serviceTier));
    }
    requireValid(previous === null || previous.windowStart < row.windowStart
      || (previous.windowStart === row.windowStart && (previous.serviceId < row.serviceId
        || (previous.serviceId === row.serviceId && previous.environment < row.environment))));
    previous = row;
    return row;
  });
  return {
    publication: value.publication, productId: 'application-reliability', productContractVersion: 2, processorRevision: 2,
    inputFormat: value.inputFormat, origin: 'synthetic', label: RELIABILITY_LABEL,
    reference: { referenceId: value.reference.referenceId, revision: value.reference.revision, snapshotFingerprint: value.reference.snapshotFingerprint },
    filters: { serviceId: checked.serviceId ?? null, environment: checked.environment ?? null, start: checked.start, end: checked.end },
    data, meta: { returned: data.length, limit: checked.limit, offset: checked.offset, hasMore: value.meta.hasMore },
  };
}

export async function fetchRequestWindows(query: ReliabilityQuery, signal: AbortSignal): Promise<ReliabilityResponse> {
  const checked = checkedQuery(query);
  let response: Response;
  try {
    response = await fetch(`/api/v1/reliability/request-windows?${reliabilityQueryParameters(checked)}`, {
      signal, cache: 'no-store', credentials: 'omit', headers: { Accept: 'application/json' },
    });
  } catch (error) {
    if (signal.aborted) throw error;
    throw new ReliabilityError('network');
  }
  if (response.status !== 200) {
    // Remote messages, paths, diagnostic fields and raw bodies never reach the UI.
    if (response.status === 503) {
      let body: unknown;
      try { body = await response.json(); } catch { /* A proxy may return non-JSON. */ }
      throw new ReliabilityError(isRecord(body) && body.code === 'ANALYTICS_CAPACITY_EXCEEDED' ? 'capacity' : 'unavailable');
    }
    throw new ReliabilityError(response.status === 400 ? 'invalid-argument' : response.status === 504 ? 'timeout' : 'execution');
  }
  try {
    requireValid(response.headers.get('content-type')?.split(';', 1)[0]?.trim().toLowerCase() === 'application/json');
    return parseRequestWindows(await response.json(), checked);
  } catch (error) {
    if (signal.aborted) throw error;
    throw new ReliabilityError('invalid-response');
  }
}
