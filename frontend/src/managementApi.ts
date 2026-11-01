/** Management v1 wire contracts. Analytics publication envelopes remain independent. */
export type ProductId = 'content-intelligence' | 'application-reliability';
export interface ManagementEnvelope<T> { apiVersion: 1; data: T; meta: { correlationId: string } }
export interface ManagementPage<T> extends ManagementEnvelope<T[]> {
  meta: { correlationId: string; limit: number; hasMore: boolean; nextCursor: string | null };
}
export interface SourceSummary {
  productId: ProductId; sourceId: string; displayName: string; enabled: boolean;
  currentRevision: number; rowVersion: string; configSha256: string; createdAt: string; updatedAt: string;
}
export interface SourcesQuery { productId?: ProductId; enabled?: boolean; limit?: number; cursor?: string }
export type SourcesPage = ManagementPage<SourceSummary>;
export const SOURCE_PAGE_LIMIT = 20;

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
    public readonly correlationId: string | null = null) {
    super(messages[kind]); this.name = 'ManagementError';
  }
}

const uuid = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const cursorPattern = /^[A-Za-z0-9_-]+\.[A-Za-z0-9_-]{43}$/;
const isProduct = (value: unknown): value is ProductId => value === 'content-intelligence' || value === 'application-reliability';
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

export function parseSources(value: unknown, query: SourcesQuery = {}): SourcesPage {
  parameters(query);
  const root = object(value); fields(root, ['apiVersion', 'data', 'meta']); need(root.apiVersion === 1);
  const meta = object(root.meta); fields(meta, ['correlationId', 'limit', 'hasMore', 'nextCursor']);
  need(typeof meta.correlationId === 'string' && uuid.test(meta.correlationId));
  const limit = query.limit ?? SOURCE_PAGE_LIMIT;
  need(meta.limit === limit && typeof meta.hasMore === 'boolean');
  need(meta.nextCursor === null || cursor(meta.nextCursor));
  need(meta.hasMore === (meta.nextCursor !== null) && (meta.nextCursor === null || meta.nextCursor !== query.cursor));
  need(Array.isArray(root.data) && root.data.length <= limit && (!meta.hasMore || root.data.length === limit));
  let previous = '';
  const data = root.data.map((item): SourceSummary => {
    const row = object(item);
    fields(row, ['productId', 'sourceId', 'displayName', 'enabled', 'currentRevision', 'rowVersion', 'configSha256', 'createdAt', 'updatedAt']);
    need(isProduct(row.productId) && (query.productId === undefined || row.productId === query.productId));
    need(typeof row.sourceId === 'string' && /^[a-z0-9][a-z0-9_-]{0,127}$/.test(row.sourceId));
    need(typeof row.displayName === 'string' && label(row.displayName));
    need(typeof row.enabled === 'boolean' && (query.enabled === undefined || row.enabled === query.enabled));
    need(typeof row.currentRevision === 'number' && Number.isInteger(row.currentRevision) && row.currentRevision >= 1 && row.currentRevision <= 1000);
    need(typeof row.configSha256 === 'string' && /^[0-9a-f]{64}$/.test(row.configSha256));
    const key = `${row.productId}:${row.sourceId}`;
    need(previous < key); previous = key; // Both parts are ASCII, matching the API's binary ordering.
    return { productId: row.productId, sourceId: row.sourceId, displayName: row.displayName, enabled: row.enabled,
      currentRevision: row.currentRevision, rowVersion: version(row.rowVersion), configSha256: row.configSha256,
      createdAt: timestamp(row.createdAt), updatedAt: timestamp(row.updatedAt) };
  });
  return { apiVersion: 1, data, meta: { correlationId: meta.correlationId, limit, hasMore: meta.hasMore, nextCursor: meta.nextCursor } };
}

const failures: Record<number, { kind: ManagementErrorKind; code: string }> = {
  400: { kind: 'invalid-request', code: 'INVALID_ARGUMENT' },
  403: { kind: 'local-boundary', code: 'LOCAL_BOUNDARY_REJECTED' },
  404: { kind: 'not-found', code: 'NOT_FOUND' },
  429: { kind: 'capacity', code: 'MANAGEMENT_CAPACITY_EXCEEDED' },
  500: { kind: 'server', code: 'INTERNAL_FAILURE' },
  503: { kind: 'unavailable', code: 'MANAGEMENT_UNAVAILABLE' },
};
export async function fetchSources(query: SourcesQuery, signal: AbortSignal): Promise<SourcesPage> {
  const search = parameters(query);
  let response: Response;
  try {
    response = await fetch(`/api/management/v1/sources?${search}`, {
      method: 'GET', signal, cache: 'no-store', credentials: 'omit', headers: { Accept: 'application/json' },
    });
  } catch (error) {
    if (signal.aborted) throw error;
    throw new ManagementError('network');
  }
  if (response.status !== 200) {
    const failure = failures[response.status];
    let code: string | null = null, correlationId: string | null = null;
    try {
      const body = object(await response.json());
      fields(body, ['apiVersion', 'code', 'message', 'correlationId', 'retryable']);
      if (failure && body.apiVersion === 1 && body.code === failure.code && typeof body.message === 'string'
        && typeof body.retryable === 'boolean' && typeof body.correlationId === 'string' && uuid.test(body.correlationId)) {
        code = failure.code; correlationId = body.correlationId;
      }
    } catch { /* Proxy errors can be non-JSON. No server text crosses into the UI. */ }
    throw new ManagementError(failure?.kind ?? 'server', code, correlationId);
  }
  try {
    need(response.headers.get('content-type')?.split(';', 1)[0]?.trim().toLowerCase() === 'application/json');
    return parseSources(await response.json(), query);
  } catch (error) {
    if (signal.aborted) throw error;
    throw new ManagementError('invalid-response');
  }
}
