/** The M6.2 publisher REST contract. Counters stay decimal strings at this boundary. */
export interface Publisher {
  sourceId: string;
  articleCount: string;
  observationCount: string;
  firstObservedAt: string | null;
  lastObservedAt: string | null;
  articlesWithoutPublishedAt: string;
}

export interface PublishersResponse {
  publication: string;
  data: Publisher[];
  meta: { returned: number; limit: number };
}

export const PUBLISHER_LIMIT = 100;
export const MAX_COUNTER = 9223372036854775807n;
export const isFilterableSource = (source: string) => /^[a-z0-9][a-z0-9_-]{0,127}$/.test(source);

export type ErrorKind = 'invalid-argument' | 'unavailable' | 'capacity' | 'timeout' | 'execution'
  | 'network' | 'invalid-response' | 'publication-changed';

const messages: Record<ErrorKind, string> = {
  'invalid-argument': 'The publisher request was not accepted. Reset the filter and try again.',
  unavailable: 'Publisher analytics are unavailable. Check that the content API has a configured, completed Gold publication.',
  capacity: 'The analytics service is busy. Wait a moment, then try again.',
  timeout: 'The analytics request took too long. You can try again when the service is ready.',
  execution: 'The analytics service could not complete this request. Please try again.',
  network: 'Cannot reach the content API. Check the local API and development proxy, then try again.',
  'invalid-response': 'The API returned data that does not match the publisher contract. No results are displayed.',
  'publication-changed': 'The API is serving a different Gold publication. Reload the dashboard to start a new session. Results from different publications are never combined.',
};

export class DashboardError extends Error {
  constructor(public readonly kind: ErrorKind) {
    super(messages[kind]);
    this.name = 'DashboardError';
  }
}

function requireValid(condition: unknown): asserts condition {
  if (!condition) throw new DashboardError('invalid-response');
}

function record(value: unknown): asserts value is Record<string, unknown> {
  requireValid(value !== null && typeof value === 'object' && !Array.isArray(value));
}

function fields(value: Record<string, unknown>, expected: string[]) {
  requireValid(Object.keys(value).length === expected.length && expected.every((key) => Object.hasOwn(value, key)));
}

function counter(value: unknown): string {
  requireValid(typeof value === 'string' && /^(0|[1-9][0-9]{0,18})$/.test(value));
  requireValid(BigInt(value) <= MAX_COUNTER);
  return value;
}

function timestamp(value: unknown): string | null {
  if (value === null) return null;
  requireValid(typeof value === 'string' && /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z$/.test(value));
  requireValid(value.slice(0, 4) !== '0000');
  // Date is used only to validate the calendar. Never return its millisecond-truncated value.
  const milliseconds = value.slice(0, 23) + 'Z';
  const parsed = new Date(milliseconds);
  requireValid(Number.isFinite(parsed.getTime()) && parsed.toISOString() === milliseconds);
  return value;
}

export function parsePublishers(value: unknown, limit: number, sourceId = ''): PublishersResponse {
  record(value);
  fields(value, ['publication', 'data', 'meta']);
  requireValid(typeof value.publication === 'string' && /^[0-9a-f]{64}$/.test(value.publication));
  record(value.meta);
  fields(value.meta, ['returned', 'limit']);
  requireValid(value.meta.limit === limit && Number.isInteger(value.meta.returned));
  requireValid(Array.isArray(value.data) && value.data.length <= limit && value.meta.returned === value.data.length);
  const seen = new Set<string>();
  const data = value.data.map((item: unknown): Publisher => {
    record(item);
    fields(item, ['sourceId', 'articleCount', 'observationCount', 'firstObservedAt', 'lastObservedAt', 'articlesWithoutPublishedAt']);
    requireValid(typeof item.sourceId === 'string' && item.sourceId.trim().length > 0);
    requireValid(!seen.has(item.sourceId) && (!sourceId || item.sourceId === sourceId));
    seen.add(item.sourceId);
    return {
      sourceId: item.sourceId,
      articleCount: counter(item.articleCount),
      observationCount: counter(item.observationCount),
      articlesWithoutPublishedAt: counter(item.articlesWithoutPublishedAt),
      firstObservedAt: timestamp(item.firstObservedAt),
      lastObservedAt: timestamp(item.lastObservedAt),
    };
  });
  return { publication: value.publication, data, meta: { returned: data.length, limit } };
}

export async function fetchPublishers(sourceId: string, signal: AbortSignal, limit = PUBLISHER_LIMIT): Promise<PublishersResponse> {
  if (!Number.isInteger(limit) || limit < 1 || limit > 1000 || (sourceId !== '' && !isFilterableSource(sourceId))) {
    throw new DashboardError('invalid-argument');
  }
  const query = new URLSearchParams({ limit: String(limit) });
  if (sourceId) query.set('sourceId', sourceId);
  let response: Response;
  try {
    response = await fetch(`/api/v1/publishers?${query}`, {
      signal, cache: 'no-store', credentials: 'omit', headers: { Accept: 'application/json' },
    });
  } catch (error) {
    if (signal.aborted) throw error;
    throw new DashboardError('network');
  }
  if (response.status !== 200) {
    // Only this fixed code affects presentation. Never render server messages, paths or raw bodies.
    if (response.status === 503) {
      let body: unknown;
      try { body = await response.json(); } catch { /* Proxy errors can be non-JSON. */ }
      if (body && typeof body === 'object' && 'code' in body && body.code === 'ANALYTICS_CAPACITY_EXCEEDED') {
        throw new DashboardError('capacity');
      }
      throw new DashboardError('unavailable');
    }
    throw new DashboardError(response.status === 400 ? 'invalid-argument' : response.status === 504 ? 'timeout' : 'execution');
  }
  try {
    requireValid(response.headers.get('content-type')?.includes('application/json'));
    return parsePublishers(await response.json(), limit, sourceId);
  } catch {
    throw new DashboardError('invalid-response');
  }
}
