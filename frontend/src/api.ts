/** The M6.2/M6.4 REST contracts. Counters stay decimal strings at this boundary. */
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

export interface Article {
  articleId: string; sourceId: string; title: string; url: string | null; summary: string | null;
  publishedAt: string | null; updatedAt: string | null; firstObservedAt: string; lastObservedAt: string;
  observationCount: string; distinctIngestionCount: string; qualityWarnings: string[];
}
export interface DailyRow {
  sourceId: string; ingestionDate: string; observationCount: string; distinctArticleCount: string;
  distinctIngestionCount: string; qualityWarningCount: string; latestIngestedAt: string;
}
export interface QualityRow {
  sourceId: string; observationCount: string; qualityWarningCount: string; warningCodesPerObservation: number;
}
export interface ArticlesResponse {
  publication: string; data: Article[];
  meta: { returned: number; limit: number; offset: number; hasMore: boolean };
}
export interface DailyResponse { publication: string; data: DailyRow[]; meta: { returned: number; limit: number } }
export interface QualityResponse { publication: string; data: QualityRow[]; meta: { returned: number; limit: number } }
export type View = 'publishers' | 'daily' | 'quality' | 'articles';
export interface DateFilters { from: string; to: string }
export const EMPTY_DATES: DateFilters = { from: '', to: '' };
export const ARTICLE_LIMIT = 10;
export const MAX_OFFSET = 1_000_000;

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
  constructor(public readonly kind: ErrorKind, report: View = 'publishers') {
    super(report === 'publishers' ? messages[kind] : messages[kind].replace(/publisher/gi,
      report === 'articles' ? 'article' : report));
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

function envelope(value: unknown, limit: number, extraMeta: string[] = []) {
  requireValid(Number.isInteger(limit) && limit >= 1 && limit <= 1000);
  record(value);
  fields(value, ['publication', 'data', 'meta']);
  requireValid(typeof value.publication === 'string' && /^[0-9a-f]{64}$/.test(value.publication));
  record(value.meta);
  fields(value.meta, ['returned', 'limit', ...extraMeta]);
  requireValid(value.meta.limit === limit && Number.isInteger(value.meta.returned));
  requireValid(Array.isArray(value.data) && value.data.length <= limit && value.meta.returned === value.data.length);
  return { publication: value.publication, rows: value.data as unknown[], meta: value.meta };
}

export function parsePublishers(value: unknown, limit: number, sourceId = ''): PublishersResponse {
  const { publication, rows } = envelope(value, limit);
  const seen = new Set<string>();
  let previous: string | null = null;
  const data = rows.map((item: unknown): Publisher => {
    record(item);
    fields(item, ['sourceId', 'articleCount', 'observationCount', 'firstObservedAt', 'lastObservedAt', 'articlesWithoutPublishedAt']);
    requireValid(typeof item.sourceId === 'string' && item.sourceId.trim().length > 0);
    requireValid(!seen.has(item.sourceId) && (!sourceId || item.sourceId === sourceId));
    requireValid(previous === null || compareSourceIds(previous, item.sourceId) < 0);
    previous = item.sourceId;
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
  return { publication, data, meta: { returned: data.length, limit } };
}

/** DuckDB's source ordering is binary UTF-8, not locale-dependent sorting. */
export function compareSourceIds(left: string, right: string): number {
  const encoder = new TextEncoder();
  const a = encoder.encode(left), b = encoder.encode(right);
  for (let i = 0; i < Math.min(a.length, b.length); i++) {
    if (a[i] !== b[i]) return a[i]! - b[i]!;
  }
  return a.length - b.length;
}

export function isUtcDate(value: string): boolean {
  if (!/^[0-9]{4}-[0-9]{2}-[0-9]{2}$/.test(value) || value.startsWith('0000')) return false;
  const parsed = new Date(`${value}T00:00:00.000Z`);
  return Number.isFinite(parsed.getTime()) && parsed.toISOString().slice(0, 10) === value;
}
export function dateFilterError({ from, to }: DateFilters): string | null {
  if ((from !== '' && !isUtcDate(from)) || (to !== '' && !isUtcDate(to))) return 'Use valid UTC dates in YYYY-MM-DD format.';
  if (from && to && from > to) return 'From date must be on or before To date.';
  return null;
}
function source(value: unknown, filter: string): string {
  requireValid(typeof value === 'string' && value.trim().length > 0 && (!filter || value === filter));
  return value;
}
function positiveCounter(value: unknown): string {
  const result = counter(value);
  requireValid(BigInt(result) > 0n);
  return result;
}
function requiredTimestamp(value: unknown): string {
  const result = timestamp(value);
  requireValid(result !== null);
  return result;
}
function nullableText(value: unknown): string | null {
  requireValid(value === null || typeof value === 'string');
  return value;
}

export function parseArticles(value: unknown, limit: number, offset: number, sourceId = ''): ArticlesResponse {
  const { publication, rows, meta } = envelope(value, limit, ['offset', 'hasMore']);
  requireValid(Number.isInteger(meta.offset) && meta.offset === offset && offset >= 0 && offset <= MAX_OFFSET);
  requireValid(typeof meta.hasMore === 'boolean' && (!meta.hasMore || rows.length === limit));
  const ids = new Set<string>();
  let previous: Article | null = null;
  const data = rows.map((item): Article => {
    record(item);
    fields(item, ['articleId', 'sourceId', 'title', 'url', 'summary', 'publishedAt', 'updatedAt',
      'firstObservedAt', 'lastObservedAt', 'observationCount', 'distinctIngestionCount', 'qualityWarnings']);
    requireValid(typeof item.articleId === 'string' && /^[0-9a-f]{64}$/.test(item.articleId) && !ids.has(item.articleId));
    requireValid(typeof item.title === 'string' && item.title.trim().length > 0);
    const row: Article = {
      articleId: item.articleId, sourceId: source(item.sourceId, sourceId), title: item.title,
      url: nullableText(item.url), summary: nullableText(item.summary),
      publishedAt: timestamp(item.publishedAt), updatedAt: timestamp(item.updatedAt),
      firstObservedAt: requiredTimestamp(item.firstObservedAt), lastObservedAt: requiredTimestamp(item.lastObservedAt),
      observationCount: positiveCounter(item.observationCount), distinctIngestionCount: positiveCounter(item.distinctIngestionCount),
      qualityWarnings: [],
    };
    const warnings = item.qualityWarnings;
    requireValid(Array.isArray(warnings));
    requireValid([[], ['INVALID_PUBLISHED_AT'], ['INVALID_UPDATED_AT'],
      ['INVALID_PUBLISHED_AT', 'INVALID_UPDATED_AT']].some((codes) =>
      codes.length === warnings.length && codes.every((code, index) => code === warnings[index])));
    row.qualityWarnings = [...warnings] as string[];
    requireValid(row.firstObservedAt <= row.lastObservedAt && BigInt(row.distinctIngestionCount) <= BigInt(row.observationCount));
    requireValid(previous === null || previous.lastObservedAt > row.lastObservedAt
      || (previous.lastObservedAt === row.lastObservedAt && previous.articleId < row.articleId));
    ids.add(row.articleId);
    previous = row;
    return row;
  });
  return { publication, data, meta: { returned: data.length, limit, offset, hasMore: meta.hasMore } };
}

export function parseDaily(value: unknown, limit: number, sourceId = '', dates = EMPTY_DATES): DailyResponse {
  const { publication, rows } = envelope(value, limit);
  let previous: DailyRow | null = null;
  const data = rows.map((item): DailyRow => {
    record(item);
    fields(item, ['sourceId', 'ingestionDate', 'observationCount', 'distinctArticleCount', 'distinctIngestionCount',
      'qualityWarningCount', 'latestIngestedAt']);
    requireValid(typeof item.ingestionDate === 'string' && isUtcDate(item.ingestionDate));
    requireValid((!dates.from || item.ingestionDate >= dates.from) && (!dates.to || item.ingestionDate <= dates.to));
    const row: DailyRow = {
      sourceId: source(item.sourceId, sourceId), ingestionDate: item.ingestionDate,
      observationCount: positiveCounter(item.observationCount), distinctArticleCount: positiveCounter(item.distinctArticleCount),
      distinctIngestionCount: positiveCounter(item.distinctIngestionCount), qualityWarningCount: counter(item.qualityWarningCount),
      latestIngestedAt: requiredTimestamp(item.latestIngestedAt),
    };
    requireValid(row.latestIngestedAt.startsWith(`${row.ingestionDate}T`));
    requireValid(BigInt(row.distinctArticleCount) <= BigInt(row.observationCount)
      && BigInt(row.distinctIngestionCount) <= BigInt(row.observationCount));
    requireValid(previous === null || compareSourceIds(previous.sourceId, row.sourceId) < 0
      || (previous.sourceId === row.sourceId && previous.ingestionDate < row.ingestionDate));
    previous = row;
    return row;
  });
  return { publication, data, meta: { returned: data.length, limit } };
}

export function parseQuality(value: unknown, limit: number, sourceId = ''): QualityResponse {
  const { publication, rows } = envelope(value, limit);
  let previous: string | null = null;
  const data = rows.map((item): QualityRow => {
    record(item);
    fields(item, ['sourceId', 'observationCount', 'qualityWarningCount', 'warningCodesPerObservation']);
    const id = source(item.sourceId, sourceId);
    requireValid(previous === null || compareSourceIds(previous, id) < 0);
    requireValid(typeof item.warningCodesPerObservation === 'number' && Number.isFinite(item.warningCodesPerObservation)
      && item.warningCodesPerObservation >= 0);
    previous = id;
    return { sourceId: id, observationCount: positiveCounter(item.observationCount),
      qualityWarningCount: counter(item.qualityWarningCount), warningCodesPerObservation: item.warningCodesPerObservation };
  });
  return { publication, data, meta: { returned: data.length, limit } };
}

function queryParameters(sourceId: string, limit: number, report: View = 'publishers'): URLSearchParams {
  if (!Number.isInteger(limit) || limit < 1 || limit > 1000 || (sourceId !== '' && !isFilterableSource(sourceId))) {
    throw new DashboardError('invalid-argument', report);
  }
  const query = new URLSearchParams({ limit: String(limit) });
  if (sourceId) query.set('sourceId', sourceId);
  return query;
}
function withDates(query: URLSearchParams, dates: DateFilters, report: 'daily' | 'quality'): URLSearchParams {
  if (dateFilterError(dates)) throw new DashboardError('invalid-argument', report);
  if (dates.from) query.set('from', dates.from);
  if (dates.to) query.set('to', dates.to);
  return query;
}

export async function fetchPublishers(sourceId: string, signal: AbortSignal, limit = PUBLISHER_LIMIT): Promise<PublishersResponse> {
  return request('publishers', queryParameters(sourceId, limit), signal, (value) => parsePublishers(value, limit, sourceId));
}
export async function fetchArticles(sourceId: string, signal: AbortSignal, offset = 0, limit = ARTICLE_LIMIT): Promise<ArticlesResponse> {
  const query = queryParameters(sourceId, limit, 'articles');
  if (!Number.isInteger(offset) || offset < 0 || offset > MAX_OFFSET) throw new DashboardError('invalid-argument', 'articles');
  query.set('offset', String(offset));
  return request('articles', query, signal, (value) => parseArticles(value, limit, offset, sourceId));
}
export async function fetchDaily(sourceId: string, signal: AbortSignal, dates = EMPTY_DATES, limit = PUBLISHER_LIMIT): Promise<DailyResponse> {
  return request('daily', withDates(queryParameters(sourceId, limit, 'daily'), dates, 'daily'), signal, (value) => parseDaily(value, limit, sourceId, dates));
}
export async function fetchQuality(sourceId: string, signal: AbortSignal, dates = EMPTY_DATES, limit = PUBLISHER_LIMIT): Promise<QualityResponse> {
  return request('quality', withDates(queryParameters(sourceId, limit, 'quality'), dates, 'quality'), signal, (value) => parseQuality(value, limit, sourceId));
}

async function request<T>(report: View, query: URLSearchParams, signal: AbortSignal, parse: (value: unknown) => T): Promise<T> {
  let response: Response;
  try {
    response = await fetch(`/api/v1/${report}?${query}`, {
      signal, cache: 'no-store', credentials: 'omit', headers: { Accept: 'application/json' },
    });
  } catch (error) {
    if (signal.aborted) throw error;
    throw new DashboardError('network', report);
  }
  if (response.status !== 200) {
    // Only this fixed code affects presentation. Never render server messages, paths or raw bodies.
    if (response.status === 503) {
      let body: unknown;
      try { body = await response.json(); } catch { /* Proxy errors can be non-JSON. */ }
      if (body && typeof body === 'object' && 'code' in body && body.code === 'ANALYTICS_CAPACITY_EXCEEDED') {
        throw new DashboardError('capacity', report);
      }
      throw new DashboardError('unavailable', report);
    }
    throw new DashboardError(response.status === 400 ? 'invalid-argument' : response.status === 504 ? 'timeout' : 'execution', report);
  }
  try {
    requireValid(response.headers.get('content-type')?.includes('application/json'));
    return parse(await response.json());
  } catch {
    throw new DashboardError('invalid-response', report);
  }
}
