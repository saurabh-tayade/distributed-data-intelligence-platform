import { isUtcDate, parseArticles } from './api';
import type { Article, ErrorKind } from './api';

export const CONTENT_MAX_OFFSET = 1_000_000;
export const CONTENT_VOCABULARY = {
  referenceStatus: ['MATCHED', 'MISSING'],
  publisherCategory: ['business', 'culture', 'general-news', 'science', 'technology'],
  declaredLanguage: ['en', 'es', 'fr'], countryCode: ['CA', 'GB', 'US'],
} as const;
export interface ContentQuery {
  sourceId?: string; from?: string; to?: string;
  referenceStatus?: 'MATCHED' | 'MISSING';
  publisherCategory?: typeof CONTENT_VOCABULARY.publisherCategory[number];
  declaredLanguage?: typeof CONTENT_VOCABULARY.declaredLanguage[number];
  countryCode?: typeof CONTENT_VOCABULARY.countryCode[number];
  limit?: number; offset?: number;
}
export interface ContentRow extends Article {
  identifierKind: 'rss_guid' | 'rss_link' | 'atom_id' | 'atom_link'; sourceArticleId: string; feedFormat: 'rss' | 'atom';
  discoveredAt: string; discoveryDate: string; publisherName: string | null;
  publisherCategory: Exclude<ContentQuery['publisherCategory'], undefined> | null;
  declaredLanguage: Exclude<ContentQuery['declaredLanguage'], undefined> | null; countryCode: Exclude<ContentQuery['countryCode'], undefined> | null;
  referenceStatus: 'MATCHED' | 'MISSING'; referenceId: string; referenceRevision: string; referenceFingerprint: string;
}
export interface ContentIdentity {
  publication: string; goldPublication: string;
  reference: { referenceId: string; revision: string; snapshotFingerprint: string };
}
export interface ContentResponse extends ContentIdentity {
  productId: 'content-intelligence';
  filters: { [K in Exclude<keyof ContentQuery, 'limit' | 'offset'>]-?: Exclude<ContentQuery[K], undefined> | null };
  data: ContentRow[]; meta: { returned: number; limit: number; offset: number; hasMore: boolean };
}
const messages: Record<ErrorKind, string> = {
  'publication-changed': 'The Content publication, base Gold selection or publisher reference has changed. Reload the dashboard to start a new session. Results are never combined across selections.',
  'invalid-argument': 'Check the supported Content filters and try again.',
  'invalid-response': 'The API returned data that does not match the Content report contract. No results are displayed.',
  network: 'Cannot reach the Content API. Check the local API and development proxy, then try again.',
  unavailable: 'Content reports are unavailable. Check that the API has a completed Content publication and its Gold and publisher-reference dependencies.',
  capacity: 'The Content report service is busy. Wait a moment, then try again.',
  timeout: 'The Content report request took too long. Please try again.',
  execution: 'The Content report request could not be completed. Please try again.',
};
export class ContentError extends Error {
  constructor(public readonly kind: ErrorKind) { super(messages[kind]); this.name = 'ContentError'; }
}
const FILTERS = ['sourceId', 'from', 'to', 'referenceStatus', 'publisherCategory', 'declaredLanguage', 'countryCode'] as const;
const BASE_FIELDS = ['articleId', 'sourceId', 'title', 'url', 'summary', 'publishedAt', 'updatedAt', 'firstObservedAt', 'lastObservedAt', 'observationCount', 'distinctIngestionCount', 'qualityWarnings'];
const EXTRA_FIELDS = ['identifierKind', 'sourceArticleId', 'feedFormat', 'discoveredAt', 'discoveryDate', 'publisherName', 'publisherCategory', 'declaredLanguage', 'countryCode', 'referenceStatus', 'referenceId', 'referenceRevision', 'referenceFingerprint'];
const SOURCE = /^[a-z0-9][a-z0-9_-]{0,127}$/;
const HEX = /^[0-9a-f]{64}$/;
function matches(value: unknown, pattern: RegExp): value is string { return typeof value === 'string' && pattern.exec(value)?.[0] === value; }
function isRecord(value: unknown): value is Record<string, unknown> { return value !== null && typeof value === 'object' && !Array.isArray(value); }
function need(value: unknown): asserts value { if (!value) throw new ContentError('invalid-response'); }
function record(value: unknown): asserts value is Record<string, unknown> { need(isRecord(value)); }
function fields(value: Record<string, unknown>, keys: readonly string[]) { need(Reflect.ownKeys(value).length === keys.length && keys.every((key) => Object.hasOwn(value, key))); }
function date(value: unknown): value is string { return matches(value, /^[0-9]{4}-[0-9]{2}-[0-9]{2}$/) && isUtcDate(value); }
function timestamp(value: unknown): value is string {
  if (!matches(value, /^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{6}Z$/) || value.startsWith('0000')) return false;
  const ms = value.slice(0, 23) + 'Z', parsed = new Date(ms);
  return Number.isFinite(parsed.getTime()) && parsed.toISOString() === ms;
}
function scalarName(value: unknown): boolean {
  if (typeof value !== 'string') return false;
  const chars = Array.from(value), boundary = new Set([9, 10, 11, 12, 13, 32, 133, 160, 5760,
    8192, 8193, 8194, 8195, 8196, 8197, 8198, 8199, 8200, 8201, 8202, 8232, 8233, 8239, 8287, 12288]);
  return chars.length >= 1 && chars.length <= 200 && !boundary.has(chars[0]!.codePointAt(0)!) && !boundary.has(chars.at(-1)!.codePointAt(0)!)
    && chars.every((c) => { const n = c.codePointAt(0)!; return n >= 32 && !(n >= 127 && n <= 159) && n !== 8232 && n !== 8233 && n !== 65279 && !(n >= 0xd800 && n <= 0xdfff); });
}
export function contentQueryError(query: unknown): string | null {
  if (!isRecord(query) || Reflect.ownKeys(query).some((key) => typeof key !== 'string' || ![...FILTERS, 'limit', 'offset'].includes(key))) return 'Use only supported Content catalog filters.';
  if (Object.hasOwn(query, 'sourceId') && !matches(query.sourceId, SOURCE)) return 'Source ID must use 1–128 lowercase letters, digits, underscores or hyphens, starting with a letter or digit.';
  if (['from', 'to'].some((key) => Object.hasOwn(query, key) && !date(query[key]))) return 'Use valid discovery dates in YYYY-MM-DD format.';
  if (typeof query.from === 'string' && typeof query.to === 'string' && query.from > query.to) return 'From date must be on or before To date.';
  for (const [key, values] of Object.entries(CONTENT_VOCABULARY)) if (Object.hasOwn(query, key) && !(values as readonly unknown[]).includes(query[key])) return 'Choose a supported publisher-reference filter value.';
  if (Object.hasOwn(query, 'limit') && (!Number.isInteger(query.limit) || Number(query.limit) < 1 || Number(query.limit) > 1000)) return 'Limit must be a whole number from 1 to 1000.';
  if (Object.hasOwn(query, 'offset') && (!Number.isInteger(query.offset) || Number(query.offset) < 0 || Number(query.offset) > CONTENT_MAX_OFFSET)) return 'Offset must be a whole number from 0 to 1000000.';
  return null;
}
export function contentQueryParameters(query: ContentQuery): URLSearchParams {
  if (contentQueryError(query)) throw new ContentError('invalid-argument');
  const search = new URLSearchParams({ limit: String(query.limit ?? 10), offset: String(query.offset ?? 0) });
  for (const field of FILTERS) if (query[field] !== undefined) search.set(field, query[field]);
  return search;
}
export function contentIdentityKey(value: ContentIdentity): string {
  return JSON.stringify([value.publication, value.goldPublication, value.reference.referenceId, value.reference.revision, value.reference.snapshotFingerprint]);
}
export function parseContentCatalog(value: unknown, query: ContentQuery): ContentResponse {
  contentQueryParameters(query);
  record(value); fields(value, ['publication', 'productId', 'goldPublication', 'reference', 'filters', 'data', 'meta']);
  need(matches(value.publication, HEX) && matches(value.goldPublication, HEX) && value.productId === 'content-intelligence');
  record(value.reference); fields(value.reference, ['referenceId', 'revision', 'snapshotFingerprint']);
  const reference = value.reference;
  need(matches(reference.referenceId, /^[a-z0-9][a-z0-9_-]{0,63}$/) && matches(reference.revision, /^[a-z0-9][a-z0-9_-]{0,31}$/) && matches(reference.snapshotFingerprint, HEX));
  record(value.filters); fields(value.filters, FILTERS);
  for (const field of FILTERS) need(value.filters[field] === (query[field] ?? null));
  record(value.meta); fields(value.meta, ['returned', 'limit', 'offset', 'hasMore']);
  need(Array.isArray(value.data));
  const seen = new Set<string>();
  const baseRows = value.data.map((item: unknown) => {
    record(item); fields(item, [...BASE_FIELDS, ...EXTRA_FIELDS]);
    need(matches(item.articleId, HEX) && !seen.has(item.articleId) && matches(item.sourceId, SOURCE)); seen.add(item.articleId);
    for (const key of ['firstObservedAt', 'lastObservedAt', 'discoveredAt']) need(timestamp(item[key]));
    for (const key of ['publishedAt', 'updatedAt']) need(item[key] === null || timestamp(item[key]));
    for (const key of ['observationCount', 'distinctIngestionCount']) need(matches(item[key], /^(0|[1-9][0-9]{0,18})$/));
    need(date(item.discoveryDate) && item.discoveredAt === item.firstObservedAt && item.discoveryDate === String(item.discoveredAt).slice(0, 10));
    need((!query.from || item.discoveryDate >= query.from) && (!query.to || item.discoveryDate <= query.to));
    need(typeof item.sourceArticleId === 'string' && item.sourceArticleId.trim().length > 0);
    need(item.feedFormat === 'rss' ? ['rss_guid', 'rss_link'].includes(String(item.identifierKind)) : item.feedFormat === 'atom' && ['atom_id', 'atom_link'].includes(String(item.identifierKind)));
    need(Array.isArray(item.qualityWarnings));
    need(item.referenceId === reference.referenceId && item.referenceRevision === reference.revision && item.referenceFingerprint === reference.snapshotFingerprint);
    need(item.referenceStatus === 'MATCHED' || item.referenceStatus === 'MISSING');
    for (const key of Object.keys(CONTENT_VOCABULARY) as (keyof typeof CONTENT_VOCABULARY)[]) {
      need((key !== 'referenceStatus' && item[key] === null) || (CONTENT_VOCABULARY[key] as readonly unknown[]).includes(item[key]));
      need(query[key] === undefined || item[key] === query[key]);
    }
    if (item.referenceStatus === 'MISSING') need(['publisherName', 'publisherCategory', 'declaredLanguage', 'countryCode'].every((key) => item[key] === null));
    else need(scalarName(item.publisherName));
    return Object.fromEntries(BASE_FIELDS.map((key) => [key, item[key]]));
  });
  // Share Gold field validation, never the Gold workspace's session pin.
  try { parseArticles({ publication: value.publication, data: baseRows, meta: value.meta }, query.limit ?? 10, query.offset ?? 0, query.sourceId ?? ''); }
  catch { throw new ContentError('invalid-response'); }
  return value as unknown as ContentResponse;
}
export async function fetchContentCatalog(query: ContentQuery, signal: AbortSignal): Promise<ContentResponse> {
  const search = contentQueryParameters(query);
  let response: Response;
  try { response = await fetch(`/api/v1/content/catalog?${search}`, { signal, cache: 'no-store', credentials: 'omit', headers: { Accept: 'application/json' } }); }
  catch (error) { if (signal.aborted) throw error; throw new ContentError('network'); }
  if (response.status !== 200) {
    if (response.status === 503) {
      let body: unknown; try { body = await response.json(); } catch { /* A proxy may return non-JSON. */ }
      throw new ContentError(isRecord(body) && body.code === 'ANALYTICS_CAPACITY_EXCEEDED' ? 'capacity' : 'unavailable');
    }
    throw new ContentError(response.status === 400 ? 'invalid-argument' : response.status === 504 ? 'timeout' : 'execution');
  }
  try {
    need(response.headers.get('content-type')?.split(';', 1)[0]?.trim().toLowerCase() === 'application/json');
    return parseContentCatalog(await response.json(), query);
  } catch (error) { if (signal.aborted) throw error; throw new ContentError('invalid-response'); }
}

export type ContentReport = 'catalog' | 'source_discovery_daily' | 'reference_coverage';
export interface ContentDailyRow {
  sourceId: string; discoveryDate: string; discoveredArticleCount: string; articlesMissingReferenceCount: string;
}
export interface ContentCoverageRow {
  sourceId: string; referenceStatus: 'MATCHED' | 'MISSING'; referenceUsage: 'USED' | 'UNUSED' | 'ABSENT';
  publisherName: string | null; publisherCategory: ContentRow['publisherCategory'];
  declaredLanguage: ContentRow['declaredLanguage']; countryCode: ContentRow['countryCode'];
  articleCount: string; articlesMissingReferenceCount: string;
  missingOptionalAttributes: ('publisher_category' | 'declared_language' | 'country_code')[];
}
export const CONTENT_COVERAGE_TOTALS = ['articleCount', 'matchedArticleCount', 'missingArticleCount', 'catalogSourceCount', 'matchedSourceCount', 'missingSourceCount', 'matchedSourcesMissingOptionalAttributesCount', 'referenceSourceCount', 'unusedReferenceSourceCount'] as const;
export type ContentCoverageTotals = Record<typeof CONTENT_COVERAGE_TOTALS[number], string>;
export interface ContentDailyResponse extends ContentIdentity {
  productId: 'content-intelligence'; filters: { sourceId: string | null; from: string | null; to: string | null };
  data: ContentDailyRow[]; meta: ContentResponse['meta'];
}
export interface ContentCoverageResponse extends ContentIdentity {
  productId: 'content-intelligence'; filters: { sourceId: string | null };
  data: ContentCoverageRow[]; totals: ContentCoverageTotals; meta: ContentResponse['meta'];
}
export type ContentReportResponse = ContentResponse | ContentDailyResponse | ContentCoverageResponse;
const REPORT_FILTERS = { catalog: FILTERS, source_discovery_daily: ['sourceId', 'from', 'to'], reference_coverage: ['sourceId'] } as const;
export function contentReportQueryError(report: ContentReport, query: unknown): string | null {
  if (!Object.hasOwn(REPORT_FILTERS, report)) return 'Choose a supported Content report.';
  const allowed: readonly string[] = [...REPORT_FILTERS[report], 'limit', 'offset'];
  if (!isRecord(query) || Reflect.ownKeys(query).some(key => typeof key !== 'string' || !allowed.includes(key))) return 'Use only supported filters for this Content report.';
  return contentQueryError(query);
}
export function contentReportQueryParameters(report: ContentReport, query: ContentQuery): URLSearchParams {
  if (contentReportQueryError(report, query)) throw new ContentError('invalid-argument');
  return contentQueryParameters({ ...query, limit: query.limit ?? (report === 'catalog' ? 10 : 100) });
}
function reportEnvelope(value: unknown, report: Exclude<ContentReport, 'catalog'>, query: ContentQuery): asserts value is Record<string, unknown> & { data: unknown[]; reference: ContentIdentity['reference'] } {
  contentReportQueryParameters(report, query);
  record(value); fields(value, ['publication', 'productId', 'goldPublication', 'reference', 'filters', 'data', 'meta', ...(report === 'reference_coverage' ? ['totals'] : [])]);
  need(matches(value.publication, HEX) && matches(value.goldPublication, HEX) && value.productId === 'content-intelligence');
  record(value.reference); fields(value.reference, ['referenceId', 'revision', 'snapshotFingerprint']);
  need(matches(value.reference.referenceId, /^[a-z0-9][a-z0-9_-]{0,63}$/) && matches(value.reference.revision, /^[a-z0-9][a-z0-9_-]{0,31}$/) && matches(value.reference.snapshotFingerprint, HEX));
  record(value.filters); fields(value.filters, REPORT_FILTERS[report]);
  for (const field of REPORT_FILTERS[report]) need(value.filters[field] === (query[field] ?? null));
  record(value.meta); fields(value.meta, ['returned', 'limit', 'offset', 'hasMore']);
  need(Array.isArray(value.data) && value.data.length <= (query.limit ?? 100));
  need(value.meta.limit === (query.limit ?? 100) && value.meta.offset === (query.offset ?? 0) && value.meta.returned === value.data.length);
  need(typeof value.meta.hasMore === 'boolean' && (!value.meta.hasMore || value.data.length === value.meta.limit));
}
function metric(value: unknown): bigint {
  need(matches(value, /^(0|[1-9][0-9]{0,18})$/));
  const count = BigInt(value); need(count <= 9223372036854775807n); return count;
}
export function parseContentDiscoveryDaily(value: unknown, query: ContentQuery): ContentDailyResponse {
  reportEnvelope(value, 'source_discovery_daily', query);
  let previous = '';
  const missingBySource = new Map<string, boolean>();
  for (const row of value.data) {
    record(row); fields(row, ['sourceId', 'discoveryDate', 'discoveredArticleCount', 'articlesMissingReferenceCount']);
    need(matches(row.sourceId, SOURCE) && date(row.discoveryDate));
    need(!query.sourceId || row.sourceId === query.sourceId);
    need((!query.from || row.discoveryDate >= query.from) && (!query.to || row.discoveryDate <= query.to));
    const key = `${row.sourceId}\0${row.discoveryDate}`; need(key > previous); previous = key;
    const count = metric(row.discoveredArticleCount), missing = metric(row.articlesMissingReferenceCount);
    need(count > 0n && (missing === 0n || missing === count));
    need(!missingBySource.has(row.sourceId) || missingBySource.get(row.sourceId) === (missing > 0n));
    missingBySource.set(row.sourceId, missing > 0n);
  }
  return value as unknown as ContentDailyResponse;
}
export function parseContentReferenceCoverage(value: unknown, query: ContentQuery): ContentCoverageResponse {
  reportEnvelope(value, 'reference_coverage', query);
  record(value.totals); fields(value.totals, CONTENT_COVERAGE_TOTALS);
  const totals = Object.fromEntries(CONTENT_COVERAGE_TOTALS.map(key => [key, metric(value.totals && (value.totals as Record<string, unknown>)[key])])) as Record<typeof CONTENT_COVERAGE_TOTALS[number], bigint>;
  need(totals.articleCount === totals.matchedArticleCount + totals.missingArticleCount);
  need(totals.catalogSourceCount === totals.matchedSourceCount + totals.missingSourceCount);
  need(totals.referenceSourceCount === totals.matchedSourceCount + totals.unusedReferenceSourceCount);
  need(totals.matchedSourcesMissingOptionalAttributesCount <= totals.matchedSourceCount);
  need(totals.matchedArticleCount >= totals.matchedSourceCount && totals.missingArticleCount >= totals.missingSourceCount);
  need((totals.matchedArticleCount === 0n) === (totals.matchedSourceCount === 0n));
  need((totals.missingArticleCount === 0n) === (totals.missingSourceCount === 0n));
  const domain = totals.catalogSourceCount + totals.unusedReferenceSourceCount;
  if (query.sourceId) need(domain <= 1n);
  const offset = BigInt(query.offset ?? 0), limit = BigInt(query.limit ?? 100);
  const remaining = domain > offset ? domain - offset : 0n;
  need(BigInt(value.data.length) === (remaining < limit ? remaining : limit));
  record(value.meta); need(value.meta.hasMore === (remaining > limit));
  const sums = Object.fromEntries(CONTENT_COVERAGE_TOTALS.map(key => [key, 0n])) as typeof totals;
  let previous = '';
  for (const row of value.data) {
    record(row); fields(row, ['sourceId', 'referenceStatus', 'referenceUsage', 'publisherName', 'publisherCategory', 'declaredLanguage', 'countryCode', 'articleCount', 'articlesMissingReferenceCount', 'missingOptionalAttributes']);
    need(matches(row.sourceId, SOURCE) && row.sourceId > previous && (!query.sourceId || row.sourceId === query.sourceId)); previous = row.sourceId;
    const count = metric(row.articleCount), missing = metric(row.articlesMissingReferenceCount);
    need(['USED', 'UNUSED', 'ABSENT'].includes(String(row.referenceUsage)));
    const absent = row.referenceUsage === 'ABSENT', used = row.referenceUsage === 'USED';
    need(row.referenceStatus === (absent ? 'MISSING' : 'MATCHED'));
    need(row.referenceUsage === 'UNUSED' ? count === 0n : count > 0n);
    need(missing === (absent ? count : 0n));
    for (const key of ['publisherCategory', 'declaredLanguage', 'countryCode'] as const) need(row[key] === null || (CONTENT_VOCABULARY[key] as readonly unknown[]).includes(row[key]));
    need(absent ? ['publisherName', 'publisherCategory', 'declaredLanguage', 'countryCode'].every(key => row[key] === null) : scalarName(row.publisherName));
    const optional = [['publisherCategory', 'publisher_category'], ['declaredLanguage', 'declared_language'], ['countryCode', 'country_code']] as const;
    const expected = absent ? [] : optional.filter(([key]) => row[key] === null).map(([, name]) => name);
    need(Array.isArray(row.missingOptionalAttributes) && row.missingOptionalAttributes.length === expected.length && expected.every((name, i) => (row.missingOptionalAttributes as unknown[])[i] === name));
    sums.articleCount += count; sums.matchedArticleCount += absent ? 0n : count; sums.missingArticleCount += missing;
    sums.catalogSourceCount += count > 0n ? 1n : 0n; sums.matchedSourceCount += used ? 1n : 0n; sums.missingSourceCount += absent ? 1n : 0n;
    sums.matchedSourcesMissingOptionalAttributesCount += used && expected.length ? 1n : 0n;
    sums.referenceSourceCount += absent ? 0n : 1n; sums.unusedReferenceSourceCount += row.referenceUsage === 'UNUSED' ? 1n : 0n;
  }
  for (const key of CONTENT_COVERAGE_TOTALS) need(offset === 0n && !value.meta.hasMore ? sums[key] === totals[key] : sums[key] <= totals[key]);
  return value as unknown as ContentCoverageResponse;
}
export async function fetchContentReport(report: ContentReport, query: ContentQuery, signal: AbortSignal): Promise<ContentReportResponse> {
  if (report === 'catalog') return fetchContentCatalog(query, signal);
  const search = contentReportQueryParameters(report, query);
  const path = report === 'source_discovery_daily' ? 'source-discovery-daily' : 'reference-coverage';
  let response: Response;
  try { response = await fetch(`/api/v1/content/${path}?${search}`, { signal, cache: 'no-store', credentials: 'omit', headers: { Accept: 'application/json' } }); }
  catch (error) { if (signal.aborted) throw error; throw new ContentError('network'); }
  if (response.status !== 200) {
    if (response.status === 503) {
      let body: unknown; try { body = await response.json(); } catch { /* A proxy may return non-JSON. */ }
      throw new ContentError(isRecord(body) && body.code === 'ANALYTICS_CAPACITY_EXCEEDED' ? 'capacity' : 'unavailable');
    }
    throw new ContentError(response.status === 400 ? 'invalid-argument' : response.status === 504 ? 'timeout' : 'execution');
  }
  try {
    need(response.headers.get('content-type')?.split(';', 1)[0]?.trim().toLowerCase() === 'application/json');
    const value: unknown = await response.json();
    return report === 'source_discovery_daily' ? parseContentDiscoveryDaily(value, query) : parseContentReferenceCoverage(value, query);
  } catch (error) { if (signal.aborted) throw error; throw new ContentError('invalid-response'); }
}
