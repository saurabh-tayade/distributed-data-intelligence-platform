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
  'invalid-response': 'The API returned data that does not match the Content catalog contract. No results are displayed.',
  network: 'Cannot reach the Content catalog API. Check the local API and development proxy, then try again.',
  unavailable: 'Content catalog is unavailable. Check that the API has a completed Content publication and its Gold and publisher-reference dependencies.',
  capacity: 'The Content catalog service is busy. Wait a moment, then try again.',
  timeout: 'The Content catalog request took too long. Please try again.',
  execution: 'The Content catalog request could not be completed. Please try again.',
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
