import { describe, expect, it, vi } from 'vitest';
import { fetchReliabilityArtifacts, fetchReliabilityProfiles, parseReliabilityArtifacts, parseReliabilityProfiles } from './managementApi';
import { jsonResponse } from './test/fixtures';
import { CORRELATION, FIRST_CURSOR, managementFailure } from './test/managementFixtures';
import { batch, fullProfiles, historyPage, profile, PROFILE_ID, reference } from './test/submissionFixtures';

const query = { executionProfileId: PROFILE_ID, role: 'RELIABILITY_BATCH' as const };
describe('bounded Reliability discovery contracts', () => {
  it('reads both profile formats and keeps exact UUID, digest and timestamps', () => {
    const rows = [profile(), profile('80000000-0000-4000-8000-000000000002', true)];
    expect(parseReliabilityProfiles(historyPage(rows)).data).toEqual(rows);
    expect(parseReliabilityProfiles(historyPage([])).data).toEqual([]);
  });
  it.each([
    { productId: 'content-intelligence' }, { pipeline: 'CONTENT_RSS_V1' }, { runProfileVersion: 2 }, { pipelineVersion: '1' },
    { sourceProfile: 'unknown' }, { inputFormat: 'PARQUET' }, { executionProfileId: 'NOT-A-UUID' }, { configurationSha256: 'bad' }, { createdAt: null }, { secret: 'private' },
  ])('rejects incompatible or malformed profiles %j', (change) => {
    expect(() => parseReliabilityProfiles(historyPage([{ ...profile(), ...change }]))).toThrow();
  });
  it.each(['executionProfileId', 'sourceProfile', 'configurationSha256'])('rejects missing profile %s', (field) => {
    const row: Record<string, unknown> = { ...profile() }; delete row[field];
    expect(() => parseReliabilityProfiles(historyPage([row]))).toThrow();
  });
  it('validates envelopes, exact limits, cursor syntax, page fullness and ordering', () => {
    const page = fullProfiles(FIRST_CURSOR); expect(parseReliabilityProfiles(page).meta.nextCursor).toBe(FIRST_CURSOR);
    for (const invalid of [{ ...page, apiVersion: 2 }, { ...page, extra: 1 }, { ...page, meta: { ...page.meta, limit: 100 } },
      { ...page, meta: { ...page.meta, nextCursor: 'arbitrary' } }, { ...page, data: page.data.slice(0, 1) }, { ...page, data: [...page.data].reverse() },
      { ...page, meta: { ...page.meta, hasMore: false } }]) expect(() => parseReliabilityProfiles(invalid)).toThrow();
    expect(() => parseReliabilityProfiles(page, { cursor: FIRST_CURSOR })).toThrow();
  });
  it.each([1, 20, 100])('accepts an explicit bounded page size %s', (limit) => {
    expect(parseReliabilityProfiles(historyPage([profile()], limit), { limit }).meta.limit).toBe(limit);
  });
  it('keeps batch and service-reference identities separate, including null native format', () => {
    expect(parseReliabilityArtifacts(historyPage([batch()]), query).data).toEqual([batch()]);
    expect(parseReliabilityArtifacts(historyPage([reference()]), { ...query, role: 'SERVICE_REFERENCE' }).data).toEqual([reference()]);
    expect(parseReliabilityArtifacts(historyPage([]), query).data).toEqual([]);
  });
  it.each([
    { role: 'SERVICE_REFERENCE' }, { productId: 'content-intelligence' }, { kind: 'SILVER' }, { contractVersion: 1 }, { processorRevision: 1 },
    { nativeFormat: null }, { storageBindingId: '/private/path' }, { nativeFingerprint: null }, { relativeAddress: 'private' }, { verifiedAt: 'yesterday' },
  ])('rejects malformed or wrong-role artifact %j', (change) => {
    expect(() => parseReliabilityArtifacts(historyPage([{ ...batch(), ...change }]), query)).toThrow();
  });
  it.each([{ sourceRevision: '2' }, { sourceRevision: 1001 }, { batchId: 'A/secret' }, { inputFormat: 'PARQUET' }, { registeredProfileSha256: null }])('validates captured batch identity %j', (change) => {
    expect(() => parseReliabilityArtifacts(historyPage([{ ...batch(), identity: { ...batch().identity, ...change } }]), query)).toThrow();
  });
  it('rejects duplicates, absent fields, wrong source filters and invalid reference identities', () => {
    expect(() => parseReliabilityArtifacts(historyPage([batch(), batch()]), query)).toThrow();
    expect(() => parseReliabilityArtifacts(historyPage([batch()]), { ...query, sourceId: 'other' })).toThrow();
    const row = { ...reference() } as Record<string, unknown>; delete row.nativeFingerprint;
    expect(() => parseReliabilityArtifacts(historyPage([row]), { ...query, role: 'SERVICE_REFERENCE' })).toThrow();
    expect(() => parseReliabilityArtifacts(historyPage([{ ...reference(), identity: { referenceId: 'services', revision: 1 } }]), { ...query, role: 'SERVICE_REFERENCE' })).toThrow();
  });
  it('sends only relative GETs, supported filters and the returned opaque cursor', async () => {
    const fetch = vi.fn().mockResolvedValueOnce(jsonResponse(historyPage([profile()])))
      .mockResolvedValueOnce(jsonResponse(historyPage([batch()], 100)));
    vi.stubGlobal('fetch', fetch);
    const abort = new AbortController(); await fetchReliabilityProfiles({}, abort.signal);
    await fetchReliabilityArtifacts({ ...query, sourceId: 'alpha', limit: 100, cursor: FIRST_CURSOR }, abort.signal);
    expect(fetch.mock.calls[0]![0]).toBe('/api/management/v1/execution-profiles?limit=20');
    const url = new URL(fetch.mock.calls[1]![0], 'http://localhost');
    expect(Object.fromEntries(url.searchParams)).toEqual({ limit: '100', executionProfileId: PROFILE_ID, role: 'RELIABILITY_BATCH', sourceId: 'alpha', cursor: FIRST_CURSOR });
    expect(fetch.mock.calls[1]![1]).toMatchObject({ method: 'GET', credentials: 'omit', cache: 'no-store', signal: abort.signal });
  });
  it.each([0, 101, 1.5])('rejects invalid limits before making a request: %s', async (limit) => {
    const fetch = vi.fn(); vi.stubGlobal('fetch', fetch);
    await expect(fetchReliabilityProfiles({ limit }, new AbortController().signal)).rejects.toMatchObject({ kind: 'invalid-request' }); expect(fetch).not.toHaveBeenCalled();
  });
  it('rejects invalid artifact filters before fetching', async () => {
    const fetch = vi.fn(); vi.stubGlobal('fetch', fetch);
    for (const invalid of [{ ...query, executionProfileId: 'bad' }, { ...query, role: 'SERVICE_REFERENCE' as const, sourceId: 'alpha' }, { ...query, sourceId: ' Alpha ' }]) {
      await expect(fetchReliabilityArtifacts(invalid, new AbortController().signal)).rejects.toMatchObject({ kind: 'invalid-request' });
    }
    expect(fetch).not.toHaveBeenCalled();
  });
  it.each([[403, 'LOCAL_BOUNDARY_REJECTED', 'local-boundary'], [503, 'MANAGEMENT_UNAVAILABLE', 'unavailable'], [400, 'INVALID_ARGUMENT', 'invalid-request']])('decodes only safe errors for %s', async (status, code, kind) => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(jsonResponse(managementFailure(String(code)), Number(status))));
    await expect(fetchReliabilityProfiles({}, new AbortController().signal)).rejects.toMatchObject({ kind, code, correlationId: CORRELATION });
  });
  it('distinguishes a malformed success from empty discovery and propagates cancellation', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(jsonResponse({ apiVersion: 1, data: [] })));
    await expect(fetchReliabilityProfiles({}, new AbortController().signal)).rejects.toMatchObject({ kind: 'invalid-response' });
    const abort = new AbortController(); abort.abort();
    vi.stubGlobal('fetch', vi.fn().mockRejectedValue(new DOMException('Aborted', 'AbortError')));
    await expect(fetchReliabilityProfiles({}, abort.signal)).rejects.toMatchObject({ name: 'AbortError' });
  });
});
