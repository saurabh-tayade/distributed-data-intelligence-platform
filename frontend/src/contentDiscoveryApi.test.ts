import { describe, expect, it, vi } from 'vitest';
import { fetchContentProfiles, fetchContentSources, fetchPublisherReferences, parseContentProfiles, parseContentSources, parsePublisherReferences } from './managementApi';
import { jsonResponse } from './test/fixtures';
import { CORRELATION, FIRST_CURSOR, managementFailure } from './test/managementFixtures';
import { contentProfile, contentSource, CONTENT_PROFILE, historyPage, publisherReference } from './test/contentSubmissionFixtures';

const query = { executionProfileId: CONTENT_PROFILE };
describe('metadata-only Content discovery contracts', () => {
  it('retains exact narrow profile, source and publisher-reference metadata', () => {
    expect(parseContentProfiles(historyPage([contentProfile()])).data).toEqual([contentProfile()]);
    expect(parseContentSources(historyPage([contentSource()]), query).data).toEqual([contentSource()]);
    expect(parsePublisherReferences(historyPage([publisherReference()]), query).data).toEqual([publisherReference()]);
    expect(parseContentProfiles(historyPage([])).data).toEqual([]);
    expect(parseContentSources(historyPage([]), query).data).toEqual([]);
    expect(parsePublisherReferences(historyPage([]), query).data).toEqual([]);
  });
  it.each([
    { productId: 'application-reliability' }, { pipeline: 'RELIABILITY_V2' }, { runProfileVersion: 1 }, { pipelineVersion: '1' },
    { sourceProfile: 'request-jsonl-v2' }, { inputFormat: 'JSONL' }, { executionProfileId: 'bad' }, { configurationSha256: 'X'.repeat(64) },
    { createdAt: 'yesterday' }, { supportedSelection: { freshSourceCount: 2, retainedSilverCount: 0, publisherReferenceCount: 1 } },
    { supportedSelection: { freshSourceCount: 1, retainedSilverCount: 1, publisherReferenceCount: 1 } },
    { supportedSelection: { freshSourceCount: 1, retainedSilverCount: 0, publisherReferenceCount: 2 } },
    { supportedSelection: { freshSourceCount: 1, retainedSilverCount: 0, publisherReferenceCount: 1, force: true } }, { privatePath: '/private' },
  ])('rejects incompatible profile %j', (change) => expect(() => parseContentProfiles(historyPage([{ ...contentProfile(), ...change }]))).toThrow());
  it.each([
    { dispatchDisabled: false }, { dispatchDisabled: 'true' }, { qualificationScope: 'INSTALLED_RUNTIME' }, { runtimeQualification: 'READY' },
  ])('rejects contradictory qualification markers in every catalog %j', (change) => {
    expect(() => parseContentProfiles(historyPage([{ ...contentProfile(), ...change }]))).toThrow();
    expect(() => parseContentSources(historyPage([{ ...contentSource(), ...change }]), query)).toThrow();
    expect(() => parsePublisherReferences(historyPage([{ ...publisherReference(), ...change }]), query)).toThrow();
  });
  it.each([
    { executionProfileId: '81000000-0000-4000-8000-000000000002' }, { productId: 'application-reliability' }, { pipeline: 'RELIABILITY_V2' },
    { sourceId: ' Alpha ' }, { revision: '2' }, { revision: 0 }, { revision: 1001 }, { sourceProfile: 'rss' }, { configurationSha256: null }, { enabled: true },
  ])('rejects malformed or cross-profile source %j', (change) => expect(() => parseContentSources(historyPage([{ ...contentSource(), ...change }]), query)).toThrow());
  it.each([
    { role: 'SERVICE_REFERENCE' }, { kind: 'SILVER' }, { productId: 'application-reliability' }, { contractVersion: 2 }, { processorRevision: 2 },
    { nativeFormat: 'JSONL' }, { artifactId: 'bad' }, { storageBindingId: '/private' }, { nativeFingerprint: null }, { verifiedAt: 'bad' }, { relativeAddress: 'private' },
    { identity: { referenceId: 'pub', revision: 1 } }, { identity: { referenceId: 'pub/private', revision: 'r1' } }, { identity: { referenceId: 'pub', revision: 'r1', secret: true } },
  ])('rejects incompatible publisher reference %j', (change) => expect(() => parsePublisherReferences(historyPage([{ ...publisherReference(), ...change }]), query)).toThrow());
  it('requires every declared field, including metadata and nested selection fields', () => {
    for (const [row, parse] of [
      [contentProfile(), (value: unknown) => parseContentProfiles(value)],
      [contentSource(), (value: unknown) => parseContentSources(value, query)],
      [publisherReference(), (value: unknown) => parsePublisherReferences(value, query)],
    ] as const) {
      for (const key of Object.keys(row)) {
        const incomplete: Record<string, unknown> = { ...row }; delete incomplete[key]; expect(() => parse(historyPage([incomplete]))).toThrow();
      }
    }
  });
  it('enforces envelopes, ordering, unique rows, page bounds and returned opaque cursors', () => {
    const profiles = historyPage(Array.from({ length: 20 }, (_, i) => contentProfile(`81000000-0000-4000-8000-${(i + 1).toString(16).padStart(12, '0')}`)), 20, FIRST_CURSOR);
    expect(parseContentProfiles(profiles).meta.nextCursor).toBe(FIRST_CURSOR);
    for (const invalid of [{ ...profiles, apiVersion: 2 }, { ...profiles, secret: 1 }, { ...profiles, data: profiles.data.slice(0, 1) },
      { ...profiles, data: [...profiles.data].reverse() }, { ...profiles, meta: { ...profiles.meta, nextCursor: 'arbitrary' } },
      { ...profiles, meta: { ...profiles.meta, limit: 21 } }, { ...profiles, meta: { ...profiles.meta, hasMore: false } }]) expect(() => parseContentProfiles(invalid)).toThrow();
    expect(() => parseContentProfiles(profiles, { cursor: FIRST_CURSOR })).toThrow();
    expect(() => parseContentSources(historyPage([contentSource('z'), contentSource('a')]), query)).toThrow();
    expect(() => parseContentSources(historyPage([contentSource(), contentSource()]), query)).toThrow();
    expect(() => parsePublisherReferences(historyPage([publisherReference(), publisherReference()]), query)).toThrow();
  });
  it.each([1, 20, 100])('supports bounded page limit %s', (limit) => {
    expect(parseContentProfiles(historyPage([contentProfile()], limit), { limit }).meta.limit).toBe(limit);
    expect(parseContentSources(historyPage([contentSource()], limit), { ...query, limit }).meta.limit).toBe(limit);
    expect(parsePublisherReferences(historyPage([publisherReference()], limit), { ...query, limit }).meta.limit).toBe(limit);
  });
  it('uses explicit opt-in GETs and scoped filters, without mutation or background traversal', async () => {
    const fetch = vi.fn().mockResolvedValueOnce(jsonResponse(historyPage([contentProfile()])))
      .mockResolvedValueOnce(jsonResponse(historyPage([contentSource()], 100))).mockResolvedValueOnce(jsonResponse(historyPage([publisherReference()])));
    vi.stubGlobal('fetch', fetch); const abort = new AbortController();
    await fetchContentProfiles({}, abort.signal); await fetchContentSources({ ...query, limit: 100, cursor: FIRST_CURSOR }, abort.signal); await fetchPublisherReferences(query, abort.signal);
    const urls = fetch.mock.calls.map(([path]) => Object.fromEntries(new URL(path, window.location.origin).searchParams));
    expect(urls).toEqual([{ limit: '20', pipeline: 'CONTENT_RSS_V1' }, { limit: '100', cursor: FIRST_CURSOR, executionProfileId: CONTENT_PROFILE }, { limit: '20', executionProfileId: CONTENT_PROFILE, role: 'PUBLISHER_REFERENCE' }]);
    expect(fetch.mock.calls.every(([, init]) => init.method === 'GET' && init.signal === abort.signal && init.cache === 'no-store' && init.credentials === 'omit')).toBe(true);
  });
  it('rejects invalid profile/filter/limit/cursor queries before fetching', async () => {
    const fetch = vi.fn(); vi.stubGlobal('fetch', fetch); const signal = new AbortController().signal;
    for (const invalid of [{ executionProfileId: 'bad' }, { ...query, sourceId: 'alpha' }, { ...query, role: 'RETAINED_SILVER' }, { ...query, limit: 101 }, { ...query, cursor: 'bad' }]) {
      await expect(fetchContentSources(invalid, signal)).rejects.toMatchObject({ kind: 'invalid-request' });
      await expect(fetchPublisherReferences(invalid, signal)).rejects.toMatchObject({ kind: 'invalid-request' });
    }
    await expect(fetchContentProfiles({ limit: 0 }, signal)).rejects.toMatchObject({ kind: 'invalid-request' }); expect(fetch).not.toHaveBeenCalled();
  });
  it.each([[403, 'LOCAL_BOUNDARY_REJECTED', 'local-boundary'], [503, 'MANAGEMENT_UNAVAILABLE', 'unavailable'], [400, 'INVALID_ARGUMENT', 'invalid-request']])('retains sanitized discovery errors for HTTP %s', async (status, code, kind) => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(jsonResponse(managementFailure(String(code)), Number(status))));
    await expect(fetchContentProfiles({}, new AbortController().signal)).rejects.toMatchObject({ kind, code, correlationId: CORRELATION });
  });
  it('distinguishes malformed responses from empty pages and respects cancellation', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(jsonResponse({ apiVersion: 1, data: [] })));
    await expect(fetchContentSources(query, new AbortController().signal)).rejects.toMatchObject({ kind: 'invalid-response' });
    const abort = new AbortController(); abort.abort(); vi.stubGlobal('fetch', vi.fn().mockRejectedValue(new DOMException('Aborted', 'AbortError')));
    await expect(fetchPublisherReferences(query, abort.signal)).rejects.toMatchObject({ name: 'AbortError' });
  });
});
