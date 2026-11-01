import { describe, expect, it, vi } from 'vitest';
import { fetchSource, fetchRevisions, ManagementError, parseSource, parseRevisions } from './managementApi';
import { jsonResponse } from './test/fixtures';
import { CORRELATION, FIRST_CURSOR, fullRevisionPage, managementFailure, revision, revisionPage, sourceEnvelope, sourceRecord, sourceResponse } from './test/managementFixtures';

const identity = { productId: 'content-intelligence' as const, sourceId: 'alpha' };
const etag = '"9007199254740993"';

describe('source detail and immutable revision contracts', () => {
  it('retains decimal row versions, ETags, microseconds and nullable Content references', async () => {
    const record = sourceRecord();
    const fetch = vi.fn().mockImplementation(async () => sourceResponse(record)); vi.stubGlobal('fetch', fetch);
    const signal = new AbortController().signal, result = await fetchSource(identity, signal);
    expect(result).toEqual({ ...sourceEnvelope(record), etag });
    expect(result.data.rowVersion).toBe('9007199254740993'); expect(result.data.config.registeredProfileSha256).toBeNull();
    expect(fetch).toHaveBeenCalledWith('/api/management/v1/sources/content-intelligence/alpha', {
      method: 'GET', signal, credentials: 'omit', cache: 'no-store', headers: { Accept: 'application/json' },
    });
  });

  it.each(['request-jsonl-v2', 'request-parquet-v1'] as const)('validates Reliability profile %s and nullable endpoint', (profile) => {
    const record = sourceRecord('same', 'application-reliability');
    if (record.config.dataOrigin !== 'synthetic') throw new Error('fixture');
    record.config.profile = profile;
    expect(parseSource(sourceEnvelope(record), record, etag).data).toEqual(record);
    expect(parseRevisions(revisionPage([revision(1, record)]), record).data[0]?.config.feedEndpointId).toBeNull();
  });

  it.each([null, '', '9007199254740993', 'W/"9007199254740993"', '"1"', '"09007199254740993"'])('rejects missing or incompatible ETag %s', (tag) => {
    expect(() => parseSource(sourceEnvelope(), identity, tag)).toThrow(ManagementError);
  });

  it.each([
    ['version', (v) => ({ ...v, apiVersion: 2 })],
    ['extra meta', (v) => ({ ...v, meta: { ...v.meta, extra: true } })],
    ['missing data', (v) => ({ ...v, data: null })],
    ['wrong source', (v) => ({ ...v, data: { ...v.data, sourceId: 'beta' } })],
    ['wrong product', (v) => ({ ...v, data: { ...v.data, productId: 'application-reliability' } })],
    ['numeric version', (v) => ({ ...v, data: { ...v.data, rowVersion: 42 } })],
    ['extra private field', (v) => ({ ...v, data: { ...v.data, privatePath: '/private/secret' } })],
    ['missing config', (v) => ({ ...v, data: { ...v.data, config: undefined } })],
    ['unknown config field', (v) => ({ ...v, data: { ...v.data, config: { ...v.data.config, url: 'https://secret' } } })],
    ['config version', (v) => ({ ...v, data: { ...v.data, config: { ...v.data.config, configVersion: 2 } } })],
    ['wrong profile', (v) => ({ ...v, data: { ...v.data, config: { ...v.data.config, profile: 'request-jsonl-v2' } } })],
    ['wrong origin', (v) => ({ ...v, data: { ...v.data, config: { ...v.data.config, dataOrigin: 'synthetic' } } })],
    ['nullable storage', (v) => ({ ...v, data: { ...v.data, config: { ...v.data.config, inputStorageProfileId: null } } })],
    ['nullable Content endpoint', (v) => ({ ...v, data: { ...v.data, config: { ...v.data.config, feedEndpointId: null } } })],
    ['non-null Content profile hash', (v) => ({ ...v, data: { ...v.data, config: { ...v.data.config, registeredProfileSha256: 'b'.repeat(64) } } })],
  ] satisfies [string, (v: ReturnType<typeof sourceEnvelope>) => unknown][])('rejects %s', (_, change) => {
    expect(() => parseSource(change(sourceEnvelope()), identity, etag)).toThrow(ManagementError);
  });

  it('rejects invalid Reliability nullability and malformed reference identities', () => {
    const record = sourceRecord('r', 'application-reliability');
    for (const change of [{ feedEndpointId: CORRELATION }, { registeredProfileSha256: null }, { registeredProfileSha256: 'bad' }, { inputStorageProfileId: '/private/path' }]) {
      expect(() => parseSource(sourceEnvelope({ ...record, config: { ...record.config, ...change } } as typeof record), record, etag)).toThrow();
    }
  });

  it('preserves server revision order, independent historical configurations and exact page bounds', async () => {
    const historical = sourceRecord(); historical.config.feedEndpointId = '30000000-0000-4000-8000-000000000002';
    historical.configSha256 = 'c'.repeat(64);
    const page = revisionPage([revision(1, historical), revision(2)]);
    expect(parseRevisions(page, identity)).toEqual(page);
    expect(parseRevisions(revisionPage([]), identity).data).toEqual([]);
    const hundred = revisionPage(Array.from({ length: 100 }, (_, i) => revision(i + 1)), 100);
    expect(parseRevisions(hundred, identity, { limit: 100 }).data).toHaveLength(100);
    expect(parseRevisions(revisionPage([revision(1000)], 1), identity, { limit: 1 }).data[0]?.revision).toBe(1000);
    const fetch = vi.fn().mockResolvedValue(jsonResponse(fullRevisionPage(21, null))); vi.stubGlobal('fetch', fetch);
    await fetchRevisions(identity, { cursor: FIRST_CURSOR }, new AbortController().signal);
    expect(fetch.mock.calls[0]?.[0]).toBe(`/api/management/v1/sources/content-intelligence/alpha/revisions?limit=20&cursor=${FIRST_CURSOR}`);
  });

  it.each([
    ['duplicate', (v) => ({ ...v, data: [revision(1), revision(1)] })],
    ['descending', (v) => ({ ...v, data: [revision(2), revision(1)] })],
    ['out of range', (v) => ({ ...v, data: [revision(1001)] })],
    ['fraction', (v) => ({ ...v, data: [revision(1.5)] })],
    ['zero', (v) => ({ ...v, data: [revision(0)] })],
    ['wrong identity', (v) => ({ ...v, data: [revision(1, sourceRecord('other'))] })],
    ['missing configuration', (v) => ({ ...v, data: [{ ...revision(), config: null }] })],
    ['unexpected revision field', (v) => ({ ...v, data: [{ ...revision(), enabled: true }] })],
    ['bad fingerprint', (v) => ({ ...v, data: [{ ...revision(), configSha256: 'bad' }] })],
    ['bad label', (v) => ({ ...v, data: [{ ...revision(), displayName: '' }] })],
    ['bad timestamp', (v) => ({ ...v, data: [{ ...revision(), createdAt: null }] })],
    ['missing next cursor', (v) => ({ ...v, meta: { ...v.meta, nextCursor: undefined } })],
    ['short page with more', (v) => ({ ...v, meta: { ...v.meta, hasMore: true, nextCursor: FIRST_CURSOR } })],
    ['oversized page', (v) => ({ ...v, data: Array.from({ length: 21 }, (_, i) => revision(i + 1)) })],
    ['wrong API version', (v) => ({ ...v, apiVersion: '1' })],
  ] satisfies [string, (v: ReturnType<typeof revisionPage>) => unknown][])('rejects revision %s', (_, change) => {
    expect(() => parseRevisions(change(revisionPage()), identity)).toThrow(ManagementError);
  });

  it.each([0, 101, 1.5])('rejects revision limit %s before transport', async (limit) => {
    const fetch = vi.fn(); vi.stubGlobal('fetch', fetch);
    await expect(fetchRevisions(identity, { limit }, new AbortController().signal)).rejects.toMatchObject({ kind: 'invalid-request' });
    expect(fetch).not.toHaveBeenCalled();
  });

  it.each([[404, 'NOT_FOUND', 'not-found'], [403, 'LOCAL_BOUNDARY_REJECTED', 'local-boundary'], [503, 'MANAGEMENT_UNAVAILABLE', 'unavailable']] as const)(
    'decodes safe HTTP %s detail and history failures', async (status, code, kind) => {
      vi.stubGlobal('fetch', vi.fn().mockImplementation(async () => jsonResponse(managementFailure(code), status)));
      for (const call of [fetchSource(identity, new AbortController().signal), fetchRevisions(identity, {}, new AbortController().signal)]) {
        await expect(call).rejects.toMatchObject({ kind, code, correlationId: CORRELATION, retryable: status === 503 });
      }
    });

  it('rejects malformed wire bodies and media types without leaking diagnostics', async () => {
    for (const response of [jsonResponse({ apiVersion: 1, data: '/private/secret' }), new Response('traceback'), sourceResponse()]) {
      vi.stubGlobal('fetch', vi.fn().mockResolvedValue(response));
      // The otherwise valid response deliberately has the wrong requested identity.
      await expect(fetchSource({ ...identity, sourceId: 'other' }, new AbortController().signal)).rejects.toMatchObject({ kind: 'invalid-response' });
    }
  });

  it('rejects invalid identities and cursors and propagates transport aborts', async () => {
    const fetch = vi.fn(); vi.stubGlobal('fetch', fetch);
    await expect(fetchSource({ ...identity, sourceId: '../private' }, new AbortController().signal)).rejects.toMatchObject({ kind: 'invalid-request' });
    await expect(fetchRevisions(identity, { cursor: 'invented' }, new AbortController().signal)).rejects.toMatchObject({ kind: 'invalid-request' });
    expect(fetch).not.toHaveBeenCalled();
    const controller = new AbortController(), abort = new DOMException('aborted', 'AbortError'); controller.abort();
    fetch.mockRejectedValue(abort);
    await expect(fetchRevisions(identity, {}, controller.signal)).rejects.toBe(abort);
  });
});
