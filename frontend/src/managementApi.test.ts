import { describe, expect, it, vi } from 'vitest';
import { fetchSources, ManagementError, parseSources } from './managementApi';
import type { SourcesQuery } from './managementApi';
import { jsonResponse } from './test/fixtures';
import { CORRELATION, FIRST_CURSOR, fullSourcePage, managementFailure, source, sourcePage } from './test/managementFixtures';

describe('management source contract', () => {
  it('preserves exact large versions, UTC microseconds, composite source keys and empty results', () => {
    const value = sourcePage([source('same', 'application-reliability'), source('same')]);
    expect(parseSources(value)).toEqual(value);
    expect(parseSources(value).data[0]?.rowVersion).toBe('9007199254740993');
    expect(parseSources(sourcePage([])).data).toEqual([]);
    expect(parseSources(fullSourcePage()).meta.nextCursor).toBe(FIRST_CURSOR);
    for (const displayName of ['\u00a0', '\u2007', '\u202f', '\ufeff', 'é'.repeat(64)]) {
      const page = sourcePage([{ ...source(), displayName }]);
      expect(parseSources(page)).toEqual(page);
    }
  });

  it.each([
    ['analytics envelope', () => ({ publication: 'a'.repeat(64), data: [], meta: { limit: 20, returned: 0 } })],
    ['wrong version', (v) => ({ ...v, apiVersion: '1' })],
    ['missing meta', (v) => ({ ...v, meta: null })],
    ['extra root field', (v) => ({ ...v, privateEvidence: {} })],
    ['invalid correlation', (v) => ({ ...v, meta: { ...v.meta, correlationId: '<secret>' } })],
    ['wrong limit', (v) => ({ ...v, meta: { ...v.meta, limit: 100 } })],
    ['extra meta', (v) => ({ ...v, meta: { ...v.meta, returned: 1 } })],
    ['missing cursor', (v) => ({ ...v, meta: { correlationId: CORRELATION, limit: 20, hasMore: false } })],
    ['cursor mismatch', (v) => ({ ...v, meta: { ...v.meta, nextCursor: FIRST_CURSOR } })],
    ['short page with more', (v) => ({ ...v, meta: { ...v.meta, nextCursor: FIRST_CURSOR, hasMore: true } })],
    ['unbounded rows', (v) => ({ ...v, data: Array.from({ length: 21 }, (_, i) => source(`s${i}`)) })],
    ['duplicate rows', (v) => ({ ...v, data: [source(), source()] })],
    ['unordered rows', (v) => ({ ...v, data: [source('beta'), source('alpha')] })],
    ['row config included', (v) => ({ ...v, data: [{ ...source(), config: {} }] })],
    ['invalid source ID', (v) => ({ ...v, data: [{ ...source(), sourceId: '<script>' }] })],
    ['unknown product', (v) => ({ ...v, data: [{ ...source(), productId: 'unknown' }] })],
    ['nonboolean gate', (v) => ({ ...v, data: [{ ...source(), enabled: 'true' }] })],
    ['oversized revision', (v) => ({ ...v, data: [{ ...source(), currentRevision: 1001 }] })],
    ['numeric version', (v) => ({ ...v, data: [{ ...source(), rowVersion: 1 }] })],
    ['overflow version', (v) => ({ ...v, data: [{ ...source(), rowVersion: '9223372036854775808' }] })],
    ['leading-zero version', (v) => ({ ...v, data: [{ ...source(), rowVersion: '01' }] })],
    ['bad digest', (v) => ({ ...v, data: [{ ...source(), configSha256: 'latest' }] })],
    ['bad calendar', (v) => ({ ...v, data: [{ ...source(), updatedAt: '2026-02-30T12:00:00.000000Z' }] })],
    ['missing timestamp', (v) => ({ ...v, data: [{ ...source(), createdAt: null }] })],
    ['long UTF-8 label', (v) => ({ ...v, data: [{ ...source(), displayName: 'é'.repeat(65) }] })],
    ['blank label', (v) => ({ ...v, data: [{ ...source(), displayName: '' }] })],
    ['whitespace label', (v) => ({ ...v, data: [{ ...source(), displayName: ' \u2000\u3000' }] })],
    ['control label', (v) => ({ ...v, data: [{ ...source(), displayName: 'bad\nlabel' }] })],
    ['surrogate label', (v) => ({ ...v, data: [{ ...source(), displayName: '\ud800' }] })],
  ] satisfies [string, (value: ReturnType<typeof sourcePage>) => unknown][])('rejects %s', (_, change) => {
    expect(() => parseSources(change(sourcePage()))).toThrow(ManagementError);
  });

  it('refuses rows outside the requested filters and cursors that do not advance', () => {
    expect(() => parseSources(sourcePage(), { productId: 'application-reliability' })).toThrow();
    expect(() => parseSources(sourcePage(), { enabled: false })).toThrow();
    expect(() => parseSources(fullSourcePage(), { cursor: FIRST_CURSOR })).toThrow();
    const tooLong = fullSourcePage('x'.repeat(470) + '.' + 's'.repeat(43));
    expect(() => parseSources(tooLong)).toThrow();
  });

  it('sends only a bounded GET with encoded filters and an opaque cursor', async () => {
    const row = { ...source('alpha', 'application-reliability'), enabled: false };
    const fetch = vi.fn().mockResolvedValue(jsonResponse(sourcePage([row], 100)));
    vi.stubGlobal('fetch', fetch);
    const signal = new AbortController().signal;
    await fetchSources({ productId: 'application-reliability', enabled: false, limit: 100, cursor: FIRST_CURSOR }, signal);
    expect(fetch).toHaveBeenCalledWith(`/api/management/v1/sources?limit=100&productId=application-reliability&enabled=false&cursor=${FIRST_CURSOR}`, {
      method: 'GET', signal, cache: 'no-store', credentials: 'omit', headers: { Accept: 'application/json' },
    });
  });

  it.each([{ limit: 0 }, { limit: 101 }, { limit: 1.5 }, { cursor: '' }, { cursor: 'not-signed' },
    { productId: 'unknown' }, { enabled: 'false' }])('rejects invalid query %j before transport', async (query) => {
    const fetch = vi.fn(); vi.stubGlobal('fetch', fetch);
    await expect(fetchSources(query as SourcesQuery, new AbortController().signal)).rejects.toHaveProperty('kind', 'invalid-request');
    expect(fetch).not.toHaveBeenCalled();
  });

  it.each([
    [400, 'INVALID_ARGUMENT', 'invalid-request'], [403, 'LOCAL_BOUNDARY_REJECTED', 'local-boundary'],
    [404, 'NOT_FOUND', 'not-found'], [429, 'MANAGEMENT_CAPACITY_EXCEEDED', 'capacity'],
    [500, 'INTERNAL_FAILURE', 'server'], [503, 'MANAGEMENT_UNAVAILABLE', 'unavailable'],
  ])('maps HTTP %s to safe classifications, never remote messages', async (status, code, kind) => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(jsonResponse(managementFailure(code as string), status as number)));
    const error = await fetchSources({}, new AbortController().signal).catch((error: unknown) => error);
    expect(error).toMatchObject({ kind, code, correlationId: CORRELATION, retryable: code === 'MANAGEMENT_UNAVAILABLE' });
    expect((error as Error).message).not.toMatch(/secret|traceback/);
  });

  it.each([true, false])('retains explicit server retryability %s without scheduling another request', async (retryable) => {
    const fetch = vi.fn().mockResolvedValue(jsonResponse({ ...managementFailure(), retryable }, 503));
    vi.stubGlobal('fetch', fetch);
    const error = await fetchSources({}, new AbortController().signal).catch((error: unknown) => error);
    expect(error).toMatchObject({ kind: 'unavailable', retryable, code: 'MANAGEMENT_UNAVAILABLE', correlationId: CORRELATION });
    expect(fetch).toHaveBeenCalledTimes(1);
  });

  it('handles non-JSON errors, malformed success and unsupported codes without disclosing bodies', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValueOnce(new Response('<html>secret</html>', { status: 503 }))
      .mockResolvedValueOnce(jsonResponse({ ...managementFailure('PRIVATE_SECRET'), correlationId: '/private/path' }, 503))
      .mockResolvedValueOnce(jsonResponse({ ...managementFailure(), retryable: 'true' }, 503))
      .mockResolvedValueOnce(new Response('not JSON', { headers: { 'Content-Type': 'application/json' } }))
      .mockResolvedValueOnce(new Response(JSON.stringify(sourcePage()), { headers: { 'Content-Type': 'text/html' } })));
    for (const kind of ['unavailable', 'unavailable', 'unavailable', 'invalid-response', 'invalid-response']) {
      const error = await fetchSources({}, new AbortController().signal).catch((error: unknown) => error);
      expect(error).toMatchObject({ kind, code: null, correlationId: null, retryable: null });
      expect((error as Error).message).not.toContain('secret');
    }
  });

  it('distinguishes a network error from cancellation', async () => {
    const controller = new AbortController(), abort = new DOMException('aborted', 'AbortError');
    vi.stubGlobal('fetch', vi.fn().mockRejectedValueOnce(new Error('secret')).mockRejectedValueOnce(abort));
    await expect(fetchSources({}, controller.signal)).rejects.toHaveProperty('kind', 'network');
    controller.abort();
    await expect(fetchSources({}, controller.signal)).rejects.toBe(abort);
  });
});
