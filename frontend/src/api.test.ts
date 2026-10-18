import { describe, expect, it, vi } from 'vitest';
import { DashboardError, fetchPublishers, parsePublishers } from './api';
import { fixture, jsonResponse } from './test/fixtures';

describe('publisher REST boundary', () => {
  it('requests the existing same-origin endpoint with the exact source and limit', async () => {
    const response = fixture();
    response.data = response.data.slice(0, 1);
    response.meta = { returned: 1, limit: 10 };
    const fetch = vi.fn().mockResolvedValue(jsonResponse(response));
    vi.stubGlobal('fetch', fetch);
    const signal = new AbortController().signal;
    await expect(fetchPublishers('alpha', signal, 10)).resolves.toEqual(response);
    expect(fetch).toHaveBeenCalledWith('/api/v1/publishers?limit=10&sourceId=alpha', {
      signal, cache: 'no-store', credentials: 'omit', headers: { Accept: 'application/json' },
    });
  });

  it('uses limit 100 without a source by default and preserves null/microseconds', async () => {
    const fetch = vi.fn().mockResolvedValue(jsonResponse(fixture()));
    vi.stubGlobal('fetch', fetch);
    await expect(fetchPublishers('', new AbortController().signal)).resolves.toEqual(fixture());
    expect(fetch.mock.calls[0]?.[0]).toBe('/api/v1/publishers?limit=100');
  });

  it('accepts a verified empty result and exact maximum signed 64-bit counts', () => {
    const empty = fixture();
    empty.data = [];
    empty.meta.returned = 0;
    expect(parsePublishers(empty, 100)).toEqual(empty);
    const large = fixture();
    large.data[0]!.observationCount = '9223372036854775807';
    expect(parsePublishers(large, 100).data[0]?.observationCount).toBe('9223372036854775807');
  });

  it.each([
    [400, 'INVALID_ARGUMENT', 'invalid-argument'],
    [503, 'PUBLICATION_UNAVAILABLE', 'unavailable'],
    [503, 'ANALYTICS_RUNTIME_UNAVAILABLE', 'unavailable'],
    [503, 'ANALYTICS_CAPACITY_EXCEEDED', 'capacity'],
    [500, 'ANALYTICS_EXECUTION_FAILED', 'execution'],
    [500, 'ANALYTICS_RESPONSE_INVALID', 'execution'],
    [504, 'ANALYTICS_TIMEOUT', 'timeout'],
    [502, 'UNKNOWN', 'execution'],
  ])('handles HTTP %s / %s without displaying internal diagnostics', async (status, code, kind) => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(jsonResponse({ code, message: '/private/secret traceback' }, Number(status))));
    const error = await fetchPublishers('', new AbortController().signal).catch((error: unknown) => error);
    expect(error).toBeInstanceOf(DashboardError);
    expect(error).toHaveProperty('kind', kind);
    expect((error as Error).message).not.toMatch(/secret|traceback/);
  });

  it('handles a non-JSON proxy failure', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response('private path', { status: 503 })));
    await expect(fetchPublishers('', new AbortController().signal)).rejects.toHaveProperty('kind', 'unavailable');
  });

  it('sanitizes network failures', async () => {
    vi.stubGlobal('fetch', vi.fn().mockRejectedValue(new Error('secret network config')));
    await expect(fetchPublishers('', new AbortController().signal)).rejects.toHaveProperty('kind', 'network');
  });

  it('does not turn cancellation into a network error', async () => {
    const controller = new AbortController();
    controller.abort();
    const abort = new DOMException('aborted', 'AbortError');
    vi.stubGlobal('fetch', vi.fn().mockRejectedValue(abort));
    await expect(fetchPublishers('', controller.signal)).rejects.toBe(abort);
  });

  it.each(['alpha OR 1=1', '../gold', '--report', 'ALPHA', ' '])('rejects invalid sources before fetching: %s', async (source) => {
    const fetch = vi.fn();
    vi.stubGlobal('fetch', fetch);
    await expect(fetchPublishers(source, new AbortController().signal)).rejects.toHaveProperty('kind', 'invalid-argument');
    expect(fetch).not.toHaveBeenCalled();
  });

  it.each([0, -1, 1001, 1.5, Number.NaN])('rejects invalid limits: %s', async (limit) => {
    await expect(fetchPublishers('', new AbortController().signal, limit)).rejects.toHaveProperty('kind', 'invalid-argument');
  });

  const malformed: [string, (value: ReturnType<typeof fixture>) => unknown][] = [
    ['null', () => null], ['array envelope', () => []], ['absent fields', () => ({})],
    ['extra fields', (v) => ({ ...v, unexpected: true })],
    ['wrong fingerprint', (v) => ({ ...v, publication: 'latest' })],
    ['wrong rows type', (v) => ({ ...v, data: {} })],
    ['wrong returned count', (v) => ({ ...v, meta: { returned: 1, limit: 100 } })],
    ['wrong limit', (v) => ({ ...v, meta: { returned: 2, limit: 10 } })],
    ['string returned count', (v) => ({ ...v, meta: { returned: '2', limit: 100 } })],
    ['blank source', (v) => { v.data[0]!.sourceId = ' '; return v; }],
    ['duplicate sources', (v) => { v.data[1]!.sourceId = 'alpha'; return v; }],
    ['missing row fields', (v) => ({ ...v, data: [{ sourceId: 'alpha' }, v.data[1]] })],
    ['non-string count', (v) => ({ ...v, data: [{ ...v.data[0], articleCount: 3 }, v.data[1]] })],
    ['null count', (v) => ({ ...v, data: [{ ...v.data[0], observationCount: null }, v.data[1]] })],
    ...['-1', '1.5', '01', '1e3', '', '9223372036854775808'].map((count): [string, (v: ReturnType<typeof fixture>) => unknown] =>
      [`invalid count ${count}`, (v) => { v.data[0]!.articleCount = count; return v; }]),
    ...['2026-02-30T23:59:59.123456Z', '2026-10-12T24:00:00.123456Z', '2026-10-12T23:59:59.123Z',
      '2026-10-12T23:59:59.123456+00:00', '0000-10-12T23:59:59.123456Z'].map((time): [string, (v: ReturnType<typeof fixture>) => unknown] =>
      [`invalid timestamp ${time}`, (v) => { v.data[0]!.firstObservedAt = time; return v; }]),
  ];

  it.each(malformed)('rejects %s', (_, mutate) => {
    expect(() => parsePublishers(mutate(fixture()), 100)).toThrow(DashboardError);
  });

  it('rejects rows outside the requested source or limit', () => {
    expect(() => parsePublishers(fixture(), 100, 'alpha')).toThrow(DashboardError);
    const tooMany = fixture();
    tooMany.meta.limit = 1;
    expect(() => parsePublishers(tooMany, 1)).toThrow(DashboardError);
  });

  it.each(['not JSON', '{}'])('rejects malformed successful responses: %s', async (body) => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response(body, { headers: { 'Content-Type': 'application/json' } })));
    await expect(fetchPublishers('', new AbortController().signal)).rejects.toHaveProperty('kind', 'invalid-response');
  });

  it('rejects an HTML success response', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response('<html>proxy</html>')));
    await expect(fetchPublishers('', new AbortController().signal)).rejects.toHaveProperty('kind', 'invalid-response');
  });
});
