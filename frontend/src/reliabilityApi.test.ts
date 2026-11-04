import { describe, expect, it, vi } from 'vitest';
import {
  fetchRequestWindows, parseRequestWindows, RELIABILITY_LABEL, RELIABILITY_MAX_OFFSET,
  ReliabilityError, reliabilityQueryError, reliabilityQueryParameters, type ReliabilityQuery,
} from './reliabilityApi';
import { jsonResponse } from './test/fixtures';
import { reliabilityFixture, reliabilityQuery, reliabilityRow } from './test/reliabilityFixtures';

describe('reliability query boundary', () => {
  it('preserves exact six-digit UTC timestamps and applies bounded pagination defaults', () => {
    expect(reliabilityQueryError(reliabilityQuery())).toBeNull();
    expect(Object.fromEntries(reliabilityQueryParameters(reliabilityQuery()))).toEqual({
      start: '2026-10-20T12:00:00.000000Z', end: '2026-10-20T13:00:00.000000Z', limit: '100', offset: '0',
    });
  });

  it.each([
    { start: '0001-01-01T00:00:00.000000Z', end: '0001-01-01T00:05:00.000000Z' },
    { start: '1969-12-31T23:55:00.000000Z', end: '1970-01-01T00:00:00.000000Z' },
    { start: '2024-02-29T23:55:00.000000Z', end: '2024-03-01T00:00:00.000000Z' },
    { start: '2026-01-01T00:00:00.000000Z', end: '2026-02-01T00:00:00.000000Z' },
    { start: '9999-12-31T23:55:00.000000Z', end: '10000-01-01T00:00:00.000000Z' },
  ])('accepts calendar and interval boundary %j', (query) => {
    expect(reliabilityQueryError(query)).toBeNull();
    expect(parseRequestWindows(reliabilityFixture(query), query).filters).toMatchObject(query);
  });

  it('accepts exact optional-field limits and all native environments', () => {
    for (const environment of ['dev', 'test', 'production'] as const) {
      expect(reliabilityQueryError(reliabilityQuery({ serviceId: 'a'.repeat(128), environment, limit: 1000, offset: RELIABILITY_MAX_OFFSET }))).toBeNull();
    }
    expect(reliabilityQueryError(reliabilityQuery({ serviceId: '0_service-id', limit: 1, offset: 0 }))).toBeNull();
  });

  const invalid: [string, unknown][] = [
    ['null', null], ['array', []], ['missing bounds', {}], ['missing start', { end: reliabilityQuery().end }],
    ['missing end', { start: reliabilityQuery().start }],
    ['unknown source', { ...reliabilityQuery(), sourceId: 'alpha' }],
    ['unknown filter', { ...reliabilityQuery(), unexpected: undefined }],
    ['symbol key', { ...reliabilityQuery(), [Symbol('extra')]: true }],
    ...['', ' ', '2026-10-20', '2026-10-20T12:00:00Z', '2026-10-20T12:00:00.000Z',
      '2026-10-20T12:00:00.000001Z', '2026-10-20T12:00:01.000000Z', '2026-10-20T12:01:00.000000Z',
      '2026-10-20T12:00:00.000000+00:00', '2026-10-20T12:00:00.000000z', '2026-10-20T12:00:00.000000Z\n',
      '2026-02-29T12:00:00.000000Z', '2026-04-31T12:00:00.000000Z', '0000-10-20T12:00:00.000000Z',
      '2026-10-20T24:00:00.000000Z', '2026-10-20T12:60:00.000000Z', '2026-10-20T12:00:60.000000Z',
      '10000-01-01T00:00:00.000000Z', null, 123, true].map((start): [string, unknown] => [`bad start ${String(start)}`, { ...reliabilityQuery(), start }]),
    ...['', '2026-10-20T12:00:00.000000Z', '2026-10-20T11:55:00.000000Z', '2026-11-20T12:05:00.000000Z',
      '2026-10-20T13:01:00.000000Z', '10000-01-01T00:05:00.000000Z', '+010000-01-01T00:00:00.000000Z',
      '10000-01-01T00:00:00.000000Z\n', undefined, false].map((end): [string, unknown] => [`bad end ${String(end)}`, { ...reliabilityQuery(), end }]),
    ...['', ' ', 'ALPHA', '-alpha', '../service', 'alpha OR 1=1', 'a'.repeat(129), 'alpha\n', null, undefined, 1].map((serviceId): [string, unknown] =>
      [`bad service ${String(serviceId)}`, { ...reliabilityQuery(), serviceId }]),
    ...['', 'staging', 'TEST', 'test\n', null, undefined, true].map((environment): [string, unknown] =>
      [`bad environment ${String(environment)}`, { ...reliabilityQuery(), environment }]),
    ...[0, -1, 1001, 1.5, NaN, Infinity, '100', true, null, undefined].map((limit): [string, unknown] =>
      [`bad limit ${String(limit)}`, { ...reliabilityQuery(), limit }]),
    ...[-1, 1_000_001, 0.1, NaN, Infinity, '0', false, null, undefined].map((offset): [string, unknown] =>
      [`bad offset ${String(offset)}`, { ...reliabilityQuery(), offset }]),
  ];
  it.each(invalid)('rejects %s before network access', async (_, query) => {
    const fetch = vi.fn();
    vi.stubGlobal('fetch', fetch);
    expect(reliabilityQueryError(query)).not.toBeNull();
    await expect(fetchRequestWindows(query as ReliabilityQuery, new AbortController().signal)).rejects.toHaveProperty('kind', 'invalid-argument');
    expect(fetch).not.toHaveBeenCalled();
  });
});

describe('reliability response boundary', () => {
  it.each(['JSONL', 'PARQUET'] as const)('accepts %s and preserves exact decimal strings and null reference context', (inputFormat) => {
    const response = reliabilityFixture();
    response.inputFormat = inputFormat;
    response.data[0] = reliabilityRow({ requestCount: '2000000', serverErrorCount: '2000000', serverErrorRate: 1,
      durationSumMs: '7200000000000', meanDurationMs: 3600000, p95DurationMsApprox: '3600000',
      serviceName: null, ownerTeam: null, serviceTier: null, referenceStatus: 'MISSING' });
    expect(parseRequestWindows(response, reliabilityQuery())).toEqual(response);
  });

  it('accepts empty results, nullable matched metadata and valid Unicode scalar text without normalization', () => {
    const empty = reliabilityFixture();
    empty.data = [];
    empty.meta.returned = 0;
    expect(parseRequestWindows(empty, reliabilityQuery())).toEqual(empty);
    for (const serviceName of ['🚀'.repeat(200), 'Service e\u0301', 'Service\u200bName', 'NULL']) {
      const response = reliabilityFixture();
      response.data[0] = reliabilityRow({ serviceName, ownerTeam: null, serviceTier: null });
      expect(parseRequestWindows(response, reliabilityQuery())).toEqual(response);
    }
  });

  it('accepts the exact 64-character reference ID and 32-character revision boundaries', () => {
    const response = reliabilityFixture();
    response.reference.referenceId = 'r'.repeat(64);
    response.reference.revision = 'v'.repeat(32);
    expect(parseRequestWindows(response, reliabilityQuery())).toEqual(response);
  });

  it.each([null, 'example-platform', 'example-service'])('accepts the native owner-team vocabulary member %s', (ownerTeam) => {
    const response = reliabilityFixture();
    response.data[0] = reliabilityRow({ ownerTeam });
    expect(parseRequestWindows(response, reliabilityQuery())).toEqual(response);
  });

  it('accepts zero durations/errors and the native absolute/relative ratio tolerance', () => {
    for (const row of [
      reliabilityRow({ requestCount: '1', serverErrorCount: '0', serverErrorRate: 0, durationSumMs: '0', meanDurationMs: 0, p95DurationMsApprox: '0' }),
      reliabilityRow({ serverErrorRate: 0.1 + 5e-13, meanDurationMs: 10.5 + 5e-12 }),
    ]) {
      const response = reliabilityFixture();
      response.data = [row];
      expect(parseRequestWindows(response, reliabilityQuery())).toEqual(response);
    }
  });

  it.each([
    { serverErrorRate: 0.1 + 2e-12 }, { meanDurationMs: 10.5 + 3e-11 },
    { serverErrorRate: 0 }, { meanDurationMs: 0 },
  ])('rejects inconsistent rate or mean outside the native tolerance: %j', (overrides) => {
    const response = reliabilityFixture();
    response.data = [reliabilityRow(overrides)];
    expect(() => parseRequestWindows(response, reliabilityQuery())).toThrow(ReliabilityError);
  });

  it('validates strict window, service and environment order using ASCII, not locale sorting', () => {
    const response = reliabilityFixture();
    response.data = [
      reliabilityRow({ serviceId: 'a-1', environment: 'dev' }),
      reliabilityRow({ serviceId: 'a-1', environment: 'production' }),
      reliabilityRow({ serviceId: 'a-1', environment: 'test' }),
      reliabilityRow({ serviceId: 'a_1' }),
      reliabilityRow({ windowStart: '2026-10-20T12:05:00.000000Z', serviceId: '0' }),
    ];
    response.meta.returned = response.data.length;
    expect(parseRequestWindows(response, reliabilityQuery())).toEqual(response);
    for (const pair of [[0, 1], [1, 2], [2, 3], [3, 4]]) {
      const bad = structuredClone(response);
      const [a, b] = pair as [number, number];
      [bad.data[a], bad.data[b]] = [bad.data[b]!, bad.data[a]!];
      expect(() => parseRequestWindows(bad, reliabilityQuery())).toThrow(ReliabilityError);
    }
  });

  it('requires exact filter echo and all rows inside the requested service and environment', () => {
    const query = reliabilityQuery({ serviceId: 'alpha', environment: 'test', limit: 1, offset: 25 });
    const response = reliabilityFixture(query);
    response.meta.hasMore = true;
    expect(parseRequestWindows(response, query)).toEqual(response);
    for (const wrong of [
      { ...response, filters: { ...response.filters, serviceId: null } },
      { ...response, filters: { ...response.filters, environment: null } },
      { ...response, data: [reliabilityRow({ serviceId: 'beta' })] },
      { ...response, data: [reliabilityRow({ environment: 'dev' })] },
    ]) expect(() => parseRequestWindows(wrong, query)).toThrow(ReliabilityError);
  });

  type Fixture = ReturnType<typeof reliabilityFixture>;
  const malformed: [string, (value: Fixture) => unknown][] = [
    ['null envelope', () => null], ['array envelope', () => []], ['empty envelope', () => ({})],
    ['extra envelope field', (v) => ({ ...v, diagnostics: 'secret' })],
    ['missing envelope field', (v) => { const { origin: _, ...rest } = v; void _; return rest; }],
    ...['latest', 'A'.repeat(64), 'a'.repeat(63), 'a'.repeat(64) + '\n', 1, null].map((publication): [string, (v: Fixture) => unknown] =>
      [`invalid publication ${String(publication)}`, (v) => ({ ...v, publication })]),
    ['wrong product', (v) => ({ ...v, productId: 'content-intelligence' })],
    ['wrong contract', (v) => ({ ...v, productContractVersion: 1 })],
    ['wrong revision', (v) => ({ ...v, processorRevision: '2' })],
    ['wrong format', (v) => ({ ...v, inputFormat: 'CSV' })],
    ['wrong origin', (v) => ({ ...v, origin: 'production' })],
    ['wrong label', (v) => ({ ...v, label: 'Production reliability' })],
    ['null reference', (v) => ({ ...v, reference: null })],
    ['extra reference field', (v) => ({ ...v, reference: { ...v.reference, path: 'secret' } })],
    ['wrong reference fingerprint', (v) => ({ ...v, reference: { ...v.reference, snapshotFingerprint: 'latest' } })],
    ['reference revision exceeds 32 characters', (v) => ({ ...v, reference: { ...v.reference, revision: 'r'.repeat(33) } })],
    ...['', ' ', 'UPPER', '../id', 'a'.repeat(65), 'r1\n', null].flatMap((id): [string, (v: Fixture) => unknown][] => [
      [`bad reference id ${String(id)}`, (v) => ({ ...v, reference: { ...v.reference, referenceId: id } })],
      [`bad reference revision ${String(id)}`, (v) => ({ ...v, reference: { ...v.reference, revision: id } })],
    ]),
    ['null filters', (v) => ({ ...v, filters: null })],
    ['extra filter', (v) => ({ ...v, filters: { ...v.filters, sourceId: null } })],
    ['wrong start echo', (v) => ({ ...v, filters: { ...v.filters, start: '2026-10-20T12:05:00.000000Z' } })],
    ['wrong end echo', (v) => ({ ...v, filters: { ...v.filters, end: '2026-10-20T14:00:00.000000Z' } })],
    ['absent filter is not blank', (v) => ({ ...v, filters: { ...v.filters, serviceId: '' } })],
    ['absent environment is not blank', (v) => ({ ...v, filters: { ...v.filters, environment: '' } })],
    ['wrong data type', (v) => ({ ...v, data: {} })],
    ['null row', (v) => ({ ...v, data: [null] })],
    ['array row', (v) => ({ ...v, data: [[]] })],
    ['missing row field', (v) => ({ ...v, data: [{ serviceId: 'alpha' }] })],
    ['extra row field', (v) => ({ ...v, data: [{ ...v.data[0], privatePath: 'secret' }] })],
    ['duplicate rows', (v) => ({ ...v, data: [v.data[0], v.data[0]], meta: { ...v.meta, returned: 2 } })],
    ['null meta', (v) => ({ ...v, meta: null })],
    ['extra meta field', (v) => ({ ...v, meta: { ...v.meta, total: 1 } })],
    ['wrong returned', (v) => ({ ...v, meta: { ...v.meta, returned: 0 } })],
    ['wrong returned type', (v) => ({ ...v, meta: { ...v.meta, returned: '1' } })],
    ['wrong limit echo', (v) => ({ ...v, meta: { ...v.meta, limit: 1 } })],
    ['wrong offset echo', (v) => ({ ...v, meta: { ...v.meta, offset: 1 } })],
    ['wrong hasMore type', (v) => ({ ...v, meta: { ...v.meta, hasMore: 'false' } })],
    ['hasMore requires a full page', (v) => ({ ...v, meta: { ...v.meta, hasMore: true } })],
  ];
  it.each(malformed)('rejects %s', (_, mutate) => {
    expect(() => parseRequestWindows(mutate(reliabilityFixture()), reliabilityQuery())).toThrow(ReliabilityError);
  });

  const badRows: [string, unknown][] = [
    ...['', '2026-10-20T11:55:00.000000Z', '2026-10-20T13:00:00.000000Z', '2026-10-20T12:00:00.000001Z',
      '2026-10-20T12:01:00.000000Z', '2026-10-20T12:00:00.000Z', '2026-10-20T12:00:00.000000Z\n',
      '10000-01-01T00:00:00.000000Z', null, 1].map((windowStart): [string, unknown] => ['windowStart', windowStart]),
    ...['', 'ALPHA', '../secret', 'a'.repeat(129), 'alpha\n', null].map((serviceId): [string, unknown] => ['serviceId', serviceId]),
    ...['', 'staging', 'TEST', null].map((environment): [string, unknown] => ['environment', environment]),
    ...['0', '-1', '01', '1e1', '1.0', '2000001', '20\n', 20, null].map((requestCount): [string, unknown] => ['requestCount', requestCount]),
    ...['-1', '00', '2.0', '21', '2000001', '2\n', 2, null].map((serverErrorCount): [string, unknown] => ['serverErrorCount', serverErrorCount]),
    ...[-0.01, 1.01, NaN, Infinity, '0.1', null].map((serverErrorRate): [string, unknown] => ['serverErrorRate', serverErrorRate]),
    ...['-1', '00', '1e3', '210.0', '72000001', '9'.repeat(38), '1' + '0'.repeat(38), '210\n', 210, null].map((durationSumMs): [string, unknown] => ['durationSumMs', durationSumMs]),
    ...[-1, 3600000.01, NaN, Infinity, '10.5', null].map((meanDurationMs): [string, unknown] => ['meanDurationMs', meanDurationMs]),
    ...['-1', '00', '19.5', '3600001', '19\n', 19, null].map((p95): [string, unknown] => ['p95DurationMsApprox', p95]),
    ...[null, '', ' ', ' Name', 'Name\u00a0', 'Name\u2000', '\u1680Name', 'Name\n', 'Name\u007f', 'Name\u0080',
      'Name\u2028', 'Name\u2029', 'Name\ufeff', 'Name\ud800', 'Name\udfff', '🚀'.repeat(201), 1].map((serviceName): [string, unknown] => ['serviceName', serviceName]),
    ...['', 'UPPER', 'some_team', '1team', 't'.repeat(65), 'team\n', 'not-in-vocabulary', 'platform', 1].map((ownerTeam): [string, unknown] => ['ownerTeam', ownerTeam]),
    ...['', 'gold', 'CRITICAL', 'critical\n', 1].map((serviceTier): [string, unknown] => ['serviceTier', serviceTier]),
    ...['missing', 'UNKNOWN', null, true].map((referenceStatus): [string, unknown] => ['referenceStatus', referenceStatus]),
  ];
  it.each(badRows)('rejects invalid row %s: %s', (field, value) => {
    const response = reliabilityFixture();
    response.data = [{ ...reliabilityRow(), [field]: value }];
    expect(() => parseRequestWindows(response, reliabilityQuery())).toThrow(ReliabilityError);
  });

  it('requires all missing-reference metadata to be null and rejects oversized pages', () => {
    for (const overrides of [{ serviceName: 'Name' }, { ownerTeam: 'example-platform' }, { serviceTier: 'critical' }]) {
      const response = reliabilityFixture();
      response.data = [reliabilityRow({ referenceStatus: 'MISSING', serviceName: null, ownerTeam: null, serviceTier: null, ...overrides })];
      expect(() => parseRequestWindows(response, reliabilityQuery())).toThrow(ReliabilityError);
    }
    const query = reliabilityQuery({ limit: 1 });
    const response = reliabilityFixture(query);
    response.data.push(reliabilityRow({ serviceId: 'beta' }));
    response.meta.returned = 2;
    expect(() => parseRequestWindows(response, query)).toThrow(ReliabilityError);
  });
});

describe('reliability HTTP client', () => {
  it('sends the exact same-origin GET query with credentials omitted, no cache and an abort signal', async () => {
    const query = reliabilityQuery({ serviceId: 'alpha_1', environment: 'production', limit: 10, offset: 20 });
    const response = reliabilityFixture(query);
    const fetch = vi.fn().mockResolvedValue(jsonResponse(response));
    vi.stubGlobal('fetch', fetch);
    const signal = new AbortController().signal;
    await expect(fetchRequestWindows(query, signal)).resolves.toEqual(response);
    expect(fetch).toHaveBeenCalledExactlyOnceWith(
      '/api/v1/reliability/request-windows?start=2026-10-20T12%3A00%3A00.000000Z&end=2026-10-20T13%3A00%3A00.000000Z&limit=10&offset=20&serviceId=alpha_1&environment=production',
      { signal, cache: 'no-store', credentials: 'omit', headers: { Accept: 'application/json' } },
    );
  });

  it('omits absent filters and sends explicit pagination defaults', async () => {
    const fetch = vi.fn().mockResolvedValue(jsonResponse(reliabilityFixture()));
    vi.stubGlobal('fetch', fetch);
    await fetchRequestWindows(reliabilityQuery(), new AbortController().signal);
    expect(fetch.mock.calls[0]?.[0]).toBe('/api/v1/reliability/request-windows?start=2026-10-20T12%3A00%3A00.000000Z&end=2026-10-20T13%3A00%3A00.000000Z&limit=100&offset=0');
  });

  it('captures the query before a caller can mutate it while the request is pending', async () => {
    const query = reliabilityQuery();
    const response = reliabilityFixture(query);
    vi.stubGlobal('fetch', vi.fn().mockImplementation(async () => { query.start = '2026-10-20T12:05:00.000000Z'; return jsonResponse(response); }));
    await expect(fetchRequestWindows(query, new AbortController().signal)).resolves.toEqual(response);
  });

  it.each([
    [400, 'INVALID_ARGUMENT', 'invalid-argument'], [503, 'PUBLICATION_UNAVAILABLE', 'unavailable'],
    [503, 'ANALYTICS_RUNTIME_UNAVAILABLE', 'unavailable'], [503, 'ANALYTICS_CAPACITY_EXCEEDED', 'capacity'],
    [500, 'ANALYTICS_EXECUTION_FAILED', 'execution'], [500, 'ANALYTICS_RESPONSE_INVALID', 'execution'],
    [504, 'ANALYTICS_TIMEOUT', 'timeout'], [502, 'UNKNOWN', 'execution'], [201, 'UNKNOWN', 'execution'],
    [500, 'ANALYTICS_CAPACITY_EXCEEDED', 'execution'],
  ])('maps HTTP %s / %s to safe local %s copy', async (status, code, kind) => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(jsonResponse({ code, message: '/private/secret traceback', label: 'evil' }, Number(status))));
    const error: unknown = await fetchRequestWindows(reliabilityQuery(), new AbortController().signal).catch((error: unknown) => error);
    expect(error).toBeInstanceOf(ReliabilityError);
    expect(error).toHaveProperty('kind', kind);
    expect((error as Error).message).not.toMatch(/secret|traceback|evil|gold|publisher/i);
  });

  it('sanitizes non-JSON proxy and network failures', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response('private path', { status: 503 })));
    await expect(fetchRequestWindows(reliabilityQuery(), new AbortController().signal)).rejects.toHaveProperty('kind', 'unavailable');
    vi.stubGlobal('fetch', vi.fn().mockRejectedValue(new Error('private config')));
    const error: unknown = await fetchRequestWindows(reliabilityQuery(), new AbortController().signal).catch((error: unknown) => error);
    expect(error).toHaveProperty('kind', 'network');
    expect((error as Error).message).not.toMatch(/private config/);
  });

  it('preserves fetch cancellation and body-read cancellation instead of showing an error', async () => {
    const controller = new AbortController();
    controller.abort();
    const abort = new DOMException('aborted', 'AbortError');
    vi.stubGlobal('fetch', vi.fn().mockRejectedValue(abort));
    await expect(fetchRequestWindows(reliabilityQuery(), controller.signal)).rejects.toBe(abort);
    const response = jsonResponse(reliabilityFixture());
    vi.spyOn(response, 'json').mockRejectedValue(abort);
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(response));
    await expect(fetchRequestWindows(reliabilityQuery(), controller.signal)).rejects.toBe(abort);
  });

  it.each([
    ['not JSON', 'application/json'], ['{}', 'application/json'], ['<html>secret</html>', 'text/html'],
    [JSON.stringify(reliabilityFixture()), 'text/application/json'],
  ])('rejects malformed success body or media type: %s / %s', async (body, contentType) => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response(body, { headers: { 'Content-Type': contentType } })));
    await expect(fetchRequestWindows(reliabilityQuery(), new AbortController().signal)).rejects.toHaveProperty('kind', 'invalid-response');
  });

  it('accepts JSON media types with parameters and keeps the synthetic label exact', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response(JSON.stringify(reliabilityFixture()), { headers: { 'Content-Type': 'Application/JSON; charset=utf-8' } })));
    await expect(fetchRequestWindows(reliabilityQuery(), new AbortController().signal)).resolves.toHaveProperty('label', RELIABILITY_LABEL);
    expect(new ReliabilityError('publication-changed').message).not.toMatch(/gold|publisher/i);
  });
});
