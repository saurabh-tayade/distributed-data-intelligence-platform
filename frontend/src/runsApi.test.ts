import { describe, expect, it, vi } from 'vitest';
import { fetchRuns, ManagementError, parseRuns, RUN_OUTCOMES, RUN_STAGES, RUN_STATES } from './managementApi';
import type { RunsQuery } from './managementApi';
import { jsonResponse } from './test/fixtures';
import { CORRELATION, FIRST_CURSOR, managementFailure } from './test/managementFixtures';
import { fullRunPage, run, runPage } from './test/runFixtures';

const signal = () => new AbortController().signal;
describe('persisted execution history contract', () => {
  it('preserves nullable records, UUIDs, decimal versions, precision and server submission order', () => {
    const first = run(100), second = { ...run(1), requestedAt: '2026-11-02T12:34:56.654321Z' };
    const body = runPage([first, second]);
    expect(parseRuns(body)).toEqual(body); // submission_seq is not exposed; timestamps/UUIDs cannot replace API ordering.
    expect(parseRuns(body).data[0]).toMatchObject({ rowVersion: '9007199254740993', currentStage: null, result: null, outcomeCode: null, finishedAt: null });
    expect(parseRuns(runPage([])).data).toEqual([]);
  });

  it.each(RUN_STATES)('accepts supported run state %s', (state) => {
    expect(parseRuns(runPage([run(1, state)]), { state }).data[0]?.state).toBe(state);
  });

  it('preserves Content and Reliability verified results and synthetic metadata', () => {
    for (const product of ['content-intelligence', 'application-reliability'] as const) {
      const row = run(1, 'SUCCEEDED', product), body = runPage([row]);
      expect(parseRuns(body)).toEqual(body); expect(parseRuns(body).data[0]?.result?.verificationStatus).toBe('VERIFIED_AT_EXECUTION');
      expect(parseRuns(body).data[0]?.result?.verifiedAt).toBe('2026-11-01T12:39:59.987654Z');
    }
  });

  it('accepts every public outcome/stage without treating it as an HTTP error or action', () => {
    for (const outcomeCode of RUN_OUTCOMES) expect(parseRuns(runPage([{ ...run(1, 'FAILED'), outcomeCode }])).data[0]?.outcomeCode).toBe(outcomeCode);
    for (const currentStage of RUN_STAGES) expect(parseRuns(runPage([{ ...run(1, 'RUNNING'), currentStage }])).data[0]?.currentStage).toBe(currentStage);
    expect(parseRuns(runPage([{ ...run(), retryEligibility: true }])).data[0]?.retryEligibility).toBe(true);
    expect(parseRuns(runPage([{ ...run(), rowVersion: '9223372036854775807' }])).data[0]?.rowVersion).toBe('9223372036854775807');
  });

  it('uses exactly supported bounded GET parameters and relative proxy access', async () => {
    const body = runPage([run(3, 'RUNNING', 'application-reliability')], 100), fetch = vi.fn().mockResolvedValue(jsonResponse(body));
    vi.stubGlobal('fetch', fetch); const requestSignal = signal();
    await expect(fetchRuns({ productId: 'application-reliability', sourceId: 'exact_source-1', state: 'RUNNING', limit: 100, cursor: FIRST_CURSOR }, requestSignal)).resolves.toEqual(body);
    const [path, options] = fetch.mock.calls[0]!, url = new URL(path as string, 'http://localhost');
    expect(url.pathname).toBe('/api/management/v1/runs');
    expect(Object.fromEntries(url.searchParams)).toEqual({ productId: 'application-reliability', sourceId: 'exact_source-1', state: 'RUNNING', limit: '100', cursor: FIRST_CURSOR });
    expect(options).toEqual({ method: 'GET', signal: requestSignal, cache: 'no-store', credentials: 'omit', headers: { Accept: 'application/json' } });
  });

  it.each([
    ['bad API version', (v) => ({ ...v, apiVersion: '1' })],
    ['extra envelope field', (v) => ({ ...v, private: true })],
    ['missing data', (v) => ({ ...v, data: undefined })],
    ['missing meta', (v) => ({ ...v, meta: null })],
    ['bad correlation', (v) => ({ ...v, meta: { ...v.meta, correlationId: 'raw secret' } })],
    ['extra meta', (v) => ({ ...v, meta: { ...v.meta, returned: 1 } })],
    ['wrong limit', (v) => ({ ...v, meta: { ...v.meta, limit: 100 } })],
    ['missing cursor', (v) => ({ ...v, meta: { ...v.meta, nextCursor: undefined } })],
    ['nonboolean hasMore', (v) => ({ ...v, meta: { ...v.meta, hasMore: 'false' } })],
    ['cursor mismatch', (v) => ({ ...v, meta: { ...v.meta, nextCursor: FIRST_CURSOR } })],
    ['short page has more', (v) => ({ ...v, meta: { ...v.meta, hasMore: true, nextCursor: FIRST_CURSOR } })],
    ['too many records', (v) => ({ ...v, data: Array.from({ length: 21 }, (_, i) => run(i)) })],
    ['duplicate UUID', (v) => ({ ...v, data: [run(), run()] })],
    ['bad run UUID', (v) => ({ ...v, data: [{ ...run(), runId: '/private/path' }] })],
    ['uppercase UUID', (v) => ({ ...v, data: [{ ...run(15), runId: run(15).runId.toUpperCase() }] })],
    ['unknown product', (v) => ({ ...v, data: [{ ...run(), productId: 'unknown' }] })],
    ['pipeline mismatch', (v) => ({ ...v, data: [{ ...run(), pipeline: 'RELIABILITY_V2' }] })],
    ['unknown state', (v) => ({ ...v, data: [{ ...run(), state: 'ACCEPTED' }] })],
    ['unknown stage', (v) => ({ ...v, data: [{ ...run(), currentStage: 'FETCH_PRIVATE_PATH' }] })],
    ['missing stage', (v) => ({ ...v, data: [{ ...run(), currentStage: undefined }] })],
    ['raw outcome', (v) => ({ ...v, data: [{ ...run(), outcomeCode: 'PRIVATE_DIAGNOSTIC' }] })],
    ['missing outcome', (v) => ({ ...v, data: [{ ...run(), outcomeCode: undefined }] })],
    ['numeric version', (v) => ({ ...v, data: [{ ...run(), rowVersion: 42 }] })],
    ['overflow version', (v) => ({ ...v, data: [{ ...run(), rowVersion: '9223372036854775808' }] })],
    ['padded version', (v) => ({ ...v, data: [{ ...run(), rowVersion: '01' }] })],
    ['bad timestamp', (v) => ({ ...v, data: [{ ...run(), requestedAt: '2026-02-30T00:00:00.000000Z' }] })],
    ['null requested time', (v) => ({ ...v, data: [{ ...run(), requestedAt: null }] })],
    ['bad finished time', (v) => ({ ...v, data: [{ ...run(), finishedAt: 'yesterday' }] })],
    ['retry coercion', (v) => ({ ...v, data: [{ ...run(), retryEligibility: 'true' }] })],
    ['private evidence', (v) => ({ ...v, data: [{ ...run(), evidence: '/private/secret' }] })],
    ['missing result', (v) => ({ ...v, data: [{ ...run(), result: undefined }] })],
    ['result on active run', (v) => ({ ...v, data: [{ ...run(), result: run(1, 'SUCCEEDED').result }] })],
    ['succeeded without result', (v) => ({ ...v, data: [{ ...run(1, 'SUCCEEDED'), result: null }] })],
    ['terminal without timestamp', (v) => ({ ...v, data: [{ ...run(1, 'FAILED'), finishedAt: null }] })],
    ['Content synthetic metadata', (v) => ({ ...v, data: [{ ...run(), dataOrigin: 'synthetic' }] })],
  ] satisfies [string, (body: ReturnType<typeof runPage>) => unknown][])('rejects %s', (_, change) => {
    expect(() => parseRuns(change(runPage()))).toThrow(ManagementError);
  });

  it.each([
    ['artifactId', '/private/artifact'], ['nativeFingerprint', 'A'.repeat(64)], ['productContractVersion', 2], ['processorRevision', '1'],
    ['verificationStatus', 'VERIFIED_NOW'], ['verifiedAt', null], ['extraEvidence', 'private'],
  ])('rejects malformed Content result %s', (field, value) => {
    const row = run(1, 'SUCCEEDED');
    expect(() => parseRuns(runPage([{ ...row, result: { ...row.result, [field]: value } as typeof row.result }]))).toThrow();
  });

  it('requires exact Reliability metadata and supported result versions', () => {
    const row = run(1, 'SUCCEEDED', 'application-reliability');
    const { dataOrigin: omitted, ...missing } = row; expect(omitted).toBe('synthetic');
    for (const changed of [missing, { ...row, dataOrigin: 'production' }, { ...row, result: { ...row.result, processorRevision: 1 } }]) {
      expect(() => parseRuns({ ...runPage(), data: [changed] })).toThrow();
    }
  });

  it('bounds pages, preserves opaque cursors, rejects non-advancement and validates visible filters only', () => {
    expect(parseRuns(fullRunPage()).meta.nextCursor).toBe(FIRST_CURSOR);
    expect(parseRuns(runPage(Array.from({ length: 100 }, (_, i) => run(i)), 100), { limit: 100 }).data).toHaveLength(100);
    expect(parseRuns(runPage([run()], 1), { limit: 1 }).data).toHaveLength(1);
    expect(() => parseRuns(fullRunPage(), { cursor: FIRST_CURSOR })).toThrow();
    expect(() => parseRuns(runPage(), { productId: 'application-reliability' })).toThrow();
    expect(() => parseRuns(runPage(), { state: 'FAILED' })).toThrow();
    // Summaries omit selected sources; provenance membership remains a server-side filter.
    expect(parseRuns(runPage(), { sourceId: 'exact-source' }).data).toHaveLength(1);
  });

  it.each([{ limit: 0 }, { limit: 101 }, { limit: 1.5 }, { sourceId: '' }, { sourceId: ' Alpha ' }, { sourceId: 'a'.repeat(129) },
    { sourceId: '../x' }, { state: 'waiting' }, { productId: 'unknown' }, { cursor: '' }, { cursor: 'x'.repeat(470) + '.' + 's'.repeat(43) }])(
    'rejects invalid filters %j before transport', async (query) => {
      const fetch = vi.fn(); vi.stubGlobal('fetch', fetch);
      await expect(fetchRuns(query as RunsQuery, signal())).rejects.toMatchObject({ kind: 'invalid-request' }); expect(fetch).not.toHaveBeenCalled();
    });

  it.each([[400, 'INVALID_ARGUMENT', 'invalid-request'], [403, 'LOCAL_BOUNDARY_REJECTED', 'local-boundary'], [404, 'NOT_FOUND', 'not-found'],
    [429, 'MANAGEMENT_CAPACITY_EXCEEDED', 'capacity'], [500, 'INTERNAL_FAILURE', 'server'], [503, 'MANAGEMENT_UNAVAILABLE', 'unavailable']] as const)(
    'preserves safe HTTP %s metadata independently of run outcomes', async (status, code, kind) => {
      vi.stubGlobal('fetch', vi.fn().mockResolvedValue(jsonResponse(managementFailure(code), status)));
      await expect(fetchRuns({}, signal())).rejects.toMatchObject({ kind, code, correlationId: CORRELATION, retryable: status === 503 });
    });

  it('discards unrecognized/malformed error metadata and never displays remote text', async () => {
    for (const error of [{ ...managementFailure(), code: 'PRIVATE_CODE' }, { ...managementFailure(), correlationId: 'secret' },
      { ...managementFailure(), retryable: 'yes' }, { ...managementFailure(), extra: 'secret' }]) {
      vi.stubGlobal('fetch', vi.fn().mockResolvedValue(jsonResponse(error, 503)));
      await expect(fetchRuns({}, signal())).rejects.toMatchObject({ kind: 'unavailable', code: null, correlationId: null, retryable: null });
    }
  });

  it('handles network, malformed/non-JSON success and abort without leaking bodies', async () => {
    const fetch = vi.fn().mockRejectedValue(new Error('/private/secret')); vi.stubGlobal('fetch', fetch);
    await expect(fetchRuns({}, signal())).rejects.toMatchObject({ kind: 'network' });
    for (const response of [new Response('private traceback'), jsonResponse({ apiVersion: 2 }), new Response('{bad', { headers: { 'Content-Type': 'application/json' } })]) {
      fetch.mockResolvedValue(response); await expect(fetchRuns({}, signal())).rejects.toMatchObject({ kind: 'invalid-response' });
    }
    const controller = new AbortController(), abort = new DOMException('aborted', 'AbortError'); controller.abort(); fetch.mockRejectedValue(abort);
    await expect(fetchRuns({}, controller.signal)).rejects.toBe(abort);
  });
});
