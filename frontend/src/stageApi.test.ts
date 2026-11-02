import { describe, expect, it, vi } from 'vitest';
import { fetchStages, ManagementError, parseStages, RUN_STAGES, STAGE_COUNT_KEYS, STAGE_STATES } from './managementApi';
import { jsonResponse } from './test/fixtures';
import { CORRELATION, FIRST_CURSOR, managementFailure } from './test/managementFixtures';
import { ATTEMPT_ID, attempt, historyPage, runDetail, stageRecord, stagesResponse } from './test/runInspectionFixtures';

const runId = runDetail().runId;
describe('G2a stage wire contract', () => {
  it.each(STAGE_STATES)('preserves stage state %s, exact counts and recorded timestamps', (state) => {
    const row = { ...stageRecord(), state, startedAt: null, outcomeCode: null };
    if (state === 'INTENT' || state === 'RUNNING' || state === 'UNKNOWN') row.finishedAt = null;
    const page = historyPage([row]); expect(parseStages(page, ATTEMPT_ID)).toEqual(page);
    expect(parseStages(page, ATTEMPT_ID).data[0]?.counts?.rows).toBe('9007199254740993');
  });
  it.each(RUN_STAGES)('accepts persisted stage %s', (stage) => {
    const page = historyPage([stageRecord(0, stage)]); expect(parseStages(page, ATTEMPT_ID)).toEqual(page);
  });
  it.each([null, {}])('preserves unavailable or empty counts %j', (counts) => {
    const page = historyPage([{ ...stageRecord(), counts }]); expect(parseStages(page, ATTEMPT_ID)).toEqual(page);
  });
  it('accepts all reviewed counters as exact strings and separates VERIFIED from outcome', () => {
    const counts = Object.fromEntries(STAGE_COUNT_KEYS.map((key) => [key, '9223372036854775807']));
    const row = { ...stageRecord(), outcomeCode: 'PUBLICATION_INVALID' as const, counts };
    expect(parseStages(historyPage([row]), ATTEMPT_ID).data).toEqual([row]);
    expect(parseStages(historyPage([]), ATTEMPT_ID).data).toEqual([]);
  });
  it('keeps binary identity order despite reverse timestamps and multiple item ordinals', () => {
    const rows = [stageRecord(0, 'CHECK_INPUTS'), stageRecord(0, 'PUBLISH_CONTENT'), stageRecord(1, 'PUBLISH_CONTENT')];
    rows[0]!.startedAt = '2026-11-02T12:00:00.000000Z'; rows[2]!.startedAt = null;
    expect(parseStages(historyPage(rows), ATTEMPT_ID).data).toEqual(rows);
    expect(() => parseStages(historyPage([...rows].reverse()), ATTEMPT_ID)).toThrow(ManagementError);
    expect(() => parseStages(historyPage([rows[0], rows[0]]), ATTEMPT_ID)).toThrow(ManagementError);
    expect(() => parseStages(historyPage([rows[0], { ...rows[1], checkpointId: rows[0]!.checkpointId }]), ATTEMPT_ID)).toThrow(ManagementError);
  });
  it.each([
    ['foreign attempt', { attemptId: attempt(2).attemptId }], ['invalid attempt', { attemptId: 'private' }],
    ['unknown stage', { stage: 'PRIVATE' }], ['run state', { state: 'SUCCEEDED' }], ['unknown state', { state: 'PRIVATE' }],
    ['private outcome', { outcomeCode: 'PRIVATE_SECRET' }], ['numeric outcome', { outcomeCode: 1 }],
    ['private field', { command: '/private/command' }], ['uppercase checkpoint', { checkpointId: '80000000-0000-4000-8000-00000000000A' }],
    ['missing checkpoint', { checkpointId: null }], ['negative ordinal', { itemOrdinal: -1 }], ['large ordinal', { itemOrdinal: 512 }],
    ['fractional ordinal', { itemOrdinal: 1.5 }], ['string ordinal', { itemOrdinal: '0' }],
    ['invalid time', { startedAt: '2026-02-30T00:00:00.000000Z' }], ['missing precision', { finishedAt: '2026-11-01T12:00:00Z' }],
    ['terminal without finish', { finishedAt: null }], ['numeric count', { counts: { rows: 1 } }],
    ['negative count', { counts: { rows: '-1' } }], ['count overflow', { counts: { rows: '9223372036854775808' } }],
    ['leading zero', { counts: { rows: '01' } }], ['private count', { counts: { privateEvidence: '123' } }], ['array counts', { counts: [] }],
  ])('rejects malformed stage: %s', (_, patch) => {
    expect(() => parseStages(historyPage([{ ...stageRecord(), ...patch }]), ATTEMPT_ID)).toThrow(ManagementError);
  });
  it.each([
    ['wrong version', { apiVersion: 2 }], ['missing metadata', { meta: null }], ['non-array', { data: {} }],
    ['private envelope', { proof: 'PRIVATE' }], ['wrong limit', { meta: { ...historyPage([]).meta, limit: 100 } }],
    ['invalid correlation', { meta: { ...historyPage([]).meta, correlationId: 'PRIVATE' } }],
    ['invented total', { meta: { ...historyPage([]).meta, total: 1 } }],
    ['cursor mismatch', { meta: { ...historyPage([]).meta, nextCursor: FIRST_CURSOR } }],
    ['invalid cursor', { meta: { ...historyPage([]).meta, hasMore: true, nextCursor: 'PRIVATE' } }],
    ['short page with more', { meta: { ...historyPage([]).meta, hasMore: true, nextCursor: FIRST_CURSOR } }],
    ['unbounded rows', { data: Array.from({ length: 21 }, (_, i) => stageRecord(i)) }],
  ])('rejects malformed page: %s', (_, patch) => {
    expect(() => parseStages({ ...historyPage([stageRecord()]), ...patch }, ATTEMPT_ID)).toThrow(ManagementError);
  });
  it('rejects omitted DTO fields and nonadvancing cursors', () => {
    const { counts: _counts, ...missing } = stageRecord();
    expect(_counts).not.toBeNull(); expect(() => parseStages(historyPage([missing]), ATTEMPT_ID)).toThrow(ManagementError);
    const page = historyPage(Array.from({ length: 20 }, (_, i) => stageRecord(i)), 20, FIRST_CURSOR);
    expect(() => parseStages(page, ATTEMPT_ID, { cursor: FIRST_CURSOR })).toThrow(ManagementError);
  });
});

describe('stage transport safety', () => {
  it('fetches only the selected run/attempt with bounded GET, opaque cursor and no mutation headers', async () => {
    const fetch = vi.fn().mockResolvedValue(stagesResponse([], 100)); vi.stubGlobal('fetch', fetch);
    const signal = new AbortController().signal;
    await fetchStages(runId, ATTEMPT_ID, { limit: 100, cursor: FIRST_CURSOR }, signal);
    expect(fetch).toHaveBeenCalledWith(`/api/management/v1/runs/${runId}/attempts/${ATTEMPT_ID}/stages?limit=100&cursor=${encodeURIComponent(FIRST_CURSOR)}`,
      { method: 'GET', signal, cache: 'no-store', credentials: 'omit', headers: { Accept: 'application/json' } });
  });
  it.each(['bad', '../private', '60000000-0000-4000-8000-00000000000A'])('rejects invalid run/attempt identity %s before fetch', async (id) => {
    const fetch = vi.fn(); vi.stubGlobal('fetch', fetch); const signal = new AbortController().signal;
    await expect(fetchStages(id, ATTEMPT_ID, {}, signal)).rejects.toMatchObject({ kind: 'invalid-request' });
    await expect(fetchStages(runId, id, {}, signal)).rejects.toMatchObject({ kind: 'invalid-request' }); expect(fetch).not.toHaveBeenCalled();
  });
  it.each([{ limit: 0 }, { limit: 101 }, { limit: 1.5 }, { cursor: '' }, { cursor: 'PRIVATE' }])('rejects invalid query %j', async (query) => {
    const fetch = vi.fn(); vi.stubGlobal('fetch', fetch);
    await expect(fetchStages(runId, ATTEMPT_ID, query, new AbortController().signal)).rejects.toMatchObject({ kind: 'invalid-request' }); expect(fetch).not.toHaveBeenCalled();
  });
  it.each([[400, 'INVALID_ARGUMENT', 'invalid-request'], [403, 'LOCAL_BOUNDARY_REJECTED', 'local-boundary'], [404, 'NOT_FOUND', 'not-found'],
    [429, 'MANAGEMENT_CAPACITY_EXCEEDED', 'capacity'], [500, 'INTERNAL_FAILURE', 'server'], [503, 'MANAGEMENT_UNAVAILABLE', 'unavailable']] as const)('uses safe HTTP %s errors', async (status, code, kind) => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(jsonResponse(managementFailure(code), status)));
    await expect(fetchStages(runId, ATTEMPT_ID, {}, new AbortController().signal)).rejects.toMatchObject({ kind, code, correlationId: CORRELATION });
  });
  it('classifies malformed, foreign-owner, proxy and network responses without exposing raw diagnostics', async () => {
    for (const response of [jsonResponse({ private: 'PRIVATE' }), stagesResponse([stageRecord(0, 'CHECK_INPUTS', attempt(2).attemptId)]), new Response('PRIVATE', { headers: { 'Content-Type': 'text/html' } })]) {
      vi.stubGlobal('fetch', vi.fn().mockResolvedValue(response));
      await expect(fetchStages(runId, ATTEMPT_ID, {}, new AbortController().signal)).rejects.toMatchObject({ kind: 'invalid-response', code: null });
    }
    vi.stubGlobal('fetch', vi.fn().mockRejectedValue(new Error('PRIVATE')));
    await expect(fetchStages(runId, ATTEMPT_ID, {}, new AbortController().signal)).rejects.toMatchObject({ kind: 'network' });
  });
  it('preserves transport aborts for request invalidation', async () => {
    const controller = new AbortController(), error = new DOMException('aborted', 'AbortError'); controller.abort();
    vi.stubGlobal('fetch', vi.fn().mockRejectedValue(error)); await expect(fetchStages(runId, ATTEMPT_ID, {}, controller.signal)).rejects.toBe(error);
  });
});
