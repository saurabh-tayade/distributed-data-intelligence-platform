import { describe, expect, it, vi } from 'vitest';
import { ATTEMPT_STATES, AUDIT_ACTIONS, AUDIT_STATES, CONTROL_STATES, CONTROL_TYPES, RUN_COUNT_KEYS, fetchAttempts, fetchControl, fetchEvents, fetchRunDetail,
  ManagementError, parseAttempts, parseControl, parseEvents, parseRunDetail } from './managementApi';
import { jsonResponse } from './test/fixtures';
import { CORRELATION, FIRST_CURSOR, managementFailure } from './test/managementFixtures';
import { run } from './test/runFixtures';
import { ATTEMPT_ID, CONTROL_ID, attempt, auditEvent, controlReceipt, controlResponse, detailResponse, historyPage, latestControl, recordEnvelope, runDetail } from './test/runInspectionFixtures';

const id = run().runId, tag = '"9007199254740993"', signal = () => new AbortController().signal;
const parse = (value: unknown) => parseRunDetail(value, id, tag);
describe('run inspection wire contracts', () => {
  it.each(['content-intelligence', 'application-reliability'] as const)('retains exact %s detail, captured references, counts and run ETag', async (product) => {
    const record = runDetail(1, 'SUCCEEDED', product); record.counts = { rows: '9007199254740993', bytes: '9223372036854775807' };
    record.attempts = { count: '20', currentAttemptId: ATTEMPT_ID }; record.latestControl = latestControl();
    record.inputArtifactIds.push(record.inputArtifactIds[0]!); // The same artifact may appear in different input roles.
    expect(parse(recordEnvelope(record))).toEqual({ ...recordEnvelope(record), etag: tag });
    vi.stubGlobal('fetch', vi.fn().mockImplementation(async () => detailResponse(record)));
    const value = await fetchRunDetail(id, signal()); expect(value.data.counts?.rows).toBe('9007199254740993'); expect(value.etag).toBe(tag);
    expect(value.data.sourceRevisions[0]?.revision).toBe(1); expect(value.data.result?.verificationStatus).toBe('VERIFIED_AT_EXECUTION');
  });

  it('supports nullable/empty values and every safe count key without numerical conversion', () => {
    const record = runDetail(); expect(parse(recordEnvelope(record)).data).toEqual(record);
    record.counts = {}; record.sourceRevisions = []; record.inputArtifactIds = [];
    expect(parse(recordEnvelope(record)).data.counts).toEqual({});
    for (const key of RUN_COUNT_KEYS) {
      record.counts = { [key]: '9007199254740993' }; expect(parse(recordEnvelope(record)).data.counts?.[key]).toBe('9007199254740993');
    }
  });

  it.each([
    ['version', (v) => ({ ...v, apiVersion: 2 })], ['extra envelope', (v) => ({ ...v, extra: true })],
    ['bad meta', (v) => ({ ...v, meta: { correlationId: 'secret' } })], ['missing data', (v) => ({ ...v, data: null })],
    ['wrong run', (v) => ({ ...v, data: { ...v.data, runId: run(2).runId } })],
    ['private field', (v) => ({ ...v, data: { ...v.data, privateCommand: '/private' } })],
    ['missing counts', (v) => ({ ...v, data: { ...v.data, counts: undefined } })],
    ['unknown count', (v) => ({ ...v, data: { ...v.data, counts: { secret: '1' } } })],
    ['numeric count', (v) => ({ ...v, data: { ...v.data, counts: { rows: 42 } } })],
    ['overflow count', (v) => ({ ...v, data: { ...v.data, counts: { rows: '9223372036854775808' } } })],
    ['missing attempts', (v) => ({ ...v, data: { ...v.data, attempts: null } })],
    ['numeric attempt count', (v) => ({ ...v, data: { ...v.data, attempts: { count: 1, currentAttemptId: null } } })],
    ['too many attempts', (v) => ({ ...v, data: { ...v.data, attempts: { count: '21', currentAttemptId: null } } })],
    ['bad attempt ID', (v) => ({ ...v, data: { ...v.data, attempts: { count: '1', currentAttemptId: 'bad' } } })],
    ['missing sources', (v) => ({ ...v, data: { ...v.data, sourceRevisions: undefined } })],
    ['duplicate sources', (v) => ({ ...v, data: { ...v.data, sourceRevisions: [{ sourceId: 'alpha', revision: 1 }, { sourceId: 'alpha', revision: 2 }] } })],
    ['out of order sources', (v) => ({ ...v, data: { ...v.data, sourceRevisions: [{ sourceId: 'beta', revision: 1 }, { sourceId: 'alpha', revision: 2 }] } })],
    ['source revision overflow', (v) => ({ ...v, data: { ...v.data, sourceRevisions: [{ sourceId: 'alpha', revision: 1001 }] } })],
    ['raw source configuration', (v) => ({ ...v, data: { ...v.data, sourceRevisions: [{ sourceId: 'alpha', revision: 1, config: {} }] } })],
    ['input overflow', (v) => ({ ...v, data: { ...v.data, inputArtifactIds: Array.from({ length: 130 }, () => ATTEMPT_ID) } })],
    ['private artifact path', (v) => ({ ...v, data: { ...v.data, inputArtifactIds: ['/private'] } })],
    ['extra control field', (v) => ({ ...v, data: { ...v.data, latestControl: { ...latestControl(), runVersion: '1' } } })],
    ['unknown control type', (v) => ({ ...v, data: { ...v.data, latestControl: { ...latestControl(), type: 'RESTART' } } })],
  ] satisfies [string, (value: ReturnType<typeof recordEnvelope<ReturnType<typeof runDetail>>>) => unknown][])('rejects detail %s', (_, change) => {
    expect(() => parse(change(recordEnvelope(runDetail())))).toThrow(ManagementError);
  });

  it('enforces exact source/input collection bounds', () => {
    for (const [product, maximum] of [['content-intelligence', 8], ['application-reliability', 32]] as const) {
      const record = runDetail(1, 'QUEUED', product);
      record.sourceRevisions = Array.from({ length: maximum }, (_, i) => ({ sourceId: 'source-' + String(i).padStart(2, '0'), revision: 1000 }));
      record.inputArtifactIds = Array.from({ length: 129 }, () => ATTEMPT_ID); expect(parse(recordEnvelope(record)).data.sourceRevisions).toHaveLength(maximum);
      record.sourceRevisions.push({ sourceId: 'source-zz', revision: 1 }); expect(() => parse(recordEnvelope(record))).toThrow();
    }
  });

  it.each([null, '', 'W/"9007199254740993"', '"1"'])('rejects run ETag %s', (etag) => {
    expect(() => parseRunDetail(recordEnvelope(runDetail()), id, etag)).toThrow();
  });

  it('validates attempt domains, ordering, historical kind and exact stage counts', () => {
    const rows = [attempt(1), { ...attempt(2, 'RECONCILE'), stageCount: '9007199254740993' }];
    expect(parseAttempts(historyPage(rows)).data).toEqual(rows);
    for (const state of ATTEMPT_STATES) expect(parseAttempts(historyPage([{ ...attempt(), state }])).data[0]?.state).toBe(state);
    expect(parseAttempts(historyPage([{ ...attempt(), state: 'RUNNING', finishedAt: null, outcomeCode: null }])).data[0]?.finishedAt).toBeNull();
    expect(parseAttempts(historyPage([])).data).toEqual([]);
  });

  it.each([
    ['descending', [attempt(2), attempt(1)]], ['duplicate number', [attempt(1), attempt(1)]],
    ['duplicate ID', [attempt(1), { ...attempt(2), attemptId: ATTEMPT_ID }]], ['overflow', [attempt(21)]],
    ['unknown kind', [{ ...attempt(), kind: 'RETRY' }]], ['run state', [{ ...attempt(), state: 'PAUSED' }]],
    ['numeric stage count', [{ ...attempt(), stageCount: 1 }]], ['private outcome', [{ ...attempt(), outcomeCode: 'PRIVATE_DIAGNOSTIC' }]],
    ['missing timestamp', [{ ...attempt(), startedAt: null }]], ['terminal no timestamp', [{ ...attempt(), finishedAt: null }]],
    ['private command', [{ ...attempt(), command: '/private' }]],
  ])('rejects attempt %s', (_, data) => { expect(() => parseAttempts({ ...historyPage([]), data })).toThrow(); });

  it('orders event sequences beyond JS-safe integers using exact decimals, not timestamps or lexicographic sorting', () => {
    const rows = ['2', '10', '9007199254740993', '9007199254740994', '9223372036854775807'].map((seq) => auditEvent(seq));
    expect(parseEvents(historyPage(rows)).data).toEqual(rows);
    const nullable = { ...auditEvent(), beforeState: null, afterState: null, code: null, beforeVersion: null, afterVersion: null, context: { attemptId: null, controlId: null } };
    expect(parseEvents(historyPage([nullable])).data[0]).toEqual(nullable);
    for (const action of AUDIT_ACTIONS) expect(parseEvents(historyPage([{ ...auditEvent(), action }])).data[0]?.action).toBe(action);
    for (const state of AUDIT_STATES) expect(parseEvents(historyPage([{ ...auditEvent(), afterState: state }])).data[0]?.afterState).toBe(state);
  });

  it.each([
    ['descending', [auditEvent('10'), auditEvent('2')]], ['duplicate', [auditEvent(), auditEvent()]],
    ['numeric sequence', [{ ...auditEvent(), eventSeq: 42 }]], ['zero sequence', [auditEvent('0')]], ['overflow', [auditEvent('9223372036854775808')]],
    ['unknown action', [{ ...auditEvent(), action: 'PRIVATE_ACTION' }]], ['unknown audit state', [{ ...auditEvent(), beforeState: 'SUCCESSFUL' }]],
    ['numeric version', [{ ...auditEvent(), afterVersion: 42 }]], ['bad context ID', [{ ...auditEvent(), context: { attemptId: '/private', controlId: null } }]],
    ['missing context', [{ ...auditEvent(), context: undefined }]], ['extra context', [{ ...auditEvent(), context: { ...auditEvent().context, processId: 'private' } }]],
    ['invented stage field', [{ ...auditEvent(), stage: 'SILVER' }]],
  ])('rejects event %s', (_, data) => { expect(() => parseEvents({ ...historyPage([]), data })).toThrow(); });

  it.each(CONTROL_STATES)('keeps control state %s and control/run ETags distinct', async (state) => {
    const value = controlReceipt(state); const parsed = parseControl(recordEnvelope(value), id, CONTROL_ID, '"12"');
    expect(parsed).toEqual({ ...recordEnvelope(value), etag: '"12"' }); expect(parsed.etag).not.toBe(tag);
    vi.stubGlobal('fetch', vi.fn().mockImplementation(async () => controlResponse(value)));
    expect((await fetchControl(id, CONTROL_ID, signal())).data.runVersion).toBe('9007199254740994');
    for (const type of CONTROL_TYPES) expect(parseControl(recordEnvelope({ ...value, type }), id, CONTROL_ID, '"12"').data.type).toBe(type);
  });

  it('rejects mismatched control identity, versions, tags and malformed summaries', () => {
    const valid = controlReceipt('APPLIED');
    for (const change of [{ runId: run(2).runId }, { controlId: ATTEMPT_ID }, { runVersion: 2 }, { rowVersion: 2 }, { reason: 'PRIVATE' },
      { state: 'SUCCEEDED' }, { type: 'RESTART' }, { appliedAt: null }, { reason: null }, { secret: '/private' }]) {
      expect(() => parseControl(recordEnvelope({ ...valid, ...change }), id, CONTROL_ID, '"12"')).toThrow();
    }
    for (const etag of [null, tag, 'W/"12"']) expect(() => parseControl(recordEnvelope(valid), id, CONTROL_ID, etag)).toThrow();
  });

  it('uses exact bounded child GET routes and validates query/ID boundaries before transport', async () => {
    const fetch = vi.fn().mockImplementation(async (path: string) => jsonResponse(historyPage([], path.includes('limit=100') ? 100 : 20)));
    vi.stubGlobal('fetch', fetch); await fetchAttempts(id, { limit: 100, cursor: FIRST_CURSOR }, signal()); await fetchEvents(id, {}, signal());
    expect(fetch.mock.calls.map((call) => call[0])).toEqual([`/api/management/v1/runs/${id}/attempts?limit=100&cursor=${FIRST_CURSOR}`, `/api/management/v1/runs/${id}/events?limit=20`]);
    for (const limit of [0, 101, 1.5]) await expect(fetchEvents(id, { limit }, signal())).rejects.toMatchObject({ kind: 'invalid-request' });
    await expect(fetchAttempts('invalid', {}, signal())).rejects.toMatchObject({ kind: 'invalid-request' });
    await expect(fetchControl(id, 'invalid', signal())).rejects.toMatchObject({ kind: 'invalid-request' }); expect(fetch).toHaveBeenCalledTimes(2);
  });

  it.each([[403, 'LOCAL_BOUNDARY_REJECTED'], [404, 'NOT_FOUND'], [503, 'MANAGEMENT_UNAVAILABLE'], [400, 'INVALID_ARGUMENT']] as const)('preserves safe HTTP %s metadata for all inspection reads', async (status, code) => {
    vi.stubGlobal('fetch', vi.fn().mockImplementation(async () => jsonResponse(managementFailure(code), status)));
    for (const read of [() => fetchRunDetail(id, signal()), () => fetchAttempts(id, {}, signal()), () => fetchEvents(id, {}, signal()), () => fetchControl(id, CONTROL_ID, signal())]) {
      await expect(read()).rejects.toMatchObject({ code, correlationId: CORRELATION, retryable: status === 503 });
    }
  });
});
