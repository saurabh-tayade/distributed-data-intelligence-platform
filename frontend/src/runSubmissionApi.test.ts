import { describe, expect, it, vi } from 'vitest';
import { parseReliabilityRequest, sendRunSubmission, validateRunSubmission } from './managementApi';
import { CORRELATION, managementFailure } from './test/managementFixtures';
import { BATCH_ID, request, submissionCommand, submissionResponse, submittedRun } from './test/submissionFixtures';
import { recordEnvelope } from './test/runInspectionFixtures';
import { jsonResponse } from './test/fixtures';

describe('Reliability run creation DTO and transport', () => {
  it('sends exactly the reviewed body and required headers, without If-Match', async () => {
    const fetch = vi.fn().mockResolvedValue(submissionResponse()); vi.stubGlobal('fetch', fetch);
    const command = submissionCommand(), abort = new AbortController(); const response = await sendRunSubmission(command, abort.signal);
    expect(fetch).toHaveBeenCalledExactlyOnceWith('/api/management/v1/runs', {
      method: 'POST', credentials: 'omit', cache: 'no-store', signal: abort.signal, body: command.body,
      headers: { Accept: 'application/json', 'Content-Type': 'application/json', 'Idempotency-Key': command.key, 'X-DDIP-Local-Request': '1' },
    });
    expect(response).toMatchObject({ data: submittedRun(), etag: `"${submittedRun().rowVersion}"`, location: `/api/management/v1/runs/${submittedRun().runId}` });
  });
  it('keeps large decimal versions exact and permits a replay with terminal persisted state', async () => {
    const record = { ...submittedRun('SUCCEEDED'), rowVersion: '9007199254740994' };
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(submissionResponse(record)));
    const result = await sendRunSubmission(submissionCommand(), new AbortController().signal);
    expect(result.data.rowVersion).toBe('9007199254740994'); expect(result.etag).toBe('"9007199254740994"'); expect(result.data.state).toBe('SUCCEEDED');
  });
  it.each([
    { pipeline: 'CONTENT_RSS_V1' }, { pipeline: 'other' }, { executionProfileId: 'not-a-uuid' }, { sources: [] }, { batchArtifactIds: [] },
    { sources: [{ sourceId: ' Alpha ', revision: 2 }] }, { sources: [{ sourceId: 'alpha', revision: '2' }] },
    { sources: [{ sourceId: 'alpha', revision: 0 }] }, { sources: [{ sourceId: 'alpha', revision: 1001 }] },
    { serviceReferenceArtifactId: BATCH_ID }, { batchArtifactIds: [BATCH_ID, BATCH_ID] }, { sources: [request().sources[0], request().sources[0]] },
    { force: true }, { IfMatch: '"0"' },
  ])('rejects invalid exact submission shape %j', (change) => {
    expect(() => parseReliabilityRequest({ ...request(), ...change })).toThrow();
  });
  it('enforces both selection count bounds and exact command fields before any POST', async () => {
    const max = request(); max.sources = Array.from({ length: 32 }, (_, i) => ({ sourceId: `source-${i}`, revision: 1000 }));
    max.batchArtifactIds = Array.from({ length: 32 }, (_, i) => `90000000-0000-4000-8000-${i.toString(16).padStart(12, '0')}`);
    expect(parseReliabilityRequest(max)).toEqual(max);
    expect(() => parseReliabilityRequest({ ...max, sources: [...max.sources, { sourceId: 'overflow', revision: 1 }] })).toThrow();
    expect(() => parseReliabilityRequest({ ...max, batchArtifactIds: [...max.batchArtifactIds, '90000000-0000-4000-8000-000000000099'] })).toThrow();
    expect(() => validateRunSubmission({ ...submissionCommand(), ifMatch: '"0"' } as never)).toThrow();
    const fetch = vi.fn(); vi.stubGlobal('fetch', fetch);
    await expect(sendRunSubmission({ ...submissionCommand(), key: 'unsafe key' }, new AbortController().signal)).rejects.toThrow(); expect(fetch).not.toHaveBeenCalled();
  });
  it.each([
    'https://evil.example/api/management/v1/runs/RUN', '//evil.example/api/management/v1/runs/RUN',
    '/api/management/v1/runs/RUN?token=secret', '/api/management/v1/runs/RUN#other', '/api/management/v1/runs/80000000-0000-4000-8000-000000000001',
    'http://name:password@localhost:3000/api/management/v1/runs/RUN',
  ])('rejects unsafe or mismatched Location without treating admission as failed: %s', async (location) => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(submissionResponse(submittedRun(), location.replace('RUN', submittedRun().runId))));
    await expect(sendRunSubmission(submissionCommand(), new AbortController().signal)).rejects.toMatchObject({ status: 202, ambiguous: true });
  });
  it('accepts an exact same-origin absolute Location and stores only the canonical relative route', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(submissionResponse(submittedRun(), `${window.location.origin}/api/management/v1/runs/${submittedRun().runId}`)));
    expect((await sendRunSubmission(submissionCommand(), new AbortController().signal)).location).toBe(`/api/management/v1/runs/${submittedRun().runId}`);
  });
  it.each(['etag', 'location', 'envelope', 'product', 'sources', 'inputs', 'status'])('keeps a malformed successful %s response ambiguous', async (field) => {
    let data = submittedRun(), status = 202;
    if (field === 'product') data = { ...data, productId: 'content-intelligence', pipeline: 'CONTENT_RSS_V1' };
    if (field === 'sources') data = { ...data, sourceRevisions: [{ sourceId: 'wrong-source', revision: 1 }] };
    if (field === 'inputs') data = { ...data, inputArtifactIds: [] };
    if (field === 'status') status = 200;
    const response = new Response(JSON.stringify(field === 'envelope' ? { apiVersion: 2, data, meta: {} } : recordEnvelope(data)), { status,
      headers: { 'Content-Type': 'application/json', ...(field === 'etag' ? {} : { ETag: `"${data.rowVersion}"` }), ...(field === 'location' ? {} : { Location: `/api/management/v1/runs/${data.runId}` }) } });
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(response));
    await expect(sendRunSubmission(submissionCommand(), new AbortController().signal)).rejects.toMatchObject({ ambiguous: true });
  });
  it.each([
    [400, 'INVALID_ARGUMENT', false], [403, 'LOCAL_BOUNDARY_REJECTED', false], [409, 'SOURCE_DISABLED', false], [409, 'STATE_CONFLICT', false],
    [409, 'IDEMPOTENCY_CONFLICT', false], [429, 'MANAGEMENT_CAPACITY_EXCEEDED', false], [503, 'COORDINATOR_RECOVERY_REQUIRED', true], [503, 'MANAGEMENT_UNAVAILABLE', true], [500, 'INTERNAL_FAILURE', true],
  ])('classifies %s %s without private diagnostics', async (status, code, ambiguous) => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(jsonResponse(managementFailure(String(code)), Number(status))));
    await expect(sendRunSubmission(submissionCommand(), new AbortController().signal)).rejects.toMatchObject({ status, code, ambiguous, correlationId: CORRELATION });
  });
  it('treats lost, aborted and unrecognized responses as unknown without retrying', async () => {
    for (const response of [Promise.reject(new Error('private transport detail')), Promise.resolve(jsonResponse(managementFailure('ARBITRARY_PRIVATE_CODE'), 409)), Promise.resolve(new Response('private html', { status: 502 }))]) {
      const fetch = vi.fn().mockReturnValue(response); vi.stubGlobal('fetch', fetch);
      await expect(sendRunSubmission(submissionCommand(), new AbortController().signal)).rejects.toMatchObject({ ambiguous: true, code: null }); expect(fetch).toHaveBeenCalledTimes(1);
    }
  });
});
