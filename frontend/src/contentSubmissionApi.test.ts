import { describe, expect, it, vi } from 'vitest';
import { parseContentRequest, sendRunSubmission, validateRunSubmission } from './managementApi';
import { contentCommand, contentReceipt, contentRequest, contentRun, PUBLISHER_REFERENCE } from './test/contentSubmissionFixtures';
import { jsonResponse } from './test/fixtures';
import { CORRELATION, managementFailure } from './test/managementFixtures';
import { recordEnvelope } from './test/runInspectionFixtures';

describe('bounded Content request and admission contract', () => {
  it('supports exactly the narrow request and sends the original body bytes and key', async () => {
    const command = { ...contentCommand(), body: JSON.stringify(contentRequest(), null, 2) }, abort = new AbortController();
    expect(parseContentRequest(contentRequest())).toEqual(contentRequest()); expect(validateRunSubmission(command)).toEqual(contentRequest());
    const fetch = vi.fn().mockResolvedValue(contentReceipt()); vi.stubGlobal('fetch', fetch);
    const receipt = await sendRunSubmission(command, abort.signal);
    expect(fetch).toHaveBeenCalledExactlyOnceWith('/api/management/v1/runs', { method: 'POST', body: command.body, credentials: 'omit', cache: 'no-store', signal: abort.signal,
      headers: { Accept: 'application/json', 'Content-Type': 'application/json', 'Idempotency-Key': command.key, 'X-DDIP-Local-Request': '1' } });
    expect(receipt.data).toEqual(contentRun()); expect(receipt.location).toBe(`/api/management/v1/runs/${contentRun().runId}`);
  });
  it.each([
    { pipeline: 'RELIABILITY_V2' }, { executionProfileId: 'bad' }, { publisherReferenceArtifactId: 'bad' }, { fetchSources: [] },
    { fetchSources: [{ sourceId: 'alpha', revision: 1 }, { sourceId: 'beta', revision: 1 }] }, { fetchSources: [{ sourceId: ' Alpha ', revision: 1 }] },
    { fetchSources: [{ sourceId: 'alpha', revision: '1' }] }, { fetchSources: [{ sourceId: 'alpha', revision: 0 }] }, { fetchSources: [{ sourceId: 'alpha', revision: 1001 }] },
    { fetchSources: [{ sourceId: 'alpha', revision: 1, force: true }] }, { retainedSilverArtifactIds: [PUBLISHER_REFERENCE] }, { retainedSilverArtifactIds: null }, { sources: [] }, { force: true },
  ])('rejects unsupported request %j before any mutation', async (change) => {
    const command = { ...contentCommand(), body: JSON.stringify({ ...contentRequest(), ...change }) }, fetch = vi.fn(); vi.stubGlobal('fetch', fetch);
    expect(() => parseContentRequest(JSON.parse(command.body))).toThrow(); await expect(sendRunSubmission(command, new AbortController().signal)).rejects.toThrow(); expect(fetch).not.toHaveBeenCalled();
  });
  it.each(['pipeline', 'product', 'source', 'revision', 'extra-source', 'missing-reference', 'other-reference', 'extra-input', 'duplicate-input', 'etag', 'location', 'envelope', 'content-type'])('treats inconsistent 202 %s as ambiguous', async (field) => {
    const record = contentRun();
    if (field === 'pipeline') Object.assign(record, { pipeline: 'RELIABILITY_V2', productId: 'application-reliability', dataOrigin: 'synthetic' });
    if (field === 'product') Object.assign(record, { productId: 'application-reliability' });
    if (field === 'source') record.sourceRevisions = [{ sourceId: 'other', revision: 2 }];
    if (field === 'revision') record.sourceRevisions = [{ sourceId: 'alpha', revision: 3 }];
    if (field === 'extra-source') record.sourceRevisions.push({ sourceId: 'beta', revision: 2 });
    if (field === 'missing-reference') record.inputArtifactIds = [];
    if (field === 'other-reference') record.inputArtifactIds = ['a1000000-0000-4000-8000-000000000002'];
    if (field === 'extra-input') record.inputArtifactIds.push('a1000000-0000-4000-8000-000000000002');
    if (field === 'duplicate-input') record.inputArtifactIds.push(PUBLISHER_REFERENCE);
    const headers = { 'Content-Type': field === 'content-type' ? 'text/plain' : 'application/json',
      ...(field === 'etag' ? {} : { ETag: `"${record.rowVersion}"` }), ...(field === 'location' ? {} : { Location: `/api/management/v1/runs/${record.runId}` }) };
    const fetch = vi.fn().mockResolvedValue(new Response(JSON.stringify(field === 'envelope' ? { apiVersion: 2 } : recordEnvelope(record)), { status: 202, headers })); vi.stubGlobal('fetch', fetch);
    await expect(sendRunSubmission(contentCommand(), new AbortController().signal)).rejects.toMatchObject({ status: 202, ambiguous: true }); expect(fetch).toHaveBeenCalledTimes(1);
  });
  it('accepts exact replays returning an advanced persisted state without rounding versions', async () => {
    const record = { ...contentRun('SUCCEEDED'), rowVersion: '9007199254740994' }; vi.stubGlobal('fetch', vi.fn().mockResolvedValue(contentReceipt(record)));
    const result = await sendRunSubmission(contentCommand(), new AbortController().signal); expect(result.data).toEqual(record); expect(result.etag).toBe('"9007199254740994"');
  });
  it.each([[400, 'INVALID_ARGUMENT', false], [403, 'LOCAL_BOUNDARY_REJECTED', false], [404, 'NOT_FOUND', false], [409, 'SOURCE_DISABLED', false],
    [409, 'STATE_CONFLICT', false], [409, 'IDEMPOTENCY_CONFLICT', false], [429, 'MANAGEMENT_CAPACITY_EXCEEDED', false], [500, 'INTERNAL_FAILURE', true],
    [503, 'MANAGEMENT_UNAVAILABLE', true], [503, 'COORDINATOR_RECOVERY_REQUIRED', true]])('retains HTTP %s %s without retrying', async (status, code, ambiguous) => {
    const fetch = vi.fn().mockResolvedValue(jsonResponse(managementFailure(String(code)), Number(status))); vi.stubGlobal('fetch', fetch);
    await expect(sendRunSubmission(contentCommand(), new AbortController().signal)).rejects.toMatchObject({ status, code, ambiguous, correlationId: CORRELATION }); expect(fetch).toHaveBeenCalledTimes(1);
  });
  it.each(['lost', 'abort', 'unknown-error', 'unexpected-status'])('keeps %s outcomes uncertain without retry', async (fault) => {
    const fetch = vi.fn().mockImplementation(() => {
      if (fault === 'lost') throw new Error('private transport'); if (fault === 'abort') throw new DOMException('Aborted', 'AbortError');
      return Promise.resolve(jsonResponse(managementFailure(fault === 'unknown-error' ? 'UNKNOWN' : 'INVALID_ARGUMENT'), fault === 'unexpected-status' ? 200 : 409));
    }); vi.stubGlobal('fetch', fetch);
    await expect(sendRunSubmission(contentCommand(), new AbortController().signal)).rejects.toMatchObject({ ambiguous: true, code: null }); expect(fetch).toHaveBeenCalledTimes(1);
  });
});
