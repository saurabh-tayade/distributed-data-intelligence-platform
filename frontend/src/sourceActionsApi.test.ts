import { describe, expect, it, vi } from 'vitest';
import { fetchSourceCapabilities, isSourceDisplayName, parseSourceCapabilities, sendSourceCommand, validateSourceCommand } from './managementApi';
import type { SourceCommand } from './managementApi';
import { jsonResponse } from './test/fixtures';
import { CORRELATION, managementFailure, sourceEnvelope, sourceRecord, sourceResponse } from './test/managementFixtures';
import { capabilities, command } from './test/sourceActionFixtures';

describe('reviewed source capabilities', () => {
  it('validates exact public references and bounded limits, including an empty catalog', async () => {
    const value = capabilities(); expect(parseSourceCapabilities(value)).toEqual(value);
    value.data.endpoints = []; value.data.reliabilityProfiles = []; value.data.storageProfiles = [];
    expect(parseSourceCapabilities(value)).toEqual(value);
    const fetch = vi.fn().mockResolvedValue(jsonResponse(value)); vi.stubGlobal('fetch', fetch);
    const signal = new AbortController().signal;
    await expect(fetchSourceCapabilities(signal)).resolves.toEqual(value);
    expect(fetch).toHaveBeenCalledWith('/api/management/v1/capabilities', { method: 'GET', signal, credentials: 'omit', cache: 'no-store', headers: { Accept: 'application/json' } });
  });
  it.each([
    ['wrong version', (v) => ({ ...v, apiVersion: 2 })],
    ['private endpoint URL', (v) => ({ ...v, data: { ...v.data, endpoints: [{ ...v.data.endpoints[0], uri: 'file:///private' }] } })],
    ['wrong scope', (v) => ({ ...v, data: { ...v.data, scope: 'RUN_SUBMISSION' } })],
    ['unknown profile', (v) => ({ ...v, data: { ...v.data, profiles: ['rss-atom-v1', 'request-jsonl-v2', 'unknown'] } })],
    ['wrong capacity', (v) => ({ ...v, data: { ...v.data, maxSources: 129 } })],
    ['wrong revision bound', (v) => ({ ...v, data: { ...v.data, maxRevisionsPerSource: 999 } })],
    ['invalid identity', (v) => ({ ...v, data: { ...v.data, endpoints: [{ endpointId: v.data.endpoints[0]!.endpointId, sourceId: 'Upper' }] } })],
    ['duplicate endpoints', (v) => ({ ...v, data: { ...v.data, endpoints: [v.data.endpoints[0], v.data.endpoints[0]] } })],
    ['oversized catalog', (v) => ({ ...v, data: { ...v.data, endpoints: Array(129).fill(v.data.endpoints[0]) } })],
    ['wrong storage role', (v) => ({ ...v, data: { ...v.data, storageProfiles: [{ ...v.data.storageProfiles[0], role: 'BATCH' }] } })],
    ['private profile fields', (v) => ({ ...v, data: { ...v.data, reliabilityProfiles: [{ ...v.data.reliabilityProfiles[0], path: '/secret' }] } })],
    ['bad profile digest', (v) => ({ ...v, data: { ...v.data, reliabilityProfiles: [{ ...v.data.reliabilityProfiles[0], registeredProfileSha256: 'latest' }] } })],
    ['duplicate profiles', (v) => ({ ...v, data: { ...v.data, reliabilityProfiles: [v.data.reliabilityProfiles[0], v.data.reliabilityProfiles[0]] } })],
  ] satisfies [string, (v: ReturnType<typeof capabilities>) => unknown][])('rejects %s', (_, change) => {
    expect(() => parseSourceCapabilities(change(capabilities()))).toThrow();
  });
});

describe('source mutation transport', () => {
  it.each(['create', 'revise', 'enabled'] as const)('sends exact %s method, original body, key and precondition', async (action) => {
    const intent = command(action), record = sourceRecord();
    const fetch = vi.fn().mockResolvedValue(new Response(JSON.stringify(sourceEnvelope(record)), {
      status: action === 'create' ? 201 : 200, headers: { 'Content-Type': 'application/json', ETag: `"${record.rowVersion}"` },
    })); vi.stubGlobal('fetch', fetch);
    const signal = new AbortController().signal;
    const result = await sendSourceCommand(intent, signal);
    expect(result.etag).toBe('"9007199254740993"'); expect(result.data.rowVersion).toBe('9007199254740993');
    const base = '/api/management/v1/sources', path = action === 'create' ? base : `${base}/content-intelligence/alpha/${action === 'revise' ? 'revisions' : 'enabled'}`;
    expect(fetch).toHaveBeenCalledWith(path, { method: action === 'enabled' ? 'PUT' : 'POST', body: intent.body, signal, credentials: 'omit', cache: 'no-store',
      headers: { Accept: 'application/json', 'Content-Type': 'application/json', 'X-DDIP-Local-Request': '1', 'Idempotency-Key': 'test-intent-1',
        ...(action === 'create' ? {} : { 'If-Match': intent.ifMatch }) } });
    expect(fetch).toHaveBeenCalledTimes(1);
  });
  it.each([
    ['missing key', { key: '' }], ['unsafe key', { key: 'a\nb' }], ['arbitrary endpoint', { identity: { productId: 'other', sourceId: 'alpha' } }],
    ['unquoted ETag', { action: 'enabled', body: '{"enabled":true}', ifMatch: '1' }],
    ['overflow ETag', { action: 'enabled', body: '{"enabled":true}', ifMatch: '"9223372036854775808"' }],
    ['registration ETag', { ifMatch: '"1"' }], ['unknown enabled field', { action: 'enabled', ifMatch: '"1"', body: '{"enabled":false,"cancel":true}' }],
    ['string admission', { action: 'enabled', ifMatch: '"1"', body: '{"enabled":"false"}' }],
    ['empty display name', { body: JSON.stringify({ ...JSON.parse(command().body), displayName: ' ' }) }],
    ['wrong variant', { body: JSON.stringify({ ...JSON.parse(command().body), config: { ...sourceRecord().config, dataOrigin: 'synthetic' } }) }],
    ['raw URL field', { body: JSON.stringify({ ...JSON.parse(command().body), config: { ...sourceRecord().config, endpoint: 'http://secret' } }) }],
  ])('rejects %s without a network request', async (_, overrides) => {
    const fetch = vi.fn(); vi.stubGlobal('fetch', fetch);
    await expect(sendSourceCommand({ ...command(), ...overrides } as SourceCommand, new AbortController().signal)).rejects.toHaveProperty('kind', 'invalid-request');
    expect(fetch).not.toHaveBeenCalled();
  });
  it('validates UTF-8 byte and identity boundaries without normalization', () => {
    expect(isSourceDisplayName('é'.repeat(64))).toBe(true); expect(isSourceDisplayName('é'.repeat(65))).toBe(false);
    expect(isSourceDisplayName('a\nb')).toBe(false); expect(isSourceDisplayName('  ')).toBe(false); expect(isSourceDisplayName(' A ')).toBe(true);
    const intent = command(); const body = JSON.parse(intent.body); body.sourceId = 'a'.repeat(128); intent.identity.sourceId = body.sourceId;
    intent.body = JSON.stringify(body); expect(() => validateSourceCommand(intent)).not.toThrow();
    body.sourceId += 'a'; intent.body = JSON.stringify(body); expect(() => validateSourceCommand(intent)).toThrow();
  });
  it.each([
    [400, 'INVALID_ARGUMENT', false], [403, 'LOCAL_BOUNDARY_REJECTED', false], [404, 'NOT_FOUND', false], [409, 'STATE_CONFLICT', false],
    [409, 'IDEMPOTENCY_CONFLICT', false], [412, 'PRECONDITION_FAILED', false], [428, 'PRECONDITION_REQUIRED', false],
    [429, 'MANAGEMENT_CAPACITY_EXCEEDED', false], [503, 'MANAGEMENT_UNAVAILABLE', true], [500, 'INTERNAL_FAILURE', true],
  ])('preserves safe HTTP %s %s metadata; ambiguity=%s', async (status, code, ambiguous) => {
    const fetch = vi.fn().mockResolvedValue(jsonResponse(managementFailure(code as string), status as number)); vi.stubGlobal('fetch', fetch);
    const error = await sendSourceCommand(command(), new AbortController().signal).catch((error: unknown) => error);
    expect(error).toMatchObject({ status, code, correlationId: CORRELATION, retryable: code === 'MANAGEMENT_UNAVAILABLE', ambiguous });
    expect((error as Error).message).not.toMatch(/private|traceback/); expect(fetch).toHaveBeenCalledTimes(1);
  });
  it.each([
    ['lost connection', () => Promise.reject(new Error('private transport error'))],
    ['proxy error', () => Promise.resolve(new Response('secret proxy body', { status: 503 }))],
    ['unrecognized error', () => Promise.resolve(jsonResponse(managementFailure('SECRET_CODE'), 409))],
    ['malformed success', () => Promise.resolve(jsonResponse({ private: true }, 201))],
    ['wrong success status', () => Promise.resolve(sourceResponse())],
    ['missing ETag', () => Promise.resolve(jsonResponse(sourceEnvelope(), 201))],
    ['wrong source', () => Promise.resolve(new Response(JSON.stringify(sourceEnvelope(sourceRecord('other'))), { status: 201, headers: { 'Content-Type': 'application/json', ETag: '"9007199254740993"' } }))],
  ])('retains an ambiguous outcome after %s', async (_, respond) => {
    const fetch = vi.fn().mockImplementation(respond); vi.stubGlobal('fetch', fetch);
    await expect(sendSourceCommand(command(), new AbortController().signal)).rejects.toMatchObject({ ambiguous: true });
    expect(fetch).toHaveBeenCalledTimes(1);
  });
});
