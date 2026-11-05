import { describe, expect, it, vi } from 'vitest';
import { coverageQueryError, fetchCoverage, parseInputCoverage, parseReferenceCoverage, reliabilityIdentity } from './reliabilityCoverageApi';
import { inputCoverageFixture, referenceCoverageFixture } from './test/reliabilityCoverageFixtures';
import { jsonResponse } from './test/fixtures';

describe('native coverage boundary', () => {
 it('preserves overlapping rejection counts, exact timestamps and signed delays', () => {
  const response = inputCoverageFixture(); expect(parseInputCoverage(response)).toEqual(response);
  expect(parseReferenceCoverage(referenceCoverageFixture())).toEqual(referenceCoverageFixture());
  response.data[0]!.firstEventTime = '0001-01-01T00:00:00.000000Z'; response.data[0]!.lastEventTime = '0001-01-01T00:00:00.000000Z';
  response.data[0]!.receivedAt = '9999-12-31T23:59:59.999999Z'; response.data[0]!.minReceiptDelayUs = '315537897599999999'; response.data[0]!.maxReceiptDelayUs = '315537897599999999'; response.data[0]!.futureOccurrenceCount = '0';
  expect(parseInputCoverage(response).data[0]!.minReceiptDelayUs).toBe('315537897599999999');
  [response.data[0]!.receivedAt,response.data[0]!.firstEventTime] = [response.data[0]!.firstEventTime,response.data[0]!.receivedAt]; response.data[0]!.lastEventTime = response.data[0]!.firstEventTime;
  response.data[0]!.minReceiptDelayUs = '-315537897599999999'; response.data[0]!.maxReceiptDelayUs = '-315537897599999999'; response.data[0]!.futureOccurrenceCount = '2';
  expect(parseInputCoverage(response).data[0]!.minReceiptDelayUs).toBe('-315537897599999999');
 });
 it('permits stored duplicates without the representative in a filtered scope', () => {
  const response = inputCoverageFixture({batchId:'duplicates'}); response.data[0]!.acceptedRepresentativeCount = '0'; response.data[0]!.duplicateOccurrenceCount = '2';
  response.totals.acceptedRepresentativeCount = '0'; response.totals.duplicateOccurrenceCount = '2';
  expect(parseInputCoverage(response,{batchId:'duplicates'}).totals.duplicateExcessInScope).toBe('1');
 });
 it.each(['start','end','environment','serviceId'])('rejects unsupported input filter %s', key => expect(coverageQueryError('input-coverage',{[key]:'test'})).not.toBeNull());
 it.each([{sourceId:'Upper'}, {batchId:'a\n'}, {sourceId:' a'}, {batchId:'a'.repeat(65)}, {limit:0}, {limit:true}, {offset:1000001}, {offset:0.1}])('rejects invalid query %j', query => expect(coverageQueryError('input-coverage',query)).not.toBeNull());
 it('accepts exact identifier limits and rejects explicit undefined', () => {
  expect(coverageQueryError('input-coverage',{sourceId:'a'.repeat(128),batchId:'a'.repeat(64),limit:1000,offset:1000000})).toBeNull();
  expect(coverageQueryError('reference-coverage',{serviceId:undefined})).not.toBeNull();
 });
 it.each([
  (r: ReturnType<typeof inputCoverageFixture>) => { r.totals.inputBatchCount = '33'; },
  r => { r.totals.inputFileCount = '129'; }, r => { r.totals.inputByteLength = '1073741825'; },
  r => { r.totals.physicalOccurrenceCount = '2000001'; }, r => { r.data[0]!.inputFileCount = '17'; },
  r => { r.data[0]!.minReceiptDelayUs = '-0'; }, r => { r.data[0]!.maxReceiptDelayUs = '0'; },
  r => { r.data[0]!.minReceiptDelayUs = '-315537897600000000'; }, r => { r.data[0]!.firstEventTime = '2026-02-30T00:00:00.000000Z'; },
  r => { r.data[0]!.receivedAt = '10000-01-01T00:00:00.000000Z'; }, r => { r.data[0]!.futureOccurrenceCount = '0'; },
  r => { r.data[0]!.physicalOccurrenceCount = '3'; }, r => { r.data[0]!.rejectedReasonCounts.FIELD_NULL = '3'; },
  r => { r.data[0]!.rejectedReasonCounts.FIELD_NULL = '0'; r.data[0]!.rejectedReasonCounts.FIELD_VALUE = '0'; },
  r => { r.data[0]!.batchFingerprint = 'X'.repeat(64); }, r => { r.data[0]!.ingestionId = 'not-a-uuid'; },
  r => { r.totals.distinctValidIdentityCount = '0'; }, r => { r.meta.hasMore = true; },
  r => { r.data.push({...r.data[0]!}); r.meta.returned = 2; r.totals.inputBatchCount = '2'; },
 ])('rejects invalid input schema, totals, order and value equations %#', mutate => {
  const response = inputCoverageFixture(); mutate(response); expect(() => parseInputCoverage(response)).toThrow('contract');
 });
 it.each([
  (r: ReturnType<typeof referenceCoverageFixture>) => { r.data[0]!.referenceUsage = 'UNUSED'; },
  r => { r.data[0]!.missingOptionalAttributes.reverse(); }, r => { r.data[0]!.missingOptionalAttributes = []; },
  r => { r.data[0]!.serviceName = ' Alpha'; }, r => { r.data[0]!.ownerTeam = 'private-team'; },
  r => { r.data[0]!.missingReferenceEventCount = '1'; }, r => { r.totals.referenceServiceCount = '10001'; },
  r => { r.totals.matchedServicesMissingOptionalAttributesCount = '2'; }, r => { r.totals.eventCount = '3'; },
  r => { r.data[0]!.referenceStatus = 'MISSING'; },
 ])('rejects invalid reference semantics %#', mutate => { const response = referenceCoverageFixture(); mutate(response); expect(() => parseReferenceCoverage(response)).toThrow('contract'); });
 it('accepts unused and absent services with their different optional-null semantics', () => {
  const r = referenceCoverageFixture(); r.data[0]!.referenceUsage = 'UNUSED'; r.data[0]!.eventCount = '0';
  Object.assign(r.totals,{eventCount:'0',matchedEventCount:'0',eventServiceCount:'0',matchedServiceCount:'0',matchedServicesMissingOptionalAttributesCount:'0',unusedReferenceServiceCount:'1'});
  expect(parseReferenceCoverage(r)).toEqual(r);
  Object.assign(r.data[0]!,{referenceUsage:'ABSENT',referenceStatus:'MISSING',serviceName:null,eventCount:'1',missingReferenceEventCount:'1',missingOptionalAttributes:[]});
  Object.assign(r.totals,{eventCount:'1',missingEventCount:'1',eventServiceCount:'1',missingServiceCount:'1',referenceServiceCount:'0',unusedReferenceServiceCount:'0'});
  expect(parseReferenceCoverage(r)).toEqual(r);
 });
 it('checks schema extras, echoed filters and complete identity tuple', () => {
  const r = inputCoverageFixture(); expect(() => parseInputCoverage({...r,extra:1})).toThrow(); expect(() => parseInputCoverage(r,{sourceId:'other'})).toThrow();
  const identity = reliabilityIdentity(r); r.inputFormat = 'PARQUET'; expect(reliabilityIdentity(r)).not.toBe(identity);
  r.inputFormat = 'JSONL'; r.reference.revision = 'v2'; expect(reliabilityIdentity(r)).not.toBe(identity);
 });
 it('rejects impossible full-scope totals even beyond the last page', () => {
  const input = inputCoverageFixture({offset:100}); input.data=[]; input.meta.returned=0;
  input.totals.distinctValidIdentityCount='0'; input.totals.acceptedRepresentativeCount='0'; input.totals.duplicateOccurrenceCount='2'; input.totals.duplicateExcessInScope='2';
  expect(() => parseInputCoverage(input,{offset:100})).toThrow();
  const empty = inputCoverageFixture({offset:100}); empty.data=[]; empty.meta.returned=0; empty.totals.inputBatchCount='0'; empty.totals.inputFileCount='0';
  expect(() => parseInputCoverage(empty,{offset:100})).toThrow();
  const reference = referenceCoverageFixture({offset:100}); reference.data=[]; reference.meta.returned=0;
  Object.assign(reference.totals,{eventServiceCount:'0',matchedServiceCount:'0',matchedServicesMissingOptionalAttributesCount:'0',unusedReferenceServiceCount:'1'});
  expect(() => parseReferenceCoverage(reference,{offset:100})).toThrow();
 });
 it('rejects an unsupported runtime report before fetching', async () => {
  const fetch = vi.fn(); vi.stubGlobal('fetch',fetch);
  await expect(fetchCoverage('unexpected' as 'input-coverage',{},new AbortController().signal)).rejects.toThrow('not accepted');
  expect(fetch).not.toHaveBeenCalled();
 });
 it('uses only a fixed report endpoint, supported query and safe fetch options', async () => {
  const fetch = vi.fn().mockResolvedValue(jsonResponse(inputCoverageFixture({batchId:'batch'}))); vi.stubGlobal('fetch',fetch);
  const signal = new AbortController().signal; await fetchCoverage('input-coverage',{batchId:'batch'},signal);
  expect(fetch).toHaveBeenCalledWith('/api/v1/reliability/input-coverage?limit=100&offset=0&batchId=batch',expect.objectContaining({signal,cache:'no-store',credentials:'omit'}));
 });
 it.each([400,500,503,504])('keeps upstream %s errors private', async status => {
  vi.stubGlobal('fetch',vi.fn().mockResolvedValue(jsonResponse({message:'/private/path',code:'ANALYTICS_CAPACITY_EXCEEDED'},status)));
  await expect(fetchCoverage('reference-coverage',{},new AbortController().signal)).rejects.not.toThrow('/private');
 });
});
