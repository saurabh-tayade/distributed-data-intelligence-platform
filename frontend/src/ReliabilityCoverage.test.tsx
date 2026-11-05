import { StrictMode } from 'react';
import { act, fireEvent, render, screen, waitFor, within } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { App } from './App';
import { inputCoverageFixture, referenceCoverageFixture } from './test/reliabilityCoverageFixtures';
import { reliabilityFixture, reliabilityQuery } from './test/reliabilityFixtures';
import { deferred, fixture, jsonResponse } from './test/fixtures';
import { sourcePage } from './test/managementFixtures';
import type { CoverageQuery } from './reliabilityCoverageApi';

function coverageQuery(path: string): CoverageQuery {
 const search = new URL(path,'http://localhost').searchParams;
 return { limit:Number(search.get('limit') ?? 100),offset:Number(search.get('offset') ?? 0),
  ...(search.has('sourceId') ? {sourceId:search.get('sourceId')!} : {}),...(search.has('batchId') ? {batchId:search.get('batchId')!} : {}),...(search.has('serviceId') ? {serviceId:search.get('serviceId')!} : {}) };
}
function mockApi() {
 const fetch = vi.fn(async (path: string) => {
  if (path.includes('/input-coverage?')) return jsonResponse(inputCoverageFixture(coverageQuery(path)));
  if (path.includes('/reference-coverage?')) return jsonResponse(referenceCoverageFixture(coverageQuery(path)));
  if (path.includes('/request-windows?')) return jsonResponse(reliabilityFixture(reliabilityQuery()));
  return path.startsWith('/api/management/') ? jsonResponse(sourcePage()) : jsonResponse(fixture());
 }); vi.stubGlobal('fetch',fetch); return fetch;
}
function tab(name: string) { fireEvent.click(within(screen.getByRole('navigation',{name:'Reliability reports'})).getByRole('button',{name})); }
function edit(name: string, value: string) { fireEvent.change(screen.getByRole('textbox',{name}),{target:{value}}); }
async function inputReady() { await screen.findByRole('region',{name:'Input coverage totals'}); }
async function referenceReady() { await screen.findByRole('region',{name:'Reference coverage totals'}); }
beforeEach(() => { window.history.replaceState(null,'','/#/analytics/reliability'); });
describe('Reliability coverage workspace', () => {
 it('loads only the selected report and shows all totals, per-batch reasons and signed delay semantics', async () => {
  const fetch = mockApi(); render(<App/>); expect(fetch).not.toHaveBeenCalled(); tab('Input coverage'); await inputReady();
  expect(fetch).toHaveBeenCalledTimes(1); expect(fetch.mock.calls[0]![0]).toBe('/api/v1/reliability/input-coverage?limit=100&offset=0');
  expect(screen.queryByRole('textbox',{name:'Start inclusive (UTC)'})).not.toBeInTheDocument();
  expect(screen.queryByRole('textbox',{name:'Exact service ID'})).not.toBeInTheDocument();
  expect(screen.getAllByText('Stored DUPLICATE occurrences')).toHaveLength(2);
  expect(screen.getByText('Duplicate excess in scope')).toBeVisible(); expect(screen.getByText('JSON_NONFINITE')).toBeVisible();
  expect(screen.getByText(/Rejection reasons overlap/)).toBeVisible(); expect(screen.getAllByText('-1')).toHaveLength(2);
  expect(screen.getAllByText(/12:00:00.000002 UTC/)).toHaveLength(2);
  tab('Reference coverage'); await referenceReady(); expect(fetch).toHaveBeenCalledTimes(2);
  expect(screen.queryByRole('region',{name:'Input coverage totals'})).not.toBeInTheDocument();
  expect(screen.getByText('USED / MATCHED')).toBeVisible(); expect(screen.getByText('owner_team, service_tier')).toBeVisible();
  expect(screen.getAllByText('Not supplied (null)')).toHaveLength(2);
  expect(document.title).toBe('Reliability reference coverage | DDIP');
  tab('Request windows'); expect(fetch).toHaveBeenCalledTimes(2); expect(screen.getByRole('heading',{name:'Choose a UTC window interval'})).toBeVisible();
 });
 it('applies only exact supported filters, does not trim, refreshes, and restores applied report filters', async () => {
  const fetch = mockApi(); render(<App/>); tab('Input coverage'); await inputReady();
  edit('Exact batch ID',' batch'); fireEvent.click(screen.getByRole('button',{name:'Apply coverage filters'})); expect(screen.getByRole('alert')).toHaveTextContent('without whitespace'); expect(fetch).toHaveBeenCalledTimes(1);
  edit('Exact batch ID','batch'); edit('Exact source ID','source'); fireEvent.click(screen.getByRole('button',{name:'Apply coverage filters'})); await inputReady();
  expect(fetch.mock.calls.at(-1)![0]).toContain('sourceId=source&batchId=batch');
  fireEvent.click(screen.getByRole('button',{name:'Refresh page'})); await inputReady(); expect(fetch).toHaveBeenCalledTimes(3);
  tab('Reference coverage'); await referenceReady(); edit('Exact service ID','beta'); fireEvent.click(screen.getByRole('button',{name:'Apply coverage filters'})); await referenceReady();
  expect(fetch.mock.calls.at(-1)![0]).toContain('serviceId=beta');
  tab('Input coverage'); await inputReady(); expect(screen.getByRole('textbox',{name:'Exact source ID'})).toHaveValue('source'); expect(screen.getByRole('textbox',{name:'Exact batch ID'})).toHaveValue('batch');
 });
 it('clears totals and rows on refresh, rejects late aborted success from a previous report, and keeps the shared pin', async () => {
  const fetch = mockApi(); render(<StrictMode><App/></StrictMode>); tab('Input coverage'); await inputReady();
  const late = deferred<Response>(); fetch.mockReturnValueOnce(late.promise); fireEvent.click(screen.getByRole('button',{name:'Refresh page'}));
  expect(screen.queryByRole('region',{name:'Input coverage totals'})).not.toBeInTheDocument(); expect(screen.queryByRole('table')).not.toBeInTheDocument();
  expect(screen.getByRole('heading',{name:'Loading input coverage'})).toBeVisible();
  tab('Reference coverage'); await referenceReady(); const changed = inputCoverageFixture(); changed.publication='f'.repeat(64);
  await act(async () => {late.resolve(jsonResponse(changed));}); expect(screen.queryByRole('alert')).not.toBeInTheDocument();
  expect(screen.getByRole('region',{name:'Reference coverage totals'})).toBeVisible();
 });
 it('clears totals and rows on failures and retry and ignores late rejected requests', async () => {
  const fetch = mockApi(); render(<App/>); tab('Input coverage'); await inputReady();
  fetch.mockResolvedValueOnce(jsonResponse({message:'/private/raw'},500)); fireEvent.click(screen.getByRole('button',{name:'Refresh page'}));
  expect(await screen.findByRole('alert')).not.toHaveTextContent('/private'); expect(screen.queryByRole('table')).not.toBeInTheDocument(); expect(screen.queryByRole('region',{name:'Input coverage totals'})).not.toBeInTheDocument();
  fireEvent.click(screen.getByRole('button',{name:'Try again'})); await inputReady();
  const late=deferred<Response>(); fetch.mockReturnValueOnce(late.promise); fireEvent.click(screen.getByRole('button',{name:'Refresh page'})); tab('Reference coverage'); await referenceReady();
  await act(async()=>{late.reject(new Error('late error'));}); expect(screen.queryByRole('alert')).not.toBeInTheDocument();
 });
 it.each(['publication','inputFormat','referenceId','revision','snapshotFingerprint'])('latches %s identity changes across reports and workspace history until reload', async field => {
  const fetch = mockApi(); render(<App/>); tab('Input coverage'); await inputReady();
  const changed=referenceCoverageFixture();
  if(field==='publication') changed.publication='f'.repeat(64); else if(field==='inputFormat') changed.inputFormat='PARQUET'; else changed.reference[field as keyof typeof changed.reference]=field==='snapshotFingerprint'?'f'.repeat(64):'changed';
  fetch.mockResolvedValueOnce(jsonResponse(changed)); tab('Reference coverage');
  expect(await screen.findByRole('heading',{name:'Publication changed'})).toBeVisible();
  expect(screen.queryByRole('table')).not.toBeInTheDocument(); expect(screen.queryByRole('region',{name:'Input coverage totals'})).not.toBeInTheDocument(); expect(screen.queryByRole('region',{name:'Reference coverage totals'})).not.toBeInTheDocument();
  expect(screen.queryByLabelText('Reliability publication fingerprint')).not.toBeInTheDocument();
  expect(within(screen.getByRole('navigation',{name:'Reliability reports'})).getAllByRole('button').every(button=>(button as HTMLButtonElement).disabled)).toBe(true);
  const count=fetch.mock.calls.filter(([path])=>path.startsWith('/api/v1/reliability/')).length;
  await act(async()=>{window.history.pushState(null,'','/#/analytics'); window.dispatchEvent(new PopStateEvent('popstate'));});
  await screen.findByRole('heading',{name:'Publisher breakdown'});
  await act(async()=>{window.history.pushState(null,'','/#/operations/sources'); window.dispatchEvent(new PopStateEvent('popstate'));});
  await screen.findByRole('heading',{name:'Registered sources'});
  await act(async()=>{window.history.back();}); await screen.findByRole('heading',{name:'Publisher breakdown'});
  await act(async()=>{window.history.back();}); await screen.findByRole('heading',{name:'Publication changed'});
  expect(fetch.mock.calls.filter(([path])=>path.startsWith('/api/v1/reliability/'))).toHaveLength(count);
 });
 it('uses full-scope native totals across pages, resets offset when filters change, and clears pages while loading', async () => {
  const fetch=mockApi(); fetch.mockImplementation(async(path:string)=>{
   const q=coverageQuery(path); const r=referenceCoverageFixture(q); const offset=q.offset??0;
   r.totals={eventCount:'21',matchedEventCount:'21',missingEventCount:'0',eventServiceCount:'21',matchedServiceCount:'21',missingServiceCount:'0',matchedServicesMissingOptionalAttributesCount:'21',referenceServiceCount:'21',unusedReferenceServiceCount:'0'};
   r.data=Array.from({length:Math.max(0,Math.min(q.limit??100,21-offset))},(_,index)=>({...r.data[0]!,serviceId:`service-${String(offset+index).padStart(2,'0')}`,eventCount:'1'}));
   r.meta.returned=r.data.length;r.meta.hasMore=offset+r.data.length<21;return jsonResponse(r);
  }); render(<App/>);tab('Reference coverage');await referenceReady();
  fireEvent.change(screen.getByRole('combobox',{name:'Coverage rows per page'}),{target:{value:'20'}});fireEvent.click(screen.getByRole('button',{name:'Apply coverage filters'}));await referenceReady();
  fireEvent.click(screen.getByRole('button',{name:'Next coverage'})); await referenceReady();expect(fetch.mock.calls.at(-1)![0]).toContain('offset=20');
  expect(screen.getByRole('table')).toHaveTextContent('service-20');expect(screen.getByRole('table')).not.toHaveTextContent('service-00');
  expect(screen.getByRole('region',{name:'Reference coverage totals'})).toHaveTextContent('21');expect(screen.getByRole('button',{name:'Next coverage'})).toBeDisabled();
  fireEvent.click(screen.getByRole('button',{name:'Previous coverage'})); await referenceReady();expect(fetch.mock.calls.at(-1)![0]).toContain('offset=0');
  fireEvent.click(screen.getByRole('button',{name:'Next coverage'})); await referenceReady();fireEvent.click(screen.getByRole('button',{name:'Apply coverage filters'})); await referenceReady();expect(fetch.mock.calls.at(-1)![0]).toContain('offset=0');
 });
 it('aborts a coverage request when leaving Reliability and refetches only that report on return', async () => {
  const fetch=mockApi();const late=deferred<Response>();fetch.mockReturnValueOnce(late.promise);render(<App/>);tab('Input coverage');
  await act(async()=>{window.location.hash='#/analytics';});await screen.findByRole('heading',{name:'Publisher breakdown'});
  await act(async()=>{late.resolve(jsonResponse(inputCoverageFixture()));});
  fireEvent.click(screen.getByRole('link',{name:'Application Reliability'}));await inputReady();
  await waitFor(()=>expect(fetch.mock.calls.filter(([path])=>path.includes('/input-coverage?'))).toHaveLength(2));
 });
});
