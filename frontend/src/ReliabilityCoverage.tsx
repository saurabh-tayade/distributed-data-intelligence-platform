import { useState } from 'react';
import { coverageQueryError, INPUT_TOTALS, REFERENCE_TOTALS, REASONS } from './reliabilityCoverageApi';
import type { CoverageQuery, CoverageReport, InputCoverageResponse, ReferenceCoverageResponse } from './reliabilityCoverageApi';
import { RELIABILITY_MAX_OFFSET } from './reliabilityApi';
import type { useReliability } from './useReliability';
import { ReliabilityProvenance } from './ReliabilityWorkspace';
import { TableScroll } from './components/ReportParts';
import { Timestamp } from './components/Timestamp';
import { ErrorState } from './components/States';
import { formatCount } from './metrics';
const labels: Record<string, string> = {
 inputBatchCount: 'Registered batches', inputFileCount: 'Input files', inputByteLength: 'Input bytes', physicalOccurrenceCount: 'Physical occurrences',
 validOccurrenceCount: 'Valid occurrences', acceptedRepresentativeCount: 'Accepted representatives', duplicateOccurrenceCount: 'Stored DUPLICATE occurrences',
 rejectedOccurrenceCount: 'Rejected occurrences', distinctValidIdentityCount: 'Distinct valid identities in scope', duplicateExcessInScope: 'Duplicate excess in scope',
 eventCount: 'Unique events', matchedEventCount: 'Matched events', missingEventCount: 'Missing-reference events', eventServiceCount: 'Services with events',
 matchedServiceCount: 'Used matched services', missingServiceCount: 'Missing-reference services', matchedServicesMissingOptionalAttributesCount: 'Used matched services missing optional attributes',
 referenceServiceCount: 'Reference services', unusedReferenceServiceCount: 'Unused reference services',
};
function CoverageFilters({ report, query, onApply }: { report: CoverageReport; query: CoverageQuery; onApply: (query: CoverageQuery) => void }) {
 const [draft, setDraft] = useState({ sourceId: query.sourceId ?? '', batchId: query.batchId ?? '', serviceId: query.serviceId ?? '', limit: query.limit ?? 100 });
 const [error, setError] = useState<string | null>(null);
 return <form className="source-filters reliability-filters" aria-label="Coverage filters" noValidate onSubmit={event => {
  event.preventDefault(); const next: CoverageQuery = { limit: draft.limit, ...(report === 'input-coverage'
   ? { ...(draft.sourceId ? { sourceId: draft.sourceId } : {}), ...(draft.batchId ? { batchId: draft.batchId } : {}) }
   : draft.serviceId ? { serviceId: draft.serviceId } : {}) };
  const invalid = coverageQueryError(report,next); setError(invalid); if (!invalid) onApply(next);
 }}>
 { (report === 'input-coverage' ? ['sourceId','batchId'] as const : ['serviceId'] as const).map(key => <label key={key}>
   {key === 'sourceId' ? 'Exact source ID' : key === 'batchId' ? 'Exact batch ID' : 'Exact service ID'}
   <input value={draft[key]} autoCapitalize="none" autoCorrect="off" spellCheck={false} aria-invalid={!!error} aria-describedby="coverage-filter-help coverage-filter-error"
    onChange={event => {setDraft({...draft,[key]:event.target.value}); setError(null);}} /></label>)}
 <label>Coverage rows per page<select value={draft.limit} onChange={event => setDraft({...draft,limit:Number(event.target.value)})}>
  <option value="20">20</option><option value="50">50</option><option value="100">100</option></select></label>
 <button type="submit" className="primary-button">Apply coverage filters</button>
 <p id="coverage-filter-help">Leave identifiers blank for the complete scope. Filters are exact and are not trimmed. {report === 'input-coverage'
  ? 'Batch ID alone selects that ID across all sources. Whole registered batches only; no service, environment or time filters.'
  : 'Service ID only; no source, environment or time filters.'}</p>
 <p id="coverage-filter-error" className="field-error" role={error ? 'alert' : undefined}>{error}</p>
 </form>;
}
function InputReport({ response }: { response: InputCoverageResponse }) {
 return <><p className="metric-explanation">Counts include physical deliveries and rejected lines. Stored DUPLICATE is relative to the entire publication;
  duplicate excess in scope is valid occurrences minus distinct valid identities in the filtered batches. These counts can differ when the accepted representative is outside this scope.</p>
 <section className="panel" aria-label="Input coverage totals"><h2>Full filtered-scope totals</h2><dl className="article-detail-grid">
  {INPUT_TOTALS.map(key => <div key={key}><dt>{labels[key]}</dt><dd>{formatCount(response.totals[key])}</dd></div>)}
 </dl><p>Native totals before pagination. No whole-scope event-time extrema or rejection-reason histogram are supplied.</p></section>
 <section className="panel table-panel"><h2>Registered batch details</h2><TableScroll label="Input coverage batch details"><table className="reliability-table">
 <caption>Whole registered batches ordered by source ID, batch ID and batch fingerprint.</caption>
 <thead><tr><th scope="col">Source / batch</th><th scope="col">Receipt / identity</th><th scope="col">Physical counts</th><th scope="col">Valid event times (UTC)</th><th scope="col">Receipt delays (µs)</th><th scope="col">Rejected reasons</th></tr></thead>
 <tbody>{response.data.map(row => <tr key={`${row.sourceId}:${row.batchId}:${row.batchFingerprint}`}>
  <th scope="row">{row.sourceId}<span className="run-metadata">{row.batchId}</span></th>
  <td><Timestamp value={row.receivedAt}/><dl><div><dt>Batch fingerprint</dt><dd><code>{row.batchFingerprint}</code></dd></div><div><dt>Ingestion ID</dt><dd><code>{row.ingestionId}</code></dd></div></dl></td>
  <td><dl>{(['inputFileCount','inputByteLength','physicalOccurrenceCount','validOccurrenceCount','acceptedRepresentativeCount','duplicateOccurrenceCount','rejectedOccurrenceCount'] as const).map(key => <div key={key}><dt>{labels[key]}</dt><dd>{formatCount(row[key])}</dd></div>)}</dl></td>
  <td><dl><div><dt>First</dt><dd>{row.firstEventTime ? <Timestamp value={row.firstEventTime}/> : 'No valid occurrences'}</dd></div>
   <div><dt>Last</dt><dd>{row.lastEventTime ? <Timestamp value={row.lastEventTime}/> : 'No valid occurrences'}</dd></div>
   <div><dt>Future occurrences</dt><dd>{formatCount(row.futureOccurrenceCount)}</dd></div></dl></td>
  <td><dl><div><dt>Minimum signed delay</dt><dd>{row.minReceiptDelayUs ?? 'No valid occurrences'}</dd></div><div><dt>Maximum signed delay</dt><dd>{row.maxReceiptDelayUs ?? 'No valid occurrences'}</dd></div></dl></td>
  <td><dl>{REASONS.map(reason => <div key={reason}><dt>{reason}</dt><dd>{formatCount(row.rejectedReasonCounts[reason])}</dd></div>)}</dl></td>
 </tr>)}</tbody></table></TableScroll>
 <p>Event times and delays include valid duplicate deliveries. Signed receipt delay is received-at minus event-time in exact microseconds, not a lateness classification.
  Future means event-time is after receipt. Rejection reasons overlap, so their counts may sum above rejected occurrences.</p></section></>;
}
function ReferenceReport({ response }: { response: ReferenceCoverageResponse }) {
 return <><p className="metric-explanation">Unique events only, never physical deliveries or rejected records. The scope includes all event services and all selected reference keys.</p>
 <section className="panel" aria-label="Reference coverage totals"><h2>Full filtered-scope totals</h2><dl className="article-detail-grid">
  {REFERENCE_TOTALS.map(key => <div key={key}><dt>{labels[key]}</dt><dd>{formatCount(response.totals[key])}</dd></div>)}
 </dl><p>Native totals before pagination. Optional-missing service totals include used matched services only.</p></section>
 <section className="panel table-panel"><h2>Service reference coverage</h2><TableScroll label="Service reference coverage"><table className="reliability-table">
  <caption>Services ordered by exact service ID.</caption><thead><tr><th scope="col">Service</th><th scope="col">Usage / reference status</th><th scope="col">Reference attributes</th><th scope="col">Unique events</th><th scope="col">Missing-reference events</th><th scope="col">Missing optional attributes</th></tr></thead>
  <tbody>{response.data.map(row => <tr key={row.serviceId}><th scope="row">{row.serviceId}</th><td>{row.referenceUsage} / {row.referenceStatus}</td>
   <td><dl><div><dt>Service name</dt><dd>{row.serviceName ?? 'No matched reference'}</dd></div><div><dt>Owner team</dt><dd>{row.ownerTeam ?? (row.referenceStatus === 'MATCHED' ? 'Not supplied (null)' : 'No matched reference')}</dd></div>
   <div><dt>Service tier</dt><dd>{row.serviceTier ?? (row.referenceStatus === 'MATCHED' ? 'Not supplied (null)' : 'No matched reference')}</dd></div></dl></td>
   <td>{formatCount(row.eventCount)}</td><td>{formatCount(row.missingReferenceEventCount)}</td><td>{row.referenceStatus === 'MISSING' ? 'Not applicable (absent reference)' : row.missingOptionalAttributes.join(', ') || 'None'}</td></tr>)}</tbody>
 </table></TableScroll><p>USED / MATCHED: reference with events. UNUSED / MATCHED: reference key with no events. ABSENT / MISSING: events with no reference key.
 Nullable owner team or service tier never makes a matched reference missing.</p></section></>;
}
export function ReliabilityCoverage({ model }: { model: ReturnType<typeof useReliability> }) {
 const report = model.report as CoverageReport, current = model.coverageCurrent;
 const response = current?.status === 'ready' ? current.response : null;
 const loading = current === null && !model.blocked, title = report === 'input-coverage' ? 'Input coverage' : 'Reference coverage';
 return <section aria-label={title}><h2>{title}</h2>
 {!model.blocked && <><CoverageFilters key={report} report={report} query={model.coverageQuery} onApply={model.applyCoverage}/>
 <section aria-label="Applied coverage filters"><p>{Object.entries(model.coverageQuery).filter(([key]) => !['limit','offset'].includes(key)).map(([key,value]) => `${key}: ${value}`).join(' · ') || 'All records in the native report scope'}</p>
 <button className="secondary-button" type="button" onClick={model.retry}>Refresh page</button></section>
 <div className="pagination-bar" aria-label="Coverage pagination"><p>Requested offset {formatCount(model.offset)} · {model.limit} rows per request{response ? ` · ${response.meta.returned} returned rows` : ''}</p>
 <div className="pagination-controls"><button type="button" className="secondary-button" disabled={loading || model.offset === 0} onClick={() => model.movePage(Math.max(0,model.offset-model.limit))}>Previous coverage</button>
 <button type="button" className="secondary-button" disabled={loading || !response?.meta.hasMore || model.offset+model.limit>RELIABILITY_MAX_OFFSET} onClick={() => model.movePage(model.offset+model.limit)}>Next coverage</button></div></div></>}
 <div aria-busy={loading}>{loading && <section className="state-panel" role="status"><h2>Loading {title.toLowerCase()}</h2><p>Verifying the Reliability publication.</p></section>}
 {current?.status === 'error' && <ErrorState error={current.error} onRetry={model.retry} label={title.toLowerCase()}/>}
 {response && <>{response.meta.hasMore && <p className="scope-notice">More coverage rows are available. {model.offset+model.limit>RELIABILITY_MAX_OFFSET ? 'Maximum offset reached. Narrow the exact filters to continue.' : 'Use Next coverage to request another page.'}</p>}
 {!response.data.length && <p role="status">No coverage rows at this offset. Totals still describe the full filtered scope.</p>}
 {report === 'input-coverage' ? <InputReport response={response as InputCoverageResponse}/> : <ReferenceReport response={response as ReferenceCoverageResponse}/>}
 <ReliabilityProvenance response={response}/></>}
 </div></section>;
}
