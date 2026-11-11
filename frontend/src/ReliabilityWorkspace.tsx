import { useState } from 'react';
import { ReliabilityCoverage } from './ReliabilityCoverage';
import type { ReliabilityIdentity } from './reliabilityCoverageApi';
import { RELIABILITY_LABEL, RELIABILITY_MAX_OFFSET, reliabilityQueryError } from './reliabilityApi';
import type { ReliabilityQuery, ReliabilityResponse } from './reliabilityApi';
import type { useReliability } from './useReliability';
import { ErrorState } from './components/States';
import { TableScroll } from './components/ReportParts';
import { Timestamp } from './components/Timestamp';
import { formatCount } from './metrics';

function ReliabilityFilters({ onApply, query }: { onApply: (query: ReliabilityQuery) => void; query: ReliabilityQuery | null }) {
  const [draft, setDraft] = useState({ start: query?.start ?? '', end: query?.end ?? '', serviceId: query?.serviceId ?? '', environment: query?.environment ?? '', limit: query?.limit ?? 100 });
  const [error, setError] = useState<string | null>(null);
  return <form className="reliability-filters source-filters execution-filters" aria-label="Reliability window filters" noValidate onSubmit={(event) => {
    event.preventDefault();
    const query: ReliabilityQuery = { start: draft.start, end: draft.end, limit: draft.limit,
      ...(draft.serviceId ? { serviceId: draft.serviceId } : {}),
      ...(draft.environment ? { environment: draft.environment as NonNullable<ReliabilityQuery['environment']> } : {}) };
    const invalid = reliabilityQueryError(query);
    setError(invalid);
    if (!invalid) onApply(query);
  }}>
    <div className="reliability-range">
      <label>Start inclusive (UTC)<input type="text" required value={draft.start} placeholder="2026-10-12T00:00:00.000000Z"
        autoCapitalize="none" autoCorrect="off" spellCheck={false} aria-describedby="reliability-range-help reliability-filter-error"
        aria-invalid={error !== null} onChange={(event) => { setDraft({ ...draft, start: event.target.value }); setError(null); }} /></label>
      <label>End exclusive (UTC)<input type="text" required value={draft.end} placeholder="2026-10-13T00:00:00.000000Z"
        autoCapitalize="none" autoCorrect="off" spellCheck={false} aria-describedby="reliability-range-help reliability-filter-error"
        aria-invalid={error !== null} onChange={(event) => { setDraft({ ...draft, end: event.target.value }); setError(null); }} /></label>
    </div>
    <label>Exact service ID<input type="text" value={draft.serviceId} autoCapitalize="none" autoCorrect="off" spellCheck={false}
      aria-describedby="reliability-service-help reliability-filter-error" aria-invalid={error !== null}
      onChange={(event) => { setDraft({ ...draft, serviceId: event.target.value }); setError(null); }} /></label>
    <label>Environment<select value={draft.environment} onChange={(event) => { setDraft({ ...draft, environment: event.target.value }); setError(null); }}>
      <option value="">All environments</option><option value="dev">dev</option><option value="test">test</option><option value="production">production</option>
    </select></label>
    <label>Windows per page<select value={draft.limit} onChange={(event) => { setDraft({ ...draft, limit: Number(event.target.value) }); setError(null); }}>
      <option value="20">20</option><option value="50">50</option><option value="100">100</option>
    </select></label>
    <button type="submit" className="primary-button">Apply window filters</button>
    <div className="reliability-filter-help">
      <p id="reliability-range-help">Use exact UTC timestamps: YYYY-MM-DDTHH:mm:ss.ffffffZ. Both bounds must be on five-minute boundaries.
        Start is included; end is excluded. Choose a positive interval of at most 31 days. End alone also permits 10000-01-01T00:00:00.000000Z.</p>
      <p id="reliability-service-help">Leave service ID blank for all services, or use 1–128 lowercase letters, digits, underscores or hyphens, starting with a letter or digit.
        Values are not trimmed or converted to local time.</p>
      <p id="reliability-filter-error" className="field-error" role={error ? 'alert' : undefined}>{error}</p>
    </div>
  </form>;
}

export function ReliabilityProvenance({ response }: { response: ReliabilityIdentity }) {
  return <details className="reliability-provenance"><summary>Publication and service-reference details</summary>
    <dl className="article-detail-grid">
      <div><dt>Product</dt><dd>{response.productId}</dd></div>
      <div><dt>Product contract / processor revision</dt><dd>{response.productContractVersion} / {response.processorRevision}</dd></div>
      <div><dt>Input format</dt><dd>{response.inputFormat}</dd></div>
      <div><dt>Data origin</dt><dd>{response.origin}</dd></div>
      <div><dt>Service reference</dt><dd>{response.reference.referenceId}</dd></div>
      <div><dt>Reference revision</dt><dd>{response.reference.revision}</dd></div>
      <div className="detail-wide"><dt>Reference snapshot fingerprint</dt><dd><code>{response.reference.snapshotFingerprint}</code></dd></div>
    </dl>
  </details>;
}

export function ReliabilityTable({ response }: { response: ReliabilityResponse }) {
  return <section className="panel table-panel" aria-labelledby="reliability-table-heading">
    <div className="panel-heading"><div><p className="eyebrow">NATIVE FIVE-MINUTE GRAIN</p><h2 id="reliability-table-heading">Request windows</h2></div>
      <span className="row-badge">{response.meta.returned} returned rows</span></div>
    <TableScroll label="Reliability request windows, scroll horizontally for all columns">
      <table className="reliability-table"><caption className="visually-hidden">Synthetic request windows ordered by UTC window start, service ID, then environment, ascending.
        Each row is one five-minute interval for one service and environment. Integral values are exact.</caption>
        <thead><tr><th scope="col">Window start (UTC)</th><th scope="col">Service / environment</th><th scope="col" className="numeric">Recorded requests</th>
          <th scope="col" className="numeric">5xx responses</th><th scope="col" className="numeric">5xx / requests (ratio)</th>
          <th scope="col" className="numeric">Duration sum (ms)</th><th scope="col" className="numeric">Mean duration (ms)</th>
          <th scope="col" className="numeric">Approximate p95 (ms)</th><th scope="col">Service reference</th></tr></thead>
        <tbody>{response.data.map((row) => <tr key={`${row.windowStart}:${row.serviceId}:${row.environment}`}>
          <th scope="row"><Timestamp value={row.windowStart} /></th>
          <td><strong>{row.serviceId}</strong><span className="run-metadata">{row.environment}</span></td>
          <td className="numeric">{formatCount(row.requestCount)}</td><td className="numeric">{formatCount(row.serverErrorCount)}</td>
          <td className="numeric">{String(row.serverErrorRate)}</td><td className="numeric">{formatCount(row.durationSumMs)}</td>
          <td className="numeric">{String(row.meanDurationMs)}</td><td className="numeric">{formatCount(row.p95DurationMsApprox)}</td>
          <td className="reliability-reference"><strong>{row.referenceStatus === 'MISSING' ? 'Missing reference' : 'Matched reference'}</strong>
            <dl><div><dt>Service name</dt><dd>{row.serviceName ?? 'Not available'}</dd></div>
              <div><dt>Owner team</dt><dd>{row.ownerTeam ?? 'Not available'}</dd></div>
              <div><dt>Service tier</dt><dd>{row.serviceTier ?? 'Not available'}</dd></div></dl></td>
        </tr>)}</tbody>
      </table>
    </TableScroll>
    <div className="table-footnote"><span>Returned rows only. No global totals or zero-filled windows.</span><span>Approximate p95 values are never combined across rows.</span></div>
  </section>;
}

export function ReliabilityWorkspace({ model }: { model: ReturnType<typeof useReliability> }) {
  const { current, query, blocked, publication, limit, offset } = model;
  const response = current?.status === 'ready' ? current.response : null;
  const loading = query !== null && current === null;
  const nextOffset = offset + limit;
  return <section aria-label="Reliability analytics">
    <div className="page-heading"><div><p className="eyebrow">ANALYTICS WORKSPACE <span aria-hidden="true">/</span> APPLICATION RELIABILITY</p>
      <h1>{model.report === 'request-windows' ? 'Reliability request windows' : model.report === 'input-coverage' ? 'Reliability input coverage' : 'Reliability reference coverage'}<span className="title-period">.</span></h1>
      <p className="page-description">Request windows and coverage reports from one verified Reliability publication.</p></div>
      <span className="workspace-label">Read-only analytics</span></div>
    <p className="scope-notice reliability-origin" role="note"><strong>{RELIABILITY_LABEL}</strong></p>
    <section className="publication-panel" aria-label="Reliability publication">
      <div className="publication-label"><span>RELIABILITY PUBLICATION<span className="publication-note">Pinned independently for this session</span></span></div>
      <div className="publication-value">{publication ? <code aria-label="Reliability publication fingerprint">{publication}</code> : <span>Awaiting a verified Reliability publication</span>}
        <span className="publication-description">{publication ? 'Snapshot identity · SHA-256' : blocked ? 'Session invalidated. Reload to verify a new publication.' : 'Choose a report to request verified data.'}</span></div>
    </section>
    <p className="scope-notice">These reports use the Reliability publication selected by the local operator at API startup.
      {' '}Completing a managed run does not activate its result here. Gold and enriched Content retain independent selections.</p>
    <nav aria-label="Reliability reports" className="pagination-controls">
      {(['request-windows', 'input-coverage', 'reference-coverage'] as const).map(report => <button key={report} type="button"
        className="secondary-button" aria-current={model.report === report ? 'page' : undefined} disabled={blocked}
        onClick={() => model.selectReport(report)}>{report === 'request-windows' ? 'Request windows' : report === 'input-coverage' ? 'Input coverage' : 'Reference coverage'}</button>)}
    </nav>
    {model.report !== 'request-windows' ? <ReliabilityCoverage model={model} /> : <>
    {!blocked && <ReliabilityFilters onApply={model.apply} query={query} />}
    <p className="metric-explanation reliability-semantics">5xx responses / recorded requests is a ratio from 0 to 1, not a percentage or availability score.
      4xx responses remain in the request denominator. Unrecorded requests and transport failures are not represented.
      Each window is [start, start + 5 minutes). Missing reference metadata does not remove recorded requests.</p>
    {query && !blocked && <section className="reliability-applied" aria-label="Applied Reliability filters"><p>
      Window starts from <strong>{query.start}</strong> inclusive to <strong>{query.end}</strong> exclusive.</p>
      <p>Service: <strong>{query.serviceId ?? 'All services'}</strong> · Environment: <strong>{query.environment ?? 'All environments'}</strong></p>
      <button type="button" className="secondary-button" onClick={model.retry}>Refresh page</button></section>}
    {query && !blocked && <div className="pagination-bar" aria-label="Request window pagination"><div><strong>Window page</strong><p>
      {response ? response.data.length ? `Rows ${formatCount(offset + 1)}–${formatCount(offset + response.meta.returned)} in this scope` : `No rows at offset ${formatCount(offset)}`
        : `Requested offset ${formatCount(offset)}`}<span>{limit} rows per request · No total supplied</span></p></div>
      <div className="pagination-controls"><button type="button" className="secondary-button" disabled={loading || offset === 0}
        onClick={() => model.movePage(Math.max(0, offset - limit))}>Previous windows</button>
        <button type="button" className="secondary-button" disabled={loading || !response?.meta.hasMore || nextOffset > RELIABILITY_MAX_OFFSET}
          onClick={() => model.movePage(nextOffset)}>Next windows</button></div></div>}
    <div aria-busy={loading}>
      {loading && <section className="state-panel loading-state" role="status"><span className="loading-indicator" aria-hidden="true" />
        <h2>Loading request windows</h2><p>Waiting for the API to verify the Reliability publication.</p></section>}
      {current?.status === 'error' && <ErrorState error={current.error} onRetry={model.retry} label="Reliability request windows" />}
      {!query && <section className="state-panel"><h2>Choose a UTC window interval</h2><p>Apply the filters to read one bounded page. No calendar-day or current-time range is assumed.</p></section>}
      {response && <>
        <p role="status" className="visually-hidden">{response.meta.returned} request windows returned at offset {offset}.</p>
        {response.meta.hasMore && <p className="scope-notice">More request windows are available.
          {nextOffset > RELIABILITY_MAX_OFFSET ? ' The API maximum offset has been reached. Narrow the interval or service filters to continue.' : ' Use Next windows to request another page.'}</p>}
        {response.data.length ? <ReliabilityTable response={response} /> : <section className="state-panel" role="status"><h2>No request windows in this scope</h2>
          <p>The API returned no matching groups. No zero request counts, rates or durations are inferred.</p></section>}
        <ReliabilityProvenance response={response} />
      </>}
    </div>
    </>}
  </section>;
}
