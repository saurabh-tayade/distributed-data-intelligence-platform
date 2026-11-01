import { useState } from 'react';
import { PublicationResult } from './PublicationResult';
import { Timestamp } from '../components/Timestamp';
import { isSourceId, RUN_STATES } from '../managementApi';
import type { ManagementErrorKind, ProductId, RunState } from '../managementApi';
import { executionsHref, runHref } from '../navigation';
import type { ExecutionFilters } from '../navigation';
import { ManagementErrorDetails } from './ManagementErrorDetails';
import { productLabel } from './SourceList';
import { useRuns } from './useRuns';

const messages: Record<ManagementErrorKind, string> = {
  'invalid-request': 'The execution request or cursor was not accepted. Review the filters or start from the first page. Cursors expire after 24 hours.',
  'local-boundary': 'Local access was rejected. Check the approved UI origin and API host.',
  'not-found': 'Execution history is unavailable at this address. Check that the local service is running in API mode.',
  capacity: 'The management service is at capacity. Try again when capacity is available.',
  unavailable: 'Execution history is unavailable. Check the local management service, then try again.',
  server: 'The management service could not complete the history request. Check the service before trying again.',
  network: 'Cannot reach the local management service. Check the API and development proxy, then try again.',
  'invalid-response': 'The API returned data that does not match the execution history contract. No execution records are displayed.',
};
const draftFrom = (query: ExecutionFilters) => ({ productId: query.productId ?? '', sourceId: query.sourceId ?? '', state: query.state ?? '', limit: query.limit ?? 20 });

export function RunList({ query, active = true }: { query: ExecutionFilters; active?: boolean }) {
  const model = useRuns(query, active);
  const [draft, setDraft] = useState(draftFrom(query));
  const [sourceInvalid, setSourceInvalid] = useState(false);
  const page = model.current?.status === 'ready' ? model.current.page : null;
  const error = model.current?.status === 'error' ? model.current.error : null;
  function apply(filters: ExecutionFilters) {
    const href = executionsHref(filters);
    if (href === window.location.hash) model.refresh(true);
    else window.location.hash = href;
  }
  return <section aria-label="Operations executions">
    <div className="page-heading"><div>
      <p className="eyebrow">OPERATIONS WORKSPACE <span aria-hidden="true">/</span> EXECUTION HISTORY</p>
      <h1>Executions<span className="title-period">.</span></h1>
      <p className="page-description">Persisted execution records in newest-submission-first order.</p>
    </div><span className="row-badge">Read-only</span></div>
    <p className="scope-notice">Historical Content runs can appear here. Ordinary Content worker dispatch remains disabled. This view does not submit runs or recovery requests.</p>
    <form className="source-filters execution-filters" aria-label="Execution filters" noValidate onSubmit={(event) => {
      event.preventDefault();
      if (draft.sourceId !== '' && !isSourceId(draft.sourceId)) { setSourceInvalid(true); return; }
      setSourceInvalid(false);
      apply({ limit: draft.limit, ...(draft.productId ? { productId: draft.productId as ProductId } : {}),
        ...(draft.sourceId ? { sourceId: draft.sourceId } : {}), ...(draft.state ? { state: draft.state as RunState } : {}) });
    }}>
      <label>Product<select value={draft.productId} onChange={(event) => setDraft({ ...draft, productId: event.target.value })}>
        <option value="">All products</option><option value="content-intelligence">Content Intelligence</option><option value="application-reliability">Application Reliability</option>
      </select></label>
      <label>Exact source ID<input type="text" value={draft.sourceId} autoCapitalize="none" autoCorrect="off" spellCheck={false}
        aria-invalid={sourceInvalid} aria-describedby="execution-source-help" onChange={(event) => { setDraft({ ...draft, sourceId: event.target.value }); setSourceInvalid(false); }} /></label>
      <label>Run state<select value={draft.state} onChange={(event) => setDraft({ ...draft, state: event.target.value })}>
        <option value="">All states</option>{RUN_STATES.map((state) => <option key={state} value={state}>{state}</option>)}
      </select></label>
      <label>Executions per page<select value={draft.limit} onChange={(event) => setDraft({ ...draft, limit: Number(event.target.value) })}>
        {![20, 50, 100].includes(draft.limit) && <option value={draft.limit}>{draft.limit}</option>}
        <option value="20">20</option><option value="50">50</option><option value="100">100</option>
      </select></label>
      <button className="primary-button" type="submit">Apply filters</button>
      <button className="secondary-button" type="button" onClick={() => { setDraft(draftFrom({})); setSourceInvalid(false); apply({}); }}>Reset filters</button>
    </form>
    <p id="execution-source-help" className={`execution-filter-note${sourceInvalid ? ' field-error' : ''}`} role={sourceInvalid ? 'alert' : undefined}>
      {sourceInvalid ? 'Enter an exact source ID: 1–128 lowercase letters, digits, underscores or hyphens, starting with a letter or digit. Whitespace is not removed.'
        : 'Apply filters to start a new traversal. Source ID matches selected run sources, not complete retained-artifact provenance.'}
    </p>
    <div className="execution-query"><p>Applied: {query.productId ? productLabel(query.productId) : 'All products'} · Source: {query.sourceId ?? 'All sources'} · State: {query.state ?? 'All states'}</p>
      <button className="secondary-button" type="button" onClick={() => model.refresh(true)}>Refresh executions</button></div>
    <details className="execution-guide"><summary>How to interpret execution records</summary>
      <p>QUEUED means waiting, not active processing. PAUSED is not cancelled. RECOVERY_REQUIRED is distinct from FAILED.</p>
      <p>Retry eligibility is recorded metadata, not permission to retry. An absent result is not itself a failure.</p>
      <p>VERIFIED_AT_EXECUTION records historical verification. It does not establish current verification or activate an Analytics publication.</p>
    </details>
    <div aria-busy={model.current === null}>
      {model.current === null && <section className="state-panel" role="status" aria-live="polite">
        <span className="loading-indicator" aria-hidden="true" /><h2>Loading executions</h2><p>Reading one page of persisted execution history.</p>
      </section>}
      {error && <section className="state-panel error-state" role="alert">
        <h2>Unable to load executions</h2><p>{messages[error.kind]}</p><ManagementErrorDetails error={error} />
        <button className="primary-button" type="button" onClick={() => model.refresh()}>Try again</button>
        {model.hasCursor && error.kind === 'invalid-request' && <button className="secondary-button" type="button" onClick={() => model.refresh(true)}>Start from first execution page</button>}
      </section>}
      {page && page.data.length === 0 && <section className="state-panel" role="status">
        <h2>{query.productId || query.sourceId || query.state ? 'No executions match these filters' : 'No executions returned'}</h2>
        <p>The API returned an empty history page. Reset filters or refresh to start a new traversal.</p>
      </section>}
      {page && page.data.length > 0 && <section className="panel table-panel" aria-labelledby="executions-title">
        <div className="panel-heading"><h2 id="executions-title">Execution history</h2><span className="row-badge">{page.data.length} returned on this page</span></div>
        {/* eslint-disable-next-line jsx-a11y/no-noninteractive-tabindex -- Keyboard users must be able to scroll the table. */}
        <div className="table-scroll" role="region" aria-label="Execution history table; scroll horizontally on small screens" tabIndex={0}>
          <table className="execution-table"><caption className="visually-hidden">Persisted run summaries in server submission order, newest first. Times are UTC. Run identifiers open read-only run inspection.</caption>
            <thead><tr><th scope="col">Execution</th><th scope="col">Run state / current stage</th><th scope="col">Persisted outcome</th>
              <th scope="col">Requested (UTC)</th><th scope="col">Finished (UTC)</th><th scope="col">Result at execution</th></tr></thead>
            <tbody>{page.data.map((run) => <tr key={run.runId}>
              <th scope="row"><a href={runHref(run.runId, query)}><code className="run-id">{run.runId}</code></a><span className="run-metadata">{productLabel(run.productId)}</span>
                <code className="run-metadata">{run.pipeline}</code>{run.dataOrigin && <span className="run-metadata">Synthetic data origin</span>}
                <span className="run-metadata">Row version: <code>{run.rowVersion}</code></span></th>
              <td><span className="run-state">{run.state}</span><span className="run-metadata">Current stage: {run.currentStage ?? 'Not recorded'}</span></td>
              <td><code>{run.outcomeCode ?? 'No outcome recorded'}</code><span className="run-metadata">Retry eligibility (recorded): {run.retryEligibility ? 'true' : 'false'}</span></td>
              <td><Timestamp value={run.requestedAt} /></td><td><Timestamp value={run.finishedAt} /></td>
              <td><PublicationResult result={run.result} /></td>
            </tr>)}</tbody>
          </table>
        </div>
        <p className="source-table-note">One history page, not a complete run inventory. Mutable run fields can change during a high-water traversal; refresh explicitly for new membership and current values.</p>
      </section>}
    </div>
    <nav className="source-pagination" aria-label="Execution pages">
      <button className="secondary-button" type="button" disabled={model.current === null || !model.canGoBack} onClick={model.back}>Previous executions</button>
      <p role="status" aria-live="polite">Execution page {model.pageNumber}{page ? ` · ${page.data.length} returned${page.meta.hasMore ? ' · More executions available' : ' · End of results'}` : ''}</p>
      <button className="secondary-button" type="button" disabled={page === null || !page.meta.hasMore} onClick={model.next}>Next executions</button>
    </nav>
    {model.historyTruncated && <p className="execution-filter-note">Only the latest 20 cursor positions are retained. Refresh executions to restart from the first page.</p>}
  </section>;
}
