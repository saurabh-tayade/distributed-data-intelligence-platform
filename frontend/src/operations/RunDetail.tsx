import { useState } from 'react';
import { canRetryRun, reviewRetryRun, useRetryIntent } from './retryIntent';
import { canCancelRun, reviewCancelRun, useCancelIntent } from './cancelIntent';
import type { ControlReceipt, LatestControl, RunState } from '../managementApi';
import type { ExecutionFilters } from '../navigation';
import { executionsHref } from '../navigation';
import { Timestamp } from '../components/Timestamp';
import { productLabel } from './SourceList';
import { InspectionError } from './InspectionError';
import { PublicationResult } from './PublicationResult';
import { AttemptHistory, AuditTimeline } from './RunHistoryPanels';
import { useControl } from './useControl';
import { useRunDetail } from './useRunDetail';

const stateGuidance: Record<RunState, { title: string; next: string }> = {
  QUEUED: { title: 'Waiting for execution', next: 'Admission is recorded. A compatible configured worker must claim the run before processing starts. Inspect the current stage and attempts for recorded execution.' },
  RUNNING: { title: 'Execution in progress', next: 'Inspect the current stage, attempts and recorded counts. Run reads refresh while visible; refresh stages and audit history separately.' },
  CANCEL_REQUESTED: { title: 'Cancellation requested', next: 'The cancellation intent is recorded; cancellation is not yet confirmed. Inspect the latest control receipt and run outcome. Independent operator recovery may be required.' },
  SUCCEEDED: { title: 'Run completed', next: 'Inspect the recorded publication and captured inputs below. Verification was performed at execution time; the selected Analytics publication is independent.' },
  FAILED: { title: 'Run failed', next: 'Inspect the outcome, attempts and audit events before deciding what to do next. Retry requires recorded eligibility, explicit review and separate operator commands; it is not automatic.' },
  CANCELLED: { title: 'Cancellation recorded', next: 'The persisted run records cancellation. Inspect its control receipt and history; cancellation does not remove retained inputs, outputs or earlier attempts.' },
  PAUSED: { title: 'Paused at an operator boundary', next: 'Inspect the recorded checkpoint and history with the local operator. This screen does not resume the run or grant recovery authority.' },
  RECOVERY_REQUIRED: { title: 'Operator recovery required', next: 'Preserve the installation and retained evidence. The local operator must follow the documented guarded recovery procedure; this screen does not clear ownership or retry the run.' },
};

function ControlFields({ control }: { control: LatestControl | ControlReceipt }) {
  return <dl className="source-metadata">
    <div><dt>Control ID</dt><dd><code>{control.controlId}</code></dd></div>
    <div><dt>Control type</dt><dd>{control.type}</dd></div><div><dt>Control state</dt><dd>{control.state}</dd></div>
    <div><dt>Reason</dt><dd><code>{control.reason ?? 'Not recorded'}</code></dd></div>
    <div><dt>Requested (UTC)</dt><dd><Timestamp value={control.requestedAt} /></dd></div>
    <div><dt>Applied (UTC)</dt><dd><Timestamp value={control.appliedAt} /></dd></div>
    <div><dt>Control row version</dt><dd><code>{control.rowVersion}</code></dd></div>
    {'runVersion' in control && <div><dt>Run version observed by receipt read</dt><dd><code>{control.runVersion}</code></dd></div>}
  </dl>;
}
function ControlPanel({ runId, controlId }: { runId: string; controlId: string }) {
  const model = useControl(runId, controlId);
  return <>
    <button className="secondary-button" type="button" disabled={model.current === null} onClick={model.refresh}>Refresh control receipt</button>
    {model.current === null && <p role="status" className="inspection-note">Loading control receipt</p>}
    {model.current?.error && <InspectionError title="Unable to load control receipt" error={model.current.error} retry={model.refresh} />}
    {model.current?.data && <ControlFields control={model.current.data.data} />}
  </>;
}
export function RunDetail({ runId, returnQuery }: { runId: string; returnQuery: ExecutionFilters }) {
  const model = useRunDetail(runId), record = model.data?.data ?? null;
  const retry = useRetryIntent(), [retryStorageError, setRetryStorageError] = useState(false);
  const cancellation = useCancelIntent(), [storageError, setStorageError] = useState(false);
  return <section aria-label="Run inspection">
    <a className="source-back" href={executionsHref(returnQuery)}>Back to Executions</a>
    <div className="page-heading"><div><p className="eyebrow">OPERATIONS WORKSPACE <span aria-hidden="true">/</span> RUN INSPECTION</p>
      <h1>Run detail<span className="title-period">.</span></h1><code className="inspection-id">{runId}</code>
    </div><button className="secondary-button" type="button" disabled={model.refreshing || model.hidden} onClick={model.refresh}>Refresh run detail</button></div>
    <p className="scope-notice">Queued-run cancellation requires explicit review. Retry eligibility does not authorize RETRY. Historical verification does not requalify an artifact or activate Analytics. Ordinary Content dispatch remains disabled.</p>
    <section className="panel source-detail-panel" aria-label="Run record" aria-busy={model.refreshing}>
      <h2>Persisted run record</h2>
      {model.hidden && <p role="status" className="inspection-note">Run reads are suspended while the document is hidden.</p>}
      {model.refreshing && <p role="status" className="inspection-note">{record ? 'Refreshing run detail' : 'Loading run detail'}</p>}
      {model.lastUpdated && <p className="inspection-note">Last successful read (UTC): <Timestamp value={model.lastUpdated} /></p>}
      {model.error && <>
        {record && <p className="scope-notice" role="status">Stale run record: the latest read failed. Values below are from the last successful read.</p>}
        <InspectionError title={record ? 'Unable to refresh run detail' : 'Unable to load run detail'} error={model.error} retry={model.refresh} disabled={model.refreshing || model.hidden} />
      </>}
      {model.nextPollMs !== null && !model.hidden && <p className="inspection-note" role="status">{model.error ? 'Automatic read retry' : 'Next active-run read'} scheduled {model.nextPollMs / 1000} seconds after the previous read settled.</p>}
      {record && <>
        <div className="operator-run-summary">
          <h3>{model.error ? 'Last recorded state: ' : ''}{stateGuidance[record.state].title}</h3>
          <p>Outcome code: <code>{record.outcomeCode ?? 'Not recorded'}</code></p>
          <p>{stateGuidance[record.state].next}</p>
        </div>
        {['PAUSED', 'RECOVERY_REQUIRED'].includes(record.state) && <p className="scope-notice" role="status">{record.state === 'PAUSED' ? 'Paused at an operator boundary; this is not cancellation.' : 'Recovery is required; this is distinct from a processing failure.'} Operator attention is required. Refresh manually; no automatic recovery is performed.</p>}
        <dl className="source-metadata">
          <div><dt>Run ID</dt><dd><code>{record.runId}</code></dd></div><div><dt>Product</dt><dd>{productLabel(record.productId)}</dd></div>
          <div><dt>Pipeline</dt><dd><code>{record.pipeline}</code></dd></div>{record.dataOrigin && <div><dt>Data origin</dt><dd>{record.dataOrigin}</dd></div>}
          <div><dt>Run state</dt><dd>{record.state}</dd></div><div><dt>Current stage</dt><dd>{record.currentStage ?? 'Not recorded'}</dd></div>
          <div><dt>Run row version</dt><dd><code>{record.rowVersion}</code></dd></div>
          <div><dt>Requested (UTC)</dt><dd><Timestamp value={record.requestedAt} /></dd></div><div><dt>Finished (UTC)</dt><dd><Timestamp value={record.finishedAt} /></dd></div>
          <div><dt>Retry eligibility (informational)</dt><dd>{String(record.retryEligibility)}</dd></div>
          <div><dt>Attempt count</dt><dd><code>{record.attempts.count}</code></dd></div>
          <div><dt>Current attempt ID</dt><dd><code>{record.attempts.currentAttemptId ?? 'Not recorded'}</code></dd></div>
        </dl>
        <h3>Safe counts</h3>{record.counts === null || Object.keys(record.counts).length === 0 ? <p className="inspection-note">No counts recorded.</p>
          : <dl className="source-metadata">{Object.entries(record.counts).map(([name, count]) => <div key={name}><dt>{name}</dt><dd><code>{count}</code></dd></div>)}</dl>}
        <h3>Publication result at execution</h3><PublicationResult result={record.result} />
        <h3>Captured references</h3><p className="inspection-note">These captured revisions are immutable selections, not current source configurations. Artifact roles and full frozen selection metadata are not exposed by this API.</p>
        <h4>Selected source revisions</h4>{record.sourceRevisions.length === 0 ? <p className="inspection-note">No source references returned.</p>
          : <ul className="inspection-references">{record.sourceRevisions.map((source) => <li key={source.sourceId}><code>{source.sourceId}</code> · Captured revision {source.revision}</li>)}</ul>}
        <h4>Input artifact IDs</h4>{record.inputArtifactIds.length === 0 ? <p className="inspection-note">No input artifact references returned.</p>
          : <ul className="inspection-references">{record.inputArtifactIds.map((id, index) => <li key={`${index}:${id}`}><code>{id}</code></li>)}</ul>}
        {record.latestControl && <><h4>Latest control reference in displayed run record</h4><ControlFields control={record.latestControl} /></>}
      </>}
    </section>
    {record?.productId === 'application-reliability' && <aside className="panel operator-guide" aria-label="Reliability analytics handoff">
      <h2>Explore the selected Reliability analytics</h2>
      <p>A successful run does not select its result in Analytics. The local operator selects an exact publication at API startup; reloading this page does not change that selection.</p>
      <p><a className="secondary-button" href="#/analytics/reliability">Open independently selected Reliability analytics</a></p>
      <p>Compare the Analytics publication fingerprint with the recorded result before attributing analytical rows to this run. Gold and enriched Content have their own independent selections.</p>
    </aside>}
    <section className="panel" aria-label="Run cancellation"><h2>Run cancellation</h2>
      <p>Cancel never-started QUEUED runs or an admitted queued Reliability retry, or request a bounded stop of a running Reliability attempt. Displayed retry receipts are review hints; the server validates the exact current lifecycle. Queued cancellation prevents the next execution without erasing history. Running cancellation may require recovery and verified publication can still succeed. Content running cancellation and Content retry cancellation remain disabled.</p>
      <button type="button" disabled={!model.data || !canCancelRun(model.data) || model.refreshing || model.hidden || !!model.error || cancellation.unavailable || !!cancellation.intent} onClick={() => {
        try { if (model.data) reviewCancelRun(model.data); } catch { setStorageError(true); }
      }}>Review {record?.state === 'RUNNING' ? 'running' : 'queued'}-run cancellation</button>
      {storageError && <p role="alert">Unable to retain cancellation review. No request was sent.</p>}
    </section>
    <section className="panel" aria-label="Guarded retry"><h2>Guarded Reliability retry</h2>
      <p>A historical eligibility flag allows review, not execution. The server admits intent; a separate local operator command freshly qualifies immutable dependencies and the absent publication namespace before requeueing. Content retry remains disabled.</p>
      <button type="button" disabled={!model.data || !canRetryRun(model.data) || model.refreshing || model.hidden || !!model.error || retry.unavailable || !!retry.intent} onClick={() => {
        try { if (model.data) reviewRetryRun(model.data); } catch { setRetryStorageError(true); }
      }}>Review guarded retry</button>
      {retryStorageError && <p role="alert">Unable to retain retry review. No request was sent.</p>}
    </section>
    <AttemptHistory runId={runId} /><AuditTimeline runId={runId} />
    <section className="panel source-detail-panel" aria-label="Latest control receipt"><h2>Latest control receipt</h2>
      <p className="inspection-note">REQUESTED records intent, not completed recovery. APPLIED, TOO_LATE and REJECTED are control receipt states, not run outcomes. The receipt is a separate read and may differ from the displayed run reference. Refresh this receipt explicitly; a newer control reference from run refresh triggers a new lookup.</p>
      {record === null ? <p className="inspection-note">A successful run read is needed to discover the latest control reference.</p>
        : record.latestControl === null ? <p className="inspection-note">No latest control reference returned.</p>
          : <ControlPanel key={`${record.latestControl.controlId}:${record.latestControl.rowVersion}`} runId={runId} controlId={record.latestControl.controlId} />}
    </section>
  </section>;
}
