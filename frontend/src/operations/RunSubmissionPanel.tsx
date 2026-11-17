import { useEffect, useRef, useState } from 'react';
import { validateRunSubmission, RunSubmissionError, sendRunSubmission } from '../managementApi';
import { runHref } from '../navigation';
import { ManagementErrorDetails } from './ManagementErrorDetails';
import { confirmedRunIntent, readRunIntent, saveRunIntent, useRunIntent } from './runIntent';
import type { RunIntent } from './runIntent';

function explanation(status: number | null) {
  switch (status) {
    case 400: return 'The selection or configuration was rejected. Refresh discovery and deliberately review the compatible registered choices.';
    case 403: return 'Local access was rejected. Check the approved UI origin and API host.';
    case 404: return 'The management submission endpoint was not found. Check the API-mode service.';
    case 409: return 'The source, artifact or idempotency binding conflicts with this request. Refresh and review selections before creating a different intent.';
    case 429: return 'Management capacity is exhausted. No automatic retry will be submitted.';
    case 503: return 'The service or coordinator is unavailable. Resolve this original request when the service is ready.';
    default: return 'The response did not confirm admission. The request may have committed. Resolve it using the original request.';
  }
}

/** Mounted once above routes: navigation never replaces or silently forgets a pending mutation. */
export function RunSubmissionPanel({ visible }: { visible: boolean }) {
  const { intent, unavailable } = useRunIntent();
  const [busy, setBusy] = useState(false), [storageError, setStorageError] = useState(false);
  const [acknowledgedKey, setAcknowledgedKey] = useState<string | null>(null);
  const controller = useRef<AbortController | null>(null), heading = useRef<HTMLHeadingElement>(null);
  const key = intent?.command.key;
  const selection = intent ? validateRunSubmission(intent.command) : null;
  const content = selection?.pipeline === 'CONTENT_RSS_V1';
  useEffect(() => { if (visible && key) heading.current?.focus(); }, [key, visible]);
  useEffect(() => () => {
    controller.current?.abort(); controller.current = null;
    try { const old = readRunIntent(); if (old?.phase === 'pending') saveRunIntent({ ...old, phase: 'uncertain' }); }
    catch { /* Storage failure disables new intents on the next mount. */ }
  }, []);
  function retain(value: RunIntent | null) {
    try { saveRunIntent(value); setStorageError(false); return true; }
    catch { setStorageError(true); return false; }
  }
  async function submit() {
    if (controller.current || !intent || !['review', 'uncertain', 'pending'].includes(intent.phase)) return;
    if (content && intent.phase === 'review' && acknowledgedKey !== intent.command.key) return;
    const original = intent, abort = new AbortController(); controller.current = abort; setBusy(true);
    const current = () => { try { const saved = readRunIntent(); return !abort.signal.aborted && saved?.command.key === original.command.key && saved.command.body === original.command.body; } catch { return false; } };
    try {
      if (!retain({ version: 1, command: original.command, phase: 'pending' })) return;
      const response = await sendRunSubmission(original.command, abort.signal);
      if (current()) retain(confirmedRunIntent(original, response));
    } catch (caught) {
      if (!current()) return;
      const safe = caught instanceof RunSubmissionError ? caught : new RunSubmissionError(null, null, null, null, true);
      retain({ version: 1, command: original.command, phase: safe.ambiguous ? 'uncertain' : 'rejected', error: {
        status: safe.status, code: safe.code, correlationId: safe.correlationId, retryable: safe.retryable, ambiguous: safe.ambiguous,
      } });
    } finally { if (controller.current === abort) { controller.current = null; setBusy(false); } }
  }
  if (!visible) return null;
  if (unavailable) return <div className="state-panel error-state" role="alert"><h2>Run submission unavailable</h2>
    <p>Tab storage is unavailable or contains an invalid unresolved run intent. Preserve it for operator inspection. New run submissions are disabled.</p></div>;
  if (!intent || !selection) return null;
  const { command, phase, error, receipt } = intent;
  return <section className="panel source-action-review" aria-labelledby="run-review-title" aria-busy={busy}>
    <h2 id="run-review-title" ref={heading} tabIndex={-1}>{phase === 'review' ? `Review ${content ? 'Content' : 'Reliability'} run` : `${content ? 'Content' : 'Reliability'} submission receipt`}</h2>
    <p>Pipeline: <strong>{selection.pipeline}</strong> · Product: {content ? 'Content Intelligence' : 'Application Reliability'}.</p>
    {content ? <aside className="scope-notice" aria-label="Content queue limitations">
      <p>Public Content execution has not completed operational qualification. Queue admission does not start or guarantee processing.</p>
      <p>This run may remain queued indefinitely and consumes shared unfinished-run capacity (20 total). A separately approved compatible worker could claim it in the future.</p>
      <p>Successful processing does not automatically change the Analytics publication selected for reading. The backend remains authoritative for admission, capacity, validation and execution controls.</p>
      <p>Discovery qualification: REGISTRATION_METADATA_ONLY · Runtime qualification: NOT_CHECKED · Dispatch disabled: true. Registration does not prove installed-runtime readiness or that a worker is operating.</p>
    </aside> : <p>Review all immutable identifiers and source revisions below. Admission does not start a worker or activate Analytics results. Subsequent processing depends on the configured Reliability worker.</p>}
    <section aria-label="Frozen selection summary">
      <h3>Frozen selection</h3>
      <dl className="source-metadata">
        <div><dt>Execution profile</dt><dd><code>{selection.executionProfileId}</code></dd></div>
        {selection.pipeline === 'RELIABILITY_V2' ? <div><dt>Selected input batches</dt><dd>{selection.batchArtifactIds.length}</dd></div>
          : <div><dt>Retained Silver artifacts</dt><dd>0</dd></div>}
        <div><dt>Captured source revisions</dt><dd><ul className="inspection-references">{(selection.pipeline === 'CONTENT_RSS_V1' ? selection.fetchSources : selection.sources).map((source) => <li key={source.sourceId}>
          <code>{source.sourceId}</code> · revision {source.revision}</li>)}</ul></dd></div>
        {selection.pipeline === 'CONTENT_RSS_V1' ? <div><dt>Publisher-reference artifact</dt><dd><code>{selection.publisherReferenceArtifactId}</code></dd></div>
          : <div><dt>Service-reference artifact</dt><dd><code>{selection.serviceReferenceArtifactId}</code></dd></div>}
      </dl>
      {selection.pipeline === 'RELIABILITY_V2' && <details><summary>Selected batch artifact IDs</summary><ul className="inspection-references">{selection.batchArtifactIds.map((id) => <li key={id}><code>{id}</code></li>)}</ul></details>}
    </section>
    <p>Idempotency key: <code>{command.key}</code></p>
    <details><summary>Complete frozen submission</summary><pre className="source-request-body">{JSON.stringify(JSON.parse(command.body), null, 2)}</pre></details>
    {error && <div role="alert"><p>{explanation(error.status)}</p><ManagementErrorDetails error={error} mutation /></div>}
    {(phase === 'uncertain' || phase === 'pending' && !busy) && <p role="alert">Outcome unknown. The original key and exact body are retained in this tab. Resolve the same request before starting a different intent. No automatic replay occurs.</p>}
    {phase === 'pending' && busy && <p role="status">Admission request pending.</p>}
    {receipt && <div role="status"><p>Admission confirmed for <code>{receipt.runId}</code>. Persisted state at receipt: <strong>{receipt.state}</strong>.</p>
      <p>{receipt.state === 'QUEUED' ? 'QUEUED means accepted for admission, not actively processing.' : 'An exact replay can return an advanced or terminal persisted run.'} Inspect the run for its current state.</p>
      <p>Run ETag: <code>{receipt.etag}</code> · Reference: <code>{receipt.correlationId}</code></p>
      <a className="primary-button" href={runHref(receipt.runId)}>Open admitted run</a></div>}
    {storageError && <p role="alert">Tab storage could not be updated. Preserve the original request in this tab; do not start another intent.</p>}
    {content && phase === 'review' && <label className="scope-notice"><input type="checkbox" checked={acknowledgedKey === command.key} disabled={busy}
      onChange={(event) => setAcknowledgedKey(event.target.checked ? command.key : null)} />
      I acknowledge the Content queue limitations above and deliberately submit this unqualified Content request for queue admission.</label>}
    <div className="source-action-buttons">
      {phase === 'review' && <><button type="button" className="primary-button" disabled={busy || content && acknowledgedKey !== command.key} onClick={() => void submit()}>Confirm {content ? 'Content' : 'Reliability'} submission</button>
        <button type="button" className="secondary-button" disabled={busy} onClick={() => retain(null)}>Cancel run review</button></>}
      {(phase === 'pending' || phase === 'uncertain') && <button type="button" className="primary-button" disabled={busy} onClick={() => void submit()}>{busy ? 'Request pending' : 'Resolve same run request'}</button>}
      {phase === 'rejected' && <button type="button" className="secondary-button" onClick={() => retain(null)}>Return to selection and deliberate review</button>}
      {phase === 'confirmed' && <button type="button" className="secondary-button" onClick={() => retain(null)}>Dismiss confirmed receipt</button>}
    </div>
  </section>;
}
