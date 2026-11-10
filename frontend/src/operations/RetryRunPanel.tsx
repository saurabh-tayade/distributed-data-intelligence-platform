import { useEffect, useRef, useState } from 'react';
import { RunSubmissionError, sendRetryCommand } from '../managementApi';
import { runHref } from '../navigation';
import { ManagementErrorDetails } from './ManagementErrorDetails';
import { readRetryIntent, saveRetryIntent, useRetryIntent } from './retryIntent';
import type { RetryIntent } from './retryIntent';
import { useRunDetail } from './useRunDetail';
function FreshRun({ runId }: { runId: string }) {
  const model = useRunDetail(runId);
  return <div><p>{model.data ? `Fresh run read: ${model.data.data.state} (version ${model.data.data.rowVersion}).` : 'Reading current persisted run.'}</p>
    {model.error && <p role="alert">Current run read failed. Any displayed run values are stale. The persisted control receipt is retained.</p>}
    <button type="button" disabled={model.refreshing || model.hidden} onClick={model.refresh}>Refresh retry run</button></div>;
}
/** Mounted once above routes: navigation never replaces or silently forgets a pending mutation. */
export function RetryRunPanel({ visible }: { visible: boolean }) {
  const { intent, unavailable } = useRetryIntent();
  const [busy, setBusy] = useState(false), [storageError, setStorageError] = useState(false);
  const controller = useRef<AbortController | null>(null), heading = useRef<HTMLHeadingElement>(null);
  const key = intent?.command.key;
  useEffect(() => { if (visible && key) heading.current?.focus(); }, [key, visible]);
  useEffect(() => () => {
    controller.current?.abort(); controller.current = null;
    try { const old = readRetryIntent(); if (old?.phase === 'pending') saveRetryIntent({ ...old, phase: 'uncertain' }); }
    catch { /* Storage failure disables new intents on the next mount. */ }
  }, []);
  function retain(value: RetryIntent | null) {
    try { saveRetryIntent(value); setStorageError(false); return true; }
    catch { setStorageError(true); return false; }
  }
  async function submit() {
    if (controller.current || !intent || !['review', 'uncertain', 'pending'].includes(intent.phase)) return;
    const original = intent, abort = new AbortController(); controller.current = abort; setBusy(true);
    const current = () => { try { return !abort.signal.aborted && readRetryIntent()?.command.key === original.command.key; } catch { return false; } };
    try {
      if (!retain({ version: 1, command: original.command, phase: 'pending' })) return;
      const response = await sendRetryCommand(original.command, abort.signal);
      if (current()) retain({ version: 1, command: original.command, phase: 'confirmed', receipt: response });
    } catch (caught) {
      if (!current()) return;
      const safe = caught instanceof RunSubmissionError ? caught : new RunSubmissionError(null, null, null, null, true);
      retain({ version: 1, command: original.command, phase: safe.ambiguous ? 'uncertain' : 'rejected', error: {
        status: safe.status, code: safe.code, correlationId: safe.correlationId, retryable: safe.retryable, ambiguous: safe.ambiguous,
      } });
    } finally { if (controller.current === abort) { controller.current = null; setBusy(false); } }
  }
  if (!visible) return null;
  if (unavailable) return <div className="state-panel error-state" role="alert"><h2>Run retry unavailable</h2>
    <p>Tab storage is unavailable or contains an invalid unresolved retry intent. Preserve it for operator inspection. New retries are disabled.</p></div>;
  if (!intent) return null;
  const { command, phase, error, receipt } = intent;
  return <section className="panel source-action-review" aria-labelledby="retry-review-title" aria-busy={busy}>
    <h2 id="retry-review-title" ref={heading} tabIndex={-1}>{phase === 'review' ? 'Review guarded Reliability retry' : 'Retry receipt'}</h2>
    <p>Request retry of Reliability run <code>{command.runId}</code>. The server checks the captured run ETag <code>{command.ifMatch}</code>.</p>
    <p>Admission records intent only. A separate local operator command must freshly verify immutable inputs and an absent output namespace under the installation guard before requeueing. Historical retry eligibility is not launch authority.</p>
    <p>No output is deleted or repaired. Content retry and cancellation of later attempts remain unsupported.</p>
    <p>Idempotency key: <code>{command.key}</code></p>
    <details open><summary>Complete frozen retry</summary><pre className="source-request-body">{JSON.stringify(JSON.parse(command.body), null, 2)}</pre></details>
    {error && <div role="alert"><p>Retry was not confirmed. Review the reported failure. No automatic retry occurs.</p><ManagementErrorDetails error={error} mutation /></div>}
    {(phase === 'uncertain' || phase === 'pending' && !busy) && <p role="alert">Outcome unknown. The original key, run ETag and exact body are retained in this tab. Resolve the same request before starting a different intent. No automatic replay occurs.</p>}
    {phase === 'pending' && busy && <p role="status">Retry request pending.</p>}
    {receipt && <div role="status"><p>Persisted control <code>{receipt.data.controlId}</code>: <strong>{receipt.data.state}</strong>. A control receipt is not the current run outcome.</p>
      {receipt.data.state === 'REQUESTED' && <p>Retry intent accepted, not requeued or running. Use the local guarded retry qualification command, then refresh the run and control receipt.</p>}
      {receipt.data.state === 'APPLIED' && <p>Guarded retry was queued. This does not establish that execution started or succeeded. Check the fresh run read for its persisted outcome.</p>}
      <p>Control ETag: <code>{receipt.etag}</code> · Reference: <code>{receipt.meta.correlationId}</code></p>
      <FreshRun runId={command.runId} />
      <a href={runHref(command.runId)}>Open run inspection</a></div>}
    {storageError && <p role="alert">Tab storage could not be updated. Preserve the original request in this tab; do not start another intent.</p>}
    <div className="source-action-buttons">
      {phase === 'review' && <><button type="button" className="primary-button" disabled={busy} onClick={() => void submit()}>Confirm guarded retry</button>
        <button type="button" className="secondary-button" disabled={busy} onClick={() => retain(null)}>Discard retry review</button></>}
      {(phase === 'pending' || phase === 'uncertain') && <button type="button" className="primary-button" disabled={busy} onClick={() => void submit()}>{busy ? 'Request pending' : 'Resolve same retry request'}</button>}
      {phase === 'rejected' && <button type="button" className="secondary-button" onClick={() => retain(null)}>Dismiss rejected retry</button>}
      {phase === 'confirmed' && <button type="button" className="secondary-button" onClick={() => retain(null)}>Dismiss confirmed receipt</button>}
    </div>
  </section>;
}
