import { useEffect, useRef, useState } from 'react';
import { RunSubmissionError, sendCancelCommand } from '../managementApi';
import { runHref } from '../navigation';
import { ManagementErrorDetails } from './ManagementErrorDetails';
import { readCancelIntent, saveCancelIntent, useCancelIntent } from './cancelIntent';
import type { CancelIntent } from './cancelIntent';
import { useRunDetail } from './useRunDetail';
function FreshRun({ runId }: { runId: string }) {
  const model = useRunDetail(runId);
  return <div><p>{model.data ? `Fresh run read: ${model.data.data.state} (version ${model.data.data.rowVersion}).` : 'Reading current persisted run.'}</p>
    {model.error && <p role="alert">Current run read failed. Any displayed run values are stale. The persisted control receipt is retained.</p>}
    <button type="button" disabled={model.refreshing || model.hidden} onClick={model.refresh}>Refresh cancellation run</button></div>;
}
/** Mounted once above routes: navigation never replaces or silently forgets a pending mutation. */
export function CancelRunPanel({ visible }: { visible: boolean }) {
  const { intent, unavailable } = useCancelIntent();
  const [busy, setBusy] = useState(false), [storageError, setStorageError] = useState(false);
  const controller = useRef<AbortController | null>(null), heading = useRef<HTMLHeadingElement>(null);
  const key = intent?.command.key;
  useEffect(() => { if (visible && key) heading.current?.focus(); }, [key, visible]);
  useEffect(() => () => {
    controller.current?.abort(); controller.current = null;
    try { const old = readCancelIntent(); if (old?.phase === 'pending') saveCancelIntent({ ...old, phase: 'uncertain' }); }
    catch { /* Storage failure disables new intents on the next mount. */ }
  }, []);
  function retain(value: CancelIntent | null) {
    try { saveCancelIntent(value); setStorageError(false); return true; }
    catch { setStorageError(true); return false; }
  }
  async function submit() {
    if (controller.current || !intent || !['review', 'uncertain', 'pending'].includes(intent.phase)) return;
    const original = intent, abort = new AbortController(); controller.current = abort; setBusy(true);
    const current = () => { try { return !abort.signal.aborted && readCancelIntent()?.command.key === original.command.key; } catch { return false; } };
    try {
      if (!retain({ version: 1, mode: original.mode ?? 'queued', command: original.command, phase: 'pending' })) return;
      const response = await sendCancelCommand(original.command, abort.signal);
      if (current()) retain({ version: 1, mode: original.mode ?? 'queued', command: original.command, phase: 'confirmed', receipt: response });
    } catch (caught) {
      if (!current()) return;
      const safe = caught instanceof RunSubmissionError ? caught : new RunSubmissionError(null, null, null, null, true);
      retain({ version: 1, mode: original.mode ?? 'queued', command: original.command, phase: safe.ambiguous ? 'uncertain' : 'rejected', error: {
        status: safe.status, code: safe.code, correlationId: safe.correlationId, retryable: safe.retryable, ambiguous: safe.ambiguous,
      } });
    } finally { if (controller.current === abort) { controller.current = null; setBusy(false); } }
  }
  if (!visible) return null;
  if (unavailable) return <div className="state-panel error-state" role="alert"><h2>Run cancellation unavailable</h2>
    <p>Tab storage is unavailable or contains an invalid unresolved cancellation intent. Preserve it for operator inspection. New cancellations are disabled.</p></div>;
  if (!intent) return null;
  const { command, phase, error, receipt } = intent;
  const running = intent.mode === 'running';
  return <section className="panel source-action-review" aria-labelledby="cancel-review-title" aria-busy={busy}>
    <h2 id="cancel-review-title" ref={heading} tabIndex={-1}>{phase === 'review' ? `Review ${running ? 'running' : 'queued'}-run cancellation` : 'Cancellation receipt'}</h2>
    <p>Request cancellation of {running ? 'running Reliability' : 'queued'} run <code>{command.runId}</code>. The server checks the captured run ETag <code>{command.ifMatch}</code>. RETRY remains disabled.</p>
    {running ? <p>Acceptance records a stop request, not a completed cancellation. Stopping work does not roll back a publication. Independent verification may report success and TOO_LATE, or require explicit recovery before an outcome is known.</p>
      : <p>Queued cancellation applies only if no attempt has started.</p>}
    <p>Idempotency key: <code>{command.key}</code></p>
    <details open><summary>Complete frozen cancellation</summary><pre className="source-request-body">{JSON.stringify(JSON.parse(command.body), null, 2)}</pre></details>
    {error && <div role="alert"><p>Cancellation was not confirmed. Review the reported failure. No automatic retry occurs.</p><ManagementErrorDetails error={error} mutation /></div>}
    {(phase === 'uncertain' || phase === 'pending' && !busy) && <p role="alert">Outcome unknown. The original key, run ETag and exact body are retained in this tab. Resolve the same request before starting a different intent. No automatic replay occurs.</p>}
    {phase === 'pending' && busy && <p role="status">Cancellation request pending.</p>}
    {receipt && <div role="status"><p>Persisted control <code>{receipt.data.controlId}</code>: <strong>{receipt.data.state}</strong>. A control receipt is not the current run outcome.</p>
      {receipt.data.state === 'REQUESTED' && <p>Stop request accepted; cancellation is not yet proven. Inspect and refresh the run and its latest control receipt for progress. Recovery may be required.</p>}
      {receipt.data.state === 'APPLIED' && <p>Cancellation was applied. Check the fresh run read for its persisted outcome.</p>}
      {receipt.data.state === 'TOO_LATE' && <p>Cancellation was too late: verified publication succeeded. No rollback is claimed.</p>}
      <p>Control ETag: <code>{receipt.etag}</code> · Reference: <code>{receipt.meta.correlationId}</code></p>
      <FreshRun runId={command.runId} />
      <a href={runHref(command.runId)}>Open run inspection</a></div>}
    {storageError && <p role="alert">Tab storage could not be updated. Preserve the original request in this tab; do not start another intent.</p>}
    <div className="source-action-buttons">
      {phase === 'review' && <><button type="button" className="primary-button" disabled={busy} onClick={() => void submit()}>Confirm {running ? 'running' : 'queued'}-run cancellation</button>
        <button type="button" className="secondary-button" disabled={busy} onClick={() => retain(null)}>Discard cancellation review</button></>}
      {(phase === 'pending' || phase === 'uncertain') && <button type="button" className="primary-button" disabled={busy} onClick={() => void submit()}>{busy ? 'Request pending' : 'Resolve same cancellation request'}</button>}
      {phase === 'rejected' && <button type="button" className="secondary-button" onClick={() => retain(null)}>Dismiss rejected cancellation</button>}
      {phase === 'confirmed' && <button type="button" className="secondary-button" onClick={() => retain(null)}>Dismiss confirmed receipt</button>}
    </div>
  </section>;
}
