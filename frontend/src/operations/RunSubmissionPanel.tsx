import { useEffect, useRef, useState } from 'react';
import { RunSubmissionError, sendRunSubmission } from '../managementApi';
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
  const controller = useRef<AbortController | null>(null), heading = useRef<HTMLHeadingElement>(null);
  const key = intent?.command.key;
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
    const original = intent, abort = new AbortController(); controller.current = abort; setBusy(true);
    const current = () => { try { return !abort.signal.aborted && readRunIntent()?.command.key === original.command.key; } catch { return false; } };
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
  if (!intent) return null;
  const { command, phase, error, receipt } = intent;
  return <section className="panel source-action-review" aria-labelledby="run-review-title" aria-busy={busy}>
    <h2 id="run-review-title" ref={heading} tabIndex={-1}>{phase === 'review' ? 'Review Reliability run' : 'Reliability submission receipt'}</h2>
    <p>Pipeline: <strong>RELIABILITY_V2</strong> · Product: Application Reliability.</p>
    <p>Review all immutable identifiers and source revisions below. Admission does not start a worker or activate Analytics results. Subsequent processing depends on the configured Reliability worker.</p>
    <p>Idempotency key: <code>{command.key}</code></p>
    <details open><summary>Complete frozen submission</summary><pre className="source-request-body">{JSON.stringify(JSON.parse(command.body), null, 2)}</pre></details>
    {error && <div role="alert"><p>{explanation(error.status)}</p><ManagementErrorDetails error={error} mutation /></div>}
    {(phase === 'uncertain' || phase === 'pending' && !busy) && <p role="alert">Outcome unknown. The original key and exact body are retained in this tab. Resolve the same request before starting a different intent. No automatic replay occurs.</p>}
    {phase === 'pending' && busy && <p role="status">Admission request pending.</p>}
    {receipt && <div role="status"><p>Admission confirmed for <code>{receipt.runId}</code>. Persisted state at receipt: <strong>{receipt.state}</strong>.</p>
      <p>{receipt.state === 'QUEUED' ? 'QUEUED means accepted for admission, not actively processing.' : 'An exact replay can return an advanced or terminal persisted run.'} Inspect the run for its current state.</p>
      <p>Run ETag: <code>{receipt.etag}</code> · Reference: <code>{receipt.correlationId}</code></p>
      <a className="primary-button" href={runHref(receipt.runId)}>Open admitted run</a></div>}
    {storageError && <p role="alert">Tab storage could not be updated. Preserve the original request in this tab; do not start another intent.</p>}
    <div className="source-action-buttons">
      {phase === 'review' && <><button type="button" className="primary-button" disabled={busy} onClick={() => void submit()}>Confirm Reliability submission</button>
        <button type="button" className="secondary-button" disabled={busy} onClick={() => retain(null)}>Cancel run review</button></>}
      {(phase === 'pending' || phase === 'uncertain') && <button type="button" className="primary-button" disabled={busy} onClick={() => void submit()}>{busy ? 'Request pending' : 'Resolve same run request'}</button>}
      {phase === 'rejected' && <button type="button" className="secondary-button" onClick={() => retain(null)}>Return to selection and deliberate review</button>}
      {phase === 'confirmed' && <button type="button" className="secondary-button" onClick={() => retain(null)}>Dismiss confirmed receipt</button>}
    </div>
  </section>;
}
