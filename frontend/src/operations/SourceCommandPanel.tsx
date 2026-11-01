import { useEffect, useRef, useState } from 'react';
import { fetchSource, sendSourceCommand } from '../managementApi';
import { sourceHref } from '../navigation';
import { ManagementErrorDetails } from './ManagementErrorDetails';
import { commandError, readSourceIntent, saveSourceIntent, sourceChanged, useSourceIntent } from './sourceIntent';
import type { SourceIntent } from './sourceIntent';

function explanation(status: number | null) {
  switch (status) {
    case 400: return 'The server rejected the configuration. Review the registered references and fields.';
    case 403: return 'Local access was rejected. Check the approved UI origin and API host.';
    case 404: return 'The source was not found. Check the selected identity.';
    case 409: return 'The source state or idempotency binding conflicts with this request. Inspect the persisted source before starting a different intent.';
    case 412: return 'The source changed since review. Refresh and deliberately review the current record before creating another request.';
    case 428: return 'A current source precondition is required. Refresh and review the current record.';
    case 429: return 'Management capacity is exhausted. No automatic retry will be submitted.';
    case 503: return 'Management is unavailable. The outcome may be unknown; resolve the original request when the service is available.';
    default: return 'The response did not confirm the change. The request may have committed. Resolve it using the original request.';
  }
}

/** A saved response may describe an earlier version. Always GET the current record after confirmation. */
export function SourceCommandPanel({ visible }: { visible: boolean }) {
  const { intent, unavailable } = useSourceIntent();
  const [busy, setBusy] = useState(false);
  const [notice, setNotice] = useState('');
  const [reviewedKey, setReviewedKey] = useState<string | null>(null);
  const controller = useRef<AbortController | null>(null);
  const heading = useRef<HTMLHeadingElement>(null);
  const key = intent?.command.key;
  function retain(value: SourceIntent | null) {
    try { saveSourceIntent(value); return true; }
    catch { setNotice('Tab storage could not be updated. Preserve this tab and its original request; no new intent can be started safely.'); return false; }
  }
  useEffect(() => { if (visible && key) heading.current?.focus(); }, [key, visible]);
  useEffect(() => {
    return () => {
      controller.current?.abort(); controller.current = null;
      try {
        const current = readSourceIntent();
        if (current?.phase === 'pending') saveSourceIntent({ ...current, phase: 'uncertain' });
      } catch { /* Invalid/unavailable tab storage keeps actions disabled on the next mount. */ }
    };
  }, []);

  async function run(mode: 'submit' | 'read') {
    if (controller.current || !intent) return;
    const current = intent;
    const abort = new AbortController(); controller.current = abort; setBusy(true); setNotice('');
    const isCurrent = () => {
      try { return !abort.signal.aborted && readSourceIntent()?.command.key === current.command.key; }
      catch { return false; }
    };
    let confirmed = mode === 'read' && current.phase === 'confirmed';
    try {
      if (mode === 'submit') {
        if (!retain({ command: current.command, phase: 'pending' })) return;
        await sendSourceCommand(current.command, abort.signal);
        if (!isCurrent()) return;
        confirmed = true;
        retain({ command: current.command, phase: 'confirmed' });
      }
      await fetchSource(current.command.identity, abort.signal);
      if (!isCurrent()) return;
      if (confirmed) {
        if (!retain(null)) return;
        sourceChanged();
        document.getElementById('main')?.focus();
        window.location.hash = sourceHref(current.command.identity);
        setNotice('Source change confirmed. The persisted current record has been refreshed.');
      } else {
        setReviewedKey(current.command.key); sourceChanged();
        setNotice('Current record refreshed. Review it before starting a new mutation intent.');
      }
    } catch (error) {
      if (!isCurrent()) return;
      if (mode === 'read' || confirmed) {
        setNotice('The current record could not be refreshed. Retry the read; no new mutation will be submitted.');
      } else {
        const safe = commandError(error);
        const rejected: SourceIntent = { command: current.command, phase: safe.ambiguous ? 'uncertain' : 'rejected', error: {
          status: safe.status, code: safe.code, correlationId: safe.correlationId, retryable: safe.retryable, ambiguous: safe.ambiguous,
        } };
        retain(rejected);
        if (safe.status === 412 || safe.status === 428) {
          try {
            await fetchSource(current.command.identity, abort.signal);
            if (isCurrent()) { setReviewedKey(current.command.key); sourceChanged(); setNotice('Current source refreshed. Review the new record before another request.'); }
          } catch { if (isCurrent()) setNotice('Refresh failed. Read the current source before reviewing another request.'); }
        }
      }
    } finally {
      if (controller.current === abort) { controller.current = null; setBusy(false); }
    }
  }
  if (!visible) return null;
  if (unavailable) return <div className="state-panel error-state" role="alert"><h2>Source actions unavailable</h2>
    <p>Tab storage is unavailable or contains an invalid unresolved request. Preserve the tab data for operator inspection before clearing it. Source writes are disabled.</p></div>;
  if (!intent) return notice ? <p role="status" className="scope-notice">{notice}</p> : null;
  const { command, phase, error } = intent;
  const mustRead = phase === 'rejected' && [409, 412, 428].includes(error?.status ?? 0);
  const label = command.action === 'create' ? 'Register source' : command.action === 'revise' ? 'Create immutable revision'
    : JSON.parse(command.body).enabled ? 'Enable admission' : 'Disable admission';
  return <section className="panel source-action-review" aria-labelledby="source-review-heading" aria-busy={busy}>
    <h2 id="source-review-heading" ref={heading} tabIndex={-1}>{phase === 'review' ? 'Review source change' : 'Source request receipt'}</h2>
    <p><strong>{label}</strong> for <code>{command.identity.productId} / {command.identity.sourceId}</code>.</p>
    <p><a href={sourceHref(command.identity)}>Inspect persisted source</a></p>
    <p>Registration and configuration changes do not ingest data or start processing. Disabling admission does not cancel active processing.</p>
    <dl className="source-metadata"><div><dt>Original precondition</dt><dd><code>{command.ifMatch ?? 'New source (no If-Match)'}</code></dd></div>
      <div><dt>Idempotency key</dt><dd><code>{command.key}</code></dd></div></dl>
    <details open={phase === 'review'}><summary>Exact request configuration</summary><pre className="source-request-body">{JSON.stringify(JSON.parse(command.body), null, 2)}</pre></details>
    {error && <div role="alert"><p>{explanation(error.status)}</p><ManagementErrorDetails error={error} mutation /></div>}
    {(phase === 'uncertain' || phase === 'pending' && !busy) && <p role="alert">Outcome unknown. This tab retains the exact key, body and original precondition. Resolve this request before editing or starting another source change.</p>}
    {phase === 'confirmed' && <p role="status">The server confirmed this command. Refreshing the persisted current record is still required.</p>}
    {notice && <p role="status">{notice}</p>}
    <div className="source-action-buttons">
      {phase === 'review' && <><button type="button" className="primary-button" disabled={busy} onClick={() => void run('submit')}>Confirm {label.toLowerCase()}</button>
        <button type="button" className="secondary-button" disabled={busy} onClick={() => { if (retain(null)) document.getElementById('main')?.focus(); }}>Cancel review</button></>}
      {(phase === 'uncertain' || phase === 'pending') && <button type="button" className="primary-button" disabled={busy} onClick={() => void run('submit')}>{busy ? 'Request pending' : 'Resolve same request'}</button>}
      {(phase === 'confirmed' || phase === 'rejected') && <button type="button" className="secondary-button" disabled={busy} onClick={() => void run('read')}>Refresh current source</button>}
      {phase === 'rejected' && <button type="button" className="secondary-button" disabled={busy || mustRead && reviewedKey !== command.key}
        onClick={() => { if (retain(null)) { sourceChanged(); document.getElementById('main')?.focus(); } }}>Return to deliberate review</button>}
    </div>
  </section>;
}
