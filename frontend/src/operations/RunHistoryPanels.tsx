import type { ReactNode } from 'react';
import { fetchAttempts, fetchEvents } from '../managementApi';
import type { AuditEvent, RunAttempt } from '../managementApi';
import { Timestamp } from '../components/Timestamp';
import { InspectionError } from './InspectionError';
import { useRunHistory } from './useRunHistory';

const attemptSequence = (row: RunAttempt) => BigInt(row.attemptNo);
const eventSequence = (row: AuditEvent) => BigInt(row.eventSeq);
function HistoryPanel<T>({ title, label, model, children }: { title: string; label: string; model: ReturnType<typeof useRunHistory<T>>; children: ReactNode }) {
  const page = model.current?.status === 'ready' ? model.current.page : null;
  return <section className="panel source-detail-panel" aria-label={title}>
    <div className="panel-heading"><h2>{title}</h2><button className="secondary-button" type="button" disabled={model.current === null} onClick={() => model.refresh()}>Refresh {label}</button></div>
    <p className="inspection-note">One high-water page in server order. Refresh starts a new traversal; run refresh does not merge or refresh this history.</p>
    <label className="history-limit">{title} per page<select value={model.limit} disabled={model.current === null} onChange={(event) => model.changeLimit(Number(event.target.value))}>
      {[5, 10, 20, 50, 100].map((limit) => <option key={limit} value={limit}>{limit}</option>)}
    </select></label>
    <div aria-busy={model.current === null}>
      {model.current === null && <p className="inspection-note" role="status">Loading {label}</p>}
      {model.current?.status === 'error' && <InspectionError title={`Unable to load ${label}`} error={model.current.error}
        retry={() => model.refresh(false)} restart={model.hasCursor ? () => model.refresh() : undefined} />}
      {page && page.data.length === 0 && <p className="inspection-note" role="status">No {label} returned on this page.</p>}
      {page && page.data.length > 0 && children}
    </div>
    <nav className="source-pagination" aria-label={`${title} pages`}>
      <button className="secondary-button" type="button" disabled={model.current === null || !model.canGoBack} onClick={model.back}>Previous {label}</button>
      <p role="status">Page {model.pageNumber}{page ? ` · ${page.data.length} returned${page.meta.hasMore ? ' · More available' : ' · End of results'}` : ''}</p>
      <button className="secondary-button" type="button" disabled={page === null || !page.meta.hasMore} onClick={model.next}>Next {label}</button>
    </nav>
    {model.historyTruncated && <p className="inspection-note">Only 20 cursor positions are retained. Refresh to restart from the first page.</p>}
  </section>;
}
export function AttemptHistory({ runId }: { runId: string }) {
  const model = useRunHistory(runId, fetchAttempts, attemptSequence);
  return <HistoryPanel title="Attempts" label="attempts" model={model}>
    <p className="inspection-note">EXECUTE is an execution attempt; RECONCILE is a reconciliation attempt. Each record remains separate. Stage count does not identify stages or inherited work.</p>
    {/* eslint-disable-next-line jsx-a11y/no-noninteractive-tabindex -- Keyboard users must be able to scroll the table. */}
    <div className="table-scroll" role="region" aria-label="Attempt records table" tabIndex={0}><table>
      <caption className="visually-hidden">Attempts in ascending attempt-number order. States belong to attempts, not the current run.</caption>
      <thead><tr><th scope="col">Attempt</th><th scope="col">Kind / attempt state</th><th scope="col">Started (UTC)</th><th scope="col">Finished (UTC)</th><th scope="col">Outcome / stage count</th></tr></thead>
      <tbody>{model.current?.status === 'ready' && model.current.page.data.map((attempt) => <tr key={attempt.attemptId}>
        <th scope="row">Attempt {attempt.attemptNo}<code className="inspection-id">{attempt.attemptId}</code></th>
        <td>{attempt.kind}<span className="run-metadata">{attempt.state}</span></td>
        <td><Timestamp value={attempt.startedAt} /></td><td><Timestamp value={attempt.finishedAt} /></td>
        <td><code>{attempt.outcomeCode ?? 'Not recorded'}</code><span className="run-metadata">Stage count: {attempt.stageCount}</span></td>
      </tr>)}</tbody>
    </table></div>
  </HistoryPanel>;
}
export function AuditTimeline({ runId }: { runId: string }) {
  const model = useRunHistory(runId, fetchEvents, eventSequence);
  return <HistoryPanel title="Audit events" label="audit events" model={model}>
    <p className="inspection-note">Audit transitions can describe run, attempt, stage, control or coordinator states. These are not named stage records; no stage names, durations or process commands are inferred.</p>
    <ol className="revision-list audit-list" aria-label="Audit events in server sequence order">
      {model.current?.status === 'ready' && model.current.page.data.map((event) => <li className="revision-card" key={event.eventSeq}>
        <h3>Event {event.eventSeq} <code>{event.action}</code></h3>
        <dl className="source-metadata">
          <div><dt>Timestamp (UTC)</dt><dd><Timestamp value={event.timestamp} /></dd></div>
          <div><dt>Public classification</dt><dd><code>{event.code ?? 'Not recorded'}</code></dd></div>
          <div><dt>Before state</dt><dd>{event.beforeState ?? 'Not recorded'}</dd></div><div><dt>After state</dt><dd>{event.afterState ?? 'Not recorded'}</dd></div>
          <div><dt>Before row version</dt><dd><code>{event.beforeVersion ?? 'Not recorded'}</code></dd></div><div><dt>After row version</dt><dd><code>{event.afterVersion ?? 'Not recorded'}</code></dd></div>
          <div><dt>Related attempt ID</dt><dd><code>{event.context.attemptId ?? 'Not recorded'}</code></dd></div>
          <div><dt>Related control ID</dt><dd><code>{event.context.controlId ?? 'Not recorded'}</code></dd></div>
        </dl>
      </li>)}
    </ol>
  </HistoryPanel>;
}
