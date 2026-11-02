import { useState } from 'react';
import { fetchAttempts, fetchEvents } from '../managementApi';
import type { AuditEvent, RunAttempt } from '../managementApi';
import { Timestamp } from '../components/Timestamp';
import { HistoryPanel } from './HistoryPanel';
import { useRunHistory } from './useRunHistory';
import { StageRecords } from './StageRecords';

const attemptSequence = (row: RunAttempt) => BigInt(row.attemptNo);
const eventSequence = (row: AuditEvent) => BigInt(row.eventSeq);
export function AttemptHistory({ runId }: { runId: string }) {
  const model = useRunHistory(runId, fetchAttempts, attemptSequence);
  const [selected, setSelected] = useState<RunAttempt | null>(null);
  return <><HistoryPanel title="Attempts" label="attempts" model={model}>
    <p className="inspection-note">EXECUTE is an execution attempt; RECONCILE is a reconciliation attempt. Each record remains separate. Stage count does not identify stages or inherited work.</p>
    {/* eslint-disable-next-line jsx-a11y/no-noninteractive-tabindex -- Keyboard users must be able to scroll the table. */}
    <div className="table-scroll" role="region" aria-label="Attempt records table" tabIndex={0}><table>
      <caption className="visually-hidden">Attempts in ascending attempt-number order. States belong to attempts, not the current run.</caption>
      <thead><tr><th scope="col">Attempt</th><th scope="col">Kind / attempt state</th><th scope="col">Started (UTC)</th><th scope="col">Finished (UTC)</th><th scope="col">Outcome / stage count</th><th scope="col">Stage records</th></tr></thead>
      <tbody>{model.current?.status === 'ready' && model.current.page.data.map((attempt) => <tr key={attempt.attemptId}>
        <th scope="row">Attempt {attempt.attemptNo}<code className="inspection-id">{attempt.attemptId}</code></th>
        <td>{attempt.kind}<span className="run-metadata">{attempt.state}</span></td>
        <td><Timestamp value={attempt.startedAt} /></td><td><Timestamp value={attempt.finishedAt} /></td>
        <td><code>{attempt.outcomeCode ?? 'Not recorded'}</code><span className="run-metadata">Stage count: {attempt.stageCount}</span></td>
        <td><button type="button" className="secondary-button" aria-pressed={selected?.attemptId === attempt.attemptId} aria-label={`Inspect stages for attempt ${attempt.attemptNo}`} onClick={() => setSelected(attempt)}>Inspect stages</button></td>
      </tr>)}</tbody>
    </table></div>
  </HistoryPanel>
    {selected && <StageRecords key={`${runId}:${selected.attemptId}`} runId={runId} attempt={selected} />}
  </>;
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
