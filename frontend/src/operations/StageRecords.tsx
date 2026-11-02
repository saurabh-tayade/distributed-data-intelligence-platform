import { useCallback, useEffect, useState } from 'react';
import { fetchStages, stagePosition } from '../managementApi';
import type { RevisionQuery, RunAttempt } from '../managementApi';
import { Timestamp } from '../components/Timestamp';
import { HistoryPanel } from './HistoryPanel';
import { useRunHistory } from './useRunHistory';

/** Mounted for one selected run/attempt identity. No timer or execution action. */
export function StageRecords({ runId, attempt }: { runId: string; attempt: RunAttempt }) {
  const [visible, setVisible] = useState(document.visibilityState !== 'hidden');
  useEffect(() => {
    const visibility = () => setVisible(document.visibilityState !== 'hidden');
    document.addEventListener('visibilitychange', visibility);
    return () => document.removeEventListener('visibilitychange', visibility);
  }, []);
  const read = useCallback((id: string, query: RevisionQuery, signal: AbortSignal) => fetchStages(id, attempt.attemptId, query, signal), [attempt.attemptId]);
  const model = useRunHistory(runId, read, stagePosition, visible);
  const rows = model.current?.status === 'ready' ? model.current.page.data : [];
  return <HistoryPanel title="Stage Records" label="stages" model={model} suspended={!visible} refreshWhileLoading description={<>
    <p className="inspection-note">Selected attempt {attempt.attemptNo} · {attempt.kind}<code className="inspection-id">{attempt.attemptId}</code></p>
    <p className="inspection-note">Records are ordered by stage name and item ordinal, not execution time. VERIFIED records historical verification, not a fresh integrity check. Each stage stays with its owning attempt.</p>
    <p className="inspection-note">This page is not a snapshot. Refresh stages starts a new traversal to include newly recorded work. Run detail refresh does not refresh stages.</p>
  </>}>
    <ol className="revision-list" aria-label="Stage records in server identity order">
      {rows.map((stage) => <li className="revision-card" key={stage.checkpointId}>
        <h3><code>{stage.stage}</code> · Item {stage.itemOrdinal}</h3>
        <dl className="source-metadata">
          <div><dt>Stage state</dt><dd>{stage.state}</dd></div>
          <div><dt>Started (UTC)</dt><dd><Timestamp value={stage.startedAt} /></dd></div>
          <div><dt>Finished (UTC)</dt><dd><Timestamp value={stage.finishedAt} /></dd></div>
          <div><dt>Outcome</dt><dd><code>{stage.outcomeCode ?? 'Not recorded'}</code></dd></div>
          <div><dt>Checkpoint ID</dt><dd><code>{stage.checkpointId}</code></dd></div>
        </dl>
        <h4>Safe counts</h4>{stage.counts === null || Object.keys(stage.counts).length === 0
          ? <p className="inspection-note">No counts recorded.</p>
          : <dl className="source-metadata">{Object.entries(stage.counts).map(([name, count]) => <div key={name}><dt>{name}</dt><dd><code>{count}</code></dd></div>)}</dl>}
      </li>)}
    </ol>
  </HistoryPanel>;
}
