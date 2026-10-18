import { useEffect, useRef, useState } from 'react';
import { DashboardError, fetchPublishers } from './api';
import type { PublishersResponse } from './api';

type Result = { key: string } & ({ status: 'ready'; response: PublishersResponse } | { status: 'error'; error: DashboardError });
type Catalog = { publication: string; sources: string[]; bounded: boolean };

export function usePublishers() {
  const [sourceId, setSourceId] = useState('');
  const [attempt, setAttempt] = useState(0);
  const [result, setResult] = useState<Result | null>(null);
  const [catalog, setCatalog] = useState<Catalog | null>(null);
  const pinnedPublication = useRef<string | null>(null);
  const key = JSON.stringify([sourceId, attempt]);

  useEffect(() => {
    const controller = new AbortController();
    let active = true;
    void fetchPublishers(sourceId, controller.signal).then((response) => {
      if (!active || controller.signal.aborted) return;
      if (pinnedPublication.current !== null && pinnedPublication.current !== response.publication) {
        throw new DashboardError('publication-changed');
      }
      pinnedPublication.current = response.publication;
      if (sourceId === '') {
        setCatalog({ publication: response.publication, sources: response.data.map((row) => row.sourceId),
          bounded: response.meta.returned === response.meta.limit });
      }
      setResult({ key, status: 'ready', response });
    }).catch((error: unknown) => {
      if (!active || controller.signal.aborted) return;
      setResult({ key, status: 'error', error: error instanceof DashboardError ? error : new DashboardError('network') });
    });
    return () => { active = false; controller.abort(); };
  }, [sourceId, key]);

  // Changing the request hides old data immediately, including before effect cleanup runs.
  const current = result?.key === key ? result : null;
  return { sourceId, setSourceId, catalog, current, retry: () => setAttempt((value) => value + 1) };
}
