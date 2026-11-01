import { useEffect, useRef, useState } from 'react';
import { fetchRunDetail, ManagementError } from '../managementApi';
import type { RunDetailResponse } from '../managementApi';

export const ACTIVE_POLL_MS = 5000;
export const MAX_POLL_BACKOFF_MS = 30000;
const active = (value: RunDetailResponse | null) => value !== null && ['QUEUED', 'RUNNING', 'CANCEL_REQUESTED'].includes(value.data.state);
interface Snapshot {
  data: RunDetailResponse | null; error: ManagementError | null; refreshing: boolean; lastUpdated: string | null;
  hidden: boolean; nextPollMs: number | null;
}

/** One settled GET at a time. A hidden/obsolete transport can never publish its response. */
export function useRunDetail(runId: string) {
  const [snapshot, setSnapshot] = useState<Snapshot>({ data: null, error: null, refreshing: true, lastUpdated: null,
    hidden: document.visibilityState === 'hidden', nextPollMs: null });
  const refreshCommand = useRef<(() => void) | null>(null);
  useEffect(() => {
    let alive = true, busy = false, resume = false, automatic = true, failures = 0, documentVisible = document.visibilityState !== 'hidden';
    let last: RunDetailResponse | null = null, controller: AbortController | null = null;
    let timer: ReturnType<typeof setTimeout> | undefined;
    const visible = () => document.visibilityState !== 'hidden';
    const clear = () => { clearTimeout(timer); timer = undefined; };
    async function request(initial = false) {
      if (!alive || busy || !visible()) return;
      clear(); busy = true; resume = false;
      const transport = new AbortController(); controller = transport;
      if (!initial) setSnapshot((old) => ({ ...old, refreshing: true, nextPollMs: null }));
      let delay: number | null = null;
      try {
        const data = await fetchRunDetail(runId, transport.signal);
        if (!alive || transport.signal.aborted || !visible()) return;
        last = data; failures = 0; automatic = true; delay = active(data) ? ACTIVE_POLL_MS : null;
        setSnapshot({ data, error: null, refreshing: false, lastUpdated: new Date().toISOString(), hidden: false, nextPollMs: delay });
      } catch (cause) {
        if (!alive || transport.signal.aborted || !visible()) return;
        const error = cause instanceof ManagementError ? cause : new ManagementError('network');
        const transient = ['network', 'unavailable', 'server', 'capacity'].includes(error.kind) && error.retryable !== false;
        automatic = transient;
        failures = Math.min(failures + 1, 6);
        delay = transient && (last === null || active(last)) ? Math.min(MAX_POLL_BACKOFF_MS, ACTIVE_POLL_MS * 2 ** failures) : null;
        setSnapshot((old) => ({ ...old, error, refreshing: false, nextPollMs: delay }));
      } finally {
        busy = false; controller = null;
        if (alive && visible()) {
          if (resume) void request();
          else if (delay !== null) timer = setTimeout(() => { void request(); }, delay);
          else if (transport.signal.aborted) setSnapshot((old) => ({ ...old, refreshing: false }));
        }
      }
    }
    function visibility() {
      if (visible() === documentVisible) return;
      documentVisible = visible();
      if (!visible()) {
        clear(); resume = true; controller?.abort();
        setSnapshot((old) => ({ ...old, hidden: true, refreshing: false, nextPollMs: null }));
      } else {
        setSnapshot((old) => ({ ...old, hidden: false, refreshing: busy }));
        resume = automatic && (last === null || active(last)); if (resume && !busy) void request();
      }
    }
    refreshCommand.current = () => { if (!busy && visible()) void request(); };
    document.addEventListener('visibilitychange', visibility);
    if (visible()) void request(true);
    return () => { alive = false; clear(); controller?.abort(); refreshCommand.current = null; document.removeEventListener('visibilitychange', visibility); };
  }, [runId]);
  return { ...snapshot, refresh: () => refreshCommand.current?.() };
}
