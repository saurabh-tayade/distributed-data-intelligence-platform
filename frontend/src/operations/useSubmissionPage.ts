import { useEffect, useState } from 'react';
import { ManagementError } from '../managementApi';
import type { ManagementPage, RevisionQuery } from '../managementApi';

type Result<T> = { key: string } & ({ status: 'ready'; page: ManagementPage<T> } | { status: 'error'; error: ManagementError });
const HISTORY_LIMIT = 20;

/** One visible catalog page, with cancellation and bounded cursor history. No background traversal. */
export function useSubmissionPage<T>(scope: string, active: boolean, load: (query: RevisionQuery, signal: AbortSignal) => Promise<ManagementPage<T>>) {
  const [state, setState] = useState({ scope, active, cursors: [undefined] as (string | undefined)[], refresh: 0, truncated: false });
  const [result, setResult] = useState<Result<T> | null>(null);
  if (state.scope !== scope || state.active !== active) setState({ scope, active, cursors: [undefined], refresh: state.refresh + 1, truncated: false });
  const { cursors } = state, cursor = cursors.at(-1), key = JSON.stringify([scope, cursor, state.refresh]);
  useEffect(() => {
    if (!active || state.scope !== scope) return;
    const abort = new AbortController(); let current = true;
    void load({ limit: 20, ...(cursor === undefined ? {} : { cursor }) }, abort.signal).then((page) => {
      if (!current || abort.signal.aborted) return;
      if (page.meta.nextCursor !== null && cursors.includes(page.meta.nextCursor)) throw new ManagementError('invalid-response');
      setResult({ key, status: 'ready', page });
    }).catch((error: unknown) => {
      if (!current || abort.signal.aborted) return;
      setResult({ key, status: 'error', error: error instanceof ManagementError ? error : new ManagementError('network') });
    });
    return () => { current = false; abort.abort(); };
  }, [active, scope, state.scope, key, cursor, cursors, load]);
  const current = active && result?.key === key ? result : null;
  function refresh() { setState((old) => ({ ...old, cursors: [undefined], truncated: false, refresh: old.refresh + 1 })); }
  function retry() { setState((old) => ({ ...old, refresh: old.refresh + 1 })); }
  function next() {
    if (current?.status !== 'ready' || current.page.meta.nextCursor === null) return;
    const nextCursor = current.page.meta.nextCursor;
    setState((old) => old !== state ? old : { ...old, cursors: [...cursors, nextCursor].slice(-HISTORY_LIMIT), truncated: old.truncated || cursors.length >= HISTORY_LIMIT });
  }
  function back() { if (cursors.length > 1) setState((old) => old !== state ? old : { ...old, cursors: cursors.slice(0, -1) }); }
  return { current, next, back, refresh, retry, canGoBack: cursors.length > 1, truncated: state.truncated };
}
