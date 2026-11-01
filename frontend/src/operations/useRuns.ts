import { useEffect, useState } from 'react';
import { fetchRuns, ManagementError } from '../managementApi';
import type { RunsPage } from '../managementApi';
import type { ExecutionFilters } from '../navigation';

export const RUN_CURSOR_HISTORY_LIMIT = 20;
type Result = { key: string } & ({ status: 'ready'; page: RunsPage } | { status: 'error'; error: ManagementError });

/** Mounted only on Executions. Retain at most 20 cursor positions, and just one result page. */
export function useRuns(query: ExecutionFilters) {
  const [state, setState] = useState<{ cursors: (string | undefined)[]; firstPage: number; refresh: number }>({
    cursors: [undefined], firstPage: 1, refresh: 0,
  });
  const [result, setResult] = useState<Result | null>(null);
  const { cursors } = state, cursor = cursors.at(-1);
  const key = JSON.stringify([query, cursor, state.refresh]);
  useEffect(() => {
    const controller = new AbortController(); let current = true;
    void fetchRuns({ ...query, ...(cursor === undefined ? {} : { cursor }) }, controller.signal).then((page) => {
      if (!current || controller.signal.aborted) return;
      if (page.meta.nextCursor !== null && cursors.includes(page.meta.nextCursor)) throw new ManagementError('invalid-response');
      setResult({ key, status: 'ready', page });
    }).catch((error: unknown) => {
      if (!current || controller.signal.aborted) return;
      setResult({ key, status: 'error', error: error instanceof ManagementError ? error : new ManagementError('network') });
    });
    return () => { current = false; controller.abort(); };
  }, [query, cursor, key, cursors]);

  const current = result?.key === key ? result : null;
  function next() {
    if (current?.status !== 'ready' || !current.page.meta.hasMore || current.page.meta.nextCursor === null) return;
    const nextCursor = current.page.meta.nextCursor;
    setState((old) => {
      if (old !== state) return old;
      const pages = [...old.cursors, nextCursor], overflow = pages.length > RUN_CURSOR_HISTORY_LIMIT;
      return { ...old, cursors: overflow ? pages.slice(1) : pages, firstPage: old.firstPage + Number(overflow) };
    });
  }
  function back() {
    if (cursors.length > 1) setState((old) => old === state ? { ...old, cursors: old.cursors.slice(0, -1) } : old);
  }
  function refresh(firstPage = false) {
    setState((old) => ({ cursors: firstPage ? [undefined] : old.cursors, firstPage: firstPage ? 1 : old.firstPage, refresh: old.refresh + 1 }));
  }
  return { current, next, back, refresh, pageNumber: state.firstPage + cursors.length - 1, canGoBack: cursors.length > 1,
    hasCursor: cursor !== undefined, historyTruncated: state.firstPage > 1 };
}
