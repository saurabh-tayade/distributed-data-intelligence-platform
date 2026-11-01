import { useEffect, useState } from 'react';
import { ManagementError } from '../managementApi';
import type { ManagementPage, RevisionQuery } from '../managementApi';
import { RUN_CURSOR_HISTORY_LIMIT } from './useRuns';

type Result<T> = { key: string } & ({ status: 'ready'; page: ManagementPage<T> } | { status: 'error'; error: ManagementError });
export function useRunHistory<T>(runId: string, read: (id: string, query: RevisionQuery, signal: AbortSignal) => Promise<ManagementPage<T>>, sequence: (row: T) => bigint) {
  const [state, setState] = useState<{ positions: { cursor?: string; after: string }[]; firstPage: number; limit: number; refresh: number }>({
    positions: [{ after: '0' }], firstPage: 1, limit: 20, refresh: 0,
  });
  const [result, setResult] = useState<Result<T> | null>(null);
  const position = state.positions.at(-1)!, key = JSON.stringify([runId, state]);
  useEffect(() => {
    const controller = new AbortController(); let current = true;
    void read(runId, { limit: state.limit, ...(position.cursor === undefined ? {} : { cursor: position.cursor }) }, controller.signal).then((page) => {
      if (!current || controller.signal.aborted) return;
      if ((page.meta.nextCursor !== null && state.positions.some((entry) => entry.cursor === page.meta.nextCursor))
        || (page.data[0] !== undefined && sequence(page.data[0]) <= BigInt(position.after))) throw new ManagementError('invalid-response');
      setResult({ key, status: 'ready', page });
    }).catch((error: unknown) => {
      if (!current || controller.signal.aborted) return;
      setResult({ key, status: 'error', error: error instanceof ManagementError ? error : new ManagementError('network') });
    });
    return () => { current = false; controller.abort(); };
  }, [runId, read, sequence, state, position, key]);
  const current = result?.key === key ? result : null;
  function next() {
    if (current?.status !== 'ready' || current.page.meta.nextCursor === null) return;
    const cursor = current.page.meta.nextCursor, after = sequence(current.page.data.at(-1)!).toString();
    setState((old) => {
      if (old !== state) return old;
      const positions = [...old.positions, { cursor, after }], overflow = positions.length > RUN_CURSOR_HISTORY_LIMIT;
      return { ...old, positions: overflow ? positions.slice(1) : positions, firstPage: old.firstPage + Number(overflow) };
    });
  }
  function back() {
    if (state.positions.length > 1) setState((old) => old === state ? { ...old, positions: old.positions.slice(0, -1) } : old);
  }
  function refresh(first = true) {
    if (current === null) return;
    setState((old) => old === state ? { ...old, positions: first ? [{ after: '0' }] : old.positions, firstPage: first ? 1 : old.firstPage, refresh: old.refresh + 1 } : old);
  }
  function changeLimit(limit: number) {
    if (current !== null && Number.isInteger(limit) && limit >= 1 && limit <= 100) setState((old) => ({ ...old, limit, positions: [{ after: '0' }], firstPage: 1 }));
  }
  return { current, next, back, refresh, changeLimit, limit: state.limit, pageNumber: state.firstPage + state.positions.length - 1,
    canGoBack: state.positions.length > 1, hasCursor: position.cursor !== undefined, historyTruncated: state.firstPage > 1 };
}
