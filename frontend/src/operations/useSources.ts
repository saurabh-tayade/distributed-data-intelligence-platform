import { useEffect, useState } from 'react';
import { fetchSources, ManagementError, SOURCE_PAGE_LIMIT } from '../managementApi';
import type { ProductId, SourcesPage, SourcesQuery } from '../managementApi';

export interface SourceFilters { productId: ProductId | ''; enabled: '' | 'true' | 'false'; limit: number }
type Result = { key: string } & ({ status: 'ready'; page: SourcesPage } | { status: 'error'; error: ManagementError });

/** One explicit page per request; only the visible Operations workspace fetches. */
export function useSources(active: boolean) {
  const [state, setState] = useState<{ filters: SourceFilters; cursors: (string | undefined)[]; refresh: number }>({
    filters: { productId: '', enabled: '', limit: SOURCE_PAGE_LIMIT }, cursors: [undefined], refresh: 0,
  });
  const [result, setResult] = useState<Result | null>(null);
  const { filters, cursors } = state, cursor = cursors.at(-1);
  const key = JSON.stringify([filters.productId, filters.enabled, filters.limit, cursor, state.refresh]);
  useEffect(() => {
    if (!active) return;
    const controller = new AbortController();
    let current = true;
    const query: SourcesQuery = { limit: filters.limit };
    if (filters.productId) query.productId = filters.productId;
    if (filters.enabled) query.enabled = filters.enabled === 'true';
    if (cursor !== undefined) query.cursor = cursor;
    void fetchSources(query, controller.signal).then((page) => {
      if (!current || controller.signal.aborted) return;
      if (page.meta.nextCursor !== null && cursors.includes(page.meta.nextCursor)) throw new ManagementError('invalid-response');
      setResult({ key, status: 'ready', page });
    }).catch((error: unknown) => {
      if (!current || controller.signal.aborted) return;
      setResult({ key, status: 'error', error: error instanceof ManagementError ? error : new ManagementError('network') });
    });
    return () => { current = false; controller.abort(); };
  }, [active, filters, cursor, cursors, key]);

  const current = result?.key === key ? result : null;
  function changeFilters(change: Partial<SourceFilters>) {
    setState((previous) => ({ ...previous, filters: { ...previous.filters, ...change }, cursors: [undefined] }));
  }
  function next() {
    if (current?.status !== 'ready' || !current.page.meta.hasMore || current.page.meta.nextCursor === null) return;
    const nextCursor = current.page.meta.nextCursor;
    setState((previous) => previous === state ? { ...previous, cursors: [...cursors, nextCursor] } : previous);
  }
  function back() {
    if (cursors.length > 1) setState((previous) => previous === state ? { ...previous, cursors: cursors.slice(0, -1) } : previous);
  }
  function refresh(firstPage = false) {
    setState((previous) => ({ ...previous, refresh: previous.refresh + 1, cursors: firstPage ? [undefined] : previous.cursors }));
  }
  return { filters, current, changeFilters, next, back, refresh, pageNumber: cursors.length, hasCursor: cursor !== undefined };
}
