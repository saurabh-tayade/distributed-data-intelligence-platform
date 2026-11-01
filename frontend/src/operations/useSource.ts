import { useEffect, useState } from 'react';
import { fetchSource, fetchRevisions, ManagementError, SOURCE_PAGE_LIMIT } from '../managementApi';
import type { SourceIdentity, SourceResponse, RevisionsPage, RevisionQuery } from '../managementApi';

type Result<T> = { key: string } & ({ status: 'ready'; value: T } | { status: 'error'; error: ManagementError });
const safeError = (error: unknown) => error instanceof ManagementError ? error : new ManagementError('network');

/** Independent reads; keys hide obsolete results before effect cleanup, even if fetch ignores abort. */
export function useSource(identity: SourceIdentity) {
  const { productId, sourceId } = identity;
  const [detailRefresh, setDetailRefresh] = useState(0);
  const [traversal, setTraversal] = useState<{ pages: { cursor?: string; after: number }[]; refresh: number }>({
    pages: [{ after: 0 }], refresh: 0,
  });
  const [detailResult, setDetailResult] = useState<Result<SourceResponse> | null>(null);
  const [revisionResult, setRevisionResult] = useState<Result<RevisionsPage> | null>(null);
  const detailKey = JSON.stringify([productId, sourceId, detailRefresh]);
  const { pages } = traversal, position = pages.at(-1)!;
  const revisionKey = JSON.stringify([productId, sourceId, pages, traversal.refresh]);

  useEffect(() => {
    const controller = new AbortController(); let current = true;
    void fetchSource({ productId, sourceId }, controller.signal).then((value) => {
      if (current && !controller.signal.aborted) setDetailResult({ key: detailKey, status: 'ready', value });
    }).catch((error: unknown) => {
      if (current && !controller.signal.aborted) setDetailResult({ key: detailKey, status: 'error', error: safeError(error) });
    });
    return () => { current = false; controller.abort(); };
  }, [productId, sourceId, detailKey]);

  useEffect(() => {
    const controller = new AbortController(); let current = true;
    const query: RevisionQuery = { limit: SOURCE_PAGE_LIMIT };
    if (position.cursor !== undefined) query.cursor = position.cursor;
    void fetchRevisions({ productId, sourceId }, query, controller.signal).then((value) => {
      if (!current || controller.signal.aborted) return;
      if ((value.meta.nextCursor !== null && pages.some((page) => page.cursor === value.meta.nextCursor))
        || (value.data[0] !== undefined && value.data[0].revision <= position.after)) throw new ManagementError('invalid-response');
      setRevisionResult({ key: revisionKey, status: 'ready', value });
    }).catch((error: unknown) => {
      if (current && !controller.signal.aborted) setRevisionResult({ key: revisionKey, status: 'error', error: safeError(error) });
    });
    return () => { current = false; controller.abort(); };
  }, [productId, sourceId, revisionKey, position, pages]);

  const detail = detailResult?.key === detailKey ? detailResult : null;
  const revisions = revisionResult?.key === revisionKey ? revisionResult : null;
  function refreshRevisions(firstPage = false) {
    setTraversal((old) => ({ pages: firstPage ? [{ after: 0 }] : old.pages, refresh: old.refresh + 1 }));
  }
  function next() {
    if (revisions?.status !== 'ready' || !revisions.value.meta.hasMore || revisions.value.meta.nextCursor === null) return;
    const cursor = revisions.value.meta.nextCursor, after = revisions.value.data.at(-1)!.revision;
    setTraversal((old) => old === traversal ? { ...old, pages: [...old.pages, { cursor, after }] } : old);
  }
  function back() {
    if (pages.length > 1) setTraversal((old) => old === traversal ? { ...old, pages: old.pages.slice(0, -1) } : old);
  }
  return { detail, revisions, next, back, pageNumber: pages.length, hasCursor: position.cursor !== undefined,
    retryDetail: () => setDetailRefresh((old) => old + 1), refreshRevisions,
    refresh: () => { setDetailRefresh((old) => old + 1); refreshRevisions(true); } };
}
