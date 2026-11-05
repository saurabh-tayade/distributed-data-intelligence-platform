import { useEffect, useRef, useState } from 'react';
import { CONTENT_MAX_OFFSET, ContentError, contentIdentityKey, contentReportQueryError, fetchContentReport } from './contentApi';
import type { ContentIdentity, ContentQuery, ContentReport, ContentReportResponse } from './contentApi';
type Result = { key: string } & ({ status: 'ready'; response: ContentReportResponse } | { status: 'error'; error: ContentError });

/** Independent mounted-session pin survives navigation; only the visible report fetches. */
export function useContent(enabled: boolean) {
  const [report, setReport] = useState<ContentReport>('catalog');
  const [query, setQuery] = useState<ContentQuery>({ limit: 10, offset: 0 });
  const [attempt, setAttempt] = useState(0);
  const [result, setResult] = useState<Result | null>(null);
  const [identity, setIdentity] = useState<ContentIdentity | null>(null);
  const [blocked, setBlocked] = useState(false);
  const pin = useRef<string | null>(null);
  const key = JSON.stringify([report, query, attempt]);
  useEffect(() => {
    if (!enabled || blocked) return;
    const controller = new AbortController(); let active = true;
    void fetchContentReport(report, query, controller.signal).then((response) => {
      if (!active || controller.signal.aborted) return;
      const next = contentIdentityKey(response);
      if (pin.current !== null && pin.current !== next) { setBlocked(true); return; }
      pin.current = next;
      setIdentity({ publication: response.publication, goldPublication: response.goldPublication, reference: response.reference });
      setResult({ key, status: 'ready', response });
    }).catch((error: unknown) => {
      if (!active || controller.signal.aborted) return;
      setResult({ key, status: 'error', error: error instanceof ContentError ? error : new ContentError('network') });
    });
    return () => { active = false; controller.abort(); setResult(null); };
  }, [enabled, blocked, report, query, key]);
  function apply(next: ContentQuery) { if (!blocked && !contentReportQueryError(report, next)) { setQuery({ ...next, offset: 0 }); setAttempt((n) => n + 1); } }
  function movePage(offset: number) { if (!blocked && Number.isInteger(offset) && offset >= 0 && offset <= CONTENT_MAX_OFFSET) setQuery({ ...query, offset }); }
  const current = blocked ? { status: 'error' as const, error: new ContentError('publication-changed') } : result?.key === key ? result : null;
  function selectReport(next: ContentReport) { if (!blocked && next !== report) { setReport(next); setQuery({ limit: next === 'catalog' ? 10 : 100, offset: 0 }); } }
  return { report, selectReport, query, identity, blocked, current, apply, movePage, retry: () => setAttempt((n) => n + 1) };
}
