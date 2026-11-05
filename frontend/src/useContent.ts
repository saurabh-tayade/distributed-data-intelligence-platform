import { useEffect, useRef, useState } from 'react';
import { CONTENT_MAX_OFFSET, ContentError, contentIdentityKey, contentQueryError, fetchContentCatalog } from './contentApi';
import type { ContentIdentity, ContentQuery, ContentResponse } from './contentApi';
type Result = { key: string } & ({ status: 'ready'; response: ContentResponse } | { status: 'error'; error: ContentError });

/** Independent mounted-session pin survives navigation; only the visible catalog fetches. */
export function useContent(enabled: boolean) {
  const [query, setQuery] = useState<ContentQuery>({ limit: 10, offset: 0 });
  const [attempt, setAttempt] = useState(0);
  const [result, setResult] = useState<Result | null>(null);
  const [identity, setIdentity] = useState<ContentIdentity | null>(null);
  const [blocked, setBlocked] = useState(false);
  const pin = useRef<string | null>(null);
  const key = JSON.stringify([query, attempt]);
  useEffect(() => {
    if (!enabled || blocked) return;
    const controller = new AbortController(); let active = true;
    void fetchContentCatalog(query, controller.signal).then((response) => {
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
  }, [enabled, blocked, query, key]);
  function apply(next: ContentQuery) { if (!blocked && !contentQueryError(next)) { setQuery({ ...next, offset: 0 }); setAttempt((n) => n + 1); } }
  function movePage(offset: number) { if (!blocked && Number.isInteger(offset) && offset >= 0 && offset <= CONTENT_MAX_OFFSET) setQuery({ ...query, offset }); }
  const current = blocked ? { status: 'error' as const, error: new ContentError('publication-changed') } : result?.key === key ? result : null;
  return { query, identity, blocked, current, apply, movePage, retry: () => setAttempt((n) => n + 1) };
}
