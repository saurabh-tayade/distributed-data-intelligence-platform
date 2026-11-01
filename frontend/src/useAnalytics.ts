import { useEffect, useRef, useState } from 'react';
import { ARTICLE_LIMIT, DashboardError, EMPTY_DATES, fetchArticles, fetchDaily, fetchPublishers, fetchQuality, MAX_OFFSET } from './api';
import type { ArticlesResponse, DailyResponse, DateFilters, PublishersResponse, QualityResponse, View } from './api';

type Success = { view: 'publishers'; response: PublishersResponse } | { view: 'daily'; response: DailyResponse }
  | { view: 'quality'; response: QualityResponse } | { view: 'articles'; response: ArticlesResponse };
type Result = { key: string } & ((Success & { status: 'ready' }) | { status: 'error'; error: DashboardError });
type Catalog = { sources: string[]; bounded: boolean; fromPublishers: boolean };

/** One mounted app owns one pin, one active request, and a fail-closed publication-change latch. */
export function useAnalytics(enabled = true) {
  const [view, setView] = useState<View>('publishers');
  const [sourceId, updateSource] = useState('');
  const [dates, setDates] = useState<DateFilters>(EMPTY_DATES);
  const [offset, setOffset] = useState(0);
  const [attempt, setAttempt] = useState(0);
  const [result, setResult] = useState<Result | null>(null);
  const [catalog, setCatalog] = useState<Catalog | null>(null);
  const [publication, setPublication] = useState<string | null>(null);
  const [blocked, setBlocked] = useState(false);
  const pin = useRef<string | null>(null);
  const dateView = view === 'daily' || view === 'quality';
  const from = dateView ? dates.from : '', to = dateView ? dates.to : '';
  const pageOffset = view === 'articles' ? offset : 0;
  const key = JSON.stringify([view, sourceId, from, to, pageOffset, attempt]);

  useEffect(() => {
    if (!enabled || blocked) return;
    const controller = new AbortController();
    let active = true;
    async function load(): Promise<Success> {
      switch (view) {
        case 'publishers': return { view, response: await fetchPublishers(sourceId, controller.signal) };
        case 'daily': return { view, response: await fetchDaily(sourceId, controller.signal, { from, to }) };
        case 'quality': return { view, response: await fetchQuality(sourceId, controller.signal, { from, to }) };
        case 'articles': return { view, response: await fetchArticles(sourceId, controller.signal, pageOffset) };
      }
    }
    void load().then((success) => {
      // An obsolete request cannot pin, poison, or replace the current session, even if fetch ignores abort.
      if (!active || controller.signal.aborted) return;
      if (pin.current !== null && pin.current !== success.response.publication) {
        setBlocked(true);
        return;
      }
      pin.current = success.response.publication;
      setPublication(success.response.publication);
      if (sourceId === '') {
        const sources = [...new Set(success.response.data.map((row) => row.sourceId))];
        setCatalog((previous) => {
          // Prefer the publisher inventory; if it was cancelled before completion, use observed sources until it loads.
          if (success.view !== 'publishers' && previous?.fromPublishers) return previous;
          return { sources, bounded: success.response.meta.returned === success.response.meta.limit,
            fromPublishers: success.view === 'publishers' };
        });
      }
      setResult({ key, status: 'ready', ...success });
    }).catch((error: unknown) => {
      if (!active || controller.signal.aborted) return;
      setResult({ key, status: 'error', error: error instanceof DashboardError ? error : new DashboardError('network', view) });
    });
    return () => { active = false; controller.abort(); };
  }, [view, sourceId, from, to, pageOffset, key, blocked, enabled]);

  // Request keys hide old results synchronously, before effect cleanup. A changed pin blocks every view until reload.
  const current = blocked ? { status: 'error' as const, error: new DashboardError('publication-changed') }
    : result?.key === key ? result : null;
  function setSourceId(source: string) { updateSource(source); setOffset(0); }
  function movePage(next: number) {
    if (!blocked && Number.isInteger(next) && next >= 0 && next <= MAX_OFFSET) setOffset(next);
  }
  return { view, setView, sourceId, setSourceId, dates, setDates, offset, movePage, pageSize: ARTICLE_LIMIT,
    catalog, publication, blocked, current, retry: () => setAttempt((value) => value + 1) };
}
