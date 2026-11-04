import { useEffect, useRef, useState } from 'react';
import { fetchRequestWindows, ReliabilityError, RELIABILITY_MAX_OFFSET, reliabilityQueryError } from './reliabilityApi';
import type { ReliabilityQuery, ReliabilityResponse } from './reliabilityApi';

type Result = { key: string } & ({ status: 'ready'; response: ReliabilityResponse } | { status: 'error'; error: ReliabilityError });

/** Reliability owns its pin and fail-closed latch, independently of Content Gold. */
export function useReliability(enabled: boolean) {
  const [state, setState] = useState<{ query: ReliabilityQuery | null; attempt: number }>({ query: null, attempt: 0 });
  const [result, setResult] = useState<Result | null>(null);
  const [publication, setPublication] = useState<string | null>(null);
  const [blocked, setBlocked] = useState(false);
  const pin = useRef<string | null>(null);
  const { query } = state;
  const key = JSON.stringify(state);
  const offset = query?.offset ?? 0, limit = query?.limit ?? 100;

  useEffect(() => {
    if (!enabled || blocked || query === null) return;
    const controller = new AbortController();
    let active = true;
    void fetchRequestWindows(query, controller.signal).then((response) => {
      // A superseded request must not pin, poison or replace the visible selection.
      if (!active || controller.signal.aborted) return;
      if (pin.current !== null && pin.current !== response.publication) {
        setBlocked(true);
        return;
      }
      pin.current = response.publication;
      setPublication(response.publication);
      setResult({ key, status: 'ready', response });
    }).catch((error: unknown) => {
      if (!active || controller.signal.aborted) return;
      setResult({ key, status: 'error', error: error instanceof ReliabilityError ? error : new ReliabilityError('network') });
    });
    return () => { active = false; controller.abort(); setResult(null); };
  }, [enabled, blocked, query, key]);

  const current = blocked ? { status: 'error' as const, error: new ReliabilityError('publication-changed') }
    : enabled && result?.key === key ? result : null;
  function apply(filters: ReliabilityQuery) {
    if (blocked) return;
    const first = { ...filters, offset: 0 };
    if (reliabilityQueryError(first)) return;
    setState((old) => ({ query: first, attempt: old.attempt + 1 }));
  }
  function movePage(next: number) {
    if (blocked || query === null || current === null || !Number.isInteger(next) || next < 0 || next > RELIABILITY_MAX_OFFSET) return;
    setState((old) => old === state ? { ...old, query: { ...query, offset: next } } : old);
  }
  function retry() {
    if (!blocked) setState((old) => ({ ...old, attempt: old.attempt + 1 }));
  }
  return { query, offset, limit, current, publication, blocked, apply, movePage, retry };
}
