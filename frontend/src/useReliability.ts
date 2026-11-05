import { useEffect, useRef, useState } from 'react';
import { fetchRequestWindows, ReliabilityError, RELIABILITY_MAX_OFFSET, reliabilityQueryError } from './reliabilityApi';
import type { ReliabilityQuery, ReliabilityResponse } from './reliabilityApi';
import { coverageQueryError, fetchCoverage, reliabilityIdentity } from './reliabilityCoverageApi';
import type { AnyCoverageResponse, CoverageQuery, CoverageReport } from './reliabilityCoverageApi';

type Report = 'request-windows' | CoverageReport;
type Result = { key: string } & ({ status: 'ready'; response: ReliabilityResponse | AnyCoverageResponse } | { status: 'error'; error: ReliabilityError });

/** All Reliability reports share one complete identity pin and irreversible session latch. */
export function useReliability(enabled: boolean) {
  const [state, setState] = useState<{ report: Report; query: ReliabilityQuery | null; coverageQueries: Record<CoverageReport, CoverageQuery>; attempt: number }>(
    { report: 'request-windows', query: null, coverageQueries: { 'input-coverage': {}, 'reference-coverage': {} }, attempt: 0 });
  const [result, setResult] = useState<Result | null>(null);
  const [publication, setPublication] = useState<string | null>(null);
  const [blocked, setBlocked] = useState(false);
  const pin = useRef<string | null>(null);
  const { query, report } = state;
  const activeQuery = report === 'request-windows' ? query : state.coverageQueries[report];
  const key = JSON.stringify(state);
  const offset = activeQuery?.offset ?? 0, limit = activeQuery?.limit ?? 100;

  useEffect(() => {
    if (!enabled || blocked || activeQuery === null) return;
    const controller = new AbortController();
    let active = true;
    const request = report === 'request-windows' ? fetchRequestWindows(activeQuery as ReliabilityQuery, controller.signal)
      : fetchCoverage(report, activeQuery, controller.signal);
    void request.then((response) => {
      // Superseded requests cannot pin, poison or replace the current report.
      if (!active || controller.signal.aborted) return;
      const identity = reliabilityIdentity(response);
      if (pin.current !== null && pin.current !== identity) {
        setResult(null); setPublication(null); setBlocked(true);
        return;
      }
      pin.current = identity;
      setPublication(response.publication);
      setResult({ key, status: 'ready', response });
    }).catch((error: unknown) => {
      if (!active || controller.signal.aborted) return;
      setResult({ key, status: 'error', error: error instanceof ReliabilityError ? error : new ReliabilityError('network') });
    });
    return () => { active = false; controller.abort(); setResult(null); };
  }, [enabled, blocked, activeQuery, report, key]);

  const visible = blocked ? { status: 'error' as const, error: new ReliabilityError('publication-changed') }
    : enabled && result?.key === key ? result : null;
  const current = report === 'request-windows' ? visible as ({ status: 'ready'; response: ReliabilityResponse } | { status: 'error'; error: ReliabilityError } | null) : null;
  const coverageCurrent = report !== 'request-windows' ? visible as ({ status: 'ready'; response: AnyCoverageResponse } | { status: 'error'; error: ReliabilityError } | null) : null;
  function apply(filters: ReliabilityQuery) {
    if (blocked) return;
    const first = { ...filters, offset: 0 };
    if (reliabilityQueryError(first)) return;
    setState((old) => ({ ...old, query: first, attempt: old.attempt + 1 }));
  }
  function applyCoverage(filters: CoverageQuery) {
    if (blocked || report === 'request-windows') return;
    const first = { ...filters, offset: 0 };
    if (coverageQueryError(report, first)) return;
    setState(old => ({ ...old, coverageQueries: { ...old.coverageQueries, [report]: first }, attempt: old.attempt + 1 }));
  }
  function selectReport(next: Report) { if (!blocked && next !== report) setState(old => ({ ...old, report: next, attempt: old.attempt + 1 })); }
  function movePage(next: number) {
    if (blocked || activeQuery === null || visible === null || !Number.isInteger(next) || next < 0 || next > RELIABILITY_MAX_OFFSET) return;
    setState(old => old !== state ? old : report === 'request-windows' ? { ...old, query: { ...query!, offset: next } }
      : { ...old, coverageQueries: { ...old.coverageQueries, [report]: { ...activeQuery, offset: next } } });
  }
  function retry() { if (!blocked) setState((old) => ({ ...old, attempt: old.attempt + 1 })); }
  return { query, offset, limit, current, publication, blocked, apply, movePage, retry,
    report, selectReport, coverageQuery: report === 'request-windows' ? {} : state.coverageQueries[report], coverageCurrent, applyCoverage };
}
