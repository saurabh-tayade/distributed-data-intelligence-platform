import { useMemo, useSyncExternalStore } from 'react';
import { isSourceIdentity, runsParameters } from './managementApi';
import type { RunsQuery, SourceIdentity } from './managementApi';

export type ExecutionFilters = Omit<RunsQuery, 'cursor'>;
export type WorkspaceRoute = 'analytics' | 'sources' | 'not-found' | (SourceIdentity & { kind: 'source-detail' })
  | { kind: 'executions'; query: ExecutionFilters };

/** Only the four supported execution filters live in the hash; cursors stay in memory. */
export function executionsHref(query: ExecutionFilters = {}): string {
  const search = runsParameters(query);
  if (search.get('limit') === '20') search.delete('limit');
  return '#/operations/executions' + (search.size ? `?${search}` : '');
}

export function sourceHref(source: SourceIdentity): string {
  return `#/operations/sources/${encodeURIComponent(source.productId)}/${encodeURIComponent(source.sourceId)}`;
}

export function parseWorkspaceRoute(hash: string): WorkspaceRoute {
  if (['', '#', '#/analytics', '#main', '#publisher-details', '#daily-details', '#quality-details'].includes(hash)) return 'analytics';
  if (hash === '#/operations' || hash === '#/operations/sources') return 'sources';
  const executions = /^#\/operations\/executions(?:\?([^#]*))?$/.exec(hash);
  if (executions) {
    const search = new URLSearchParams(executions[1]);
    if ([...search.keys()].some((key) => !['productId', 'sourceId', 'state', 'limit'].includes(key) || search.getAll(key).length !== 1)) return 'not-found';
    const query: ExecutionFilters = {};
    if (search.has('productId')) query.productId = search.get('productId') as NonNullable<RunsQuery['productId']>;
    if (search.has('sourceId')) query.sourceId = search.get('sourceId')!;
    if (search.has('state')) query.state = search.get('state') as NonNullable<RunsQuery['state']>;
    if (search.has('limit')) {
      if (!/^[1-9][0-9]{0,2}$/.test(search.get('limit')!)) return 'not-found';
      query.limit = Number(search.get('limit'));
    }
    try { runsParameters(query); return { kind: 'executions', query }; } catch { return 'not-found'; }
  }
  const match = /^#\/operations\/sources\/([^/]+)\/([^/]+)$/.exec(hash);
  if (match) {
    try {
      const identity = { productId: decodeURIComponent(match[1]!), sourceId: decodeURIComponent(match[2]!) };
      if (isSourceIdentity(identity)) return { kind: 'source-detail', ...identity };
    } catch { /* Malformed encoded identities use the navigation fallback. */ }
  }
  return 'not-found';
}

function subscribe(listener: () => void) {
  window.addEventListener('hashchange', listener);
  window.addEventListener('popstate', listener);
  return () => {
    window.removeEventListener('hashchange', listener);
    window.removeEventListener('popstate', listener);
  };
}

export function useWorkspaceRoute(): WorkspaceRoute {
  const hash = useSyncExternalStore(subscribe, () => window.location.hash, () => '');
  return useMemo(() => parseWorkspaceRoute(hash), [hash]);
}
