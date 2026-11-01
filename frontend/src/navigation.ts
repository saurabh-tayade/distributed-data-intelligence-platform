import { useMemo, useSyncExternalStore } from 'react';
import { isSourceIdentity } from './managementApi';
import type { SourceIdentity } from './managementApi';

export type WorkspaceRoute = 'analytics' | 'sources' | 'not-found' | (SourceIdentity & { kind: 'source-detail' });

export function sourceHref(source: SourceIdentity): string {
  return `#/operations/sources/${encodeURIComponent(source.productId)}/${encodeURIComponent(source.sourceId)}`;
}

export function parseWorkspaceRoute(hash: string): WorkspaceRoute {
  if (['', '#', '#/analytics', '#main', '#publisher-details', '#daily-details', '#quality-details'].includes(hash)) return 'analytics';
  if (hash === '#/operations' || hash === '#/operations/sources') return 'sources';
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
