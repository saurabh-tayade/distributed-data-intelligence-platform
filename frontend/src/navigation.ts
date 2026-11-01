import { useSyncExternalStore } from 'react';

export type WorkspaceRoute = 'analytics' | 'sources' | 'not-found';

export function parseWorkspaceRoute(hash: string): WorkspaceRoute {
  if (['', '#', '#/analytics', '#main', '#publisher-details', '#daily-details', '#quality-details'].includes(hash)) return 'analytics';
  if (hash === '#/operations' || hash === '#/operations/sources') return 'sources';
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
  return parseWorkspaceRoute(hash);
}
