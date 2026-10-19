import type { DashboardError } from '../api';

export function LoadingState({ label = 'publisher analytics' }: { label?: string }) {
  return <section className="state-panel loading-state" role="status" aria-live="polite">
    <span className="loading-indicator" aria-hidden="true" /><h2>Loading {label}</h2>
    <p>Waiting for the API to verify the pinned Gold publication.</p>
  </section>;
}

export function ErrorState({ error, onRetry, label = 'publisher analytics' }: { error: DashboardError; onRetry: () => void; label?: string }) {
  return <section className="state-panel error-state" role="alert">
    <span className="state-icon" aria-hidden="true">!</span><h2>{error.kind === 'publication-changed' ? 'Publication changed' : `Unable to load ${label}`}</h2>
    <p>{error.message}</p>
    {error.kind === 'publication-changed'
      ? <button className="primary-button" onClick={() => window.location.reload()}>Reload dashboard</button>
      : <button className="primary-button" onClick={onRetry}>Try again</button>}
  </section>;
}

export function EmptyState({ filtered }: { filtered: boolean }) {
  return <section className="state-panel empty-state" role="status">
    <span className="state-icon" aria-hidden="true">∅</span><h2>No publishers in this scope</h2>
    <p>{filtered ? 'The API returned no rows for this publisher. Reset the filter to explore the returned publisher scope.'
      : 'This verified publication returned no publisher rows. No chart or publisher records are available.'}</p>
  </section>;
}

export function ReportEmptyState({ label }: { label: string }) {
  return <section className="state-panel empty-state" role="status"><span className="state-icon" aria-hidden="true">∅</span>
    <h2>No {label} in this scope</h2><p>The API returned no matching rows. Adjust the source or supported date filters to explore another scope.</p></section>;
}
