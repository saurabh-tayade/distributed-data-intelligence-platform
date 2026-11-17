import type { ReactNode } from 'react';
import type { ManagementPage, RevisionQuery } from '../managementApi';
import { InspectionError } from './InspectionError';
import { useSubmissionPage } from './useSubmissionPage';

export function SubmissionCatalog<T>({ title, scope, active, load, empty, children }: {
  title: string; scope: string; active: boolean; load: (query: RevisionQuery, signal: AbortSignal) => Promise<ManagementPage<T>>;
  empty: string; children: (rows: T[]) => ReactNode;
}) {
  const model = useSubmissionPage(scope, active, load), current = model.current;
  return <section className="panel submission-catalog" aria-label={title}>
    <h2>{title}</h2>
    <button type="button" className="secondary-button" disabled={!active} onClick={model.refresh}>Refresh {title.toLowerCase()}</button>
    {active && current === null && <p role="status">Loading {title.toLowerCase()}…</p>}
    {active && current?.status === 'error' && <InspectionError title={`Unable to load ${title.toLowerCase()}`} error={current.error} retry={model.retry} restart={model.refresh} />}
    {active && current?.status === 'ready' && (current.page.data.length ? children(current.page.data) : <p role="status">{empty}</p>)}
    <nav className="source-pagination" aria-label={`${title} pages`}>
      <button type="button" className="secondary-button" disabled={!active || !current || !model.canGoBack} onClick={model.back}>Previous {title.toLowerCase()}</button>
      {current?.status === 'ready' && <p role="status">{current.page.data.length} returned · {current.page.meta.hasMore ? 'More available' : 'End of results'}</p>}
      <button type="button" className="secondary-button" disabled={!active || current?.status !== 'ready' || !current.page.meta.hasMore} onClick={model.next}>Next {title.toLowerCase()}</button>
    </nav>
    {model.truncated && <p>Only the latest 20 cursor positions are retained. Refresh to restart from the first page.</p>}
  </section>;
}
