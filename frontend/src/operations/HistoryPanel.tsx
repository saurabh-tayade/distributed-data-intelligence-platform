import type { ReactNode } from 'react';
import { InspectionError } from './InspectionError';
import type { useRunHistory } from './useRunHistory';

export function HistoryPanel<T>({ title, label, model, children, description, suspended = false, refreshWhileLoading = false }: {
  title: string; label: string; model: ReturnType<typeof useRunHistory<T>>; children: ReactNode; description?: ReactNode; suspended?: boolean; refreshWhileLoading?: boolean;
}) {
  const page = model.current?.status === 'ready' ? model.current.page : null;
  return <section className="panel source-detail-panel" aria-label={title}>
    <div className="panel-heading"><h2>{title}</h2><button className="secondary-button" type="button" disabled={suspended || (model.current === null && !refreshWhileLoading)} onClick={() => model.refresh()}>Refresh {label}</button></div>
    {description ?? <p className="inspection-note">One high-water page in server order. Refresh starts a new traversal; run refresh does not merge or refresh this history.</p>}
    <label className="history-limit">{title} per page<select value={model.limit} disabled={model.current === null} onChange={(event) => model.changeLimit(Number(event.target.value))}>
      {[5, 10, 20, 50, 100].map((limit) => <option key={limit} value={limit}>{limit}</option>)}
    </select></label>
    <div aria-busy={!suspended && model.current === null}>
      {suspended ? <p className="inspection-note" role="status">Stage reads are suspended while the document is hidden.</p>
        : model.current === null && <p className="inspection-note" role="status">Loading {label}</p>}
      {model.current?.status === 'error' && <InspectionError title={`Unable to load ${label}`} error={model.current.error}
        retry={() => model.refresh(false)} restart={model.hasCursor ? () => model.refresh() : undefined} />}
      {page && page.data.length === 0 && <p className="inspection-note" role="status">No {label} returned on this page.</p>}
      {page && page.data.length > 0 && children}
    </div>
    <nav className="source-pagination" aria-label={`${title} pages`}>
      <button className="secondary-button" type="button" disabled={model.current === null || !model.canGoBack} onClick={model.back}>Previous {label}</button>
      <p role="status">Page {model.pageNumber}{page ? ` · ${page.data.length} returned${page.meta.hasMore ? ' · More available' : ' · End of results'}` : ''}</p>
      <button className="secondary-button" type="button" disabled={page === null || !page.meta.hasMore} onClick={model.next}>Next {label}</button>
    </nav>
    {model.historyTruncated && <p className="inspection-note">Only 20 cursor positions are retained. Refresh to restart from the first page.</p>}
  </section>;
}
