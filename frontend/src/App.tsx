import { AppShell } from './components/AppShell';
import { PublicationIndicator } from './components/PublicationIndicator';
import { PublisherFilter } from './components/PublisherFilter';
import { SummaryCards } from './components/SummaryCards';
import { PublisherComparisonChart } from './components/PublisherComparisonChart';
import { PublisherTable } from './components/PublisherTable';
import { EmptyState, ErrorState, LoadingState } from './components/States';
import { usePublishers } from './usePublishers';

export function App() {
  const { sourceId, setSourceId, catalog, current, retry } = usePublishers();
  const response = current?.status === 'ready' ? current.response : null;
  return <AppShell>
    <div className="page-heading"><div><p className="eyebrow">ANALYTICS WORKSPACE <span aria-hidden="true">/</span> GOLD LAYER</p>
      <h1>Publisher analytics<span className="title-period">.</span></h1>
      <p className="page-description">A clear view of the content we’ve observed, grounded in one verified publication.</p></div>
      <span className="workspace-label"><span aria-hidden="true">▤</span> Content overview</span>
    </div>
    <PublicationIndicator publication={catalog?.publication ?? response?.publication ?? null} />
    <PublisherFilter sourceId={sourceId} sources={catalog?.sources ?? []} bounded={catalog?.bounded ?? false} onChange={setSourceId} />
    <div aria-busy={current === null}>
      {current === null && <LoadingState />}
      {current?.status === 'error' && <ErrorState error={current.error} onRetry={retry} />}
      {response && <>
        <div className="result-announcement visually-hidden" role="status">{response.meta.returned} {response.meta.returned === 1 ? 'publisher' : 'publishers'} returned for {sourceId || 'all returned publishers'}.</div>
        {response.meta.returned === response.meta.limit && <p className="scope-notice" role="note"><strong>Bounded results.</strong> The {response.meta.limit}-row limit was reached. Additional publishers may exist; these are returned-scope totals, not global totals.</p>}
        <SummaryCards rows={response.data} />
        {response.data.length === 0 ? <EmptyState filtered={sourceId !== ''} /> : <>
          <div className="analysis-grid"><PublisherComparisonChart rows={response.data} />
            <aside className="metric-guide" aria-labelledby="guide-title"><span className="eyebrow">BEHIND THE NUMBERS</span>
              <h2 id="guide-title">Content, in context.</h2>
              <p>A publisher can have more observations than articles. The same article may be observed in multiple ingestions.</p>
              <dl><div><dt>Articles</dt><dd>Catalog articles identified within each publisher.</dd></div>
                <div><dt>Observations</dt><dd>Accepted content observations represented by those articles.</dd></div>
                <div><dt>Without publication date</dt><dd>Articles without a publication timestamp. This is not a warning count.</dd></div></dl>
              <p className="guide-callout">Observations are not views, clicks, or user engagement.</p>
            </aside></div>
          <PublisherTable rows={response.data} />
        </>}
      </>}
    </div>
  </AppShell>;
}
