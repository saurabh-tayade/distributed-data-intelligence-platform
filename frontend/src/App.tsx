import { lazy, Suspense } from 'react';
import type { View } from './api';
import { AppShell } from './components/AppShell';
import { PublicationIndicator } from './components/PublicationIndicator';
import { PublisherFilter } from './components/PublisherFilter';
import { DateFilters } from './components/DateFilters';
import { ErrorState, LoadingState } from './components/States';
import { useAnalytics } from './useAnalytics';
import { PublisherOverview } from './views/PublisherOverview';

const DailyActivity = lazy(() => import('./views/DailyActivity'));
const DataQuality = lazy(() => import('./views/DataQuality'));
const ArticleExplorer = lazy(() => import('./views/ArticleExplorer'));
const views: { id: View; label: string; title: string; description: string }[] = [
  { id: 'publishers', label: 'Publisher Overview', title: 'Publisher analytics', description: 'A clear view of the content we’ve observed, grounded in one verified publication.' },
  { id: 'daily', label: 'Daily Activity', title: 'Daily activity', description: 'Accepted content observations by UTC ingestion date. Each date reflects the data actually returned.' },
  { id: 'quality', label: 'Data Quality', title: 'Data quality', description: 'Warning codes across accepted observations. Counts and rates, with their scope kept explicit.' },
  { id: 'articles', label: 'Article Explorer', title: 'Article explorer', description: 'Explore the selected article catalog, one verified page at a time.' },
];

export function App() {
  const model = useAnalytics();
  const { view, sourceId, catalog, current, retry } = model;
  const selected = views.find((item) => item.id === view)!;
  const loadingLabel = view === 'publishers' ? 'publisher analytics' : selected.title.toLowerCase();
  return <AppShell section={selected.label}>
    <div className="page-heading"><div><p className="eyebrow">ANALYTICS WORKSPACE <span aria-hidden="true">/</span> GOLD LAYER</p>
      <h1>{selected.title}<span className="title-period">.</span></h1>
      <p className="page-description">{selected.description}</p></div>
      <span className="workspace-label"><span aria-hidden="true">▤</span> Content overview</span>
    </div>
    <PublicationIndicator publication={model.publication} />
    {!model.blocked && <PublisherFilter sourceId={sourceId} sources={catalog?.sources ?? []} bounded={catalog?.bounded ?? false}
      rowLimit={view === 'articles' ? model.pageSize : 100}
      {...(catalog && !catalog.fromPublishers ? { catalogNote: 'Selector lists sources seen in returned rows. Visit Publisher Overview for its bounded inventory.' } : {})}
      onChange={model.setSourceId} />}
    <nav className="view-navigation" aria-label="Analytics views">
      {views.map((item, index) => <button key={item.id} type="button" aria-current={view === item.id ? 'page' : undefined}
        onClick={() => model.setView(item.id)}><span aria-hidden="true">0{index + 1}</span>{item.label}</button>)}
    </nav>
    {!model.blocked && (view === 'daily' || view === 'quality') && <DateFilters key={view} applied={model.dates} onApply={model.setDates} />}
    <div aria-busy={current === null}>
      {current === null && <LoadingState label={loadingLabel} />}
      {current?.status === 'error' && <ErrorState error={current.error} onRetry={retry} label={loadingLabel} />}
      <Suspense fallback={current === null ? null : <LoadingState label={loadingLabel} />}>
        {current?.status === 'ready' && current.view === 'publishers' && <PublisherOverview response={current.response} sourceId={sourceId} />}
        {current?.status === 'ready' && current.view === 'daily' && <DailyActivity response={current.response} />}
        {current?.status === 'ready' && current.view === 'quality' && <DataQuality response={current.response} />}
        {view === 'articles' && !model.blocked && <ArticleExplorer
          response={current?.status === 'ready' && current.view === 'articles' ? current.response : null}
          offset={model.offset} limit={model.pageSize} loading={current === null} onPage={model.movePage} />}
      </Suspense>
    </div>
  </AppShell>;
}
