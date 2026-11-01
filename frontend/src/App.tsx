import { useEffect, useRef } from 'react';
import { AnalyticsWorkspace, analyticsViews } from './AnalyticsWorkspace';
import { AppShell } from './components/AppShell';
import { useWorkspaceRoute } from './navigation';
import { SourceList } from './operations/SourceList';
import { SourceDetail } from './operations/SourceDetail';
import { RunDetail } from './operations/RunDetail';
import { RunList } from './operations/RunList';
import { useAnalytics } from './useAnalytics';

export function App() {
  const route = useWorkspaceRoute();
  const source = typeof route === 'object' && route.kind === 'source-detail' ? route : null;
  const executions = typeof route === 'object' && route.kind === 'executions' ? route : null;
  const inspection = typeof route === 'object' && route.kind === 'run-detail' ? route : null;
  const executionContext = executions ?? inspection;
  const model = useAnalytics(route === 'analytics');
  const previousRoute = useRef(route);
  const section = route === 'analytics' ? analyticsViews.find((item) => item.id === model.view)!.label
    : source ? 'Source detail' : inspection ? 'Run detail' : executions ? 'Executions' : route === 'sources' ? 'Sources' : 'Page not found';
  useEffect(() => {
    document.title = `${section} | DDIP`;
  }, [section]);
  useEffect(() => {
    if (previousRoute.current !== route) document.getElementById('main')?.focus();
    previousRoute.current = route;
  }, [route]);

  return <AppShell section={section} area={route === 'analytics' ? 'analytics' : route === 'sources' || source || executionContext ? 'operations' : 'none'}>
    {(route === 'sources' || source || executionContext) && <nav className="operations-navigation" aria-label="Operations views">
      <a href="#/operations/sources" aria-current={route === 'sources' || source ? 'page' : undefined}>Sources</a>
      <a href="#/operations/executions" aria-current={executionContext ? 'page' : undefined}>Executions</a>
    </nav>}
    {/* Keep the Analytics model and publication pin across workspace navigation. */}
    <div hidden={route !== 'analytics'}><AnalyticsWorkspace model={model} /></div>
    <div hidden={route !== 'sources'}><SourceList active={route === 'sources'} /></div>
    {source && <SourceDetail key={`${source.productId}:${source.sourceId}`} identity={source} />}
    {executionContext && <div hidden={!executions}><RunList key={JSON.stringify(executionContext.query)} query={executionContext.query} active={!!executions} /></div>}
    {inspection && <RunDetail key={inspection.runId} runId={inspection.runId} returnQuery={inspection.query} />}
    {route === 'not-found' && <section className="state-panel" aria-labelledby="not-found-title">
      <h1 id="not-found-title">Page not found</h1><p>Choose Analytics or Operations from the workspace navigation.</p>
    </section>}
  </AppShell>;
}
