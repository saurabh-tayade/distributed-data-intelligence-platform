import { useEffect, useRef } from 'react';
import { AnalyticsWorkspace, analyticsViews } from './AnalyticsWorkspace';
import { AppShell } from './components/AppShell';
import { useWorkspaceRoute } from './navigation';
import { SourceList } from './operations/SourceList';
import { SourceDetail } from './operations/SourceDetail';
import { RunDetail } from './operations/RunDetail';
import { RunList } from './operations/RunList';
import { useAnalytics } from './useAnalytics';
import { SourceRegistration } from './operations/SourceEditor';
import { SourceCommandPanel } from './operations/SourceCommandPanel';
import { ReliabilitySubmission } from './operations/ReliabilitySubmission';
import { RunSubmissionPanel } from './operations/RunSubmissionPanel';
import { useReliability } from './useReliability';
import { ContentWorkspace } from './ContentWorkspace';
import { useContent } from './useContent';
import { ReliabilityWorkspace } from './ReliabilityWorkspace';

export function App() {
  const route = useWorkspaceRoute();
  const source = typeof route === 'object' && route.kind === 'source-detail' ? route : null;
  const executions = typeof route === 'object' && route.kind === 'executions' ? route : null;
  const inspection = typeof route === 'object' && route.kind === 'run-detail' ? route : null;
  const executionContext = executions ?? inspection;
  const sourceArea = route === 'sources' || route === 'source-new' || !!source;
  const submission = route === 'run-submit';
  const operations = !!(sourceArea || executionContext || submission);
  const model = useAnalytics(route === 'analytics');
  const content = useContent(route === 'content');
  const reliability = useReliability(route === 'reliability');
  const analyticsArea = route === 'analytics' || route === 'content' || route === 'reliability';
  const previousRoute = useRef(route);
  const section = route === 'analytics' ? analyticsViews.find((item) => item.id === model.view)!.label
    : route === 'content' ? content.report === 'catalog' ? 'Enriched Content catalog' : content.report === 'source_discovery_daily' ? 'Daily source discovery' : 'Publisher reference coverage' : route === 'reliability' ? `Reliability ${reliability.report === 'request-windows' ? 'request windows' : reliability.report === 'input-coverage' ? 'input coverage' : 'reference coverage'}` : source ? 'Source detail' : route === 'source-new' ? 'Register source' : submission ? 'Submit Reliability Run' : inspection ? 'Run detail' : executions ? 'Executions' : route === 'sources' ? 'Sources' : 'Page not found';
  useEffect(() => {
    document.title = `${section} | DDIP`;
  }, [section]);
  useEffect(() => {
    if (previousRoute.current !== route) document.getElementById('main')?.focus();
    previousRoute.current = route;
  }, [route]);

  return <AppShell section={section} area={analyticsArea ? 'analytics' : operations ? 'operations' : 'none'} product={route === 'reliability' ? 'Application Reliability' : 'Content Intelligence'}>
    {analyticsArea && <nav className="operations-navigation" aria-label="Analytics products">
      <a href="#/analytics" aria-current={route === 'analytics' ? 'page' : undefined}>Content Intelligence</a>
      <a href="#/content" aria-current={route === 'content' ? 'page' : undefined}>Enriched Content catalog</a>
      <a href="#/analytics/reliability" aria-current={route === 'reliability' ? 'page' : undefined}>Application Reliability</a>
    </nav>}
    {operations && <nav className="operations-navigation" aria-label="Operations views">
      <a href="#/operations/sources" aria-current={sourceArea ? 'page' : undefined}>Sources</a>
      <a href="#/operations/executions" aria-current={executionContext ? 'page' : undefined}>Executions</a>
      <a href="#/operations/submit" aria-current={submission ? 'page' : undefined}>Submit Reliability Run</a>
    </nav>}
    <SourceCommandPanel key={JSON.stringify(route)} visible={operations} />
    <RunSubmissionPanel visible={operations} />
    {/* Keep the Analytics model and publication pin across workspace navigation. */}
    <div hidden={route !== 'analytics'}><AnalyticsWorkspace model={model} /></div>
    <div hidden={route !== 'content'}><ContentWorkspace model={content} /></div>
    <div hidden={route !== 'reliability'}><ReliabilityWorkspace model={reliability} /></div>
    <div hidden={route !== 'sources'}><SourceList active={route === 'sources'} /></div>
    {route === 'source-new' && <SourceRegistration />}
    {submission && <ReliabilitySubmission />}
    {source && <SourceDetail key={`${source.productId}:${source.sourceId}`} identity={source} />}
    {executionContext && <div hidden={!executions}><RunList key={JSON.stringify(executionContext.query)} query={executionContext.query} active={!!executions} /></div>}
    {inspection && <RunDetail key={inspection.runId} runId={inspection.runId} returnQuery={inspection.query} />}
    {route === 'not-found' && <section className="state-panel" aria-labelledby="not-found-title">
      <h1 id="not-found-title">Page not found</h1><p>Choose Analytics or Operations from the workspace navigation.</p>
    </section>}
  </AppShell>;
}
