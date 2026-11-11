import { useState } from 'react';
import { CONTENT_MAX_OFFSET, CONTENT_VOCABULARY, contentReportQueryError } from './contentApi';
import type { ContentQuery, ContentResponse, ContentReport, ContentDailyResponse, ContentCoverageResponse, ContentCoverageTotals } from './contentApi';
import type { useContent } from './useContent';
import { safeArticleUrl } from './articleLinks';
import { ErrorState } from './components/States';
import { Timestamp } from './components/Timestamp';
import { formatCount } from './metrics';

const labels = { referenceStatus: 'Reference status', publisherCategory: 'Publisher category', declaredLanguage: 'Declared language', countryCode: 'Country code' };
const reportLabels = { catalog: 'catalog', source_discovery_daily: 'daily discovery', reference_coverage: 'reference coverage' };
function ContentFilters({ report, onApply }: { report: ContentReport; onApply: (query: ContentQuery) => void }) {
  const [draft, setDraft] = useState({ sourceId: '', from: '', to: '', referenceStatus: '', publisherCategory: '', declaredLanguage: '', countryCode: '', limit: report === 'catalog' ? 10 : 100 });
  const [error, setError] = useState<string | null>(null);
  return <form className="source-filters execution-filters" aria-label={`Content ${reportLabels[report]} filters`} noValidate onSubmit={(event) => {
    event.preventDefault();
    const query = Object.fromEntries(Object.entries(draft).filter(([, value]) => value !== '')) as ContentQuery;
    const invalid = contentReportQueryError(report, query); setError(invalid); if (!invalid) onApply(query);
  }}>
    <label>Exact source ID<input value={draft.sourceId} autoCapitalize="none" autoCorrect="off" spellCheck={false}
      onChange={(event) => { setDraft({ ...draft, sourceId: event.target.value }); setError(null); }} /></label>
    {report !== 'reference_coverage' && (['from', 'to'] as const).map((key) => <label key={key}>{key === 'from' ? 'Discovery from (inclusive UTC)' : 'Discovery to (inclusive UTC)'}
      <input type="text" placeholder="YYYY-MM-DD" value={draft[key]} aria-describedby="content-date-help" onChange={(event) => { setDraft({ ...draft, [key]: event.target.value }); setError(null); }} /></label>)}
    {report === 'catalog' && (Object.keys(CONTENT_VOCABULARY) as (keyof typeof CONTENT_VOCABULARY)[]).map((key) => <label key={key}>{labels[key]}
      <select value={draft[key]} onChange={(event) => { setDraft({ ...draft, [key]: event.target.value }); setError(null); }}>
        <option value="">All</option>{CONTENT_VOCABULARY[key].map((value) => <option key={value} value={value}>{value}</option>)}
      </select></label>)}
    <label>{report === 'catalog' ? 'Articles per page' : 'Rows per page'}<select value={draft.limit} onChange={(event) => setDraft({ ...draft, limit: Number(event.target.value) })}>
      {[10, 25, 100].map((n) => <option key={n} value={n}>{n}</option>)}</select></label>
    <button className="primary-button" type="submit">Apply {reportLabels[report]} filters</button>
    <p id="content-date-help">{report === 'reference_coverage' ? 'Coverage spans the entire selected catalog and reference snapshot. Date filters do not apply.' : 'Discovery dates include both bounds, in UTC. Leave either bound blank for an open range.'} Values are not trimmed.</p>
    {error && <p role="alert" className="field-error">{error}</p>}
  </form>;
}
export function ContentCatalogRows({ response }: { response: ContentResponse }) {
  return <section aria-label="Enriched catalog results">
    {response.data.map((row) => {
      const href = safeArticleUrl(row.url);
      return <article key={row.articleId} className="panel article-card">
        <h2>{row.title}</h2><p className="article-summary content-summary">{row.summary ?? 'Summary not available'}</p>
        {href ? <a href={href} target="_blank" rel="noopener noreferrer">Open article in new tab</a> : <p>Article link not available</p>}
        <dl className="article-detail-grid">
          <div><dt>Source ID</dt><dd>{row.sourceId}</dd></div><div><dt>Article ID</dt><dd><code>{row.articleId}</code></dd></div>
          <div><dt>Identifier kind</dt><dd>{row.identifierKind}</dd></div><div><dt>Source article ID</dt><dd>{row.sourceArticleId}</dd></div>
          <div><dt>Feed format</dt><dd>{row.feedFormat}</dd></div><div><dt>Reference status</dt><dd>{row.referenceStatus}</dd></div>
          <div><dt>Publisher name</dt><dd>{row.publisherName ?? 'Not available'}</dd></div>
          <div><dt>Publisher category</dt><dd>{row.publisherCategory ?? 'Not available'}</dd></div>
          <div><dt>Declared language</dt><dd>{row.declaredLanguage ?? 'Not available'}</dd></div>
          <div><dt>Country code</dt><dd>{row.countryCode ?? 'Not available'}</dd></div>
          <div><dt>Discovery date (UTC)</dt><dd>{row.discoveryDate}</dd></div>
          <div><dt>Discovered at</dt><dd><Timestamp value={row.discoveredAt} /></dd></div>
          <div><dt>First observed</dt><dd><Timestamp value={row.firstObservedAt} /></dd></div>
          <div><dt>Last observed</dt><dd><Timestamp value={row.lastObservedAt} /></dd></div>
          <div><dt>Published at</dt><dd><Timestamp value={row.publishedAt} /></dd></div>
          <div><dt>Updated at</dt><dd><Timestamp value={row.updatedAt} /></dd></div>
          <div><dt>Recorded observations</dt><dd>{formatCount(row.observationCount)}</dd></div>
          <div><dt>Distinct ingestions</dt><dd>{formatCount(row.distinctIngestionCount)}</dd></div>
          <div><dt>Quality warnings</dt><dd>{row.qualityWarnings.length ? row.qualityWarnings.join(', ') : 'None'}</dd></div>
        </dl>
      </article>;
    })}
  </section>;
}
export function ContentWorkspace({ model }: { model: ReturnType<typeof useContent> }) {
  const { current, identity, query, blocked, report } = model;
  const response = current?.status === 'ready' ? current.response : null;
  const label = reportLabels[report];
  const limit = query.limit ?? (report === 'catalog' ? 10 : 100), offset = query.offset ?? 0, loading = current === null;
  return <section aria-label="Enriched Content catalog">
    <div className="page-heading"><div><p className="eyebrow">CONTENT INTELLIGENCE / ENRICHED CONTENT</p><h1>{report === 'catalog' ? 'Enriched Content catalog' : report === 'source_discovery_daily' ? 'Daily source discovery' : 'Publisher reference coverage'}</h1>
      <p className="page-description">Explore catalog articles, first-observed discovery groups and publisher-reference coverage within one pinned Content selection.</p></div></div>
    <section className="publication-panel content-selection" aria-label="Content selection identities">
      {identity ? <dl className="article-detail-grid">
        <div><dt>Content publication</dt><dd><code>{identity.publication}</code></dd></div>
        <div><dt>Base Gold selection</dt><dd><code>{identity.goldPublication}</code></dd></div>
        <div><dt>Publisher reference ID</dt><dd>{identity.reference.referenceId}</dd></div>
        <div><dt>Publisher reference revision</dt><dd>{identity.reference.revision}</dd></div>
        <div><dt>Publisher reference fingerprint</dt><dd><code>{identity.reference.snapshotFingerprint}</code></dd></div>
      </dl> : <p>Awaiting verified Content selection identities.</p>}
    </section>
    <p className="scope-notice">This Content selection is pinned independently from the existing Gold analytics view. Its base Gold selection may differ from that view.
      {' '}The local operator selects it at API startup. Completing an Operations run does not change this selection.</p>
    <p className="metric-explanation">Discovery means first observed within the selected history, not original publication or first appearance on the internet.
      Observations count recorded ingestion observations, not readership. Missing reference metadata does not remove articles.</p>
    <nav className="operations-navigation" aria-label="Content reports">
      {(Object.keys(reportLabels) as ContentReport[]).map((name) => <button type="button" key={name} aria-pressed={report === name} disabled={blocked} onClick={() => model.selectReport(name)}>{name === 'catalog' ? 'Enriched catalog' : name === 'source_discovery_daily' ? 'Daily discovery' : 'Reference coverage'}</button>)}
    </nav>
    {!blocked && <ContentFilters key={report} report={report} onApply={model.apply} />}
    {!blocked && <><section aria-label="Applied Content filters"><p>Applied filters: {Object.entries(query).filter(([key]) => key !== 'limit' && key !== 'offset').map(([key, value]) => `${key}: ${value}`).join(' · ') || (report === 'catalog' ? 'All catalog articles' : 'All sources')}</p></section>
      <div className="pagination-bar" aria-label={`Content ${label} pagination`}><p>{response ? `${response.meta.returned} returned rows` : 'Requested page'} · Offset {offset} · {limit} rows per page · {report === 'reference_coverage' ? 'Totals describe the full source-filtered domain' : 'No total supplied'}</p>
        <button type="button" disabled={loading || offset === 0} onClick={() => model.movePage(Math.max(0, offset - limit))}>Previous {label} page</button>
        <button type="button" disabled={loading || !response?.meta.hasMore || offset + limit > CONTENT_MAX_OFFSET} onClick={() => model.movePage(offset + limit)}>Next {label} page</button>
        <button type="button" onClick={model.retry}>Refresh {label} page</button>
      </div></>}
    <div aria-busy={loading}>
      {loading && <section className="state-panel" role="status"><h2>Loading Content {label}</h2><p>Waiting for verified Content and dependency identities.</p></section>}
      {current?.status === 'error' && <ErrorState error={current.error} onRetry={model.retry} label={`Content ${label}`} />}
      {response && report === 'reference_coverage' && <CoverageTotals totals={(response as ContentCoverageResponse).totals} />}
      {response && (response.data.length ? <><p className="scope-notice">{report === 'catalog' ? 'Returned page only, ordered by last observed time descending, then article ID ascending.' : report === 'source_discovery_daily' ? 'Returned page only, ordered by source ID then discovery date ascending (UTC).' : 'Returned page only, ordered by source ID ascending. USED means a referenced source with articles; UNUSED means a reference-only source; ABSENT means articles without a reference.'}{response.meta.hasMore ? ' More rows are available.' : ''}</p>
        {report === 'catalog' ? <ContentCatalogRows response={response as ContentResponse} /> : report === 'source_discovery_daily' ? <DiscoveryRows response={response as ContentDailyResponse} /> : <CoverageRows response={response as ContentCoverageResponse} />}</> : <section className="state-panel" role="status"><h2>{report === 'catalog' ? 'No articles in this catalog scope' : `No rows in this ${label} page`}</h2><p>Adjust the supported filters or return to a previous page.</p></section>)}
    </div>
  </section>;
}

const totalLabels: Record<keyof ContentCoverageTotals, string> = {
  articleCount: 'Catalog articles', matchedArticleCount: 'Articles with a reference', missingArticleCount: 'Articles without a reference',
  catalogSourceCount: 'Sources with articles', matchedSourceCount: 'Used matched sources', missingSourceCount: 'Catalog-only sources',
  matchedSourcesMissingOptionalAttributesCount: 'Used matched sources missing optional attributes', referenceSourceCount: 'Selected reference sources', unusedReferenceSourceCount: 'Unused reference sources',
};
function CoverageTotals({ totals }: { totals: ContentCoverageTotals }) {
  return <section className="panel" aria-label="Reference coverage totals"><h2>Reference coverage totals</h2><p>Whole source-filtered domain before pagination. Optional-attribute gaps count used matched sources, not articles or unused sources.</p>
    <dl className="article-detail-grid">{(Object.keys(totalLabels) as (keyof ContentCoverageTotals)[]).map((key) => <div key={key}><dt>{totalLabels[key]}</dt><dd>{formatCount(totals[key])}</dd></div>)}</dl></section>;
}
function DiscoveryRows({ response }: { response: ContentDailyResponse }) {
  return <div className="table-scroll"><table><caption>Daily source discovery</caption><thead><tr><th scope="col">Source ID</th><th scope="col">Discovery date (UTC)</th><th scope="col">Discovered articles</th><th scope="col">Articles missing a reference</th></tr></thead>
    <tbody>{response.data.map((row) => <tr key={`${row.sourceId}:${row.discoveryDate}`}><th scope="row">{row.sourceId}</th><td>{row.discoveryDate}</td><td>{formatCount(row.discoveredArticleCount)}</td><td>{formatCount(row.articlesMissingReferenceCount)}</td></tr>)}</tbody></table></div>;
}
function CoverageRows({ response }: { response: ContentCoverageResponse }) {
  return <div className="table-scroll"><table><caption>Publisher reference coverage</caption><thead><tr>{['Source ID', 'Reference status', 'Reference usage', 'Publisher name', 'Publisher category', 'Declared language', 'Country code', 'Articles', 'Articles missing a reference', 'Missing optional attributes'].map((label) => <th scope="col" key={label}>{label}</th>)}</tr></thead>
    <tbody>{response.data.map((row) => <tr key={row.sourceId}><th scope="row">{row.sourceId}</th><td>{row.referenceStatus}</td><td>{row.referenceUsage}</td><td>{row.publisherName ?? 'Not available'}</td><td>{row.publisherCategory ?? 'Not available'}</td><td>{row.declaredLanguage ?? 'Not available'}</td><td>{row.countryCode ?? 'Not available'}</td><td>{formatCount(row.articleCount)}</td><td>{formatCount(row.articlesMissingReferenceCount)}</td><td>{row.missingOptionalAttributes.length ? row.missingOptionalAttributes.join(', ') : row.referenceUsage === 'ABSENT' ? 'Not applicable (reference absent)' : 'None'}</td></tr>)}</tbody></table></div>;
}
