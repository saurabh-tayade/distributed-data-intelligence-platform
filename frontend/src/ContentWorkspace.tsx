import { useState } from 'react';
import { CONTENT_MAX_OFFSET, CONTENT_VOCABULARY, contentQueryError } from './contentApi';
import type { ContentQuery, ContentResponse } from './contentApi';
import type { useContent } from './useContent';
import { safeArticleUrl } from './articleLinks';
import { ErrorState } from './components/States';
import { Timestamp } from './components/Timestamp';
import { formatCount } from './metrics';

const labels = { referenceStatus: 'Reference status', publisherCategory: 'Publisher category', declaredLanguage: 'Declared language', countryCode: 'Country code' };
function ContentFilters({ onApply }: { onApply: (query: ContentQuery) => void }) {
  const [draft, setDraft] = useState({ sourceId: '', from: '', to: '', referenceStatus: '', publisherCategory: '', declaredLanguage: '', countryCode: '', limit: 10 });
  const [error, setError] = useState<string | null>(null);
  return <form className="source-filters execution-filters" aria-label="Content catalog filters" noValidate onSubmit={(event) => {
    event.preventDefault();
    const query = Object.fromEntries(Object.entries(draft).filter(([, value]) => value !== '')) as ContentQuery;
    const invalid = contentQueryError(query); setError(invalid); if (!invalid) onApply(query);
  }}>
    <label>Exact source ID<input value={draft.sourceId} autoCapitalize="none" autoCorrect="off" spellCheck={false}
      onChange={(event) => { setDraft({ ...draft, sourceId: event.target.value }); setError(null); }} /></label>
    {(['from', 'to'] as const).map((key) => <label key={key}>{key === 'from' ? 'Discovery from (inclusive UTC)' : 'Discovery to (inclusive UTC)'}
      <input type="text" placeholder="YYYY-MM-DD" value={draft[key]} aria-describedby="content-date-help" onChange={(event) => { setDraft({ ...draft, [key]: event.target.value }); setError(null); }} /></label>)}
    {(Object.keys(CONTENT_VOCABULARY) as (keyof typeof CONTENT_VOCABULARY)[]).map((key) => <label key={key}>{labels[key]}
      <select value={draft[key]} onChange={(event) => { setDraft({ ...draft, [key]: event.target.value }); setError(null); }}>
        <option value="">All</option>{CONTENT_VOCABULARY[key].map((value) => <option key={value} value={value}>{value}</option>)}
      </select></label>)}
    <label>Articles per page<select value={draft.limit} onChange={(event) => setDraft({ ...draft, limit: Number(event.target.value) })}>
      {[10, 25, 100].map((n) => <option key={n} value={n}>{n}</option>)}</select></label>
    <button className="primary-button" type="submit">Apply catalog filters</button>
    <p id="content-date-help">Discovery dates include both bounds, in UTC. Leave either bound blank for an open range. Values are not trimmed.</p>
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
  const { current, identity, query, blocked } = model;
  const response = current?.status === 'ready' ? current.response : null;
  const limit = query.limit ?? 10, offset = query.offset ?? 0, loading = current === null;
  return <section aria-label="Enriched Content catalog">
    <div className="page-heading"><div><p className="eyebrow">CONTENT INTELLIGENCE / ENRICHED CATALOG</p><h1>Enriched Content catalog</h1>
      <p className="page-description">One bounded page of articles enriched with a selected publisher-reference snapshot.</p></div></div>
    <section className="publication-panel content-selection" aria-label="Content selection identities">
      {identity ? <dl className="article-detail-grid">
        <div><dt>Content publication</dt><dd><code>{identity.publication}</code></dd></div>
        <div><dt>Base Gold selection</dt><dd><code>{identity.goldPublication}</code></dd></div>
        <div><dt>Publisher reference ID</dt><dd>{identity.reference.referenceId}</dd></div>
        <div><dt>Publisher reference revision</dt><dd>{identity.reference.revision}</dd></div>
        <div><dt>Publisher reference fingerprint</dt><dd><code>{identity.reference.snapshotFingerprint}</code></dd></div>
      </dl> : <p>Awaiting verified Content selection identities.</p>}
    </section>
    <p className="scope-notice">This Content selection is pinned independently from the existing Gold analytics view. Its base Gold selection may differ from that view.</p>
    <p className="metric-explanation">Discovery means first observed within the selected history, not original publication or first appearance on the internet.
      Observations count recorded ingestion observations, not readership. Missing reference metadata does not remove articles.</p>
    {!blocked && <ContentFilters onApply={model.apply} />}
    {!blocked && <><section aria-label="Applied Content filters"><p>Applied filters: {Object.entries(query).filter(([key]) => key !== 'limit' && key !== 'offset').map(([key, value]) => `${key}: ${value}`).join(' · ') || 'All catalog articles'}</p></section>
      <div className="pagination-bar" aria-label="Content catalog pagination"><p>{response ? `${response.meta.returned} returned rows` : 'Requested page'} · Offset {offset} · {limit} rows per page · No total supplied</p>
        <button type="button" disabled={loading || offset === 0} onClick={() => model.movePage(Math.max(0, offset - limit))}>Previous catalog page</button>
        <button type="button" disabled={loading || !response?.meta.hasMore || offset + limit > CONTENT_MAX_OFFSET} onClick={() => model.movePage(offset + limit)}>Next catalog page</button>
        <button type="button" onClick={model.retry}>Refresh catalog page</button>
      </div></>}
    <div aria-busy={loading}>
      {loading && <section className="state-panel" role="status"><h2>Loading Content catalog</h2><p>Waiting for verified Content and dependency identities.</p></section>}
      {current?.status === 'error' && <ErrorState error={current.error} onRetry={model.retry} label="Content catalog" />}
      {response && (response.data.length ? <><p className="scope-notice">Returned page only, ordered by last observed time descending, then article ID ascending.{response.meta.hasMore ? ' More articles are available.' : ''}</p>
        <ContentCatalogRows response={response} /></> : <section className="state-panel" role="status"><h2>No articles in this catalog scope</h2><p>Adjust the supported filters to explore another scope.</p></section>)}
    </div>
  </section>;
}
