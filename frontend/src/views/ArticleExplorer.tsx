import { MAX_OFFSET } from '../api';
import type { ArticlesResponse } from '../api';
import { safeArticleUrl } from '../articleLinks';
import { formatCount } from '../metrics';
import { ReportEmptyState } from '../components/States';
import { Timestamp } from '../components/Timestamp';

interface Props { response: ArticlesResponse | null; offset: number; limit: number; loading: boolean; onPage: (offset: number) => void }

export default function ArticleExplorer({ response, offset, limit, loading, onPage }: Props) {
  const nextOffset = response ? response.meta.offset + response.meta.returned : offset;
  const canNext = !loading && response !== null && response.meta.hasMore && nextOffset <= MAX_OFFSET;
  return <section className="article-explorer" aria-label="Paginated article catalog">
    <div className="pagination-bar"><div><strong>Catalog page</strong><p>
      {response ? (response.meta.returned > 0 ? `Rows ${formatCount(offset + 1)}–${formatCount(offset + response.meta.returned)} in this source scope`
        : `No rows at offset ${formatCount(offset)}`) : `Requested offset ${formatCount(offset)}`}
      <span>{limit} rows per request · No global article total supplied</span></p></div>
      <div className="pagination-controls"><button type="button" className="secondary-button" disabled={loading || offset === 0}
        onClick={() => onPage(Math.max(0, offset - limit))}>Previous</button>
      <button type="button" className="secondary-button" disabled={!canNext} onClick={() => onPage(nextOffset)}>Next</button></div>
    </div>
    {response && <>
      <p className="visually-hidden" role="status">{response.meta.returned} articles returned at offset {response.meta.offset}.</p>
      {response.meta.hasMore && <p className="scope-notice" role="note">More articles are available in this source scope.
        {nextOffset > MAX_OFFSET ? ' The API maximum offset has been reached; further traversal is unavailable.' : ' Use Next to request the next page.'}</p>}
      <p className="metric-explanation">Ordered by latest observation descending, then article ID ascending. Date filters apply to daily and quality views only.
        Each identity is a source-scoped article, not a unique story across publishers.</p>
      {response.data.length === 0 ? <ReportEmptyState label="articles" /> : <ol className="article-list" start={offset + 1} aria-label="Articles in API order">
        {response.data.map((row) => {
          const href = safeArticleUrl(row.url);
          return <li key={row.articleId} className="panel article-card" data-article-id={row.articleId}>
            <div className="article-heading"><div><p className="eyebrow article-source">{row.sourceId}</p>
              <h2>{href ? <a href={href} target="_blank" rel="noopener noreferrer">{row.title}{' '}<span className="visually-hidden">(opens in a new tab)</span></a> : row.title}</h2>
              <p className="article-link-note">{href ? 'External article · opens in a new tab' : row.url === null ? 'No article URL supplied' : 'External navigation unavailable for this URL'}</p>
            </div><div className="article-counts"><span><strong>{formatCount(row.observationCount)}</strong> observations</span>
              <span><strong>{formatCount(row.distinctIngestionCount)}</strong> distinct ingestions</span></div></div>
            <div className="article-timing"><span>Last observed (UTC)<Timestamp value={row.lastObservedAt} /></span>
              <span>Published (UTC)<Timestamp value={row.publishedAt} /></span></div>
            <details className="article-details"><summary>Article details{' '}<span className="visually-hidden">for {row.title}</span></summary>
              <dl className="article-detail-grid">
                <div className="detail-wide"><dt>Article identity</dt><dd><code>{row.articleId}</code></dd></div>
                <div className="detail-wide"><dt>Publisher-provided summary</dt><dd className="article-summary">{row.summary ?? 'Not available'}</dd></div>
                <div className="detail-wide"><dt>Publisher-provided URL</dt><dd className="article-raw-url">{row.url ?? 'Not available'}</dd></div>
                <div><dt>Updated (UTC)</dt><dd><Timestamp value={row.updatedAt} /></dd></div>
                <div><dt>First observed (UTC)</dt><dd><Timestamp value={row.firstObservedAt} /></dd></div>
                <div className="detail-wide"><dt>Selected observation warning codes</dt><dd>{row.qualityWarnings.length === 0 ? 'No warning codes' :
                  <ul className="warning-codes">{row.qualityWarnings.map((code) => <li key={code}><code>{code}</code></li>)}</ul>}</dd></div>
              </dl>
              <p className="article-detail-note">Titles and summaries are untrusted publisher text. Warning codes describe this selected observation, not the article’s historical warning total. Missing timestamps are not warnings.</p>
            </details>
          </li>;
        })}
      </ol>}
    </>}
  </section>;
}
