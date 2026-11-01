import type { ProductId } from '../managementApi';
import { Timestamp } from '../components/Timestamp';
import { ManagementErrorDetails } from './ManagementErrorDetails';
import { useSources } from './useSources';
import { sourceHref } from '../navigation';

export const productLabel = (product: ProductId) => product === 'content-intelligence' ? 'Content Intelligence' : 'Application Reliability';

export function SourceList({ active }: { active: boolean }) {
  const model = useSources(active);
  const { current, filters } = model;
  const page = current?.status === 'ready' ? current.page : null;
  const error = current?.status === 'error' ? current.error : null;
  return <section aria-label="Operations sources">
    <div className="page-heading"><div>
      <p className="eyebrow">OPERATIONS WORKSPACE <span aria-hidden="true">/</span> SOURCE CATALOG</p>
      <h1>Sources<span className="title-period">.</span></h1>
      <p className="page-description">Registered sources, current revisions and admission status.</p>
    </div><a className="secondary-button" href="#/operations/sources/new">Register source</a></div>
    <p className="scope-notice">Enabled sources are eligible for admission. This catalog does not start ingestion or activate Content dispatch.</p>
    <div className="source-filters" role="group" aria-label="Source filters">
      <label>Product<select value={filters.productId} onChange={(event) => model.changeFilters({ productId: event.target.value as ProductId | '' })}>
        <option value="">All products</option><option value="application-reliability">Application Reliability</option><option value="content-intelligence">Content Intelligence</option>
      </select></label>
      <label>Admission status<select value={filters.enabled} onChange={(event) => model.changeFilters({ enabled: event.target.value as '' | 'true' | 'false' })}>
        <option value="">All statuses</option><option value="true">Enabled</option><option value="false">Disabled</option>
      </select></label>
      <label>Sources per page<select value={filters.limit} onChange={(event) => model.changeFilters({ limit: Number(event.target.value) })}>
        <option value="20">20</option><option value="50">50</option><option value="100">100</option>
      </select></label>
      <button className="secondary-button" type="button" onClick={() => model.refresh(true)}>Refresh sources</button>
    </div>
    <div aria-busy={current === null}>
      {current === null && <section className="state-panel" role="status" aria-live="polite">
        <span className="loading-indicator" aria-hidden="true" /><h2>Loading sources</h2><p>Reading the registered source catalog.</p>
      </section>}
      {error && <section className="state-panel error-state" role="alert">
        <h2>Unable to load sources</h2><p>{error.message}</p>
        <ManagementErrorDetails error={error} />
        <button className="primary-button" type="button" onClick={() => model.refresh()}>Try again</button>
        {model.hasCursor && error.kind === 'invalid-request' && <button className="secondary-button" type="button" onClick={() => model.refresh(true)}>Start from first page</button>}
      </section>}
      {page && page.data.length === 0 && <section className="state-panel" role="status">
        <h2>{filters.productId || filters.enabled ? 'No sources match these filters' : 'No registered sources'}</h2>
        <p>{filters.productId || filters.enabled ? 'Choose another product or admission status, or refresh the list.' : 'The management API returned an empty source catalog.'}</p>
      </section>}
      {page && page.data.length > 0 && <section className="panel table-panel" aria-labelledby="sources-title">
        <div className="panel-heading"><h2 id="sources-title">Registered sources</h2><span className="row-badge">{page.data.length} returned on this page</span></div>
        {/* eslint-disable-next-line jsx-a11y/no-noninteractive-tabindex -- Keyboard users must be able to scroll the table. */}
        <div className="table-scroll" role="region" aria-label="Registered sources table; scroll horizontally on small screens" tabIndex={0}>
          <table><caption className="visually-hidden">Registered sources in API order. Revision and admission status are current; timestamps are UTC.</caption>
            <thead><tr><th scope="col">Source</th><th scope="col">Product</th><th scope="col">Admission</th><th scope="col">Revision</th>
              <th scope="col">Row version</th><th scope="col">Updated (UTC)</th><th scope="col">Created (UTC)</th><th scope="col">Configuration SHA-256</th></tr></thead>
            <tbody>{page.data.map((source) => <tr key={`${source.productId}:${source.sourceId}`}>
              <th scope="row"><a href={sourceHref(source)}><span className="source-name">{source.displayName}</span>{' '}<code className="source-id">{source.sourceId}</code></a></th>
              <td>{productLabel(source.productId)}</td><td><span className={`admission-status ${source.enabled ? 'enabled' : 'disabled'}`}>{source.enabled ? 'Enabled' : 'Disabled'}</span></td>
              <td>{source.currentRevision}</td><td><code>{source.rowVersion}</code></td>
              <td><Timestamp value={source.updatedAt} /></td><td><Timestamp value={source.createdAt} /></td><td><code className="config-digest">{source.configSha256}</code></td>
            </tr>)}</tbody>
          </table>
        </div>
        <p className="source-table-note">Enabled is an admission gate, not execution status or revision approval.</p>
      </section>}
    </div>
    <nav className="source-pagination" aria-label="Source pages">
      <button className="secondary-button" type="button" disabled={current === null || model.pageNumber === 1} onClick={model.back}>Previous sources</button>
      <p role="status" aria-live="polite">Page {model.pageNumber}{page ? ` · ${page.data.length} returned${page.meta.hasMore ? ' · More sources available' : ' · End of results'}` : ''}</p>
      <button className="secondary-button" type="button" disabled={page === null || !page.meta.hasMore} onClick={model.next}>Next sources</button>
    </nav>
  </section>;
}
