import { Timestamp } from '../components/Timestamp';
import { ManagementErrorDetails } from './ManagementErrorDetails';
import type { ManagementError, SourceConfig, SourceIdentity } from '../managementApi';
import { productLabel } from './SourceList';
import { useSource } from './useSource';
import { executionsHref } from '../navigation';

function ReadError({ error, title, retry, restart }: {
  error: ManagementError; title: string; retry: () => void; restart?: (() => void) | undefined;
}) {
  return <div className="state-panel error-state" role="alert">
    <h3>{error.kind === 'not-found' ? 'Source not found' : title}</h3>
    <p>{error.kind === 'not-found' ? 'No source was found for this product and source ID. Check the identity and API-mode service.'
      : error.kind === 'invalid-response' ? 'The API returned data that does not match the source contract. No data from this response is displayed.' : error.message}</p>
    <ManagementErrorDetails error={error} />
    <button className="primary-button" type="button" onClick={retry}>Try again</button>
    {restart && error.kind === 'invalid-request' && <button className="secondary-button" type="button" onClick={restart}>Start from first revision page</button>}
  </div>;
}

function Configuration({ config }: { config: SourceConfig }) {
  return <dl className="source-metadata">
    <div><dt>Profile</dt><dd><code>{config.profile}</code></dd></div>
    <div><dt>Data origin</dt><dd>{config.dataOrigin}</dd></div>
    <div><dt>Feed endpoint reference</dt><dd>{config.feedEndpointId === null ? 'Not applicable' : <code>{config.feedEndpointId}</code>}</dd></div>
    <div><dt>Registered profile SHA-256</dt><dd>{config.registeredProfileSha256 === null ? 'Not applicable' : <code>{config.registeredProfileSha256}</code>}</dd></div>
    <div><dt>Input storage profile reference</dt><dd><code>{config.inputStorageProfileId}</code></dd></div>
    <div><dt>Configuration version</dt><dd>{config.configVersion}</dd></div>
  </dl>;
}

function Loading({ title }: { title: string }) {
  return <div className="state-panel" role="status"><span className="loading-indicator" aria-hidden="true" /><h3>{title}</h3></div>;
}

export function SourceDetail({ identity }: { identity: SourceIdentity }) {
  const model = useSource(identity);
  const source = model.detail?.status === 'ready' ? model.detail.value.data : null;
  const history = model.revisions?.status === 'ready' ? model.revisions.value : null;
  return <section aria-label="Source detail">
    <a className="source-back" href="#/operations/sources">Back to Sources</a>
    <div className="page-heading"><div>
      <p className="eyebrow">OPERATIONS WORKSPACE <span aria-hidden="true">/</span> SOURCE DETAIL</p>
      <h1>Source detail<span className="title-period">.</span></h1>
      <p className="page-description">{productLabel(identity.productId)} <span aria-hidden="true">/</span> <code>{identity.sourceId}</code></p>
    </div><button className="secondary-button" type="button" onClick={model.refresh}>Refresh source and revisions</button></div>
    <p className="related-executions"><a href={executionsHref(identity)}>View executions for this source</a></p>
    <p className="scope-notice">Enabled is an admission gate, not health or revision approval. A registered source does not imply ingestion or processing. Content dispatch is not activated here.</p>

    <section className="panel source-detail-panel" aria-labelledby="current-source-title" aria-busy={model.detail === null}>
      <h2 id="current-source-title">Current source record</h2>
      {model.detail === null && <Loading title="Loading source detail" />}
      {model.detail?.status === 'error' && <ReadError title="Unable to load source detail" error={model.detail.error} retry={model.retryDetail} />}
      {source && <>
        <h3 className="source-display-name">{source.displayName}</h3>
        <dl className="source-metadata">
          <div><dt>Product</dt><dd>{productLabel(source.productId)}</dd></div>
          <div><dt>Source ID</dt><dd><code>{source.sourceId}</code></dd></div>
          <div><dt>Admission</dt><dd><span className={`admission-status ${source.enabled ? 'enabled' : 'disabled'}`}>{source.enabled ? 'Enabled' : 'Disabled'}</span></dd></div>
          <div><dt>Current revision</dt><dd>{source.currentRevision}</dd></div>
          <div><dt>Configuration SHA-256</dt><dd><code>{source.configSha256}</code></dd></div>
          <div><dt>Row version</dt><dd><code>{source.rowVersion}</code></dd></div>
          <div><dt>Created (UTC)</dt><dd><Timestamp value={source.createdAt} /></dd></div>
          <div><dt>Updated (UTC)</dt><dd><Timestamp value={source.updatedAt} /></dd></div>
        </dl>
        <h3>Current configuration references</h3><Configuration config={source.config} />
      </>}
    </section>

    <section className="panel source-detail-panel" aria-labelledby="revisions-title" aria-busy={model.revisions === null}>
      <div className="panel-heading"><h2 id="revisions-title">Revision history</h2>
        <button className="secondary-button" type="button" onClick={() => model.refreshRevisions(true)}>Refresh revisions</button></div>
      <p className="source-table-note">Immutable configurations in ascending revision order. Current labels refer to the displayed source record and can change after refresh. Historical run selections must retain their captured revision. New revisions require a new traversal.</p>
      {model.revisions === null && <Loading title="Loading revision history" />}
      {model.revisions?.status === 'error' && <ReadError title="Unable to load revision history" error={model.revisions.error}
        retry={() => model.refreshRevisions()} restart={model.hasCursor ? () => model.refreshRevisions(true) : undefined} />}
      {history && history.data.length === 0 && <div className="state-panel" role="status"><h3>No revisions returned</h3><p>The API returned an empty revision page. Refresh to start a new traversal.</p></div>}
      {history && history.data.length > 0 && <ol className="revision-list" aria-label="Immutable source revisions">
        {history.data.map((revision) => <li key={revision.revision} className="revision-card">
          <h3>Revision {revision.revision} <span className="row-badge">{!source ? 'Current revision not verified'
            : revision.revision === source.currentRevision ? 'Current in displayed source record'
              : revision.revision < source.currentRevision ? 'Historical revision' : 'Newer than displayed source record'}</span></h3>
          <p className="source-display-name">{revision.displayName}</p>
          <dl className="source-metadata">
            <div><dt>Configuration SHA-256</dt><dd><code>{revision.configSha256}</code></dd></div>
            <div><dt>Created (UTC)</dt><dd><Timestamp value={revision.createdAt} /></dd></div>
          </dl>
          <details><summary>Configuration references for revision {revision.revision}</summary><Configuration config={revision.config} /></details>
        </li>)}
      </ol>}
      <nav className="source-pagination" aria-label="Revision pages">
        <button className="secondary-button" type="button" disabled={model.revisions === null || model.pageNumber === 1} onClick={model.back}>Previous revisions</button>
        <p role="status" aria-live="polite">Revision page {model.pageNumber}{history ? ` · ${history.data.length} returned${history.meta.hasMore ? ' · More revisions available' : ' · End of results'}` : ''}</p>
        <button className="secondary-button" type="button" disabled={history === null || !history.meta.hasMore} onClick={model.next}>Next revisions</button>
      </nav>
    </section>
  </section>;
}
