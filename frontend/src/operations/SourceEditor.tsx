import { useEffect, useState } from 'react';
import { fetchSourceCapabilities, isSourceDisplayName, ManagementError } from '../managementApi';
import type { ProductId, SourceCapabilities, SourceConfig, SourceResponse } from '../managementApi';
import { ManagementErrorDetails } from './ManagementErrorDetails';
import { productLabel } from './SourceList';
import { reviewSourceCommand, useSourceIntent } from './sourceIntent';

export function SourceEditor({ baseline, onCancel }: { baseline?: SourceResponse; onCancel?: () => void }) {
  const [capabilities, setCapabilities] = useState<SourceCapabilities | null>(null);
  const [error, setError] = useState<ManagementError | null>(null);
  const [refresh, setRefresh] = useState(0);
  const [product, setProduct] = useState<ProductId>(baseline?.data.productId ?? 'content-intelligence');
  const [sourceId, setSourceId] = useState(baseline?.data.sourceId ?? '');
  const [displayName, setDisplayName] = useState(baseline?.data.displayName ?? '');
  const [enabled, setEnabled] = useState(false);
  const [endpoint, setEndpoint] = useState(baseline?.data.config.feedEndpointId ?? '');
  const [profileRef, setProfileRef] = useState(baseline?.data.config.registeredProfileSha256 ? JSON.stringify([
    baseline.data.config.profile, baseline.data.config.registeredProfileSha256,
  ]) : '');
  const [storage, setStorage] = useState(baseline?.data.config.inputStorageProfileId ?? '');
  const [validation, setValidation] = useState('');
  const { intent, unavailable } = useSourceIntent();
  useEffect(() => {
    const controller = new AbortController(); let current = true;
    void fetchSourceCapabilities(controller.signal).then((response) => {
      if (current && !controller.signal.aborted) { setCapabilities(response.data); setError(null); }
    }).catch((caught: unknown) => {
      if (current && !controller.signal.aborted) setError(caught instanceof ManagementError ? caught : new ManagementError('network'));
    });
    return () => { current = false; controller.abort(); };
  }, [refresh]);
  const endpoints = capabilities?.endpoints.filter((entry) => entry.sourceId === sourceId) ?? [];
  const profiles = capabilities?.reliabilityProfiles.filter((entry) => entry.sourceId === sourceId
    && (!baseline || entry.profile === baseline.data.config.profile)) ?? [];
  const storages = capabilities?.storageProfiles.filter((entry) => entry.productId === product) ?? [];
  const sources = [...new Set(product === 'content-intelligence' ? capabilities?.endpoints.map((entry) => entry.sourceId)
    : capabilities?.reliabilityProfiles.map((entry) => entry.sourceId))];
  const selectedProfile = profiles.find((entry) => JSON.stringify([entry.profile, entry.registeredProfileSha256]) === profileRef);
  const valid = isSourceDisplayName(displayName) && sources.includes(sourceId) && storages.some((entry) => entry.profileId === storage)
    && (product === 'content-intelligence' ? endpoints.some((entry) => entry.endpointId === endpoint) : !!selectedProfile);
  function review() {
    if (!valid || intent || unavailable) { setValidation('Choose reviewed references and a valid display name before review.'); return; }
    const config: SourceConfig = product === 'content-intelligence'
      ? { configVersion: 1, profile: 'rss-atom-v1', dataOrigin: 'publisher', feedEndpointId: endpoint, registeredProfileSha256: null, inputStorageProfileId: storage }
      : { configVersion: 1, profile: selectedProfile!.profile, dataOrigin: 'synthetic', feedEndpointId: null,
        registeredProfileSha256: selectedProfile!.registeredProfileSha256, inputStorageProfileId: storage };
    try {
      reviewSourceCommand({ action: baseline ? 'revise' : 'create', identity: { productId: product, sourceId }, ifMatch: baseline?.etag ?? null,
        body: JSON.stringify(baseline ? { displayName, config } : { productId: product, sourceId, displayName, enabled, config }) });
      setValidation('');
    } catch { setValidation('The request could not be retained safely in this tab. No mutation was submitted.'); }
  }
  return <section className="panel source-editor" aria-labelledby="source-editor-title">
    <h2 id="source-editor-title">{baseline ? 'New immutable revision' : 'Register a source'}</h2>
    <p>Use reviewed references only. The server validates the current catalog and installed storage binding. This operation does not start ingestion or enable Content dispatch.</p>
    {baseline && <p>Based on revision {baseline.data.currentRevision}, row version <code>{baseline.data.rowVersion}</code>. The profile remains <code>{baseline.data.config.profile}</code>. A changed name or configuration creates an immutable revision; an identical request may leave the revision unchanged.</p>}
    {!capabilities && !error && <p role="status">Loading reviewed source capabilities</p>}
    {error && <div role="alert"><p>Reviewed source capabilities are unavailable. Source configuration cannot be submitted.</p><ManagementErrorDetails error={error} />
      <button type="button" className="secondary-button" onClick={() => { setError(null); setRefresh((value) => value + 1); }}>Retry capabilities</button></div>}
    {capabilities && sources.length === 0 && <p role="status">No reviewed source references are available for this product. Ask the local operator to configure an existing reviewed catalog.</p>}
    <form onSubmit={(event) => { event.preventDefault(); review(); }}>
      <fieldset disabled={!capabilities || !!error || !!intent || unavailable} className="source-form-fields">
        <legend>{baseline ? 'Revision configuration' : 'Registration configuration'}</legend>
        <label>Product<select value={product} disabled={!!baseline} onChange={(event) => {
          setProduct(event.target.value as ProductId); setSourceId(''); setEndpoint(''); setProfileRef(''); setStorage('');
        }}><option value="content-intelligence">Content Intelligence</option><option value="application-reliability">Application Reliability</option></select></label>
        <label>Source ID<select value={sourceId} disabled={!!baseline} required onChange={(event) => { setSourceId(event.target.value); setEndpoint(''); setProfileRef(''); }}>
          <option value="">Select a reviewed source</option>{baseline && !sources.includes(sourceId) && <option value={sourceId}>{sourceId} (no longer in catalog)</option>}
          {sources.map((id) => <option key={id} value={id}>{id}</option>)}
        </select></label>
        <label>Display name<input value={displayName} required aria-invalid={displayName !== '' && !isSourceDisplayName(displayName)} aria-describedby="source-name-help" onChange={(event) => setDisplayName(event.target.value)} /></label>
        <p id="source-name-help">Nonblank text, at most 128 UTF-8 bytes, without control characters. Whitespace is preserved.</p>
        {displayName !== '' && !isSourceDisplayName(displayName) && <p role="alert">Enter a nonblank display name within the UTF-8 byte limit, without control characters.</p>}
        {product === 'content-intelligence' ? <><p>Profile: <code>rss-atom-v1</code> · Publisher data</p><label>Feed endpoint reference<select required value={endpoint} onChange={(event) => setEndpoint(event.target.value)}>
          <option value="">Select a reviewed endpoint</option>{endpoints.map((entry) => <option key={entry.endpointId} value={entry.endpointId}>{entry.endpointId}</option>)}
        </select></label></> : <><p>Synthetic Reliability data; revisions retain the existing profile.</p><label>Registered Reliability profile<select required value={profileRef} onChange={(event) => setProfileRef(event.target.value)}>
          <option value="">Select a reviewed profile</option>{profiles.map((entry) => { const ref = JSON.stringify([entry.profile, entry.registeredProfileSha256]);
            return <option key={ref} value={ref}>{entry.profile} · {entry.registeredProfileSha256}</option>; })}
        </select></label></>}
        <label>Input storage profile reference<select required value={storage} onChange={(event) => setStorage(event.target.value)}>
          <option value="">Select a reviewed storage profile</option>{storages.map((entry) => <option key={entry.profileId} value={entry.profileId}>{entry.profileId} · {entry.role}</option>)}
        </select></label>
        {!baseline && <label className="source-checkbox"><input type="checkbox" checked={enabled} onChange={(event) => setEnabled(event.target.checked)} /> Enable admission after registration</label>}
        <button type="submit" className="primary-button" disabled={!valid}>Review {baseline ? 'revision' : 'registration'}</button>
      </fieldset>
      {validation && <p role="alert">{validation}</p>}
    </form>
    {onCancel && <button type="button" className="secondary-button" disabled={!!intent} onClick={onCancel}>Close revision editor</button>}
    {baseline && <p className="source-table-note">Identity: {productLabel(product)} / <code>{sourceId}</code>. Historical configurations and run selections remain immutable.</p>}
  </section>;
}

export function SourceRegistration() {
  return <section aria-label="Source registration"><a className="source-back" href="#/operations/sources">Back to Sources</a>
    <div className="page-heading"><div><p className="eyebrow">OPERATIONS WORKSPACE / SOURCE REGISTRATION</p><h1>Register source<span className="title-period">.</span></h1></div></div>
    <SourceEditor />
  </section>;
}
