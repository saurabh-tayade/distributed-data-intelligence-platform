import { useCallback, useEffect, useRef, useState } from 'react';
import type { ReactNode } from 'react';
import { fetchReliabilityArtifacts, fetchReliabilityProfiles, fetchSource, fetchSources, ManagementError, parseReliabilityRequest } from '../managementApi';
import type { ManagementPage, ReliabilityArtifact, ReliabilityBatch, ReliabilityProfile, RevisionQuery, ServiceReference, SourceRecord, SourceSummary } from '../managementApi';
import { Timestamp } from '../components/Timestamp';
import { InspectionError } from './InspectionError';
import { ManagementErrorDetails } from './ManagementErrorDetails';
import { reviewRunSubmission, useRunIntent } from './runIntent';
import { useSubmissionPage } from './useSubmissionPage';

export function batchMatches(profile: ReliabilityProfile, sources: SourceRecord[], batch: ReliabilityBatch): boolean {
  const source = sources.find((item) => item.sourceId === batch.identity.sourceId);
  return !!source && source.productId === 'application-reliability' && source.enabled && source.config.profile === profile.sourceProfile
    && batch.nativeFormat === profile.inputFormat && batch.identity.sourceRevision === source.currentRevision
    && batch.identity.registeredProfileSha256 === source.config.registeredProfileSha256 && batch.storageBindingId === source.config.inputStorageProfileId;
}
export function selectionErrors(profile: ReliabilityProfile | null, sources: SourceRecord[], batches: ReliabilityBatch[], reference: ServiceReference | null): string[] {
  const errors: string[] = [];
  if (!profile || profile.productId !== 'application-reliability' || profile.pipeline !== 'RELIABILITY_V2') errors.push('Select a registered Reliability execution profile.');
  if (sources.length < 1 || sources.length > 32 || new Set(sources.map((s) => s.sourceId)).size !== sources.length) errors.push('Select 1–32 distinct enabled Reliability sources.');
  if (profile && sources.some((s) => s.productId !== 'application-reliability' || !s.enabled || s.config.profile !== profile.sourceProfile)) errors.push('Selected sources must be enabled and match the execution profile.');
  if (batches.length < 1 || batches.length > 32 || new Set(batches.map((b) => b.artifactId)).size !== batches.length) errors.push('Select 1–32 distinct compatible batch artifacts.');
  if (profile && batches.some((b) => !batchMatches(profile, sources, b))) errors.push('Batch artifacts must match the selected current source revisions, format and registered references.');
  if (sources.some((s) => !batches.some((b) => b.identity.sourceId === s.sourceId))) errors.push('Select at least one batch for every selected source.');
  if (!reference || reference.productId !== 'application-reliability' || reference.role !== 'SERVICE_REFERENCE' || batches.some((b) => b.artifactId === reference.artifactId)) errors.push('Select one compatible service-reference artifact.');
  return errors;
}

function Catalog<T>({ title, scope, active, load, empty, children }: {
  title: string; scope: string; active: boolean; load: (query: RevisionQuery, signal: AbortSignal) => Promise<ManagementPage<T>>;
  empty: string; children: (rows: T[]) => ReactNode;
}) {
  const model = useSubmissionPage(scope, active, load), current = model.current;
  return <section className="panel submission-catalog" aria-label={title}>
    <h2>{title}</h2>
    <button type="button" className="secondary-button" disabled={!active} onClick={model.refresh}>Refresh {title.toLowerCase()}</button>
    {active && current === null && <p role="status">Loading {title.toLowerCase()}…</p>}
    {active && current?.status === 'error' && <InspectionError title={`Unable to load ${title.toLowerCase()}`} error={current.error} retry={model.retry} restart={model.refresh} />}
    {active && current?.status === 'ready' && (current.page.data.length ? children(current.page.data) : <p role="status">{empty}</p>)}
    <nav className="source-pagination" aria-label={`${title} pages`}>
      <button type="button" className="secondary-button" disabled={!active || !current || !model.canGoBack} onClick={model.back}>Previous {title.toLowerCase()}</button>
      {current?.status === 'ready' && <p role="status">{current.page.data.length} returned · {current.page.meta.hasMore ? 'More available' : 'End of results'}</p>}
      <button type="button" className="secondary-button" disabled={!active || current?.status !== 'ready' || !current.page.meta.hasMore} onClick={model.next}>Next {title.toLowerCase()}</button>
    </nav>
    {model.truncated && <p>Only the latest 20 cursor positions are retained. Refresh to restart from the first page.</p>}
  </section>;
}
function ArtifactMetadata({ artifact }: { artifact: ReliabilityArtifact }) {
  return <details><summary>Historical registration metadata</summary><dl className="source-metadata">
    <div><dt>Contract / processor</dt><dd>{artifact.contractVersion} / {artifact.processorRevision}</dd></div>
    <div><dt>Storage reference</dt><dd><code>{artifact.storageBindingId}</code></dd></div>
    <div><dt>Native fingerprint</dt><dd><code>{artifact.nativeFingerprint}</code></dd></div>
    <div><dt>Verified at registration (UTC)</dt><dd><Timestamp value={artifact.verifiedAt} /></dd></div>
  </dl></details>;
}

function SubmissionSelection({ active }: { active: boolean }) {
  const [profile, setProfile] = useState<ReliabilityProfile | null>(null);
  const [sources, setSources] = useState<SourceRecord[]>([]), [batches, setBatches] = useState<ReliabilityBatch[]>([]);
  const [reference, setReference] = useState<ServiceReference | null>(null), [sourceFilter, setSourceFilter] = useState('');
  const [checking, setChecking] = useState(false), [validation, setValidation] = useState<string[]>([]);
  const [error, setError] = useState<ManagementError | null>(null), [epoch, setEpoch] = useState(0);
  const pending = useRef<AbortController | null>(null);
  const chosenProfile = profile?.executionProfileId;
  useEffect(() => () => { pending.current?.abort(); pending.current = null; }, [active, chosenProfile]);
  const loadSources = useCallback((query: RevisionQuery, signal: AbortSignal) => fetchSources({ ...query, productId: 'application-reliability', enabled: true }, signal), []);
  const loadBatches = useCallback((query: RevisionQuery, signal: AbortSignal) => fetchReliabilityArtifacts({ ...query,
    executionProfileId: chosenProfile!, role: 'RELIABILITY_BATCH', ...(sourceFilter ? { sourceId: sourceFilter } : {}) }, signal), [chosenProfile, sourceFilter]);
  const loadReferences = useCallback((query: RevisionQuery, signal: AbortSignal) => fetchReliabilityArtifacts({ ...query, executionProfileId: chosenProfile!, role: 'SERVICE_REFERENCE' }, signal), [chosenProfile]);
  const sourceScope = sources.map((s) => `${s.sourceId}:${s.currentRevision}`).sort().join(',');
  function changeProfile(next: ReliabilityProfile) {
    if (next.executionProfileId === chosenProfile) return;
    setProfile(next); setSources([]); setBatches([]); setReference(null); setSourceFilter(''); setValidation([]); setError(null);
  }
  function removeSource(id: string) {
    setSources((old) => old.filter((s) => s.sourceId !== id)); setBatches((old) => old.filter((b) => b.identity.sourceId !== id));
    if (sourceFilter === id) setSourceFilter('');
  }
  async function addSource(discovered: SourceSummary) {
    const id = discovered.sourceId;
    if (!profile || pending.current || !active) return;
    if (sources.some((s) => s.sourceId === id)) { removeSource(id); return; }
    if (sources.length >= 32) { setValidation(['At most 32 sources may be selected.']); return; }
    const abort = new AbortController(); pending.current = abort; setChecking(true); setError(null); setValidation([]);
    try {
      const response = await fetchSource({ productId: 'application-reliability', sourceId: id }, abort.signal);
      if (abort.signal.aborted) return;
      const source = response.data;
      if (!source.enabled || source.config.profile !== profile.sourceProfile) { setValidation(['This source must be enabled and match the selected execution profile.']); return; }
      if (source.currentRevision !== discovered.currentRevision || source.rowVersion !== discovered.rowVersion || source.configSha256 !== discovered.configSha256) {
        setValidation(['This source changed since discovery. Refresh enabled Reliability sources and deliberately select its current revision.']); return;
      }
      setSources((old) => old.some((s) => s.sourceId === id) ? old : [...old, source]);
    } catch (caught) { if (!abort.signal.aborted) setError(caught instanceof ManagementError ? caught : new ManagementError('network')); }
    finally { if (pending.current === abort) { pending.current = null; setChecking(false); } }
  }
  function refreshSelections() {
    setSources([]); setBatches([]); setReference(null); setSourceFilter(''); setEpoch((old) => old + 1); setValidation([]); setError(null);
  }
  async function review() {
    if (pending.current || !active) return;
    const errors = selectionErrors(profile, sources, batches, reference); setValidation(errors); setError(null);
    if (errors.length) return;
    const abort = new AbortController(); pending.current = abort; setChecking(true);
    try {
      const current = await Promise.all(sources.map((s) => fetchSource({ productId: 'application-reliability', sourceId: s.sourceId }, abort.signal)));
      if (abort.signal.aborted) return;
      if (current.some(({ data }, i) => !data.enabled || data.currentRevision !== sources[i]!.currentRevision || data.rowVersion !== sources[i]!.rowVersion
        || data.configSha256 !== sources[i]!.configSha256 || data.config.profile !== profile!.sourceProfile)) {
        setValidation(['A selected source changed before review. Refresh selections and deliberately select its current revision and compatible batches.']); return;
      }
      const request = parseReliabilityRequest({ pipeline: 'RELIABILITY_V2', executionProfileId: profile!.executionProfileId,
        sources: sources.map((s) => ({ sourceId: s.sourceId, revision: s.currentRevision })).sort((a, b) => a.sourceId < b.sourceId ? -1 : 1),
        batchArtifactIds: batches.map((b) => b.artifactId).sort(), serviceReferenceArtifactId: reference!.artifactId });
      reviewRunSubmission(JSON.stringify(request));
    } catch (caught) {
      if (!abort.signal.aborted) {
        if (caught instanceof ManagementError) setError(caught);
        else setValidation(['The exact request could not be retained safely in this tab. No mutation was submitted.']);
      }
    } finally { if (pending.current === abort) { pending.current = null; setChecking(false); } }
  }
  if (!active) return null;
  return <div className="reliability-submission">
    <p className="scope-notice">Discovery is advisory registration metadata, not worker health or native-file readiness. Source admission and current revisions can change. Final server admission is authoritative.</p>
    <fieldset className="source-form-fields" disabled={checking}>
      <legend className="visually-hidden">Reliability run selections</legend>
      <p className="inspection-note">1. Choose an approved execution profile. Then select enabled sources, matching imported batches and one service reference.
        Review creates a frozen request in this tab; only confirmation sends it to the server.</p>
      <Catalog title="Execution profiles" scope="profiles" active load={fetchReliabilityProfiles} empty="No compatible registered Reliability execution profiles were returned. Install a supported profile through the existing operator procedure.">
        {(rows) => <div className="submission-choices">{rows.map((item) => <div key={item.executionProfileId}><label>
          <input type="radio" name="execution-profile" checked={chosenProfile === item.executionProfileId} onChange={() => changeProfile(item)} />
          <span><strong>{item.inputFormat} request batches</strong> · {item.sourceProfile}<code className="inspection-id">{item.executionProfileId}</code></span></label>
          <details><summary>Profile contract metadata</summary><p>RELIABILITY_V2 · Run profile {item.runProfileVersion} · Pipeline {item.pipelineVersion}</p>
            <p>Configuration SHA-256: <code>{item.configurationSha256}</code></p><p>Registered: <Timestamp value={item.createdAt} /></p></details></div>)}</div>}
      </Catalog>
      {profile && <>
        <p className="inspection-note">2. Select enabled sources. Each source needs at least one batch matching its captured current revision, profile and input storage reference.</p>
        <p>Selected profile: <code>{chosenProfile}</code> · {profile.sourceProfile} · SHA-256 <code>{profile.configurationSha256}</code>.</p>
        <Catalog title="Enabled Reliability sources" scope={`${chosenProfile}:${epoch}`} active load={loadSources} empty="No enabled Reliability sources were returned. Source configuration and admission are managed separately.">
          {(rows) => <div className="submission-choices">{rows.map((s) => <label key={s.sourceId}><input type="checkbox" checked={sources.some((chosen) => chosen.sourceId === s.sourceId)}
            disabled={!s.enabled || sources.length >= 32 && !sources.some((chosen) => chosen.sourceId === s.sourceId)} onChange={() => void addSource(s)} />
            <span>{s.displayName} · <code>{s.sourceId}</code> · {s.enabled ? 'Enabled' : 'Disabled'} · Current revision {s.currentRevision}
              {sources.some((chosen) => chosen.sourceId === s.sourceId && chosen.rowVersion !== s.rowVersion) && ' · Selected record changed; refresh selections.'}</span></label>)}</div>}
        </Catalog>
        <section className="panel submission-catalog" aria-label="Selected sources"><h2>Selected sources ({sources.length}/32)</h2>
          <p>Enabled is an admission gate. Each source must have a matching selected batch. Current source configuration is checked before review.</p>
          {sources.map((s) => <p key={s.sourceId}><code>{s.sourceId}</code> · revision {s.currentRevision} · {s.config.profile}{' '}
            <button type="button" className="secondary-button" onClick={() => removeSource(s.sourceId)}>Remove source {s.sourceId}</button></p>)}
        </section>
        <label>Batch source filter<select value={sourceFilter} onChange={(e) => setSourceFilter(e.target.value)}>
          <option value="">All sources in the selected profile</option>{sources.map((s) => <option key={s.sourceId} value={s.sourceId}>{s.sourceId}</option>)}
        </select></label>
        {!sources.length && <p>Select a current compatible source before browsing batch artifacts.</p>}
        {!!sources.length && <p className="inspection-note">3. Choose matching imported batches. Unavailable or incompatible inputs cannot be made ready by registering a source here.</p>}
        <Catalog title="Batch artifacts" scope={`${chosenProfile}:${sourceScope}:${sourceFilter}:${epoch}`} active={!!sources.length} load={loadBatches}
          empty="No compatible receipt-linked batch artifacts were returned. Discovery omits registrations without successful operator-import receipts; no import is performed here.">
          {(rows) => <div className="submission-choices">{rows.map((item) => item.role === 'RELIABILITY_BATCH' && <div key={item.artifactId}>
            <label><input type="checkbox" checked={batches.some((b) => b.artifactId === item.artifactId)}
              disabled={!batchMatches(profile, sources, item) || batches.length >= 32 && !batches.some((b) => b.artifactId === item.artifactId)}
              onChange={() => setBatches((old) => old.some((b) => b.artifactId === item.artifactId) ? old.filter((b) => b.artifactId !== item.artifactId) : [...old, item])} />
              <span><strong>{item.identity.batchId}</strong> · {item.identity.sourceId} revision {item.identity.sourceRevision} · {item.nativeFormat}
                <code className="inspection-id">{item.artifactId}</code></span></label>
            {!batchMatches(profile, sources, item) && <p>Select the matching current source revision, profile and storage reference to use this batch.</p>}<ArtifactMetadata artifact={item} />
          </div>)}</div>}
        </Catalog>
        <section className="panel submission-catalog" aria-label="Selected batches"><h2>Selected batches ({batches.length}/32)</h2>
          {batches.map((b) => <p key={b.artifactId}><code>{b.artifactId}</code> · {b.identity.sourceId} revision {b.identity.sourceRevision}{' '}
            <button type="button" className="secondary-button" onClick={() => setBatches((old) => old.filter((item) => item.artifactId !== b.artifactId))}>Remove batch {b.identity.batchId}</button></p>)}
        </section>
        <p className="inspection-note">4. Choose one imported service-reference snapshot for the whole run. Then review the exact frozen selection before confirming admission.</p>
        <Catalog title="Service references" scope={`${chosenProfile}:${epoch}`} active load={loadReferences}
          empty="No compatible receipt-linked service-reference artifacts were returned. Existing operator imports and profile storage associations are required.">
          {(rows) => <div className="submission-choices">{rows.map((item) => item.role === 'SERVICE_REFERENCE' && <div key={item.artifactId}>
            <label><input type="radio" name="service-reference" checked={reference?.artifactId === item.artifactId} onChange={() => setReference(item)} />
              <span><strong>{item.identity.referenceId}</strong> · revision {item.identity.revision}<code className="inspection-id">{item.artifactId}</code></span></label><ArtifactMetadata artifact={item} />
          </div>)}</div>}
        </Catalog>
        {reference && <p>Selected service reference: <code>{reference.artifactId}</code> · {reference.identity.referenceId} revision {reference.identity.revision}.</p>}
      </>}
      {validation.length > 0 && <div role="alert"><ul>{validation.map((message) => <li key={message}>{message}</li>)}</ul></div>}
      {error && <div role="alert"><p>Unable to check the current source. No run intent was created.</p><ManagementErrorDetails error={error} /></div>}
      <div className="source-action-buttons"><button type="button" className="primary-button" onClick={() => void review()}>Review Reliability submission</button>
        <button type="button" className="secondary-button" onClick={refreshSelections}>Refresh selections</button></div>
    </fieldset>
    {checking && <p role="status">Checking current source configuration…</p>}
  </div>;
}

export function ReliabilitySubmission() {
  const { intent, unavailable } = useRunIntent();
  return <section aria-label="Reliability run submission"><div className="page-heading"><div>
    <p className="eyebrow">OPERATIONS WORKSPACE / RELIABILITY</p><h1>Submit Reliability Run</h1>
    <p className="page-description">Select registered inputs, review the frozen request and explicitly confirm admission.</p>
  </div></div>
    <aside className="panel operator-guide" aria-label="Reliability prerequisites"><h2>Before you submit</h2>
      <p>The local operator must install an approved execution profile and import synthetic batches and a service reference. Source registration and admission are managed separately.</p>
      <p><a href="#/operations/sources">Review registered sources</a>. Discovery lists registered choices; it does not establish that a compatible worker is ready.</p>
      <p>Submission does not start a worker or change the publication selected in Analytics.</p>
    </aside>
    {intent && <p className="scope-notice">Resolve, dismiss or cancel the retained run intent above before preparing another submission.</p>}
    <SubmissionSelection active={!intent && !unavailable} />
  </section>;
}
