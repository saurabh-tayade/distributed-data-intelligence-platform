import { useCallback, useEffect, useRef, useState } from 'react';
import { fetchContentProfiles, fetchContentSources, fetchPublisherReferences, fetchSource, ManagementError, parseContentRequest } from '../managementApi';
import type { ContentProfile, ContentSource, PublisherReference, RevisionQuery, SourceRecord } from '../managementApi';
import { Timestamp } from '../components/Timestamp';
import { ManagementErrorDetails } from './ManagementErrorDetails';
import { reviewRunSubmission, useRunIntent } from './runIntent';
import { SubmissionCatalog as Catalog } from './SubmissionCatalog';

/** Discovery is profile-scoped; the current source must still match its discovered revision and digest. */
export function contentSourceMatches(candidate: ContentSource, source: SourceRecord): boolean {
  return source.productId === 'content-intelligence' && source.sourceId === candidate.sourceId && source.enabled
    && source.config.profile === 'rss-atom-v1' && source.currentRevision === candidate.revision && source.configSha256 === candidate.configurationSha256;
}

function ContentSelection({ active }: { active: boolean }) {
  const [profile, setProfile] = useState<ContentProfile | null>(null), [source, setSource] = useState<ContentSource | null>(null);
  const [reference, setReference] = useState<PublisherReference | null>(null), [epoch, setEpoch] = useState(0);
  const [checking, setChecking] = useState(false), [validation, setValidation] = useState<string[]>([]), [error, setError] = useState<ManagementError | null>(null);
  const pending = useRef<AbortController | null>(null), profileId = profile?.executionProfileId;
  useEffect(() => () => { pending.current?.abort(); }, []);
  useEffect(() => { if (!active) { pending.current?.abort(); pending.current = null; } }, [active]);
  const loadSources = useCallback((query: RevisionQuery, signal: AbortSignal) => fetchContentSources({ ...query, executionProfileId: profileId! }, signal), [profileId]);
  const loadReferences = useCallback((query: RevisionQuery, signal: AbortSignal) => fetchPublisherReferences({ ...query, executionProfileId: profileId! }, signal), [profileId]);
  function changeProfile(next: ContentProfile) {
    if (next.executionProfileId === profileId) return;
    setProfile(next); setSource(null); setReference(null); setValidation([]); setError(null);
  }
  async function review() {
    if (pending.current || !active) return;
    const errors = [];
    if (!profile) errors.push('Select a registered Content execution profile.');
    if (!source || source.executionProfileId !== profileId) errors.push('Select exactly one compatible current Content source.');
    if (!reference) errors.push('Select one imported publisher-reference artifact.');
    setValidation(errors); setError(null);
    if (errors.length || !profile || !source || !reference) return;
    const abort = new AbortController(); pending.current = abort; setChecking(true);
    try {
      const { data } = await fetchSource({ productId: 'content-intelligence', sourceId: source.sourceId }, abort.signal);
      if (abort.signal.aborted) return;
      if (!contentSourceMatches(source, data)) {
        setValidation(['The selected source is disabled or changed since discovery. Refresh selections and deliberately select a compatible current revision.']); return;
      }
      const request = parseContentRequest({ pipeline: 'CONTENT_RSS_V1', executionProfileId: profile.executionProfileId,
        fetchSources: [{ sourceId: source.sourceId, revision: source.revision }], retainedSilverArtifactIds: [], publisherReferenceArtifactId: reference.artifactId });
      reviewRunSubmission(JSON.stringify(request));
    } catch (caught) {
      if (!abort.signal.aborted) {
        if (caught instanceof ManagementError) setError(caught);
        else setValidation(['The exact request could not be retained safely in this tab. No mutation was submitted.']);
      }
    } finally { if (pending.current === abort) { pending.current = null; setChecking(false); } }
  }
  function refresh() { setSource(null); setReference(null); setEpoch((old) => old + 1); setValidation([]); setError(null); }
  if (!active) return null;
  return <div className="reliability-submission">
    <p className="scope-notice">Discovery is registration metadata only. Dispatch disabled: true · Qualification scope: REGISTRATION_METADATA_ONLY · Runtime qualification: NOT_CHECKED.
      These markers do not establish installed-runtime readiness, worker operation or public-feed qualification.</p>
    <fieldset className="source-form-fields" disabled={checking}>
      <legend className="visually-hidden">Content run selections</legend>
      <p className="inspection-note">1. Choose a registered Content execution profile. Then select exactly one compatible current RSS/Atom source and one imported publisher reference. No retained Silver is supported.</p>
      <Catalog title="Content execution profiles" scope="content-profiles" active load={fetchContentProfiles} empty="No compatible registered Content execution profiles were returned. Registration is managed through the existing operator procedure.">
        {(rows) => <div className="submission-choices">{rows.map((item) => <div key={item.executionProfileId}>
          <label><input type="radio" name="content-execution-profile" checked={profileId === item.executionProfileId} onChange={() => changeProfile(item)} />
            <span><strong>RSS/Atom Content</strong> · {item.sourceProfile}<code className="inspection-id">{item.executionProfileId}</code></span></label>
          <details><summary>Content profile contract metadata</summary><p>CONTENT_RSS_V1 · Run profile {item.runProfileVersion} · Pipeline {item.pipelineVersion} · One fresh source, zero retained Silver, one publisher reference.</p>
            <p>Configuration SHA-256: <code>{item.configurationSha256}</code></p><p>Registered: <Timestamp value={item.createdAt} /></p></details>
        </div>)}</div>}
      </Catalog>
      {profile && <>
        <p>Selected Content profile: <code>{profileId}</code>.</p>
        <p className="inspection-note">2. Choose one enabled source revision advertised as compatible with this profile. Its current source record will be rechecked before freezing the request.</p>
        <Catalog title="Compatible Content sources" scope={`${profileId}:${epoch}`} active load={loadSources} empty="No compatible enabled Content source revisions were returned. Review source admission, the reviewed catalog and profile associations separately.">
          {(rows) => <div className="submission-choices">{rows.map((item) => <label key={item.sourceId}>
            <input type="radio" name="content-source" checked={source?.sourceId === item.sourceId && source.revision === item.revision && source.configurationSha256 === item.configurationSha256} onChange={() => setSource(item)} />
            <span><code>{item.sourceId}</code> · Current revision {item.revision} · {item.sourceProfile}<code className="inspection-id">{item.configurationSha256}</code></span>
          </label>)}</div>}
        </Catalog>
        {source && <p>Selected Content source: <code>{source.sourceId}</code> · revision {source.revision}.{' '}
          <button type="button" className="secondary-button" onClick={() => setSource(null)}>Remove Content source</button></p>}
        <p className="inspection-note">3. Choose one imported publisher-reference snapshot. Historical registration and verification do not establish current native-file readiness.</p>
        <Catalog title="Publisher references" scope={`${profileId}:${epoch}`} active load={loadReferences} empty="No compatible receipt-linked publisher-reference artifacts were returned. A successful operator import and profile storage association are required; no import is performed here.">
          {(rows) => <div className="submission-choices">{rows.map((item) => <div key={item.artifactId}>
            <label><input type="radio" name="publisher-reference" checked={reference?.artifactId === item.artifactId} onChange={() => setReference(item)} />
              <span><strong>{item.identity.referenceId}</strong> · revision {item.identity.revision}<code className="inspection-id">{item.artifactId}</code></span></label>
            <details><summary>Publisher reference registration metadata</summary><dl className="source-metadata">
              <div><dt>Contract / processor</dt><dd>{item.contractVersion} / {item.processorRevision}</dd></div>
              <div><dt>Storage binding</dt><dd><code>{item.storageBindingId}</code></dd></div>
              <div><dt>Native fingerprint</dt><dd><code>{item.nativeFingerprint}</code></dd></div>
              <div><dt>Historically verified (UTC)</dt><dd><Timestamp value={item.verifiedAt} /></dd></div>
            </dl></details>
          </div>)}</div>}
        </Catalog>
        {reference && <p>Selected publisher reference: <code>{reference.artifactId}</code> · {reference.identity.referenceId} revision {reference.identity.revision}.</p>}
        <p>Retained Silver artifacts: <strong>0</strong>.</p>
      </>}
      {validation.length > 0 && <div role="alert"><ul>{validation.map((message) => <li key={message}>{message}</li>)}</ul></div>}
      {error && <div role="alert"><p>Unable to check the current Content source. No run intent was created.</p><ManagementErrorDetails error={error} /></div>}
      <div className="source-action-buttons"><button type="button" className="primary-button" onClick={() => void review()}>Review Content submission</button>
        <button type="button" className="secondary-button" onClick={refresh}>Refresh Content selections</button></div>
    </fieldset>
    {checking && <p role="status">Checking current Content source configuration…</p>}
  </div>;
}

export function ContentSubmission() {
  const { intent, unavailable } = useRunIntent();
  return <section aria-label="Content run submission"><div className="page-heading"><div>
    <p className="eyebrow">OPERATIONS WORKSPACE / CONTENT</p><h1>Submit Queued Content Run</h1>
    <p className="page-description">Select registered inputs and deliberately request durable queue admission. Public Content execution remains unqualified.</p>
  </div></div>
    <aside className="panel operator-guide" aria-label="Content prerequisites"><h2>Before you queue Content</h2>
      <p>A local operator must register a compatible Content execution profile, enable a reviewed RSS/Atom source and import a publisher reference separately.</p>
      <p>Queue admission does not start or guarantee processing. A run may remain queued indefinitely and consumes shared unfinished-run capacity (20 total).
        A compatible worker started under separate approval could claim it in the future.</p>
      <p>Successful processing does not automatically change the Analytics publication selected for reading. The backend remains authoritative for admission, validation, capacity and execution controls.</p>
      <p><a href="#/operations/sources">Review registered sources</a>. The profile-scoped discovery below determines compatible choices.</p>
    </aside>
    {intent && <p className="scope-notice">Resolve, dismiss or cancel the retained run intent above before preparing another submission. Content and Reliability share one intent per tab.</p>}
    <ContentSelection active={!intent && !unavailable} />
  </section>;
}
