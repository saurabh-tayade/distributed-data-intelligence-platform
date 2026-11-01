import type { RunResult } from '../managementApi';
import { Timestamp } from '../components/Timestamp';

export function PublicationResult({ result }: { result: RunResult | null }) {
  if (result === null) return <span>No result recorded</span>;
  return <>
    <strong>VERIFIED_AT_EXECUTION</strong><span className="run-metadata">Historical verification only</span>
    <details className="run-result"><summary>Recorded result metadata</summary><dl>
      <dt>Artifact ID</dt><dd><code>{result.artifactId}</code></dd>
      <dt>Native fingerprint</dt><dd><code>{result.nativeFingerprint}</code></dd>
      <dt>Product contract version</dt><dd>{result.productContractVersion}</dd>
      <dt>Processor revision</dt><dd>{result.processorRevision}</dd>
      <dt>Verified at (UTC)</dt><dd><Timestamp value={result.verifiedAt} /></dd>
    </dl></details>
  </>;
}
