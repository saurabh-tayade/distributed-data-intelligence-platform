import type { ManagementError } from '../managementApi';

/** Only validated public metadata; arbitrary server messages never cross this boundary. */
export function ManagementErrorDetails({ error }: { error: ManagementError }) {
  return <>
    {error.code && <p><code>{error.code}</code></p>}
    {error.correlationId && <p>Reference: <code>{error.correlationId}</code></p>}
    {error.retryable !== null && <p>Retry guidance: {error.retryable
      ? 'The service marked this read as retryable.' : 'Resolve the reported condition before trying again.'}</p>}
  </>;
}
