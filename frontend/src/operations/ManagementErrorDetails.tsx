import type { ManagementError } from '../managementApi';

/** Only validated public metadata; arbitrary server messages never cross this boundary. */
export function ManagementErrorDetails({ error, mutation = false }: { error: Pick<ManagementError, 'code' | 'correlationId' | 'retryable'>; mutation?: boolean }) {
  return <>
    {error.code && <p><code>{error.code}</code></p>}
    {error.correlationId && <p>Reference: <code>{error.correlationId}</code></p>}
    {error.retryable !== null && <p>Retry guidance: {error.retryable
      ? mutation ? 'The service marked this request as retryable. Resolve an unknown outcome with the same request.' : 'The service marked this read as retryable.'
      : 'Resolve the reported condition before trying again.'}</p>}
  </>;
}
