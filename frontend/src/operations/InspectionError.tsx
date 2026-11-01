import type { ManagementError, ManagementErrorKind } from '../managementApi';
import { ManagementErrorDetails } from './ManagementErrorDetails';

const messages: Record<ManagementErrorKind, string> = {
  'invalid-request': 'This read or page cursor was rejected. History cursors expire after 24 hours; restart the panel traversal if needed.',
  'local-boundary': 'Local access was rejected. Check the approved UI origin and API host.',
  'not-found': 'The requested record was not found. Check the run or control identity and the API-mode service.',
  capacity: 'Management capacity is unavailable. Read again when capacity is available.',
  unavailable: 'Management data is unavailable. Check the local service before trying again.',
  server: 'The management service could not complete this read.',
  network: 'Cannot reach the local management service. Check the API and development proxy.',
  'invalid-response': 'The response does not match the inspection contract. No data from that response is displayed.',
};
export function InspectionError({ title, error, retry, restart, disabled = false }: { title: string; error: ManagementError; retry: () => void; restart?: (() => void) | undefined; disabled?: boolean }) {
  return <div className="state-panel error-state" role="alert"><h3>{title}</h3><p>{messages[error.kind]}</p><ManagementErrorDetails error={error} />
    <button className="primary-button" type="button" disabled={disabled} onClick={retry}>Try again</button>
    {restart && error.kind === 'invalid-request' && <button className="secondary-button" type="button" onClick={restart}>Start from first page</button>}
  </div>;
}
