import { useSyncExternalStore } from 'react';
import { isManagementId, RUN_STATES, validateRunSubmission } from '../managementApi';
import type { RunState, RunSubmissionCommand, RunSubmissionResponse } from '../managementApi';

export const RUN_INTENT_KEY = 'ddip.reliability-run-intent.v1';
const EVENT = 'ddip-run-intent';
export interface RunIntent {
  version: 1; command: RunSubmissionCommand; phase: 'review' | 'pending' | 'uncertain' | 'rejected' | 'confirmed';
  error?: { status: number | null; code: string | null; correlationId: string | null; retryable: boolean | null; ambiguous: boolean };
  receipt?: { runId: string; state: RunState; rowVersion: string; etag: string; correlationId: string; location: string };
}
function snapshot() { try { return sessionStorage.getItem(RUN_INTENT_KEY) ?? ''; } catch { return '!unavailable'; } }
function subscribe(listener: () => void) {
  window.addEventListener(EVENT, listener); window.addEventListener('storage', listener);
  return () => { window.removeEventListener(EVENT, listener); window.removeEventListener('storage', listener); };
}
function require(value: unknown): asserts value { if (!value) throw new Error('Invalid saved run intent'); }
export function readRunIntent(raw = snapshot()): RunIntent | null {
  if (!raw) return null;
  require(raw.length <= 24576);
  const value = JSON.parse(raw) as RunIntent;
  require(value && value.version === 1 && ['review', 'pending', 'uncertain', 'rejected', 'confirmed'].includes(value.phase));
  require(Object.keys(value).every((key) => ['version', 'command', 'phase', 'error', 'receipt'].includes(key)));
  validateRunSubmission(value.command);
  if (value.error) {
    const { status, code, correlationId, retryable, ambiguous } = value.error;
    require((status === null || Number.isInteger(status) && status >= 100 && status <= 599)
      && (code === null || ['INVALID_ARGUMENT', 'LOCAL_BOUNDARY_REJECTED', 'NOT_FOUND', 'STATE_CONFLICT', 'SOURCE_DISABLED', 'IDEMPOTENCY_CONFLICT', 'MANAGEMENT_CAPACITY_EXCEEDED', 'INTERNAL_FAILURE', 'MANAGEMENT_UNAVAILABLE', 'COORDINATOR_RECOVERY_REQUIRED'].includes(code))
      && (correlationId === null || isManagementId(correlationId)) && (retryable === null || typeof retryable === 'boolean') && typeof ambiguous === 'boolean');
  }
  if (value.receipt) {
    const r = value.receipt;
    require(value.phase === 'confirmed' && isManagementId(r.runId) && RUN_STATES.includes(r.state) && isManagementId(r.correlationId)
      && typeof r.rowVersion === 'string' && /^(0|[1-9][0-9]{0,18})$/.test(r.rowVersion) && BigInt(r.rowVersion) <= 9223372036854775807n
      && r.etag === `"${r.rowVersion}"` && r.location === `/api/management/v1/runs/${r.runId}`);
  }
  require(value.phase !== 'confirmed' || value.receipt);
  return value;
}
export function useRunIntent() {
  const raw = useSyncExternalStore(subscribe, snapshot, () => '');
  try { return { intent: readRunIntent(raw), unavailable: false }; }
  catch { return { intent: null, unavailable: true }; }
}
export function saveRunIntent(value: RunIntent | null) {
  if (value) { const raw = JSON.stringify(value); readRunIntent(raw); sessionStorage.setItem(RUN_INTENT_KEY, raw); }
  else sessionStorage.removeItem(RUN_INTENT_KEY);
  window.dispatchEvent(new Event(EVENT));
}
export function reviewRunSubmission(body: string) {
  if (snapshot() !== '') throw new Error('Resolve the existing run intent first');
  saveRunIntent({ version: 1, command: { key: crypto.randomUUID(), body }, phase: 'review' });
}
export function confirmedRunIntent(intent: RunIntent, response: RunSubmissionResponse): RunIntent {
  const { runId, state, rowVersion } = response.data;
  return { version: 1, command: intent.command, phase: 'confirmed', receipt: { runId, state, rowVersion, etag: response.etag,
    correlationId: response.meta.correlationId, location: response.location } };
}
