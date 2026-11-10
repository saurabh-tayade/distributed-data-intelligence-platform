import { useSyncExternalStore } from 'react';
import { CANCEL_BODY, parseControl, validateCancelCommand } from '../managementApi';
import type { CancelCommand, CancelResponse, RunDetailResponse } from '../managementApi';
import type { RunIntent } from './runIntent';

export const CANCEL_INTENT_KEY = 'ddip.cancel-run-intent.v1';
const EVENT = 'ddip-cancel-intent';
export interface CancelIntent {
  version: 1; command: CancelCommand; phase: RunIntent['phase']; error?: RunIntent['error']; receipt?: CancelResponse;
}
function snapshot() { try { return sessionStorage.getItem(CANCEL_INTENT_KEY) ?? ''; } catch { return '!unavailable'; } }
function subscribe(listener: () => void) {
  window.addEventListener(EVENT, listener); window.addEventListener('storage', listener);
  return () => { window.removeEventListener(EVENT, listener); window.removeEventListener('storage', listener); };
}
export function readCancelIntent(raw = snapshot()): CancelIntent | null {
  if (!raw) return null;
  if (raw.length > 24576) throw new Error('Oversized saved cancellation');
  const value = JSON.parse(raw) as CancelIntent;
  if (!value || value.version !== 1 || !['review', 'pending', 'uncertain', 'rejected', 'confirmed'].includes(value.phase)
    || Object.keys(value).some((key) => !['version', 'command', 'phase', 'error', 'receipt'].includes(key))) throw new Error('Invalid saved cancellation');
  validateCancelCommand(value.command);
  if (value.receipt) {
    const r = value.receipt;
    parseControl({ apiVersion: r.apiVersion, data: r.data, meta: r.meta }, value.command.runId, r.data.controlId, r.etag);
    if (value.phase !== 'confirmed' || r.data.type !== 'CANCEL' || r.data.state !== 'APPLIED' || r.data.reason !== 'CANCELLED' || r.location !== `/api/management/v1/runs/${value.command.runId}/controls/${r.data.controlId}`) throw new Error('Invalid receipt');
  }
  if (value.phase === 'confirmed' && !value.receipt) throw new Error('Missing receipt');
  if (value.error && (typeof value.error.ambiguous !== 'boolean' || ![null, true, false].includes(value.error.retryable)
    || value.error.status !== null && (!Number.isInteger(value.error.status) || value.error.status < 100 || value.error.status > 599)
    || value.error.code !== null && (typeof value.error.code !== 'string' || !/^[A-Z_]{1,80}$/.test(value.error.code))
    || value.error.correlationId !== null && (typeof value.error.correlationId !== 'string' || !/^[a-zA-Z0-9-]{1,128}$/.test(value.error.correlationId)))) throw new Error('Invalid error');
  return value;
}
export function useCancelIntent() {
  const raw = useSyncExternalStore(subscribe, snapshot, () => '');
  try { return { intent: readCancelIntent(raw), unavailable: false }; } catch { return { intent: null, unavailable: true }; }
}
export function saveCancelIntent(value: CancelIntent | null) {
  if (value) { const raw = JSON.stringify(value); readCancelIntent(raw); sessionStorage.setItem(CANCEL_INTENT_KEY, raw); }
  else sessionStorage.removeItem(CANCEL_INTENT_KEY);
  window.dispatchEvent(new Event(EVENT));
}
export function canCancelRun(record: RunDetailResponse) {
  return record.data.state === 'QUEUED' && record.data.attempts.count === '0' && record.data.attempts.currentAttemptId === null;
}
export function reviewCancelRun(record: RunDetailResponse) {
  if (snapshot() !== '' || !canCancelRun(record)) throw new Error('Cancellation unavailable');
  saveCancelIntent({ version: 1, phase: 'review', command: { runId: record.data.runId, key: crypto.randomUUID(), body: CANCEL_BODY, ifMatch: record.etag } });
}
