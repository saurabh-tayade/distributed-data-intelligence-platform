import { useSyncExternalStore } from 'react';
import { CANCEL_BODY, isCancellationReceipt, parseControl, validateCancelCommand } from '../managementApi';
import type { CancelCommand, CancelResponse, RunDetailResponse } from '../managementApi';
import type { RunIntent } from './runIntent';

export const CANCEL_INTENT_KEY = 'ddip.cancel-run-intent.v1';
const EVENT = 'ddip-cancel-intent';
export interface CancelIntent {
  version: 1; mode?: 'queued' | 'running'; command: CancelCommand; phase: RunIntent['phase']; error?: RunIntent['error']; receipt?: CancelResponse;
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
    || Object.keys(value).some((key) => !['version', 'command', 'phase', 'error', 'receipt', 'mode'].includes(key))) throw new Error('Invalid saved cancellation');
  if (value.mode !== undefined && value.mode !== 'queued' && value.mode !== 'running') throw new Error('Invalid cancellation mode');
  validateCancelCommand(value.command);
  if (value.receipt) {
    const r = value.receipt;
    parseControl({ apiVersion: r.apiVersion, data: r.data, meta: r.meta }, value.command.runId, r.data.controlId, r.etag);
    if (value.phase !== 'confirmed' || !isCancellationReceipt(r.data) || r.location !== `/api/management/v1/runs/${value.command.runId}/controls/${r.data.controlId}`) throw new Error('Invalid receipt');
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
  const run = record.data;
  // Displayed facts are review hints, never ownership or launch authority. The server
  // validates the exact current retry lifecycle under the captured run version.
  const retry = run.pipeline === 'RELIABILITY_V2' && run.latestControl?.type === 'RETRY'
    && run.latestControl.state === 'APPLIED' && run.latestControl.reason === 'RETRY_QUEUED';
  if (run.result !== null || run.latestControl?.state === 'REQUESTED') return false;
  return run.state === 'QUEUED' && run.attempts.currentAttemptId === null
    && (run.attempts.count === '0' || retry && run.outcomeCode === 'RETRY_QUEUED' && BigInt(run.attempts.count) > 0n)
    || run.state === 'RUNNING' && run.pipeline === 'RELIABILITY_V2' && run.attempts.currentAttemptId !== null
      && (run.attempts.count === '1' || retry && BigInt(run.attempts.count) > 1n);
}
export function reviewCancelRun(record: RunDetailResponse) {
  if (snapshot() !== '' || !canCancelRun(record)) throw new Error('Cancellation unavailable');
  saveCancelIntent({ version: 1, mode: record.data.state === 'RUNNING' ? 'running' : 'queued', phase: 'review', command: { runId: record.data.runId, key: crypto.randomUUID(), body: CANCEL_BODY, ifMatch: record.etag } });
}
