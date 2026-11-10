import { useSyncExternalStore } from 'react';
import { RETRY_BODY, isRetryReceipt, parseControl, validateRetryCommand } from '../managementApi';
import type { RetryCommand, RetryResponse, RunDetailResponse } from '../managementApi';
import type { RunIntent } from './runIntent';

export const RETRY_INTENT_KEY = 'ddip.retry-run-intent.v1';
const EVENT = 'ddip-retry-intent';
export interface RetryIntent {
  version: 1; command: RetryCommand; phase: RunIntent['phase']; error?: RunIntent['error']; receipt?: RetryResponse;
}
function snapshot() { try { return sessionStorage.getItem(RETRY_INTENT_KEY) ?? ''; } catch { return '!unavailable'; } }
function subscribe(listener: () => void) {
  window.addEventListener(EVENT, listener); window.addEventListener('storage', listener);
  return () => { window.removeEventListener(EVENT, listener); window.removeEventListener('storage', listener); };
}
export function readRetryIntent(raw = snapshot()): RetryIntent | null {
  if (!raw) return null;
  if (raw.length > 24576) throw new Error('Oversized saved retry');
  const value = JSON.parse(raw) as RetryIntent;
  if (!value || value.version !== 1 || !['review', 'pending', 'uncertain', 'rejected', 'confirmed'].includes(value.phase)
    || Object.keys(value).some((key) => !['version', 'command', 'phase', 'error', 'receipt'].includes(key))) throw new Error('Invalid saved retry');
  validateRetryCommand(value.command);
  if (value.receipt) {
    const r = value.receipt;
    parseControl({ apiVersion: r.apiVersion, data: r.data, meta: r.meta }, value.command.runId, r.data.controlId, r.etag);
    if (value.phase !== 'confirmed' || !isRetryReceipt(r.data) || r.location !== `/api/management/v1/runs/${value.command.runId}/controls/${r.data.controlId}`) throw new Error('Invalid receipt');
  }
  if (value.phase === 'confirmed' && !value.receipt) throw new Error('Missing receipt');
  if (value.error && (typeof value.error.ambiguous !== 'boolean' || ![null, true, false].includes(value.error.retryable)
    || value.error.status !== null && (!Number.isInteger(value.error.status) || value.error.status < 100 || value.error.status > 599)
    || value.error.code !== null && (typeof value.error.code !== 'string' || !/^[A-Z_]{1,80}$/.test(value.error.code))
    || value.error.correlationId !== null && (typeof value.error.correlationId !== 'string' || !/^[a-zA-Z0-9-]{1,128}$/.test(value.error.correlationId)))) throw new Error('Invalid error');
  return value;
}
export function useRetryIntent() {
  const raw = useSyncExternalStore(subscribe, snapshot, () => '');
  try { return { intent: readRetryIntent(raw), unavailable: false }; } catch { return { intent: null, unavailable: true }; }
}
export function saveRetryIntent(value: RetryIntent | null) {
  if (value) { const raw = JSON.stringify(value); readRetryIntent(raw); sessionStorage.setItem(RETRY_INTENT_KEY, raw); }
  else sessionStorage.removeItem(RETRY_INTENT_KEY);
  window.dispatchEvent(new Event(EVENT));
}
export function canRetryRun(record: RunDetailResponse) {
  const run = record.data;
  return run.pipeline === 'RELIABILITY_V2' && ['FAILED', 'CANCELLED'].includes(run.state) && run.retryEligibility
    && run.result === null && run.attempts.currentAttemptId !== null && run.latestControl?.state !== 'REQUESTED';
}
export function reviewRetryRun(record: RunDetailResponse) {
  if (snapshot() !== '' || !canRetryRun(record)) throw new Error('Retry unavailable');
  saveRetryIntent({ version: 1, phase: 'review', command: { runId: record.data.runId, key: crypto.randomUUID(), body: RETRY_BODY, ifMatch: record.etag } });
}
