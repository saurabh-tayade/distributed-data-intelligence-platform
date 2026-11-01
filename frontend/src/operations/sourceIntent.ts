import { useSyncExternalStore } from 'react';
import { SourceCommandError, validateSourceCommand } from '../managementApi';
import type { SourceCommand } from '../managementApi';

// One unresolved operator intent per tab, retained across reloads. No credentials or private configuration.
export const SOURCE_INTENT_KEY = 'ddip.source-intent.v1';
const EVENT = 'ddip-source-intent';
export const SOURCE_CHANGED = 'ddip-source-changed';
export interface SourceIntent {
  command: SourceCommand;
  phase: 'review' | 'pending' | 'uncertain' | 'confirmed' | 'rejected';
  error?: { status: number | null; code: string | null; correlationId: string | null; retryable: boolean | null; ambiguous: boolean };
}
function snapshot() {
  try { return sessionStorage.getItem(SOURCE_INTENT_KEY) ?? ''; } catch { return '!unavailable'; }
}
function subscribe(listener: () => void) {
  window.addEventListener(EVENT, listener); window.addEventListener('storage', listener);
  return () => { window.removeEventListener(EVENT, listener); window.removeEventListener('storage', listener); };
}
export function readSourceIntent(raw = snapshot()): SourceIntent | null {
  if (!raw) return null;
  if (raw.length > 8192) throw new Error('Invalid saved intent');
  const value = JSON.parse(raw) as SourceIntent;
  if (!value || !['review', 'pending', 'uncertain', 'confirmed', 'rejected'].includes(value.phase)) throw new Error('Invalid saved intent');
  validateSourceCommand(value.command);
  if (value.error) {
    // Only metadata produced by the validated transport is persisted and displayed.
    const { status, code, correlationId, retryable, ambiguous } = value.error;
    if (!(status === null || Number.isInteger(status) && status >= 100 && status <= 599)
      || !(code === null || /^[A-Z_]{1,64}$/.test(code))
      || !(correlationId === null || /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(correlationId))
      || !(retryable === null || typeof retryable === 'boolean') || typeof ambiguous !== 'boolean') throw new Error('Invalid saved intent');
  }
  return value;
}
export function useSourceIntent() {
  const raw = useSyncExternalStore(subscribe, snapshot, () => '');
  try { return { intent: readSourceIntent(raw), unavailable: false }; }
  catch { return { intent: null, unavailable: true }; }
}
export function saveSourceIntent(intent: SourceIntent | null) {
  if (intent) {
    const raw = JSON.stringify(intent); readSourceIntent(raw); sessionStorage.setItem(SOURCE_INTENT_KEY, raw);
  } else sessionStorage.removeItem(SOURCE_INTENT_KEY);
  window.dispatchEvent(new Event(EVENT));
}
export function reviewSourceCommand(command: Omit<SourceCommand, 'key'>) {
  if (snapshot() !== '') throw new Error('Resolve the existing source intent first');
  saveSourceIntent({ command: { ...command, key: crypto.randomUUID() }, phase: 'review' });
}
export function commandError(error: unknown): SourceCommandError {
  return error instanceof SourceCommandError ? error : new SourceCommandError(null, null, null, null, true);
}
export function sourceChanged() { window.dispatchEvent(new Event(SOURCE_CHANGED)); }
