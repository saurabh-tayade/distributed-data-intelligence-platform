import type { AuditEvent, ControlReceipt, LatestControl, ManagementPage, RunAttempt, RunDetail } from '../managementApi';
import { jsonResponse } from './fixtures';
import { CORRELATION } from './managementFixtures';
import { run } from './runFixtures';

export const ATTEMPT_ID = '60000000-0000-4000-8000-000000000001';
export const CONTROL_ID = '70000000-0000-4000-8000-000000000001';
export function latestControl(state: LatestControl['state'] = 'REQUESTED'): LatestControl {
  return { controlId: CONTROL_ID, type: 'RECONCILE', state, reason: state === 'REQUESTED' ? null : 'RECOVERED_CHECKPOINT',
    requestedAt: '2026-11-01T12:34:56.654321Z', appliedAt: state === 'REQUESTED' ? null : '2026-11-01T12:40:00.123456Z', rowVersion: '12' };
}
export function runDetail(index = 1, state: RunDetail['state'] = 'QUEUED', product: RunDetail['productId'] = 'content-intelligence'): RunDetail {
  return { ...run(index, state, product), counts: null, attempts: { count: '0', currentAttemptId: null }, latestControl: null,
    sourceRevisions: [{ sourceId: 'alpha', revision: 1 }], inputArtifactIds: ['50000000-0000-4000-8000-000000000001'] };
}
export function recordEnvelope<T>(data: T) { return { apiVersion: 1 as const, data, meta: { correlationId: CORRELATION } }; }
export function detailResponse(record = runDetail()): Response {
  return new Response(JSON.stringify(recordEnvelope(record)), { headers: { 'Content-Type': 'application/json', ETag: `"${record.rowVersion}"` } });
}
export function controlReceipt(state: LatestControl['state'] = 'REQUESTED', runId = run().runId): ControlReceipt {
  return { ...latestControl(state), runId, runVersion: '9007199254740994' };
}
export function controlResponse(record = controlReceipt()): Response {
  return new Response(JSON.stringify(recordEnvelope(record)), { headers: { 'Content-Type': 'application/json', ETag: `"${record.rowVersion}"` } });
}
export function attempt(attemptNo = 1, kind: RunAttempt['kind'] = 'EXECUTE'): RunAttempt {
  return { attemptId: `60000000-0000-4000-8000-${attemptNo.toString(16).padStart(12, '0')}`, attemptNo, kind,
    state: 'FAILED', startedAt: '2026-11-01T12:34:56.654321Z', finishedAt: '2026-11-01T12:40:00.123456Z', outcomeCode: 'INPUT_INVALID', stageCount: '2' };
}
export function auditEvent(eventSeq = '9007199254740993'): AuditEvent {
  return { eventSeq, timestamp: '2026-11-01T12:34:56.654321Z', action: 'STAGE_STARTED', beforeState: 'INTENT', afterState: 'RUNNING', code: 'STARTED',
    beforeVersion: '9007199254740993', afterVersion: '9007199254740994', context: { attemptId: ATTEMPT_ID, controlId: null } };
}
export function historyPage<T>(rows: T[], limit = 20, nextCursor: string | null = null): ManagementPage<T> {
  return { apiVersion: 1, data: rows, meta: { correlationId: CORRELATION, limit, nextCursor, hasMore: nextCursor !== null } };
}
export const attemptsResponse = (rows = [attempt()], limit = 20, cursor: string | null = null) => jsonResponse(historyPage(rows, limit, cursor));
export const eventsResponse = (rows = [auditEvent()], limit = 20, cursor: string | null = null) => jsonResponse(historyPage(rows, limit, cursor));
