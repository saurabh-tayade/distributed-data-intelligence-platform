import type { ProductId, RunState, RunSummary, RunsPage } from '../managementApi';
import { CORRELATION, FIRST_CURSOR } from './managementFixtures';

export function run(index = 1, state: RunState = 'QUEUED', productId: ProductId = 'content-intelligence'): RunSummary {
  const reliability = productId === 'application-reliability';
  return { runId: `40000000-0000-4000-8000-${index.toString(16).padStart(12, '0')}`, productId,
    pipeline: reliability ? 'RELIABILITY_V2' : 'CONTENT_RSS_V1', state,
    currentStage: state === 'RUNNING' ? 'CHECK_INPUTS' : state === 'SUCCEEDED' ? 'VERIFY_RESULT' : null,
    rowVersion: '9007199254740993', requestedAt: '2026-11-01T12:34:56.654321Z',
    finishedAt: ['SUCCEEDED', 'FAILED', 'CANCELLED'].includes(state) ? '2026-11-01T12:40:00.123456Z' : null,
    outcomeCode: state === 'FAILED' ? 'INPUT_INVALID' : state === 'SUCCEEDED' ? 'SUCCEEDED' : state === 'CANCELLED' ? 'CANCELLED' : null,
    retryEligibility: false, ...(reliability ? { dataOrigin: 'synthetic' as const } : {}),
    result: state === 'SUCCEEDED' ? { artifactId: '50000000-0000-4000-8000-000000000001', nativeFingerprint: 'a'.repeat(64),
      productContractVersion: reliability ? 2 : 1, processorRevision: reliability ? 2 : 1, verificationStatus: 'VERIFIED_AT_EXECUTION',
      verifiedAt: '2026-11-01T12:39:59.987654Z' } : null };
}
export function runPage(rows: RunSummary[] = [run()], limit = 20, nextCursor: string | null = null): RunsPage {
  return { apiVersion: 1, data: rows, meta: { correlationId: CORRELATION, limit, hasMore: nextCursor !== null, nextCursor } };
}
export function fullRunPage(start = 20, nextCursor: string | null = FIRST_CURSOR): RunsPage {
  return runPage(Array.from({ length: 20 }, (_, i) => run(start - i)), 20, nextCursor);
}
