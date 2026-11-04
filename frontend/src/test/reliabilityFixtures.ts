import { RELIABILITY_LABEL, type ReliabilityQuery, type ReliabilityResponse, type ReliabilityRow } from '../reliabilityApi';

export function reliabilityQuery(overrides: Partial<ReliabilityQuery> = {}): ReliabilityQuery {
  return { start: '2026-10-20T12:00:00.000000Z', end: '2026-10-20T13:00:00.000000Z', ...overrides };
}
export function reliabilityRow(overrides: Partial<ReliabilityRow> = {}): ReliabilityRow {
  return {
    windowStart: '2026-10-20T12:00:00.000000Z', serviceId: 'alpha', environment: 'test',
    requestCount: '20', serverErrorCount: '2', serverErrorRate: 0.1, durationSumMs: '210',
    meanDurationMs: 10.5, p95DurationMsApprox: '19', serviceName: 'Alpha Service', ownerTeam: 'example-platform',
    serviceTier: 'critical', referenceStatus: 'MATCHED', ...overrides,
  };
}
export function reliabilityFixture(query: ReliabilityQuery = reliabilityQuery()): ReliabilityResponse {
  return {
    publication: 'a'.repeat(64), productId: 'application-reliability', productContractVersion: 2, processorRevision: 2,
    inputFormat: 'JSONL', origin: 'synthetic', label: RELIABILITY_LABEL,
    reference: { referenceId: 'services', revision: 'r1', snapshotFingerprint: 'b'.repeat(64) },
    filters: { serviceId: query.serviceId ?? null, environment: query.environment ?? null, start: query.start, end: query.end },
    data: [reliabilityRow({ windowStart: query.start, serviceId: query.serviceId ?? 'alpha', environment: query.environment ?? 'test' })],
    meta: { returned: 1, limit: query.limit ?? 100, offset: query.offset ?? 0, hasMore: false },
  };
}
