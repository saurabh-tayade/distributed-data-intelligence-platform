import { describe, expect, it } from 'vitest';
import { dailyChartRows, observationTotals } from './metrics';
import { dailyFixture, qualityFixture } from './test/analyticsFixtures';

describe('returned-scope metrics', () => {
  it('sums only additive observations and codes, never daily distinct counts or per-source rates', () => {
    expect(observationTotals(dailyFixture().data)).toEqual({ observations: 10n, warnings: 9n });
    expect(observationTotals(qualityFixture().data)).toEqual({ observations: 5n, warnings: 9n });
  });
  it('groups actual returned dates without synthesizing missing dates', () => {
    expect(dailyChartRows(dailyFixture().data)).toEqual([
      { date: '2026-10-12', observations: 6 }, { date: '2026-10-14', observations: 4 },
    ]);
  });
  it('keeps totals beyond signed 64-bit exact and checks chart safety after grouping', () => {
    const rows = dailyFixture().data;
    for (const row of rows) row.observationCount = '9223372036854775807';
    expect(observationTotals(rows).observations).toBe(27670116110564327421n);
    expect(dailyChartRows(rows)).toBeNull();
    const row = { ...rows[0]!, observationCount: '9007199254740991' };
    expect(dailyChartRows([row])).toEqual([{ date: '2026-10-12', observations: Number.MAX_SAFE_INTEGER }]);
    expect(dailyChartRows([row, { ...row, sourceId: 'other', observationCount: '1' }])).toBeNull();
  });
});
