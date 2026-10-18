import { describe, expect, it } from 'vitest';
import { chartRows, formatCount, summarize } from './metrics';
import { fixture } from './test/fixtures';

describe('exact metric handling', () => {
  it('aggregates only supplied publisher rows', () => {
    expect(summarize(fixture().data)).toEqual({ publishers: 2, articles: 8n, observations: 15n, withoutDate: 1n });
    expect(summarize([])).toEqual({ publishers: 0, articles: 0n, observations: 0n, withoutDate: 0n });
  });
  it('preserves totals beyond both Number precision and a single signed 64-bit counter', () => {
    const rows = fixture().data.map((row) => ({ ...row, articleCount: '9223372036854775807' }));
    const total = summarize(rows).articles;
    expect(total).toBe(18446744073709551614n);
    expect(formatCount(total)).toBe('18,446,744,073,709,551,614');
    expect(formatCount('9007199254740993')).toBe('9,007,199,254,740,993');
  });
  it('converts chart data only within the safe-integer boundary', () => {
    const rows = fixture().data;
    rows[0]!.articleCount = '9007199254740991';
    expect(chartRows(rows)?.[0]?.articles).toBe(Number.MAX_SAFE_INTEGER);
    rows[0]!.articleCount = '9007199254740992';
    expect(chartRows(rows)).toBeNull();
    rows[0]!.articleCount = '0';
    rows[1]!.observationCount = '9223372036854775807';
    expect(chartRows(rows)).toBeNull();
  });
});
