import type { Publisher } from './api';

const integerFormat = new Intl.NumberFormat('en-US');
export const formatCount = (value: string | bigint | number) => integerFormat.format(typeof value === 'string' ? BigInt(value) : value);

export function summarize(rows: Publisher[]) {
  return rows.reduce((totals, row) => ({
    publishers: totals.publishers + 1,
    articles: totals.articles + BigInt(row.articleCount),
    observations: totals.observations + BigInt(row.observationCount),
    withoutDate: totals.withoutDate + BigInt(row.articlesWithoutPublishedAt),
  }), { publishers: 0, articles: 0n, observations: 0n, withoutDate: 0n });
}

export function chartRows(rows: Publisher[]) {
  // A rounded chart can misrepresent relative magnitudes. Keep exact values in the table instead.
  if (rows.some((row) => [row.articleCount, row.observationCount].some((count) => BigInt(count) > BigInt(Number.MAX_SAFE_INTEGER)))) {
    return null;
  }
  return rows.map((row) => ({ sourceId: row.sourceId, articles: Number(row.articleCount), observations: Number(row.observationCount) }));
}

/** Only additive observations and warning codes are aggregated; never daily distinct identities or rates. */
export function observationTotals(rows: { observationCount: string; qualityWarningCount: string }[]) {
  return rows.reduce((totals, row) => ({ observations: totals.observations + BigInt(row.observationCount),
    warnings: totals.warnings + BigInt(row.qualityWarningCount) }), { observations: 0n, warnings: 0n });
}

export function dailyObservationTotals(rows: import('./api').DailyRow[]) {
  const dates = new Map<string, bigint>();
  for (const row of rows) dates.set(row.ingestionDate, (dates.get(row.ingestionDate) ?? 0n) + BigInt(row.observationCount));
  return [...dates].sort(([a], [b]) => a < b ? -1 : a > b ? 1 : 0).map(([date, observations]) => ({ date, observations }));
}

export function dailyChartRows(rows: import('./api').DailyRow[]) {
  const dates = dailyObservationTotals(rows);
  if (dates.some(({ observations }) => observations > BigInt(Number.MAX_SAFE_INTEGER))) return null;
  return dates.map(({ date, observations }) => ({ date, observations: Number(observations) }));
}
