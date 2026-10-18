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
