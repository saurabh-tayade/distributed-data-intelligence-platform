import type { Article, ArticlesResponse, DailyResponse, QualityResponse } from '../api';
import { FINGERPRINT, MICROSECOND_TIME } from './fixtures';

export function article(index = 1, sourceId = 'alpha'): Article {
  return { articleId: index.toString(16).padStart(64, '0'), sourceId, title: `Article ${index}`,
    url: `https://example.test/articles/${index}`, summary: `Plain summary ${index}`,
    publishedAt: null, updatedAt: null, firstObservedAt: MICROSECOND_TIME, lastObservedAt: MICROSECOND_TIME,
    observationCount: '2', distinctIngestionCount: '1', qualityWarnings: [] };
}
export function articlesFixture(offset = 0, total = 23, sourceId = 'alpha', limit = 10): ArticlesResponse {
  const count = Math.max(0, Math.min(limit, total - offset));
  return { publication: FINGERPRINT, data: Array.from({ length: count }, (_, i) => article(offset + i + 1, sourceId)),
    meta: { returned: count, limit, offset, hasMore: offset + count < total } };
}
export function dailyFixture(): DailyResponse {
  return { publication: FINGERPRINT, data: [
    { sourceId: 'alpha', ingestionDate: '2026-10-12', observationCount: '5', distinctArticleCount: '3', distinctIngestionCount: '2',
      qualityWarningCount: '0', latestIngestedAt: MICROSECOND_TIME },
    { sourceId: 'alpha', ingestionDate: '2026-10-14', observationCount: '4', distinctArticleCount: '3', distinctIngestionCount: '1',
      qualityWarningCount: '8', latestIngestedAt: '2026-10-14T11:12:13.999999Z' },
    { sourceId: 'beta', ingestionDate: '2026-10-12', observationCount: '1', distinctArticleCount: '1', distinctIngestionCount: '1',
      qualityWarningCount: '1', latestIngestedAt: MICROSECOND_TIME },
  ], meta: { returned: 3, limit: 100 } };
}
export function qualityFixture(): QualityResponse {
  return { publication: FINGERPRINT, data: [
    { sourceId: 'alpha', observationCount: '4', qualityWarningCount: '8', warningCodesPerObservation: 2 },
    { sourceId: 'beta', observationCount: '1', qualityWarningCount: '1', warningCodesPerObservation: 1 },
  ], meta: { returned: 2, limit: 100 } };
}
