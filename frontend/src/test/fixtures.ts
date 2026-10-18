import type { PublishersResponse } from '../api';

export const FINGERPRINT = 'a'.repeat(64);
export const MICROSECOND_TIME = '2026-10-12T23:59:59.123456Z';

export function fixture(): PublishersResponse {
  return {
    publication: FINGERPRINT,
    data: [
      { sourceId: 'alpha', articleCount: '3', observationCount: '7', articlesWithoutPublishedAt: '1',
        firstObservedAt: MICROSECOND_TIME, lastObservedAt: '2026-10-13T00:00:00.000001Z' },
      { sourceId: 'beta', articleCount: '5', observationCount: '8', articlesWithoutPublishedAt: '0',
        firstObservedAt: null, lastObservedAt: null },
    ],
    meta: { returned: 2, limit: 100 },
  };
}

export const jsonResponse = (body: unknown, status = 200) => new Response(JSON.stringify(body), {
  status, headers: { 'Content-Type': 'application/json' },
});

export function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((yes, no) => { resolve = yes; reject = no; });
  return { promise, resolve, reject };
}
