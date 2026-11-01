import { act, renderHook, waitFor } from '@testing-library/react';
import { expect, it, vi } from 'vitest';
import { jsonResponse } from '../test/fixtures';
import { run, runPage } from '../test/runFixtures';
import { RUN_CURSOR_HISTORY_LIMIT, useRuns } from './useRuns';

it('bounds cursor memory, permits continued forward traversal and honestly limits Previous history', async () => {
  const token = (page: number) => `cGFnZQ${page}.${'s'.repeat(43)}`;
  const fetch = vi.fn().mockImplementation(async (path: string) => {
    const cursor = new URL(path, 'http://localhost').searchParams.get('cursor');
    const page = cursor ? Number(cursor.match(/^cGFnZQ(\d+)/)![1]) : 1;
    return jsonResponse(runPage(Array.from({ length: 20 }, (_, i) => run(page * 100 + i)), 20, token(page + 1)));
  });
  vi.stubGlobal('fetch', fetch); const query = {}, view = renderHook(() => useRuns(query));
  await waitFor(() => expect(view.result.current.current?.status).toBe('ready'));
  for (let page = 2; page <= RUN_CURSOR_HISTORY_LIMIT + 3; page++) {
    act(() => { view.result.current.next(); view.result.current.next(); }); // Duplicate clicks cannot advance twice.
    await waitFor(() => expect(view.result.current.current?.status).toBe('ready'));
    expect(view.result.current.pageNumber).toBe(page);
  }
  expect(view.result.current.historyTruncated).toBe(true);
  for (let previous = 0; previous < RUN_CURSOR_HISTORY_LIMIT - 1; previous++) {
    act(() => view.result.current.back()); await waitFor(() => expect(view.result.current.current?.status).toBe('ready'));
  }
  expect(view.result.current.canGoBack).toBe(false); expect(view.result.current.pageNumber).toBe(4);
  const calls = fetch.mock.calls.length; act(() => view.result.current.back()); expect(fetch).toHaveBeenCalledTimes(calls);
  act(() => view.result.current.refresh(true)); await waitFor(() => expect(view.result.current.current?.status).toBe('ready'));
  expect(view.result.current.pageNumber).toBe(1); expect(view.result.current.historyTruncated).toBe(false);
  expect(fetch.mock.calls.at(-1)?.[0]).toBe('/api/management/v1/runs?limit=20');
});
