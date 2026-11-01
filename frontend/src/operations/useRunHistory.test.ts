import { act, renderHook, waitFor } from '@testing-library/react';
import { expect, it, vi } from 'vitest';
import { fetchEvents } from '../managementApi';
import type { AuditEvent } from '../managementApi';
import { deferred, jsonResponse } from '../test/fixtures';
import { FIRST_CURSOR } from '../test/managementFixtures';
import { auditEvent, historyPage, runDetail } from '../test/runInspectionFixtures';
import { useRunHistory } from './useRunHistory';

const sequence = (event: AuditEvent) => BigInt(event.eventSeq);
const id = runDetail().runId;
it('bounds event cursors at 20 positions without merging rows or losing large sequence precision', async () => {
  const token = (page: number) => `cGFnZQ${page}.${'s'.repeat(43)}`;
  const fetch = vi.fn().mockImplementation(async (path: string) => {
    const query = new URL(path, 'http://localhost').searchParams, cursor = query.get('cursor');
    const page = cursor ? Number(cursor.match(/^cGFnZQ(\d+)/)![1]) : 1;
    return jsonResponse(historyPage(Array.from({ length: 20 }, (_, i) => auditEvent((9007199254740993n + BigInt(page * 20 + i)).toString())), 20, token(page + 1)));
  });
  vi.stubGlobal('fetch', fetch); const view = renderHook(() => useRunHistory(id, fetchEvents, sequence));
  await waitFor(() => expect(view.result.current.current?.status).toBe('ready'));
  for (let page = 2; page <= 23; page++) {
    act(() => { view.result.current.next(); view.result.current.next(); });
    await waitFor(() => expect(view.result.current.current?.status).toBe('ready'));
    expect(view.result.current.pageNumber).toBe(page);
    const current = view.result.current.current; if (current?.status === 'ready') expect(current.page.data).toHaveLength(20);
  }
  expect(view.result.current.historyTruncated).toBe(true);
  for (let page = 0; page < 19; page++) {
    act(() => view.result.current.back()); await waitFor(() => expect(view.result.current.current?.status).toBe('ready'));
  }
  expect(view.result.current.canGoBack).toBe(false); expect(view.result.current.pageNumber).toBe(4);
  act(() => view.result.current.refresh()); await waitFor(() => expect(view.result.current.current?.status).toBe('ready'));
  expect(view.result.current.pageNumber).toBe(1); expect(view.result.current.historyTruncated).toBe(false);
});

it.each(['regressing sequence', 'cursor cycle'])('rejects cross-page %s independently', async (kind) => {
  const first = Array.from({ length: 20 }, (_, i) => auditEvent((9007199254740993n + BigInt(i)).toString()));
  const fetch = vi.fn().mockResolvedValueOnce(jsonResponse(historyPage(first, 20, FIRST_CURSOR)))
    .mockResolvedValueOnce(jsonResponse(historyPage([auditEvent('9007199254740994')])));
  if (kind === 'cursor cycle') fetch.mockReset().mockResolvedValueOnce(jsonResponse(historyPage(first, 20, FIRST_CURSOR)))
    .mockResolvedValueOnce(jsonResponse(historyPage(first, 20, FIRST_CURSOR)));
  vi.stubGlobal('fetch', fetch); const view = renderHook(() => useRunHistory(id, fetchEvents, sequence));
  await waitFor(() => expect(view.result.current.current?.status).toBe('ready')); act(() => view.result.current.next());
  await waitFor(() => expect(view.result.current.current?.status).toBe('error'));
  expect(view.result.current.current).toMatchObject({ error: { kind: 'invalid-response' } });
});

it('cancels a pending history page on unmount and never publishes late transport failure', async () => {
  const late = deferred<Response>(), fetch = vi.fn().mockResolvedValueOnce(jsonResponse(historyPage(Array.from({ length: 20 }, (_, i) => auditEvent(String(i + 1))), 20, FIRST_CURSOR))).mockReturnValueOnce(late.promise);
  vi.stubGlobal('fetch', fetch); const view = renderHook(() => useRunHistory(id, fetchEvents, sequence));
  await waitFor(() => expect(view.result.current.current?.status).toBe('ready')); act(() => view.result.current.next());
  view.unmount(); expect((fetch.mock.calls[1]?.[1] as RequestInit).signal?.aborted).toBe(true);
  await act(async () => late.reject(new DOMException('aborted', 'AbortError'))); expect(fetch).toHaveBeenCalledTimes(2);
});
