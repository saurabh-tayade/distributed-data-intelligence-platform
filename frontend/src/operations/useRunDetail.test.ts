import { act, cleanup, renderHook } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { deferred, jsonResponse } from '../test/fixtures';
import { managementFailure } from '../test/managementFixtures';
import { detailResponse, runDetail } from '../test/runInspectionFixtures';
import { ACTIVE_POLL_MS, MAX_POLL_BACKOFF_MS, useRunDetail } from './useRunDetail';

const id = runDetail().runId;
beforeEach(() => {
  vi.useFakeTimers(); vi.setSystemTime(new Date('2026-11-01T12:00:00Z'));
  vi.spyOn(document, 'visibilityState', 'get').mockReturnValue('visible');
});
afterEach(() => { cleanup(); vi.useRealTimers(); vi.restoreAllMocks(); });
const advance = async (ms = 0) => { await act(async () => { await vi.advanceTimersByTimeAsync(ms); }); };
function visibility(value: DocumentVisibilityState) {
  act(() => { vi.spyOn(document, 'visibilityState', 'get').mockReturnValue(value); document.dispatchEvent(new Event('visibilitychange')); });
}

describe('settlement-driven visible run polling', () => {
  it('polls after five seconds, never overlaps or queues repeated manual reads, and stops at terminal', async () => {
    const pending = deferred<Response>(), fetch = vi.fn().mockResolvedValueOnce(detailResponse()).mockReturnValueOnce(pending.promise)
      .mockResolvedValueOnce(detailResponse(runDetail(1, 'SUCCEEDED')));
    vi.stubGlobal('fetch', fetch); const view = renderHook(() => useRunDetail(id)); await advance();
    expect(view.result.current.data?.etag).toBe('"9007199254740993"'); expect(view.result.current.nextPollMs).toBe(ACTIVE_POLL_MS);
    await advance(4999); expect(fetch).toHaveBeenCalledTimes(1); await advance(1); expect(fetch).toHaveBeenCalledTimes(2);
    act(() => { view.result.current.refresh(); view.result.current.refresh(); }); await advance(60000); expect(fetch).toHaveBeenCalledTimes(2);
    await act(async () => pending.resolve(detailResponse(runDetail(1, 'RUNNING')))); await advance();
    await advance(4999); expect(fetch).toHaveBeenCalledTimes(2); await advance(1); expect(fetch).toHaveBeenCalledTimes(3);
    expect(view.result.current.data?.data.state).toBe('SUCCEEDED'); expect(view.result.current.nextPollMs).toBeNull();
    await advance(120000); expect(fetch).toHaveBeenCalledTimes(3);
    expect(fetch.mock.calls.every((call) => (call[1] as RequestInit).method === 'GET')).toBe(true);
  });

  it.each(['QUEUED', 'RUNNING', 'CANCEL_REQUESTED'] as const)('polls active %s only after settlement', async (state) => {
    const fetch = vi.fn().mockImplementation(async () => detailResponse(runDetail(1, state))); vi.stubGlobal('fetch', fetch);
    renderHook(() => useRunDetail(id)); await advance(); await advance(5000); expect(fetch).toHaveBeenCalledTimes(2);
  });

  it.each(['SUCCEEDED', 'FAILED', 'CANCELLED', 'PAUSED', 'RECOVERY_REQUIRED'] as const)('keeps %s manual-only, including visibility changes', async (state) => {
    const fetch = vi.fn().mockImplementation(async () => detailResponse(runDetail(1, state))); vi.stubGlobal('fetch', fetch);
    const view = renderHook(() => useRunDetail(id)); await advance(); await advance(120000); expect(fetch).toHaveBeenCalledTimes(1);
    visibility('hidden'); await advance(60000); visibility('visible'); await advance(); expect(fetch).toHaveBeenCalledTimes(1);
    act(() => view.result.current.refresh()); await advance(); expect(fetch).toHaveBeenCalledTimes(2);
    await advance(60000); expect(fetch).toHaveBeenCalledTimes(2);
  });

  it('suspends pending timers while hidden and rereads an active record when visible', async () => {
    const fetch = vi.fn().mockImplementation(async () => detailResponse()); vi.stubGlobal('fetch', fetch);
    const view = renderHook(() => useRunDetail(id)); await advance(); visibility('hidden'); await advance(120000);
    expect(view.result.current.hidden).toBe(true); expect(fetch).toHaveBeenCalledTimes(1);
    act(() => view.result.current.refresh()); expect(fetch).toHaveBeenCalledTimes(1);
    visibility('visible'); await advance(); expect(fetch).toHaveBeenCalledTimes(2);
    expect(view.result.current.nextPollMs).toBe(5000);
  });

  it('waits for an aborted hidden transport to settle before resuming, and ignores its late response', async () => {
    const late = deferred<Response>(), fetch = vi.fn().mockReturnValueOnce(late.promise).mockResolvedValueOnce(detailResponse(runDetail(1, 'FAILED')));
    vi.stubGlobal('fetch', fetch); const view = renderHook(() => useRunDetail(id));
    visibility('hidden'); expect((fetch.mock.calls[0]?.[1] as RequestInit).signal?.aborted).toBe(true);
    visibility('visible'); await advance(120000); expect(fetch).toHaveBeenCalledTimes(1);
    await act(async () => late.resolve(detailResponse({ ...runDetail(), rowVersion: '999' }))); await advance();
    expect(fetch).toHaveBeenCalledTimes(2); expect(view.result.current.data?.data.state).toBe('FAILED');
    expect(view.result.current.data?.data.rowVersion).toBe('9007199254740993');
  });

  it('does not fetch an initially hidden record until visible', async () => {
    visibility('hidden'); const fetch = vi.fn().mockImplementation(async () => detailResponse()); vi.stubGlobal('fetch', fetch);
    renderHook(() => useRunDetail(id)); await advance(60000); expect(fetch).not.toHaveBeenCalled();
    visibility('visible'); await advance(); expect(fetch).toHaveBeenCalledTimes(1);
  });

  it('releases a cancelled manual-only read after visibility returns without starting an automatic read', async () => {
    const late = deferred<Response>(), fetch = vi.fn().mockResolvedValueOnce(detailResponse(runDetail(1, 'PAUSED')))
      .mockReturnValueOnce(late.promise).mockResolvedValueOnce(detailResponse(runDetail(1, 'PAUSED')));
    vi.stubGlobal('fetch', fetch); const view = renderHook(() => useRunDetail(id)); await advance();
    act(() => view.result.current.refresh()); visibility('hidden'); visibility('visible');
    expect(view.result.current.refreshing).toBe(true); expect(fetch).toHaveBeenCalledTimes(2);
    await act(async () => late.resolve(detailResponse(runDetail(1, 'SUCCEEDED')))); await advance();
    expect(view.result.current.data?.data.state).toBe('PAUSED'); expect(view.result.current.refreshing).toBe(false);
    expect(fetch).toHaveBeenCalledTimes(2); act(() => view.result.current.refresh()); await advance(); expect(fetch).toHaveBeenCalledTimes(3);
  });

  it('retains an explicitly stale same-run snapshot/time and bounds transient backoff at 10/20/30 seconds', async () => {
    const fetch = vi.fn().mockResolvedValueOnce(detailResponse()).mockResolvedValueOnce(jsonResponse(managementFailure(), 503))
      .mockRejectedValueOnce(new Error('private')).mockResolvedValueOnce(jsonResponse(managementFailure(), 503)).mockResolvedValueOnce(detailResponse(runDetail(1, 'FAILED')));
    vi.stubGlobal('fetch', fetch); const view = renderHook(() => useRunDetail(id)); await advance(); const lastUpdated = view.result.current.lastUpdated;
    await advance(5000); expect(view.result.current.data?.data.runId).toBe(id); expect(view.result.current.error?.code).toBe('MANAGEMENT_UNAVAILABLE');
    expect(view.result.current.lastUpdated).toBe(lastUpdated); expect(view.result.current.nextPollMs).toBe(10000);
    await advance(9999); expect(fetch).toHaveBeenCalledTimes(2); await advance(1); expect(view.result.current.nextPollMs).toBe(20000);
    await advance(20000); expect(view.result.current.nextPollMs).toBe(MAX_POLL_BACKOFF_MS);
    await advance(29999); expect(fetch).toHaveBeenCalledTimes(4); await advance(1);
    expect(view.result.current.data?.data.state).toBe('FAILED'); expect(view.result.current.error).toBeNull(); expect(view.result.current.nextPollMs).toBeNull();
  });

  it('backs off an initial transient failure without a fabricated successful record', async () => {
    const fetch = vi.fn().mockResolvedValueOnce(jsonResponse(managementFailure(), 503)).mockResolvedValueOnce(detailResponse(runDetail(1, 'CANCELLED')));
    vi.stubGlobal('fetch', fetch); const view = renderHook(() => useRunDetail(id)); await advance();
    expect(view.result.current.data).toBeNull(); expect(view.result.current.lastUpdated).toBeNull();
    await advance(10000); expect(view.result.current.data?.data.state).toBe('CANCELLED');
  });

  it.each([403, 404, 200])('does not automatically retry permanent/malformed HTTP %s reads', async (status) => {
    const code = status === 403 ? 'LOCAL_BOUNDARY_REJECTED' : 'NOT_FOUND';
    const fetch = vi.fn().mockImplementation(async () => jsonResponse(managementFailure(code), status)); vi.stubGlobal('fetch', fetch);
    const view = renderHook(() => useRunDetail(id)); await advance(); expect(view.result.current.error).not.toBeNull();
    await advance(120000); expect(fetch).toHaveBeenCalledTimes(1);
    visibility('hidden'); visibility('visible'); await advance(); expect(fetch).toHaveBeenCalledTimes(1);
    act(() => view.result.current.refresh()); await advance(); expect(fetch).toHaveBeenCalledTimes(2);
  });

  it('keeps a failed manual read of a terminal snapshot manual-only', async () => {
    const fetch = vi.fn().mockResolvedValueOnce(detailResponse(runDetail(1, 'SUCCEEDED'))).mockResolvedValueOnce(jsonResponse(managementFailure(), 503));
    vi.stubGlobal('fetch', fetch); const view = renderHook(() => useRunDetail(id)); await advance();
    act(() => view.result.current.refresh()); await advance(); expect(view.result.current.error).not.toBeNull();
    expect(view.result.current.data?.data.state).toBe('SUCCEEDED'); expect(view.result.current.nextPollMs).toBeNull();
    await advance(120000); expect(fetch).toHaveBeenCalledTimes(2);
  });

  it('cancels a late poll on navigation/unmount and schedules no further reads', async () => {
    const late = deferred<Response>(), fetch = vi.fn().mockResolvedValueOnce(detailResponse()).mockReturnValueOnce(late.promise);
    vi.stubGlobal('fetch', fetch); const view = renderHook(() => useRunDetail(id)); await advance(); await advance(5000);
    view.unmount(); expect((fetch.mock.calls[1]?.[1] as RequestInit).signal?.aborted).toBe(true);
    await act(async () => late.resolve(detailResponse())); await advance(120000); expect(fetch).toHaveBeenCalledTimes(2);
  });
});
