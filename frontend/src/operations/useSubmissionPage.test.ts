import { act, renderHook, waitFor } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';
import { ManagementError } from '../managementApi';
import { deferred } from '../test/fixtures';
import { FIRST_CURSOR, SECOND_CURSOR } from '../test/managementFixtures';
import { historyPage, profile } from '../test/submissionFixtures';
import { useSubmissionPage } from './useSubmissionPage';

describe('submission discovery request lifecycle', () => {
  it('fetches one page, follows only opaque cursors and refreshes from the first page', async () => {
    const load = vi.fn().mockResolvedValueOnce(historyPage([profile()], 20, FIRST_CURSOR)).mockResolvedValue(historyPage([profile()]));
    const { result } = renderHook(() => useSubmissionPage('profiles', true, load));
    await waitFor(() => expect(result.current.current?.status).toBe('ready')); expect(load).toHaveBeenCalledTimes(1);
    act(() => result.current.next()); await waitFor(() => expect(load).toHaveBeenCalledTimes(2));
    expect(load.mock.calls[1]![0]).toEqual({ limit: 20, cursor: FIRST_CURSOR });
    await waitFor(() => expect(result.current.current?.status).toBe('ready'));
    act(() => result.current.back()); await waitFor(() => expect(load).toHaveBeenCalledTimes(3)); expect(load.mock.calls[2]![0]).toEqual({ limit: 20 });
    act(() => result.current.refresh()); await waitFor(() => expect(load).toHaveBeenCalledTimes(4)); expect(load.mock.calls[3]![0]).toEqual({ limit: 20 });
  });
  it('invalidates delayed responses on filter/profile changes and cancels on unmount', async () => {
    const late = deferred<ReturnType<typeof historyPage<ReturnType<typeof profile>>>>();
    const load = vi.fn().mockReturnValueOnce(late.promise).mockResolvedValue(historyPage([profile('80000000-0000-4000-8000-000000000002')]));
    const { result, rerender, unmount } = renderHook(({ scope }) => useSubmissionPage(scope, true, load), { initialProps: { scope: 'old' } });
    const oldSignal = load.mock.calls[0]![1] as AbortSignal;
    rerender({ scope: 'new' }); expect(oldSignal.aborted).toBe(true);
    await waitFor(() => expect(result.current.current?.status).toBe('ready'));
    await act(async () => late.resolve(historyPage([profile()])));
    expect(result.current.current).toMatchObject({ status: 'ready', page: { data: [profile('80000000-0000-4000-8000-000000000002')] } });
    const signal = load.mock.calls.at(-1)![1] as AbortSignal; unmount(); expect(signal.aborted).toBe(true);
  });
  it('handles rapid scope changes without merging pages or reusing previous cursors', async () => {
    const late = deferred<ReturnType<typeof historyPage<ReturnType<typeof profile>>>>();
    const load = vi.fn().mockResolvedValueOnce(historyPage([profile()], 20, FIRST_CURSOR)).mockReturnValueOnce(late.promise).mockResolvedValue(historyPage([]));
    const { result, rerender } = renderHook(({ scope }) => useSubmissionPage(scope, true, load), { initialProps: { scope: 'a' } });
    await waitFor(() => expect(result.current.current?.status).toBe('ready'));
    act(() => result.current.next()); rerender({ scope: 'b' }); rerender({ scope: 'c' });
    await waitFor(() => expect(result.current.current).toMatchObject({ status: 'ready', page: { data: [] } }));
    expect(load.mock.calls.at(-1)![0]).toEqual({ limit: 20 });
    await act(async () => late.resolve(historyPage([profile()])));
    expect(result.current.current).toMatchObject({ status: 'ready', page: { data: [] } });
  });
  it('does not fetch hidden catalogs and starts fresh after reactivation', async () => {
    const load = vi.fn().mockResolvedValue(historyPage([profile()]));
    const { result, rerender } = renderHook(({ active }) => useSubmissionPage('profiles', active, load), { initialProps: { active: false } });
    expect(load).not.toHaveBeenCalled(); rerender({ active: true }); await waitFor(() => expect(result.current.current?.status).toBe('ready'));
    rerender({ active: false }); expect(result.current.current).toBeNull(); expect((load.mock.calls[0]![1] as AbortSignal).aborted).toBe(true);
    load.mockReturnValueOnce(new Promise(() => {})); rerender({ active: true }); expect(result.current.current).toBeNull(); expect(load).toHaveBeenCalledTimes(2);
  });
  it('keeps errors distinct and never retains stale data after a failed refresh', async () => {
    const load = vi.fn().mockResolvedValueOnce(historyPage([profile()])).mockRejectedValue(new ManagementError('invalid-request', 'INVALID_ARGUMENT'));
    const { result } = renderHook(() => useSubmissionPage('profiles', true, load)); await waitFor(() => expect(result.current.current?.status).toBe('ready'));
    act(() => result.current.refresh()); await waitFor(() => expect(result.current.current).toMatchObject({ status: 'error', error: { kind: 'invalid-request' } }));
    expect(result.current.current).not.toHaveProperty('page'); expect(load).toHaveBeenCalledTimes(2);
  });
  it('retries the same failed cursor only on request and distinguishes an explicit restart', async () => {
    const load = vi.fn().mockResolvedValueOnce(historyPage([profile()], 20, FIRST_CURSOR)).mockRejectedValue(new ManagementError('invalid-request'));
    const { result } = renderHook(() => useSubmissionPage('profiles', true, load)); await waitFor(() => expect(result.current.current?.status).toBe('ready'));
    act(() => result.current.next()); await waitFor(() => expect(result.current.current?.status).toBe('error'));
    expect(load).toHaveBeenCalledTimes(2); act(() => result.current.retry()); await waitFor(() => expect(load).toHaveBeenCalledTimes(3));
    expect(load.mock.calls[2]![0]).toEqual({ limit: 20, cursor: FIRST_CURSOR });
    act(() => result.current.refresh()); await waitFor(() => expect(load).toHaveBeenCalledTimes(4)); expect(load.mock.calls[3]![0]).toEqual({ limit: 20 });
  });
  it('rejects cursor cycles and bounds previous-page history to 20 positions', async () => {
    const load = vi.fn().mockResolvedValueOnce(historyPage([profile()], 20, FIRST_CURSOR)).mockResolvedValueOnce(historyPage([profile()], 20, FIRST_CURSOR));
    const { result } = renderHook(() => useSubmissionPage('cycle', true, load)); await waitFor(() => expect(result.current.current?.status).toBe('ready'));
    act(() => result.current.next()); await waitFor(() => expect(result.current.current).toMatchObject({ status: 'error', error: { kind: 'invalid-response' } }));
    load.mockReset().mockImplementation(async (query) => historyPage([profile()], 20, query.cursor === SECOND_CURSOR ? FIRST_CURSOR : SECOND_CURSOR));
    act(() => result.current.refresh()); await waitFor(() => expect(result.current.current?.status).toBe('ready'));
    // Distinct signed positions; the hook treats their content as opaque.
    let index = 0; load.mockImplementation(() => Promise.resolve(historyPage([profile()], 20, `position-${++index}`)));
    for (let i = 0; i < 22; i++) {
      act(() => result.current.next()); await waitFor(() => expect(result.current.current?.status).toBe('ready'));
    }
    expect(result.current.truncated).toBe(true);
    let previous = 0;
    while (result.current.canGoBack) { act(() => result.current.back()); await waitFor(() => expect(result.current.current?.status).toBe('ready')); previous++; }
    expect(previous).toBe(19);
  });
});
