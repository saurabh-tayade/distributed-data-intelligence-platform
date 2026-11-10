import { StrictMode } from 'react';
import { act, fireEvent, render, screen } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { parseRunDetail, sendCancelCommand } from './managementApi';
import { CancelRunPanel } from './operations/CancelRunPanel';
import { canCancelRun, readCancelIntent, reviewCancelRun } from './operations/cancelIntent';
import { deferred } from './test/fixtures';
import { ATTEMPT_ID, CONTROL_ID, controlReceipt, detailResponse, recordEnvelope, runDetail } from './test/runInspectionFixtures';
const running = { ...runDetail(1, 'RUNNING', 'application-reliability'), attempts: { count: '1', currentAttemptId: ATTEMPT_ID } };
function parsed(record = running) { return parseRunDetail(recordEnvelope(record), record.runId, `"${record.rowVersion}"`); }
function review() { reviewCancelRun(parsed()); }
function receipt(state: 'REQUESTED' | 'APPLIED' | 'TOO_LATE', reason: string | null = state === 'REQUESTED' ? null : state === 'TOO_LATE' ? 'SUCCEEDED' : 'CANCELLED') {
  return new Response(JSON.stringify(recordEnvelope({ ...controlReceipt(state), type: 'CANCEL', reason })), { status: 202,
    headers: { 'Content-Type': 'application/json', ETag: '"12"', Location: `/api/management/v1/runs/${running.runId}/controls/${CONTROL_ID}` } });
}
function mount() { return render(<StrictMode><CancelRunPanel visible /></StrictMode>); }
beforeEach(() => { sessionStorage.clear(); });
describe('running Reliability cancellation safety', () => {
  it('retains the running review context and explains stop versus publication without sending', () => {
    vi.stubGlobal('fetch', vi.fn()); review(); mount();
    expect(readCancelIntent()).toMatchObject({ mode: 'running', phase: 'review' });
    expect(screen.getByRole('button', { name: 'Confirm running-run cancellation' })).toBeEnabled();
    expect(screen.getByText(/Stopping work does not roll back a publication/)).toBeVisible();
    expect(fetch).not.toHaveBeenCalled();
  });
  it.each([
    { ...running, pipeline: 'CONTENT_RSS_V1' as const, productId: 'content-intelligence' as const },
    { ...running, attempts: { count: '2', currentAttemptId: ATTEMPT_ID } },
    { ...running, attempts: { count: '1', currentAttemptId: null } },
    { ...running, state: 'CANCEL_REQUESTED' as const },
    { ...running, state: 'RECOVERY_REQUIRED' as const },
  ])('does not offer unsupported running control for %j', (record) => {
    // Eligibility does not confer backend authorization, even on an otherwise valid inspected record.
    expect(canCancelRun({ ...parsed(), data: record })).toBe(false);
  });
  it('keeps accepted stop intent distinct from cancelled outcome and retains recovery status', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValueOnce(receipt('REQUESTED')).mockImplementation(() => Promise.resolve(detailResponse(runDetail(1, 'RECOVERY_REQUIRED', 'application-reliability')))));
    review(); mount(); fireEvent.click(screen.getByRole('button', { name: 'Confirm running-run cancellation' }));
    await screen.findByText(/Stop request accepted; cancellation is not yet proven/);
    await screen.findByText(/Fresh run read: RECOVERY_REQUIRED/);
    expect(readCancelIntent()).toMatchObject({ mode: 'running', phase: 'confirmed', receipt: { data: { state: 'REQUESTED' } } });
    expect(screen.queryByText(/Cancellation was applied/)).not.toBeInTheDocument();
  });
  it('resolves the same exact request after lost acknowledgement as TOO_LATE without claiming rollback', async () => {
    const fetch = vi.fn().mockRejectedValueOnce(new TypeError('lost')).mockResolvedValueOnce(receipt('TOO_LATE')).mockImplementation(() => Promise.resolve(detailResponse(runDetail(1, 'SUCCEEDED', 'application-reliability'))));
    vi.stubGlobal('fetch', fetch); review(); const original = readCancelIntent()!.command; const view = mount();
    fireEvent.click(screen.getByRole('button', { name: 'Confirm running-run cancellation' })); await screen.findByText(/Outcome unknown/);
    view.unmount(); mount(); expect(readCancelIntent()!.mode).toBe('running'); expect(fetch).toHaveBeenCalledTimes(1);
    fireEvent.click(screen.getByRole('button', { name: 'Resolve same cancellation request' }));
    await screen.findByText(/Cancellation was too late: verified publication succeeded/);
    expect(readCancelIntent()!.command).toEqual(original);
    expect(fetch.mock.calls[1]![1]).toMatchObject({ body: original.body, headers: { 'Idempotency-Key': original.key, 'If-Match': original.ifMatch } });
  });
  it('does not accept a late running receipt after unmount', async () => {
    const late = deferred<Response>(); vi.stubGlobal('fetch', vi.fn().mockReturnValue(late.promise)); review(); const view = mount();
    fireEvent.click(screen.getByRole('button', { name: 'Confirm running-run cancellation' })); view.unmount();
    await act(async () => late.resolve(receipt('REQUESTED')));
    expect(readCancelIntent()).toMatchObject({ mode: 'running', phase: 'uncertain' });
  });
  it.each([['REQUESTED', 'CANCELLED'], ['TOO_LATE', 'RECOVERED_PUBLICATION'], ['APPLIED', null]] as const)('rejects contradictory %s/%s receipt', async (state, reason) => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(receipt(state, reason))); review();
    await expect(sendCancelCommand(readCancelIntent()!.command, new AbortController().signal)).rejects.toMatchObject({ ambiguous: true });
  });
});
