import { act, cleanup, render, screen, within } from '@testing-library/react';
import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import { App } from './App';
import { deferred, fixture, FINGERPRINT, jsonResponse } from './test/fixtures';
import { managementFailure } from './test/managementFixtures';
import { attemptsResponse, controlResponse, detailResponse, eventsResponse, latestControl, runDetail } from './test/runInspectionFixtures';

const id = runDetail().runId;
const record = () => within(screen.getByRole('region', { name: 'Run record' }));
const advance = async (ms = 0) => { await act(async () => { await vi.advanceTimersByTimeAsync(ms); }); };
beforeEach(() => {
  vi.useFakeTimers(); vi.setSystemTime(new Date('2026-11-01T12:00:00Z')); vi.spyOn(document, 'visibilityState', 'get').mockReturnValue('visible');
  window.history.replaceState(null, '', '/#/operations/executions/' + id);
});
afterEach(() => { cleanup(); vi.useRealTimers(); vi.restoreAllMocks(); });
function histories(path: string): Response | null {
  if (path.includes('/attempts?')) return attemptsResponse(); if (path.includes('/events?')) return eventsResponse();
  if (path.includes('/controls/')) return controlResponse(); return null;
}

it('polls only the run record, retains independent pages/control receipt and stops at terminal', async () => {
  let reads = 0; const first = runDetail(); first.latestControl = latestControl();
  const fetch = vi.fn().mockImplementation(async (path: string) => histories(path) ?? detailResponse(++reads === 1 ? first : { ...runDetail(1, 'SUCCEEDED'), latestControl: first.latestControl }));
  vi.stubGlobal('fetch', fetch); render(<App />); await advance(); expect(record().getByText('QUEUED')).toBeVisible();
  await advance(5000); expect(record().getByText('SUCCEEDED', { selector: 'dd' })).toBeVisible();
  await advance(120000);
  expect(fetch.mock.calls.filter((call) => String(call[0]).endsWith('/' + id))).toHaveLength(2);
  for (const kind of ['attempts?', 'events?', 'controls/']) expect(fetch.mock.calls.filter((call) => String(call[0]).includes(kind))).toHaveLength(1);
  expect(fetch.mock.calls.every((call) => (call[1] as RequestInit).method === 'GET')).toBe(true);
});

it('shows stale active data and timestamp during a backed-off poll failure, then recovers without merging histories', async () => {
  let reads = 0;
  const fetch = vi.fn().mockImplementation(async (path: string) => histories(path) ?? (++reads === 1 ? detailResponse()
    : reads === 2 ? jsonResponse(managementFailure(), 503) : detailResponse(runDetail(1, 'FAILED'))));
  vi.stubGlobal('fetch', fetch); render(<App />); await advance();
  const last = record().getByText(/Last successful read/).querySelector('time')?.getAttribute('datetime');
  await advance(5000); expect(record().getByRole('alert')).toHaveTextContent('MANAGEMENT_UNAVAILABLE');
  expect(record().getByText(/Stale run record/)).toBeVisible(); expect(record().getByText(/Last successful read/).querySelector('time')).toHaveAttribute('datetime', last);
  expect(record().getByText(/Automatic read retry scheduled 10 seconds/)).toBeVisible();
  await advance(10000); expect(record().queryByRole('alert')).not.toBeInTheDocument(); expect(record().queryByText(/Stale run record/)).not.toBeInTheDocument();
  expect(record().getByText('FAILED', { selector: 'dd' })).toBeVisible();
  expect(fetch.mock.calls.filter((call) => String(call[0]).includes('/events?'))).toHaveLength(1);
});

it('aborts a late poll after navigation to Analytics without affecting the Analytics publication session', async () => {
  let reads = 0; const late = deferred<Response>();
  const fetch = vi.fn().mockImplementation(async (path: string) => !path.startsWith('/api/management/') ? jsonResponse(fixture())
    : histories(path) ?? (++reads === 1 ? detailResponse() : late.promise));
  vi.stubGlobal('fetch', fetch); render(<App />); await advance(); await advance(5000);
  const pending = fetch.mock.calls.filter((call) => String(call[0]).endsWith('/' + id)).at(-1);
  act(() => { window.location.hash = '#/analytics'; }); await advance();
  expect((pending?.[1] as RequestInit).signal?.aborted).toBe(true);
  await act(async () => late.resolve(detailResponse(runDetail(1, 'SUCCEEDED')))); await advance(120000);
  expect(screen.queryByRole('region', { name: 'Run record' })).not.toBeInTheDocument();
  expect(fetch.mock.calls.filter((call) => String(call[0]).endsWith('/' + id))).toHaveLength(2);
  expect(screen.getByLabelText('Publication fingerprint')).toHaveTextContent(FINGERPRINT);
});
