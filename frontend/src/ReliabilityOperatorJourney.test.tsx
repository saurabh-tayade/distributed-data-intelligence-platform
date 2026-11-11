import { act, render, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { App } from './App';
import type { RunState } from './managementApi';
import { deferred, fixture, FINGERPRINT, jsonResponse } from './test/fixtures';
import { contentFixture } from './test/contentFixtures';
import { managementFailure, revision, revisionPage, sourcePage, sourceResponse } from './test/managementFixtures';
import { runPage } from './test/runFixtures';
import { attempt, attemptsResponse, detailResponse, eventsResponse, stagesResponse } from './test/runInspectionFixtures';
import { reliabilityFixture, reliabilityQuery } from './test/reliabilityFixtures';
import { batch, BATCH_ID, historyPage, profile, PROFILE_ID, reference, REFERENCE_ID, request, selectedSource, submittedRun, submissionResponse } from './test/submissionFixtures';
import { readRunIntent } from './operations/runIntent';

const run = submittedRun('SUCCEEDED');
const analyticsPublication = 'f'.repeat(64);

function service() {
  const fetch = vi.fn().mockImplementation(async (path: string, init: RequestInit) => {
    if (path === '/api/management/v1/runs' && init.method === 'POST') return submissionResponse();
    if (path.includes('/execution-profiles?')) return jsonResponse(historyPage([profile()]));
    if (path.includes('/sources?')) {
      const { config: _config, ...summary } = selectedSource(); void _config;
      return jsonResponse(sourcePage([summary]));
    }
    if (path.includes('/revisions?')) return jsonResponse(revisionPage([revision(2, selectedSource())]));
    if (path.includes('/sources/application-reliability/alpha')) return sourceResponse(selectedSource());
    if (path.includes('/artifacts?')) return path.includes('role=RELIABILITY_BATCH') ? jsonResponse(historyPage([batch()])) : jsonResponse(historyPage([reference()]));
    if (path.includes('/stages?')) return stagesResponse();
    if (path.includes('/attempts?')) return attemptsResponse([{ ...attempt(), state: 'SUCCEEDED', outcomeCode: 'SUCCEEDED' }]);
    if (path.includes('/events?')) return eventsResponse();
    if (path.includes('/runs?')) return jsonResponse(runPage([run]));
    if (path.includes('/runs/')) return detailResponse(run);
    if (path.includes('/reliability/request-windows?')) {
      return jsonResponse({ ...reliabilityFixture(), publication: analyticsPublication });
    }
    if (path.includes('/content/catalog?')) return jsonResponse(contentFixture());
    return jsonResponse(fixture());
  });
  vi.stubGlobal('fetch', fetch);
  return fetch;
}

async function chooseInputs(user: ReturnType<typeof userEvent.setup>) {
  await user.click(await screen.findByRole('radio', { name: new RegExp(PROFILE_ID) }));
  await user.click(await screen.findByRole('checkbox', { name: /Source alpha/ }));
  await screen.findByRole('button', { name: 'Remove source alpha' });
  await user.click(await screen.findByRole('checkbox', { name: new RegExp(BATCH_ID) }));
  await user.click(await screen.findByRole('radio', { name: new RegExp(REFERENCE_ID) }));
}

beforeEach(() => {
  sessionStorage.clear();
  window.history.replaceState(null, '', '/#/operations/sources/application-reliability/alpha');
});

describe('R1.1 Reliability operator journey', () => {
  it('navigates from a reviewed source through frozen confirmation and historical result to independent analytics', async () => {
    const fetch = service(), user = userEvent.setup(); render(<App />);
    await screen.findByRole('heading', { name: 'Source alpha' });
    expect(screen.getByText(/Inputs are not preselected from this page/)).toBeVisible();
    await user.click(screen.getByRole('link', { name: 'Choose Reliability inputs' }));
    expect(screen.getByRole('complementary', { name: 'Reliability prerequisites' })).toHaveTextContent(/import synthetic batches and a service reference/);
    await screen.findByRole('radio', { name: new RegExp(PROFILE_ID) });
    expect(screen.getByRole('radio', { name: new RegExp(PROFILE_ID) })).not.toBeChecked();
    expect(readRunIntent()).toBeNull();
    await chooseInputs(user);
    await user.click(screen.getByRole('button', { name: 'Review Reliability submission' }));
    const heading = await screen.findByRole('heading', { name: 'Review Reliability run' });
    expect(heading).toHaveFocus();
    const summary = screen.getByRole('region', { name: 'Frozen selection summary' });
    expect(summary).toHaveTextContent(PROFILE_ID);
    expect(summary).toHaveTextContent('alpha · revision 2');
    expect(summary).toHaveTextContent(REFERENCE_ID);
    expect(within(summary).getByText('1', { selector: 'dd' })).toBeVisible();
    await user.click(within(summary).getByText('Selected batch artifact IDs'));
    expect(within(summary).getByText(BATCH_ID)).toBeVisible();
    await user.click(screen.getByText('Complete frozen submission'));
    expect(screen.getByText('Complete frozen submission').nextElementSibling).toHaveTextContent(JSON.stringify(request(), null, 2).replace(/\s+/g, ' '));
    const original = readRunIntent()!.command;
    expect(JSON.parse(original.body)).toEqual(request());
    expect(fetch.mock.calls.every(([, init]) => init.method === 'GET')).toBe(true);
    await user.click(screen.getByRole('button', { name: 'Confirm Reliability submission' }));
    await user.click(await screen.findByRole('link', { name: 'Open admitted run' }));
    await screen.findByRole('heading', { name: 'Run completed' });
    const record = within(screen.getByRole('region', { name: 'Run record' }));
    expect(record.getByText('VERIFIED_AT_EXECUTION')).toBeVisible();
    await user.click(record.getByText('Recorded result metadata'));
    expect(record.getByText(run.result!.nativeFingerprint)).toBeVisible();
    expect(record.getByText('Historical verification only')).toBeVisible();
    await user.click(await screen.findByRole('button', { name: /Inspect stages for attempt 1/ }));
    expect(await within(screen.getByRole('region', { name: 'Stage Records' })).findByText('CHECK_INPUTS')).toBeVisible();
    expect(screen.getByRole('region', { name: 'Audit events' })).toBeInTheDocument();
    const handoff = screen.getByRole('complementary', { name: 'Reliability analytics handoff' });
    expect(handoff).toHaveTextContent(/A successful run does not select its result in Analytics/);
    await user.click(within(handoff).getByRole('link', { name: 'Open independently selected Reliability analytics' }));
    const query = reliabilityQuery();
    await user.type(screen.getByRole('textbox', { name: 'Start inclusive (UTC)' }), query.start);
    await user.type(screen.getByRole('textbox', { name: 'End exclusive (UTC)' }), query.end);
    await user.click(screen.getByRole('button', { name: 'Apply window filters' }));
    expect(await screen.findByLabelText('Reliability publication fingerprint')).toHaveTextContent(analyticsPublication);
    expect(screen.getByText(/Completing a managed run does not activate its result here/)).toBeVisible();
    expect(readRunIntent()!.command).toEqual(original);
    const posts = fetch.mock.calls.filter(([, init]) => ['POST', 'PUT', 'PATCH', 'DELETE'].includes(init.method ?? 'GET'));
    expect(posts).toHaveLength(1);
    expect(posts[0]![1].body).toBe(original.body);
    expect(new Headers(posts[0]![1].headers).get('Idempotency-Key')).toBe(original.key);
    expect(fetch.mock.calls.filter(([path]) => path.includes('/reliability/request-windows?')).every(([path]) => !path.includes('publication'))).toBe(true);
  });

  it.each([['QUEUED', 'Waiting for execution'], ['RUNNING', 'Execution in progress'], ['CANCEL_REQUESTED', 'Cancellation requested'],
    ['SUCCEEDED', 'Run completed'], ['FAILED', 'Run failed'], ['CANCELLED', 'Cancellation recorded'], ['PAUSED', 'Paused at an operator boundary'],
    ['RECOVERY_REQUIRED', 'Operator recovery required']] as const)('explains %s without issuing a control request', async (state: RunState, title) => {
    const fetch = service(), base = fetch.getMockImplementation()!;
    fetch.mockImplementation((path: string, init: RequestInit) => path === `/api/management/v1/runs/${run.runId}`
      ? Promise.resolve(detailResponse(submittedRun(state))) : base(path, init));
    window.history.replaceState(null, '', `/#/operations/executions/${run.runId}`);
    render(<App />);
    expect(await screen.findByRole('heading', { name: title })).toBeVisible();
    if (state === 'CANCEL_REQUESTED') expect(screen.getByText(/cancellation is not yet confirmed/)).toBeVisible();
    if (state !== 'SUCCEEDED') expect(screen.getByRole('region', { name: 'Run record' })).toHaveTextContent('No result recorded');
    expect(fetch.mock.calls.every(([, init]) => init.method === 'GET')).toBe(true);
  });

  it('keeps a failed refresh visibly historical and disables stale controls', async () => {
    const fetch = service(), base = fetch.getMockImplementation()!; let fail = false;
    fetch.mockImplementation((path: string, init: RequestInit) => fail && path === `/api/management/v1/runs/${run.runId}`
      ? Promise.resolve(jsonResponse(managementFailure(), 503)) : base(path, init));
    window.history.replaceState(null, '', `/#/operations/executions/${run.runId}`);
    const user = userEvent.setup(); render(<App />); await screen.findByRole('heading', { name: 'Run completed' });
    fail = true; await user.click(screen.getByRole('button', { name: 'Refresh run detail' }));
    expect(await screen.findByRole('heading', { name: 'Last recorded state: Run completed' })).toBeVisible();
    const record = screen.getByRole('region', { name: 'Run record' });
    expect(record).toHaveTextContent('Stale run record');
    expect(within(record).getByRole('alert')).not.toHaveTextContent('/private/secret');
    expect(screen.getByRole('button', { name: 'Review queued-run cancellation' })).toBeDisabled();
    expect(screen.getByRole('button', { name: 'Review guarded retry' })).toBeDisabled();
    expect(fetch.mock.calls.every(([, init]) => init.method === 'GET')).toBe(true);
  });

  it('does not show run guidance or a result when the first read is pending or fails', async () => {
    const fetch = service(), base = fetch.getMockImplementation()!, pending = deferred<Response>();
    fetch.mockImplementation((path: string, init: RequestInit) => path === `/api/management/v1/runs/${run.runId}` ? pending.promise : base(path, init));
    window.history.replaceState(null, '', `/#/operations/executions/${run.runId}`);
    render(<App />);
    expect(screen.getByText('Loading run detail')).toBeVisible();
    expect(screen.queryByRole('heading', { name: 'Run completed' })).not.toBeInTheDocument();
    expect(screen.queryByRole('complementary', { name: 'Reliability analytics handoff' })).not.toBeInTheDocument();
    await act(async () => pending.resolve(jsonResponse(managementFailure('NOT_FOUND'), 404)));
    expect(await screen.findByRole('heading', { name: 'Unable to load run detail' })).toBeVisible();
    expect(within(screen.getByRole('region', { name: 'Run record' })).getByRole('alert')).toHaveTextContent('NOT_FOUND');
    expect(screen.queryByText('VERIFIED_AT_EXECUTION')).not.toBeInTheDocument();
    expect(fetch.mock.calls.every(([, init]) => init.method === 'GET')).toBe(true);
  });

  it('keeps disabled sources at the admission-management step without preparing a run', async () => {
    const fetch = service(), base = fetch.getMockImplementation()!;
    fetch.mockImplementation((path: string, init: RequestInit) => path === '/api/management/v1/sources/application-reliability/alpha'
      ? Promise.resolve(sourceResponse({ ...selectedSource(), enabled: false })) : base(path, init));
    render(<App />); await screen.findByRole('heading', { name: 'Source alpha' });
    expect(screen.getByText(/enable admission through Source management actions/)).toBeVisible();
    expect(screen.queryByRole('link', { name: 'Choose Reliability inputs' })).not.toBeInTheDocument();
    expect(readRunIntent()).toBeNull();
    await waitFor(() => expect(fetch.mock.calls.every(([, init]) => init.method === 'GET')).toBe(true));
  });

  it('retains separate Gold, Content and Reliability identities across the result handoff and Back/Forward', async () => {
    const fetch = service(), user = userEvent.setup();
    window.history.replaceState(null, '', '/#/analytics'); render(<App />);
    expect(await screen.findByLabelText('Publication fingerprint')).toHaveTextContent(FINGERPRINT);
    await user.click(screen.getByRole('link', { name: 'Enriched Content catalog' }));
    const selection = await screen.findByRole('region', { name: 'Content selection identities' });
    await waitFor(() => expect(selection).toHaveTextContent(contentFixture().publication));
    await act(async () => { window.location.hash = `#/operations/executions/${run.runId}`; });
    await screen.findByRole('heading', { name: 'Run completed' });
    await user.click(screen.getByRole('link', { name: 'Open independently selected Reliability analytics' }));
    const query = reliabilityQuery();
    await user.type(screen.getByRole('textbox', { name: 'Start inclusive (UTC)' }), query.start);
    await user.type(screen.getByRole('textbox', { name: 'End exclusive (UTC)' }), query.end);
    await user.click(screen.getByRole('button', { name: 'Apply window filters' }));
    expect(await screen.findByLabelText('Reliability publication fingerprint')).toHaveTextContent(analyticsPublication);
    act(() => window.history.back());
    expect(await screen.findByRole('heading', { name: 'Run completed' })).toBeVisible();
    act(() => window.history.forward());
    await waitFor(() => expect(screen.getByLabelText('Reliability publication fingerprint')).toBeVisible());
    expect(screen.getByLabelText('Reliability publication fingerprint')).toHaveTextContent(analyticsPublication);
    await user.click(screen.getByRole('link', { name: 'Content Intelligence' }));
    expect(await screen.findByLabelText('Publication fingerprint')).toHaveTextContent(FINGERPRINT);
    await user.click(screen.getByRole('link', { name: 'Enriched Content catalog' }));
    expect(await screen.findByRole('region', { name: 'Content selection identities' })).toHaveTextContent(contentFixture().publication);
    expect(readRunIntent()).toBeNull();
    expect(fetch.mock.calls.every(([, init]) => [undefined, 'GET'].includes(init.method))).toBe(true);
  });
});
