import { act, render, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { App } from './App';
import type { SourceRecord } from './managementApi';
import { parseWorkspaceRoute } from './navigation';
import { contentSourceMatches } from './operations/ContentSubmission';
import { readRunIntent, reviewRunSubmission } from './operations/runIntent';
import { contentProfile, contentReceipt, contentRequest, contentRun, contentSource, CONTENT_PROFILE, historyPage, publisherReference, PUBLISHER_REFERENCE } from './test/contentSubmissionFixtures';
import { deferred, fixture, jsonResponse } from './test/fixtures';
import { FIRST_CURSOR, managementFailure, revisionPage, sourceRecord, sourceResponse } from './test/managementFixtures';
import { attemptsResponse, detailResponse, eventsResponse } from './test/runInspectionFixtures';
import { request as reliabilityRequest } from './test/submissionFixtures';

function service() {
  let source = sourceRecord();
  const fetch = vi.fn().mockImplementation(async (path: string, init: RequestInit) => {
    if (init.method === 'POST') return contentReceipt();
    if (path.includes('/execution-profiles?')) return jsonResponse(historyPage([contentProfile(), contentProfile('81000000-0000-4000-8000-000000000002')]));
    if (path.includes('/execution-profile-sources?')) {
      const profileId = new URL(path, window.location.origin).searchParams.get('executionProfileId')!;
      return jsonResponse(historyPage([contentSource('alpha', profileId)]));
    }
    if (path.includes('/artifacts?')) return jsonResponse(historyPage([publisherReference()]));
    if (path.includes('/sources/content-intelligence/alpha/revisions?')) return jsonResponse(revisionPage());
    if (path.includes('/sources/content-intelligence/alpha')) return sourceResponse(source);
    if (path.includes('/attempts?')) return attemptsResponse([]);
    if (path.includes('/events?')) return eventsResponse([]);
    if (path.includes(`/runs/${contentRun().runId}`)) return detailResponse(contentRun());
    if (path.includes('/runs?') || path.includes('/sources?')) return jsonResponse(historyPage([]));
    return jsonResponse(fixture());
  }); vi.stubGlobal('fetch', fetch);
  return { fetch, setSource: (current: SourceRecord) => { source = current; } };
}
async function select(user: ReturnType<typeof userEvent.setup>) {
  await user.click(await screen.findByRole('radio', { name: new RegExp(CONTENT_PROFILE) }));
  await user.click(await within(screen.getByRole('region', { name: 'Compatible Content sources' })).findByRole('radio', { name: /alpha/ }));
  await user.click(await screen.findByRole('radio', { name: new RegExp(PUBLISHER_REFERENCE) }));
}
const posts = (fetch: ReturnType<typeof service>['fetch']) => fetch.mock.calls.filter(([, init]) => init.method === 'POST');
beforeEach(() => { sessionStorage.clear(); window.history.replaceState(null, '', '/#/operations/submit/content'); });

describe('explicit queued Content selection journey', () => {
  it('uses a distinguished strict route and rejects unknown route parameters', () => {
    expect(parseWorkspaceRoute('#/operations/submit/content')).toBe('content-submit');
    for (const route of ['#/operations/submit/content?force=1', '#/operations/submit/content/other', '#/operations/submit/content?sourceId=alpha']) expect(parseWorkspaceRoute(route)).toBe('not-found');
    expect(parseWorkspaceRoute('#/operations/submit')).toBe('run-submit');
  });
  it('selects, freezes, acknowledges, queues and navigates to admitted run history', async () => {
    const api = service(), user = userEvent.setup(); render(<App />); await select(user);
    await user.click(screen.getByRole('button', { name: 'Review Content submission' }));
    const heading = await screen.findByRole('heading', { name: 'Review Content run' });
    expect(heading).toHaveFocus(); expect(readRunIntent()!.phase).toBe('review'); expect(JSON.parse(readRunIntent()!.command.body)).toEqual(contentRequest());
    expect(api.fetch.mock.calls.filter(([path]) => path.includes('/sources/content-intelligence/alpha'))).toHaveLength(1); expect(posts(api.fetch)).toHaveLength(0);
    expect(screen.getByText(/REGISTRATION_METADATA_ONLY/)).toBeInTheDocument(); expect(screen.getByText(/Public Content execution has not completed operational qualification/)).toBeInTheDocument();
    const confirm = screen.getByRole('button', { name: 'Confirm Content submission' }); expect(confirm).toBeDisabled();
    await user.click(confirm); expect(posts(api.fetch)).toHaveLength(0);
    await user.click(screen.getByRole('checkbox', { name: /I acknowledge the Content queue limitations/ })); expect(confirm).toBeEnabled();
    const original = readRunIntent()!.command; await user.click(confirm); await screen.findByRole('link', { name: 'Open admitted run' });
    expect(readRunIntent()).toMatchObject({ command: original, phase: 'confirmed', receipt: { state: 'QUEUED' } }); expect(posts(api.fetch)).toHaveLength(1);
    expect(posts(api.fetch)[0]![1]).toMatchObject({ body: original.body, headers: { 'Idempotency-Key': original.key } });
    expect(screen.getByText(/QUEUED means accepted for admission, not actively processing/)).toBeInTheDocument();
    await user.click(screen.getByRole('link', { name: 'Open admitted run' })); await screen.findByRole('heading', { name: /^Run detail/ });
    expect(window.location.hash).toBe(`#/operations/executions/${contentRun().runId}`); expect(posts(api.fetch)).toHaveLength(1);
  });
  it('reports every missing selection without POST or a new intent', async () => {
    const api = service(), user = userEvent.setup(); render(<App />);
    await user.click(screen.getByRole('button', { name: 'Review Content submission' }));
    expect(screen.getByText('Select a registered Content execution profile.')).toBeInTheDocument();
    expect(screen.getByText('Select exactly one compatible current Content source.')).toBeInTheDocument();
    expect(screen.getByText('Select one imported publisher-reference artifact.')).toBeInTheDocument(); expect(readRunIntent()).toBeNull(); expect(posts(api.fetch)).toHaveLength(0);
  });
  it('clears source and reference selections on profile change and refresh', async () => {
    const api = service(), user = userEvent.setup(); render(<App />); await select(user);
    await user.click(screen.getByRole('radio', { name: /81000000-0000-4000-8000-000000000002/ }));
    await waitFor(() => expect(screen.getByRole('radio', { name: new RegExp(PUBLISHER_REFERENCE) })).not.toBeChecked());
    expect(await screen.findByRole('radio', { name: /alpha/ })).not.toBeChecked();
    await user.click(screen.getByRole('radio', { name: /alpha/ })); await user.click(screen.getByRole('radio', { name: new RegExp(PUBLISHER_REFERENCE) }));
    await user.click(screen.getByRole('button', { name: 'Refresh Content selections' }));
    expect(await screen.findByRole('radio', { name: /alpha/ })).not.toBeChecked(); expect(await screen.findByRole('radio', { name: new RegExp(PUBLISHER_REFERENCE) })).not.toBeChecked();
    expect(posts(api.fetch)).toHaveLength(0);
  });
  it.each(['disabled', 'revision', 'digest'])('rejects a %s source recheck before freezing', async (change) => {
    const api = service(), user = userEvent.setup(); render(<App />); await select(user);
    api.setSource({ ...sourceRecord(), ...(change === 'disabled' ? { enabled: false } : change === 'revision' ? { currentRevision: 3 } : { configSha256: 'f'.repeat(64) }) });
    await user.click(screen.getByRole('button', { name: 'Review Content submission' }));
    await screen.findByText(/The selected source is disabled or changed/); expect(readRunIntent()).toBeNull(); expect(posts(api.fetch)).toHaveLength(0);
  });
  it('handles source-check failure without exposing private diagnostics or creating an intent', async () => {
    const api = service(), base = api.fetch.getMockImplementation()!, user = userEvent.setup();
    api.fetch.mockImplementation((path: string, init: RequestInit) => path.includes('/sources/content-intelligence/alpha') ? Promise.resolve(jsonResponse(managementFailure(), 503)) : base(path, init));
    render(<App />); await select(user); await user.click(screen.getByRole('button', { name: 'Review Content submission' }));
    await screen.findByText('Unable to check the current Content source. No run intent was created.'); expect(screen.queryByText('/private/secret traceback')).not.toBeInTheDocument();
    expect(readRunIntent()).toBeNull(); expect(posts(api.fetch)).toHaveLength(0);
  });
  it.each(['profiles', 'sources', 'references'])('displays an empty %s catalog without inventing readiness', async (catalog) => {
    const api = service(), base = api.fetch.getMockImplementation()!, user = userEvent.setup();
    api.fetch.mockImplementation((path: string, init: RequestInit) => path.includes(catalog === 'profiles' ? '/execution-profiles?' : catalog === 'sources' ? '/execution-profile-sources?' : '/artifacts?') ? Promise.resolve(jsonResponse(historyPage([]))) : base(path, init));
    render(<App />); if (catalog !== 'profiles') await user.click(await screen.findByRole('radio', { name: new RegExp(CONTENT_PROFILE) }));
    await screen.findByText(catalog === 'profiles' ? /No compatible registered Content execution profiles/ : catalog === 'sources' ? /No compatible enabled Content source revisions/ : /No compatible receipt-linked publisher-reference/);
    expect(posts(api.fetch)).toHaveLength(0); expect(readRunIntent()).toBeNull();
  });
  it.each(['profile', 'source', 'reference'])('refuses malformed %s discovery as a contract error', async (catalog) => {
    const api = service(), base = api.fetch.getMockImplementation()!, user = userEvent.setup();
    const needle = catalog === 'profile' ? '/execution-profiles?' : catalog === 'source' ? '/execution-profile-sources?' : '/artifacts?';
    const row = catalog === 'profile' ? { ...contentProfile(), runtimeQualification: 'READY' } : catalog === 'source' ? { ...contentSource(), executionProfileId: 'other' } : { ...publisherReference(), role: 'SERVICE_REFERENCE' };
    api.fetch.mockImplementation((path: string, init: RequestInit) => path.includes(needle) ? Promise.resolve(jsonResponse(historyPage([row]))) : base(path, init));
    render(<App />); if (catalog !== 'profile') await user.click(await screen.findByRole('radio', { name: new RegExp(CONTENT_PROFILE) }));
    await screen.findByRole('heading', { name: new RegExp('Unable to load') }); expect(readRunIntent()).toBeNull(); expect(posts(api.fetch)).toHaveLength(0);
  });
  it('preserves selected sources and references across bounded pages without background traversal', async () => {
    const api = service(), base = api.fetch.getMockImplementation()!, user = userEvent.setup();
    api.fetch.mockImplementation((path: string, init: RequestInit) => {
      if (path.includes('/execution-profiles?')) return Promise.resolve(jsonResponse(path.includes('cursor=') ? historyPage([]) : historyPage(Array.from({ length: 20 }, (_, i) => contentProfile(`81000000-0000-4000-8000-${(i + 1).toString(16).padStart(12, '0')}`)), 20, FIRST_CURSOR)));
      if (path.includes('/execution-profile-sources?')) return Promise.resolve(jsonResponse(path.includes('cursor=') ? historyPage([]) : historyPage([contentSource(), ...Array.from({ length: 19 }, (_, i) => contentSource(`source-${i.toString().padStart(2, '0')}`))], 20, FIRST_CURSOR)));
      if (path.includes('/artifacts?')) return Promise.resolve(jsonResponse(path.includes('cursor=') ? historyPage([]) : historyPage(Array.from({ length: 20 }, (_, i) => publisherReference(`a1000000-0000-4000-8000-${(i + 1).toString(16).padStart(12, '0')}`)), 20, FIRST_CURSOR)));
      return base(path, init);
    }); render(<App />); await select(user);
    expect(api.fetch.mock.calls.some(([path]) => path.includes('cursor='))).toBe(false);
    for (const [next, empty, previous, selected] of [
      ['content execution profiles', /No compatible registered Content execution profiles/, 'content execution profiles', new RegExp(CONTENT_PROFILE)],
      ['compatible content sources', /No compatible enabled Content source revisions/, 'compatible content sources', /alpha/],
      ['publisher references', /No compatible receipt-linked publisher-reference/, 'publisher references', new RegExp(PUBLISHER_REFERENCE)],
    ] as const) {
      await user.click(screen.getByRole('button', { name: `Next ${next}` })); await screen.findByText(empty);
      await user.click(screen.getByRole('button', { name: `Previous ${previous}` })); expect(await screen.findByRole('radio', { name: selected })).toBeChecked();
    }
    await user.click(screen.getByRole('button', { name: 'Review Content submission' })); await screen.findByRole('heading', { name: 'Review Content run' });
    expect(JSON.parse(readRunIntent()!.command.body)).toEqual(contentRequest()); expect(posts(api.fetch)).toHaveLength(0);
  });
  it('aborts a pending current-source recheck on navigation and ignores late results', async () => {
    const api = service(), base = api.fetch.getMockImplementation()!, late = deferred<Response>(), user = userEvent.setup();
    api.fetch.mockImplementation((path: string, init: RequestInit) => path.includes('/sources/content-intelligence/alpha') ? late.promise : base(path, init));
    render(<App />); await select(user); await user.click(screen.getByRole('button', { name: 'Review Content submission' }));
    const signal = api.fetch.mock.calls.find(([path]) => path.includes('/sources/content-intelligence/alpha'))![1].signal as AbortSignal;
    await user.click(screen.getByRole('link', { name: 'Submit Reliability Run' })); expect(signal.aborted).toBe(true);
    await act(async () => late.resolve(sourceResponse())); expect(readRunIntent()).toBeNull(); expect(posts(api.fetch)).toHaveLength(0);
  });
  it('opens Content selection from enabled RSS source detail without preselecting inputs', async () => {
    service(); window.history.replaceState(null, '', '/#/operations/sources/content-intelligence/alpha'); const user = userEvent.setup(); render(<App />);
    await user.click(await screen.findByRole('link', { name: 'Choose queued Content inputs' })); await screen.findByRole('heading', { name: 'Submit Queued Content Run' });
    expect(await screen.findByRole('radio', { name: new RegExp(CONTENT_PROFILE) })).not.toBeChecked(); expect(readRunIntent()).toBeNull();
  });
  it('does not offer queued submission from a disabled Content source', async () => {
    const api = service(); api.setSource({ ...sourceRecord(), enabled: false }); window.history.replaceState(null, '', '/#/operations/sources/content-intelligence/alpha'); render(<App />);
    await screen.findByRole('heading', { name: 'Source alpha' }); expect(screen.queryByRole('link', { name: 'Choose queued Content inputs' })).not.toBeInTheDocument();
  });
  it('supports Back/Forward and stops discovery outside Content selection', async () => {
    const api = service(), user = userEvent.setup(); render(<App />); await screen.findByRole('radio', { name: new RegExp(CONTENT_PROFILE) });
    await user.click(screen.getByRole('link', { name: 'Executions' })); const count = () => api.fetch.mock.calls.filter(([path]) => path.includes('/execution-profiles?')).length;
    expect(count()).toBe(1); await act(async () => window.history.back()); await screen.findByRole('heading', { name: 'Submit Queued Content Run' });
    await screen.findByRole('radio', { name: new RegExp(CONTENT_PROFILE) }); expect(count()).toBe(2);
    await act(async () => window.history.forward()); await waitFor(() => expect(window.location.hash).toBe('#/operations/executions')); expect(count()).toBe(2);
  });
  it('blocks discovery and replacement when a Reliability intent already occupies the tab', () => {
    const api = service(); reviewRunSubmission(JSON.stringify(reliabilityRequest())); const before = sessionStorage.getItem('ddip.reliability-run-intent.v1'); render(<App />);
    expect(screen.getByRole('heading', { name: 'Review Reliability run' })).toBeInTheDocument(); expect(screen.queryByRole('radio')).not.toBeInTheDocument();
    expect(sessionStorage.getItem('ddip.reliability-run-intent.v1')).toBe(before); expect(api.fetch).not.toHaveBeenCalled();
  });
});

describe('source candidate/current-record correspondence', () => {
  it('requires the same Content source, current revision, digest, enabled gate and RSS profile', () => {
    expect(contentSourceMatches(contentSource(), sourceRecord())).toBe(true);
    for (const source of [{ ...sourceRecord(), sourceId: 'other' }, { ...sourceRecord(), enabled: false }, { ...sourceRecord(), currentRevision: 3 },
      { ...sourceRecord(), configSha256: 'e'.repeat(64) }, sourceRecord('alpha', 'application-reliability')]) expect(contentSourceMatches(contentSource(), source)).toBe(false);
  });
});
