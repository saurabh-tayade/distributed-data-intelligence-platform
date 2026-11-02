import { StrictMode } from 'react';
import { act, render, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { App } from './App';
import { SourceCommandPanel } from './operations/SourceCommandPanel';
import { SourceEditor } from './operations/SourceEditor';
import { readSourceIntent, reviewSourceCommand, saveSourceIntent, SOURCE_INTENT_KEY } from './operations/sourceIntent';
import type { SourceCommand, SourceRecord } from './managementApi';
import { capabilities, command, baseline, ENDPOINT, STORAGE, REL_STORAGE } from './test/sourceActionFixtures';
import { CORRELATION, managementFailure, revision, revisionPage, sourceEnvelope, sourcePage, sourceRecord, sourceResponse } from './test/managementFixtures';
import { deferred, fixture, jsonResponse } from './test/fixtures';

const detailRoute = '#/operations/sources/content-intelligence/alpha';
const mutations = (fetch: ReturnType<typeof vi.fn>) => fetch.mock.calls.filter((call) => ['POST', 'PUT', 'DELETE', 'PATCH'].includes(call[1]?.method));
function service(initial = sourceRecord()) {
  let record: SourceRecord = initial;
  const fetch = vi.fn().mockImplementation(async (path: string, init: RequestInit) => {
    if (['POST', 'PUT'].includes(init.method ?? 'GET')) {
      const body = JSON.parse(init.body as string);
      if (path === '/api/management/v1/sources') record = { ...record, ...body, currentRevision: 1, rowVersion: '1' };
      else if (path.endsWith('/enabled')) record = { ...record, enabled: body.enabled, rowVersion: '9007199254740994' };
      else record = { ...record, ...body, currentRevision: 3, rowVersion: '9007199254740994' };
      return new Response(JSON.stringify(sourceEnvelope(record)), { status: path === '/api/management/v1/sources' ? 201 : 200,
        headers: { 'Content-Type': 'application/json', ETag: `"${record.rowVersion}"` } });
    }
    if (path === '/api/management/v1/capabilities') return jsonResponse(capabilities());
    if (path.startsWith('/api/management/v1/sources?')) { const { config, ...summary } = record; void config; return jsonResponse(sourcePage([summary])); }
    if (path.includes('/revisions?')) return jsonResponse(revisionPage([revision(1, initial), revision(record.currentRevision, record)].filter((entry, index) => index === 0 || entry.revision !== 1)));
    if (path.startsWith('/api/management/v1/')) return sourceResponse(record);
    return jsonResponse(fixture());
  });
  vi.stubGlobal('fetch', fetch);
  return { fetch, setRecord: (value: SourceRecord) => { record = value; } };
}
async function openActions(user: ReturnType<typeof userEvent.setup>) {
  await screen.findByRole('heading', { name: 'Source alpha' });
  await user.click(screen.getByText('Source management actions'));
}
async function fillRegistration(user: ReturnType<typeof userEvent.setup>, reliability = false) {
  await screen.findByRole('option', { name: 'alpha' });
  if (reliability) await user.selectOptions(screen.getByRole('combobox', { name: 'Product' }), 'application-reliability');
  await user.selectOptions(screen.getByRole('combobox', { name: 'Source ID' }), reliability ? 'reliability' : 'alpha');
  await user.type(screen.getByRole('textbox', { name: 'Display name' }), 'Registered fixture');
  if (reliability) await user.selectOptions(screen.getByRole('combobox', { name: 'Registered Reliability profile' }), JSON.stringify(['request-parquet-v1', 'c'.repeat(64)]));
  else await user.selectOptions(screen.getByRole('combobox', { name: 'Feed endpoint reference' }), ENDPOINT);
  await user.selectOptions(screen.getByRole('combobox', { name: 'Input storage profile reference' }), reliability ? REL_STORAGE : STORAGE);
}
beforeEach(() => { sessionStorage.clear(); window.history.replaceState(null, '', '/#/operations/sources/new'); });

describe('source forms and persisted confirmation', () => {
  it.each([false, true])('registers a reviewed source (Reliability=%s), then opens the persisted current record', async (reliability) => {
    const { fetch } = service(); const user = userEvent.setup(); render(<App />);
    await fillRegistration(user, reliability);
    expect(mutations(fetch)).toHaveLength(0);
    await user.click(screen.getByRole('button', { name: 'Review registration' }));
    expect(screen.getByRole('heading', { name: 'Review source change' })).toHaveFocus();
    expect(screen.getByRole('textbox', { name: 'Display name' })).toBeDisabled();
    const saved = readSourceIntent()!, body = JSON.parse(saved.command.body);
    expect(body.enabled).toBe(false); expect(saved.command.ifMatch).toBeNull();
    expect(body.config).toEqual(reliability ? { configVersion: 1, profile: 'request-parquet-v1', dataOrigin: 'synthetic', feedEndpointId: null,
      registeredProfileSha256: 'c'.repeat(64), inputStorageProfileId: REL_STORAGE } : sourceRecord().config);
    await user.click(screen.getByRole('button', { name: 'Confirm register source' }));
    await screen.findByRole('heading', { name: 'Current source record' });
    await waitFor(() => expect(within(screen.getByRole('region', { name: 'Current source record' })).getByRole('heading', { name: 'Registered fixture' })).toBeVisible());
    expect(window.location.hash).toBe(`#/operations/sources/${reliability ? 'application-reliability/reliability' : 'content-intelligence/alpha'}`);
    expect(mutations(fetch)).toHaveLength(1); expect(readSourceIntent()).toBeNull();
    expect(mutations(fetch)[0]![1].headers['Idempotency-Key']).toBe(saved.command.key);
    expect(fetch.mock.calls.filter((call) => call[1].method === 'GET' && call[0] === `/api/management/v1/sources/${body.productId}/${body.sourceId}`).length).toBeGreaterThanOrEqual(1);
  });
  it('creates a revision with the captured ETag and preserves historical records', async () => {
    window.history.replaceState(null, '', '/' + detailRoute); const { fetch } = service(); const user = userEvent.setup(); render(<App />);
    await openActions(user); await user.click(screen.getByRole('button', { name: 'Create revision' }));
    await screen.findByRole('option', { name: ENDPOINT });
    await user.clear(screen.getByRole('textbox', { name: 'Display name' })); await user.type(screen.getByRole('textbox', { name: 'Display name' }), 'New revision name');
    await user.click(screen.getByRole('button', { name: 'Review revision' }));
    const intent = readSourceIntent()!; expect(intent.command.ifMatch).toBe('"9007199254740993"');
    expect(Object.keys(JSON.parse(intent.command.body))).toEqual(['displayName', 'config']);
    await user.click(screen.getByRole('button', { name: 'Confirm create immutable revision' }));
    await waitFor(() => expect(within(screen.getByRole('region', { name: 'Current source record' })).getByRole('heading', { name: 'New revision name' })).toBeVisible());
    expect(screen.getByText('Historical revision')).toBeVisible(); expect(screen.getByText('Current in displayed source record')).toBeVisible();
    expect(mutations(fetch)).toHaveLength(1); expect(mutations(fetch)[0]![0]).toBe('/api/management/v1/sources/content-intelligence/alpha/revisions');
  });
  it.each([true, false])('confirms admission toggle from enabled=%s with no configuration or revision mutation', async (enabled) => {
    window.history.replaceState(null, '', '/' + detailRoute); const { fetch } = service({ ...sourceRecord(), enabled });
    const user = userEvent.setup(); render(<App />); await openActions(user);
    const action = enabled ? 'Disable admission' : 'Enable admission'; await user.click(screen.getByRole('button', { name: action }));
    expect(screen.getAllByText(/Disabling admission does not cancel active processing/).length).toBeGreaterThan(0);
    expect(mutations(fetch)).toHaveLength(0);
    await user.click(screen.getByRole('button', { name: `Confirm ${action.toLowerCase()}` }));
    await waitFor(() => expect(readSourceIntent()).toBeNull());
    const request = mutations(fetch)[0]![1]; expect(request.method).toBe('PUT'); expect(request.body).toBe(JSON.stringify({ enabled: !enabled }));
    expect(request.headers['If-Match']).toBe('"9007199254740993"');
    await waitFor(() => expect(within(screen.getByRole('region', { name: 'Current source record' })).getByText(enabled ? 'Disabled' : 'Enabled')).toBeVisible());
    expect(mutations(fetch)).toHaveLength(1);
  });
  it('locks Reliability revisions to the exact existing profile and renders nullable references safely', async () => {
    const value = baseline(); value.data = { ...sourceRecord('reliability', 'application-reliability'), config: { ...sourceRecord('reliability', 'application-reliability').config,
      inputStorageProfileId: REL_STORAGE } };
    service(); render(<SourceEditor baseline={value} />);
    const user = userEvent.setup(); await screen.findByRole('option', { name: /request-jsonl-v2 ·/ });
    const profile = screen.getByRole('combobox', { name: 'Registered Reliability profile' });
    expect(within(profile).queryByRole('option', { name: /request-parquet/ })).not.toBeInTheDocument();
    expect(screen.getByRole('combobox', { name: 'Product' })).toBeDisabled(); expect(screen.getByRole('combobox', { name: 'Source ID' })).toBeDisabled();
    await user.click(screen.getByRole('button', { name: 'Review revision' }));
    expect(JSON.parse(readSourceIntent()!.command.body).config.feedEndpointId).toBeNull();
    expect(JSON.parse(readSourceIntent()!.command.body).config.profile).toBe('request-jsonl-v2');
  });
  it('rejects invalid names and incomplete references, with no URL, secret or path input', async () => {
    const { fetch } = service(); const user = userEvent.setup(); render(<App />); await fillRegistration(user);
    const name = screen.getByRole('textbox', { name: 'Display name' });
    await user.clear(name); await user.type(name, 'é'.repeat(65));
    expect(screen.getByRole('button', { name: 'Review registration' })).toBeDisabled();
    await user.clear(name); await user.type(name, '   '); expect(screen.getByRole('button', { name: 'Review registration' })).toBeDisabled();
    await user.clear(name); await user.type(name, 'Valid');
    await user.selectOptions(screen.getByRole('combobox', { name: 'Input storage profile reference' }), '');
    expect(screen.getByRole('button', { name: 'Review registration' })).toBeDisabled();
    expect(screen.getAllByRole('textbox')).toHaveLength(1); expect(mutations(fetch)).toHaveLength(0);
  });
  it('disables configuration writes for an empty or unavailable reviewed catalog, with explicit retry', async () => {
    const empty = capabilities(); empty.data.endpoints = []; empty.data.reliabilityProfiles = []; empty.data.storageProfiles = [];
    const fetch = vi.fn().mockResolvedValueOnce(jsonResponse(managementFailure(), 503)).mockResolvedValue(jsonResponse(empty)); vi.stubGlobal('fetch', fetch);
    const user = userEvent.setup(); render(<App />);
    await screen.findByText(/Reviewed source capabilities are unavailable/);
    expect(screen.getByRole('button', { name: 'Review registration' })).toBeDisabled();
    await user.click(screen.getByRole('button', { name: 'Retry capabilities' }));
    await screen.findByText(/No reviewed source references are available/); expect(mutations(fetch)).toHaveLength(0);
  });
  it('cancels an unsent review without submitting and creates a new key only for a new intent', async () => {
    const { fetch } = service(); const user = userEvent.setup(); render(<App />); await fillRegistration(user);
    await user.click(screen.getByRole('button', { name: 'Review registration' })); const first = readSourceIntent()!.command.key;
    await user.click(screen.getByRole('button', { name: 'Cancel review' }));
    await user.click(screen.getByRole('button', { name: 'Review registration' })); expect(readSourceIntent()!.command.key).not.toBe(first);
    expect(mutations(fetch)).toHaveLength(0);
  });
});

describe('source intent notice ownership', () => {
  const success = 'Source change confirmed. The persisted current record has been refreshed.';

  it('does not attach a resolved success to a new unsubmitted review or resurrect it after cancellation', async () => {
    const { fetch } = service(); const user = userEvent.setup();
    reviewSourceCommand(command('revise')); render(<SourceCommandPanel visible />);
    await user.click(screen.getByRole('button', { name: 'Confirm create immutable revision' }));
    await screen.findByText(success); expect(readSourceIntent()).toBeNull();
    act(() => reviewSourceCommand({ ...command('enabled'), ifMatch: '"9007199254740994"' }));
    const next = readSourceIntent()!;
    expect(screen.queryByText(success)).not.toBeInTheDocument();
    expect(next.phase).toBe('review'); expect(mutations(fetch)).toHaveLength(1);
    await user.click(screen.getByRole('button', { name: 'Cancel review' }));
    expect(screen.queryByText(success)).not.toBeInTheDocument(); expect(readSourceIntent()).toBeNull();
    expect(mutations(fetch)).toHaveLength(1);
  });

  it.each([
    ['registration then revision', 'create', 'revise', 'register source', 'create immutable revision'],
    ['revision then disable', 'revise', 'enabled', 'create immutable revision', 'disable admission'],
    ['disable then enable', 'enabled', 'enabled', 'disable admission', 'enable admission'],
  ] as const)('confirms only the submitted intent for two consecutive successes: %s', async (_, firstAction, secondAction, firstLabel, secondLabel) => {
    const { fetch } = service(); const base = fetch.getMockImplementation()!;
    const pending = deferred<void>(); let writes = 0;
    fetch.mockImplementation((path: string, init: RequestInit) => {
      if (['POST', 'PUT'].includes(init.method ?? 'GET') && ++writes === 2) return pending.promise.then(() => base(path, init));
      return base(path, init);
    });
    reviewSourceCommand(command(firstAction)); const first = readSourceIntent()!.command;
    const user = userEvent.setup(); const mounted = render(<SourceCommandPanel visible />);
    await user.click(screen.getByRole('button', { name: `Confirm ${firstLabel}` }));
    await screen.findByText(success); expect(readSourceIntent()).toBeNull();
    const second = { ...command(secondAction), ifMatch: firstAction === 'create' ? '"1"' : '"9007199254740994"',
      body: secondAction === 'revise' ? JSON.stringify({ displayName: 'Second revision', config: sourceRecord().config })
        : JSON.stringify({ enabled: secondLabel === 'enable admission' }) };
    act(() => reviewSourceCommand(second)); const retained = readSourceIntent()!.command;
    expect(retained.key).not.toBe(first.key); expect(retained.body).toBe(second.body); expect(retained.ifMatch).toBe(second.ifMatch);
    expect(screen.queryByText(success)).not.toBeInTheDocument(); expect(mutations(fetch)).toHaveLength(1);
    await user.click(screen.getByRole('button', { name: `Confirm ${secondLabel}` }));
    mounted.rerender(<SourceCommandPanel visible />);
    expect(readSourceIntent()).toMatchObject({ phase: 'pending', command: retained });
    expect(screen.queryByText(success)).not.toBeInTheDocument(); expect(mutations(fetch)).toHaveLength(2);
    await act(async () => pending.resolve()); await screen.findByText(success);
    expect(readSourceIntent()).toBeNull(); expect(mutations(fetch)).toHaveLength(2);
    expect(mutations(fetch)[1]![1]).toMatchObject({ body: retained.body, headers: { 'If-Match': retained.ifMatch, 'Idempotency-Key': retained.key } });
  });

  it('keeps registration confirmation out of the next revision review through real application navigation', async () => {
    const { fetch } = service(); const user = userEvent.setup(); render(<App />); await fillRegistration(user);
    await user.click(screen.getByRole('button', { name: 'Review registration' }));
    await user.click(screen.getByRole('button', { name: 'Confirm register source' }));
    await within(await screen.findByRole('region', { name: 'Current source record' })).findByRole('heading', { name: 'Registered fixture' });
    await user.click(screen.getByText('Source management actions')); await user.click(screen.getByRole('button', { name: 'Create revision' }));
    await screen.findByRole('option', { name: ENDPOINT }); await user.click(screen.getByRole('button', { name: 'Review revision' }));
    expect(readSourceIntent()).toMatchObject({ phase: 'review', command: { action: 'revise', ifMatch: '"1"' } });
    expect(screen.queryByText(success)).not.toBeInTheDocument(); expect(mutations(fetch)).toHaveLength(1);
    await user.click(screen.getByRole('button', { name: 'Confirm create immutable revision' }));
    await screen.findByText(success); expect(mutations(fetch)).toHaveLength(2);
  });

  it.each([true, false])('does not label an unsubmitted admission action after a revision as confirmed (enabled=%s)', async (enabled) => {
    window.history.replaceState(null, '', '/' + detailRoute); const { fetch } = service({ ...sourceRecord(), enabled });
    const user = userEvent.setup(); render(<App />); await openActions(user);
    await user.click(screen.getByRole('button', { name: 'Create revision' })); await screen.findByRole('option', { name: ENDPOINT });
    await user.click(screen.getByRole('button', { name: 'Review revision' }));
    await user.click(screen.getByRole('button', { name: 'Confirm create immutable revision' })); await screen.findByText(success);
    await within(screen.getByRole('region', { name: 'Current source record' })).findByRole('heading', { name: 'Source alpha' });
    await user.click(screen.getByText('Source management actions'));
    await user.click(screen.getByRole('button', { name: enabled ? 'Disable admission' : 'Enable admission' }));
    expect(screen.queryByText(success)).not.toBeInTheDocument(); expect(mutations(fetch)).toHaveLength(1);
    expect(readSourceIntent()).toMatchObject({ phase: 'review', command: { action: 'enabled', body: JSON.stringify({ enabled: !enabled }), ifMatch: '"9007199254740994"' } });
  });
});

describe('source request ambiguity, preconditions and lifecycle', () => {
  function review(action: SourceCommand['action'] = 'enabled') { const value = command(action); reviewSourceCommand(value); return readSourceIntent()!; }
  it('retains an ambiguous intent across reload, replays the exact body/key/ETag, then GETs the current record', async () => {
    let replay = false;
    const fetch = vi.fn().mockImplementation(async (_path: string, init: RequestInit) => {
      if (init.method === 'GET') return sourceResponse({ ...sourceRecord(), rowVersion: '9007199254740999' });
      if (!replay) { replay = true; throw new Error('/private/connection lost'); }
      return sourceResponse(); // historical saved response, deliberately older than the current GET
    }); vi.stubGlobal('fetch', fetch); const intent = review(); const user = userEvent.setup();
    const mounted = render(<SourceCommandPanel visible />);
    await user.click(screen.getByRole('button', { name: 'Confirm disable admission' }));
    await screen.findByRole('button', { name: 'Resolve same request' });
    expect(readSourceIntent()).toMatchObject({ phase: 'uncertain', command: intent.command });
    expect(screen.queryByRole('button', { name: /Cancel review|Return to deliberate review/ })).not.toBeInTheDocument();
    mounted.rerender(<SourceCommandPanel visible />);
    expect(readSourceIntent()).toMatchObject({ phase: 'uncertain', command: intent.command }); expect(mutations(fetch)).toHaveLength(1);
    mounted.unmount(); render(<SourceCommandPanel visible />);
    expect(mutations(fetch)).toHaveLength(1); // no automatic replay on mount
    await user.click(screen.getByRole('button', { name: 'Resolve same request' }));
    await waitFor(() => expect(readSourceIntent()).toBeNull());
    const writes = mutations(fetch); expect(writes).toHaveLength(2);
    expect(writes[1]![0]).toBe(writes[0]![0]);
    expect(writes[1]![1]).toMatchObject({ method: writes[0]![1].method, body: writes[0]![1].body, headers: writes[0]![1].headers });
    expect(writes[1]![1].headers['If-Match']).toBe(intent.command.ifMatch);
    expect(fetch.mock.calls.at(-1)?.[1].method).toBe('GET'); expect(window.location.hash).toBe(detailRoute);
  });
  it.each([412, 428])('HTTP %s refreshes only, requires deliberate review and never automatically rebases', async (status) => {
    const fetch = vi.fn().mockImplementation(async (_path: string, init: RequestInit) => init.method === 'GET'
      ? sourceResponse({ ...sourceRecord(), rowVersion: '9007199254740994' }) : jsonResponse(managementFailure(status === 412 ? 'PRECONDITION_FAILED' : 'PRECONDITION_REQUIRED'), status));
    vi.stubGlobal('fetch', fetch); const old = review(); const user = userEvent.setup(); render(<SourceCommandPanel visible />);
    await user.click(screen.getByRole('button', { name: 'Confirm disable admission' }));
    await screen.findByText(/Current source refreshed/); expect(mutations(fetch)).toHaveLength(1);
    expect(readSourceIntent()).toMatchObject({ phase: 'rejected', command: old.command });
    expect(screen.queryByRole('button', { name: 'Resolve same request' })).not.toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'Return to deliberate review' })); expect(readSourceIntent()).toBeNull();
    act(() => reviewSourceCommand({ ...command('enabled'), ifMatch: '"9007199254740994"' }));
    expect(readSourceIntent()!.command.key).not.toBe(old.command.key); expect(mutations(fetch)).toHaveLength(1);
    expect(readSourceIntent()).toMatchObject({ phase: 'review', command: { ifMatch: '"9007199254740994"' } });
    expect(screen.queryByText(/Current source refreshed|Current record refreshed|PRECONDITION_FAILED|PRECONDITION_REQUIRED/)).not.toBeInTheDocument();
  });
  it('keeps a failed precondition blocked until the current record can be read', async () => {
    const fetch = vi.fn().mockResolvedValueOnce(jsonResponse(managementFailure('PRECONDITION_FAILED'), 412))
      .mockResolvedValueOnce(jsonResponse(managementFailure(), 503)).mockResolvedValue(sourceResponse()); vi.stubGlobal('fetch', fetch);
    review(); const user = userEvent.setup(); render(<SourceCommandPanel visible />);
    await user.click(screen.getByRole('button', { name: 'Confirm disable admission' })); await screen.findByText(/Refresh failed/);
    expect(screen.getByRole('button', { name: 'Return to deliberate review' })).toBeDisabled();
    await user.click(screen.getByRole('button', { name: 'Refresh current source' }));
    await waitFor(() => expect(screen.getByRole('button', { name: 'Return to deliberate review' })).toBeEnabled()); expect(mutations(fetch)).toHaveLength(1);
  });
  it.each([409, 429, 403, 503])('shows safe HTTP %s classification without automatic retry or raw messages', async (status) => {
    const code = ({ 409: 'STATE_CONFLICT', 429: 'MANAGEMENT_CAPACITY_EXCEEDED', 403: 'LOCAL_BOUNDARY_REJECTED', 503: 'MANAGEMENT_UNAVAILABLE' })[status]!;
    const fetch = vi.fn().mockResolvedValue(jsonResponse(managementFailure(code), status)); vi.stubGlobal('fetch', fetch); review();
    const user = userEvent.setup(); render(<SourceCommandPanel visible />); await user.click(screen.getByRole('button', { name: 'Confirm disable admission' }));
    await screen.findByText(code); expect(screen.getByText(CORRELATION)).toBeVisible(); expect(screen.queryByText(/private|traceback/)).not.toBeInTheDocument();
    expect(mutations(fetch)).toHaveLength(1); expect(readSourceIntent()!.phase).toBe(status === 503 ? 'uncertain' : 'rejected');
    if (status === 409) expect(screen.getByRole('button', { name: 'Return to deliberate review' })).toBeDisabled();
  });
  it('does not repeat a confirmed mutation when its follow-up GET fails', async () => {
    const fetch = vi.fn().mockResolvedValueOnce(sourceResponse()).mockResolvedValueOnce(jsonResponse(managementFailure(), 503)).mockResolvedValue(sourceResponse());
    vi.stubGlobal('fetch', fetch); review(); const user = userEvent.setup(); render(<SourceCommandPanel visible />);
    await user.click(screen.getByRole('button', { name: 'Confirm disable admission' })); await screen.findByText(/current record could not be refreshed/);
    expect(readSourceIntent()!.phase).toBe('confirmed'); expect(screen.queryByRole('button', { name: 'Resolve same request' })).not.toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'Refresh current source' })); await waitFor(() => expect(readSourceIntent()).toBeNull());
    expect(mutations(fetch)).toHaveLength(1); expect(fetch).toHaveBeenCalledTimes(3);
  });
  it('blocks duplicate submission, aborts on navigation and ignores a late successful response', async () => {
    const pending = deferred<Response>(); const { fetch } = service();
    const base = fetch.getMockImplementation()!;
    fetch.mockImplementation((path: string, init: RequestInit) => ['POST', 'PUT'].includes(init.method ?? 'GET') ? pending.promise : base(path, init));
    const user = userEvent.setup(); render(<App />); await fillRegistration(user);
    await user.click(screen.getByRole('button', { name: 'Review registration' }));
    await user.dblClick(screen.getByRole('button', { name: 'Confirm register source' })); expect(mutations(fetch)).toHaveLength(1);
    const signal = mutations(fetch)[0]![1].signal as AbortSignal;
    await user.click(within(screen.getByRole('navigation', { name: 'Workspaces' })).getByRole('link', { name: 'Analytics' }));
    await waitFor(() => expect(signal.aborted).toBe(true)); expect(readSourceIntent()!.phase).toBe('uncertain');
    await act(async () => pending.resolve(new Response(JSON.stringify(sourceEnvelope()), { status: 201, headers: { 'Content-Type': 'application/json', ETag: '"9007199254740993"' } })));
    expect(window.location.hash).toBe('#/analytics'); expect(readSourceIntent()!.phase).toBe('uncertain');
    expect(mutations(fetch)).toHaveLength(1);
  });
  it('restores a pending request as an explicit unknown outcome, never submitting during StrictMode mount', () => {
    saveSourceIntent({ command: command('enabled'), phase: 'pending' }); const fetch = vi.fn(); vi.stubGlobal('fetch', fetch);
    render(<StrictMode><SourceCommandPanel visible /></StrictMode>);
    expect(screen.getByRole('button', { name: 'Resolve same request' })).toBeEnabled(); expect(fetch).not.toHaveBeenCalled();
  });
  it('fails closed for corrupt tab storage and for unavailable storage', () => {
    sessionStorage.setItem(SOURCE_INTENT_KEY, '{broken'); const { unmount } = render(<SourceCommandPanel visible />);
    expect(screen.getByRole('heading', { name: 'Source actions unavailable' })).toBeVisible(); unmount(); sessionStorage.clear();
    vi.spyOn(Storage.prototype, 'getItem').mockImplementation(() => { throw new Error('disabled'); });
    render(<SourceCommandPanel visible />); expect(screen.getByRole('heading', { name: 'Source actions unavailable' })).toBeVisible();
  });
  it('keeps a reviewed revision on its original ETag, then requires a new deliberate review after conflict', async () => {
    window.history.replaceState(null, '', '/' + detailRoute); const { fetch, setRecord } = service();
    const base = fetch.getMockImplementation()!;
    fetch.mockImplementation((path: string, init: RequestInit) => init.method === 'POST'
      ? Promise.resolve(jsonResponse(managementFailure('PRECONDITION_FAILED'), 412)) : base(path, init));
    const user = userEvent.setup(); render(<App />); await openActions(user);
    await user.click(screen.getByRole('button', { name: 'Create revision' })); await screen.findByRole('option', { name: ENDPOINT });
    await user.click(screen.getByRole('button', { name: 'Review revision' })); const previous = readSourceIntent()!.command;
    setRecord({ ...sourceRecord(), rowVersion: '9007199254740994', displayName: 'Concurrent operator name' });
    await user.click(screen.getByRole('button', { name: 'Refresh source and revisions' }));
    await within(screen.getByRole('region', { name: 'Current source record' })).findByRole('heading', { name: 'Concurrent operator name' });
    // A parent refresh cannot silently change an already reviewed precondition.
    expect(readSourceIntent()!.command.ifMatch).toBe('"9007199254740993"');
    await user.click(screen.getByRole('button', { name: 'Confirm create immutable revision' })); await screen.findByText(/Current source refreshed/);
    await user.click(screen.getByRole('button', { name: 'Return to deliberate review' }));
    await within(screen.getByRole('region', { name: 'Current source record' })).findByRole('heading', { name: 'Concurrent operator name' });
    await user.click(screen.getByText('Source management actions'));
    await user.click(screen.getByRole('button', { name: 'Create revision' })); await screen.findByRole('option', { name: ENDPOINT });
    expect(screen.getByRole('textbox', { name: 'Display name' })).toHaveValue('Concurrent operator name');
    await user.click(screen.getByRole('button', { name: 'Review revision' }));
    expect(readSourceIntent()!.command.ifMatch).toBe('"9007199254740994"'); expect(readSourceIntent()!.command.key).not.toBe(previous.key);
    expect(mutations(fetch)).toHaveLength(1);
  });
  it('does not send when tab storage cannot retain the reviewed request', async () => {
    const { fetch } = service(); const user = userEvent.setup(); render(<App />); await fillRegistration(user);
    vi.spyOn(Storage.prototype, 'setItem').mockImplementation(() => { throw new DOMException('quota', 'QuotaExceededError'); });
    await user.click(screen.getByRole('button', { name: 'Review registration' }));
    expect(screen.getByText(/request could not be retained safely/)).toBeVisible(); expect(mutations(fetch)).toHaveLength(0);
  });
  it('does not transmit if pending intent persistence fails after review', async () => {
    const fetch = vi.fn(); vi.stubGlobal('fetch', fetch); const intent = review();
    const user = userEvent.setup(); render(<SourceCommandPanel visible />);
    vi.spyOn(Storage.prototype, 'setItem').mockImplementation(() => { throw new DOMException('quota', 'QuotaExceededError'); });
    await user.click(screen.getByRole('button', { name: 'Confirm disable admission' }));
    expect(screen.getByText(/Tab storage could not be updated/)).toBeVisible();
    expect(fetch).not.toHaveBeenCalled(); expect(readSourceIntent()).toEqual(intent);
  });
  it('invalidates late capabilities on editor unmount without submitting', async () => {
    const pending = deferred<Response>(); const fetch = vi.fn().mockReturnValue(pending.promise); vi.stubGlobal('fetch', fetch);
    const { unmount } = render(<SourceEditor />); const signal = fetch.mock.calls[0]![1].signal as AbortSignal; unmount(); expect(signal.aborted).toBe(true);
    await act(async () => pending.resolve(jsonResponse(capabilities()))); expect(readSourceIntent()).toBeNull(); expect(mutations(fetch)).toHaveLength(0);
  });
});
