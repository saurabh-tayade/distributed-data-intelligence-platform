import { act, render, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { App } from './App';
import { parseWorkspaceRoute } from './navigation';
import { selectionErrors } from './operations/ReliabilitySubmission';
import { readRunIntent } from './operations/runIntent';
import type { SourceRecord } from './managementApi';
import { deferred, fixture, jsonResponse } from './test/fixtures';
import { FIRST_CURSOR, managementFailure, sourcePage, sourceResponse } from './test/managementFixtures';
import { batch, BATCH_ID, fullProfiles, historyPage, profile, PROFILE_ID, PROFILE_2, reference, REFERENCE_ID, request, selectedSource, submissionResponse } from './test/submissionFixtures';

function service() {
  let current = selectedSource();
  const fetch = vi.fn().mockImplementation(async (path: string) => {
    if (path.startsWith('/api/management/v1/execution-profiles')) return jsonResponse(historyPage([profile(), profile(PROFILE_2)]));
    if (path.startsWith('/api/management/v1/sources?')) { const { config: _config, ...summary } = current; void _config; return jsonResponse(sourcePage([summary])); }
    if (path.startsWith('/api/management/v1/sources/')) return sourceResponse(current);
    if (path.startsWith('/api/management/v1/artifacts?')) return jsonResponse(path.includes('role=RELIABILITY_BATCH') ? historyPage([batch()]) : historyPage([reference()]));
    return jsonResponse(fixture());
  });
  vi.stubGlobal('fetch', fetch); return { fetch, setSource: (record: SourceRecord) => { current = record; } };
}
async function select(user: ReturnType<typeof userEvent.setup>) {
  await user.click(await screen.findByRole('radio', { name: new RegExp(PROFILE_ID) }));
  await user.click(await within(screen.getByRole('region', { name: 'Enabled Reliability sources' })).findByRole('checkbox', { name: /Source alpha/ }));
  await screen.findByRole('button', { name: 'Remove source alpha' });
  await user.click(await screen.findByRole('checkbox', { name: new RegExp(BATCH_ID) }));
  await user.click(await screen.findByRole('radio', { name: new RegExp(REFERENCE_ID) }));
}
beforeEach(() => { sessionStorage.clear(); window.history.replaceState(null, '', '/#/operations/submit'); });

describe('Reliability selection checkpoint A', () => {
  it('supports a dedicated direct route and safe unknown paths', () => {
    expect(parseWorkspaceRoute('#/operations/submit')).toBe('run-submit');
    for (const route of ['#/operations/submit/bad', '#/operations/submit?pipeline=CONTENT', '#/operations/executions/submit']) expect(parseWorkspaceRoute(route)).toBe('not-found');
  });
  it('reviews a complete frozen request, checks current sources and performs no automatic mutation', async () => {
    const { fetch } = service(), user = userEvent.setup(); render(<App />);
    await select(user); await user.click(screen.getByRole('button', { name: 'Review Reliability submission' }));
    await screen.findByRole('heading', { name: 'Review Reliability run' });
    expect(JSON.parse(readRunIntent()!.command.body)).toEqual(request()); expect(readRunIntent()!.phase).toBe('review');
    expect(fetch.mock.calls.filter(([path]) => path.includes('/sources/application-reliability/alpha'))).toHaveLength(2);
    expect(fetch.mock.calls.every(([, init]) => init.method === 'GET')).toBe(true);
    expect(screen.queryByRole('button', { name: /Content.*(run|submit)/i })).not.toBeInTheDocument();
  });
  it('shows inline requirements for missing choices', async () => {
    service(); const user = userEvent.setup(); render(<App />);
    await user.click(screen.getByRole('button', { name: 'Review Reliability submission' }));
    expect(screen.getByText('Select a registered Reliability execution profile.')).toBeInTheDocument();
    expect(readRunIntent()).toBeNull();
  });
  it('clears sources, batches and reference on a profile change', async () => {
    service(); const user = userEvent.setup(); render(<App />); await select(user);
    await user.click(screen.getByRole('radio', { name: new RegExp(PROFILE_2) }));
    expect(screen.getByRole('heading', { name: 'Selected sources (0/32)' })).toBeInTheDocument();
    expect(screen.getByRole('heading', { name: 'Selected batches (0/32)' })).toBeInTheDocument();
    await waitFor(() => expect(screen.getByRole('radio', { name: new RegExp(REFERENCE_ID) })).not.toBeChecked());
  });
  it('filters batches on the server and removes dependent batches when a source is removed', async () => {
    const { fetch } = service(), user = userEvent.setup(); render(<App />); await select(user);
    await user.selectOptions(screen.getByRole('combobox', { name: 'Batch source filter' }), 'alpha');
    await waitFor(() => expect(fetch.mock.calls.some(([path]) => path.includes('sourceId=alpha') && path.includes('role=RELIABILITY_BATCH'))).toBe(true));
    await user.click(screen.getByRole('button', { name: 'Remove source alpha' }));
    expect(screen.getByRole('heading', { name: 'Selected batches (0/32)' })).toBeInTheDocument();
    expect(screen.getByRole('combobox', { name: 'Batch source filter' })).toHaveValue('');
  });
  it.each(['disabled', 'profile'])('refuses a source whose current %s is incompatible', async (change) => {
    const api = service(), source = selectedSource();
    if (change === 'disabled') source.enabled = false;
    else source.config = { ...source.config, profile: 'request-parquet-v1', dataOrigin: 'synthetic', feedEndpointId: null, registeredProfileSha256: 'b'.repeat(64) };
    const user = userEvent.setup(); render(<App />);
    await user.click(await screen.findByRole('radio', { name: new RegExp(PROFILE_ID) }));
    await within(screen.getByRole('region', { name: 'Enabled Reliability sources' })).findByRole('checkbox'); api.setSource(source);
    await user.click(screen.getByRole('checkbox', { name: /Source alpha/ }));
    await screen.findByText('This source must be enabled and match the selected execution profile.');
    expect(screen.getByRole('heading', { name: 'Selected sources (0/32)' })).toBeInTheDocument();
  });
  it('requires refreshed deliberate selection if a source revision changes before review', async () => {
    const api = service(), user = userEvent.setup(); render(<App />); await select(user);
    api.setSource({ ...selectedSource(), currentRevision: 3, rowVersion: '9007199254740994' });
    await user.click(screen.getByRole('button', { name: 'Review Reliability submission' }));
    await screen.findByText(/A selected source changed before review/); expect(readRunIntent()).toBeNull();
    await user.click(screen.getByRole('button', { name: 'Refresh selections' }));
    expect(screen.getByRole('heading', { name: 'Selected sources (0/32)' })).toBeInTheDocument();
  });
  it('does not silently adopt a newer source revision after the operator selected a list row', async () => {
    const api = service(), user = userEvent.setup(); render(<App />);
    await user.click(await screen.findByRole('radio', { name: new RegExp(PROFILE_ID) }));
    const choice = await screen.findByRole('checkbox', { name: /Source alpha/ });
    api.setSource({ ...selectedSource(), currentRevision: 3, rowVersion: '9007199254740994' });
    await user.click(choice); await screen.findByText(/This source changed since discovery/);
    expect(screen.getByRole('heading', { name: 'Selected sources (0/32)' })).toBeInTheDocument(); expect(readRunIntent()).toBeNull();
  });
  it('pages profiles without losing the selected profile and restarts invalid cursors explicitly', async () => {
    const api = service(), user = userEvent.setup(), base = api.fetch.getMockImplementation()!;
    api.fetch.mockImplementation((path: string, init: RequestInit) => path.startsWith('/api/management/v1/execution-profiles')
      ? Promise.resolve(path.includes('cursor=') ? jsonResponse(managementFailure('INVALID_ARGUMENT'), 400) : jsonResponse(fullProfiles(FIRST_CURSOR))) : base(path, init));
    render(<App />); await user.click(await screen.findByRole('radio', { name: new RegExp(PROFILE_ID) }));
    await user.click(screen.getByRole('button', { name: 'Next execution profiles' }));
    await screen.findByRole('heading', { name: 'Unable to load execution profiles' });
    expect(screen.getByText(/Selected profile:/)).toHaveTextContent(PROFILE_ID);
    await user.click(screen.getByRole('button', { name: 'Start from first page' }));
    await screen.findByRole('radio', { name: new RegExp(PROFILE_ID) });
    expect(api.fetch.mock.calls.filter(([path]) => path.startsWith('/api/management/v1/execution-profiles')).at(-1)![0]).not.toContain('cursor=');
  });
  it('distinguishes empty profiles, empty receipt-linked artifacts and failed discovery', async () => {
    const api = service(), user = userEvent.setup(), base = api.fetch.getMockImplementation()!;
    api.fetch.mockResolvedValueOnce(jsonResponse(historyPage([]))); render(<App />);
    await screen.findByText(/No compatible registered Reliability execution profiles/);
    await user.click(screen.getByRole('button', { name: 'Refresh execution profiles' }));
    await user.click(await screen.findByRole('radio', { name: new RegExp(PROFILE_ID) }));
    api.fetch.mockImplementation((path: string, init: RequestInit) => path.includes('/artifacts?') ? Promise.resolve(jsonResponse(historyPage([]))) : base(path, init));
    await user.click(screen.getByRole('button', { name: 'Refresh service references' }));
    await screen.findByText(/No compatible receipt-linked service-reference artifacts/);
    api.fetch.mockResolvedValueOnce(jsonResponse(managementFailure(), 503));
    await user.click(screen.getByRole('button', { name: 'Refresh service references' }));
    await screen.findByRole('heading', { name: 'Unable to load service references' });
  });
  it('cancels late source selection and catalog GETs on navigation', async () => {
    const api = service(), user = userEvent.setup(), base = api.fetch.getMockImplementation()!, late = deferred<Response>();
    api.fetch.mockImplementation((path: string, init: RequestInit) => path.includes('/sources/application-reliability/alpha') ? late.promise : base(path, init));
    render(<App />); await user.click(await screen.findByRole('radio', { name: new RegExp(PROFILE_ID) }));
    await user.click(await screen.findByRole('checkbox', { name: /Source alpha/ }));
    const signal = api.fetch.mock.calls.find(([path]) => path.includes('/sources/application-reliability/alpha'))![1].signal as AbortSignal;
    await user.click(screen.getByRole('link', { name: 'Executions' })); expect(signal.aborted).toBe(true);
    await act(async () => late.resolve(sourceResponse())); expect(readRunIntent()).toBeNull();
    expect(screen.queryByText('Selected sources (1/32)')).not.toBeInTheDocument();
  });
  it('preserves source and batch selections across independent Next/Previous pages', async () => {
    const api = service(), user = userEvent.setup(), base = api.fetch.getMockImplementation()!;
    const sources = [selectedSource(), ...Array.from({ length: 19 }, (_, i) => selectedSource(`source-${String(i).padStart(2, '0')}`))];
    const summaries = sources.map(({ config: _config, ...summary }) => { void _config; return summary; });
    const batches = Array.from({ length: 20 }, (_, i) => batch(`90000000-0000-4000-8000-${(i + 1).toString(16).padStart(12, '0')}`));
    api.fetch.mockImplementation((path: string, init: RequestInit) => {
      if (path.includes('/sources?')) return Promise.resolve(jsonResponse(path.includes('cursor=') ? sourcePage([], 20) : sourcePage(summaries, 20, FIRST_CURSOR)));
      if (path.includes('role=RELIABILITY_BATCH')) return Promise.resolve(jsonResponse(path.includes('cursor=') ? historyPage([], 20) : historyPage(batches, 20, FIRST_CURSOR)));
      return base(path, init);
    });
    render(<App />); await select(user);
    await user.click(screen.getByRole('button', { name: 'Next enabled reliability sources' }));
    await screen.findByText(/No enabled Reliability sources were returned/);
    expect(screen.getByRole('button', { name: 'Remove source alpha' })).toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'Previous enabled reliability sources' }));
    expect(await within(screen.getByRole('region', { name: 'Enabled Reliability sources' })).findByRole('checkbox', { name: /Source alpha/ })).toBeChecked();
    await user.click(screen.getByRole('button', { name: 'Next batch artifacts' }));
    await screen.findByText(/No compatible receipt-linked batch artifacts/);
    expect(screen.getByRole('heading', { name: 'Selected batches (1/32)' })).toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'Previous batch artifacts' }));
    expect(await screen.findByRole('checkbox', { name: new RegExp(BATCH_ID) })).toBeChecked();
    const parameters = api.fetch.mock.calls.filter(([path]) => path.includes('/sources?')).map(([path]) => new URL(path, 'http://localhost').searchParams);
    expect(parameters.every((q) => q.get('productId') === 'application-reliability' && q.get('enabled') === 'true' && q.get('limit') === '20')).toBe(true);
  });
  it('cannot select incompatible batches or silently adopt stale artifact responses after a profile change', async () => {
    const api = service(), user = userEvent.setup(), base = api.fetch.getMockImplementation()!, late = deferred<Response>();
    let defer = false;
    api.fetch.mockImplementation((path: string, init: RequestInit) => path.includes('role=RELIABILITY_BATCH') ? defer ? late.promise
      : Promise.resolve(jsonResponse(historyPage([{ ...batch(), identity: { ...batch().identity, sourceRevision: 1 } }]))) : base(path, init));
    render(<App />); await user.click(await screen.findByRole('radio', { name: new RegExp(PROFILE_ID) }));
    await user.click(await screen.findByRole('checkbox', { name: /Source alpha/ }));
    expect(await screen.findByRole('checkbox', { name: new RegExp(BATCH_ID) })).toBeDisabled();
    defer = true; await user.click(screen.getByRole('button', { name: 'Refresh batch artifacts' }));
    const signal = api.fetch.mock.calls.filter(([path]) => path.includes('role=RELIABILITY_BATCH')).at(-1)![1].signal as AbortSignal;
    await user.click(screen.getByRole('radio', { name: new RegExp(PROFILE_2) })); expect(signal.aborted).toBe(true);
    await act(async () => late.resolve(jsonResponse(historyPage([batch()]))));
    expect(screen.queryByRole('checkbox', { name: new RegExp(BATCH_ID) })).not.toBeInTheDocument();
  });
  it('completes selection → immutable review → explicit POST → persisted receipt', async () => {
    const api = service(), user = userEvent.setup(), base = api.fetch.getMockImplementation()!;
    api.fetch.mockImplementation((path: string, init: RequestInit) => init.method === 'POST' ? Promise.resolve(submissionResponse()) : base(path, init));
    render(<App />); await select(user); await user.click(screen.getByRole('button', { name: 'Review Reliability submission' }));
    const original = readRunIntent()!.command;
    await user.click(await screen.findByRole('button', { name: 'Confirm Reliability submission' }));
    await screen.findByRole('link', { name: 'Open admitted run' });
    const posts = api.fetch.mock.calls.filter(([, init]) => init.method === 'POST'); expect(posts).toHaveLength(1);
    expect(posts[0]![1].body).toBe(original.body); expect(JSON.parse(posts[0]![1].body as string)).toEqual(request());
  });
  it('supports the registered Parquet profile and matching batch format without new submission fields', async () => {
    const api = service(), user = userEvent.setup(), base = api.fetch.getMockImplementation()!, source = selectedSource();
    api.setSource({ ...source, config: { ...source.config, profile: 'request-parquet-v1', dataOrigin: 'synthetic', feedEndpointId: null, registeredProfileSha256: 'b'.repeat(64) } });
    api.fetch.mockImplementation((path: string, init: RequestInit) => {
      if (path.includes('/execution-profiles?')) return Promise.resolve(jsonResponse(historyPage([profile(PROFILE_ID, true)])));
      if (path.includes('role=RELIABILITY_BATCH')) return Promise.resolve(jsonResponse(historyPage([{ ...batch(), nativeFormat: 'PARQUET', identity: { ...batch().identity, inputFormat: 'PARQUET' } }])));
      return base(path, init);
    });
    render(<App />); await select(user); await user.click(screen.getByRole('button', { name: 'Review Reliability submission' }));
    await screen.findByRole('heading', { name: 'Review Reliability run' }); expect(JSON.parse(readRunIntent()!.command.body)).toEqual(request());
  });
  it('navigates with browser Back/Forward and suspends discovery outside the submission route', async () => {
    const api = service(), user = userEvent.setup(); render(<App />);
    await screen.findByRole('radio', { name: new RegExp(PROFILE_ID) });
    await user.click(screen.getByRole('link', { name: 'Executions' }));
    const count = () => api.fetch.mock.calls.filter(([path]) => path.includes('/execution-profiles')).length;
    expect(count()).toBe(1);
    await act(async () => window.history.back());
    await screen.findByRole('heading', { name: 'Submit Reliability Run' });
    await screen.findByRole('radio', { name: new RegExp(PROFILE_ID) }); expect(count()).toBe(2);
    await act(async () => window.history.forward());
    await waitFor(() => expect(window.location.hash).toBe('#/operations/executions')); expect(count()).toBe(2);
  });
});

describe('Reliability selection bounds and compatibility', () => {
  it('accepts minimum and maximum distinct covered selections', () => {
    expect(selectionErrors(profile(), [selectedSource()], [batch()], reference())).toEqual([]);
    const sources = Array.from({ length: 32 }, (_, i) => selectedSource(`source-${i}`));
    const batches = sources.map((s, i) => batch(`90000000-0000-4000-8000-${i.toString(16).padStart(12, '0')}`, s.sourceId));
    expect(selectionErrors(profile(), sources, batches, reference())).toEqual([]);
    expect(selectionErrors(profile(), [...sources, selectedSource('overflow')], batches, reference()).length).toBeGreaterThan(0);
    expect(selectionErrors(profile(), sources, [...batches, batch('90000000-0000-4000-8000-000000000099')], reference()).length).toBeGreaterThan(0);
  });
  it('rejects duplicate identities, missing coverage, wrong products, revisions, profile digests and bindings', () => {
    for (const sources of [[selectedSource(), selectedSource()], [selectedSource('uncovered')], [{ ...selectedSource(), productId: 'content-intelligence' as const }], [{ ...selectedSource(), enabled: false }]]) {
      expect(selectionErrors(profile(), sources, [batch()], reference()).length).toBeGreaterThan(0);
    }
    for (const item of [{ ...batch(), nativeFormat: 'PARQUET' as const }, { ...batch(), storageBindingId: 'different' },
      { ...batch(), identity: { ...batch().identity, sourceRevision: 1 } }, { ...batch(), identity: { ...batch().identity, registeredProfileSha256: 'f'.repeat(64) } }]) {
      expect(selectionErrors(profile(), [selectedSource()], [item], reference()).length).toBeGreaterThan(0);
    }
    expect(selectionErrors(profile(), [selectedSource()], [batch(), batch()], reference()).length).toBeGreaterThan(0);
  });
});
