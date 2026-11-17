import { readFileSync } from 'node:fs';
import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, expect, it, vi } from 'vitest';
import { App } from './App';
import { parseContentProfiles, parseContentRequest, parseContentSources, parsePublisherReferences, parseRuns, parseSource, sendRunSubmission } from './managementApi';
import { readRunIntent } from './operations/runIntent';
import { fixture, jsonResponse } from './test/fixtures';

// Mandatory in PostgreSQL core CI after the genuine isolated backend test generates it.
// Local frontend-only tests have no database and skip this separate fixture-dependent suite.
const path = process.env.DDIP_CONTENT_SUBMISSION_HTTP_FIXTURES;
describe.skipIf(!path)('real isolated queued Content HTTP contract parity', () => {
  type Fixture = { profiles: unknown; sources: unknown; references: unknown; sourceRecord: unknown; sourceEtag: string;
    request: unknown; key: string; admission: { data: { runId: string; state: string } }; etag: string; location: string; history: unknown };
  const load = () => JSON.parse(readFileSync(path!, 'utf8')) as Fixture;
  const response = (data: Fixture) => new Response(JSON.stringify(data.admission), { status: 202,
    headers: { 'Content-Type': 'application/json', ETag: data.etag, Location: data.location } });
  it('decodes genuine discovery, current source, 202 admission and run history', async () => {
    const data = load(), profile = parseContentProfiles(data.profiles).data[0]!, query = { executionProfileId: profile.executionProfileId };
    const source = parseContentSources(data.sources, query).data[0]!, reference = parsePublisherReferences(data.references, query).data[0]!;
    const current = parseSource(data.sourceRecord, { productId: 'content-intelligence', sourceId: source.sourceId }, data.sourceEtag);
    const request = parseContentRequest(data.request);
    expect(request).toEqual({ pipeline: 'CONTENT_RSS_V1', executionProfileId: profile.executionProfileId, fetchSources: [{ sourceId: source.sourceId, revision: current.data.currentRevision }], retainedSilverArtifactIds: [], publisherReferenceArtifactId: reference.artifactId });
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(response(data)));
    const receipt = await sendRunSubmission({ key: data.key, body: JSON.stringify(request) }, new AbortController().signal);
    expect(receipt.data.state).toBe('QUEUED'); expect(receipt.data.attempts.count).toBe('0');
    expect(parseRuns(data.history, { productId: 'content-intelligence', sourceId: source.sourceId }).data[0]!.runId).toBe(receipt.data.runId);
  });
  it('selects actual backend metadata, freezes and explicitly acknowledges the genuine queued receipt in the UI', async () => {
    const data = load(), request = parseContentRequest(data.request);
    sessionStorage.clear(); window.history.replaceState(null, '', '/#/operations/submit/content');
    const fetch = vi.fn().mockImplementation(async (url: string, init: RequestInit) => {
      if (init.method === 'POST') { expect(JSON.parse(init.body as string)).toEqual(request); return response(data); }
      if (url.includes('/execution-profiles?')) return jsonResponse(data.profiles);
      if (url.includes('/execution-profile-sources?')) return jsonResponse(data.sources);
      if (url.includes('/artifacts?')) return jsonResponse(data.references);
      if (url.includes('/sources/content-intelligence/')) return new Response(JSON.stringify(data.sourceRecord), { headers: { 'Content-Type': 'application/json', ETag: data.sourceEtag } });
      return jsonResponse(fixture());
    }); vi.stubGlobal('fetch', fetch); const user = userEvent.setup(); render(<App />);
    await user.click(await screen.findByRole('radio', { name: new RegExp(request.executionProfileId) }));
    await user.click(await screen.findByRole('radio', { name: new RegExp(request.fetchSources[0].sourceId) }));
    await user.click(await screen.findByRole('radio', { name: new RegExp(request.publisherReferenceArtifactId) }));
    await user.click(screen.getByRole('button', { name: 'Review Content submission' })); await screen.findByRole('heading', { name: 'Review Content run' });
    expect(JSON.parse(readRunIntent()!.command.body)).toEqual(request); expect(fetch.mock.calls.every(([, init]) => init.method === 'GET')).toBe(true);
    expect(screen.getByRole('button', { name: 'Confirm Content submission' })).toBeDisabled();
    await user.click(screen.getByRole('checkbox', { name: /I acknowledge the Content queue limitations/ })); await user.click(screen.getByRole('button', { name: 'Confirm Content submission' }));
    await screen.findByRole('link', { name: 'Open admitted run' });
    expect(readRunIntent()!.receipt).toMatchObject({ runId: data.admission.data.runId, state: 'QUEUED' }); expect(fetch.mock.calls.filter(([, init]) => init.method === 'POST')).toHaveLength(1);
  });
});
