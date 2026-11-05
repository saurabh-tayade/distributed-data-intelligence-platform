import { readFileSync } from 'node:fs';
import { expect, it } from 'vitest';
import { parseContentCatalog, parseContentDiscoveryDaily, parseContentReferenceCoverage } from './contentApi';
import type { ContentQuery, ContentReport } from './contentApi';

// Opt-in cross-runtime check against HTTP envelopes generated from native fixtures.
// Generated publications and responses remain under the backend's ignored target/.
const fixturePath = process.env.DDIP_CONTENT_HTTP_FIXTURES;
it.skipIf(!fixturePath)('admits every native-backed HTTP envelope with the actual frontend parser', () => {
  const scenarios: { report: ContentReport; query: ContentQuery; response: unknown }[] = JSON.parse(readFileSync(fixturePath!, 'utf8'));
  expect(Array.isArray(scenarios)).toBe(true); expect(scenarios).toHaveLength(150);
  expect(Object.fromEntries((['catalog', 'source_discovery_daily', 'reference_coverage'] as const).map(report => [report, scenarios.filter(scenario => scenario.report === report).length])))
    .toEqual({ catalog: 60, source_discovery_daily: 54, reference_coverage: 36 });
  const reports = new Set(scenarios.map(scenario => scenario.report));
  expect(reports.has('source_discovery_daily')).toBe(true); expect(reports.has('reference_coverage')).toBe(true);
  for (const scenario of scenarios) {
    const parser = { catalog: parseContentCatalog, source_discovery_daily: parseContentDiscoveryDaily, reference_coverage: parseContentReferenceCoverage }[scenario.report];
    expect(parser, `Unsupported report ${scenario.report}`).toBeTypeOf('function');
    expect(parser(scenario.response, scenario.query)).toEqual(scenario.response);
  }
});
