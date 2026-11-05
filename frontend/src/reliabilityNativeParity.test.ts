import { readFileSync } from 'node:fs';
import { expect, it } from 'vitest';
import { parseRequestWindows } from './reliabilityApi';
import type { ReliabilityQuery } from './reliabilityApi';
import { parseInputCoverage, parseReferenceCoverage } from './reliabilityCoverageApi';
import type { CoverageQuery } from './reliabilityCoverageApi';

// CI requires this after HTTP parity. Ordinary frontend-only checks need no Python/JVM.
const fixturePath = process.env.DDIP_RELIABILITY_HTTP_FIXTURES;
it.skipIf(!fixturePath)('admits all real native/HTTP Reliability reports with the actual frontend parsers', () => {
  const scenarios: { name: string; report: string; query: ReliabilityQuery & CoverageQuery; response: unknown }[] =
    JSON.parse(readFileSync(fixturePath!, 'utf8'));
  expect(scenarios).toHaveLength(258);
  expect(Object.fromEntries(['requests_5m', 'input_coverage', 'reference_coverage'].map(report =>
    [report, scenarios.filter(scenario => scenario.report === report).length])))
    .toEqual({ requests_5m: 34, input_coverage: 128, reference_coverage: 96 });
  for (const scenario of scenarios) {
    const parser = { requests_5m: parseRequestWindows, input_coverage: parseInputCoverage,
      reference_coverage: parseReferenceCoverage }[scenario.report];
    expect(parser, scenario.name).toBeTypeOf('function');
    expect(parser!(scenario.response, scenario.query), scenario.name).toEqual(scenario.response);
  }
});
