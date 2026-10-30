# PR #72 — D4 fixture portability and PostgreSQL CI partitions

Status: local implementation and validation complete; hosted Ubuntu acceptance pending.
Tracking: [PR #72](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/pull/72)
and [CI issue #54](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/issues/54).
Production managed Content dispatch remains disabled; Managed Content remains **Partially implemented**.
No datasets, source contracts, native transformations, accepted ADRs or application execution code change.

## Acceptance and root cause

Require the original known-clean launch failure, no child/Gold success, occupied coordinator and no
second authorization. Require every former PostgreSQL integration class exactly once, fail-closed
new-class discovery, independent services/runners, unchanged logical required checks and 90-minute
partition ceilings. Real D1-D4 processing, native qualification and fault coverage remain mandatory.

[Run 40956221086](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/actions/runs/40956221086)
has two independent failures. Job `123778345081` is the final five-suite aggregate;
actual PostgreSQL job `123736967295` ran from 21:15:36 to 22:45:52 UTC on October 29, 2026,
then was cancelled at the configured 90-minute budget. No workflow concurrency cancellation is configured.
`RssSilverIT` had started at 22:36:47 and was still running; its duration is censored, not a completed
suite measurement. Gold's two-source native evidence appeared before its final class error report.
The 327 units passed, and the other four logical backend jobs succeeded.

`RssGoldIT.launchFailureIsKnownCleanButStillOccupied` derived an installation root from the Python
executable and copied `pyvenv.cfg`. A local venv has that file; a setup-python base installation
under `/opt/hostedtoolcache/Python/3.12.15/x64` does not. It failed after D3, before reaching the
intended launch-failure assertions. The unchanged isolated test reproduced on Linux with
`NoSuchFileException: /usr/local/pyvenv.cfg` (15.19-second D4 case, 23.10-second driver interval before final container cleanup).

The replacement injects `PROCESS_START_FAILED` in the existing Gold test fault callback invoked
inside the supervisor's `beforeStart`, immediately after one-use launch authorization. It never
changes executables, Python paths/profiles or production supervision. The child-start callback
must remain uncalled, SQL child identity null, authorization consumed, stage FAILED, Gold absent
and coordinator occupied. A repeated invocation cannot authorize or start a second child. Existing
supervisor tests still exercise a real missing executable/IOException; this integration case
checks its durable known-clean classification at the existing boundary. The corrected Linux
isolated case passed (24.58-second case and 34.15-second driver interval before final cleanup).

## Partition design and complete inventory

The prior job ran 327 units and 20 integration classes sequentially on one runner. Four callers now
fan out directly from classification using one reusable workflow. Their original logical PostgreSQL
check fans in all four explicit results; the existing five-suite processing aggregate is unchanged.
There is no matrix result ambiguity, fail-fast sibling cancellation, serial partition dependency,
new classification flag or GitHub settings change. Missing/malformed/unsuccessful partition results
fail that fan-in; deliberately unselected callers and fan-in all skip together.

Each caller owns a clean runner/workspace and digest-pinned PostgreSQL 17.11. Pinned CPython 3.12.15,
PySpark 4.0.2, Boto3/Botocore 1.43.114, DuckDB 1.4.5 and JDK 21 are retained. Content ingestion/Gold
also own separate SeaweedFS 4.48 containers and prebuilt ingestion adapters. Core/import native
fixtures host their own synthetic Bronze HTTP server. Reliability/core need no RSS JAR or SeaweedFS.
Surefire units run once in core; Failsafe integration is enabled in every partition. The HTTP job's
unit coverage is unchanged. Every execution partition retains a 90-minute timeout.

Default `-Ppostgres` now discovers `IT*`, `*IT` and `*ITCase`; only the separately executed HTTP
`GoldApiIT` is excluded. The manifest guard checks every source against one disjoint assignment,
checks that exclusion against the HTTP profile, and rejects test-bearing sources outside standard
Maven naming. It runs in documentation even when backends are unselected, and before each partition's
Maven selection. Post-execution XML must contain the exact selected class set, real cases and no
skips/failures/errors. New integration classes cannot silently escape an old hard-coded include list.

| Partition | Class | Cases | Last measured hosted seconds | Timing source |
| --- | --- | ---: | ---: | --- |
| core | `platform.contentapi.execution.ImportOwnershipIT` | 42 | 23.28 | D4 PR72 40956221086 |
| core | `platform.contentapi.execution.PersistenceIT` | 28 | 17.98 | D4 PR72 40956221086 |
| core | `platform.contentapi.importing.ImportWorkflowIT` | 38 | 34.04 | D3 main 40937613392 |
| core | `platform.contentapi.importing.SilverImportIT` | 23 | 227.5 | D3 main 40937613392 |
| core | `platform.contentapi.management.ControlAdmissionIT` | 52 | 41.32 | D4 PR72 40956221086 |
| core | `platform.contentapi.management.RunHistoryIT` | 46 | 51.29 | D4 PR72 40956221086 |
| core | `platform.contentapi.management.RunSubmissionIT` | 44 | 22.38 | D3 main 40937613392 |
| core | `platform.contentapi.management.SourceManagementIT` | 36 | 16.67 | D3 main 40937613392 |
| core | `platform.contentapi.management.WorkerCoordinationIT` | 56 | 32.95 | D3 main 40937613392 |
| reliability | `platform.contentapi.execution.OperatorRecoveryIT` | 35 | 484.7 | D4 PR72 40956221086 |
| reliability | `platform.contentapi.execution.RecoveryFinalizationIT` | 53 | 255.4 | D4 PR72 40956221086 |
| reliability | `platform.contentapi.execution.RecoveryHandoffIT` | 118 | 188.3 | D4 PR72 40956221086 |
| reliability | `platform.contentapi.execution.RecoveryVerificationIT` | 40 | 168.5 | D4 PR72 40956221086 |
| reliability | `platform.contentapi.execution.ReliabilityExecutionIT` | 27 | 538.9 | D4 PR72 40956221086 |
| reliability | `platform.contentapi.execution.ReliabilityPreflightIT` | 41 | 61.52 | D4 PR72 40956221086 |
| reliability | `platform.contentapi.execution.ReliabilityWorkerIT` | 23 | 345.3 | D4 PR72 40956221086 |
| ingestion | `platform.contentapi.management.RssBronzeIT` | 43 | 176.0 | D4 PR72 40956221086 |
| ingestion | `platform.contentapi.management.RssFetchIT` | 51 | 70.32 | D4 PR72 40956221086 |
| ingestion | `platform.contentapi.management.RssSilverIT` | 32 | 425.4 | D3 main 40937613392 |
| gold | `platform.contentapi.management.RssGoldIT` | 29 | 2355.0 | D4 PR72 40956221086 |

These are mixed, explicitly attributed observations: current PR #72 completed suites where available,
otherwise successful merged D3 run `40937613392`. They are planning inputs, not one comparable benchmark.
The totals are core 467.41s, Reliability 2042.62s, D1-D3 671.72s and Gold 2355.00s. Current cancelled
Silver already ran longer than the successful D3 425.4s used in that planning total.

Three straight family groups would leave Content around 3026.72s (50.45 minutes) using those inputs;
current censored Silver makes its observed lower bound over 52 minutes. Isolating Gold gives an
illustrative critical path near 2355s (39.25 minutes), plus setup/queue/fan-in and the repaired test's
additional preparation. This is an expected structural benefit, not a measured revised hosted result.
Four explicit partitions keep those families reviewable and independently isolated. Larger dynamic
sharding, shared fixtures/Spark sessions, timeout increases and suppressed tests were rejected.
Additional runners cost setup/compute; none introduces another persistent service.

## Runtime analysis and bounded diagnostics

The 2355s Gold duration represents 29 isolated cases repeatedly creating original D1/D2/D3 evidence.
Thirty completed source fixtures each require a D3 publisher and independent Spark qualifier. Gold
preparation, publication, independent native verification and read-only committed inspection add
separate Spark-capable children. The two-source case additionally invokes a separate standalone
publisher to prove native equivalence. Removing those boundaries or caching management ownership
would weaken coverage; no such optimization is made.

`D4_TIMING` lines now measure fixture setup, D1-D3, preparation, publication, inspection and standalone
comparison with monotonic clocks. Gold child intervals span the existing before-authorization boundary
to supervisor return; child-start counters mean observed supervised Python children, not necessarily
completed JVM startups when deliberately interrupted. Publication child 1 is the publisher, child 2
its independent verifier, child 3 the non-Spark final recheck. Intervals overlap their enclosing phase
and must not be added to it. Incomplete child intervals are expected for injected launch/cleanup errors.
The lines contain only fixed labels, method names, durations and counts, never paths or payloads.

Reviewed 300-second child budgets, 180-second standalone budget, five-second gateway/cleanup bounds
and 60-second optional restart waits are ceilings, not unconditional sleeps. Hosted invocation supplies
no restart signal, so that optional branch contributes no forced 60-second sleep. This correction
preserves deadlines and all fault cases. Gold's guard/stale-owner/DB-outage injections are detected
at the existing heartbeat boundary; measured publication phases around 5.6 seconds reflect that
five-second interval, while immediate interrupt/uncertain-termination variants return sooner. Shortening
the interval solely for tests would change the exercised supervision behavior. Separately,
Reliability's existing stale-confirmation test intentionally waits about 61 seconds to prove expiry;
that is not a contributor to the 2355-second Gold class and remains covered. Low-risk harness changes remove runtime-file manipulation,
avoid duplicating the 327 unit tests across four new jobs, and avoid unused RSS/SeaweedFS provisioning
in core/Reliability. Fresh fixtures and real native verification remain unchanged.

### Measured local Gold phase costs

The instrumented Linux matrix passed 29 cases in 1113 seconds. The sum of its measured case bodies
was 1112.142 seconds; the small difference is runner/lifecycle overhead. These are separate from
hosted Gold's 2355 seconds: different machines, architectures and instrumentation prevent attributing
the difference to this correction or claiming a measured hosted speedup.

| Enclosing measured phase | Total seconds across 29 cases | Share of measured case time |
| --- | ---: | ---: |
| D1/D2/D3 prerequisites | 551.907 | 49.63% |
| Gold preparation | 273.343 | 24.58% |
| Gold publication/independent verification/final recheck | 234.343 | 21.07% |
| Additional committed-result inspection | 32.328 | 2.91% |
| Separate standalone equivalence publication | 11.465 | 1.03% |
| Fixture/catalog/database setup | 8.369 | 0.75% |

The 27 observed preparation children totalled 256.831 seconds between authorization and return.
Publication child intervals totalled 115.852 seconds for writers, 83.854 for independent verifiers
and 1.640 for non-Spark rechecks; 33 publication-stage Python child starts were observed, including
children intentionally interrupted before JVM startup. Those intervals are inside the phase totals.
They establish repeated expensive native boundaries, not an isolated measurement of JVM boot time
versus Spark computation. Setup alone was under one percent; sharing database/ownership fixtures
would save little while increasing interference risk. Retaining qualification and parallelizing
independent suites is the justified improvement.

## Reproduction and validation

With the pinned environment, JDK 21 and disposable PostgreSQL available:

```sh
python3 .github/ci/postgres_partitions.py
python3 -m unittest discover -s .github/ci/tests -v
# Existing local real-service D4 harness:
python3 scripts/test-rss-fetch-postgres.py --offline --gold --case launchFailureIsKnownCleanButStillOccupied
python3 scripts/test-rss-fetch-postgres.py --offline --gold
# CI-equivalent selected Maven integration (core runs all units):
selection=$(python3 .github/ci/postgres_partitions.py --partition core)
mvn -B -ntp -f services/content-api/pom.xml clean verify -Ppostgres \
  "-Dit.test=$selection" -DskipUnitTests=false
python3 .github/ci/postgres_partitions.py --partition core \
  --reports services/content-api/target/failsafe-reports
npm run check
```

Repeat the selected Maven command for reliability/ingestion/gold with `-DskipUnitTests=true`;
provide SeaweedFS and build the ingestion adapter for ingestion/gold. Use clean build reports in
local repeated invocations, as CI naturally does with independent fresh workspaces. Do not treat a
dry-run/discovery-only result as executed integration coverage. Exact executed local results follow below.

### Local validation environment correction

Linux validation uses the existing Debian 12 arm64 / Temurin JDK 21 / CPython 3.12.15 test image
with the same pinned processing libraries and disposable PostgreSQL/SeaweedFS digests. This is
Linux portability evidence, not a claim to have executed hosted Ubuntu 24.04 x86 CI.

The first Reliability run executed all 337 cases but had three permission-denial assertion failures:
`OperatorRecoveryIT.uncertainOrChangedEvidenceRetainsOccupiedRecovery[unavailableDependency]` and
both `RecoveryVerificationIT.deniedIoIsUnavailableEvidenceNotKnownCorruption` variants. The Docker
runner was UID 0 with DAC bypass; chmod-zero fixtures remained readable, so the expected I/O failure
was never induced. Direct OS evidence confirmed that removing `DAC_OVERRIDE` and `DAC_READ_SEARCH`
made `os.access` false for a chmod-zero file. With those capabilities removed, the unchanged four
operator variants and two verification variants all passed (74.19 seconds including driver cleanup).
The affected Reliability partition was rerun in that permission-equivalent environment and all 337 cases passed. No assertion,
fault coverage, application classification or production behavior is weakened. Hosted runners are
unprivileged; no CI configuration change is needed for this local Docker issue.

Linux driver intervals printed by the older helper stop before its `finally` cleanup; they are
labelled as such. The regression wrapper separately measures full process wall time including
container cleanup. Optional actual database restart signals are not supplied by this Linux driver,
as in the prior hosted command; those optional restart branches are not claimed as new evidence.
Existing D4 actual-restart evidence remains in the managed Content guide.

## Final local validation results

| Validation | Executed cases | Result | Measured seconds |
| --- | ---: | --- | ---: |
| Original Linux isolated D4 case | 1 + 43 safety units | Reproduced `NoSuchFileException`; 1 D4 error | 15.19 case / 23.10 driver before cleanup |
| Corrected Linux isolated D4 case | 1 + 43 safety units | Passed | 24.58 case / 34.15 driver before cleanup |
| Focused Linux D4 matrix | 29 + 43 safety units | Passed, no skips | 1113 class / 1124.63 driver before cleanup |
| Scoped regression: core | 365 integration + 327 units | Passed; exact 9-class report set | 363.18 including cleanup |
| Initial privileged Linux Reliability run | 337 | 3 ineffective permission-denial fixtures failed; no production change | 1022.94 including cleanup |
| Corrected permission-focused validation | 6 repeated Reliability cases | Passed after removing DAC bypass | 74.19 including cleanup |
| Corrected scoped regression: Reliability | 337 | Passed; exact 7-class report set | 980.02 including cleanup |
| Scoped regression: Content ingestion | 126 | Passed; exact 3-class report set | 424.20 including cleanup |
| Corrected macOS isolated D4 case | 1 + 43 repeated safety units | Passed, no skips | 39.18 case / 50.54 driver including cleanup |
| CI classifier, workflow, inventory and aggregate gates | 37 | Passed | 3.580 |

The broader regression ran each non-Gold partition with fresh disposable PostgreSQL, clean build
reports and the exact CI selections. Core/Reliability ran without SeaweedFS and with the cached
RSS ingestion target hidden, proving their provisioning independence. Ingestion rebuilt the actual
ingestion JAR/test helper and used isolated SeaweedFS; the Gold focus already exercised that native
stack. Only the affected Reliability partition was repeated after the local privilege correction;
core and Gold were retained, not rerun. No application/native-processing platform suite was repeatedly
launched while debugging. The Linux fixtures used base Python, macOS used the existing venv.

Actual Maven test discovery/report completeness was verified against the manifest for all four
partitions, then the union was checked: exactly 20 integration classes, 857 distinct integration
case IDs, 327 distinct unit/component case IDs, and no class overlap or skipped successful cases.
No dry-run, mock or source-only discovery result is presented as executed native qualification.

**Distinct final count: 1221** = 857 integration + 327 unit/component + 37 CI policy/gate cases.
The 29-case Gold matrix includes the repaired isolated case. The 43 safety-unit subset, six
permission variants, original reproducer and macOS rerun overlap those sets and are not added again.
The 37 CI tests retain the original 32768 global selection/result combinations and add all 256
PostgreSQL partition-result combinations, plus missing/cancelled/invalid results, safe-skip wiring,
new/missing/duplicate source classes and exact actual-report verification. Combinations/assertions
inside a case are not counted as separate test cases.

Individual successful integration suite durations (Linux):

| Partition | Suite | Cases | Seconds |
| --- | --- | ---: | ---: |
| core | `ImportOwnershipIT` | 42 | 8.189 |
| core | `PersistenceIT` | 28 | 9.970 |
| core | `ImportWorkflowIT` | 38 | 32.923 |
| core | `SilverImportIT` | 23 | 183.471 |
| core | `ControlAdmissionIT` | 52 | 16.253 |
| core | `RunHistoryIT` | 46 | 35.582 |
| core | `RunSubmissionIT` | 44 | 16.247 |
| core | `SourceManagementIT` | 36 | 14.088 |
| core | `WorkerCoordinationIT` | 56 | 20.610 |
| gold | `RssGoldIT` | 29 | 1112.944 |
| ingestion | `RssBronzeIT` | 43 | 69.958 |
| ingestion | `RssFetchIT` | 51 | 28.995 |
| ingestion | `RssSilverIT` | 32 | 310.747 |
| reliability | `OperatorRecoveryIT` | 35 | 269.217 |
| reliability | `RecoveryFinalizationIT` | 53 | 108.293 |
| reliability | `RecoveryHandoffIT` | 118 | 66.260 |
| reliability | `RecoveryVerificationIT` | 40 | 67.155 |
| reliability | `ReliabilityExecutionIT` | 27 | 261.679 |
| reliability | `ReliabilityPreflightIT` | 41 | 23.780 |
| reliability | `ReliabilityWorkerIT` | 23 | 175.907 |

The real two-source native acceptance still emitted:

```text
input_set_fingerprint: 5151d38f7b601d2f04bcda04d5f2592ce9fdef1934e0f8c88d3ff37dbbaad954
complete.json SHA-256: 497f969131b679e754675082ece3ca90350b1cd1bdf46132e87aa60ca023c784
accepted/rejected:      4 / 2
Gold articles/daily:    2 / 2
```

Source identity, receipts, native standalone equivalence, independent verification, immutable replay,
occupied ownership and exclusion of Content enrichment/activation/dispatch remain asserted. The
additional diagnostics contain only bounded timings and counters. All data/services were disposable.

Both workflow files pass actionlint. Markdown/local links, repository hygiene and whitespace checks
pass. Classification of the complete 12-file change selects all five logical backend suites; its
PostgreSQL flag fans out all four execution partitions. The original required contexts, trust/fallback,
full-main validation and safe skips remain. No GitHub settings, workflow dispatch or issue changes occur.

Hosted Ubuntu 24.04 x86 must still validate reusable-workflow invocation, parallel service provisioning,
real per-partition timings/90-minute headroom, failure/cancellation propagation and representative
selected/unselected runs. The local permission adjustment is not a production or hosted configuration
change. No hosted speedup or revised hosted green result is claimed before the owner submits the change.

## Exact changes

| File | Purpose |
| --- | --- |
| [.github/ci/postgres-partitions.json](../../.github/ci/postgres-partitions.json) | Disjoint 20-class PostgreSQL inventory. |
| [.github/ci/postgres_partitions.py](../../.github/ci/postgres_partitions.py) | Source/naming/default-discovery guard, safe selectors and exact successful report checks. |
| [.github/ci/tests/test_policy.py](../../.github/ci/tests/test_policy.py) | Retain the five-suite classifier/gate tests and adapt workflow wiring assertions. |
| [.github/ci/tests/test_postgres_partitions.py](../../.github/ci/tests/test_postgres_partitions.py) | Partition result truth table, actual shell gate and source/report completeness tests. |
| [.github/workflows/ci.yml](../../.github/workflows/ci.yml) | Four independent callers and original-name PostgreSQL fan-in; original global gate unchanged. |
| [.github/workflows/postgres-partition.yml](../../.github/workflows/postgres-partition.yml) | Reusable isolated pinned provisioning, real selected tests and report verification. |
| [docs/operations/ci.md](../../docs/operations/ci.md) | Current graph, provisioning, selection and safe-skip contract. |
| [docs/operations/managed-rss-fetch.md](../../docs/operations/managed-rss-fetch.md) | D4 hosted follow-up; historical evidence retained. |
| [docs/operations/pr72-postgres-partitions.md](../../docs/operations/pr72-postgres-partitions.md) | Root cause, alternatives, inventory, measurements, commands and limits. |
| [services/content-api/pom.xml](../../services/content-api/pom.xml) | Comprehensive integration discovery and default-false Surefire-only skip property. |
| [services/content-api/src/test/java/platform/contentapi/execution/RssGoldTimings.java](../../services/content-api/src/test/java/platform/contentapi/execution/RssGoldTimings.java) | Bounded test-only phase/child timing diagnostics. |
| [services/content-api/src/test/java/platform/contentapi/management/RssGoldIT.java](../../services/content-api/src/test/java/platform/contentapi/management/RssGoldIT.java) | Portable before-start fault and unchanged native acceptance assertions, plus timings. |

No production application/processing/ingestion files, native dataset schemas, accepted ADRs or migrations change.
