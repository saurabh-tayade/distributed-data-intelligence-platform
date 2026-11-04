# Reproducible local Analytics and Operations demo

This is a disposable development demonstration, not a new execution/deployment
capability or a benchmark. It runs the packaged API, production operator importer,
actual Reliability worker and built React application. No successful artifacts,
runs, attempts, stages or audit events are inserted by setup SQL.

## Prerequisites

Run on a local POSIX host with Docker running through a **local Unix socket**,
Java 21, Maven, Node.js 24 (at least 24.15), npm and Python 3.9+ for the wrapper.
Native fixture/import/execution uses an existing **Python 3.12.15, PySpark 4.0.2,
DuckDB 1.4.5** environment. Use the existing
[Python setup instructions](../architecture/silver-processing.md#contract-correction-acceptance-and-python-setup)
and pinned `processing/requirements*.txt` files. The launcher checks these versions;
it does not install or upgrade runtimes or dependencies.

Install the existing frontend lockfile dependencies once with
`npm ci --prefix frontend`. Maven dependencies must be available normally, or in
the cache when using `--offline`. Allow several GiB of free local disk/memory for
Spark; production readiness requires at least 1 GiB free on each writer volume.

From the repository root, set the actual installed paths:

```sh
# macOS example; Linux users select their installed Java 21 home instead.
export JAVA_HOME=$(/usr/libexec/java_home -v 21)
export DDIP_DEMO_PYTHON="$PWD/outputs/silver-python312/.venv/bin/python"
# Select an installed Node 24 on PATH, then check it:
node --version
"$DDIP_DEMO_PYTHON" -c 'import sys,pyspark,duckdb; print(sys.version.split()[0],pyspark.__version__,duckdb.__version__)'
```

The example Python path is the repository's established ignored local environment;
another installed environment with the exact versions is supported. `DDIP_TEST_PYTHON`
is also accepted when `DDIP_DEMO_PYTHON` is unset. No machine-specific path is committed.

## Start, inspect and stop

```sh
# Terminal 1: stays in the foreground after READY.
python3 scripts/local-operations-demo.py up --offline

# Terminal 2: explicit read-only status.
python3 scripts/local-operations-demo.py status

# Explicit shutdown AND deletion of this disposable installation.
python3 scripts/local-operations-demo.py down
```

Omit `--offline` if Maven needs to download its existing pinned dependencies. The
launcher builds the packaged API and frontend before starting services. Default URLs:

- Analytics: `http://127.0.0.1:14173/#/analytics`
- Reliability: `http://127.0.0.1:14173/#/analytics/reliability`
- Sources: `http://127.0.0.1:14173/#/operations/sources`
- Executions: `http://127.0.0.1:14173/#/operations/executions`
- API: `http://127.0.0.1:18080`

The launcher prints the exact successful Run Inspection URL after acceptance. Use
`--api-port 18081 --ui-port 14174` if the defaults are occupied; both ports must be
free and distinct. Existing listeners are never stopped or adopted. PostgreSQL uses
a new Docker-assigned loopback port, random password, unique labelled container and
anonymous disposable volume.

**Ctrl-C** stops the demo's owned processes/container but retains private files and
logs for inspection. Run **down** afterward to remove those files before a fresh
**up**. Repeated **down** is harmless. An existing demo directory causes **up** to
refuse, including after failed initialization; it is never silently overwritten.

All state, logs, credentials-bearing configuration and new native data are private
under ignored `local-data/operations-demo/`. Do not share that directory. There are
no committed credentials or AWS resources. The launcher ignores inherited `DDIP_`,
`ANALYTICS_`, `RELIABILITY_ANALYTICS_`, `SPRING_`, `SERVER_` and JVM option configuration;
Java loads only the packaged
application configuration plus this demo's explicit environment/arguments. It never
uses the retained development database or existing native publication roots.

## What startup actually does

1. Check runtime versions, Docker, frontend dependencies and free API/UI ports; build.
2. Reuse `processing/tests/reliability_preflight_fixtures.py:create` with JSONL,
   `multi=True, publication=True`. This registers the established ten-row G1 batch
   and five-row second batch into new native input directories, with the established
   service reference. Fixture timestamps and the synthetic `111…111` deployment
   revision retain their fixture meaning; they are not a production build identity.
3. Generate the separate, documented M2.4 synthetic Gold publications using
   `processing/tests/gold_fixtures.py:create_analytics_publications`. Pin Analytics
   to its full publication fingerprint. Also generate a separate JSONL Reliability
   analytics publication using `processing/tests/reliability_format_fixtures.py:completed`,
   the existing literal G1 verifier fixture. Fully reopen it with the native reader
   before saving its selection and expected request-window reports. Pin all Reliability
   analytics roots and the exact fingerprint at API startup. Neither selection comes
   from an Operations run result, a directory scan, inherited settings or a latest pointer.
4. Start PostgreSQL 17.11 using the repository's pinned local image. Apply unchanged
   migrations through the packaged `migrate` role. Provision only immutable storage
   bindings, run/runtime/import profiles and their associations through trusted SQL.
   These references have no public provisioning API.
5. Start API mode with exact API/UI loopback origins and the private source catalog.
   Check seven bounded Reliability request-window scenarios against the native reports
   through direct HTTP, including exact identity/reference/metrics, filters, pagination,
   empty results, pre-epoch UTC and `no-store` caching.
   Register the source through `POST /api/management/v1/sources`.
6. Invoke the packaged `operator-import` role with the real bounded import catalog.
   Require a fresh VERIFIED import. Discover the run profile, two compatible imported
   batch UUIDs and service-reference UUID through the production HTTP routes.
7. Start the actual Reliability worker and wait for its persisted READY registration.
   Admit the supported run through `POST /api/management/v1/runs`, with real Origin,
   local-request, content-type and idempotency headers. No ownership locks or worker
   APIs are bypassed.
8. Wait for SUCCEEDED, inspect attempts/stages/audit/result/counts and verify coordinator
   release. Independently reopen the native publication through its existing full
   DuckDB reader, query table counts and assert native file hashes/mtimes are unchanged.
9. Read the source and its genuine immutable revision-1 history, leaving admission
   enabled. Keep revision 1 current so the fixed revision-1 inputs remain compatible
   with the existing submission UI; no extra historical revision is fabricated.
10. Start Vite preview with the existing proxy, preserving Host. Check the successful
    run through that proxy and read the separate Gold publisher report. Repeat the same
    Reliability scenarios through the proxy and require unchanged publication identity
    and responses after managed execution. Print READY and the Reliability view/UTC interval.

Expected tiny synthetic Reliability results are **15 occurrences, 10 valid, 5 rejected,
4 unique events and 3 five-minute windows**. CHECK_INPUTS, PUBLISH_RELIABILITY and
VERIFY_RESULT must all be VERIFIED. The final publication is native PySpark output,
followed by the worker's independent verification and atomic finalization. The wrapper's
additional native read is a separate acceptance observation; HTTP's
`VERIFIED_AT_EXECUTION` remains historical evidence, not continuous requalification.

The separate **Reliability analytics fixture** has **10 occurrences, 5 valid, 5 rejected,
4 unique events and 3 windows**. It is a literal verifier fixture, not another Spark run
or a claim that the worker's 15-occurrence output has been activated. Its October interval
contains two windows; the third is deliberately pre-epoch. Every API read fully verifies
the selected publication and its retained raw/reference dependencies. Missing or corrupt
artifacts fail closed, without substituting a successful run or another publication.
The launcher does not raise the existing process, timeout, response or pagination limits.

The fixture uses native local files, so SeaweedFS is not needed. The existing
[Compose file](../../infrastructure/local/compose.yaml) explicitly names its retained
PostgreSQL volume; changing only its project name would still risk sharing that
volume. This wrapper follows the isolated-container convention of the existing
[PostgreSQL acceptance script](../../scripts/test-recovery-postgres.py), reusing the
same pinned image without modifying Compose or creating a parallel production stack.

## Status, errors and safe cleanup

`status` distinguishes initialization phases from READY, reports the actual run
state when reachable, and reports supervisor/container presence. `state.json` records
cumulative wall-clock phase timings. `acceptance.json` contains successful public HTTP
observations, including Reliability report parity through the UI proxy;
`native-verification.json` contains the managed run's additional native read result.
`prepared.json` holds the fixed, separate Gold and Reliability selections.
`reliability-analytics-verification.json` contains the selected Reliability roots/fingerprint,
native report expectations and unchanged-file count for its output and dependencies.
Private per-command logs include build, fixtures, migrate, API, importer, worker,
frontend and independent verification.

A failed prerequisite creates no installation. A failure after initialization records
its phase and stops owned services; inspect logs before **down**. A worker FAILED or
RECOVERY_REQUIRED result is an actual execution failure, never converted into success
or automatically recovered. There is no retry/recovery shortcut.

Cleanup signals only subprocesses launched by this invocation. External **down**
validates the saved supervisor PID/start-time/command before signalling it. It checks
recorded processes and process groups, and checks exact container identity plus the
random ownership label before deletion. Worker shutdown gets up to 60 seconds; **down**
waits up to 90 seconds. Unconfirmed process termination, a reused PID, a remaining
process group, a foreign container label or an unavailable Docker daemon causes
cleanup to **preserve infrastructure/data and report the condition**. It does not
kill arbitrary PIDs, force-clear coordinator ownership or delete unverified resources.
Resolve that local condition explicitly before rerunning **down**. Do not manually
remove native files while a demo worker/child may still use them.

## Owner browser acceptance checklist

HTTP acceptance is automated; browser acceptance is a separate observation. The owner
has deferred the walkthrough until the entire UI is in place. The following is a checklist,
not new browser acceptance evidence. Once READY:

1. Open Analytics at the printed UI URL. Confirm Publisher Overview, Daily Activity,
   Data Quality and Article Explorer load the pinned **synthetic Gold** publication.
   Open **Application Reliability** at `#/analytics/reliability`. Enter UTC start
   `2026-10-01T00:00:00.000000Z` and end `2026-10-01T00:10:00.000000Z`, then apply the
   filters. Expect `svc00` and `svc01` windows with request counts **2** and **1**.
   Filter to `svc00` / `test` for one row, or `absent-service` for an empty result.
   For the missing-reference window use start `1969-12-31T23:55:00.000000Z`, end
   `1970-01-01T00:00:00.000000Z`, service `svc09`, environment `production`.
   The label remains **synthetic**, including that environment. Check publication and
   reference identities, nullable reference fields and explicitly approximate p95.
2. Open Operations → Sources. Inspect `fixture-jsonl`, its enabled state, current
   revision 1 and immutable revision-1 record. This initial fixture has one revision;
   editing it would deliberately make the fixed revision-1 batches undiscoverable
   for a newer current revision.
3. Open Execution History and the successful Reliability run. Confirm synthetic origin,
   captured source revision 1, three imported references, counts and historical result.
4. In Attempts choose **Inspect stages**. Confirm one genuine successful EXECUTE attempt
   and the three VERIFIED named stages, UTC times and checkpoint identities. Preserve
   stage-name/item order; do not interpret it as a guaranteed chronological timeline.
5. Inspect audit events including RUN_SUCCEEDED. Refresh run, attempts, stages and events
   independently. No latest control receipt is expected: this demo does not issue controls.
6. Check keyboard selection/focus, narrow-screen scrolling, browser Back/Forward and
   unchanged separate Content and Reliability selections. Navigating to Operations or
   completing a new run must not select another analytics publication. Reloading the
   dashboard resets its session pin, but does not change the API's configured selection.
   With only three stages, default stage paging has
   no next page; broader pagination/race coverage remains in the existing automated tests.
7. Run **status**, then **down**. Confirm ports close, the labelled container/volume is
   absent and the private demo directory is removed. Launch again to confirm a new
   installation is created rather than adopting retained state.

## Focused checks

```sh
python3 -m unittest discover -s scripts/tests -p test_local_operations_demo.py -v
# Spark-free native demo checks, with the pinned DuckDB analytics dependencies:
"$DDIP_DEMO_PYTHON" -m unittest discover -s scripts/tests -p test_demo_reliability_native.py -v
python3 -m unittest discover -s .github/ci/tests -v
npm run check
git diff --check
```

The safety tests mock external tools to check ownership, refusal, missing resources,
inherited configuration, explicit selection, response mismatches and HTTP headers. The
native tests exercise the real selected fixture, read-only reports and missing/corrupt
publications/dependencies. Run them with the pinned DuckDB 1.4.5 environment;
missing dependencies fail rather than silently skipping acceptance. Neither replaces full startup/native
acceptance above. No new production dependencies, schemas, endpoints, frontend features
or processing/recovery changes are part of this workflow.

## Reliability analytics slice validation — 2026-11-04

On Linux x86_64, local checks used Java 21.0.13, Maven 3.9.11, Node 24.22.1 and a
Python 3.12.14 analytics-only environment with DuckDB 1.4.5. That Python environment
does **not** meet the full demo's pinned Python 3.12.15/PySpark 4.0.2 prerequisite.

- **21 launcher safety tests** and **3 native demo tests** passed, including five
  wrong-identity, missing-publication and damaged-dependency cases. CI now runs the
  safety suite in the documentation job and the native suite in the existing pinned
  analytics job.
- The packaged application in database-free `analytics` role and built Vite preview
  passed all **7 selected-fixture report scenarios directly and through the proxy**.
  Four forbidden publication/path overrides or out-of-bounds requests returned 400.
  Removing the selected completion marker returned only the safe generic 500 code
  through both routes. Restoring it recovered the same selection and all seven reports.
  All **16 fixture/evidence files** retained their hashes and mtimes, and the test's
  API/frontend processes were stopped. This is HTTP acceptance, not browser interaction.
- **406 backend unit/web tests**, **16 existing Reliability HTTP/native integration
  tests** (34 JSONL/Parquet scenarios and 12 damaged cases), **15 native verifier tests**,
  **1,170 frontend tests** and **45 CI-policy tests** passed. Typecheck, lint, both
  production builds, Markdown/local-link checks, hygiene and whitespace checks passed.
  The existing frontend main-bundle warning remains (754.60 kB minified / 216.91 kB gzip).

Docker and the pinned Spark environment were unavailable, so the complete managed
demo startup/restart/teardown was **not rerun** for this slice. Full Gold/Spark and
PostgreSQL regression profiles and hosted CI were not run locally. Browser walkthrough
and personal UI refinement remain deferred. The historical full-demo evidence below
does not remove these limits, and this slice does not complete every #53 criterion.

## Local acceptance — 2026-11-02

This historical full-demo evidence predates the separate Reliability analytics view.
It does not qualify the new analytics configuration or constitute a new browser review.

Final validation used macOS arm64, Java 21.0.7, Node 24.22.1, the pinned native Python
environment and the pinned PostgreSQL 17.11 Bookworm image on local Docker Desktop.
These are tiny local fixture observations, not production-scale performance claims.

- The final owner-ready installation reached **READY in 73.93 seconds**, including
  API/frontend builds, fresh native and Gold fixture preparation, PostgreSQL startup,
  migration, provisioning, import, execution and HTTP/proxy checks. The interval from
  admitted execution to observed success was **20.03 seconds**. Earlier completed
  disposable launches reached READY in 73.66, 76.91 and 74.71 seconds; clean teardown
  and a new installation were exercised between launches.
- The final run reached SUCCEEDED through the packaged worker. Counts were
  **15 occurrences / 10 accepted / 5 rejected / 4 events / 3 windows**. HTTP returned
  one successful attempt, the three expected VERIFIED stages and **18 audit events**.
  Coordinator execution/import pointers were empty in READY mode after finalization.
- A separate full native reader reconciled the output and raw dependencies, queried
  table counts and left **all 24 native output files' hashes and mtimes unchanged**.
  Fresh installations produced distinct run UUIDs and the same fixture fingerprint,
  `25a14dd8184d5375394f2cb54e7e99c8aef11daac9a4dc9745b52797e476660d`.
- Actual `frontend/src/managementApi.ts` and `frontend/src/api.ts`, transpiled with the
  installed TypeScript compiler, successfully parsed live responses through the Vite
  proxy: source/revision, execution profile, two compatible batches, service reference,
  execution list/detail, attempt, stages and events. All four Analytics clients passed:
  **2 publisher rows, 3 daily rows, 2 quality rows and 3 articles** from separate Gold.
  The allowed UI Origin returned 200; an unapproved Origin returned 403.
- **16 focused launcher safety tests**, **40 CI policy tests**, Python compilation,
  both production builds, Markdown/relative-link checks, hygiene and whitespace checks
  passed. No new dependencies or PostgreSQL integration-test classes were introduced;
  the exact CI partition inventory and V1–V9 bytes are unchanged.
- Final **down**, repeated **down** and absent **status** passed. Demo ports closed;
  its processes, container/anonymous volume and private directory were removed.
  **40 retained Gold fixture files** retained hashes, sizes and mtimes, and **3 unrelated
  containers** retained their state/mounts across final rerun/teardown verification.

Development smoke checks corrected two contract assumptions: Analytics configuration
requires the fingerprint rather than the fixture's root-relative publication path,
and source revision creation returns HTTP 200. The final fixture intentionally keeps
source revision 1 current so compatible batches remain discoverable. No backend
contracts were changed to accommodate the wrapper.

Browser inventory returned **browser access host unavailable** and a native pipe
startup failure. No automated browser interaction or visual/screen-reader acceptance
is claimed, and no automation repair was attempted. Use the owner checklist above.
The existing frontend chunk-size warning remains. Full Spark/recovery/PostgreSQL
regression matrices and hosted CI were not run for this demo-only tooling change.
Ignored local acceptance summaries are under `outputs/local-operations-demo/`.
