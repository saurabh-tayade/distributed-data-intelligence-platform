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
no committed credentials or AWS resources. The launcher ignores inherited DDIP,
Analytics, Spring/server and JVM option configuration; Java loads only the packaged
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
   to its full publication fingerprint. Reliability output is never activated as Gold.
4. Start PostgreSQL 17.11 using the repository's pinned local image. Apply unchanged
   migrations through the packaged `migrate` role. Provision only immutable storage
   bindings, run/runtime/import profiles and their associations through trusted SQL.
   These references have no public provisioning API.
5. Start API mode with exact API/UI loopback origins and the private source catalog.
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
    run through that proxy and read the separate Gold publisher report. Print READY.

Expected tiny synthetic Reliability results are **15 occurrences, 10 valid, 5 rejected,
4 unique events and 3 five-minute windows**. CHECK_INPUTS, PUBLISH_RELIABILITY and
VERIFY_RESULT must all be VERIFIED. The final publication is native PySpark output,
followed by the worker's independent verification and atomic finalization. The wrapper's
additional native read is a separate acceptance observation; HTTP's
`VERIFIED_AT_EXECUTION` remains historical evidence, not continuous requalification.

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
observations; `native-verification.json` contains the additional native read result.
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

HTTP acceptance is automated; browser acceptance is a separate observation. Once READY:

1. Open Analytics at the printed UI URL. Confirm Publisher Overview, Daily Activity,
   Data Quality and Article Explorer load the pinned **synthetic Gold** publication.
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
   unchanged Analytics selection. With only three stages, default stage paging has
   no next page; broader pagination/race coverage remains in the existing automated tests.
7. Run **status**, then **down**. Confirm ports close, the labelled container/volume is
   absent and the private demo directory is removed. Launch again to confirm a new
   installation is created rather than adopting retained state.

## Focused checks

```sh
python3 -m unittest discover -s scripts/tests -p test_local_operations_demo.py -v
python3 -m unittest discover -s .github/ci/tests -v
npm run check
git diff --check
```

The safety tests mock external tools to check ownership, refusal, missing resources,
inherited configuration and HTTP headers. They do not replace the real startup/native
acceptance above. No new production dependencies, schemas, endpoints, frontend features
or processing/recovery changes are part of this workflow.

## Local acceptance — 2026-11-02

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
