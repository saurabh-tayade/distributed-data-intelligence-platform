# E5-F2 read-only Operations: local acceptance

**2026-11-01 — overall BLOCKED on representative management data.** Real browser and HTTP
acceptance passed for the empty/error/navigation subset. This does not establish populated
Source/Run inspection, successful detail ETags, historical paging or active-run polling.
Do not sign off the full F2 acceptance gate or advance to F3.1 on this evidence alone.

Baseline: merged `main`, `564900ab24e2a4ae590cc236df168905d8f4b7f9`. No application,
dependency, proxy, security or migration source files changed. The historical
[F1 audit](e5-f1-operations-ui-audit.md) remains unchanged.

## Acceptance layers

| Layer | Status | Observed evidence and limits |
| --- | --- | --- |
| Frontend automated tests | PASS | 571 tests / 19 files and TypeScript passed again on this baseline. These mocked tests do not substitute for live populated acceptance. |
| Real management HTTP transport/envelopes | PASS | 23 scripted GET probes through Spring/PostgreSQL and Vite, plus a real 503 outage probe. Empty list envelopes, limits, correlation IDs, fixed failures and boundary rejection verified. |
| Populated management HTTP contracts | BLOCKED | No sources, revisions, runs, attempts, events, controls or artifacts were available. No successful detail response or ETag was observed. |
| Real browser interaction | PASS | Native Chrome exercised empty/filtered-empty lists, source/run 404 panels, refresh, real 503 and successful retry. This is deliberately limited acceptance. |
| Representative data coverage | BLOCKED | Existing management databases were empty, old V1–V5 installations. Only Analytics had a supported populated synthetic fixture. |
| Navigation | PASS | Analytics/Operations switching, Sources/Executions navigation, real Back/Forward, execution-filter restoration and preserved Analytics publisher/publication selection. |
| Active/terminal/operator-state polling | NOT EXERCISED | No persisted runs in any required state. A missing run remained at its initial 404 request count over the observation interval; this is not active-run polling proof. |
| Read-only safety | PASS | All 55 recorded API requests used GET. Before/after management counts and coordinator state matched. No source/run/recovery command was submitted. |

## Environment and data

macOS ARM64; Java 21.0.7, Spring Boot 4.1.1, PostgreSQL 17.11, Node 24.22.1,
Vite 8.3.4 and Chrome 156.0.8078.74. Python/DuckDB used the existing pinned Python 3.12
environment and DuckDB 1.4.5; no Spark processing or dependency installation was performed.

Docker Desktop was initially stopped. The existing dedicated `ddip-b1-postgres-test`
container uses the pinned PostgreSQL image from [Compose](../../infrastructure/local/compose.yaml),
loopback port 15432 and its retained test volume. Its two existing databases, `ddip_test`
and `ddip_b1_restart_final`, both had V1–V5, no representative records, and coordinator
STOPPING / rowVersion 1. Neither database was upgraded, repaired, seeded or otherwise altered.
No documented retained Operations seed/dump was found. SQL lifecycle fixtures in integration
tests were not repurposed to fabricate operational states.

A new disposable database, `ddip_e5_f2_acceptance_20261101`, was created and initialized using
the documented `migrate` role, then validated by the `api` role against V1–V9. A fresh
installation UUID belonged only to this new database; existing installation bindings were
preserved. Its coordinator remained READY / version 0, with all seven inspected record
counts at zero before and after acceptance.

API: `http://127.0.0.1:8080`, `DDIP_ROLE=api`. Explicit approved origins were exactly
`http://127.0.0.1:8080,http://127.0.0.1:5173`. Vite ran at `http://127.0.0.1:5173` with the
unchanged relative `/api/` proxy and `changeOrigin:false`. No CORS or boundary bypass was used.
Credentials stayed in the child environment; the evidence configuration omits passwords and
cursor signing keys. Tomcat's temporary access log recorded method/path/status/duration.

Analytics reused the existing supported **synthetic native Gold fixture** under
`services/api/target/fixtures/gold's snapshots`, pinned to
`1d3c96d8e3dfc3bae6b611684dfbf5767deb769a8e8a94f9329a96c46e129c1b`.
It returned two publisher rows, three daily rows, two quality rows and three articles. This
is real API/Python/DuckDB access to synthetic fixture data, not a completed managed execution
or evidence of Content dispatch activation.

## Real HTTP observations

Management paths below start with `/api/management/v1`. Raw JSON responses and headers are
retained locally in the ignored evidence directory, not duplicated as large specifications.

| Request | Actual result |
| --- | --- |
| GET `/sources`, direct API and Vite proxy | 200; `apiVersion:1`, `data:[]`, meta correlation UUID, limit 20, `hasMore:false`, `nextCursor:null`; JSON and `Cache-Control:no-store`. |
| GET `/sources` with product/enabled filters and limit 100 | 200, valid empty page; no positive filter-membership claim. |
| GET `/sources/content-intelligence/acceptance-missing` and `/revisions?limit=20` | 404 `NOT_FOUND`; separate correlation IDs; no ETag. |
| GET `/runs`, direct API and proxy; product/source/state/limit filters | 200, valid empty pages; no populated ordering or provenance claim. |
| GET `/runs/00000000-0000-0000-0000-000000000001`, `/attempts`, `/events`, `/controls/00000000-0000-0000-0000-000000000002` | 404 `NOT_FOUND`; no successful record/receipt or ETag. These UUIDs identify missing-record probes only. |
| GET list with limit 101, duplicate limit, or malformed source/run cursor | 400 `INVALID_ARGUMENT`. Expiry and Next/Previous UI require real cursors and remain unexercised. |
| GET `/sources` with an unapproved loopback Host or Origin | 403 `LOCAL_BOUNDARY_REJECTED`; exact boundary remained enabled. |
| GET `/sources` with the test database stopped | 503 `MANAGEMENT_UNAVAILABLE`, `retryable:true`, safe correlation ID. Restart restored successful reads. |
| GET `/api/v1/publishers`, `/daily`, `/quality`, `/articles` through Vite | 200; populated synthetic rows and the exact pinned publication identity. |

The final access log contained 55 GETs: 31 responses at 200, 16 at 404, four at 400,
two at 403 and two at 503. Expected negative probes are not application failures.

## Browser observations

The browser connector reported an unavailable access host; native `cua_repl` Chrome control
provided real interaction and screenshots. A transient ScreenCaptureKit/input issue was
recovered by rebinding Chrome and dismissing the transient UI; the outage/retry scenario was
then actually completed. No browser workflow was inferred from curl alone.

Observed in Chrome:

- Sources initially showed **No registered sources**. Content Intelligence + Enabled filters
  showed **No sources match these filters**; Refresh completed against the actual API.
- Executions initially showed **No executions returned**. Application Reliability, exact
  source `acceptance-missing` and FAILED filters produced the filtered-empty state and matching
  hash query. Back restored unfiltered values; Forward restored all three applied filters.
- Analytics loaded populated synthetic publisher metrics. Selecting `beta`, switching through
  Operations and returning preserved both `beta` and the pinned publication fingerprint.
- Direct missing-source entry showed independent detail/revision 404 panels. Direct
  missing-run entry showed separate run/attempt/event 404 panels; no fabricated empty record
  or control receipt appeared. Navigation replaced the previous area's content.
- A controlled stop of the test PostgreSQL service caused Sources to replace the empty page
  with **Unable to load sources**, `MANAGEMENT_UNAVAILABLE`, a reference and retry guidance.
  After restart, **Try again** restored the successful filtered-empty page.

Screenshots of filtered-empty Sources, missing-run errors and the real Sources 503 state
were emitted in the acceptance conversation. No screenshot binaries were added to Git.

The inspected pre-fault Console showed no uncaught JavaScript exception, but Recharts logged
its zero-width/height warning, grouped as repeated ten times during workspace switching.
The visible Analytics chart and table recovered on return. This is a non-fatal diagnostic
issue for a separate narrow follow-up; this pass did not establish a data/rendering failure.
Investigate hidden Analytics chart layout without changing publication/filter preservation.
Do not describe this as a clean-console pass. No fix was implemented here.

## Commands, evidence and cleanup

Commands used for setup and validation, from the repository root:

```sh
docker desktop start
docker start ddip-b1-postgres-test
# Read-only psql inventories/record counts through docker exec; no retained DB migration.
JAVA_HOME=$(/usr/libexec/java_home -v 21) mvn -o -B -ntp \
  -f services/content-api/pom.xml -Dmaven.test.skip=true package
python3 outputs/e5-f2-acceptance-20261101/serve-api.py
npm run dev --prefix frontend -- --host 127.0.0.1
python3 outputs/e5-f2-acceptance-20261101/http-probe.py
npm test --prefix frontend
npm run typecheck --prefix frontend
npm run check
git diff --check
```

Node commands used the existing supported Node 24 executable on PATH. Packaging succeeded
after granting access to Maven's existing cache lock; no application workaround was made.
The two Python helpers retain the exact ephemeral setup and probe requests. Other local
evidence includes `environment.json`, `http-results.json`, `http-unavailable.json`,
`browser-observations.json`, API/Vite/migration logs, `read-only-http-summary.json`,
`database-before.json`, `database-after.json`, `cleanup.json`, and frontend test/typecheck logs
under ignored `outputs/e5-f2-acceptance-20261101/`. The package log is
`outputs/e5-f2-acceptance-build.log`. Lint/build were not rerun for this documentation-only
acceptance change; the API was packaged with tests explicitly skipped. No backend integration
or processing regression suite was run.

The acceptance tab, API and Vite processes were closed. Only the task-created database was
dropped, after an equal before/after state comparison. Both retained databases were checked
unchanged at V1–V5, STOPPING / version 1 and zero source/run/event counts. The original test
container and Docker Desktop were returned to their initial stopped state; volumes remain.

## Smallest follow-up and remaining browser checklist

Provide a dedicated disposable copy of a **populated, compatible management installation**
with its matching installation configuration and provenance for retained test records. It
must contain both products, source revisions, historical runs/attempts/events and a suitable
control; identify any genuine completed result separately from synthetic metadata. Preparing
new sources/runs or operational-state fixtures needs separate authorization, not hidden SQL
seeding in this acceptance pass. No backend redesign is indicated by the observed subset.

Then repeat read-only browser acceptance for:

1. Populated source filtering, current detail versus immutable historical configuration and
   multi-page revision traversal; verify successful source ETags over HTTP.
2. Run ordering/filter membership, captured revisions/input identities, verified-at-execution
   result metadata, independent attempt/event pages and expired-cursor restart.
3. REQUESTED versus APPLIED/TOO_LATE/REJECTED receipts and separate run/control ETags.
4. Visible five-second active polling, hidden-tab suspension, stale/backoff handling and
   terminal/PAUSED/RECOVERY_REQUIRED manual behavior, without submitting recovery controls.
5. Console diagnostics, keyboard/responsive behavior and unchanged database/audit state after
   navigation. Keep all acceptance requests read-only.

Proceed to F3.1 only after these positive-data workflows pass and the warning follow-up is
triaged. Empty/error success alone does not close E5-F2 acceptance.
