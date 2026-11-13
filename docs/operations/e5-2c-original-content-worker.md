# E5.2-C: Original-only managed Content worker integration

Owner authorization: [#49, November 13, 2026](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/issues/49#issuecomment-6856923409).
Decision record: [ADR 0050](../adr/0050-original-only-content-worker-lifecycle.md).

## Baseline and scope

Baseline main is `77ca028c997563f9ee9ffaa338de1c48438a978e`; its complete
[main-push CI](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/actions/runs/43297436820)
passed. The pre-existing untracked `docs/reviews/` is excluded. Protect Main requires a PR,
forbids deletion and non-fast-forward updates, allows no bypass actors, and requires the
documentation, ingestion, Python/Spark/DuckDB and React checks. No merge is authorized.

Reuse the existing original Content pipeline and selectively reconcile the worker portions
of closed/unmerged [PR #137](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/pull/137),
head `9b741b0869a9297acb69900b2d24358a882656c6`. Preserve current Reliability cancellation,
explicit retry exclusion/lineage and uncertain-cleanup handling. Do not reimport public
transport, V23, publication components or an opt-in public runner already excluded from main.

The packaged worker may select one compatible fixed pipeline. This is implementation
for isolated validation, not operational activation. No current installation, source,
retained evidence or publication is changed by this checkpoint's test setup.

## Implementation and compatibility

The packaged lifecycle selects exactly one fixed compatible pipeline for its lifetime.
Original Content requires RunProfile v2, exactly one complete public composition-v2 pipeline,
all three exact launch/verification IDs and digests, and installed JDK/JAR/module/storage/
credential qualification. Startup performs bounded local reads and a read-only interpreter
probe. It creates no run, attempt, admission, source request, object-storage request or native
reservation. Worker registration and idle heartbeats remain ordinary coordination metadata.

Only a genuine acknowledged original claim enrolls Content admission. The dispatcher commits
PREPARING, prepares the existing private evidence tree, commits READY and invokes the existing
17-operation pipeline. The successful atomic proof/registration/release transaction returns
its actual idle session; history, lookup and lost acknowledgments cannot reconstruct it.
One dispatcher retains the host guard, owns heartbeat/session versions and preserves ordering
and Content/Reliability/import exclusion.

Stopping reaches admission boundaries, runtime probes, FETCH, Bronze, independent Bronze
verification and every native pipeline supervisor. A read-only idle check before any claim
can retire the already acknowledged idle session on stop. Once claim authorization begins,
unknown outcomes stop dispatch and retain the existing durable ownership/evidence. Content
never enters Reliability's definitive prelaunch release or running-cancellation settlement.
A worker exit or guard release alone does not establish native writer quiescence.

The candidate worker code from PR #137 was reconciled selectively:

- Reuse its run-independent registration reader, installed startup qualifier and runtime
  check factories, original claim/admission integration, stop-aware probes/supervisors and
  acknowledged final-session return. Do not reimplement native processing.
- Keep current Reliability retry-queue exclusion, explicit retry lineage, cancellation
  recognition/settlement and uncertain-cleanup handling. The older PR would remove these
  if its repository/lifecycle/supervisor files were imported wholesale.
- Tighten ordinary startup to reject historical composition v1 before its interpreter probe.
  Rebase startup tests onto actual public composition v2. Use a synthetic reserved-domain
  catalog in tests and remove the candidate's standalone public-runner helper.
- Add actual packaged-JVM startup/rejection, exact idle acknowledgment before normal SIGTERM,
  private installed-module mutation and frozen publisher-reference assertions. The isolated
  Content installation deliberately omits unrelated Reliability execution wrappers.
- Shared path readiness validates common paths; each adapter still requires its full own
  registered module closure. No module or version check is removed from Reliability startup.

No ingestion production source, Python processing module, V1–V23 migration, native publication
format, product fingerprint or recovery receipt changes. The in-memory completion record gains
an acknowledged session only; no persisted/wire field is added. Existing loopback-v1 internal
execution remains compatible but cannot enter ordinary Content startup. The helper assertions
now explicitly describe legacy Reliability/NONE capabilities instead of implying that no
qualified Content adapter exists. Discovery remains registration metadata, not readiness or
activation authority. No API/UI endpoint, dependency, public runner or activation switch is added.

## Local validation

All local native/database execution uses existing image `ddip-pr71-linux`, exact image ID
`sha256:3c8a4598ffd777da0c786f45a2a5249f97632877eb04f5e1f830d70157caf2d7`, Linux aarch64,
Temurin `21.0.12.1+1-LTS`, Python 3.12.15, PySpark 4.0.2, Boto3 1.43.114 and DuckDB 1.4.5.
PostgreSQL is 17.11, pinned to the existing CI image digest. Test containers share an isolated
loopback namespace with no external network; `--init` reaps test descendants. A separate stable
namespace contains the disposable restart cluster. No retained installation is used.

A copied, ignored build workspace preserves earlier E5.2-A/B artifacts and evidence. Its exact
commands, source inventories, test XML/logs and restart observations are retained under
`outputs/e5-2c-20261113T154756Z/`; generated evidence is not committed. All 81 checked earlier
E5.2-B evidence/build identities remain unchanged. Existing untracked `docs/reviews/` is untouched.

| Command/selection | Result | Elapsed |
| --- | --- | --- |
| Ingestion `mvn -B -ntp -o -Daether.syncContext.named.factory=noop -f ingestion/pom.xml verify` | 338 passed; no failures/errors/skips | 1m27s Maven |
| `bash ingestion/scripts/public-fetch-packaged-smoke.sh` | 3 production-main rejection cases and JAR isolation passed; no DNS/HTTP reached | Separate smoke |
| Content API `verify -Ppostgres`, full units | 975 passed; 3 existing optional corpus skips; no failures/errors | Included in 8m46s below |
| Same invocation, three new lifecycle classes, `PersistenceIT`, `WorkerCoordinationIT`, three selected native-chain cases | 164 passed; no failures/errors/skips | 8m46s total Maven |
| `NarrowContentPipelineRestartIT` with disposable restart signal | 4 passed; 4 actual PostgreSQL restarts; no failures/errors/skips | 4m24s Maven |
| Reliability execution/cancellation/retry/worker/history regression selection | 175 passed; no failures/errors/skips | 10m04s Maven |
| `python3 -I -B -m unittest discover -s .github/ci/tests -v` | 70 passed | 6.687s initial unittest elapsed |
| Installed Node 24 `npm run check`; `git diff --check` | Markdown/links, repository hygiene and whitespace passed | Not benchmarked |

The full-unit skips are `ContentEnvelopeMeasurementTest`, `NarrowPublicationEnvelopesTest`
and `PublicationProjectionParityTest`; their separately supplied optional corpora are absent.
They are not counted as passes. Counts overlap focused and hosted runs and must not be added
as distinct coverage.

The 164-case integration selection is:

```text
NarrowContentWorkerStartupIT,OrdinaryContentWorkerLifecycleIT,OrdinaryContentLegacyLifecycleIT,
PersistenceIT,WorkerCoordinationIT,
NarrowContentPipelineIT#originalFetchThroughContentUsesAllSeventeenOperationsAndRealFinalProof
  +emptyFeedAndLostFinalCommitAcknowledgmentLeaveDurableVerifiedSuccess
  +persistentStopAfterPreflightPreventsFetchAndRetainsOriginalOccupancy
```

Use this as the single comma-separated Maven `-Dit.test` argument, without the visual line
breaks/spaces. The three new classes contain 50 cases within that total. The native chain proves
real Spark/native publication, independent final verification, API/history linkage and a second
eligible claim using the acknowledged session. It is test-only loopback integration, not public
DNS/TLS or a complete packaged public API-to-worker acceptance run.

The restart command adds `-DskipUnitTests=true -Dit.test=NarrowContentPipelineRestartIT` and
`-Dddip.test.narrow-pipeline-restart-signal=<isolated-workspace>/.restart-request`; a host watcher
restarts only the dedicated disposable PostgreSQL container and acknowledges the signal. Each
case records changed `pg_postmaster_start_time()` and unchanged retained SQL/file evidence.
Default hosted execution uses the existing explicitly supplied disposable container ID.

The Reliability regression invocation uses `-DskipUnitTests=true` and
`-Dit.test=QueuedCancelIT,RunningCancelIT,ReliabilityExecutionIT,ReliabilityWorkerIT,`
`ReliabilityRetryIT,ReliabilityRetryEvidenceIT,ReliabilityRetryCancellationIT,RunHistoryIT`
(join the two selection fragments without whitespace).

### Failed attempts and diagnosis

An initial adapted startup fixture used the artifact ID instead of the Silver storage-binding
ID; one of 46 initial integration cases errored. Correcting that private fixture preserved the
identity-replacement assertion; all 50 final cases pass. No production check was weakened.

Early packaged SIGTERM tests observed registration/application startup but had not observed
an acknowledged idle queue result. Their worker row remained READY after exit; this was not
reported as successful release. Spring lifecycle traces and the disposable SQL trace were
retained. The test now observes the fixed DEBUG idle acknowledgment before asserting normal
idle STOPPED; stops during claim/admission still retain uncertainty. Read-only pre-claim
inspection stopping is handled without treating an active Content owner as releasable.
The IDE had no Linux run configuration or attach operation for this child, so diagnosis used
packaged lifecycle and database evidence; no debugger-local observations are claimed.

The host default Node 26 failed to load its installed simdjson library. Selecting the existing
Node 24 installation for repository checks passed; no package or host installation was changed.
Every failed log remains in ignored evidence. Final successful results do not rewrite them.

## Hosted CI and activation limits

The explicit PostgreSQL inventory is now 71 classes: core=21, reliability=10, ingestion=3,
gold=3, content_recovery=3, native_fetch_recovery=13, native_bronze=6, native_publication=5,
native_pipeline_recovery=7. Core builds ingestion/provider artifacts for the new startup tests;
it does not start Bronze object storage for those read-only cases. Existing exhaustive report
verification, all required contexts, partitions and 90-minute limits remain in place.

The draft PR must receive complete exact-head CI and owner review before any merge decision.
Hosted job IDs, counts, durations and outcomes will be recorded in the PR evidence after the
run, without a documentation commit that would invalidate the tested head. Previous CI and
local component tests cannot substitute for it.

Real production-child public DNS/IPv4/TLS, full packaged public API-to-worker acceptance,
second public same-worker run/replay, public live-child stopping with fresh reviewed quiescence,
browser acceptance and operational activation remain open. No public source was fetched here.
No installation/source configuration, ordinary operational Content execution, publication
promotion, general recovery/reauthorization, network workaround, AWS resource or spending is
part of this checkpoint. Issues #123 and #124 remain deferred.

The next proposed checkpoint is owner-reviewed public production-path qualification in a
separately authorized usable native-egress environment, using the immutable PR #138 fixture
and installed runtime/JAR identities. It must use genuine packaged ownership and preserve
all existing public transport, evidence and manual recovery contracts.

## Changed paths

```text
.github/ci/postgres-partitions.json
.github/ci/tests/test_policy.py
.github/ci/tests/test_postgres_partitions.py
.github/workflows/postgres-partition.yml
README.md
docs/adr/0050-original-only-content-worker-lifecycle.md
docs/adr/README.md
docs/operations/ci.md
docs/operations/e5-2c-original-content-worker.md
services/content-api/src/main/java/platform/contentapi/execution/ContentFetchLaunchRuntime.java
services/content-api/src/main/java/platform/contentapi/execution/DatabaseConfiguration.java
services/content-api/src/main/java/platform/contentapi/execution/NarrowContentAdmission.java
services/content-api/src/main/java/platform/contentapi/execution/NarrowContentBronzeCompletion.java
services/content-api/src/main/java/platform/contentapi/execution/NarrowContentBronzeLaunch.java
services/content-api/src/main/java/platform/contentapi/execution/NarrowContentFetchLaunch.java
services/content-api/src/main/java/platform/contentapi/execution/NarrowContentPipeline.java
services/content-api/src/main/java/platform/contentapi/execution/NarrowContentRuntimeQualification.java
services/content-api/src/main/java/platform/contentapi/execution/NarrowContentWorkerStartup.java
services/content-api/src/main/java/platform/contentapi/execution/ReadOnlyVerifierProcess.java
services/content-api/src/main/java/platform/contentapi/execution/StrictBronzeLaunchRuntime.java
services/content-api/src/main/java/platform/contentapi/execution/StrictBronzeVerificationRuntime.java
services/content-api/src/main/java/platform/contentapi/execution/StrictContentPipelineRuntime.java
services/content-api/src/main/java/platform/contentapi/execution/StrictFetchLaunchRuntime.java
services/content-api/src/main/java/platform/contentapi/execution/StrictPublicFetchLaunchRuntime.java
services/content-api/src/main/java/platform/contentapi/execution/WorkerExecutionCapability.java
services/content-api/src/main/java/platform/contentapi/execution/WorkerIdleLifecycle.java
services/content-api/src/main/java/platform/contentapi/execution/WorkerRepository.java
services/content-api/src/main/resources/application.properties
services/content-api/src/test/java/platform/contentapi/execution/NarrowContentPipelineTestSupport.java
services/content-api/src/test/java/platform/contentapi/execution/NarrowContentWorkerStartupTestSupport.java
services/content-api/src/test/java/platform/contentapi/execution/WorkerCapabilityTest.java
services/content-api/src/test/java/platform/contentapi/execution/WorkerTestSupport.java
services/content-api/src/test/java/platform/contentapi/management/NarrowContentPipelineCrashIT.java
services/content-api/src/test/java/platform/contentapi/management/NarrowContentPipelineIT.java
services/content-api/src/test/java/platform/contentapi/management/NarrowContentPipelineRestartIT.java
services/content-api/src/test/java/platform/contentapi/management/NarrowContentPressureIT.java
services/content-api/src/test/java/platform/contentapi/management/NarrowContentWorkerStartupIT.java
services/content-api/src/test/java/platform/contentapi/management/OrdinaryContentLegacyLifecycleIT.java
services/content-api/src/test/java/platform/contentapi/management/OrdinaryContentWorkerFixtures.java
services/content-api/src/test/java/platform/contentapi/management/OrdinaryContentWorkerLifecycleIT.java
```
