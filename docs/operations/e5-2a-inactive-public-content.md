# E5.2-A: Inactive bounded public Content composition

Owner authorization: [#49, November 12, 2026](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/issues/49#issuecomment-6831203628).

This checkpoint prepares a separately versioned public IPv4 FETCH child and immutable
composition v2. Ordinary Content dispatch, deployment and public activation remain disabled.
Closed, unmerged [PR #137](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/pull/137)
is candidate evidence, not delivered code or acceptance evidence for this head.

## Acceptance criteria

- Classify every bounded IPv4 answer before sealing one deterministic destination;
  reject forbidden, mixed, empty, oversized and unsupported-family results without fallback.
- Verify the separate provider JAR, actual resolver callback, fresh JVM configuration,
  original Host/SNI, default TLS hostname validation, no redirects/proxies, bounded body,
  timeout/interruption, and one application invocation without refetch.
- Require exact registered v2 launch/policy/composition IDs and digests in Java and SQL;
  reject cross-version or disconnected profiles before release.
- Add V23 only; preserve V1–V22 checksums, legacy v1 packets and receipts, native publication
  identities and exact frozen source/dependency selection. Qualify historical reads/upgrades.
- Keep ordinary worker capability/lifecycle unchanged and public history ineligible for
  new legacy FETCH-only recovery handoffs. Retain occupied uncertain states and fail closed.
- Run offline security, profile/history, native Content and relevant Reliability regression
  suites; enumerate complete CI inventory and observe required checks on the new PR head.

## Baseline and reconciliation

Remote and local main were `9afeb642849223b1ee706a0b516852a0e8d1c076` before edits.
The only pre-existing worktree change was untracked `docs/reviews/`, excluded from this work.
The active Protect Main ruleset requires a PR, forbids deletion/non-fast-forward updates,
allows no bypass actors, and requires Documentation and repository hygiene, Java 21 ingestion
and Bronze storage, Python 3.12, local Spark processing and DuckDB analytics, and React
publisher dashboard. The new PR remains draft; no merge is authorized.

Main CI [run 43094071608](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/actions/runs/43094071608)
failed its Python aggregate; completed backend partitions and the PostgreSQL aggregate passed.
This baseline is not treated as a passing release gate.

Candidate ordinary-worker startup, capability, lifecycle, configuration and opt-in public
qualification code are excluded. Current Reliability cancellation/retry code and newer CI
partitions remain authoritative. No accepted ADR is changed; ADR 0049 documents this scope.

## Evidence and remaining gates

Local focused validation passed on Linux aarch64 in the existing `ddip-pr71-linux` image:
Temurin 21.0.12.1+1-LTS, Python 3.12.15, PySpark 4.0.2, Boto3 1.43.114, DuckDB 1.4.5,
PostgreSQL 17.11. Containers share an isolated loopback namespace with no external network.
This does not reproduce hosted Ubuntu x86_64 or qualify public DNS/TLS egress.

| Command/selection | Result | Maven elapsed |
| --- | --- | ---: |
| `mvn -B -ntp -o -Daether.syncContext.named.factory=noop -f ingestion/pom.xml verify` | 338 passed, zero failures/errors/skips | 1m44s |
| `bash ingestion/scripts/public-fetch-packaged-smoke.sh` | 3 production-main rejection cases and exact JAR isolation passed | separate from Maven |
| Content API `verify -Ppostgres`, units below | 145 passed | included below |
| Same invocation, five new public ITs plus `PersistenceIT` | 139 passed, zero failures/errors/skips; includes all 22 historical schema targets | 1m55s total |
| `python3 -I -B -m unittest discover -s .github/ci/tests -v` | 68 passed | 6.172s Python elapsed |
| `bash scripts/test-run-batch.sh` | 5 argument cases passed | not benchmarked |
| Node 24 `npm run check`; `git diff --check` | Markdown/local links, hygiene, whitespace passed | not benchmarked |

Focused units: `StrictPublicFetchProfileTest,NarrowContentFetchReservationTest,`
`NarrowContentPublicPacketHistoryTest,NarrowContentRuntimeQualificationTest,`
`StrictFetchLaunchRuntimeTest,StrictBronzeLaunchRuntimeTest,`
`StrictBronzeVerificationRuntimeTest,WorkerCapabilityTest`.
Focused integration: `PersistenceIT,StrictPublicFetchProfileIT,NarrowContentPublicHistoryIT,`
`NarrowContentPublicReleaseHistoryIT,NarrowContentPublicCheckpointIT,`
`NarrowContentPublicBronzeHistoryIT`. These are Maven `-Dtest` and `-Dit.test` selections;
unit/integration counts overlap later full CI and must not be added as distinct coverage.

The broader local invocation passed **977 unit cases with zero failures/errors and three
existing optional generated-fixture skips**, plus **53 SQL cases** across QueuedCancelIT,
RunningCancelIT, ReliabilityRetryIT, ReliabilityRetryEvidenceIT and ReliabilityRetryCancellationIT,
with zero failures/errors/skips. Maven elapsed was **4m18s**. The three optional skips are
ContentEnvelopeMeasurementTest, NarrowPublicationEnvelopesTest and PublicationProjectionParityTest;
these require separately supplied measurement/projection corpora and are not claimed as passes.
The Linux container required `--init` to reap deliberately orphaned test descendants: a run
without it failed an existing BoundedProcessRunnerTest assertion, while the identical code
passed with it. No application or test behavior was changed to accommodate the container.

Exhaustive native Content regressions and hosted required checks must be reported separately
on the draft PR head. The PR evidence is updated after the run, avoiding a documentation-only commit that
would invalidate the exact tested head. No earlier CI run substitutes for it. Initial
[CI #242](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/actions/runs/43276327883)
found an extra blank line in this record (MD012). The document was corrected without changing
the lint rule or application tree; its superseding head requires a fresh complete CI run.

Initial sandbox runs failed local socket/process permissions. A macOS retry also failed the
strict public environment guard because that JDK exposes default proxy properties; its native
filesystem tests encountered noncanonical temporary paths. These failed attempts are not
counted as passes. The guard was retained; macOS public-child execution remains unqualified.
The successful Linux run used no production policy bypass, custom trust store, DNS override
or public feed. Synthetic loopback pins/trust/delegates exist only in test classes, excluded
from both production JARs. Component history fixtures do not claim a real public FETCH.

Fault observations include one application invocation and one resolver delegate call for each
transport case, zero replacement-destination connections, and two pinned wire connections on
accepted/drop and response/drop faults. Exact 5 MiB succeeds; chunked 5 MiB + 1 rejects without
refetch. HTTP reconnect ambiguity remains explicit.

Public gates remain open: exact installed JDK/provider verification in the authorized native
egress environment; real packaged DNS/IPv4/TLS qualification with a separately reviewed
immutable synthetic endpoint; independent security/compatibility review; owner review of the
exact-head hosted inventory and timing; separate merge and activation decisions. This milestone
authorizes no ordinary dispatch, deployment, continuation or recovery reauthorization.

E5.2-B should first propose the bounded packaged public-path qualification and its evidence
contract. Ordinary worker lifecycle/dispatch, cancellation behavior and activation should be
reviewed explicitly before implementation, with no automatic retry or refetch.

## Contract compatibility and exact changed files

- Ingestion adds a separate main/provider/policy and test-only drivers; the shared JAR excludes
  resolver classes/services. The legacy main exposes four existing helpers at package scope
  without changing its packet/parser/command/journal behavior. No new dependency is added.
- Launch/profile/runtime classes add exact v2 registration and immutable composition checks.
  Historical validation does no installed-byte or network access. Legacy v1 remains loopback.
- Reservation, phase, candidate and pipeline readers dispatch by exact version, bind recorded
  policy/digests and preserve v1 packet bytes/fingerprints. New FETCH-only handoff entry is
  v1-only in both Java and SQL; existing receipt readers and full safe-end semantics remain.
- V23 replaces guards additively and adds policy/correspondence functions; V1–V22 files are
  byte-identical to main. Existing rows, publication formats, receipts and dependencies are
  not rewritten. Schema lifecycle and migration-test latest-version assertions advance to 23.
- Discovery advertises complete v2 registration metadata with dispatchDisabled=true and
  runtimeQualification=NOT_CHECKED. It performs no DNS, filesystem, storage or launch probes.
- New SQL/component tests cover public histories; PersistenceIT covers every prior schema
  upgrade. Existing historical native tests keep their original pre-upgrade targets and
  assertions, adding V23 only to the later-migration exclusion lists.
- CI adds the packaged rejection smoke and five explicit integration class assignments.
  Current Reliability assignments, exhaustive inventories and fail-closed fan-in are retained.
- Documentation records scope, policy provenance, tests, limits and inactive status.

WorkerExecutionCapability, WorkerIdleLifecycle, WorkerRepository, ReadOnlyVerifierProcess,
NarrowContentPipeline, application.properties, Reliability production code, frontend and
scripts/local-operations-demo.py are unchanged. No ordinary Content startup class is added.

Exact paths in this checkpoint (excluding the pre-existing untracked review document):

```text
.github/ci/postgres-partitions.json
.github/ci/tests/test_policy.py
.github/ci/tests/test_postgres_partitions.py
.github/workflows/ci.yml
docs/adr/0049-inactive-bounded-public-content-composition.md
docs/adr/README.md
docs/architecture/0049-public-fetch-ipv4-deny-snapshot.json
docs/architecture/0049-public-fetch-ipv4-deny-snapshot.sha256
docs/operations/ci.md
docs/operations/e5-2a-inactive-public-content.md
ingestion/pom.xml
ingestion/scripts/public-fetch-packaged-smoke.sh
ingestion/src/main/java/platform/ingestion/managed/PublicFetchAddressPolicy.java
ingestion/src/main/java/platform/ingestion/managed/PublicFetchNetworkPolicy.java
ingestion/src/main/java/platform/ingestion/managed/PublicFetchResolverProvider.java
ingestion/src/main/java/platform/ingestion/managed/StrictManagedRssFetch.java
ingestion/src/main/java/platform/ingestion/managed/StrictPublicManagedRssFetch.java
ingestion/src/public-fetch-resources/META-INF/services/java.net.spi.InetAddressResolverProvider
ingestion/src/test/java/platform/ingestion/managed/PublicFetchAddressPolicyTest.java
ingestion/src/test/java/platform/ingestion/managed/PublicFetchPackagedSmoke.java
ingestion/src/test/java/platform/ingestion/managed/PublicFetchResolverProviderTest.java
ingestion/src/test/java/platform/ingestion/managed/PublicFetchResolverTestDriver.java
ingestion/src/test/java/platform/ingestion/managed/PublicFetchTransportTest.java
ingestion/src/test/java/platform/ingestion/managed/PublicFetchTransportTestDriver.java
ingestion/src/test/java/platform/ingestion/managed/StrictPublicManagedRssFetchTest.java
services/content-api/src/main/java/platform/contentapi/execution/ContentFetchLaunchRuntime.java
services/content-api/src/main/java/platform/contentapi/execution/NarrowContentBronzeCompletion.java
services/content-api/src/main/java/platform/contentapi/execution/NarrowContentBronzeLaunch.java
services/content-api/src/main/java/platform/contentapi/execution/NarrowContentFetchCandidateVerification.java
services/content-api/src/main/java/platform/contentapi/execution/NarrowContentFetchLaunch.java
services/content-api/src/main/java/platform/contentapi/execution/NarrowContentFetchRecoveryHandoff.java
services/content-api/src/main/java/platform/contentapi/execution/NarrowContentFetchReservation.java
services/content-api/src/main/java/platform/contentapi/execution/NarrowContentPipelineRecoveryAssessment.java
services/content-api/src/main/java/platform/contentapi/execution/NarrowContentRuntimeQualification.java
services/content-api/src/main/java/platform/contentapi/execution/SchemaLifecycle.java
services/content-api/src/main/java/platform/contentapi/execution/StrictBronzeLaunchRuntime.java
services/content-api/src/main/java/platform/contentapi/execution/StrictBronzeVerificationRuntime.java
services/content-api/src/main/java/platform/contentapi/execution/StrictContentPipelineRuntime.java
services/content-api/src/main/java/platform/contentapi/execution/StrictFetchLaunchRuntime.java
services/content-api/src/main/java/platform/contentapi/execution/StrictPublicFetchLaunchRuntime.java
services/content-api/src/main/java/platform/contentapi/management/DiscoveryRepository.java
services/content-api/src/main/resources/db/migration/V23__bounded_public_fetch_composition.sql
services/content-api/src/test/java/platform/contentapi/execution/ImportOwnershipIT.java
services/content-api/src/test/java/platform/contentapi/execution/NarrowContentFetchReservationTest.java
services/content-api/src/test/java/platform/contentapi/execution/NarrowContentFetchReservationTestSupport.java
services/content-api/src/test/java/platform/contentapi/execution/NarrowContentPublicCheckpointTestSupport.java
services/content-api/src/test/java/platform/contentapi/execution/NarrowContentPublicHistoryTestSupport.java
services/content-api/src/test/java/platform/contentapi/execution/NarrowContentPublicPacketHistoryTest.java
services/content-api/src/test/java/platform/contentapi/execution/PersistenceIT.java
services/content-api/src/test/java/platform/contentapi/execution/PublicContentProfileTestSupport.java
services/content-api/src/test/java/platform/contentapi/execution/StrictPublicFetchProfileTest.java
services/content-api/src/test/java/platform/contentapi/management/NarrowContentAdmissionIT.java
services/content-api/src/test/java/platform/contentapi/management/NarrowContentBronzeCheckpointSqlIT.java
services/content-api/src/test/java/platform/contentapi/management/NarrowContentFetchCompletionIT.java
services/content-api/src/test/java/platform/contentapi/management/NarrowContentFetchRecoveryFinalizationIT.java
services/content-api/src/test/java/platform/contentapi/management/NarrowContentPublicBronzeHistoryIT.java
services/content-api/src/test/java/platform/contentapi/management/NarrowContentPublicCheckpointFixtures.java
services/content-api/src/test/java/platform/contentapi/management/NarrowContentPublicCheckpointIT.java
services/content-api/src/test/java/platform/contentapi/management/NarrowContentPublicHistoryIT.java
services/content-api/src/test/java/platform/contentapi/management/NarrowContentPublicReleaseHistoryIT.java
services/content-api/src/test/java/platform/contentapi/management/NarrowContentSilverReservationSqlIT.java
services/content-api/src/test/java/platform/contentapi/management/SourceManagementIT.java
services/content-api/src/test/java/platform/contentapi/management/StrictPublicFetchProfileIT.java
```
