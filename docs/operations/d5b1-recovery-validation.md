# D5b-1 Content recovery — implementation and validation

Tracking: [E5 #49](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/issues/49).
Branch: `feature/e5-d5b1-content-recovery`. No branch switch, staging, commit, push, PR or GitHub write.
D5a prerequisite verified read-only: merge `ce505e571c0825948f709a2060d946d105190cc8`,
[hosted run 41015345637](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/actions/runs/41015345637), successful.
Production Content dispatch and analytical activation remain disabled.

## Recovery matrix

| Original boundary / observation | Qualification and permitted transition |
| --- | --- |
| Before FETCH_SOURCE intent | Captured selection, no delivery/effect evidence, original writer quiescence: PAUSED; never fetch |
| Intended FETCH_SOURCE without committed original journal | FAILED / FETCH_EVIDENCE_LOST; original UUID/time never replaced |
| Committed FETCH_SOURCE | Reopen original descriptor, payload, metadata, completion and delivery acknowledgment; a future source without intent is not called lost evidence |
| STORE_BRONZE | Independent exact-key GET-only inspection; exact original receipt can be recovered transactionally; absent/payload-only storage is distinguished from conflict and unavailable I/O |
| SILVER | Complete selected-source set, exact receipts, native qualification/provenance/inventory; compare committed qualification when present; PAUSED without next-stage launch |
| PREPARE_GOLD / PUBLISH_GOLD | Original complete frozen Silver selection and manifest; independent native Gold verification; PAUSED for qualified checkpoint; positively absent/incomplete intended namespace can fail safely |
| CHECK_INPUTS | Original committed Gold and exact captured publisher reference; independent preparation: PAUSED |
| PUBLISH_CONTENT / absent or interrupted VERIFY_RESULT | Exact intended fingerprint, mandatory earlier checkpoints and full independent Content analytics reader: SUCCEEDED / RECOVERED_PUBLICATION |
| Readable demonstrably corrupt Content completion/inventory | Native rejection plus separate byte seal/proof: FAILED / PUBLICATION_INVALID; no repair |
| Dependency/verifier I/O, incompatible runtime, missing authority, contradictory or changing evidence, possible writer | RECOVERY_REQUIRED with occupied RECOVERY_ONLY; no dispatch or ownership release |

All release paths require the same installation guard, fresh explicit operator review,
independent direct-child/descendant observations, exact attempt/epoch/version fence and
unchanged retained evidence. There is no RECOVERY_REQUIRED-to-RUNNING transition.
PAUSED has no operational RESUME implementation.

## Reuse and additions

Reused unchanged protocols: ControlCommands/ControlRepository admission, idempotency and ETags;
InstallationHostGuard; C3c RecoveryQuiescence, occupied handoff, bounded lineage, coordinator-first
fence and heartbeat; existing process supervisor; original journal/receipt readers; native Silver,
Gold, publisher-reference and Content verifiers; existing artifact/dependency/audit tables.
Reliability retains its original runtime JSON, verifier and terminal finalizer.

New components: Content-specific private-attempt capture (including Silver gateway hints in
result.json), RecoveryRuntime pipeline boundary, fixed read-only native dispatcher, Content
observation/disposition adapter and native acceptance fixtures. Runtime qualification no longer
requires a committed delivery merely to load Silver/Gold profile metadata. Normal execution still
qualifies its data at the existing boundaries. V8 adds only the requested Content PAUSED state;
[ADR 0018](../adr/0018-managed-content-recovery-boundaries.md) remains pending owner acceptance.
Earlier ADR acceptance statuses are unchanged.

## Release, failure and immutability evidence

The final native acceptance creates an original worker in a separate JVM. Admission rejects it
while live; an independently UNAVAILABLE identity also vetoes handoff despite operator attestation.
The old worker exits before guarded reconciliation. Each case checks duplicate control replay,
occupied handoff, original attempt INTERRUPTED, no automatic continuation, duplicate terminal
lookup and admission of a fresh unrelated run after safe release.

The completed-Content case restarts real PostgreSQL 17.11 after occupied handoff, then recovers
without an original VERIFY_RESULT invocation. It compares all 12 Content publication files plus an untouched sentinel by
SHA-256, mtime and inode. It injects failures at artifacts, dependency edges, stage links, attempts,
runs, controls, worker status, coordinator and audit writes; every disposition rolls back without
an artifact or released slot. A PostgreSQL wire proxy then drops the actual terminal COMMIT
acknowledgment. Read-only committed-fact lookup confirms one result; a pending cancel becomes
TOO_LATE. The subsequent run is admitted through the normal repository.

Separate native cases exercise pre-fetch, journal-only, Bronze, Silver, Gold and Content-input
PAUSED recovery. Deterministic rolled-back SQL probes additionally cover FAILED and CANCELLED.
The native dispatcher tests cover missing/changed Gold/reference evidence, corrupt/incomplete
Content, identity substitution, absent namespace qualification and ordinary stat I/O. Existing D1/D2,
control-plane and C3c suites retain their original loss, orphan, concurrency, epoch and rollback cases.

The seven-case native run passed in 306.396 s of suite time (314.69 s driver wall time).
Its final publication recovered original attempt `cb5a1dca-2f66-429d-81ce-1ac21af9b4c8`
as result `ff577334-d70d-48e7-acba-3ebce4a5787f`, then admitted fresh run
`92470a09-9fa6-46f6-9a15-c4334938d49c`. The six earlier cases independently admitted fresh
runs after PAUSED release. An additional final-only run validates the final launch-time command-byte
rechecks; it is excluded from distinct-case totals. It passed in 85.38 s (89.84 s driver wall),
recovering result `71b9db79-2dc5-493e-a55f-df7958143148` from original attempt
`b5fc5ccf-0378-4ab2-9140-febb33e61a36` and admitting run
`976808f1-691f-477a-a3e4-113f018aec87`.

## Test accounting and CI gap

Counts are distinct test cases, including parameterized invocations. Repeated focused/rerun cases
are counted once. No selected test was skipped.

| Successful scope | Cases | Measured duration |
| --- | ---: | ---: |
| All content-api units, including private capture tests | 335 | 12.525 s summed suites; 16.354 s Maven wall |
| PostgreSQL scoped regression: Persistence, ControlAdmission, WorkerCoordination, RunHistory, RssFetch, RssBronze, ContentFinalization | 309 | 253.139 s summed suites; Maven including units 270 s |
| C3c: RecoveryHandoff, RecoveryVerification, RecoveryFinalization, OperatorRecovery | 246 | 595.921 s summed successful suite reports |
| ContentRecoveryIT, seven actual recovery boundaries | 7 | 306.396 s suite; 314.69 s driver wall |
| Managed Silver/Gold/Content native adapters | 44 | 17.896 s |
| CI classification, inventory and aggregate-gate policy | 37 | 1.744 s |

Verified distinct total: 978. New cases: 29 (8 file-evidence units, 14 adapter tests, 7 native
recovery invocations). Final figures are verified against actual XML/unittest reports.

The initial concurrent C3c run had one operator startup failure (245/246 passed). The complete
operator suite passed 35/35 serially, including its actual PostgreSQL restart, in 246.311 s
(253.09 s driver wall time). No Reliability timeout or behavior was weakened. The initial C3c
run performed actual bootstrap, verification, finalization and operator PostgreSQL restarts.

Scoped Maven passed all 335 units and 309 integration cases. Its generated local-only driver
then rejected the two extra D1/D2 restart signals because that driver's original selection expected
none. The XML reports and exactly one RSS plus one Bronze restart signal were independently
checked successfully. This is not represented as a passing driver invocation. Its wall time was
276.76 s. Checked-in Content recovery and C3c operator drivers passed their own restart checks.

ContentRecoveryIT is assigned exactly once to required `gold`. Inventory: core 9 classes,
reliability 7, ingestion 3, gold 4. The workflow passes only its exact disposable PostgreSQL
service container ID for the mandatory real restart. Classification, aggregate gates, comprehensive
Maven discovery, existing native regressions and all 90-minute partition ceilings are unchanged.
Policy tests, actionlint, Markdown/relative-link checks, repository hygiene and diff whitespace
checks pass locally.

These are local macOS/JDK 21/Python 3.12.15 measurements, not hosted Ubuntu measurements or
scalability claims. No D5b-1 hosted workflow was run. The complete new gold partition and its
90-minute budget, other complete partitions, the full standalone processing suite and analytics HTTP
profile still require hosted/scoped acceptance as appropriate; individual local scoped passes do not
claim those aggregate gates passed.

## Limits and owner decisions

- Accept/revise pending ADR 0018's Content-only PAUSED departure from ADR 0010; ADRs 0015–0017 retain their prior status.
- RESUME, retry execution, production Content dispatch, activation, automatic recovery, cleanup and repair remain unsupported.
- Partial fresh Silver source selection, missing bound commands/private authority, incompatible retained runtimes and unobservable writers remain blocked rather than selecting replacement evidence.
- Native invalidity without independent positive corruption proof, and ordinary unavailable dependencies/I/O, retain recovery ownership. Contradictory already-applied cancellation cannot be rewritten into TOO_LATE.
- Existing verifier/packet bounds remain fail-closed; Content private capture additionally bounds 4,096 entries and 64 MiB per original attempt. Larger retained private evidence needs a reviewed compatible recovery path; it is never truncated.
- The added native intermediate paths are exercised positively; the complete cross-product of every Content checkpoint with every outage/corruption injection is not claimed. Reused deterministic controls and native verifier tests provide the scoped negative coverage recorded above.
- D5b-2 needs separately authorized continuation/retry semantics, runtime compatibility/retention procedures, hosted acceptance and explicit production activation/readiness approval.

## Exact changed-file inventory

30 files; generated logs, private fixtures, native data, containers and build outputs are not tracked.

- Modified: [.github/ci/postgres-partitions.json](../../.github/ci/postgres-partitions.json)
- Modified: [.github/workflows/postgres-partition.yml](../../.github/workflows/postgres-partition.yml)
- New: [docs/adr/0018-managed-content-recovery-boundaries.md](../../docs/adr/0018-managed-content-recovery-boundaries.md)
- Modified: [docs/adr/README.md](../../docs/adr/README.md)
- Modified: [docs/architecture/durable-execution.md](../../docs/architecture/durable-execution.md)
- New: [docs/operations/d5b1-recovery-validation.md](../../docs/operations/d5b1-recovery-validation.md)
- Modified: [docs/operations/managed-rss-fetch.md](../../docs/operations/managed-rss-fetch.md)
- New: [processing/content_recovery_verifier.py](../../processing/content_recovery_verifier.py)
- Modified: [processing/tests/test_managed_content.py](../../processing/tests/test_managed_content.py)
- Modified: [processing/tests/test_managed_gold.py](../../processing/tests/test_managed_gold.py)
- Modified: [scripts/test-rss-fetch-postgres.py](../../scripts/test-rss-fetch-postgres.py)
- New: [services/content-api/src/main/java/platform/contentapi/execution/ContentRecovery.java](../../services/content-api/src/main/java/platform/contentapi/execution/ContentRecovery.java)
- New: [services/content-api/src/main/java/platform/contentapi/execution/ContentRecoveryFiles.java](../../services/content-api/src/main/java/platform/contentapi/execution/ContentRecoveryFiles.java)
- Modified: [services/content-api/src/main/java/platform/contentapi/execution/OperatorRecoveryConfiguration.java](../../services/content-api/src/main/java/platform/contentapi/execution/OperatorRecoveryConfiguration.java)
- Modified: [services/content-api/src/main/java/platform/contentapi/execution/RecoveryFiles.java](../../services/content-api/src/main/java/platform/contentapi/execution/RecoveryFiles.java)
- Modified: [services/content-api/src/main/java/platform/contentapi/execution/RecoveryHandoff.java](../../services/content-api/src/main/java/platform/contentapi/execution/RecoveryHandoff.java)
- Modified: [services/content-api/src/main/java/platform/contentapi/execution/RecoveryNativeSeal.java](../../services/content-api/src/main/java/platform/contentapi/execution/RecoveryNativeSeal.java)
- Modified: [services/content-api/src/main/java/platform/contentapi/execution/RecoveryOperatorInspection.java](../../services/content-api/src/main/java/platform/contentapi/execution/RecoveryOperatorInspection.java)
- Modified: [services/content-api/src/main/java/platform/contentapi/execution/RecoveryPacket.java](../../services/content-api/src/main/java/platform/contentapi/execution/RecoveryPacket.java)
- New: [services/content-api/src/main/java/platform/contentapi/execution/RecoveryRuntime.java](../../services/content-api/src/main/java/platform/contentapi/execution/RecoveryRuntime.java)
- Modified: [services/content-api/src/main/java/platform/contentapi/execution/RssBronzeExecution.java](../../services/content-api/src/main/java/platform/contentapi/execution/RssBronzeExecution.java)
- Modified: [services/content-api/src/main/java/platform/contentapi/execution/RssGoldRuntime.java](../../services/content-api/src/main/java/platform/contentapi/execution/RssGoldRuntime.java)
- Modified: [services/content-api/src/main/java/platform/contentapi/execution/RssSilverRuntime.java](../../services/content-api/src/main/java/platform/contentapi/execution/RssSilverRuntime.java)
- Modified: [services/content-api/src/main/java/platform/contentapi/execution/SchemaLifecycle.java](../../services/content-api/src/main/java/platform/contentapi/execution/SchemaLifecycle.java)
- Modified: [services/content-api/src/main/java/platform/contentapi/management/RunInspectionValues.java](../../services/content-api/src/main/java/platform/contentapi/management/RunInspectionValues.java)
- New: [services/content-api/src/main/resources/db/migration/V8__content_recovery_pause.sql](../../services/content-api/src/main/resources/db/migration/V8__content_recovery_pause.sql)
- New: [services/content-api/src/test/java/platform/contentapi/execution/ContentRecoveryFilesTest.java](../../services/content-api/src/test/java/platform/contentapi/execution/ContentRecoveryFilesTest.java)
- New: [services/content-api/src/test/java/platform/contentapi/execution/ContentRecoveryIT.java](../../services/content-api/src/test/java/platform/contentapi/execution/ContentRecoveryIT.java)
- New: [services/content-api/src/test/java/platform/contentapi/management/ContentRecoveryTestProcess.java](../../services/content-api/src/test/java/platform/contentapi/management/ContentRecoveryTestProcess.java)
- Modified: [services/content-api/src/test/java/platform/contentapi/management/RssGoldFixtures.java](../../services/content-api/src/test/java/platform/contentapi/management/RssGoldFixtures.java)
