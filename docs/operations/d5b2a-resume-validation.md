# D5b-2a explicit Content resume

Historical checkpoint record. [D5b-2b inherited recovery](d5b2b-inherited-recovery-validation.md)
now extends the interrupted-resume recovery boundary described below. Ordinary Content
dispatch remains disabled; the evidence-size activation blocker remains unresolved.

Implementation and validation record. Production Content dispatch and analytical
activation remain disabled. [ADR 0019](../adr/0019-explicit-content-resume.md) is pending
owner acceptance; historical ADR statuses and V1–V8 migration bytes are unchanged.

## Merged baseline inspection

Inspected durable-execution, managed-rss-fetch, D5b-1 validation, ADRs 0010 and 0015–0018,
control admission, worker claims, original stage readers, recovery handoff and finalization,
V8 SQL and the partition inventory. Merged D5b-1 head e8d590b passed hosted run
[41144764237](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/actions/runs/41144764237).
Merge [41149252257](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/actions/runs/41149252257) also passed at 655878f. The owner-created branch starts at that merge. Before implementation:

| Component | Actual gap |
| --- | --- |
| Management controls | Only RECONCILE admitted; RESUME/RETRY were schema labels or deferred semantics |
| V8 schema | PAUSED Content supported, but controls type excludes RESUME |
| Worker claim | Requires QUEUED/current_attempt_id null; no recovered-checkpoint lineage |
| Operator inspection / acknowledgment | Content stages needed a pipeline-specific inspection whitelist; applied PAUSED reconciliation needed historical lookup after forward resume |
| Evidence | D5b-1 retained independent qualification, without continuation verifier |
| Execution/finalization | Readers assume one data attempt; no qualified inherited prefix |
| Limits | Retained private attempt bounded at 4096 entries and 64 MiB |

## Supported boundary matrix

| Recovered PAUSED boundary | Result and continuation |
| --- | --- |
| Before fetch, no earlier intent and no retained Silver inputs | Reopen captured reference, selection and recovery evidence; new attempt fetches captured sources once |
| Durable D1 journal | RESUME_CHECKPOINT_UNSUPPORTED; no replacement HTTP retrieval |
| Verified D2 Bronze | RESUME_CHECKPOINT_UNSUPPORTED; no PUT or receipt reconstruction by resume |
| Verified D3 Silver | RESUME_CHECKPOINT_UNSUPPORTED, including partial multi-source work |
| Verified complete D4 Gold | Reopen exact Bronze/Silver/Gold provenance and reference; reuse Gold; new CHECK_INPUTS, PUBLISH_CONTENT, VERIFY_RESULT |
| Verified D5a Content inputs | Same complete dependency revalidation; reuse original preparation; only PUBLISH_CONTENT and VERIFY_RESULT |
| Final Content already present, registered previously, incomplete or ambiguous namespace | Reject; use applicable explicit reconciliation, never republish or repair |
| RECOVERY_REQUIRED / unresolved writer or reconciliation | Reject; no direct transition to execution |

Gold and Content-input support requires every stage row in the complete original prefix
to be VERIFIED, plus the independent completed recovery qualification. Recovered file
evidence cannot replace a lost original stage acknowledgment in this implementation.
An INTENT/RUNNING/UNKNOWN row, even with a valid recovered native checkpoint, returns
RESUME_CHECKPOINT_UNSUPPORTED. This narrower committed-prefix requirement is intentional.

## Preconditions and operator procedure

Read the run and its ETag. Require Content PAUSED with a successful applied recovery,
original INTERRUPTED execution, no unresolved controls/writers/ownership, valid original
source revisions and currently enabled sources. Current approved profile, deployment,
storage and host bindings must match the captured configuration. Queue capacity, audit
capacity, control/attempt limits, conditional version and idempotency rules still apply.
The existing operator quiescence review in the exact retained recovery packet remains
mandatory; resume independently checks its recorded process identities again.

POST to the existing run controls endpoint using the normal local management headers,
Idempotency-Key and If-Match. Exact body:

```json
{"action":"RESUME","attemptId":null,"quiescenceConfirmed":null}
```

202/REQUESTED is intent only. Using the existing packaged `operator-recovery` role and
its database/installation settings, supply `ddip.recovery.control-id`,
`ddip.worker.runtime-profile-id`, `ddip.worker.execution-profile-id`, and
`ddip.worker.control-binding-id` as for recovery.
Set `ddip.recovery.action=resume` for guarded read-only qualification and durable requeue.
After QUEUED, separately set `ddip.recovery.action=execute-resume` with the same control
and runtime to explicitly claim and execute that run. See managed-rss-fetch for the
existing role launch/environment conventions. No background Content dispatcher is enabled.

Requeue atomically applies the control, clears the current pointer, records the immutable
plan and changes PAUSED to QUEUED. It preserves historical attempts and stages. Claim
creates a fresh EXECUTE attempt, epoch and instance and links reused_checkpoint_id when
there is a verified native checkpoint. BEFORE_FETCH links its empty-boundary recovery
through the immutable audit plan. The original run ID, captured selection, original data
attempt and publisher-reference artifact remain fixed. D5a's existing finalizer registers
the Content result/dependencies and releases the coordinator atomically.

Read-only verification examples, substituting the exact run/control IDs:

```sql
SELECT state, current_attempt_id, result_artifact_id, row_version
FROM ddip_management.runs WHERE run_id = '<run UUID>';
SELECT attempt_id, attempt_no, kind, state, owner_epoch, reused_checkpoint_id
FROM ddip_management.attempts WHERE run_id = '<run UUID>' ORDER BY attempt_no;
SELECT type, state, reason, row_version
FROM ddip_management.controls WHERE control_id = '<RESUME control UUID>';
SELECT mode, active_attempt_id, owner_instance_id, epoch
FROM ddip_management.coordinator WHERE singleton_id = 1;
```

After requeue, the run is QUEUED, the RESUME control is APPLIED/RESUME_QUEUED, and no
new attempt exists. After claim, the run's new EXECUTE attempt owns the coordinator;
after verified success, the run/result linkage is present and the slot is empty.
An empty slot is never sufficient by itself to authorize a new execution.

Repeated admission with identical key/body/version returns the original control. A
changed key body/version conflicts. Repeated applied requeue reads committed facts. A
lost requeue acknowledgment is resolved by the applied control and queued audit, never a
second transition. A repeated execute after success independently inspects the committed
result; an in-progress/uncertain attempt is never claimed again.

| Fixed rejection | Meaning |
| --- | --- |
| RESUME_CHECKPOINT_UNSUPPORTED | No implemented safe continuation for this exact stage history |
| RESUME_EVIDENCE_INVALID | Observed mismatch in retained evidence, selection or qualified dependency |
| RESUME_EVIDENCE_UNAVAILABLE | Missing/unavailable proof, possible process, verifier I/O or incompatible runtime; no continuation |
| RESUME_NAMESPACE_UNSAFE | Intended Content namespace already exists; no overwrite |
| SOURCE_DISABLED / PRECONDITION_FAILED / IDEMPOTENCY_CONFLICT | Existing management gate/version/identity rules |
| COORDINATOR_RECOVERY_REQUIRED / STATE_CONFLICT | Ownership or lifecycle does not permit this operation |
| MANAGEMENT_CAPACITY_EXCEEDED | Queue, control, attempt or audit bound exhausted |

A failed qualification leaves the run PAUSED (or already QUEUED before execution) and
its control/evidence available for inspection. A failure after claim retains occupied
ownership; do not clear it based on heartbeat expiry or parent exit.

## Reuse and transaction boundaries

Reused: control envelope/local authorization, idempotency and ETags; captured-selection
validation and source gates; installation guard and process supervisor; C3c packet,
process identity and private-file capture; PostgreSQL coordinator/worker fencing;
D1 journal and D2 receipt contracts; native Silver, Gold, reference and Content readers;
existing D5a result registration, dependency linkage and terminal release.

New: Content-only operator inspection and historical reconciliation acknowledgment after
authorized forward transition; SQL-only resume admission/checkpoint policy, read-only resume qualification with
supervised completion seals, immutable inherited-prefix links, guarded operator actions
and V9's control label. A metadata-only Silver proof route avoids the existing reader's
new Bronze-inspection sidecars in the old attempt. The native Gold verifier still performs
full independent Bronze/Silver qualification. Default execution/inspection keeps its
existing route.

Native verification and Spark execute outside PostgreSQL transactions. Requeue checks
coordinator → sorted sources → run/control versions again after verification and commits
run/control/audit changes together. Claim uses the existing fenced transaction and adds
the checkpoint/audit link. D5a's original finalizer combines referenced old verified rows
with actual new stage rows; it never fabricates successful invocations.

## Limits and D5b-2b prerequisites

Each retained original, reconciliation and resume-qualification private tree must fit the existing
4096-entry/64 MiB verification boundary. D1 payloads count toward that bound, so otherwise
valid large feeds or source sets can remain unsupported. Before-fetch continuation
does not reserve future recovery capacity: a later large interrupted attempt can exceed
the cap and remain unsupported. This is an activation blocker, not a larger recovery guarantee. No bytes are discarded or
truncated to make them qualify. Previous resume verifiers require a retained completion
seal and independently absent/reused direct-child identity. An interrupted verifier
without that seal remains unsupported even after its parent exits; preserve its files
and seek a reviewed D5b-2b process-evidence path. Never delete sidecars to retry.
A new native namespace must be absent and have no prior registered publication before publishing;
completed native checkpoints are only read. Partial Silver selection, pre-fetch runs containing retained Silver, storage-only
continuation, interrupted-resume recovery, production activation and unattended retry
remain unsupported. RETRY still lacks an operational admission/execution implementation;
failed-publication retry is a separate D5b-2b decision and does not block these PAUSED
boundaries. Pending cancellation continues to veto D5a launch/finalization; it is not
silently converted to success by RESUME. A post-publication cancellation or lost
publication acknowledgment can therefore leave an inherited-prefix attempt occupied.
A resumed interruption must retain coordinator ownership pending a
separate design; current D5b-1 does not understand inherited execution prefixes.

## Validation

Local measurements use macOS, Docker Desktop, disposable pinned PostgreSQL 17.11 and
SeaweedFS, JDK 21.0.7, Python 3.12.15, Spark 4.0.2 and DuckDB 1.4.5. These are correctness
runs, not throughput benchmarks. Disposable databases and storage are removed by the drivers.
Generated logs and report copies are ignored under `outputs/`.

| Validation | Distinct cases | Measured duration | Result |
| --- | ---: | --- | --- |
| Complete core PostgreSQL partition | 386 integration + 344 Java unit | 417.36 s driver | Zero failures/errors/skips; all 10 configured classes represented |
| Complete Gold PostgreSQL partition | 78 integration | 2665.87 s driver | Zero failures/errors/skips; all 5 configured classes represented |
| Final focused metadata/migrations/finalization | 161 integration, overlapping the partitions | 78.589 s wall | Zero failures/errors/skips; includes the additional publication-checkpoint acknowledgment case |
| Final supported native resume paths | 3 integration, overlapping ContentResumeIT | 375.04 s driver, 368.4 s class | All three complete, release ownership and admit a fresh run |
| Reliability partition and four actual PostgreSQL restart probes | 338 integration | 1044.39 s driver | Zero failures/errors/skips; all 7 configured classes represented |
| Managed Silver/Gold/Content Python regressions | 49 | 17.178 s | Passed, including 5 new deterministic resume-qualifier cases |
| CI classification, partition inventory and aggregate-gate policy | 37 | 3.617 s | Passed; all 25 integration classes assigned exactly once |

Across the scoped suites there are 803 distinct PostgreSQL integration cases, 344 Java
unit cases, 49 Python cases and 37 CI-policy cases: 1233 distinct cases, all passing.
The machine-readable count is `outputs/d5b2a-test-inventory.json`.

Current Gold coverage is 79 distinct integration cases: the full 78-case partition plus
the added D5a lost-publication-checkpoint-acknowledgment test, verified in the final focused
run. The final three native paths were rerun after the operator inspection and historical
acknowledgment fixes. Repeated/focused runs are not additional distinct cases.
Core, Gold and Reliability remain inside the unchanged 90-minute job budgets locally. A hosted Linux
execution is still required; these local times do not predict a hosted deadline guarantee.

### Successful native evidence

The final Gold resume test used original run `b2a23500-ce67-4cf6-9aa5-e0dc51ebc218`,
interrupted attempt `a0194956-62b8-4856-baca-da534fbe013f`, completed recovery
`d87d7f3a-d5e8-4927-bd82-f96bef99972e` and fresh EXECUTE attempt
`0e2edb79-6203-4e10-81c0-6a3ca82cd63d`. The original run and new attempt became
SUCCEEDED with Content artifact `e46ce31e-76fb-42da-a230-5f808e32e4fd`. PostgreSQL's
postmaster start changed from `2026-10-31T03:29:44.935142Z` to
`2026-10-31T03:35:24.832033Z` before execution. This is a real container restart, with
persisted rows/evidence reloaded after restart.

The two-source Gold case compares 38 retained Silver/Gold tree files (including fixture
sentinels) by bytes/hash, mtime and native file identity. It also compares old stage rows,
attempts, deliveries, private evidence and dependency edges. It performs zero HTTP
retrievals and creates no new Silver/Gold stage invocation. Newer source revisions are
installed before execution; the resumed run still uses the original captured revisions
and exact reference artifact. The Content-input case preserves 25 retained tree files
and adds no replacement CHECK_INPUTS stage. Pre-fetch retrieves its original source once.

The Gold test drops the real terminal COMMIT acknowledgment at the PostgreSQL wire
boundary. The existing D5a independent inspection returns the committed original result;
no publisher is repeated. The coordinator is empty, and new Content run
`5c05ec4e-3b40-407d-acff-fba1357ab21c` obtains epoch 4. The fresh run uses current source
revisions, while the resumed run retains its older revisions. It is safely ended before
launch in test teardown. Historical RECONCILE replay continues to return the original
PAUSED recovery receipt after requeue and after successful resume, without changing the
new owner or run disposition.

Logs: `outputs/d5b2a-resume-final.log`, `outputs/d5b2a-gold-partition.log`,
`outputs/d5b2a-core/core.log`, `outputs/d5b2a-core/inventory.log` and
`outputs/d5b2a-core/focused.log`.

### Blocked outcomes and rollback evidence

ContentResumeControlIT covers stale versions, duplicate keys, conflicting pending
controls, concurrent duplicate admission, reconcile competition, disabled sources,
capacity, stale epochs, non-PAUSED states, unresolved ownership and admission audit
rollback. Native tests independently inject LIVE and UNAVAILABLE original process
observations and reject both before requeue. A verifier launch without a completion
seal, a live sealed child, changed sealed evidence and unsafe namespace ancestors are
also vetoed. Missing/changed retained qualification and an unapproved runtime prevent
execution. The full Reliability regression includes actual active-writer veto, guard contention,
unobservable descendants, stale confirmation and old-owner fencing. Its additional
malformed-confirmation case preserves the existing RECOVERY_REQUIRED/OPERATOR_FAILURE
response; resume-specific errors do not change Reliability diagnostics.

Requeue injection fails each of runs/controls/audit_events and proves atomic PAUSED
preservation. A real lost requeue COMMIT acknowledgment resolves to exactly one applied
control and queue audit. Claim injection fails attempts/runs/coordinator/audit_events;
no claim or ownership survives rollback. Finalization injection fails all eight actual
mutation sites: artifacts, artifact_dependencies, stage_artifacts, stage_invocations,
attempts, runs, coordinator and RUN_SUCCEEDED audit. Each failure retains RUNNING ownership
and registers no Content artifact. All 13 newly completed Content files remain unchanged.
A pending CANCEL vetoes the same finalizer; it does not silently become success.

The two-source/one-completed-Silver fixture remains RECOVERY_REQUIRED with ownership
occupied; it cannot qualify a complete selection. Journal/Bronze/complete-Silver
recoveries remain PAUSED with RESUME_CHECKPOINT_UNSUPPORTED. Over-limit evidence is
rejected without truncation. Registered-but-missing output and existing native
namespaces never authorize a publisher. Ordinary verifier I/O and arbitrary exception
text map to unavailable evidence, separately from fixed observed-mutation codes.

### Reproduction and hosted coverage

Use the checked-in disposable runner with pinned native dependencies:

```sh
JAVA_HOME=$(/usr/libexec/java_home -v 21) \
DDIP_TEST_PYTHON="$PWD/outputs/silver-python312/.venv/bin/python" \
python3 scripts/test-rss-fetch-postgres.py --offline --content-resume
```

The Python path is an example local environment, not a runtime default. In the hosted
workflow the exact partition command and inventory check remain:

```sh
selection=$(python3 .github/ci/postgres_partitions.py --partition "$POSTGRES_PARTITION")
skip_units=true
if [ "$POSTGRES_PARTITION" = core ]; then skip_units=false; fi
mvn -B -ntp -f services/content-api/pom.xml verify -Ppostgres \
  "-Dit.test=$selection" "-DskipUnitTests=$skip_units"
python3 .github/ci/postgres_partitions.py --partition "$POSTGRES_PARTITION" \
  --reports services/content-api/target/failsafe-reports
```

ContentResumeControlIT belongs only to core; ContentResumeIT belongs only to gold.
The inventory contains 25 integration classes: core 10, reliability 7, ingestion 3,
gold 5. Classification and the aggregate gate are unchanged. The working-tree change
set selects all five backend jobs because it includes the partition inventory and V9.
No workflow, runner setting, job gate or timeout changed. Hosted validation of these
uncommitted changes has not run. The owner must run all selected jobs and all four
PostgreSQL partitions after committing/pushing; prior merged-baseline green runs do
not validate this implementation. The separate ingestion partition and unrelated full
JDBC/HTTP/backend jobs have not been rerun locally as part of this scoped validation.
The full Reliability runner performed all four real restart probes (bootstrap,
verification, finalization and packaged operator). Its log and complete XML inventory
are retained in `outputs/d5b2a-reliability.log` and
`outputs/d5b2a-reliability-reports/`. Actionlint 1.7.12, the 37 policy checks, Markdown lint, relative-link checking,
repository hygiene and `git diff --check` pass. V1–V8 were also compared directly against
HEAD and are byte-for-byte unchanged. Python logs are retained in
`outputs/d5b2a-python-native.log`; policy/classification/lint logs use the
`outputs/d5b2a-` prefix. No branch switch, staging, commit, push or GitHub write was performed.

## Changed files

Exact inventory (42 files):

- [.github/ci/postgres-partitions.json](../../.github/ci/postgres-partitions.json)
- [docs/adr/0019-explicit-content-resume.md](../../docs/adr/0019-explicit-content-resume.md)
- [docs/architecture/durable-execution.md](../../docs/architecture/durable-execution.md)
- [docs/operations/d5b2a-resume-validation.md](../../docs/operations/d5b2a-resume-validation.md)
- [docs/operations/managed-rss-fetch.md](../../docs/operations/managed-rss-fetch.md)
- [processing/content_resume_verifier.py](../../processing/content_resume_verifier.py)
- [processing/tests/test_managed_content.py](../../processing/tests/test_managed_content.py)
- [scripts/test-rss-fetch-postgres.py](../../scripts/test-rss-fetch-postgres.py)
- [services/content-api/src/main/java/platform/contentapi/execution/ContentRecovery.java](../../services/content-api/src/main/java/platform/contentapi/execution/ContentRecovery.java)
- [services/content-api/src/main/java/platform/contentapi/execution/ContentResume.java](../../services/content-api/src/main/java/platform/contentapi/execution/ContentResume.java)
- [services/content-api/src/main/java/platform/contentapi/execution/ContentResumeEvidence.java](../../services/content-api/src/main/java/platform/contentapi/execution/ContentResumeEvidence.java)
- [services/content-api/src/main/java/platform/contentapi/execution/ContentResumeLineage.java](../../services/content-api/src/main/java/platform/contentapi/execution/ContentResumeLineage.java)
- [services/content-api/src/main/java/platform/contentapi/execution/ContentResumePlan.java](../../services/content-api/src/main/java/platform/contentapi/execution/ContentResumePlan.java)
- [services/content-api/src/main/java/platform/contentapi/execution/OperatorRecoveryConfiguration.java](../../services/content-api/src/main/java/platform/contentapi/execution/OperatorRecoveryConfiguration.java)
- [services/content-api/src/main/java/platform/contentapi/execution/RecoveryOperatorInspection.java](../../services/content-api/src/main/java/platform/contentapi/execution/RecoveryOperatorInspection.java)
- [services/content-api/src/main/java/platform/contentapi/execution/RssContentExecution.java](../../services/content-api/src/main/java/platform/contentapi/execution/RssContentExecution.java)
- [services/content-api/src/main/java/platform/contentapi/execution/RssContentInputs.java](../../services/content-api/src/main/java/platform/contentapi/execution/RssContentInputs.java)
- [services/content-api/src/main/java/platform/contentapi/execution/RssGoldExecution.java](../../services/content-api/src/main/java/platform/contentapi/execution/RssGoldExecution.java)
- [services/content-api/src/main/java/platform/contentapi/execution/RssSilverExecution.java](../../services/content-api/src/main/java/platform/contentapi/execution/RssSilverExecution.java)
- [services/content-api/src/main/java/platform/contentapi/execution/SchemaLifecycle.java](../../services/content-api/src/main/java/platform/contentapi/execution/SchemaLifecycle.java)
- [services/content-api/src/main/java/platform/contentapi/execution/WorkerRepository.java](../../services/content-api/src/main/java/platform/contentapi/execution/WorkerRepository.java)
- [services/content-api/src/main/java/platform/contentapi/management/ControlCommands.java](../../services/content-api/src/main/java/platform/contentapi/management/ControlCommands.java)
- [services/content-api/src/main/java/platform/contentapi/management/ControlRepository.java](../../services/content-api/src/main/java/platform/contentapi/management/ControlRepository.java)
- [services/content-api/src/main/java/platform/contentapi/management/ResumeControlRepository.java](../../services/content-api/src/main/java/platform/contentapi/management/ResumeControlRepository.java)
- [services/content-api/src/main/java/platform/contentapi/management/RunInspectionValues.java](../../services/content-api/src/main/java/platform/contentapi/management/RunInspectionValues.java)
- [services/content-api/src/main/java/platform/contentapi/management/SourceFailure.java](../../services/content-api/src/main/java/platform/contentapi/management/SourceFailure.java)
- [services/content-api/src/main/resources/db/migration/V9__explicit_content_resume_control.sql](../../services/content-api/src/main/resources/db/migration/V9__explicit_content_resume_control.sql)
- [services/content-api/src/test/java/platform/contentapi/execution/ContentRecoveryFilesTest.java](../../services/content-api/src/test/java/platform/contentapi/execution/ContentRecoveryFilesTest.java)
- [services/content-api/src/test/java/platform/contentapi/execution/ContentRecoveryFixture.java](../../services/content-api/src/test/java/platform/contentapi/execution/ContentRecoveryFixture.java)
- [services/content-api/src/test/java/platform/contentapi/execution/ContentRecoveryIT.java](../../services/content-api/src/test/java/platform/contentapi/execution/ContentRecoveryIT.java)
- [services/content-api/src/test/java/platform/contentapi/execution/ContentResumeFilesTest.java](../../services/content-api/src/test/java/platform/contentapi/execution/ContentResumeFilesTest.java)
- [services/content-api/src/test/java/platform/contentapi/execution/ContentResumeIT.java](../../services/content-api/src/test/java/platform/contentapi/execution/ContentResumeIT.java)
- [services/content-api/src/test/java/platform/contentapi/execution/ImportOwnershipIT.java](../../services/content-api/src/test/java/platform/contentapi/execution/ImportOwnershipIT.java)
- [services/content-api/src/test/java/platform/contentapi/execution/OperatorRecoveryIT.java](../../services/content-api/src/test/java/platform/contentapi/execution/OperatorRecoveryIT.java)
- [services/content-api/src/test/java/platform/contentapi/execution/PersistenceIT.java](../../services/content-api/src/test/java/platform/contentapi/execution/PersistenceIT.java)
- [services/content-api/src/test/java/platform/contentapi/execution/RssContentTestSupport.java](../../services/content-api/src/test/java/platform/contentapi/execution/RssContentTestSupport.java)
- [services/content-api/src/test/java/platform/contentapi/management/ContentFinalizationIT.java](../../services/content-api/src/test/java/platform/contentapi/management/ContentFinalizationIT.java)
- [services/content-api/src/test/java/platform/contentapi/management/ContentRecoveryTestProcess.java](../../services/content-api/src/test/java/platform/contentapi/management/ContentRecoveryTestProcess.java)
- [services/content-api/src/test/java/platform/contentapi/management/ContentResumeControlIT.java](../../services/content-api/src/test/java/platform/contentapi/management/ContentResumeControlIT.java)
- [services/content-api/src/test/java/platform/contentapi/management/ControlCommandsTest.java](../../services/content-api/src/test/java/platform/contentapi/management/ControlCommandsTest.java)
- [services/content-api/src/test/java/platform/contentapi/management/PreflightCatalogFixture.java](../../services/content-api/src/test/java/platform/contentapi/management/PreflightCatalogFixture.java)
- [services/content-api/src/test/java/platform/contentapi/management/SourceManagementIT.java](../../services/content-api/src/test/java/platform/contentapi/management/SourceManagementIT.java)
