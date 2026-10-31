# D5b-2b inherited Content recovery and activation blocker

Implementation and validation record for the trusted local installation. Content worker
dispatch remains disabled; E5 backend closeout is **blocked**, not complete.
[ADR 0020](../adr/0020-inherited-content-reconciliation.md) is pending owner acceptance.

## Baseline inspection and acceptance criteria

Inspection covered durable-execution, managed-rss-fetch, D5b-1/D5b-2a validation, ADRs 0010
and 0013–0019, V8/V9 and PersistenceIT historical upgrades, C3c handoff/process review,
Content recovery/resume/finalization, worker startup/claims, profile/runtime/storage bindings
and the four PostgreSQL partitions with their aggregate gate. The owner-provided prerequisite
is merged D5b-2a with successful hosted CI; no GitHub operation was performed to reconfirm it.

The following matrix was established before implementation:

| Operation | Actual baseline | Resolution in this change |
| --- | --- | --- |
| Claim new Content | Internal/test capability only; packaged worker is Reliability-only | Unchanged; activation remains blocked |
| Claim resumed Content | Explicit operator execute-resume after qualification and exact-run claim | Existing protocol retained |
| Recover original attempt | C3c plus D5b-1 independent observations and terminal transaction | Regression required; no replacement protocol |
| Recover resumed attempt | Recovery reads only interrupted attempt stages, losing inherited prefix | Explicit ancestry projection and validation |
| Inspect inherited dependencies | Resume/finalization readers follow immutable audit links; recovery does not | Guarded recovery inspection includes checkpoint IDs, plan digest and ancestor child identities |
| Release ownership | Qualified fenced terminal transaction only | Same transaction; inherited proof must also qualify |
| Reject unsupported checkpoints | Journal/Bronze/Silver/partial-source RESUME rejected | Retained; recursive RESUME also rejected explicitly |
| Oversized private evidence | Above 4096 entries or 64 MiB is unavailable; ownership retained | Limits retained; activation blocker documented |

Acceptance for this increment: real original checkpoint → explicit recovery → applied RESUME →
new separately guarded EXECUTE → interruption → ancestry-aware reconciliation; exact native
publication and historical stages preserved; live/unknown identity, invalid lineage and changed
seal vetoes; terminal rollback retains ownership. Activation must not proceed without the
resource-readiness envelope and real API/ordinary-worker acceptance.

## Recovery decision matrix

| Evidence | Outcome |
| --- | --- |
| Original pre-fetch continuation, no retained Silver | Original empty-boundary evidence remains required; new work uses its actual attempt |
| Inherited committed Gold and complete verified Content result | Independently reopen all dependencies and Content; SUCCEEDED through the existing fenced reconciliation transaction |
| Inherited Content CHECK_INPUTS and complete result | Same; reused CHECK_INPUTS remains on its original attempt |
| No final publisher intent, inherited Gold or Content inputs intact, intended Content namespace absent | PAUSED; no native publisher is launched |
| Inherited Bronze/Gold or reference changed, seal/plan/checkpoint mismatch | Fail closed; retain occupied RECOVERY_REQUIRED |
| Live/ambiguous writer, missing evidence, unknown child outcome, expired review or stale authority | Fail closed; no release, retry, fetch, PUT or publication |
| Conclusively known failed intended publication | Existing D5b-1 classification only; no repair/overwrite; retry eligibility remains strict |
| Journal/Bronze/Silver/partial source prefix or another RESUME after inherited recovery | Explicit unsupported continuation; never infer pre-fetch from an empty resumed attempt |
| Private attempt over 64 MiB or 4096 entries | EVIDENCE_UNAVAILABLE; retain evidence and occupied ownership |

Ancestry is original EXECUTE → exact successful RECONCILE/PAUSED evidence → applied RESUME
and queued plan → claimed plan/new EXECUTE/reused_checkpoint_id → actual new stages → exact
Content result/dependencies. No timestamps, latest-artifact selection or copied stage rows
establish a link. The new reconciliation is a separate attempt; both EXECUTE histories remain.

## Operator procedure and capability boundary

Use the existing [guarded local recovery procedure](managed-rss-fetch.md) and
[resume procedure](d5b2a-resume-validation.md). Submit RECONCILE against the exact interrupted
resumed attempt and current run ETag. HTTP 202 means REQUESTED only. Stop handlers, review
all recorded children/descendants, launch gaps and unmanaged writers, then use the packaged
operator-recovery role with the original reviewed profile, runtime and control binding.
Both HANDOFF and FINALIZATION require fresh explicit confirmation. Review the inherited
checkpoint summary and the complete recorded-process list. Retain all private evidence,
original publication volumes, installation root identities and database history.
Before a qualified handoff, a crashed executor's stored run/attempt may still say RUNNING or
CLAIMED. A veto preserves those facts and the occupied slot; the fresh registration is
RECOVERY_ONLY. The qualified handoff establishes RECOVERY_REQUIRED and INTERRUPTED through
the existing transaction. A stale durable label does not authorize dispatch or writer adoption.

Default Content dispatch is disabled. There is **no approved Content worker enable switch in
this increment**. Source registration, submission and RESUME do not enable it. The packaged
worker still requires its reviewed Reliability profile, deployment, runtime and storage roots;
it cannot dynamically switch profiles. A new worker registration encountering occupied
ownership is recovery-only and cannot dispatch. Resolve explicitly, stop that process and
register a fresh compatible worker afterward. An expired heartbeat or a dead parent does
not clear the coordinator. Preserve the existing loopback API and mutation headers.

Disable/teardown for this increment means stop the operator/worker process with its existing
shutdown procedure and stop local services while retaining volumes. Do not use `down -v`,
delete publications, erase the control root or create a replacement installation to bypass an
occupied slot. Restore the same PostgreSQL volume, storage volumes, installation identities
and approved profiles before attempting explicit recovery. Disposable acceptance drivers
remove only their own generated containers/databases; this is not the operator data-retention policy.

PAUSE/CANCEL/RETRY execution remains disabled where incomplete. No analytics publication
selection, HTTP response envelope, schema version, ETag, pagination or idempotency contract changes.

## Evidence and resource audit

Gate B0 erratum, October 31, 2026: the original 32-feed/160 MiB claim was wrong. Content
admission has always enforced 1–8 feeds, each at most 5 MiB: **40 MiB of payloads**.
Reliability uses 1–32 sources. No code widening was found. See the [actual envelope audit](content-recovery-envelope.md)
for the remaining unproved sidecar/retention bounds; the old payload-only blocker calculation
must not be used. Private command/evidence/selection/anchor sidecars add bytes;
a two-source successful fixture has 11 managed stage rows and additional
verification sidecars. Reconciliation and resume qualification have their own retained private
trees; inherited recovery must retain and validate all of them. Limits apply per private tree,
not as a filesystem quota or an aggregate reservation across run history.

`ContentRecoveryFiles` refuses a tree above 4096 entries/64 MiB and inventory serialization
above 1 MiB. Recovery reads bound ordinary private files at 16 MiB and payloads at 5 MiB;
these are reader limits, not pre-write quotas. No evidence is
truncated, dropped, streamed past verification or deleted to fit. Runtime preflight observes at
least 1 GiB usable space on applicable control, Silver, scratch, Gold and Content filesystems
and checks pinned root device/inode identities, real paths and non-overlap. These are point-in-time
checks, **not hard disk quotas or reserved capacity**. Native supervisors clear the child environment,
bound each output stream at 16 MiB, observe at most 32 descendants and enforce reviewed stage
deadlines (Silver/Gold/Content profiles at most 900 seconds) plus the existing attempt deadline.
None proves private evidence will remain recoverable throughout every admitted Content run.
Operator review still expires after 60 seconds and permits at most 32 reviewed descendant
identities; ancestry cannot silently discard earlier identities to fit that bound. Long or
large qualifications can therefore remain unavailable even when their writer has exited.

Activation requires either an enforceable admitted/retained evidence envelope or a separately
reviewed bounded validator improvement with its own supported envelope. This change establishes
neither and intentionally leaves normal Content dispatch unavailable. Raising a limit alone is
insufficient. No normal Content API/worker scenario A or subsequent ordinary Content execution
is claimed. Inherited recovery is the completed integration increment; Gate B remains unimplemented.

## Reproduction and validation

Use Java 21, the repository-pinned Python 3.12.15/Spark 4.0.2/DuckDB 1.4.5 environment and
the pinned disposable PostgreSQL 17.11/SeaweedFS 4.48 images in the driver. Tests use synthetic
loopback feeds and qualified reference snapshots, never public feeds or cloud credentials.

```sh
export JAVA_HOME=$(/usr/libexec/java_home -v 21)
export DDIP_TEST_PYTHON="$PWD/outputs/silver-python312/.venv/bin/python"
python3 scripts/test-rss-fetch-postgres.py --offline --content-inherited-recovery
TZ=UTC SPARK_LOCAL_IP=127.0.0.1 PYTHONPATH=processing \
  PYSPARK_PYTHON="$DDIP_TEST_PYTHON" PYSPARK_DRIVER_PYTHON="$DDIP_TEST_PYTHON" \
  "$DDIP_TEST_PYTHON" -B -m unittest discover -s processing/tests -p 'test_managed_*.py' -v
python3 -I -B -m unittest discover -s .github/ci/tests -v
python3 .github/ci/postgres_partitions.py
npm run check
git diff --check
```

Validation results and exact changed-file inventory are recorded below.
Hosted Ubuntu validation remains owner-run. V1–V9 SQL bytes/checksums and historical upgrade
coverage must remain intact. The new integration class belongs to Gold; no timeout or aggregate
gate was relaxed. All four PostgreSQL partitions, ingestion, processing/JDBC, analytics HTTP and
frontend remain required by the unchanged change-aware classifier for shared CI changes.

## Native evidence and scope of acceptance

The five final inherited cases execute the production D1–D5a adapters and C3c handoff,
using separate original and resumed executor JVMs. The ordinary packaged Content worker is
not enabled or tested. This is inherited recovery acceptance, not scenario A activation evidence.

The final two-source Gold case retained original attempt
`309112fa-cb4e-40f7-85d1-ffa15d44a170`, resumed attempt
`c08763d1-7b9e-499f-a0b3-05b521bd362c` and reconciliation
`61714dc4-1a38-4397-b740-d6b0058c404c`. It independently recovered Content artifact
`015fd217-a25c-4cf3-abc9-072bb048702f` as SUCCEEDED. PostgreSQL postmaster time changed
from `2026-10-31T14:36:10.746849Z` to `2026-10-31T14:45:54.076404Z` while that resumed
attempt still occupied the coordinator. A fresh registration remained recovery-only before
explicit handoff. The test dropped the actual RECOVERY_FINALIZED COMMIT acknowledgment;
read-only resolution confirmed exactly the committed result without another publication.

The Content-input case recovered artifact `03975d86-c41c-4e7a-b259-3586f22beb0e` using
original attempt `05b1dc2c-0f6a-4c28-bfc2-6adf2e1318e2` and resumed attempt
`ce64c0ad-2fc1-499c-bdb4-b01bd2ac6583`. Both successful cases compare all 13 completed
Content files by bytes/hash, mtime and inode across reconciliation. They also compare original
Silver/Gold trees, original private inventories, stage histories and delivery rows. Recovered
Content links the original Gold artifact and captured reference; output article/discovery counts
equal the exact selected synthetic source count. No inherited stages are registered on the new
EXECUTE. Old Gold/Content preparation stays attached to its actual attempt.

The three earlier-boundary cases retain PAUSED, release only after qualification and reject a
further RESUME explicitly. The pre-fetch case performs no retrieval. Tests reject a live executor
guard/control admission, independently LIVE/UNAVAILABLE ancestor observations, missing/corrupt
resume evidence and changed original private commands. PostgreSQL itself rejects changing the
immutable reused checkpoint. Each refusal preserves ownership and registers no Content result.
Both complete-result cases inject rollback at artifacts, dependencies, stage linkage, attempts,+runs, controls, workers, coordinator and audit mutations. All nine probes preserve the occupied
slot, REQUESTED control and native publication. They do not rerun the native publisher.

No API-submitted ordinary Content worker run, subsequent ordinary Content execution, full
activation shutdown/restoration scenario, full expanded Gold partition or hosted Linux validation
is claimed here. The evidence-envelope gate and Gate B implementation/acceptance remain E5
backend closeout blockers. Recursive RESUME and other excluded controls remain operational gaps.

## Measured local validation

macOS 27.2/arm64, Docker Desktop, GraalVM Java 21.0.7, Python 3.12.15, Spark 4.0.2,
DuckDB 1.4.5, pinned PostgreSQL 17.11 and SeaweedFS 4.48. These are correctness runs.
Class durations come from final Maven XML, not throughput measurements.

| Validation | Distinct cases | Measured elapsed | Actual result |
| --- | ---: | ---: | --- |
| Combined selected backend Maven run | 157 integration + 344 unit | 20m50s Maven; 1253.01s wrapper including cleanup | All cases passed, zero errors/failures/skips; wrapper bookkeeping exception described below |
| New inherited recovery class, included above | 5 integration | 501.451s | Passed |
| Original D5b-1 recovery, included above | 7 integration | 263.463s | Passed |
| D5b-2a resume regression, included above | 7 integration | 384.620s | Passed |
| Historical persistence/migrations, included above | 30 integration | 13.660s | Passed |
| Content terminal, worker coordination and resume control, included above | 108 integration | 68.691s combined class time | Passed |
| Reliability C3c: finalization, handoff, verification, packaged operator | 247 integration + 53 overlapping unit | 541.21s driver | Passed; four actual PostgreSQL restart probes |
| Tracked standalone inherited driver, fast pre-fetch case | 1 overlapping integration | 9.40s driver | Passed, including wrapper restart expectations and cleanup |
| Managed native Python adapters | 49 | 22.417s | Passed with pinned driver/worker Python and UTC |
| CI policy, inventory and aggregate gates | 37 | 3.804s | Passed; 26 integration classes assigned exactly once |
| Markdown/local links, repository hygiene and diff whitespace | Consistency checks | Not used as a benchmark | Passed |
| V1–V9 SQL byte/checksum comparison with HEAD | 9 files | Not used as a benchmark | All unchanged |

The combined local wrapper expanded its Maven selection to include ContentRecoveryIT but
retained the standalone inherited-only expectation of zero controller restart signals. Maven
finished BUILD SUCCESS with all 501 selected cases passing; the wrapper subsequently exited 1
because it correctly observed the additional original-recovery restart. The log records
`Actual PostgreSQL restart: content-recovery` exactly once and no controller errors. That receipt
was separately checked against the expanded selection and the ignored local wrapper expectation
corrected. This bookkeeping failure is not presented as a passing wrapper execution. The tracked
standalone `--content-inherited-recovery` driver still selects only its five cases and retains
the correct independent restart expectation.

C3c class durations: finalization 53 cases/113.352s, handoff 118/91.340s,
verification 40/81.806s and packaged operator 36/249.955s. Its live/ambiguous writer,
stale epoch, host guard, launch-gap, lost-acknowledgment, rollback and worker lifecycle
regressions passed; the four restart receipts are bootstrap, verification, finalization
and operator. Distinct final coverage is **404 PostgreSQL integration + 344 Java unit +
49 Python adapter + 37 CI-policy cases = 834 cases**, all passing. The extra 53 C3c units
and the standalone case overlap the combined run and add no distinct cases.
Machine-readable counts are `outputs/d5b2b-distinct-test-counts.json`; selected XML snapshots
are retained separately for native and C3c runs to exclude stale reports from counting.

Development failures also exposed a test probe attempting a forbidden immutable checkpoint
update and a pre-fetch interruption hook placed after retrieval. Both test seams were corrected
before the final passing Maven run. Early separate Python invocations lacked sandbox loopback,
pinned worker Python or UTC; only the final fully configured 49-case result is counted.

Reports/logs and machine-readable case inventory are retained under ignored `outputs/d5b2b-*`.
Repeated focused runs and the table's included rows are not additional distinct cases. The new
Gold class adds an observed 8m21.451s locally. D5b-2a's historical full Gold driver measured
2665.87s; this is a historical comparison, not a measurement of the expanded partition. A fresh
hosted run must establish its total within 90 minutes. If that fails, evaluate a measured split
with inventory/fan-in coverage; do not omit tests or silently increase the deadline.

## Exact changed-file inventory

- `.github/ci/postgres-partitions.json`
- `README.md`
- `docs/adr/0020-inherited-content-reconciliation.md`
- `docs/adr/README.md`
- `docs/architecture/durable-execution.md`
- `docs/operations/ci.md`
- `docs/operations/d5b2a-resume-validation.md`
- `docs/operations/d5b2b-inherited-recovery-validation.md`
- `docs/operations/managed-rss-fetch.md`
- `scripts/test-rss-fetch-postgres.py`
- `services/content-api/src/main/java/platform/contentapi/execution/ContentRecovery.java`
- `services/content-api/src/main/java/platform/contentapi/execution/ContentRecoveryAncestry.java`
- `services/content-api/src/main/java/platform/contentapi/execution/ContentResume.java`
- `services/content-api/src/main/java/platform/contentapi/execution/ContentResumeLineage.java`
- `services/content-api/src/main/java/platform/contentapi/execution/ContentResumePlan.java`
- `services/content-api/src/main/java/platform/contentapi/execution/RecoveryHandoff.java`
- `services/content-api/src/test/java/platform/contentapi/execution/ContentInheritedRecoveryIT.java`
- `services/content-api/src/test/java/platform/contentapi/execution/ContentInheritedRecoveryProcess.java`
