# Controlled local Reliability recovery (E5 C3c.4)

This is an explicitly authorized local operation for one interrupted managed Reliability run.
It connects C3a admission, C3b handoff and C3c bootstrap/verification/finalization. It is not automatic
recovery, distributed failover, an exactly-once guarantee or a production-readiness claim.
[ADR 0014](../adr/0014-controlled-operational-recovery.md) records the integration choices without
owner-accepted ADR status. ADRs 0010/0011 remain accepted; 0012/0013 remain implementation records.

## Prerequisites and exact target selection

- Use the original installation, private CONTROL root, persistent host.guard and installation.id,
  approved storage/run/runtime profiles, retained original private stage files and native inputs/output.
  Preserve file identity and permissions; never recreate the guard, edit ownership SQL or repair output.
- Use JDK 21 and the run's approved Python 3.12.15, PySpark 4.0.2 and DuckDB 1.4.5 deployment.
  The compatibility allowlist remains deliberately narrow. No Maven, pip, fetching or publisher runs
  inside recovery. PostgreSQL 17 must already have the compatible migrated schema.
- Stop worker restart services, execution handlers and unmanaged/legacy publishers. Perform the
  [C3a process and orphan review](../../services/content-api/README.md#operator-safety-checklist),
  including Python, Spark JVM and Py4J descendants, reparented children and uncertain launch gaps.
  A free guard, absent parent, reused PID or expired heartbeat proves no universal quiescence.
- GET the exact run/current attempt and its ETag through the existing management API. Follow the
  [C3a admission commands](../../services/content-api/README.md#c3a-reconciliation-intent-only) to submit
  RECONCILE with that attempt, fresh reviewed assertion, idempotency key and
  If-Match. Preserve the exact returned controlId. Resolve ambiguous admission by replaying that
  request/key; this command does not admit, discover or select a different control.
- Export the existing private DDIP_DB_URL/USER/PASSWORD and DDIP_INSTALLATION_ID, and reviewed
  DDIP_WORKER_EXECUTION_PROFILE_ID, DDIP_WORKER_CONTROL_BINDING_ID,
  DDIP_WORKER_RUNTIME_PROFILE_ID and DDIP_WORKER_DEPLOYMENT_SHA256. These are installation settings,
  not operator-supplied arbitrary commands or native addresses. Run as the installation's trusted OS user.

## Inspection and confirmation

Build once from the repository root, then select the exact canonical control UUID:

```sh
mvn -q -f services/content-api/pom.xml -DskipTests package
export DDIP_RECOVERY_CONTROL_ID="$CONTROL_ID"
java -jar services/content-api/target/content-api-0.1.0-SNAPSHOT.jar \
  --ddip.role=operator-recovery --ddip.recovery.action=inspect
java -jar services/content-api/target/content-api-0.1.0-SNAPSHOT.jar \
  --ddip.role=operator-recovery --ddip.recovery.action=recover
```

`inspect` is read-only: it registers no worker, acquires no authority and launches no native process.
It emits one bounded JSON summary of IDs, fixed states/versions, occupancy, selected-owner consistency,
recorded PID/start observations, stages, unresolved checkpoints and required manual checks.
`canQualify` means only that the selected metadata/process observations permit further C3b qualification;
it says nothing about native validity, private file availability or universal quiescence.
`authorization` is always false. An inspection result cannot be passed as a recovery token.

`recover` first acquires the approved host guard and registers the exact compatible RECOVERY_ONLY
process. It displays a fresh CONFIRMATION_REQUIRED/HANDOFF JSON object. The random challenge,
inspectionSha256, target digest and private inventory digest bind the live process's exact inspection.
Review its summary and evidence, every unresolved checkpoint and recorded process identity/status,
descendantPidHints, lineageCheckpoints, lineageEvidenceSha256 and previouslyReviewedDescendants.
Investigate the private retained files through the existing trusted local procedure; the summary
deliberately does not print native payloads, private addresses, secrets, SQL or exception traces.

After actual manual review, enter **one JSON line** with these exact fields. The placeholders below
must be replaced from the current prompt and reviewed local process evidence; this is not executable
approval automation. Do not paste the pretty-printed example as multiple lines.

```json
{
  "approval": "AUTHORIZE_HANDOFF",
  "challenge": "current-prompt-challenge",
  "inspectionSha256": "current-prompt-inspection-digest",
  "recordedProcesses": [],
  "reviewedCheckpoints": [],
  "lineageCheckpoints": [],
  "handlersStopped": true,
  "unmanagedWritersStopped": true,
  "orphanReviewComplete": true,
  "reviewMethod": "LOCAL_PROCESS_TREE_AND_WRITERS_REVIEWED",
  "descendants": []
}
```

Repeat recordedProcesses exactly from `evidence.recordedProcesses`, reviewedCheckpoints exactly from
`evidence.unresolvedCheckpoints`, and lineageCheckpoints exactly from `evidence.lineageCheckpoints`.
The recordedProcesses list ordinarily contains the former worker; the empty arrays in the example
are placeholders, never default answers. `descendants` must list every known additional descendant
using `{"pid":"12345","startedAt":"2026-10-25T12:00:00Z"}`. Include every previously reviewed
descendant. An empty descendant list asserts that the manual process-tree/orphan/writer review found
none beyond the recorded identities; it is not an automated proof. A PID-only hint requires a reviewed
start identity and must currently be absent; a live reused PID remains ambiguous for that hint.
Unknown writers or unknown start identities must be investigated, not assigned invented timestamps.

The reply is capped at 128 KiB and must arrive within 60 seconds of that inspection. Extra/missing
fields, false assertions, changed lists/digests, a wrong challenge, stale review, live/unknown process
or changed private/relational evidence reject recovery. A boolean configuration flag cannot authorize it.
The local OS operator is the trust boundary; JSON from a pipe has the same authority as interactive
input and must only be supplied by a trusted controller after actual human review.

After handoff, the same JVM bootstraps the exact committed packet and independently authorizes that
live process. The first fixed native verifier emits OBSERVED with its safe classification and whether
finalization qualification is supported. Unsupported/unavailable evidence stops occupied immediately.
Otherwise a fresh CONFIRMATION_REQUIRED/FINALIZATION prompt includes the verification checkpoint,
evidence digest, recovery instance/epoch and verifier-child identity. Repeat the manual review and
respond with AUTHORIZE_FINALIZATION and the new challenge/digest/lists. Prior confirmation is invalid.
This authorizes the second independent read-only verifier, fresh byte/identity seals and exact-owner
atomic disposition. Both approvals retain the existing 60-second freshness bounds; long/expired
qualification remains blocked rather than refreshing authority from history.

## Outcomes and interruption procedures

| Observed and freshly qualified evidence | Operational result |
| --- | --- |
| Complete exact native publication and dependencies | COMPLETED, SUCCEEDED / RECOVERED_PUBLICATION |
| Positively absent, incomplete or proven invalid publication | COMPLETED, FAILED / PUBLICATION_ABSENT, INCOMPLETE_OUTPUT or PUBLICATION_INVALID |
| Corroborated changed publication/dependency or invalid native dependency | COMPLETED, FAILED with the existing C3c.3 qualified classification |
| Unavailable/contradictory evidence, unknown process/cleanup, stale token, outage, unsupported runtime or pending cancellation | RECOVERY_REQUIRED; no disposition or coordinator release |

Successful command exit 0 means either a read-only inspection completed or exact terminal recovery
facts were confirmed. Inspect the status field: INSPECTED is not COMPLETED. A COMPLETED recovery can
resolve the run as FAILED. Recovery uncertainty/input/configuration failure exits 2 with a fixed code.
No native publication, fetch, repair, deletion, CANCEL or RETRY is performed. An absent-output retry
flag is only eligibility evidence; it does not schedule an attempt.

- Before handoff: termination or rejected/stale approval leaves the original occupied owner. Reinvoke
  the same control for a new inspection/review. Unused recovery registrations are historical only.
- After handoff: repeating the old control returns FRESH_QUALIFICATION_REQUIRED. It never revives the
  old instance, relaunches its verifier intent or clears the coordinator. Stop restart handlers, review
  all original/recovery descendants, then admit a **new** C3a control targeting the currently occupied
  recovery attempt and current run ETag. Run this workflow for that control. Immutable handoff packets
  are followed back to the original EXECUTE evidence, bounded by twenty attempts; all ancestor private
  inventories, recorded processes, unresolved checkpoints and retained descendants must be reviewed
  again. Missing/changed/unsupported ancestry stays blocked. No prior owner token is reconstructed.
- Lost transfer acknowledgment: only the same still-guarded JVM can resolve the exact committed
  packet and separately authorize its live process. Rollback/uncertainty exits occupied. After JVM
  replacement use the preceding new-control procedure if a successor exists.
- Lost final acknowledgment: resolve the exact finalization/control/checkpoint/qualification facts.
  Reinvoking an already completed control performs historical verification without registering a
  worker, appending an attempt/audit/artifact, or touching a newer owner. Unresolved facts stay blocked.
- Database outage/restart: retain evidence and occupied ownership. No heartbeat-expiry takeover exists.
  Restore the original database, inspect exact facts and follow the appropriate pre/post-handoff path.
- Guard contention: identify the holding process and investigate. Never unlink/recreate host.guard,
  force-clear SQL pointers or treat obtaining a different lock as permission.

| Fixed diagnostic | Investigation |
| --- | --- |
| QUIESCENCE_REQUIRED / PROCESS_UNRESOLVED | Review the exact process identities, launch gaps, descendant hints and unmanaged writers; unknown identity is a veto |
| FRESH_QUALIFICATION_REQUIRED | Inspect exact durable facts; use fresh confirmation before handoff, or a fresh target control after an interrupted successor |
| EVIDENCE_CHANGED / EVIDENCE_UNAVAILABLE | Preserve all evidence; investigate missing files, changed identity/bytes and access failures without repair or substitution |
| RUNTIME_MISMATCH | Compare the approved profile, pinned runtime, deployment and root identities; unsupported deployments remain blocked |
| OWNERSHIP_LOST / HANDOFF_UNRESOLVED | Inspect the exact control/current attempt and coordinator binding; never adopt a different instance/version |
| MANAGEMENT_UNAVAILABLE / STARTUP_UNAVAILABLE | Restore the original local database/configuration and inspect exact committed facts; no rollback or death is inferred |
| HOST_GUARD_OCCUPIED / HOST_BINDING_INVALID | Identify the holder and verify the original private binding/guard identities; never recreate files to bypass the check |
| CAPACITY_EXCEEDED / INVALID_REQUEST / OPERATOR_FAILURE | Check documented bounds/configuration/input and preserve the occupied owner for investigation |

After safe atomic release the coordinator is READY and the recovery worker is STOPPED. Exit the
operator process and start an ordinary worker with the existing reviewed startup procedure. Only its
fresh guard/process registration may claim unrelated queued work. Recovery never changes its own
session into a dispatching worker. Keep all private qualification/stage evidence and native data;
there is no automatic retention cleanup.

## Reproducible acceptance and limits

The dedicated harness creates and removes its own loopback PostgreSQL container using the pinned
17.11 image digest. It never uses an existing database. Set JAVA_HOME to JDK 21 and DDIP_TEST_PYTHON
to an absolute interpreter in the pinned Python/native environment from the repository setup.

```sh
python3 scripts/test-recovery-postgres.py --operator-only --offline
python3 scripts/test-recovery-postgres.py --offline
python3 scripts/test-recovery-postgres.py --full --offline
npm run check
```

Omit --offline when Maven dependencies have not been cached. Focused mode runs recovery/process/
history/role units and all four recovery integration classes. Full mode runs all content-api units
and PostgreSQL integration suites. These two modes coordinate four **actual** disposable PostgreSQL
restarts at the existing bootstrap/verification/finalization barriers and the packaged operator
inspection barrier, verifies postmaster identity changes, reports total elapsed time and removes its
container. Maven XML reports retain per-suite/case times under ignored target directories. The actual
recovery tests invoke java -jar, not a test replacement application; the acknowledgment proxy and
fixture publishers are test-only and excluded from the production JAR.

The narrower --operator-only mode runs the packaged operator scenarios and role units with one
actual restart at the operator barrier. It also starts actual packaged ordinary workers, interrupts
them after JSONL/Parquet publication at a test-controlled verification boundary, reviews recorded
descendant identities and recovers without changing native hashes or modification times.

Acceptance results and exact changed files are recorded in the
[C3c.4 implementation evidence](../architecture/durable-execution.md#c3c4-controlled-operational-recovery).
Earlier C3c.3 counts/restarts are historical evidence, not results from this checkpoint.

The existing 8 GiB/8,192-entry/depth-8 native-seal bounds and cooperative filesystem deadlines are
unchanged. Large trees, hard blocked-I/O deadlines, hostile filesystem mutation, universal OS process
containment, power-loss atomicity, hosted CI and production operations remain unqualified. Filesystem
observations and SQL commit are not atomic. CANCEL/RETRY, RSS/Content managed execution, multiworker
operation, automatic recovery, Operations UI and broader E5 release work remain outside this slice.
