# Native Content restart qualification

November 8, 2026. This is qualification work for the integrated original-run Content chain,
not ordinary dispatch activation. Issue #49 remains open. Continuation and recovery-owner
reauthorization remain separate deferred work in issues #123 and #124.

See [the original pipeline](integrated-original-content-pipeline.md) and
[the reviewed safe-end contract](../adr/0044-interrupted-content-safe-end-contract.md).
The existing FETCH-only terminal command intentionally rejects downstream Bronze history;
that exclusion is not removed by these tests or by the detached full-prefix assessment.

## Restart evidence

`NarrowContentPipelineRestartIT` runs the real original FETCH/Bronze/Silver/Gold/Content modules
against isolated PostgreSQL and synthetic loopback input/storage. Its four cases passed locally
with zero failures, errors or skips (602.6 seconds) before V22 integration, each proving a changed PostgreSQL postmaster
start time rather than simulating a reconnect:

- Stop after the first Bronze-inspection checkpoint (step 2).
- Stop after Silver recheck (step 5).
- Stop after final Content recheck (step 16), before final success.
- Lose acknowledgment after the final success transaction commits.

The tests compare every management-table row, including stored proof bytes, plus filesystem
snapshots before and after the database restart. Interrupted attempts retain global occupancy;
fresh worker registration is recovery-only, and rejected replay performs no new feed or object
requests. The committed final result remains verified historical evidence with an already released
slot. Inspection never returns continuation authority.

## Abrupt worker death and current limitations

`NarrowContentPipelineCrashIT` uses a separate worker JVM and kills it at precise durable
boundaries, including a recorded child, live Spark processing, the transactions after Silver/Gold
artifact registration, and inside/after final success commit. Test-owned descendant cleanup is
only disposable-fixture cleanup, never evidence of operator quiescence or an automatic recovery
procedure. All eight cases are locally qualified across the integrated run (six passing cases)
and a focused passing rerun of the two post-registration cases after correcting their test-only
absent-address expectation. The stable implementation commits safe-end after Silver/Gold recheck,
after their artifact registration, and before final success. Each checks preserved bytes and native
request counts, SQL closure, and receipt lookup in a fresh JVM. Live-child and live-Spark cases
prove containment only; they do not substitute fixture cleanup for trusted-local review.

The post-registration cases have no derived next-publication address yet, so they require zero
absent bindings and test closure of retained artifacts. The pre-registration cases test rejection
of new rows at the known absent addresses. All cases retain ordinary-dispatch gating.

Focused qualification also passed 35 SQL cases, 45 lifecycle/operator cases, 12 lifecycle/evidence
units, 13 historical-upgrade cases and two compatibility units. Four additional current-source
FETCH compatibility checks passed after the final fail-closed schema adjustment. The full-chain
read-only assessment passed against V22. Final hosted CI reruns the current complete candidate,
including database restarts; it remains a required gate before merge.

The read-only `NarrowContentPipelineRecoveryAssessment` separates historical SQL facts, present
checkpoint byte/inventory observations, and recorded-process observations. An incomplete active
operation is not presented as a verified whole evidence tree. It cannot mint an owner, release
occupancy, complete partial files, retry effects, or infer stopped unmanaged writers from absent
PIDs. Its current-byte checks do not establish historical inode continuity or reverify external
native storage.

Database restart survival and abrupt-process containment do not by themselves authorize terminal
recovery or ordinary dispatch. The separately versioned terminal lifecycle still requires its
reviewed phase/files rules, fresh exact-target trusted-local review, atomic SQL closure and
independent implementation/fault qualification. Pre-READY, unknown/mutated evidence and rejected
link-count states remain occupied rather than being repaired.

## Staging-only direct safe-end operator

The packaged `native-content-end` command is separate from `native-fetch-end`. It implements
ADR 0044's original Content failed end-state, including eligible incomplete prefixes; it does not
change the FETCH-only command, enable ordinary dispatch, or authorize an actual retained-data
operation. Until independent implementation review and final integrated qualification are complete,
use this procedure only in explicitly authorized disposable qualification/staging installations.
Any later operation on real retained data still needs exact-target authorization.

### Preconditions and exact target

Use Java 21 and the packaged content-api JAR built from the candidate being qualified, with all
migrations through V22 applied. Use the existing local database configuration (`DDIP_DB_URL`,
`DDIP_DB_USER`, nonblank `DDIP_DB_PASSWORD`, and `DDIP_INSTALLATION_ID`); do not put credentials
in the command line or confirmation transcript. The registered execution profile, deployment digest
and control binding must be those of the original installation. The command requires the original
host and process namespace and acquires the installation guard there. Matching guard bytes or an
available lock alone does not establish deployment identity.

Before invoking it, independently stop and review execution handlers and all relevant local,
unmanaged and remote writers, including Spark gateways, descendants, launch gaps and orphans.
Do not kill processes or alter retained files as part of this command. The operator must be able to
positively attest that the writers have stopped; an `ABSENT` or `REUSED` PID observation is only a
negative check. `LIVE` or `UNAVAILABLE` observations veto qualification.

An existing exact `REQUESTED` `RECONCILE` control must identify the original attempt. This command
does not create one, revive a rejected control, reinterpret cancellation, or adopt a FETCH recovery
owner. Competing requested controls or applied cancellation, an active import, an unsealed pre-READY
root, contradictory success, changed/missing required evidence, unknown files and rejected link-count
states all leave the original slot occupied. Never remove a second hardlink or complete a partial
marker to make a prefix pass.

```sh
java -jar services/content-api/target/content-api-0.1.0-SNAPSHOT.jar \
  --ddip.role=operator-recovery \
  --ddip.recovery.action=native-content-end \
  --ddip.recovery.control-id=EXACT_REQUESTED_RECONCILE_UUID \
  --ddip.worker.execution-profile-id=ORIGINAL_REGISTERED_PROFILE \
  --ddip.worker.control-binding-id=ORIGINAL_CONTROL_BINDING \
  --ddip.worker.deployment-sha256=ORIGINAL_DEPLOYMENT_SHA256 \
  --ddip.recovery.same-host-process-namespace=true
```

The namespace option is a required explicit assertion, not namespace discovery. The command prints
`NATIVE_CONTENT_PLAN` with `terminalId`, `terminalAttemptId`, `runId`, `originalAttemptId` and
`controlId`. Retain this output before responding. These UUIDs are historical inspection addresses,
not execution capabilities. The intended disposition is `FAILED / NATIVE_CONTENT_INTERRUPTED`,
retry disabled, with no final Content artifact or counts. Existing native publications may remain;
the command does not claim their absence, verify external native storage, clean up, repair or resume.

### One fresh interactive review

After bounded SQL and retained-file inspection, the command prints one `CONFIRMATION_REQUIRED`
challenge with phase `NATIVE_CONTENT_TERMINAL`. Verify its complete summary against the authorized
run, original attempt and control. Its target digest binds the exact SQL snapshot, the current bounded
file inventory, and a fresh inspection nonce. A new inspection requires a new response even when the
UUIDs and SQL rows are unchanged.

Only after completing the trusted-local review, enter one newline-terminated JSON object on standard
input within 60 seconds of the challenge inspection. There is no approval-file or configuration
shortcut. Do not replay a saved response or automatically pipe affirmative answers. The object must
contain exactly these fields:

- `approval`: `AUTHORIZE_NATIVE_CONTENT_TERMINAL`.
- `challenge`, `inspectionSha256`, `targetSha256`, `reviewedCheckpoints`, and `recordedProcesses`:
  copy these five values unchanged from this challenge, including the complete arrays.
- `handlersStopped`, `unmanagedWritersStopped`, `orphanReviewComplete`,
  `sameHostAndProcessNamespace`, `descendantsReviewed`, `launchGapsReviewed`,
  `sparkGatewaysReviewed`, and `remoteWritersStopped`: each must be the JSON boolean `true`,
  based on the actual review, not merely on the displayed negative PID observations.
- `reviewMethod`: `LOCAL_PROCESS_TREE_AND_ALL_WRITERS_REVIEWED`.
- `descendants`: the complete additional reviewed identity array, at most 32 unique entries.
  Each entry has exactly `pid` (a positive decimal string) and `startedAt` (an ISO-8601 instant).
  An empty array is appropriate only when the review established there are no additional identities.

The response is bounded to 16 KiB; the canonical persisted review must also fit its 8 KiB limit.
Do not omit mandatory identities or shorten the review to get under a bound. Refuse the operation
if a required identity or writer cannot be reviewed. Before mutation, the implementation reopens
retained evidence, compares the reviewed inventory, repeats process observations outside SQL, and
checks review freshness again after acquiring the coordinator lock. These checks supplement rather
than replace the stopped-writer attestation.

### Outcomes, refusal and read-only resolution

A `COMPLETED` event with `terminalStatus` `COMMITTED` or `CONFIRMED_TERMINAL`, and exit code 0,
means the atomic terminal disposition was established. The original attempt is interrupted, a new
already-terminal reconciliation attempt records failure reconciliation, the control is applied, the
terminal worker is stopped, and the fenced coordinator slot is released. Inside that one transaction,
the receipt is inserted before lifecycle changes so an immediate guard can verify the actual original
snapshot; deferred checks prevent committing it without the complete final disposition and audits.
There is no intermediate recoverable owner or continuation token. The original worker history and retained native evidence
are not rewritten to fabricate cleanup.

A refusal before mutation, timeout, malformed response or stale evidence does not authorize a retry
or any evidence repair. The command may emit `RECOVERY_REQUIRED` with a fixed diagnostic and exit 2.
It can have registered a recovery-only operator worker, but it has not thereby acquired execution
or terminal ownership. A fresh attempt requires a new exact-target inspection and review.

Once a transaction may have committed, a lost acknowledgment or postcommit guard/freshness failure
is resolved rather than reported as rollback. `NATIVE_CONTENT_UNRESOLVED` with exit 2 reports the
available `terminalStatus`: `CONFIRMED_ROLLBACK` means the exact prior SQL state was established;
`UNRESOLVED` means the outcome could not be established. Do not blindly repeat the mutation.
Receipt absence alone, process death, and an unavailable database are not proof of rollback.

Inspect the saved `terminalId` using a fresh process:

```sh
java -jar services/content-api/target/content-api-0.1.0-SNAPSHOT.jar \
  --ddip.role=operator-recovery \
  --ddip.recovery.action=native-content-inspect \
  --ddip.recovery.terminal-id=SAVED_TERMINAL_UUID
```

This command uses the same database/installation configuration but requires no guard, worker
profile, current evidence files, process inspection or new attestation. It prints
`NATIVE_CONTENT_INSPECTED`, `terminalStatus` and `authorization: false`. Exit 0 means
`CONFIRMED_TERMINAL`; exit 2 means the historical receipt is unresolved. It reconstructs retained
SQL ancestry and checks receipt/audit/disposition correspondence, including after unrelated queue
work has reused the released slot. It never reconstructs a qualification or owner. This is terminal
receipt lookup, not a general original-success lookup; a committed original success is ineligible
for failed safe-end and remains subject to the existing historical pipeline assessment.

## Focused safe-end qualification status

Focused local Java lifecycle, phase/files, exact-review, packaged-operator and direct SQL closure
checks have passed. These include current-inventory changes, fresh nonces, conservative hardlink
refusal, six-event rollback, real JDBC acknowledgment loss, lock-wait expiry, postcommit guard/expiry
resolution and historical lookup after unrelated queue work. The packaged operator tests use an
isolated original-worker identity captured at insertion and a real local process observer; they are
not substitutes for the separate abrupt original-worker JVM tests above.

Final integrated downstream crash/terminal disposition qualification, independent final-head review,
and complete CI/capacity evidence remain pending. The local focused results do not activate ordinary
dispatch or authorize actual retained-data disposition.

## Running the qualification

Use only disposable databases and storage with the pinned Java/Python/native dependencies described
in the original-pipeline procedure. Build the ingestion adapters first. The normal PostgreSQL test
profile supports:

```sh
mvn -f services/content-api/pom.xml -Ppostgres -DskipUnitTests=true \
  -Dit.test=NarrowContentPipelineRestartIT verify
mvn -f services/content-api/pom.xml -Ppostgres -DskipUnitTests=true \
  -Dit.test=NarrowContentPipelineCrashIT verify
```

The restart test requires either the exact disposable `DDIP_TEST_POSTGRES_CONTAINER_ID` (as supplied
by hosted CI), or an explicitly configured `ddip.test.narrow-pipeline-restart-signal` controller.
The controller must acknowledge each fresh request only after restarting that isolated PostgreSQL
server. The test independently verifies the postmaster timestamp changed and fails when restart
support is absent. It never infers which host database to restart.

Run heavy native integration suites sequentially in a resource-constrained local environment.
Concurrent full Spark chains exceeded local capacity during qualification; that interrupted run is
not counted as a passing test. Isolate Maven output directories when separate workers compile
concurrently. Hosted CI remains a separate required final-candidate gate.
