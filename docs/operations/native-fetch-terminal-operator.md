# Trusted-local native FETCH terminal operator

[ADR 0037](../adr/0037-native-fetch-terminal-operator.md) exposes the terminal-only contracts of
ADRs 0035 and 0036 through the existing packaged `operator-recovery` role. This is a procedure
for an independently authorized recovery, not permission to operate retained production data.
Validation of this adapter is pending; this document claims no executed integration pass.

## Read this before starting

**The command is single-shot. After handoff commits, a timeout, rejected second review, process
exit or death before terminal commit can leave the global slot occupied with no restart recovery
route. The opaque owner cannot be recovered from saved identifiers or receipts.** Prepare the
review before starting, remain present for both prompts, and do not start if the required facts
cannot be established honestly within each 60-second review window. Do not automate approvals.

The only successful disposition is `FAILED / NATIVE_FETCH_INTERRUPTED`, retry disabled, no
result artifact or pipeline success. Original files, SQL evidence and the interrupted attempt
are retained. The reconciliation attempt succeeds only in reconciling that failure; the recovery
worker stops and the slot becomes available for unrelated authorized work.

## Preconditions

- Use the installed Java 21 packaged application and existing management database configuration.
  The compatible migrations, execution profile, deployment digest and CONTROL binding must already
  be installed. This procedure does not provision them or create a reconciliation control.
- Identify the existing requested local-operator RECONCILE control targeting the interrupted
  native FETCH attempt. Confirm that ending this exact run, rather than continuing it, is intended.
- Run as the provisioned local filesystem owner on the original execution host in the same process
  namespace. Do not move the command into a new container, remote host or isolated PID namespace.
  If a reboot or namespace change makes the original process identities ambiguous, stop.
- Stop execution handlers and prevent them from restarting during both reviews and transactions.
  Stop unmanaged writers using the existing trusted local operating procedure. The adapter does
  not stop or kill processes and does not make uncooperative filesystem writes atomic with SQL.
- Review original worker and child identities, retained launch phases, known descendants, launch
  and release gaps, and possible orphans. Inspect private evidence locally without copying native
  payloads, paths or secrets into approval input. Missing or ambiguous evidence is a reason to stop.
- Preserve the original checkpoint and all evidence. Partial candidates, incomplete intents,
  marker-only states and missing or changed files cannot be completed, repaired or adopted here.

A valid guard demonstrates the expected filesystem binding and lock, not the host's boot identity
or PID namespace. The same-host confirmation is an operator attestation. Local PID absence or
reuse, an expired heartbeat and a free guard are not proofs that all writers have stopped.

## Start the terminal-only action

Use `--ddip.role=operator-recovery` with `--ddip.recovery.action=native-fetch-end` and the
following existing configuration values:

- `ddip.recovery.control-id`: exact requested RECONCILE control UUID.
- `ddip.worker.execution-profile-id`: the original installed execution profile.
- `ddip.worker.control-binding-id`: the original installed CONTROL binding.
- `ddip.worker.deployment-sha256`: the installed deployment digest matching that profile.
- `ddip.recovery.same-host-process-namespace=true`: supply only after establishing the deployment
  precondition above. This setting alone does not approve either mutation.

Do not substitute legacy `recover`, `resume` or `execute-resume`. This action needs no new runtime
execution capability and must not launch a native child or continue a pipeline stage.

Retain the emitted `NATIVE_FETCH_PLAN` identifiers before proceeding. They are allocated before
mutation and are needed for independent historical inspection if output or acknowledgment is
lost. They are not credentials, ownership tokens, proof that a mutation occurred, or retry permission.

## Two independent human reviews

The command emits `CONFIRMATION_REQUIRED` for `NATIVE_FETCH_HANDOFF`, then, only after a
successfully returned handoff owner, for `NATIVE_FETCH_TERMINAL`. Review each current prompt
independently. The terminal target digest is different and binds current controls and versions;
the prompt does not display those raw rows. Review the intended control and private evidence
through the existing trusted local procedure. Never reuse the first approval or treat handoff
success as terminal approval.

For each prompt:

1. Check the phase, exact target digest, checkpoint and original worker/child PID/start identities
   against the intended run and locally reviewed evidence. Inspect each current process observation.
   LIVE or UNAVAILABLE is a veto. ABSENT or REUSED still requires the full manual review.
2. Re-establish that handlers and unmanaged writers remain stopped. Review known descendants,
   missing-child/launch gaps and possible orphans. Record known descendants as exact PID and start-time
   identities, not PID-only guesses. The terminal review must retain every descendant from handoff.
3. Only when all checks are true, enter one UTF-8 JSON line of at most 16 KiB answering that live challenge.
   Echo `challenge`, `inspectionSha256`, `targetSha256`, `checkpointId` and `recordedProcesses`
   exactly from the current prompt. Set `approval` to `AUTHORIZE_NATIVE_FETCH_HANDOFF` for the
   first phase or `AUTHORIZE_NATIVE_FETCH_TERMINAL` for the second. Set the string `reviewMethod`
   to `LOCAL_PROCESS_TREE_AND_WRITERS_REVIEWED`.
4. Explicitly provide all required boolean attestations: `sameHostProcessNamespace`,
   `handlersStopped`, `unmanagedWritersStopped`, `orphanReviewComplete`, `descendantsReviewed`
   and `launchGapsReviewed`. Provide `descendants` as an array of objects with exactly `pid`
   (a positive decimal string) and `startedAt` (an ISO-8601 instant).
   An empty descendants list is valid only after an actual review establishes no additional known
   descendants; it is not a default approval. These are the complete response fields; do not add
   a `reviewedCheckpoints` array, phase, timestamp or other fields.

The fresh nonce, inspection timestamp and hash bind the response to that prompt. Time is measured
from inspection, not from submission of the answer. The adapter rechecks freshness, processes,
evidence and SQL versions; a timely answer cannot override a changed target or failed check.
No script, sample all-true response, prewritten approval file or reused challenge establishes an
honest review. If a check is uncertain or false, do not attest it. Exit and preserve the evidence.

## Failure and historical inspection

`native-fetch-inspect` is the separate facts-only action. Use the same packaged
`operator-recovery` role and database configuration with `ddip.recovery.action=native-fetch-inspect`,
and both saved `ddip.recovery.handoff-id` and `ddip.recovery.finalization-id` from the plan.
Both are required and must refer to the same operation. It does not require a host guard, profile,
control ID or current filesystem access. It emits `NATIVE_FETCH_INSPECTED` with separate
`handoffStatus` and `terminalStatus` and `authorization=false`. Exit status is 0 only for a
confirmed related terminal receipt; otherwise it is 2. A handoff-only confirmation is not completion.

- A confirmed handoff receipt proves historical transfer only. It cannot return an owner or finish
  a run in a new process. After loss of the owner, the occupied state has no restart path here.
- A confirmed terminal receipt proves terminal disposition. Do not repeat the mutation. Historical
  validation remains available after later unrelated work uses the global slot.
- Missing or unresolved receipts do not prove rollback. Keep the outcome unresolved; do not retry
  the mutating action, create a replacement owner or clear coordinator/worker rows manually.
- A lost terminal acknowledgment may mean terminal commit succeeded. Inspect the saved finalization
  identifier before interpreting the failure. A process exit is not evidence of rollback.

Keep retained evidence and report the exact receipt status and identifiers through the trusted
local incident procedure. This adapter offers no repair, cancellation inference, refetch,
continuation, repeated handoff, restart reauthorization or automatic rollback recovery.

## Verification and wider status

The required acceptance matrix includes packaged startup, role isolation, both fresh challenges,
actual local process observations, invalid/refused/stale inputs, immutable evidence, occupied
failure paths, acknowledgment loss, process death and facts-only restart inspection. PostgreSQL
and real-child tests plus existing Content/Recovery/Reliability regressions and exact-head hosted
CI must establish executed evidence separately; documentation and compilation are not a test pass.

No retained-data operation was executed to prepare this procedure. Issue #49, Gate B0, ordinary
Content dispatch and complete end-to-end readiness remain open. Browser walkthrough remains
deferred until the full UI exists.
