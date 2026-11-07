# ADR 0037: Trusted-local native FETCH terminal operator

Date: November 7, 2026

Status: **Implementation record for the owner-approved bounded safe end-and-release direction. Validation pending.**

## Decision and scope

Expose the existing [ADR 0035](0035-inactive-fetch-checkpoint-recovery-handoff.md) handoff
and [ADR 0036](0036-inactive-terminal-fetch-recovery.md) finalizer through the packaged,
non-web `operator-recovery` role. This supersedes their test-only/no-operator-caller restriction
only for this terminal adapter. It does not authorize an actual retained-data recovery operation.
The owner, checkpoint, evidence, review, transaction and disposition contracts remain unchanged.

`native-fetch-end` is separate from legacy `recover`, `resume` and `execute-resume`.
It accepts an existing exact RECONCILE control, installed profile/control binding and deployment
digest, and explicit same-host/process-namespace confirmation. It preallocates the
handoff, recovery-attempt and finalization identifiers and emits the handoff and finalization
inspection identifiers before mutation. It neither creates a
reconciliation control nor converts a historical receipt into an owner.

Only an intact durable native FETCH checkpoint can qualify. The adapter uses real local process
observations and two independent, fresh, exact-target operator attestations: one for the handoff
and one for terminal disposition. The second review preserves previously reviewed descendants.
There is no automatic approval from assessment results, PID absence, a free guard or elapsed time.
Each challenge binds its phase, fresh nonce, inspection time, target, checkpoint and process
observations. All required attestations are explicit; missing, false, stale or mismatched input
fails closed. Reviews expire after 60 seconds and consume bounded input.

The first successfully returned opaque handoff owner stays only in the same live process.
The finalizer retains the original interrupted attempt and all evidence, records the run as
`FAILED / NATIVE_FETCH_INTERRUPTED` with retry disabled, completes the reconciliation attempt
and control, stops the recovery worker and releases the occupied global slot. It cannot launch,
refetch, repair, resume, adopt another checkpoint, publish success or infer cancellation.

## Deployment trust boundary

[ADR 0010](0010-durable-execution-coordination-and-recovery.md) selects host-launched execution,
with PostgreSQL and SeaweedFS containerized separately. This adapter must run on the original
execution host in the same process namespace. It adds no all-container or remote recovery model.

The installation guard authenticates private filesystem ownership, device/inode identities,
installation bytes and the held lock. Worker registration binds these facts to SQL and the frozen
profile. These checks do not prove machine, boot or PID-namespace identity. The explicit
`sameHostProcessNamespace` attestation supplies the trusted operator's confirmation of this deployment
precondition, not an automated proof. `RecoveryQuiescence.LOCAL` supplies actual PID/start-time
observations in that namespace. LIVE or UNAVAILABLE veto; ABSENT or REUSED do not prove quiescence.
Stopping handlers and unmanaged writers, reviewing descendants and launch gaps, and excluding
orphans remain the human operator's responsibility.

## Interrupted calls and alternatives

This adapter is single-shot. Invalid input, timeout or failure exits rather than refreshing an
approval. After a committed handoff, exit or death before terminal commit loses the only usable
owner and leaves the slot occupied. This is deliberate fail-closed behavior, not restart support.
Lost handoff acknowledgment likewise cannot reconstruct authority. There is no command to retry
or adopt that owner, clear the slot manually, repair evidence or bypass a failed check.

`native-fetch-inspect` reads historical handoff/finalization receipts using their identifiers.
It requires no guard, current filesystem, worker profile or control identifier. A confirmed
receipt is historical evidence only. Missing or unresolved receipts do not prove rollback,
quiescence or permission to repeat a mutation. Terminal acknowledgment loss can be resolved
without reinstating authority, including after restart and unrelated later queue work.

A bounded same-live-process re-review loop could improve usability but is not included. Legacy
recovery bootstrap and restart continuation are rejected because they would cross the narrow
owner boundary. New execution, repeated takeover and automatic attestations remain excluded.

## Acceptance and evidence

Required validation covers the packaged route and role isolation, real host-bound process
observations, both independent reviews, stale/malformed/mismatched/refused input, original
evidence integrity, competing controls, lost acknowledgments, recovery-process death, receipt
inspection after restart, and subsequent unrelated slot use. Existing native recovery, Content,
Reliability, migration and CI inventory checks remain required on the exact PR head.

This record does not claim a passed integration suite or an actual operational recovery.
See the [operator procedure](../operations/native-fetch-terminal-operator.md) for the bounded
interface and failure consequences. Issue #49, ordinary Content dispatch, Gate B0, continuation
and full end-to-end readiness remain open. Browser walkthrough remains deferred until the
complete UI is available.
