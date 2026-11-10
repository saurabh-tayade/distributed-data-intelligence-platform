# ADR 0046: Reliability running cancellation

Status: **Implementation record under the owner's delegated in-scope design authority; local verification complete; PR CI pending**

## Scope and alternatives

Implement ADR 0010's request-based cancellation for the active original Reliability
EXECUTE attempt. Preserve the already implemented never-claimed queued cancellation.
This is not Content worker activation, automatic recovery, guarded retry, output cleanup,
or rollback of a native publication. The public-feed acceptance deferred to issue 47
and draft PR 137 are unchanged. Number 0045 is reserved by that draft's public-feed design.

A UI-only cancellation button would be misleading: the current worker's exact-version
ownership checks reject a changed run, while recovery finalization rejects pending
cancellation. Merely inserting intent would strand the request. Relaxing all owner
checks to accept any newer run version would undermine stale-owner fencing. Instead,
recognize only the exact admitted cancellation transition and retain the existing
publication evidence and explicit recovery boundaries.

## Acceptance criteria established before implementation

1. Admission serializes against worker completion using coordinator, run and attempt
   locks. It binds one exact original Reliability execution, reviewed run version and
   normalized idempotency identity. It never replaces the active owner or changes
   frozen inputs. A stale or ineligible request creates no control.
2. `CANCEL_REQUESTED` means accepted intent, not a cancelled publication. The ordinary
   writer cannot authorize another stage after that transition. Recognition of the
   cancellation transition must not refresh arbitrary stale owner tokens.
3. The supervising worker bounds child and descendant termination. Unknown cleanup,
   ambiguous database outcomes and restart retain occupancy; absence of a worker
   heartbeat is not proof that native writers have stopped.
4. A proven pre-writer cancellation can atomically finish `CANCELLED`, apply its
   control and release the slot. An authorized writer requires independent artifact
   verification, not an inference from process exit or a cancellation request.
5. Post-writer recovery remains an explicit guarded operator procedure with its
   existing fresh containment review, independent checks and evidence seals. Proven
   output absence can finish `CANCELLED` with control `APPLIED`. Verified publication
   finishes `SUCCEEDED` with control `TOO_LATE`. Incomplete, invalid, changed or
   unavailable output is not relabeled cancelled; it remains occupied for recovery.
6. Finalization binds the exact cancellation identity and terminal disposition into
   its durable receipt. Atomicity, stale-owner rejection and acknowledgment-loss
   resolution cover both the reconciliation and cancellation controls.
7. The UI distinguishes accepted request, applied cancellation, too late and recovery
   required. Navigation/abort never proves rollback. Replay uses the original key,
   body and conditional version, never an automatic replacement request.
8. Tests cover admission/completion serialization, prelaunch and postlaunch stop,
   uncertain cleanup, request/finalization acknowledgment loss, restart occupancy,
   publication success winning cancellation and retained UI intent.

## Relationship to existing records

ADR 0010's publication and request semantics are unchanged. This increment extends
ADR 0014's implementation-record limitation that pending cancellation cannot be
finalized, only for the explicitly qualified Reliability cases above. Content's
independent journal and safe-end contracts are not relaxed. The local trusted-user,
cooperative-lock and non-atomic-filesystem/database limitations still apply.

## Verification

Focused verification covers 11 running-control database/owner cases, 17 queued-control
regressions, seven native recovery cases and two real Spark publication/cancellation
boundaries. Full backend regression discovered 912 cases: 909 passed and three
existing cases skipped, with no failures/errors. Frontend checks cover 1,435 passing tests with two existing skips, plus
types, lint and production build. Independent static review found and corrected an
audit-reader compatibility issue. The newly added tests do not directly exercise
the packaged dispatcher lifecycle or a hard process/database restart. Existing
full CI gates remain required; local focused checks do not replace them.

A draft PR and exact-head CI are required before merge. This document is not evidence
of public network acceptance, production readiness or UI walkthrough completion.
