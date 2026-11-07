# ADR 0036: Inactive terminal FETCH recovery

Date: November 7, 2026

Status: **Owner-approved safe end-and-release direction; inactive implementation only.**

## Decision and acceptance criteria

The owner selected safely ending an interrupted run and preserving its evidence before considering
continuation. This ADR supersedes only ADR 0035's prohibition on consuming its new recovery owner
for a terminal disposition. The original checkpoint-only handoff, fresh trusted-local quiescence,
single generation, immutable evidence and no execution-authority reconstruction remain unchanged.
The separate package-private finalizer has test callers only. No operator command, endpoint,
scheduler registration or ordinary Content dispatch is added.

Acceptance criteria are:

- Consume only the opaque owner returned by the first successful ADR 0035 handoff. Historical
  receipts, stored identifiers, a new process, and repeated handoff calls cannot recreate it.
- Independently reopen the intact original completed native FETCH checkpoint. Candidate, partial
  intent, marker-only, missing, changed or unverifiable evidence is not adopted, repaired or fetched.
- Require a new review bound to the exact terminal target, all controls and current SQL versions.
  It explicitly covers handlers, unmanaged writers, descendants, launch gaps and orphans, retains
  previously reviewed descendants, and expires after 60 seconds. Negative process observations
  are veto checks, never proof of quiescence. The live finalizer is bound to its held host guard.
- In one fenced transaction terminate the run, apply its reconciliation control, stop the recovery
  worker and release the slot. Retain all original files, SQL evidence and prior attempt history.
- Resolve lost acknowledgments and restarts with read-only historical receipt validation. Never
  return a fresh owner, retry, launch, continue another stage or promote a FETCH checkpoint to a
  successful pipeline result.
- Keep ordinary Content dispatch disabled and issue #49 / Gate B0 open until the complete workflow
  and its end-to-end tests establish their separate acceptance criteria.

## Explicit disposition

This is reconciliation of an interrupted execution, not a cancellation request. The original
attempt remains `INTERRUPTED`. The distinct recovery attempt becomes `SUCCEEDED` with outcome
`RECONCILED_FAILED`, meaning that reconciliation completed, not that the pipeline succeeded.
The run becomes `FAILED / NATIVE_FETCH_INTERRUPTED`, with retry disabled and no result artifact,
counts or publication. The original `RECONCILE` control becomes `APPLIED` with the same run outcome.
These mappings follow the existing terminal recovery convention.

The recovery worker becomes `STOPPED`; the coordinator becomes `READY` with no active attempt,
owner or heartbeat. Its epoch is retained and its row version advances. This releases queue
capacity, not a Content dispatch gate. Subsequent unrelated authorized work may claim the slot.
The old execution and recovery identities may never own it again through this lifecycle.

Any other requested control or an applied cancellation vetoes finalization. A later cancellation
or second recovery-owner design is deliberately not silently interpreted as this disposition.
A failed or cancelled control is never applied merely because an operator requested reconciliation.

## Transaction, evidence and bounds

Qualification captures canonical hash/length representations of the original handoff's current
metadata and every control row, not payload copies. V16 adds one immutable terminal receipt per
handoff/run, identity version 1, policy `ddip-content-fetch-terminal-recovery-v1`. Its separate
terminal target and exact review are each bounded to 8 KiB; its snapshot is bounded to 64 KiB.
The original 64 MiB / 221-entry filesystem envelope and existing attempt/control limits do not
change. There are no new files, ledger reservations, packets, stages or native processes.

Filesystem/process checks remain outside transactions. The finalizer reopens the checkpoint and
rechecks quiescence before a coordinator-first transaction compares the exact unchanged snapshot.
Freshness is rechecked after lock acquisition and before return. In-memory guard lease checks
surround the mutations; full host-path checks surround the transaction. SQL cannot make an
uncooperative writer's filesystem activity atomic, so stopping writers remains a trusted-local
operator responsibility.

Five bounded audit events record the recovery attempt, run, control, worker and slot dispositions.
The immutable receipt is inserted after those writes. Deferred constraints require either the
original occupied handoff state or the complete terminal disposition, never a release shortcut.
Terminal run history cannot be retried, requeued, extended with another attempt/control or used
for stage/delivery/native execution. Controls and worker writers participate in the same fence;
stale snapshot writers abort instead of overlooking new receipt enrollment.

SQL enforces structural lifecycle and version constraints. Independent Java receipt inspection
also reconstructs the original native SQL ancestry, canonical handoff and review bindings, exact
before/after snapshot and audit correspondence. After later unrelated work, it verifies the
coordinator's monotonic epoch/version and absence of either former owner rather than expecting
the global slot to remain empty forever. The original handoff receipt remains inspectable through
the validated terminal receipt. Missing files after commit cannot erase historical SQL facts or
provide authority to act.

## Failure, replay and restart semantics

A qualification is consumed before the first possible mutation. A confirmed rollback leaves the
original recovery owner and occupied slot unchanged, but requires a fresh qualification and review
before another terminal attempt. Uncertain outcomes are inspected, never retried blindly. A
successful replay returns only a historical terminal receipt and cannot write again.

If the handoff commit acknowledgment is lost before its owner reaches the caller, that owner is
not reconstructed. If the recovery process dies before terminal commit, its slot remains occupied.
This narrow implementation does not add repeated takeover or restart reauthorization; either needs
a separate reviewed design. When terminal commit succeeded, acknowledgment loss or a later process
restart can confirm it without a guard, owner, current files or process inspection. SQL unavailability
or inconsistent metadata yields `UNRESOLVED`, never assumed success or authority.

## Rejected alternatives

Marking the pipeline successful would confuse a completed fetch with a verified final publication.
Marking it cancelled without a cancellation intent would change control semantics. Automatic
refetch, partial-evidence adoption and retry would create new native effects. Reconstructing a
recovery owner or permitting a second takeover would expand authority across process death.
All are outside this approved bounded direction. Continuation remains separate future work.

See the [verification record](../operations/native-fetch-terminal-recovery.md).
