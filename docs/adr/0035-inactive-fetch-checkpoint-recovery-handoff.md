# ADR 0035: Inactive FETCH checkpoint-only recovery handoff

Date: November 7, 2026

Status: **Owner-approved bounded direction; inactive implementation only.**

## Decision and acceptance criteria

[ADR 0034](0034-inactive-native-fetch-recovery-assessment.md) remains a read-only assessment.
This separate package-private seam implements the owner-approved choice of a new recovery owner
for an intact, durably completed native FETCH checkpoint. Its only callers are tests. There is no
operator command, endpoint, scheduler registration or worker dispatch route, and implementation
approval does not authorize an operational recovery.

Acceptance criteria for this slice are:

- A fresh trusted-local review explicitly covers stopped handlers and unmanaged writers, old
  process identities, known descendants, and launch gaps/orphans. Negative PID observations alone
  never establish quiescence. The review is bound to the exact target and expires after 60 seconds.
- The original completed FETCH checkpoint, immutable admission, READY ledger, reservation,
  command, launch phases, completion intent and native descriptor/reference are independently
  reconstructed. Existing bounded filesystem checks must verify the original tree intact.
- One fenced transaction creates a distinct recovery-only attempt and separately versioned
  handoff identity, interrupts the old attempt and transfers the occupied global slot. Original
  native evidence and previous attempt history are retained. No files are written.
- Every incomplete or uncertain candidate, intent or marker-only outcome remains occupied and
  unadopted. Rollback, concurrent claims and lost acknowledgments cannot create duplicate owners.
- Lookup and replay return historical facts only. They never recreate an original or recovery owner.

## Identity and transaction boundary

The V15 policy is `ddip-content-fetch-checkpoint-recovery-v1`, with identity version 1. Its immutable
handoff row is separate from legacy `RecoveryHandoff`, V10 admission, native completion, and legacy
stage checkpoints. The successor is `RECONCILE / CLAIMED`, with its own UUID distinct from the
original attempt, control and handoff. It has no legacy reused-checkpoint pointer and no narrow
execution-admission marker. An opaque private owner type cannot be supplied to either execution
or legacy recovery APIs. No operation consumes this owner in the current slice.

Qualification binds the requested RECONCILE control, exact row versions, current owner epoch,
new recovery worker registration, immutable native ancestry and bounded prior history. The review
includes the checkpoint, explicit descendant and launch-gap confirmations, and at most 32 known
PID/start identities. LIVE or UNAVAILABLE observations veto transfer. ABSENT/REUSED observations
are only negative checks alongside the manual review. The locally held installation guard must
match the original host and the newly registered recovery worker.

Filesystem/process checks happen outside SQL transactions. Immediately before transfer the
checkpoint is independently reopened again, process checks and review freshness are repeated,
and a coordinator-first transaction compares the unchanged snapshot. Review expiry while waiting
for locks fails closed. In-memory guard-lease checks before and after mutations veto observed
closure without placing filesystem IO in the transaction. Full host-path checks surround it.
Uncooperative local filesystem mutation is not made atomic by SQL or by a PID check; stopping
writers and reviewing launch gaps remains a trusted-local operator responsibility.

The transaction interrupts only the original active attempt, inserts the successor and handoff,
sets the coordinator to `RECOVERY_ONLY` with epoch and version increments, changes the run to
`RECOVERY_REQUIRED`, and writes three bounded audit events. The RECONCILE control remains
REQUESTED because reconciliation is not finished. The slot is never freed, and no success,
publication, delivery, legacy stage, or later pipeline stage is recorded. Deferred schema checks
retain this occupied checkpoint-only state; later transitions require a separate reviewed design.

## Interrupted calls and resource limits

Each in-memory qualification is consumed before its first possible mutation. A successful first
call alone returns the opaque new owner. Reuse after a committed transfer returns a receipt with
no owner. A confirmed rollback requires a new qualification; unresolved outcomes are not retried.
Read-only lookup can confirm the handoff after acknowledgment loss, restart or death of the
recovery process. It reconstructs native SQL ancestry and compares original history, transferred
metadata and audit correspondence. Changed metadata or unavailable SQL yields UNRESOLVED.
Missing current files do not erase that historical receipt, but cannot create authority.

V15 adds no filesystem path, evidence-ledger reservation, packet or budget allowance. The existing
64 MiB / 221-entry envelope, 20-attempt limit, control limit and audit-capacity reserve remain.
The additive SQL ancestry record is limited to 64 KiB; review is limited to 8 KiB. Snapshots contain
hash/length representations of existing byte fields, not copies of payloads or filesystem contents.
The handoff is single-generation per run. Prior attempts are allowed only when they are terminal,
unmarked EXECUTE attempts with no stage, native or reused-checkpoint evidence. Their history is
retained, not rewritten. Snapshot-isolated direct writers lock the affected run before checking
enrollment visibility; stale snapshots fail rather than bypassing the handoff guard. Ordinary
READ COMMITTED legacy stage writes do not acquire a new global coordinator lock.

## Rejected alternatives and remaining work

Reconstructing the original owner would turn historical proof into renewed launch authority.
Reusing legacy handoff would cross into a different checkpoint lifecycle. Finishing a partial
completion intent would bypass the intact durable-checkpoint requirement. A filesystem packet
would consume paths and budget without being needed for this metadata-only transfer. These
alternatives are rejected for this slice.

This is not recovery completion. Repair, refetch, partial-intent completion, repeated handoff,
checkpoint adoption by successor stages, resume, cancellation finalization, slot release and
ordinary Content dispatch remain outside scope. Issue #49 and Gate B0 remain open. See the
[verification record](../operations/native-fetch-checkpoint-recovery-handoff.md).
