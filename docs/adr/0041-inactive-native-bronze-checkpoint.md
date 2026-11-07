# ADR 0041: Inactive native Bronze verification checkpoint

Date: November 7, 2026

Status: **Bounded implementation within the approved original-run journal direction; dispatch remains inactive.**

## Decision and scope

Extend [ADR 0040](0040-inactive-guarded-bronze-launch.md)'s original-live-owner candidate boundary
with independent GET-only verification and an immutable native Bronze checkpoint. A private,
single-use callback carries the launcher's refreshed owner directly into verification. Historical
facts, receipt-shaped child output and an EXACT candidate claim cannot authorize this operation.
There is no dispatcher, Silver successor, release, restart or recovery expansion.

A separate installed verification profile selects `StrictBronzeVerifier`, qualifying Java and JAR
bytes independently while preserving the historical Bronze registration and linked FETCH manifest.
The verifier receives a bounded PID-bound stdin bootstrap and emits only a bounded native receipt.
It never invokes the publishing child or writer. Fresh bounded GETs compare exact payload/metadata,
lengths, hashes and supported content types against reconstructed original FETCH facts. Candidate,
FETCH and credential identities are checked between object reads. A candidate must be EXACT/exit zero
to enter, but its storage claims are not trusted as remote evidence.

## Ordered evidence and fencing

V19 adds separate immutable verification profiles, ordered verification phases and Bronze checkpoints.
The phases are VERIFY_BOUND, START_AUTHORIZED, CHILD_RECORDED, RELEASE_AUTHORIZED and OBSERVED.
OBSERVED commits the exact independently observed receipt and planned verified-anchor bytes before
any parent filesystem write. The parent publishes only the existing STORE_BRONZE/0 verified.json
slot through forced no-replace preparation, then records the native SQL checkpoint. Each row is
bound to the exact V17 reservation, V18 candidate, completed FETCH, original run and owner fence.

Checks cover original admission, frozen runtime/storage, installed bytes, bootstrap/process identity,
owner versions, controls and deadline. SQL uses coordinator-first READ COMMITTED locking; no SQL
transaction spans process, filesystem or network work. Controls are checked before delivery, during
supervision and before receipt/anchor/checkpoint acceptance. A GET already in flight may finish after
a concurrent control change; a failed fence forbids the anchor and checkpoint. These are cooperative
local checks, not remote locks or an atomic object-store transaction.

Original candidate files retain independently pinned identities and bytes while anchor creation
necessarily changes directory metadata. A dedicated post-anchor reader permits only the original
completed FETCH plus precisely the reserved Bronze candidate and anchor. Existing FETCH-only readers
and historical migrations are not broadened. The 67,108,864-byte / 221-entry plan is unchanged.

## Uncertainty and historical meaning

The verification reservation and OBSERVED intent are one-shot. A lost acknowledgment before anchor
publication stops the owner channel and cannot authorize a write. Partial prefixes, anchor-only state
or changed evidence remain occupied and untouched. A checkpoint acknowledgment loss permits only
exact read-only confirmation against the same in-memory expected facts, never refresh, repair or retry.

Historical inspection returns bounded detached identifiers and digests, not current storage integrity,
a launch token or continuation. A checkpoint records what independent GETs observed; remote mutation
can occur later. It does not make the entire run successful or authorize another stage. Existing
FETCH recovery exclusions remain in force once Bronze has been reserved.

Trusting the publishing child's receipt would skip independent verification. Rerunning that child
could PUT. Reconstructing legacy stage history would conflate safety models. Reusing interrupted
verification would weaken one-shot authority. Those alternatives are not selected.

Issue #49 and Gate B0 remain open. See the
[verification record](../operations/narrow-bronze-verification-checkpoint.md).
