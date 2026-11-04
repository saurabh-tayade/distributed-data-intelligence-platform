# ADR 0023: Tracked narrow Content admission

Date: November 4, 2026

Status: **Owner-approved lifecycle direction; inactive implementation, qualification incomplete.**

## Context

[ADR 0022](0022-narrow-managed-content-profile.md) reserves a complete narrow operation plan
before effects. Its [strict reader](../operations/narrow-content-strict-reader.md) requires
independent policy and identity expectations but deliberately did not select their persistence
or preparation ordering. The owner approved tracking a claimed attempt before preparation,
then recording durable PREPARING and READY, with no processing before readiness.

Preparing before a claim would require a separate bound and cleanup policy for orphan
candidate directories. That alternative is not implemented. The fixed original reservation
already accounts for exactly 64 MiB, so adding an unaccounted receipt sidecar is inappropriate.

## Decision

Keep the existing attempt states and coordinator exclusion protocol. Add an opt-in immutable
attempt policy marker and append-only PostgreSQL admission/milestone tables in migration V10.
Legacy attempts have a null marker and receive no backfilled admission. The new package-private
claim seam is explicit and has no production dispatcher or operator-command caller.

The sequence is:

1. Claim the exact eligible original Content run and enroll its policy in the same transaction.
   A deferred foreign key forbids committing a marked attempt without its admission row.
   Bind the installation, run, attempt, original owner/epoch, one source, CONTROL descriptor,
   frozen selection, execution profile and content runtime identity. These are canonical
   bounded JSON bytes with database-checked SHA-256 digests.
2. Under existing owner fencing, controls and deadline checks, commit PREPARING once, while
   the attempt remains CLAIMED and has no stage intents. Only an acknowledged commit permits
   filesystem preparation. An uncertain commit stops that repository's authorization.
3. Outside SQL transactions, create only a fresh private attempt namespace, retain the existing
   lock/ledger protocol, reserve the complete original plan, force the ancestor chain and
   strictly verify the result. Derive the expected ledger digest from the plan and captured
   fresh root identity, not from the disk ledger's self-description.
4. Recheck ownership, controls, deadline, enrollment and zero stages, then append READY.
   It binds the admission digest, attempt-root identity, lock identity and exact ledger digest.
   There is no filesystem receipt sidecar and no additional reserved file.
5. Reconstruct facts read-only from immutable SQL and the independently supplied CONTROL
   binding, then apply the strict reader. A successful inspection is not an Owner token,
   transferred ownership, stage authorization or permission to resume.

Both receipts have a 64 KiB ceiling. Enrollment and phase rows cannot be updated, deleted or
truncated through ordinary SQL. PREPARING cannot be retried, even if a crash left no attempt
root. READY requires PREPARING. Missing or altered root, lock or ledger fails closed. Partial
prefixes and incomplete preparations are retained; no adoption, repair, release or automatic
orphan cleanup is introduced.

## Capacity and inactive boundaries

The explicit admission checks `max(attempt_no) + 1 <= 19`, preserving attempt 20 under the
existing limit. Counting rows is insufficient because historical attempt numbers may have
gaps. This slice admits original preparation only; it does not qualify effectful continuation
or allocate a recovery attempt. Legacy admission retains its existing limit and behavior.

SQL refuses RUNNING or stage mutations for marked attempts without READY. Application stage
adapters, generic mark-running, ordinary reclaims, legacy reconciliation and RESUME additionally
refuse enrolled run histories even after READY. Those adapters do not yet consume the fixed
plan or versioned child ledger. In particular, an absent root and zero stages cannot send a
marked attempt into legacy recovery's empty-evidence shortcut.

Preparation has filesystem effects. The old clean-prelaunch terminal/release path is refused
once PREPARING exists; explicit interruption remains available and keeps the coordinator
occupied. Heartbeats remain available while preparation is active. No new public API or error
vocabulary, RunProfile version, native publisher behavior or active publication is introduced.

## Tradeoffs and remaining evidence

This favors visible, bounded, retained preparation over automatic retry. A failed preparation
can require a later explicit recovery design; this inactive slice does not claim operational
recoverability. Immutable SQL is the independent binding under the existing cooperative local
operator/database trust model, not a defense against a privileged database administrator or an
arbitrary same-user process replacing filesystem paths. Digests authenticate bytes against
that binding, not native semantics or complete sidecar contents.

The [implementation and validation report](../operations/narrow-content-admission.md) distinguishes
executed local unit/fresh-JVM evidence from PostgreSQL tests awaiting supported CI. Full native
maximum-output, precompletion, nested-envelope and original/inherited recovery qualification,
exact stage allocation and versioned child consumption remain required. Gate B0 stays closed,
and ordinary Content dispatch stays disabled. Historical ADR acceptance statuses are unchanged.
