# Inactive native FETCH checkpoint recovery handoff

The package-private `NarrowContentFetchRecoveryHandoff` implements
[ADR 0035](../adr/0035-inactive-fetch-checkpoint-recovery-handoff.md). Production has no caller.
This document describes the tested internal contract, not an operator procedure or recovery command.

## Bounded outcomes

- Only an independently verified, intact durable native FETCH checkpoint is eligible.
- A fresh exact-target review must cover old processes, descendants, handlers, unmanaged writers,
  launch gaps and orphans. Missing or uncertain process observations veto transfer.
- The new distinct attempt owns the still-occupied slot in `RECOVERY_ONLY` mode. The run remains
  `RECOVERY_REQUIRED`. Its checkpoint-only identity is separately versioned and immutable.
- Candidate, intent and marker-only outcomes are not promoted. No original owner is reconstructed,
  no native process is started, and no fetch, repair, continuation or slot release occurs.
- Lost acknowledgments are resolved through read-only SQL lookup. Receipts are not ownership
  tokens. Replayed committed calls return no owner; consumed rollback qualifications cannot retry.

The original native files and SQL evidence are unchanged. No new filesystem entry or ledger
reservation is allowed. Existing attempt, control and audit limits still apply. The new recovery
attempt cannot be used by the legacy recovery or ordinary worker execution seams.

## Verification

The integration suite covers intact checkpoints, candidate/intent/marker rejection, explicit review
fields and expiry, process vetoes, changed files and fencing, concurrent qualifications, every
mutation rollback boundary, actual JDBC commit acknowledgment loss, fresh-JVM lookup and schema
rejection of execution/release shortcuts. It checks metadata and filesystem preservation and zero
additional HTTP/native work. The suite is in the fail-closed `content_recovery` CI inventory.

Production sources compile locally with the JDK 21 compiler module and pinned production
libraries. Java test parsing and bridge/child type compilation are local checks only: Maven,
Docker and PostgreSQL are unavailable in the implementation environment. Hosted CI must execute
the full PostgreSQL/native-child integration suite on the exact submitted head before merge.

The full #49 recovery workflow, Content dispatch and Gate B0 remain open.

A separate inactive [terminal disposition](native-fetch-terminal-recovery.md), specified in
[ADR 0036](../adr/0036-inactive-terminal-fetch-recovery.md), can now consume only the original
successfully returned owner. The handoff itself still does not release the slot or recreate an owner.
