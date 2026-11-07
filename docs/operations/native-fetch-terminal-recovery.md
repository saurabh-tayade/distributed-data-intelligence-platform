# Inactive terminal native FETCH recovery

The package-private `NarrowContentFetchRecoveryFinalization` implements
[ADR 0036](../adr/0036-inactive-terminal-fetch-recovery.md). This is an internal test contract,
not an operator procedure. Production has no caller and ordinary Content dispatch remains disabled.

## Outcomes and limitations

- An intact durable native FETCH checkpoint and the first successfully returned handoff owner are
  mandatory. A new exact-target review must cover stopped handlers, unmanaged writers, original
  processes, inherited descendants, launch gaps and orphans.
- Terminal commit preserves the original interrupted attempt and evidence. It records the run
  as `FAILED / NATIVE_FETCH_INTERRUPTED`, disables retry, completes the recovery attempt and its
  `RECONCILE` control, stops the recovery worker and releases the occupied global slot.
- No fetch, repair, continuation, cancellation inference, success publication or artifact occurs.
  Unrelated authorized work can subsequently use the slot. The finalized run cannot be revived.
- Lost terminal acknowledgments are resolved by independent read-only SQL receipt inspection,
  including after restart or loss of current filesystem access. Receipts cannot mint owners.
- Death before terminal commit, or lost handoff acknowledgment before receiving the owner, leaves
  the slot occupied. There is no repeated takeover or restart reauthorization in this slice.

## Verification

Production Java compilation, test-source parsing, SQL parsing, repository hygiene and CI inventory
are local checks. Maven, Docker and PostgreSQL are unavailable in the implementation environment;
these checks do not claim an executed integration suite. Hosted CI must run the complete required
checks on the exact PR head before merge.

The new PostgreSQL/native-child suite belongs to the fail-closed `native_fetch_recovery` inventory. It
covers terminal disposition and evidence retention, subsequent unrelated slot use, direct SQL
shortcuts, competing controls and finishers, mutation rollback, process/review/file vetoes, actual
JDBC commit acknowledgment loss, and fresh-process receipt inspection. Existing handoff, migration,
inspection, Content and Reliability suites remain required.

The full issue #49 workflow and Gate B0 remain open. Browser walkthrough remains deferred until the
entire UI is in place, and this change does not establish end-to-end execution readiness.

The separate [test-only composition suite](native-fetch-recovery-composition.md) exercises the
assessment, first handoff and terminal disposition together, including real recovery-JVM death.
It adds no production caller or operational activation.
