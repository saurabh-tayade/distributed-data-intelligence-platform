# ADR 0043: Integrated original-run Content pipeline

Date: November 8, 2026

Status: **Integrated implementation locally verified within the approved delivery plan; existing activation gate remains in force.**

## Delivery boundary

Implement and qualify the complete original-run narrow Content chain together rather than releasing
further disconnected stage foundations. The user approved one integration branch, focused development
tests and a final complete CI cycle. This does not waive the restart/recovery qualification required
by [ADR 0022](0022-narrow-managed-content-profile.md) and
[ADR 0031](0031-inactive-guarded-fetch-launch.md). Normal dispatch remains unavailable until that gate
is satisfied. A test-gated full pipeline is not represented as operational activation or completion
of issue #49.

The supported envelope remains one fresh source, no retained Silver, an exact qualified publisher
reference, original uninterrupted ownership and the unchanged 67,108,864-byte / 221-entry ledger.
Historical runtime-v2 registrations keep their ledger-v1 meaning. Native checkpoints are not turned
into synthetic legacy stage or delivery history. Recovery continuation, cancellation/retry features,
UI work and wider source support are not silently added to this batch.

## Integrated contracts

The existing acknowledged FETCH, Bronze and Silver-reservation prefix is extended through distinct
Bronze inspections, Silver publication/verification/recheck, Gold preparation/publication/verification/
recheck and Content preparation/publication/verification/recheck. The fixed core order is:

1. FETCH_SOURCE/0 and STORE_BRONZE/0.
2. INSPECT_BRONZE/0, SILVER/0, VERIFY_SILVER/0 and RECHECK_SILVER/0.
3. INSPECT_BRONZE/1, PREPARE_GOLD/0, INSPECT_BRONZE/2 and PUBLISH_GOLD/0.
4. VERIFY_RESULT/0 and CHECK_INPUTS/0 for Gold verification and recheck.
5. INSPECT_BRONZE/3, CHECK_INPUTS/1 for Content preparation, and PUBLISH_CONTENT/0.
6. VERIFY_RESULT/1 and CHECK_INPUTS/2 for Content verification and final recheck.

The two post-success inspection slots remain separate and unused by core execution. The V11 probe
cannot be run on an attempt intended for this chain because it consumes a real core slot. Ledger
checkpoint identities and native command checkpoint aliases retain the original operation plan.

Strict Python execution uses a separately versioned bounded stdin/EOF bootstrap, independent frozen
expectations and a qualified module/runtime closure. Gold and Content retain native protocol-v2
commands and projection envelopes. Compact independently derived component digests avoid duplicating
large nested proof documents in the bootstrap. Candidate results include actual bounded output and
cleanup evidence; a child claim alone cannot authorize success.

An invocation-owned forced result prefix precedes external or native effects. Only that invocation
may finish it; interrupted prefixes are retained, never adopted. Spark shutdown and process cleanup
must be measured and supervised. Content verification can itself require Spark through independent
Gold verification and must use the appropriate process policy.

## Authority and persistence

Private one-use acknowledged callbacks carry the current owner, including heartbeat-refreshed versions,
through the chain. Detached history and acknowledgment-loss confirmation do not grant continuation.
Downstream directories require a dedicated exact ordered-scope reader rather than weakening the
predecessor-only reader or reusing obsolete parent-directory metadata.

Additive native operation, phase, checkpoint and terminal proof records bind the fixed slot, exact
predecessors, command and candidate digests, installed profiles and original owner. Short transactions
use coordinator-first READ COMMITTED fencing. Filesystem, process and network work stays outside SQL.
A native success transaction must require the complete verified core proof and atomically link the
actual final artifact/dependencies, finish attempt/run and release the coordinator. Legacy success
methods cannot be made applicable by fabricating stage history.

Unknown cleanup, incomplete output, failed controls, lost ownership and uncertain acknowledgments
stop continuation and retain evidence/occupancy. No new retry, restart adoption or recovered-owner
capability follows from this integrated implementation.

## Qualification and capacity

The full required runtime chain must be qualified before FETCH. Submission and profile selection must
not accept shapes the narrow executor cannot process. Setup and diagnostics must identify native
history explicitly without pretending it is legacy stage history. All such integration remains behind
the existing activation gate.

Independent review and focused local Java/PostgreSQL/Python tests accompany implementation. The final
candidate must run real Spark/native readers across the complete chain, validate empty/reference-only
outcomes, strict envelope bounds, exact operation aliases, mutation/ownership faults and uncertain
acknowledgments. Actual interruption and restart containment cannot be inferred from library tests.

CI partitions are expanded without removing tests, changing trusted classification or weakening final
gates. Python core and Reliability inventories are disjoint and exhaustive; native FETCH, Bronze and
publication each use isolated PostgreSQL/storage workers. Final-head hosted execution and measured
capacity remain required. See [CI operations](../operations/ci.md).
