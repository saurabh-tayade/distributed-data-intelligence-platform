# ADR 0042: Inactive native Silver reservation

Date: November 8, 2026

Status: **Bounded reservation-only implementation within the approved original-run journal direction; dispatch remains inactive.**

## Decision and scope

Extend [ADR 0041](0041-inactive-native-bronze-checkpoint.md) only with a private, one-use reservation
callback after an acknowledged original-owner Bronze checkpoint. The verifier's refreshed owner and
retained original candidate pins pass directly to reservation. Historical checkpoint observations and
lost-acknowledgment confirmation cannot invoke the callback or reconstruct its authority.

V20 adds an immutable SILVER/0 reservation bound to the exact Bronze checkpoint, receipt and verified
anchor, V17 reservation, V18 candidate, completed FETCH and frozen runtime/storage identities. It writes
no filesystem evidence and starts no process. The original 67,108,864-byte / 221-entry plan is unchanged.

## Distinct identities and prerequisites

STORE_BRONZE/0 is the predecessor checkpoint. INSPECT_BRONZE/0 remains a separate, unmet prerequisite
before future Silver execution. SILVER/0 is reserved but explicitly non-launchable. The reservation
records the original distinct inspection checkpoint; it neither aliases nor consumes that slot.

The bounded canonical reservation command has its own version and SILVER_RESERVATION mode, with
nativeLaunchCompatible false and the required inspection identity. It is not the legacy executable
Silver packet. Historical Silver runtime-v2 still identifies the ledger-v1 adapter and is not silently
reinterpreted as strict ledger-v2 launch capability. A later strict child and installed-runtime profile
must be separately implemented and reviewed before execution.

Correspondence includes exact receipt/anchor bytes and digests, anchor filesystem stamp, original FETCH
ingestion UUID/time and descriptor/reference, registered SILVER→BRONZE→FETCH links, frozen storage roles
and credential identity/digest. Secret credential bytes are not persisted. Output storage bindings do not reserve or prove absence of a native Silver artifact namespace.
Actual SILVER/TEMP root identities are rechecked; TEMP remains private. Native output-path semantics
remain future work.
Command size is bounded by
the already-planned 65,536-byte slot even though this slice does not write that slot.

## Fencing and uncertainty

Java independently reconstructs original enrollment and frozen facts, reopens completed FETCH and
post-anchor Bronze evidence, and checks credential identity, target absence and live owner controls.
It preserves original candidate-file pins rather than recomputing the pre-anchor directory digest
that necessarily changed when verified.json was published. There are no object-store requests.

SQL independently enforces immutable structural correspondence, original-owner versions, controls,
deadline and exact predecessor relationships under coordinator-first READ COMMITTED locking. No SQL
transaction spans filesystem or network work. A reservation is single-use; lost acknowledgment stops
the channel and leaves the reservation occupied. Historical inspection returns detached facts only.

Treating a receipt as continuation would revive a stopped owner. Producing a legacy executable command
would blur safety models. Counting STORE_BRONZE verification as INSPECT_BRONZE would silently change the
planned order. These alternatives are rejected.

No Silver launch, inspection effect, successor execution, run success, release, restart, recovery or
normal dispatcher is introduced. Issue #49 and Gate B0 remain open. See the
[verification record](../operations/narrow-native-silver-reservation.md).
