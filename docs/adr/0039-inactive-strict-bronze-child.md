# ADR 0039: Inactive strict Bronze child

Date: November 7, 2026

Status: **Bounded child-only implementation within the approved original-run journal direction; local validation complete, standard hosted build pending.**

## Decision and scope

Add a separate strict ledger-v2 Bronze candidate producer after the reservation boundary in
[ADR 0038](0038-inactive-native-bronze-reservation.md). This follows the child-only pattern of
[ADR 0029](0029-inactive-strict-fetch-child.md); it does not activate a parent launcher, register
an installed runtime, commit a Bronze receipt or enable ordinary Content dispatch.

The strict entry point accepts a bounded, separately versioned stdin-pipe bootstrap rather than
a command-path argument. It validates exact fields, scalar types, PID correspondence and
independently supplied original expectation/current writer facts before opening supplied paths.
A complete JSON prefix is not sufficient: the full frame must reach EOF within the fixed deadline.
The bootstrap is identity correspondence, not proof of SQL authorization, parent provenance or
OS process-start identity.

The original selection, profile and Content runtime identities remain distinct from registered
Bronze runtime correspondence and the strict child identity. Historical runtime-v2 registration
still identifies `BoundedManagedBronze` and its ledger-v1 wrapper. Neither that registration nor
V17 is reinterpreted as strict-launch authority. A later parent must qualify installed bytes and
bind separate launch authority explicitly.

## Evidence and one-shot boundaries

Reconstruct the unchanged original ledger-v2 plan and accept only `STORE_BRONZE/0`. The slot's
existing command/evidence/result/verified files and preparation names each retain their 64 KiB
allowance. The child publishes only evidence/result through the strict sink. It does not create
an operation directory, publish the parent-owned command or verified anchor, add a completion
marker, copy payloads, discover ancestor ledgers or extend the reservation.

Reopen the exact completed FETCH journal and verify original source/revision, ingestion identity,
metadata, completion/reference bytes and filesystem stamps. Recheck credential bytes/stamps and
journal dependencies at effect boundaries. No new HTTP feed retrieval occurs. Credential bytes
remain transient local input, never candidate evidence or diagnostic output.

Before the first object-store request, including GET, exclusively create and force the already
reserved `result.json.preparing` and its directory. It means storage work may have occurred, not
that PUT succeeded. Only the originating invocation retains the channel and identity needed to
complete this prefix. Network IO does not hold the ledger lock. Writes reacquire the lock and
revalidate the tree; forced no-replace publication retains interrupted or two-link remnants.
No fresh process adopts, retries, clears or repairs a consumed prefix.

## Existing storage semantics

Reuse the native bounded object verifier, local S3 client and low-level Bronze writer. Do not call
the legacy managed entry point or legacy evidence writer. An initial independent two-object
inspection permits a writer invocation only for ABSENT. Initially observed PAYLOAD_ONLY,
METADATA_ONLY, CONFLICT or UNAVAILABLE states never initiate a write. Preexisting EXACT pairs
require fresh independent verification and no PUT. After a writer attempt, fresh GETs classify
actual storage state; writer return values and child exit status are not verification.

This preserves the existing writer's concurrency semantics. `S3BronzeStorage.write` performs
its own second checks and can complete a matching payload-only pair that appears between the
outer ABSENT observation and those checks. Therefore this is an initial-state gate, not a remote
transaction or a guarantee that partial pairs can never be completed under concurrent mutation.
Changing that behavior would require a separate writer/storage design. Native key, metadata,
receipt, content-type and bounded-read contracts remain unchanged.

## Alternatives and limits

Calling `BoundedManagedBronze` would use ledger v1 and ancestor discovery. Reusing a FETCH-only
bootstrap as Bronze authority would conflate command, checkpoint and runtime identities. A new
marker or increased file allowance is unnecessary because the reserved result prefix provides
one-shot consumption. Holding the ledger lock across network work would obstruct inspection.
These alternatives are not selected.

This is a testable candidate producer, not managed end-to-end readiness. There is no SQL migration,
production launcher, receipt adoption, stage success, release, retry, recovery, publication activation
or ordinary dispatch. The 67,108,864-byte / 221-entry reservation remains unchanged. Issue #49 and
Gate B0 remain open. See the [verification record](../operations/narrow-strict-bronze-child.md).
