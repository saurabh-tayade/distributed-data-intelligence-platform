# ADR 0038: Inactive native Bronze reservation

Date: November 7, 2026

Status: **Owner-approved new-journal direction; bounded implementation, validation pending.**

## Decision and scope

Continue the original, uninterrupted narrow Content run through a separate native journal.
The owner selected this direction over translating native FETCH completion into legacy stage
history. This first slice permits only reservation of `STORE_BRONZE/0`; it does not launch Bronze.
[ADR 0033](0033-inactive-fetch-completion-checkpoint.md) remains the FETCH completion contract.
Its prohibition on successor authority is superseded only for this original-live-owner reservation.

The immutable `content_bronze_reservations` journal is additive. V12 FETCH reservations, launch
phases, completion intents and checkpoints keep their existing meanings. The new record links
one fixed Bronze slot to the exact original native FETCH checkpoint and its command, descriptor,
completion and reference identities. It does not insert `rss_deliveries` or manufacture historical
`stage_invocations` in INTENT, RUNNING or VERIFIED states.

The parent independently reconstructs READY, the frozen Content-to-Bronze runtime chain and
storage selection. It reopens the exact completed FETCH journal, revalidates native identities,
and checks the original owner, versions, controls and deadline before committing the reservation.
The command has a 64 KiB ceiling. Credential-file observations bind a local digest and file stamp;
secret bytes are never stored in the journal. No object-store request occurs.

A same-live-invocation bridge carries the launcher's current owner through FETCH completion into
reservation without returning that owner as a continuation. A detached completion observation or
historical reservation read cannot reconstruct it. An uncertain management outcome stops the
owner channel; readback does not restore authority.

## Identity and fencing

Ledger coordinates, native checkpoint identity, original FETCH ingestion identity and historical
SQL attribution remain distinct. The Bronze checkpoint comes from the existing original plan's
`STORE_BRONZE/0` slot, not from a caller-supplied command or the FETCH checkpoint. The command
retains the exact original journal dependency and frozen registered Bronze/storage correspondence.

Reservation is single-use, including after process death or lost commit acknowledgment. An
existing Bronze directory is not adopted. The immutable journal is bounded and guarded independently
in PostgreSQL. Coordinator-first locking under READ COMMITTED serializes reservation against
controls and FETCH-only recovery. No SQL transaction spans filesystem inspection.

FETCH-only recovery must reject downstream Bronze reservation history. Conversely, reservation
must reject a recovery handoff or terminal disposition. This protects both Java callers and direct
SQL writes; adding a new table must not silently expand the old terminal operator's remit.
A stopped run with a Bronze reservation remains occupied until separately designed disposition.
This slice adds neither that disposition nor interrupted-run continuation.

## Alternatives and limits

A compatibility bridge through legacy `rss_deliveries` and VERIFIED stages would fabricate the
historical lifecycle and expose inappropriate recovery assumptions. Calling `BoundedManagedBronze`
is also not valid strict execution: its existing `ReservedEvidence` contract accepts ledger v1,
whereas the narrow admitted plan uses ledger v2. Neither alternative is selected.

The reservation does not qualify a strict Bronze child or grant its effects. A later reviewed
slice needs a separately versioned ledger-v2-compatible child/runtime, supervised launch,
independent object verification and durable receipt. Silver, Gold and Content follow separately.
No file allocation, command publication, process launch, network request, publication completion,
run success, slot release, dispatcher, API or operator action is added here. The original 64 MiB /
221-entry plan and native reader limits are unchanged. Issue #49 and Gate B0 remain open.

See the [verification record](../operations/narrow-native-bronze-reservation.md).
