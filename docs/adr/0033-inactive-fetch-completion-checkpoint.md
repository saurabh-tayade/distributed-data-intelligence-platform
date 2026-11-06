# ADR 0033: Inactive native FETCH completion checkpoint

Date: November 6, 2026

Status: **Bounded implementation of the owner-approved original-live-owner completion direction; dispatch remains inactive.**

## Decision and scope

[ADR 0032](0032-inactive-fetch-candidate-verification.md) independently observes a raw HTTP journal
without granting write authority. This slice permits only the original, still-active owner to
finish that candidate as a durable native checkpoint. Interrupted or uncertain work remains
consumed for a separate, explicitly authorized recovery design. No restart recovery is added.

V14 adds two immutable, bounded tables: `content_fetch_completion_intents` and
`content_fetch_checkpoints`. There is at most one row in each per reserved operation. The intent
contains the original candidate digest, the exact planned native completion marker bytes and
digest, and the current owner versions. The checkpoint references that intent, the reserved
checkpoint UUID, native descriptor and existing native checkpoint-reference bytes. Descriptor,
reference and marker each have a 65,536-byte ceiling. This is native journal evidence, not a
legacy `stage_invocations` or `rss_deliveries` outcome and not a completed pipeline publication.

The attempt stays `CLAIMED`, the run stays `RUNNING`, and the global slot stays occupied.
Historical launch rows, native marker format, old runtime registrations and old completion APIs
retain their meaning. Existing legacy execution entry points continue to reject narrow admission
history. A later stage must obtain separately designed authority; this receipt grants none.

## Ordered effects and fencing

1. Reject a previously consumed completion intent, require the original live owner, and reconstruct
   READY, frozen command, native reservation and all six launch phases.
2. Capture original candidate file stamps and directory identities, then perform a fresh independent
   ADR 0032 verification. The detached observation is never accepted from a caller as authority.
3. Recheck the candidate and owner, then commit the exact planned marker intent before any write.
4. Recheck again and write only the already-reserved `complete.json.preparing` and `complete.json`
   paths using the existing forced, same-directory atomic publication protocol. Check original
   identities and live owner versions/controls/deadline at each write boundary.
5. Reopen and independently validate the native completion, strict descriptor/schema, original
   candidate digest and stamps, exact FETCH-only scope and unchanged version-2 ledger.
6. Recheck before one short owner-fenced transaction inserts the immutable native checkpoint linkage.

SQL independently enforces READ COMMITTED, coordinator-first locking, current original ownership,
owner versions, controls, deadline, READY, six phases, exact candidate/command/checkpoint identity,
marker structure/digest and descriptor/reference correspondence. Java additionally verifies canonical
bytes, native semantics and filesystem identities. No SQL transaction spans filesystem or process
IO. These are cooperative local fences, not a distributed atomic filesystem/database transaction.

A fresh inactive `launchAndCheckpoint` seam passes the launcher's current in-memory owner directly
to completion. This matters after a launch heartbeat changes owner versions. The owner never
escapes as a returned continuation and is never recreated from SQL. Existing launch calls still
return candidate-only facts. There is no production caller, endpoint, CLI or dispatcher.

## Uncertainty and restart

An intent is single-use even if no marker exists. Prefix-only, marker-only, changed, inconsistent
or uncertain evidence remains untouched and occupied. Duplicate completion never writes again.
An intent commit with lost acknowledgment cannot authorize marker publication. A checkpoint commit
with lost acknowledgment can be confirmed only by an exact read-only comparison with the same
in-memory expected intent/descriptor/reference. The stopped owner channel is not refreshed.
Absent or inconsistent confirmation does not retry, repair or adopt anything.

Historical SQL-only inspection returns bounded detached identifiers and digests. It does not need
installed runtime files, a live owner or the original filesystem, and it does not claim that those
files are currently intact. Fresh-process inspection is not recovery. A new worker still enters
recovery-only mode while the occupied attempt remains unresolved. Future restart recovery needs
explicit authority, quiescence evidence and its own reviewed design.

## Alternatives and limits

Materializing legacy INTENT/RUNNING/VERIFIED stages after execution would fabricate their historical
lifecycle and expose old continuation/recovery assumptions. Rewriting the six launch phases would
change existing evidence contracts. Both alternatives are rejected in favor of additive native
checkpoint evidence. Trusting a cached observation or successful child exit would skip the fresh
independent verification required here.

The filesystem plan remains exactly 67,108,864 reserved bytes and 221 entries, with no new path
allowance. No automatic refetch, next stage, run success, release, recovery, activation or ordinary
Content dispatch is introduced. Issue #49 and Gate B0 remain open.

See the [implementation and verification record](../operations/narrow-fetch-completion-checkpoint.md).
