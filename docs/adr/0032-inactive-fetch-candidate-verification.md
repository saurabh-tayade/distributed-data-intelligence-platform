# ADR 0032: Inactive native fetch candidate verification

Date: November 6, 2026

Status: **Additive read-only boundary within the owner-approved candidate-first direction; dispatch remains inactive.**

## Decision and scope

[ADR 0031](0031-inactive-guarded-fetch-launch.md) records a native FETCH result as a candidate,
not a completed step. This slice independently inspects that candidate without selecting a
persistence, completion or recovery design. It adds no schema version, phase or production caller.

The package-private observer accepts an installation guard and operation UUID, not a live owner.
It reconstructs the original admission and READY receipt against frozen run/worker facts, the
registered runtime chain, the immutable native reservation and all six ordered launch phases.
It validates the separate strict-fetch launch registration against that runtime chain. It does
not load old installed runtimes, launch a process, read credentials or contact the feed again.
Historical registration and launch inspection retain their existing behavior.

Before and after independent native validation, the observer checks original root/lock identities,
the unchanged version-2 reservation ledger, the exact FETCH-only subtree and the recorded five-file
candidate digest. Bounded identity/size/mtime snapshots also reject file or directory substitution
even when bytes match. Missing, added, foreign-stage, prefix and completion files fail closed.

`RssJournalFiles` is opened with creation disabled and validates the native journal's command,
HTTP result, descriptor, metadata, payload length/hash and derived Bronze keys. The new observer
also requires a canonical descriptor and the exact metadata schema version. Native Bronze metadata
retains its insertion-ordered encoding; bounded parsing still rejects duplicate keys, malformed
UTF-8/JSON, floats and trailing values. FETCH carries
raw HTTP bytes: malformed XML, non-XML data and empty successful responses are not rejected merely
for their content. Parsing remains the responsibility of a later native stage.

The returned observation contains only operation/run/attempt/checkpoint UUIDs, the candidate digest
and bounded payload length. It is detached and cannot authorize a write, launch, retry or recovery.
No descriptor, storage reference, command, packet, owner, session or continuation token escapes.
`complete.json`, verified markers, SQL verification rows, stage success, artifacts and occupied-slot
release are deliberately absent. The original 64 MiB / 221-entry envelope remains unchanged.

## Alternatives and remaining decisions

A matching inventory digest alone cannot establish journal semantics. Trusting the child's `OK`
result would repeat that mistake. Reusing the existing native journal validator adds independent
checks without changing historical completion paths. Inspecting installed runtime artifacts would
unnecessarily make historical evidence depend on a runtime that need no longer be installed.

Persisting a verification/completion phase or allowing continuation would require a separate design
for uncertainty, restart and recovery. Those choices are not made here. Failed observations retain
all evidence and leave the reservation occupied, without repair, cleanup or automatic refetch.
Issue #49 and Gate B0 remain open. Full Content processing and recovery are not enabled by this slice.

See the [verification record](../operations/narrow-fetch-candidate-verification.md).
