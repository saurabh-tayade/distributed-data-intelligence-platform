# ADR 0024: Narrow native precompletion guards

Date: November 4, 2026

Status: **Implementation direction within the owner-approved narrow profile; inactive, qualification incomplete.**

## Context

[ADR 0022](0022-narrow-managed-content-profile.md) requires a bounded complete Silver
qualification and rejection of oversized complete public report rows before publication.
The legacy managed wrappers return only after native completion. Checking their result
cannot establish this ordering. Changing those pinned modules would also silently alter
historical runtime identities.

The [tracked admission](0023-tracked-narrow-content-admission.md) foundation does not yet
authorize native stages. This slice supplies separately named, explicitly versioned
library entry points. It does not connect admission, stage allocation or a child runtime.

## Decision

Keep every existing native publisher, reader, managed adapter and runtime module unchanged.
The additive entry points reuse their transformations, semantic verification and serializers,
but own the small publication sequence necessary to place the new checks before completion.
They write no profile sidecar, change no product fingerprint and never truncate data.

### Silver prospective metadata

After dataset write and native readback, inspect the exact unfinished namespace. Count
all analytical and bookkeeping files and directories, including the future completion
file. Require at most four analytical files per dataset, 24 total files and four directories.

Measure a conservative prospective capture, inventory identity, native selection,
verification and complete qualification with the existing narrow validators. Preserve the
exact native marker serializer, including its whitespace and ASCII escaping. Future
filesystem stamps use maximum widths permitted by the narrow stamp grammar; known file
lengths, names, hashes and marker bytes remain exact. The complete qualification ceiling
is 60 KiB within the unchanged 64 KiB reader, with the existing intermediate limits.

This projection is exclusively an admission calculation. It is never persisted, returned
as verified evidence, or used instead of actual native qualification. The real completed
publication must still pass the unchanged reader and actual narrow validation. A projected
shutdown-status field budgets bytes; it is not proof that Spark has stopped.

Staging a second completed publication or changing native readers to accept marker-free
inputs would require a broader publication protocol. Neither is introduced here.

### Content complete report rows

After native readback verification and before temporary completion creation, validate
every row of all three existing public report projections. Include reference-only coverage
rows. Use the native public conversion and exact canonical serializer, with a 60 KiB limit
on the whole row, including keys, escaping, arrays, nulls, timestamps and numeric strings.

Use the existing bounded, extension-disabled DuckDB configuration. Before materializing
rows in Python, reject rows whose raw projected UTF-8 string and string-array content alone
exceeds the row limit. This necessary lower-bound check is not the final serializer check.
Fetch rows in bounded units, without report pagination, Spark collection or truncation.
Recheck retained output and dependency evidence after the added scan and before completion.

## Failure, replay and reconciliation

Failures before the completion rename retain an incomplete namespace. Existing incomplete
output, including temporary marker files, is refused without repair, retry, deletion or
adoption. A failure after rename can leave a completed publication; only full read-only
verification may establish its observed state. Verification is not authorization to resume.

A standalone publication may be valid under its historical contract and too wide for the
narrow profile. Refuse it for these entry points without changing bytes, timestamps or
labels. Absence must be established by an actual missing-path result, not by treating an
I/O or permission failure as absence. Native semantics and provenance remain authoritative.

## Obligations still open

This implements only the Silver prospective metadata and Content public-row portion of
the existing Gate B0 work. Full Gold and Content metadata allocations, nested commands and
proofs, reference qualification, observation packets, inherited evidence and recovery/resume
envelopes remain unproven obligations under issue #49, not removed or declared satisfied.
The provisional budget model is not a measurement of actual maximum native output.

No new RunProfile, child protocol, ledger consumer, stage-to-slot allocation, SQL claim
caller, dispatcher route, cleanup policy or automatic publication selection is added.
Operational acceptance still requires independent review, pinned native CI and the complete
fault/restart and resource evidence described in the earlier ADRs. Gate B0 remains closed.

The [implementation report](../operations/narrow-native-precompletion.md) records the exact
entry points, executed evidence and remaining limitations.
