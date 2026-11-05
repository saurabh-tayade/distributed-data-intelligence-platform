# ADR 0025: V2 native publication envelope guards

Date: November 5, 2026

Status: **Implementation of the owner-approved managed-only rule; inactive, qualification incomplete.**

## Context

The owner approved checking complete projected proof and recovery records before each
managed Gold or Content publication becomes complete. An output that cannot fit the
existing transport and persisted-slot limits must remain incomplete for inspection.
Standalone publication formats stay unchanged. Gold acceptance does not guarantee a later
Content stage fits. This extends the inactive work in
[ADR 0024](0024-narrow-native-precompletion-guards.md), not a dispatch authorization.

The v1 sizing helper and explicit v2 transport are separate contracts. Downgrading a v2
command, dropping its context or manufacturing a v1 context would lose its actual enclosing
cost. Content also needs actual public coverage totals from its validated precompletion
relation, not report row counts or an invented completed-reader result.

## Implementation direction

Add separately named v2 library publishers and projections. Preserve all existing native
publishers, readers, adapters, module hashes and transport/standalone formats. New modules
own the publication sequence but reuse native transformations, serializers, metadata
validation, Spark readback and independent analytical openers.

Reparse retained command bytes against independently supplied expectations at each effectful
entry. A public `CommandV2` instance or matching cached receipts are not authority. Expectation
correspondence does not prove runtime registration, READY provenance, current writer,
owner epoch, checkpoint or operation ownership. The caller owns single-writer exclusion and
the supplied Spark session. These libraries neither attest its runtime identity nor manage
or claim successful shutdown.

For new output:

1. Check live output/scratch identities and native dependency evidence. Gold loads only
   the frozen Silver marker and verifies its complete Bronze receipt and native Silver
   qualification. Content independently verifies the retained Gold proof and reference.
2. Write unchanged native datasets and run full native readback. Content then checks every
   public row and obtains actual public coverage totals from that same validated relation.
3. Create the exact native `complete.tmp` exclusively, retain its actual inode, bytes and
   full namespace capture, and prove existing files and identities did not change.
4. Build a v2 size-only verification from the capture. Qualify complete publication,
   artifact, command, recovery-evidence and recovery-observation envelopes. Content also
   qualifies both complete request and result at BEFORE_FETCH, GOLD and CONTENT_INPUTS.
5. Recheck all retained evidence, dependencies and root/ancestor identities. Establish
   final-marker absence with `lstat`; only `FileNotFoundError` establishes absence. Rename
   within the same directory.
6. Obtain real completed native verification. Gold runs its Spark verifier and independent
   analytics opener. Content opens the completed publication and runs all three reports.
   Compare it with the projection, then qualify all actual envelopes independently and
   recheck actual output capture and dependencies before returning.

Content smoke hashes and zero/one report row counts have fixed serialized widths; their
precompletion representatives are sizing values only. Actual hashes and report results
are obtained after rename and are never replaced by the projection. Neither a sizing
value nor projected shutdown status is returned as evidence or success.

## Failure and replay

Failure before rename retains incomplete files, including `complete.tmp` if created. No
cleanup, overwrite, repair, automatic retry or artifact adoption occurs. An uncertain or
failed acknowledgement after rename leaves native completion untouched. Fresh read-only
verification may establish its observed state; it does not authorize database adoption.

Existing complete output takes a separate read-only path. It must pass native, row and
whole-envelope checks without producer calls, temporary renaming or rewriting. Existing
incomplete output remains blocked. A valid standalone publication may be too large for
this managed profile and is refused without changing bytes or labels.

## Boundaries still open

No CLI, runtime manifest, bridge, SQL caller, sink, sealed-stage allocation or dispatcher
imports the new modules. No schema or new transport slot is added. Library calls return
only actual verification bodies; they emit no PUBLISH outcome or result sidecar. A future
adapter must separately qualify its actual outcome and 64 KiB result/error sidecars.

Launch integration still needs parent-owned command seals, independently loaded current
writer/owner/checkpoint/operation expectations, READY and registered module-byte validation,
ledger-v2 sinks, durable failure sidecars, recovery ancestry, restart evidence and capacity
qualification. Historical v1 evidence cannot be silently adopted as v2. Gate B0 stays closed.

See the [implementation and evidence report](../operations/narrow-v2-native-publishers.md).
