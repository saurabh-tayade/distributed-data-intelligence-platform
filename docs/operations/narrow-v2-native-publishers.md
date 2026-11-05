# Inactive v2 native publisher orchestration

This implements the owner-approved managed-only whole-envelope stop-before-completion rule
in [ADR 0025](../adr/0025-v2-native-publication-envelope-guards.md). Gate B0 and dispatch
remain closed. Standalone contracts and all previously pinned module bytes are unchanged.

## Library entry points

- `narrow_publication_projection_v2.bind_context_v2`: reparse retained complete v2 bytes
  against separately supplied expectations. Never downgrade or create a v1 context.
- `project_gold_v2`, `project_content_v2`, `compare_actual_v2`: immutable sizing values from
  exact temporary evidence, followed by comparison with actual native verification.
- `narrow_content_reports_v2.validate_precompletion_v2`: all public rows and actual public
  coverage totals from the same validated precompletion DuckDB relation.
- `narrow_gold_publication_v2.publish_v2` and `verify_publication_v2`: exact native Silver
  inputs, real Gold Spark readback, independent Gold analytics and complete-envelope guards.
- `narrow_content_publication_v2.publish_v2` and `verify_publication_v2`: retained Gold and
  reference checks, native Content readback, row guards and all three RESUME qualifications.
- `narrow_gold_publication_v2.recheck_publication_v2`: Spark-free exact retained evidence
  recheck, used after full native Gold verification. It does not create or adopt output.

These are inactive library calls, not commands accepted by a managed child. An existing
Spark session and independent expectations are required. Matching correspondence does not
attest runtime provenance or authorize a writer. No caller receives a cleanup confirmation,
projected verification, composite unbudgeted result or inferred future artifact identity.

## Ordering and retention

New output is natively written and read back before its temporary marker is captured.
Every complete projected envelope must fit before the final rename, including full receipts,
frozen reference evidence and nested v2 Gold command. Content includes BEFORE_FETCH, GOLD
and CONTENT_INPUTS request/result pairs. Existing stage, recovery and SourceJson limits stay
unchanged; no new file-count policy or extra descriptor allocation is invented.

Immediately before rename, retained temporary/output evidence and all dependencies and
identities are checked again. Immediately afterward the real native verifier runs, its
result is compared with the projection, all actual envelopes are qualified and output
capture is rechecked after dependency checks. Existing complete output is verified read-only.

A failure retains output exactly as observed. Temporary files are not cleaned up. Completed
markers are not removed or renamed back after post-rename failure or acknowledgement loss.
Read-only reconciliation can establish observed completion without authorizing resume.

## Evidence and its limits

Focused tests cover complete v2 retention and independent-expectation mismatches, actual
coverage, precise whole-envelope maximum/plus-one rejection, all RESUME boundaries,
pre-/post-rename failure, same-byte replacement with a different inode, changed mode or
hardlink count, dependency mutation, ancestor swaps, unexpected final markers, actual
post-rename mismatch and immutable replay. The 902-file escaped empty Content fixture must
be rejected before completion, with all temporary evidence retained.

Lifecycle fault tests explicitly double Spark transformations while retaining real native
files, metadata, DuckDB readers and full envelope code. They are not native provenance
proof. Separate tests exercise actual Silver production and native qualification, full
receipts served by a read-only loopback S3 fixture, Gold and Content Spark producers,
independent readers and standalone replay for nonempty and reference-only cases. The Silver
qualification subprocess owns and confirms its own shutdown; the parent session is not
misrepresented as stopped.

Mandatory Java parity consumes projected and actual complete bodies captured during real
orchestration and parses them with unchanged SourceJson, using current persisted-slot
allocations. It does not substitute the earlier synthetic shape corpus. No result-sidecar
vector is emitted because these libraries emit no sidecars; that future adapter obligation
remains open.

Local evidence uses Python 3.12.14, Spark 4.0.2, JDK 21, DuckDB 1.4.5 and boto3/botocore
1.43.114. The supported hosted CI pins Python 3.12.15 and must run on the eventual exact
commit before this slice is accepted. Local results are not that hosted proof.

## Remaining gate work

Runtime registration and exact module-byte provenance, out-of-band current-writer/owner/
checkpoint/operation binding, sealed-stage allocation, ledger-v2 child sinks, actual adapter
result/error qualification, durable failure packets, complete recovery ancestry, restart
faults and lifetime resource qualification remain incomplete. Neither these library tests
nor transport correspondence opens Gate B0 or makes dispatch safe.
