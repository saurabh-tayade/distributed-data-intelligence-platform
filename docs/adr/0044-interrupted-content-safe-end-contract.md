# ADR 0044: Interrupted Content safe-end contract

Date: November 8, 2026

Status: **Design independently reviewed and selected under the owner’s explicit in-scope decision delegation. Implementation, independent source review and local lifecycle/operator/SQL/crash qualification have passed. Final hosted CI remains pending; no dispatch activation or actual retained-data disposition is authorized.**

## Purpose and unchanged boundary

[ADR 0043](0043-integrated-original-content-pipeline.md) implements the original-live-owner
17-operation Content chain. Its interruption behavior retains evidence and global occupancy.
The FETCH-only terminal lifecycle in ADRs 0035–0037 intentionally rejects downstream Bronze
reservation history. Legacy recovery rejects narrow admission history. Neither restriction is a
bug to bypass, and this proposal does not alter either existing lifecycle.

The proposed next capability is to end an interrupted original narrow Content execution as failed
and release capacity after fresh trusted-local review. It does not continue, retry, repair, adopt
partial output, complete a marker, infer cancellation, or activate ordinary dispatch. Issue #123
(continuation), issue #124 (recovery reauthorization), issue #49 and Gate B0 remain open. Actual
retained-data operation still needs its own exact-target authorization. UI and wider source support
are outside this contract.

The user's safe-end direction and explicit delegation of recommended in-scope decisions support this purpose. Extending eligibility from an intact completed
FETCH checkpoint to a valid incomplete full-pipeline prefix is nevertheless a new evidence and
state-transition contract. It was independently reviewed as a separate contract, not inferred from the similar ADR 0036
end state. The implementation forecast fits the previously allocated recovery, envelope and
integrated acceptance work; review of the actual implementation remains required.

## Eligible original histories

Assessment covers the original narrow admission and every bounded prefix through FETCH, Bronze,
Silver reservation and the 15 suffix operations, including interruption before an operation's
reservation, between ordered launch phases, during native publication, before checkpoint commit,
after checkpoint commit, and before final success commit. Enrollment and admission preparation
before READY are classified separately; without a verifiable sealed READY root they are not
eligible for mutation under this proposal. A later pre-READY release contract is not implied.

Only structurally valid original history can qualify: exact run/attempt/control identity, supported
frozen profile, immutable admission and READY envelope, exact ordered slots and predecessors,
monotonic owner versions, phase order, canonical bytes and digests, and bounded artifact/dependency
correspondence. There must be no mixed legacy stages, V11 probe consumption, inherited execution,
prior recovery handoff, terminal disposition, or contradictory native completion. An existing
FETCH-only recovery owner is specifically ineligible. Previous attempts must satisfy the existing
bounded original-history restrictions, not become a new resume chain.

A completed final transaction is success, even if its acknowledgment was lost. Safe-end must reject
it and provide only historical resolution. Conversely, all 17 checkpoints without that final
transaction are not successful Content execution. No native marker or externally visible publication
can replace the missing SQL completion transaction.

## Incomplete output and evidence integrity

A valid interrupted prefix can contain partial native effects, preparation files, an invocation-owned
forced result prefix, or publication output without a checkpoint. These facts are retained and
reported as incomplete or unverified, never promoted into a checkpoint or result. No native reader,
Spark job, FETCH, network/storage mutation, filesystem write, deletion, rollback of published files,
or cleanup process is launched by safe-end. Existing Silver/Gold artifact registrations and their
dependencies remain untouched. Failed Content has no final result artifact or result counts; it
must not falsely claim that no intermediate publication or external effect occurred.

Distinguish an expected absence from missing evidence. Files whose durable phase/checkpoint requires
them must be present and match the evidence that was actually persisted. A file never durably
required at that interrupted boundary may be absent. Partial files allowed by the existing sealed
ledger are not rejected solely because they are incomplete, but must remain within its exact path,
entry, byte, ownership and type bounds. Unknown paths, changed required evidence, symlinks, oversized
files, unreadable roots or an unclassifiable partial tree veto mutation and leave occupancy unchanged.
No new ledger slot or ad hoc evidence file may be introduced to make a prefix eligible.

Historical SQL checkpoint existence and current filesystem integrity are separate results. Persisted
candidate inventory digests can be checked against an exact bounded current inventory where
reconstructible. In-memory pins lost at restart cannot be recreated as historical observations.
The inspector must not claim inode provenance stronger than the original records support. Native
output outside the managed evidence tree is not recursively inventoried or reverified by this
contract; its possible existence is explicitly retained as unverified external state. Inability to
establish writer quiescence for that state vetoes release.

## Recommended authority model: direct atomic safe-end

Prefer a separately versioned, single-generation **direct atomic safe-end**, rather than a committed
handoff followed by finalization. Qualification produces only an opaque, one-use, live-process
terminal capability. It cannot be passed to execution, continuation, legacy recovery, or FETCH
recovery APIs and is consumed before the first possible mutation. No durable intermediate recovery
owner is returned or reconstructible. A historical receipt is never that capability.

The existing two-transaction alternative is familiar and preserves two independent operator reviews,
but adds a durable recovery-only owner and a process-death/acknowledgment-loss gap between handoff
and terminal commit. Addressing that gap would require deferred #124. Generalizing that lifecycle
also risks widening the existing FETCH operator's meaning. A single terminal transaction is the
smaller proposed authority surface: the operator authorizes one exact failed end-state after fresh
review, rather than authorizing ownership transfer and then a separate disposition.

This is a material design choice, not an implementation shortcut. Independent review must approve
one exact-target terminal attestation instead of the two attestations attached to two distinct
mutations in ADR 0037. Do not reuse an ADR 0037 attestation or silently change its operator command.
If the single-transaction design cannot satisfy existing lifecycle constraints without broad changes,
stop and return to review rather than adding a resumable recovery owner.

For audit consistency, the proposed transaction records a distinct bounded RECONCILE attempt already
terminal as `SUCCEEDED / RECONCILED_FAILED`, with no returned execution/recovery owner. The original
attempt becomes `INTERRUPTED`; the run becomes `FAILED / NATIVE_CONTENT_INTERRUPTED`, retry disabled,
with no final artifact/counts. The exact requested RECONCILE control becomes `APPLIED` with that
outcome, the registered terminal operator worker becomes `STOPPED`, and the coordinator becomes
`READY` with no active attempt/import, owner or heartbeat. A fresh fenced epoch invalidates old
owners; exact version increments and audit correspondence must be specified and independently
reviewed with the migration. Existing worker-stop semantics for the old worker are retained; old
worker history is not rewritten to fabricate cleanup.

Any competing requested control or applied cancellation vetoes this disposition. Do not create a
RECONCILE control, revive a failed/cancelled control, interpret cancellation as failure, or override
an active import. This proposal does not make an unsuccessful terminal qualification a cancellation.

## Proposed SQL identity and exact terminal mapping

Use a new `attempts.content_terminal_policy` marker constrained to
`ddip-content-original-safe-end-v1`. Do not overload or widen `content_recovery_policy`.
A marked attempt must be `RECONCILE`, have null `content_admission_policy`, null
`content_recovery_policy` and null `reused_checkpoint_id`, and be terminal at insertion.
A separate immutable `content_original_terminal_receipts` table has unique `terminal_id`,
`run_id`, `control_id`, `original_attempt_id` and `terminal_attempt_id`, policy and identity
version 1, and the bounded target/review/snapshot digests and bytes. Composite foreign keys
`(run_id, original_attempt_id)` and `(run_id, terminal_attempt_id)` reference attempts;
`(terminal_attempt_id, policy)` is unique and is the target of the deferred FK
`attempts(attempt_id, content_terminal_policy)`. The receipt also references the exact
RECONCILE control and worker identities. Its terminal attempt, original attempt, control and
terminal IDs are distinct; the terminal worker differs from the original worker.

Coordinator-first guards must enforce mutual exclusion with existing FETCH handoff/terminal
receipts and original pipeline completion, including concurrent inserts in either direction.
The deferred marker FK alone does not enforce that exclusion. A marker cannot be inserted
without its complete receipt and disposition in the same transaction.

Initial eligibility is deliberately exact: run `RUNNING`, original attempt `CLAIMED` with the
narrow admission marker, coordinator `READY` occupied by that original attempt/worker/epoch,
original worker `READY`, newly registered terminal worker `RECOVERY_ONLY`, and the exact
RECONCILE control `REQUESTED`. Other starting states fail closed rather than acquiring implicit
support. All frozen identity and version predicates are checked. With old coordinator epoch E,
coordinator/run/attempt/control/terminal-worker versions C/R/A/K/W, commit performs exactly:

1. Original attempt becomes `INTERRUPTED / INTERRUPTED`, finished time set, version A+1.
2. Insert the distinct terminal attempt as `RECONCILE / SUCCEEDED / RECONCILED_FAILED`,
   version 0, attempt number max+1 (at most 20), terminal worker identity, epoch E+1,
   started/finished timestamps set, and the separate terminal marker. It is never CLAIMED.
3. Run becomes `FAILED / NATIVE_CONTENT_INTERRUPTED`, version R+1, `current_attempt_id`
   points to the terminal attempt, `current_stage` null, finished time set, result artifact
   and counts null, retry false. V3's deferred same-run current-attempt FK remains intact.
4. Exact control becomes `APPLIED`, version K+1, applied time set and reason
   `NATIVE_CONTENT_INTERRUPTED`; immutable control identity remains unchanged.
5. Terminal worker becomes `STOPPED`, version W+1, retaining its heartbeat. Original worker
   and its version remain unchanged. Epoch and receipt closure permanently fence its old attempt.
6. Coordinator becomes `READY`, version C+1, epoch E+1, active attempt/owner/heartbeat null;
   active import must already be null and remains null.

Each existing-row update occurs once, honoring V5's exact +1 version rule. The six audit events
are `NATIVE_CONTENT_TERMINAL_ORIGINAL`, `NATIVE_CONTENT_TERMINAL_ATTEMPT`,
`NATIVE_CONTENT_TERMINAL_RUN`, `NATIVE_CONTENT_TERMINAL_CONTROL`,
`NATIVE_CONTENT_TERMINAL_WORKER`, and `NATIVE_CONTENT_TERMINAL_RELEASE`, all with safe code
`NATIVE_CONTENT_INTERRUPTED`. Their before/after states and versions exactly match the six
steps, with null/0 versions for the inserted attempt. All bind run, terminal target, control,
original/terminal attempts and snapshot digest; exactly six bounded events are capacity-checked.
No extra claim/handoff audit or intermediate recovery state is fabricated. These names and
mappings remain the reviewed design.

**Implementation-review amendment, November 8, 2026:** insert the immutable receipt first inside
the same coordinator-locked transaction, before the six lifecycle writes. Its immediate INSERT
guard compares the canonical supplied snapshot with the actual eligible pretransition rows and
rejects preexisting terminal-safe-code audits. The terminal-attempt foreign key and complete
final disposition/audit checks remain deferred. Receipt-enrolled closure permits only the exact
current-transaction lifecycle writes and six audits; history and artifact appends remain closed.
This replaces the original receipt-last ordering after review found that `SET CONSTRAINTS`
could flush deferred OLD-row checks before enrollment. Forcing immediate validation after the
receipt INSERT must reject its incomplete disposition. No partial receipt or intermediate owner
can commit; the entire transaction still succeeds or rolls back atomically.

## Persistent terminal closure and intermediate artifacts

Existing immutable payload triggers are insufficient: they prevent UPDATE/DELETE but do not
prevent new rows or new outgoing dependency edges. Receipt-enrolled history must permanently
reject updates/requeue/retry of the failed run, additional attempts or controls for that run,
legacy stages/artifacts/deliveries, narrow admission additions, native reservations, launch and
verification phases, completion intents, checkpoints, success records, and FETCH/legacy/new
recovery handoffs. Reject modification of the terminal worker back into an owning state and
any reuse of either terminal or original attempt as coordinator owner. Keep historical reads,
unrelated worker registration and unrelated later queue ownership available. Apply guards to
both direct SQL and application paths, including stale snapshot writers.

Independently derive the exact expected Silver and Gold addresses/evidence from completed
verification/recheck proofs. At the boundaries after RECHECK_SILVER and RECHECK_GOLD, query
artifacts by exact binding/address and compare canonical evidence and dependencies even if no
successor command exists to reference their random UUID. Distinguish zero matching rows (before
registration) from one matching row (after registration); reject duplicates, mismatched evidence
or impossible registration before its prerequisite checkpoint. Snapshot both presence/absence
and the identified rows. Do not infer absence from missing `silverInput`/`goldProof` Java objects.

Terminal enrollment permanently freezes the observed presence or absence of registrations at
every exact binding/address derived for the original run, including final Content. Coordinator-fenced
artifact-insert guards reject later or concurrent registrations at receipt-enrolled absent addresses,
including same-transaction and stale-snapshot writers, without blocking unrelated artifact addresses.

Freeze the retained registered Silver and Gold nodes and their exact **outgoing** dependency
sets: Silver has the existing empty set, Gold has the exact Silver dependency. Guard new edge
insertion as well as mutation/deletion. Also freeze any graph nodes actually included in the
terminal proof, without recursively capturing unrelated users of a shared reference artifact.
New unrelated artifacts may reference an already-qualified retained artifact as an **incoming**
edge under existing ordinary admission rules; terminal failure neither invalidates that artifact
nor grants new reuse permission. Do not block unrelated consumers or all writes to the artifacts
table. No final Content artifact may be invented from native publication files. A final Content
registration must correspond to committed original success and makes terminal failure ineligible.

## Phase-to-files matrix for retained original evidence

The matrix is a read-only classification, not permission to run existing live sinks on retained
prefixes. It is derived from `PreparedCommand.writeOnce`, `StrictFetchEvidence`,
`StrictBronzeEvidence`, `strict_native_sink.EffectSink`, `narrow_evidence_sink.StrictSink`,
`strict_native_content`, and the FETCH/Bronze completion writers. All rows require the sealed
READY root/ledger/lock and completed predecessor evidence. No future operation subtree may exist.
Names below are relative to the one current operation directory, subject to exact ledger limits.

| Durable boundary | Required | Optional interrupted state | Forbidden / veto |
| --- | --- | --- | --- |
| No operation reservation | Completed predecessors only | None for this operation | Current/future operation directory or files |
| Reservation only, before LAUNCH_BOUND | No current files | None | Current directory/files; creation follows durable LAUNCH_BOUND |
| LAUNCH_BOUND, before FILES_READY | Existing predecessors | Current parent/directory absent or empty; `command.json.preparing` partial or complete, or exact `command.json` | Child files; both command names together (Java uses atomic move); nonexact final command |
| FILES_READY, START_AUTHORIZED, CHILD_RECORDED | Exact `command.json`, current directory | No child output; START_AUTHORIZED may hide a started but unrecorded child | Any child result/evidence/prefix or completion marker |
| RELEASE_AUTHORIZED, before CANDIDATE_EXITED | Exact command | No result yet; or single `result.json.preparing` (empty/partial/complete); child payload final/prefix names below; or final `result.json` | Unknown names, child output without result claim/final result, invalid type/ownership/link count; final result never proves checkpoint |
| CANDIDATE_EXITED, no checkpoint | Exact successful candidate file set and candidate digest | FETCH/Bronze completion states below only | Child `.preparing` names, missing required candidate bytes, changed candidate digest; suffix checkpoint sidecar files |
| Durable checkpoint | Exact checkpoint-bound successful file set and corresponding immutable SQL proof | Later authorized operation subtrees only | Missing/changed required bytes, preparation files for that completed operation, extra files |

FETCH child payload names are `payload.xml`, `metadata.json`, `descriptor.json`, written in that
order, followed by final `result.json`; Bronze uses `evidence.json`, then result; suffix uses
`evidence.json` and then `selection.json` only where produced by that exact operation, then result.
During RELEASE_AUTHORIZED an unfinished payload may have its `.preparing` name and already
finished earlier payloads, with the invocation's result claim retained. A final FAILED result may
coexist with earlier retained payloads because exception handling can finish failure after a payload
write; it is never a successful candidate. Check each shape against the actual writer order and
slot grammar, rather than accepting an arbitrary subset of ledger names. Incomplete JSON prefixes
are bounded retained bytes, not parsed as complete proof. A final result claiming payload digests
requires those exact final payloads. SQL candidate/checkpoint facts require successful result and
all operation-specific payloads, with no preparation files.

Java and Python child writers publish with link/unlink. Death between these steps can leave
final and `.preparing` names sharing an inode with nlink=2. This is a known interrupted writer
outcome but remains **ineligible for safe-end** under existing private-entry checks. Preserve and
report it; do not remove links or relax link-count policy. Thus full-prefix assessment does not
promise terminal eligibility for every crash shape. Java parent command/anchor writes use atomic
move and do not justify accepting dual names either.

FETCH `complete.json` and its prefix are forbidden until the durable completion intent, then absent,
partial prefix, or exact intent-bound final marker is possible. Its durable checkpoint requires
exact final `complete.json` plus the five candidate files. Bronze's separate verifier phases retain
the successful three-file candidate (`command`, `evidence`, `result`); no `verified.json` or prefix
is allowed before durable verification `OBSERVED`. After OBSERVED either absence, partial prefix,
or exact OBSERVED-bound final anchor is possible; Bronze checkpoint requires the final anchor.
The verifier's launch/child/release gaps require process review even though it writes no child
files. The 15 suffix operations have **no filesystem checkpoint marker**: SQL checkpoint insertion
adds no file. Their candidate/checkpoint set is command, result and exactly the non-null result-bound
evidence/selection files. Silver reservation itself creates no directory or output. A mere reserved
future slot is never evidence that a child was authorized to fill it.

## Trusted-local qualification and commit

Require the original host and process namespace, a held installation guard bound to the registered
terminal worker, and an explicit same-host/process-namespace confirmation. Guard bytes and PID
checks alone do not establish deployment identity.

A fresh exact-target review covers stopped handlers, unmanaged/local/remote writers involved in the
original operation, recorded worker and child identities, Spark gateways, known descendants, launch
gaps and orphans. LIVE or UNAVAILABLE process observations veto. ABSENT or REUSED are only negative
checks and never prove quiescence. Child exit, a historical cleanup claim, elapsed time or a free
guard cannot replace the operator's review. No process is killed automatically. If all required
identities cannot fit the bounded review, fail closed rather than omit them.

Bind the review to target UUIDs, frozen history digest, all control rows, exact SQL row versions,
operator worker, original owner epoch, observed process identities, inspection time and nonce. The
review expires after 60 seconds. Reopen required evidence and repeat process observations outside
SQL immediately before committing; recheck freshness after lock acquisition and before return.
Only in-memory guard lease checks occur inside SQL. SQL cannot make an uncooperative writer's
filesystem activity atomic; the trusted-local stopped-writer precondition remains explicit.

Use one short coordinator-first READ COMMITTED transaction. Lock and revalidate the unchanged
snapshot before mutations. Atomically record original interruption, terminal reconciliation attempt,
run/control/worker disposition, coordinator fencing/release, bounded audit events and immutable
receipt. Deferred constraints must admit either the complete original occupied state or the complete
terminal state, never a release shortcut. All writers that can race these rows participate in the
same fence, including direct SQL writers and snapshot-isolated transactions. No IO spans this
transaction. Failure rolls back the entire disposition.

## Existing numerical bounds and proposed metadata envelope

No existing limit is raised:

- Original filesystem envelope: 67,108,864 bytes and 221 entries; fixed 17 core operations and two
  separate unused post-success inspection slots. No recovery filesystem allocation is consumed.
- At most 20 attempts per run, reserving capacity for the terminal reconciliation attempt; at most
  128 control rows. History and controls use bounded queries with an overflow sentinel.
- At most 32 explicitly reviewed descendant identities, matching the existing review allowance.
  Mandatory recorded identities are also inspected; none may be silently discarded.
- Review freshness: 60 seconds. Existing terminal target and review limits: 8,192 bytes each.
- Existing recovery ancestry/snapshot limit: 65,536 bytes. The proposed full-prefix snapshot must
  fit that limit using ordered identifiers, versions, lengths and digests, not payload copies.
- Suffix journal: at most 15 operations, six phase rows per operation (90 total), and 15 checkpoints.
  Existing per-record maxima are 1,048,576-byte commands, 65,536-byte phases/results,
  1,048,576-byte evidence/selection documents, 3,145,728-byte checkpoint proofs and a 65,536-byte
  final completion proof. FETCH/Bronze/admission records retain their own existing bounds.
- Audit event detail remains at most 8,192 bytes. `AuditCapacity` retains its 1,000,000-event ceiling,
  4,096-event reserve per unfinished run and three-event reserve per active import. The fixed six
  safe-end events specified above are capacity-checked before mutation; no silent extra events.

Use a versioned canonical snapshot with separately computed ordered component digests for admission,
FETCH, Bronze, Silver reservation, suffix operations/phases/checkpoints, artifacts/dependencies,
controls and prior attempts. Include exact cardinalities and byte lengths, reject extras and gaps,
and verify each referenced component independently before trusting its digest. Do not treat a
caller-supplied hash as reconstruction. Bound both queries and serialization; failure to fit 64 KiB
is an explicit qualification failure, not permission to widen the bound. One immutable receipt per
run/original attempt binds the disposition, target, review and before/after metadata.

## Death, uncertain acknowledgment and historical resolution

Before commit, process death cannot create a durable terminal owner. Confirmed rollback leaves
original occupancy and evidence unchanged. A new call requires a new guard-bound qualification,
review and unchanged-original-state validation; it does not adopt a previous capability. Uncertain
commit outcomes are inspected, never blindly retried. Absence of a receipt alone is not proof of
rollback. If authoritative resolution cannot establish the complete prior state, remain unresolved.

Review expiry or guard loss observed after SQL commit must never be reported as rollback. Once
commit may have occurred, return CONFIRMED_TERMINAL from independently validated historical receipt
or UNRESOLVED if confirmation is unavailable. A late freshness/guard exception must enter that
resolution path, not a catch block that permits fresh mutation. Freshness and lease checks before
commit can abort; postcommit checks cannot revoke a committed historical disposition.

After commit, restart or acknowledgment loss can resolve a terminal receipt without a guard, current
files or process inspection. Independently reconstruct SQL ancestry, canonical digests, exact audit
correspondence and terminal rows. Later unrelated queue work is allowed: validate monotonic
coordinator epoch/version and exclusion of former owners rather than requiring an eternally empty
slot. SQL unavailability or inconsistent history yields UNRESOLVED.

Apply equivalent read-only resolution to existing original success: prove final completion, actual
artifact/dependencies, counts, attempt/run state and atomic release. Neither success nor terminal
inspection returns launch authority. Any already-committed separate recovery handoff whose opaque
owner was lost remains occupied under #124; this proposal cannot recover it or repeat takeover.

## Acceptance evidence not yet met

This document records the reviewed contract, not final integrated acceptance. Implementation and focused local tests are present; their scope and the remaining qualification gates are tracked in [the restart qualification procedure](../operations/native-content-restart-qualification.md). Required acceptance evidence includes:

1. Independent design and migration review accepting incomplete-prefix eligibility, direct atomic
   disposition, exact audit/state mappings and conservative evidence vetoes.
2. Real separate-JVM death/restart across FETCH/Bronze and each suffix family: reservation, files,
   start-before-child-record gap, release-before/after delivery, live child/Spark work, partial native
   publication, candidate, checkpoint insertion/commit, and final success insertion/commit.
3. Explicit interruption before/after Silver and Gold artifact registration, which are separate
   transactions between checkpointed steps. Preserve their evidence without final Content success.
4. No new requests, child launches or publication writes after restart assessment or terminal action;
   no growth of evidence paths/budget; retained SQL/files unchanged except authorized lifecycle rows.
5. Process/namespace mismatch, stale or incomplete reviews, unmanaged writers, orphan/gateway cases,
   missing/changed evidence, bounds overflow, competing controls, expired deadlines where applicable,
   lost ownership, direct SQL bypass and concurrent/stale-snapshot writer tests.
6. Real transaction rollback and lost-ack scenarios at every terminal mutation, receipt inspection in
   a fresh process, confirmed success rejection, and unrelated queue use after terminal release.
7. Exact final-head CI and capacity evidence without weakening existing partitions or gates.

Design review accepted the bounded contract under the owner’s in-scope decision delegation.
Implementation may proceed within this contract, with independent migration and fault-test review.
Until the acceptance evidence is complete, normal dispatch and existing terminal/recovery callers
remain unchanged.

## Delivery effort boundary

This proposal requires a bounded history validator, new SQL lifecycle/closure guards and substantive
crash/concurrency qualification. It is not a trivial operator adapter. Parent/owner acceptance must
include the resulting estimate before implementation proceeds if it exceeds the agreed batch.
