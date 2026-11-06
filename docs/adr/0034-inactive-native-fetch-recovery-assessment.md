# ADR 0034: Inactive native FETCH recovery assessment

Date: November 6, 2026

Status: **Bounded read-only implementation; no recovery action or dispatch authority.**

## Decision and scope

[ADR 0033](0033-inactive-fetch-completion-checkpoint.md) records an original-live-owner checkpoint.
The next slice assesses that historical evidence without adopting it. A package-private inactive
seam joins immutable admission, native reservation, launch, completion intent and checkpoint facts.
It returns one bounded, detached summary for an exact run and attempt. It creates no owner,
worker session, token, recovery packet or continuation. It is not installed in an API, CLI or
worker dispatcher. Existing legacy recovery handoff continues to reject narrow admission history.
The legacy operator inspection now also reports `canQualify=false` for that history.

The assessment distinguishes enrollment, PREPARING, READY, native reservation, each ordered launch
prefix, consumed completion intent and durable checkpoint. An intent without a checkpoint stays
incomplete regardless of whether marker bytes exist. Only durable SQL checkpoint correspondence
permits optional read-only filesystem verification. V11 transport-probe history is incompatible
with this native-only assessment and fails closed rather than being mislabeled as unused READY.
Mixed legacy stage history also fails closed, including a schema-valid direct SQL stage insert
after a checkpoint. No migration, historical rewrite, new path or reservation allowance is introduced.

## Separate observations

SQL inspection reconstructs immutable admission and READY expectations, frozen command and
reservation identity, launch phases, exact intent marker, descriptor and checkpoint-reference
correspondence. It does not need the installed runtime or any original file. Invalid SQL history
fails closed. A durable checkpoint remains a durable historical fact if current files are missing,
changed or inaccessible; the separate integrity result reports that limitation.

When a guard for the exact recorded host is supplied and a checkpoint exists, inspection reopens
only the original FETCH namespace. It validates the version-2 ledger, original root/lock identities,
strict scope and budget, candidate bytes and persisted stamps, completion marker, directory identity,
raw HTTP journal semantics, descriptor and native reference. It compares filesystem identities
before and after bounded reads. There is no XML parsing, native child, HTTP request, filesystem write,
repair or refetch. Budget inspection optimistically reads and revalidates bytes, stamps and identities
without opening the ledger lock or waiting on a local mutex. It neither blocks behind unknown
writers nor disturbs another process or thread's lock. An intact observation says nothing about
whether a writer holds that lock. Original completion locking is unchanged. No SQL transaction
spans file or process IO.

The ledger is historically digest-bound, not inode-bound: READY never persisted its inode. A
byte-identical ledger replacement before assessment is therefore indistinguishable and acceptable
if the recorded root/lock identities and all other checks match. Replacement during assessment is
rejected by captured stamps. This slice does not invent stronger historical provenance or claim
atomicity against an uncooperative local writer.

Recorded worker and child PID/start pairs are review targets. Current LIVE, ABSENT, REUSED or
UNAVAILABLE observations are separate, bounded facts. Default OS lookup requires a locally
acquired guard matching the recorded READY host; otherwise process state is UNAVAILABLE, not an
inference from unrelated local PIDs. Missing child records after START_AUTHORIZED,
release/result gaps and incomplete completion are explicit review targets. Absent or reused PIDs,
a successful historical child exit and intact files never establish quiescence. Handler, unmanaged
writer, descendant and orphan review remains necessary, including when no gap is recorded.

## Alternatives and limits

Reusing the legacy recovery handoff would admit narrow histories into a different lifecycle.
Returning a reconstructed owner or executable receipt would turn observation into authority.
Inspecting or promoting marker-only files would bypass the original-owner checkpoint boundary.
All are rejected. Unknown processes or uncertain history stay unresolved; nothing is killed,
released, repaired, retried, inserted or continued.

Choosing recovery ownership, adoption, successor stages or interrupted completion still requires
a separate reviewed design and owner decision. Issue #49 and Gate B0 remain open, Content dispatch
remains disabled, and this assessment cannot make a run successful or free the global slot.

See the [verification record](../operations/native-fetch-recovery-assessment.md).
