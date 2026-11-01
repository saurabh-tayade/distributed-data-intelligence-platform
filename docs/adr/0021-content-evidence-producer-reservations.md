# ADR 0021: Content evidence producer reservations

Date: October 31, 2026

Status: **Proposed — owner acceptance pending; producer safeguards partially implemented. Gate B0 remains blocked.**

## Implementation amendment — November 1, 2026

The implementation work separates durable command preparation from launch authorization.
Prepared bytes are forced and read back before an intent references their address/digest;
preparation alone never grants launch authority. Legacy retained commands keep their readers.

Capacity accounting must charge both a final file and its interrupted preparation file,
all parent directories and the canonical inventory's worst-case serialization. Reservations
are monotonic: an interrupted or rejected operation does not reclaim its allowance. New
work must validate the actual tree against its reservations before extending it. A shared
cross-process lock serializes compliant writers; a reservation is not a filesystem quota.

In particular, parent-side accounting cannot bound the current children that directly write
sidecars. A supported profile therefore also needs fixed versioned child sinks or parent-owned
result persistence. Installing a ledger alone must not advertise a supported profile, and a
legacy child must not be authorized under a profile requiring bounded sinks. The implementation
record must distinguish completed safeguards from any remaining producer integration gap.
This amendment does not accept this ADR or authorize Content dispatch.

The fixed version-2 Java/Python sinks and monotonic reservations are now implemented for
an internal one-fresh-feed validation candidate. The existing version-1 readers and Python
processor bytes are preserved. A recovery qualifier reserves the second independent
observation's exact directory identities before sealing its first observation; extending a
sealed mutable ledger would otherwise correctly fail the existing immutable-file comparison.
No evidence file is exempted from that comparison.

This implementation proves a private-write ceiling, not full qualification availability.
The native inventory/metadata ceilings have not been jointly proved to fit the smaller
private proof slots after every otherwise valid publication. The
[implementation report](../operations/content-evidence-producers.md) states that blocker,
the candidate calculation, tests and restrictions. Version 2 is not an accepted operational
profile and does not authorize dispatch. Retain this ADR as Proposed.

## Context

Gate B0 requires a useful Content execution profile whose evidence remains inspectable
after every supported interruption. The [completion investigation](../operations/content-recovery-envelope-completion.md)
does not establish that guarantee. The existing 64 MiB / 4096-entry capture is a bounded
reader of already written files. There is no aggregate producer reservation, and the
1 MiB inventory and recovery packet are independent limits. Native datasets are already
referenced and independently requalified; their Parquet bytes are not private sidecars.

The audited baseline also had a concrete small-workload interruption gap: PUBLISH_CONTENT committed an
intended namespace before creating its command; the command address/hash were recorded only
at authorization. Initial capture/review could succeed with that null link, but native
reconciliation still requires the command and cannot disposition the run. Partial child/result JSON
can likewise leave process identity unresolved. Raising the byte cap cannot resolve either
case. Missing or corrupt historical evidence must continue to fail closed.

## Proposed decision

Evaluate an additive, versioned **fixed Content evidence protocol** before widening capture.
Use the existing stage adapters, supervision, native publishers, coordinator and guarded
recovery. Do not add a workflow engine or change native schemas/identities.

1. Specify one reviewed profile's exact metadata and output envelopes, including fresh
   sources, retained inputs, reference inventories, maximum serialized names and selections.
   Choose workload limits from that proof; a one-feed label alone proves nothing.
2. Let the supervising adapter own private evidence persistence. Each fixed child returns
   a bounded typed result through the existing supervised boundary or a versioned bounded
   spool. A spool needs a real producer bound, not a watcher reacting after a write. Separate
   native data/scratch roots from the private evidence budget. Preserve full read-only native
   qualification, process identity and cleanup observations.
3. Before durable stage intent and child authorization, reserve the full enumerable file
   and directory set, canonical inventory growth and serialized packet/observation overhead.
   Charge commands, anchors, process sidecars, partial writes and orphaned reservations.
   Apply the same lifetime accounting to read-only inspections, RESUME qualifications,
   RECONCILE observations and qualification packets. Refuse exhausted capacity with a fixed
   private classification before starting more external work. Cooperative reservations are
   not hard filesystem quotas or a guarantee against unrelated disk consumers.
4. Persist and seal the command before an intent references it. Define explicit recoverable
   pre-launch and interrupted-sidecar states in the new protocol. Parent observations must
   distinguish no authorization, possible launch and known child identity. An unresolved
   writer still vetoes release; neither a partial JSON document nor a missing file proves
   that no child ran. Test every boundary before advertising recovery support.
5. Where large identical immutable metadata recurs, use only exact sealed references with
   original storage/root identity, inventory, byte/hash seals, stage roles and lineage.
   Independently reread and requalify every original dependency. Retain journal payloads
   required by the old protocol. Never substitute unchecked paths or copy VERIFIED rows.
6. Keep old runtime/module hashes and old readers for retained attempts. Introduce new
   entry points/profile versions rather than editing pinned historical modules in place.
   Capture and verify original/inherited trees independently; a new profile cannot confer
   recoverability on an oversized or unavailable old ancestor.

The implementation must demonstrate a complete numerical bound below **all** reader limits,
or separately propose a narrowly versioned reader change. No new numeric workload envelope,
public error code, migration, automatic retry or dispatch capability is approved by this ADR.

## Alternatives and tradeoffs

**A — Profile-only restriction:** smallest if proved, but current producer boundaries do not
enforce the required lifetime/partial-write budget. Retain A as the target interface, with the
narrow producer changes above supplying its missing guarantee. Do not silently narrow the
existing 1–8-feed metadata submission contract.

**B — Sealed references:** complementary to A where exact metadata is duplicated. Native
data and SQL bytea snapshots already avoid body duplication. References alone do not bound
new sidecars, inspection frequency or interrupted writes. Versioning and retained readers
cost more than deleting duplicates, but preserve historical recovery integrity.

**C — Streaming:** defer unless A/B cannot support a useful profile. Chunked hashing reduces
buffer memory, but does not bound producers or make a 1 MiB packet fit. The earlier larger
capture candidate in the B0 audit is an unimplemented alternative, not an accepted solution.
Any future streaming proposal must separately bound bytes, entries, depth, memory, elapsed
time, blocked-I/O supervision and serialized inventories.

Blanket RLIMIT_FSIZE on a Python/Spark process is not a private-tree aggregate quota: it also
affects native output/scratch, does not bound entry count, and changes failure behavior after
publication effects. Polling directory sizes after writes has the same late-detection problem.
Neither is selected as a shortcut.

## Consequences and validation required

This is a specific next implementation boundary, not completion of Gate B0. Production
Content dispatch stays disabled. No accepted ADR status changes. The current tests retain
occupied ownership for missing/ambiguous evidence and preserve incomplete output bytes.

Before acceptance as implemented: prove the full per-profile budget, exercise every maximum
and first rejected boundary, interrupt command/process/result persistence and publication,
verify no lost evidence/duplicate effect, recover original and inherited attempts after real
PostgreSQL restart, and run Content/RESUME/C3c/CI regressions within existing CI budgets.
Record measured native sidecar sizes separately from the worst-case proof.
