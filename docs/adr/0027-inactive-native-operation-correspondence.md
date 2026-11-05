# ADR 0027: Inactive native operation correspondence

Date: November 5, 2026

Status: **Inactive read-only implementation; native routing and execution policy remain proposed.**

## Context

The original narrow reservation grammar allocates 19 directories, but a directory's ordinal
is not necessarily a source index or SQL stage ordinal. The historical adapters also reuse
some native checkpoint UUIDs across different sidecar directories. Directly replacing their
random UUIDs with fixed ledger directory UUIDs would change reader assumptions.

The [V11 probe](0026-narrow-operation-journal-and-bootstrap.md) occupies CHECK_INPUTS/0 once.
That probe is neither native Content preparation nor permission to reuse its slot. The first
native CHECK_INPUTS reservation corresponds to Gold's unchanged-input recheck; Content
preparation corresponds to ledger CHECK_INPUTS/1 and historical SQL CHECK_INPUTS/0.

## Read-only boundary

Add a separate correspondence catalog, derived from the original adapter call graph and the
existing [producer budget](../operations/content-evidence-producers.md). Cover every original
slot once, keeping ledger coordinates, native command family/mode, native checkpoint origin,
and historical SQL attribution separate. The extra pair describes one post-success Content
inspection, not a general read-only inspection pool. Recovery slots are outside this catalog.

Inspect the registered Content-to-Fetch configuration chain independently of command-provided
runtime hashes. Bind it to the frozen run selection and independently reconstructed READY
identities. Check canonical registered bytes, exact supported configuration fields and versions,
parent/child hashes, profile/deployment links and frozen storage membership. Return immutable
configuration snapshots. Do not execute the legacy Bronze context loader: it depends on delivery
journals and credentials, beyond this read-only configuration boundary.

Inspect native protocol-v1 command headers against these snapshots and separately supplied exact
native checkpoint observations. Check canonical size/shape, run/attempt/epoch, family/mode,
source ordinal and relevant frozen source/runtime/binding fields. This is identity correspondence,
not complete command-payload or native dependency verification. The checkpoint observations must
come from original invocation facts, never the command under inspection. No new checkpoint is
allocated and no mapping is persisted.

All successful results explicitly remain non-launchable. Runtime version 2 still means the
historical bounded adapters using ledger version 1, not the READY ledger version 2 strict sink.
Projection protocol-v2 commands and READ_ONLY_PROBE commands are not silently translated into
native protocol-v1 commands. Installed JAR/module/executable bytes, storage root identities,
credentials, native inputs and publications remain unverified by the SQL-only chain inspector.

## Alternatives and consequences

Conflating ledger and SQL ordinals would assign the wrong stage to native work. Aliasing all
ledger checkpoint UUIDs to historical native UUIDs would change the sealed reservation grammar
and still leave the old Gold readers' directory assumptions unresolved. Reusing V11 as a native
journal would change its immutable one-shot probe contract. None of those changes is selected.

The catalog provides testable correspondence and exposes these gaps without changing accepted
historical contracts. It does not choose a future native bootstrap, journal schema, command
constructor, auxiliary-stage lifecycle, installed runtime registration, recovery policy or
post-success inspection authorization. Those decisions and their integration tests remain open.

No migration, private evidence file, reservation, publisher, process launch, stage-history
transition, dispatcher or deployment registration is added. The original 64 MiB / 221-entry plan
is unchanged. #49 remains open, Gate B0 remains incomplete, and Content dispatch stays disabled.

The [implementation and verification record](../operations/narrow-native-operation-correspondence.md)
records the complete mapping and limits of the checks.
