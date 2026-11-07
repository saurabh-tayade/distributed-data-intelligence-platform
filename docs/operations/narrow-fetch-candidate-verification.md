# Inactive native FETCH candidate observation

The package-private `NarrowContentFetchCandidateVerification` seam implements
[ADR 0032](../adr/0032-inactive-fetch-candidate-verification.md). Only tests invoke it; it is not
an API, operator command, dispatcher or recovery path.

## Observed boundary

- Independently reconstructed admission, original READY identity, runtime-chain correspondence,
  native reservation and six ordered phases through `CANDIDATE_EXITED`.
- Repeated original ledger, root/lock identity, FETCH-only scope and exact five-file inventory
  checks, with the immutable candidate digest and bounded identity/mtime/size observations.
- Existing native raw HTTP journal semantic validation, canonical descriptor and strict metadata
  parsing/schema checks without changing the native metadata encoding.
- A detached bounded observation with no owner, packet, launch, checkpoint or storage-write authority.

A valid raw HTTP journal is not XML validity, a successful pipeline step, a completed publication
or a recovery decision. Missing or changed evidence fails closed without repair or additional HTTP.
No SQL mutation, filesystem content/namespace mutation, completion file or slot release occurs.
The held installation guard and existing ledger lock coordinate reads without creating new files.
Historical FETCH registration and completion paths remain unchanged.

## Verification

Local production compilation uses the JDK 21 compiler module and existing pinned dependency
artifacts. Maven and Docker are unavailable in the implementation environment. Full Maven,
PostgreSQL, native-child loopback and restart execution therefore belong to hosted CI, not to a
local test claim. The new suites are assigned to the `native_fetch_recovery` partition; its fail-closed
report inventory must account for both suites before its aggregate check succeeds.

Authored coverage includes genuine strict-child HTTP, non-XML raw payloads, repeat and fresh-JVM
inspection, actual disposable PostgreSQL restart, unchanged SQL/filesystem snapshots, incomplete
launch phases, matching-inventory invalid journal semantics, mutated candidate files, original
identity/ledger changes, foreign stages, prefixes, completion markers and bounded path safety.
CI results for the submitted commit are required before merge.

Issue #49 and Gate B0 remain incomplete. Durable candidate disposition/completion, subsequent native
stages, explicit recovery and ordinary Content dispatch remain separate work.
