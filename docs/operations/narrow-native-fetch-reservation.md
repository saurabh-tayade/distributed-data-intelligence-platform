# Inactive native FETCH_SOURCE reservation

This is the reservation-only follow-on to the
[strict fetch child](narrow-strict-fetch-child.md), under
[ADR 0030](../adr/0030-inactive-native-fetch-reservation.md). It does not execute that child.

## Implemented boundary

- V12 adds immutable `content_native_operations`, separate from V11 probe history.
- One ORIGINAL FETCH_SOURCE/0 slot and operation ID are permanently consumed per attempt.
- Canonical command and bootstrap facts are independently constructed from READY, frozen source,
  registered runtime chain and durable ownership, then reconstructed inside the final owner fence.
- SQL validates exact nested facts, types, duplicate/unknown fields and the registered hash chain.
- Bidirectional probe/native exclusion serializes through the coordinator lock and refuses
  isolation levels other than READ COMMITTED.
- Missing READY, existing fetch directory, stale owner versions, controls and uncertain commits
  stop reservation. No filesystem writes, child launch or external request occurs.
- Historical inspection verifies SQL facts without the host guard or attempt filesystem and
  returns no owner, session, launch packet or recovery authority.
- No historical registration, native format, stage lifecycle or dispatch gate changes.

The original filesystem budget is unchanged: 64 MiB and 221 entries. SQL consumes no more than
one 64 KiB command plus a bootstrap-facts record smaller than 64 KiB per admitted attempt.

## Verification

Local execution:

- Compiled all Content API production Java sources on JDK 21 with the existing pinned dependency
  artifacts. This is compilation evidence, not a Maven test pass.
- A standalone pure-construction smoke check passed for canonical facts, separate runtime
  identities, defensive bytes and mismatched-owner rejection.
- PostgreSQL was not run locally. Database acceptance belongs to the hosted PostgreSQL partitions.

Added unit coverage constructs the exact historical command, separates Content/Fetch runtime
roles, rejects mismatched independent ownership, checks the deterministic checkpoint and tests
bounded defensive byte snapshots.

Added actual PostgreSQL acceptance covers populated V11 upgrade without backfill, immutable
rows, independent command/bootstrap identity binding, simultaneous reservations, native/probe
exclusion races, owner/control changes, commit acknowledgment loss, occupied directories, forced
writer-JVM death, actual disposable PostgreSQL restart, and fresh-JVM facts inspection. Tests assert unchanged filesystem and legacy
metadata, occupied attempts, no completion and no stage execution. The core CI partition includes
the new integration class, and its report verifier requires actual passing test results.

Hosted CI status is recorded on the pull request for the exact submitted commit. Authored tests
must not be reported as executed until that run passes.

## Still inactive

Historical runtime-v2 configurations remain ledger-v1 correspondence evidence, not strict-child
registration. This slice makes no candidate-only outcome decision. Native command-file preparation,
qualified runtime selection, process identity/release, active-prefix-aware heartbeats, candidate
verification/completion and durable failure/recovery disposition need subsequent reviewed slices.
The reservation envelope cannot be submitted to the child as a bootstrap packet. Ordinary Content
dispatch stays disabled; #49 and Gate B0 are not complete.
