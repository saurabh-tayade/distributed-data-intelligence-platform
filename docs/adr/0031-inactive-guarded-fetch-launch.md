# ADR 0031: Inactive guarded native fetch launch

Date: November 6, 2026

Status: **Bounded implementation of the owner-approved separate versioned launch identity and candidate-only outcome; dispatch remains inactive.**

## Decision and scope

The owner approved a separate launch identity while preserving historical FETCH registrations,
recording successful native output only as a candidate for verification, and retaining uncertain
reservations for explicit recovery rather than automatic refetch. This additive slice connects
[ADR 0030's reservation](0030-inactive-native-fetch-reservation.md) to the
[ADR 0029 strict child](0029-inactive-strict-fetch-child.md), only for literal loopback endpoints.
It does not complete #49 or Gate B0.

V13 adds immutable `content_fetch_launch_profiles` and `content_native_launch_phases`. A launch
profile binds the existing FETCH configuration digest, run profile and CONTROL binding, and a
separate manifest selecting `StrictManagedRssFetch`. Java validates the canonical manifest digest
and bounded installed JAR/executable identities and bytes. Historical runtime-v2 rows and their
ledger-v1 adapter remain unchanged. DNS names, credentials, queries, fragments and non-loopback
addresses are not launchable in this slice. No registration CLI or production caller is installed.

## Ordered effects

A fresh launch consumes the existing one-use native reservation. It never adopts an existing
operation, directory or candidate. Six immutable phases follow in order:

1. `LAUNCH_BOUND` fixes the separate launch profile and digest.
2. `FILES_READY` records that private directories and the immutable command were forced.
3. `START_AUTHORIZED` precedes process creation.
4. `CHILD_RECORDED` records the child PID and process-start identity before release.
5. `RELEASE_AUTHORIZED` records the exact PID-bound packet digest before pipe delivery.
6. `CANDIDATE_EXITED` records a bounded inventory digest after clean, silent child exit.

Each insert uses the live owner fence and an additive READ COMMITTED SQL guard. Phase gaps,
substitution, mutation and duplicate insertion fail closed. An uncertain commit invalidates the
owner channel; no phase is automatically repeated. The actual pipe writer rechecks installed
runtime, command, exact evidence scope, owner versions, controls and deadline before its first
byte. The child still requires bounded input and EOF before opening its sink or making HTTP.

Supervision uses the frozen ledger and exact active FETCH subtree, allowing bounded native
`.preparing` files while the child runs. It does not reclassify active output as READY admission.
Unexpected operations, completion markers, changed command/ledger, controls and lost ownership
fail closed. Process cleanup remains bounded. A killed parent can leave a child whose effects
are uncertain; immutable history neither proves termination nor grants continuation.

## Candidate and recovery boundary

Clean exit requires no observed descendants or stdout/stderr, the exact five candidate files,
a matching result identity and stable repeated inventory. This is not independent semantic
verification of the descriptor, publication verification, a checkpoint, SQL stage success,
completion or reservation release. No `complete.json` is written. Inspection verifies stored
facts without installed files, a live owner or an attempt directory and returns no resume token.

The original filesystem reservation remains 64 MiB and 221 entries. There are at most six phase
rows per operation. Explicit durable failure/recovery disposition, semantic candidate verification,
completion and subsequent native stages remain separate work. Ordinary Content dispatch stays
disabled until full end-to-end recovery is implemented and demonstrated.

## Alternatives

Rewriting the historical FETCH registration would change old-run meaning and was rejected.
Treating process exit as pipeline success would skip independent verification and was rejected.
Automatically repeating an uncertain launch could duplicate the HTTP effect and is not supported.
A separate immutable launch registration adds one bounded record while preserving old contracts.

See the [implementation and verification record](../operations/narrow-guarded-fetch-launch.md).
