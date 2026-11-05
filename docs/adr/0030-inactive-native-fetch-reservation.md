# ADR 0030: Inactive native FETCH_SOURCE reservation

Date: November 5, 2026

Status: **Bounded reservation-only implementation within the owner-approved separate-journal direction; execution remains inactive.**

## Context and boundary

[ADR 0026](0026-narrow-operation-journal-and-bootstrap.md) reserves the one-shot read-only
probe separately from historical stage history. [ADR 0029](0029-inactive-strict-fetch-child.md)
adds a candidate-producing strict fetch child, but does not grant it durable launch authority.
This slice introduces only the first native FETCH_SOURCE/0 reservation. It does not select a
new installed-runtime registration or a candidate-only execution outcome policy.

V12 adds `content_native_operations`, an immutable table distinct from V11. A live narrow
ORIGINAL owner may reserve one deterministic FETCH_SOURCE/0 slot. The operation ID and fixed
slot stay consumed, including after a lost commit acknowledgment or process death. The existing
owner channel stops after uncertain management outcomes. No automatic repair, retry, takeover,
slot release, recovery continuation or adoption is available.

A probe-used attempt is not eligible. Both directions are enforced under the same coordinator
lock: native reservation refuses V11 history, and a new probe refuses native history. The
additive exclusion trigger preserves historical rows and the original V11 migration/function.
Both additive guards require READ COMMITTED isolation. Snapshot-isolated writers are rejected
because an unchanged coordinator lock alone cannot refresh a cross-table snapshot.

## Independent facts and historical inspection

The parent reconstructs READY from immutable enrollment/milestones and the strict filesystem
ledger, then separately reads the registered Content-to-Fetch chain. It builds the exact
historical protocol-v1 fetch command from the frozen source, registered fetch configuration,
durable owner identity and fixed checkpoint. The bootstrap reservation facts contain separately
constructed expected and current records, preserving Content and Fetch runtime hash roles.

The reservation envelope deliberately uses `reservationVersion` and
`mode=FETCH_SOURCE_RESERVATION`, without a child PID. It cannot be accepted by the strict child.
It is not a complete startup packet, release authority or runtime qualification.

Before insertion, the parent repeats READY inspection, rejects even an empty existing fetch
directory, and checks the exact live owner versions, controls and deadline. Within the owner
transaction it reconstructs the metadata again and compares exact canonical bytes. The SQL
trigger independently reconstructs the command/bootstrap, traverses the registered hash chain,
and rejects unknown fields, duplicate keys and scalar-type substitutions. No SQL transaction
spans filesystem work. As with the existing protocol, filesystem observations are cooperative,
not a distributed atomic fence.

Facts-only inspection requires neither a live owner nor the attempt filesystem. It canonicalizes
stored bytes, independently reconstructs the original facts from SQL, verifies both digests and
returns only identifiers/digests with `nativeLaunchCompatible=false`. It never recreates a
session, owner, sink, child packet or continuation. Historical runtime-v2 records still describe
ledger-v1 adapters; matching their configuration does not qualify strict native execution.

## Bounds and remaining work

The database adds at most one command of 65,536 bytes and one bootstrap-facts envelope of fewer
than 65,536 bytes per narrow admitted attempt. There are no phase rows or arbitrary events.
No filesystem entry or allowance is added: the original plan remains exactly 67,108,864 reserved
bytes and 221 entries. Existing registrations, native readers, V11 rows and stage semantics are
unchanged. Startup validates all twelve migrations and the new table.

No dispatcher calls this package-private seam. It does not write command files, launch a child,
contact an endpoint, write completion, record stage success, release the attempt or settle a
candidate outcome. Qualified runtime registration, active-operation supervision, single-use
release, candidate verification/completion, durable outcome/recovery policy and broader native
routing remain separate work. #49 remains open and Gate B0 remains incomplete.

See the [implementation and validation record](../operations/narrow-native-fetch-reservation.md).
