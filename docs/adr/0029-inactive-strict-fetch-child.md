# ADR 0029: Inactive strict ledger-v2 fetch child

Date: November 5, 2026

Status: **Bounded implementation within the owner-approved narrow Content direction; inactive and not durably authorized.**

## Context

The existing fetch child can produce the original raw-feed journal, but its bounded wrapper
uses ledger v1 and ancestor discovery. Calling that wrapper or its legacy execute method
cannot establish the independent identities and exact ledger-v2 plan required by the narrow
Content profile. The existing read-only probe's V11 operation record is not a native fetch
reservation and cannot authorize this child.

## Additive child boundary

Add a separately named Java child, `StrictManagedRssFetch`, with no command-path argument.
Its fixed stdin-pipe bootstrap has an independent version and mode, a complete frame smaller
than 64 KiB, a five-second total receive deadline and an EOF gate. Parsing, exact field/type
checks and independent expected/current correspondence finish before inspecting any
caller-selected path or making a request. The packet binds the actual child PID but does not
prove its OS start identity, parent provenance, live ownership, READY state or SQL authority.

The packet supplies separate ORIGINAL expectation and current-writer records, with the frozen
selection/profile/Content-runtime identities, CONTROL/attempt/lock identities and canonical
ledger digest. Separate expected FETCH facts bind the source, revision, endpoint, registered
fetch runtime and exact historical command. The CONTENT and FETCH runtime digests are different
roles, not interchangeable identities. A future parent must qualify their registered chain.

A new strict sink reconstructs the complete original ledger independently. It retains exactly
the existing operations, deterministic checkpoints, file capacities, 64 MiB reservation,
221 entries and 128 KiB inventory ceiling. It never initializes, reserves, repairs, discovers
ancestor markers, adopts recovery evidence or writes another operation's files.

The child calls the existing low-level HTTP fetcher and metadata serializer. Its journal and
result shapes remain compatible with the existing read-only validator. A strict read-only
candidate validator reconstructs native semantics before OK while retaining the empty prefix;
the legacy read-only validator independently checks the finished candidate afterward. It never calls
`ManagedRssFetch.execute` or its legacy write helpers. Only the existing payload, metadata,
descriptor and result slots are writable. The parent-owned command and `complete.json` are
never written. There is no object-store client, SQL mutation, stage history or production
launcher in this slice.

## One-shot evidence before a possible request

A process killed after request delivery but before the first candidate write must not leave a
command-only directory that another child would fetch again. Before the HTTP call, under the
cooperative ledger lock, the new sink exclusively creates the already reserved
`result.json.preparing`, forces its file and operation directory, and retains the open file
and observed identity. It releases the shared lock before network work, so independent
inspection need not wait for HTTP. Every write reacquires the lock and revalidates the tree.

This prefix means **a fetch may have occurred**, not that it did, succeeded or was durably
permitted. Only the originating invocation can recognize its exact unchanged empty prefix
and fill it through the retained open channel. Every new process and the normal strict
reader refuse the prefix. No restart adoption, cleanup, repair or repeated fetch occurs.
A force failure before the callback sends no request but conservatively retains the prefix.

Publishing uses an atomic no-replace hard link, directory force, prefix unlink and another
directory force. A crash between link and unlink retains both names and the two-link inode;
all later invocations refuse the retained state. An interrupted request may already have
reached its peer. A bounded failure result is attempted with the interrupt temporarily
cleared, then the interrupt is restored after final evidence checks. Missing failure evidence
is not proof of quiescence or safe cancellation.

## Alternatives and remaining obligations

Holding the ledger lock across HTTP would serialize duplicates but would block inspection and
would not prevent a later restart after a kill. An unforced in-memory flag would not survive
restart. A new marker filename or allowance is unnecessary because the existing result prefix
already charges the uncertain state. None of these mechanisms replace the future durable
native operation reservation and single-use bootstrap protocol.

The active prefix also means a future native parent's heartbeat cannot blindly reuse the V11
read-only probe's full READY inspection on every heartbeat. Journal-aware active-operation
checks, owner fencing, supervision, durable failure disposition, interruption/restart tests and
recovery still require separate integration. The general strict reader is not weakened.

Filesystem checks use the existing cooperating-writer model, not sandbox isolation against an
arbitrary hostile same-user process. Historical classes, V11, readers, migrations, runtime
registrations, publication formats and dispatch capabilities stay unchanged. Gate B0 remains
incomplete and [issue #49](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/issues/49)
remains open. See the [implementation report](../operations/narrow-strict-fetch-child.md).
