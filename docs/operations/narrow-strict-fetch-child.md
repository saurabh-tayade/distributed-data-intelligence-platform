# Inactive strict ledger-v2 FETCH_SOURCE child

November 5, 2026. This is an additive child-only slice for
[issue #49](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/issues/49).
**Content dispatch remains disabled. No production caller launches or authorizes this child.**
The boundary and one-shot prefix semantics are recorded in
[ADR 0029](../adr/0029-inactive-strict-fetch-child.md).

## Bootstrap and independent identities

`platform.ingestion.managed.StrictManagedRssFetch` accepts no arguments. On Linux its main
requires FIFO stdin. A daemon receiver reads at most 65,536 bytes with a five-second total
deadline: reaching that cap fails immediately, and successful frames are nonempty and at most
65,535 bytes. Complete JSON without EOF cannot inspect caller-selected files or issue HTTP.
A stalled receiver cannot launch work after the timeout. No selected path comes from argv,
environment variables or marker discovery.

The canonical UTF-8 packet has exactly six top-level fields:

- `bootstrapVersion`: integer 1, independent of ledger and historical native versions.
- `mode`: `FETCH_SOURCE_CANDIDATE`, distinct from the V11 read-only probe.
- `childPid`: canonical positive decimal string matching the actual process.
- `expected`: installation, policy, ORIGINAL run/attempt/source, explicit CONTROL root,
  CONTROL/attempt/lock device and inode, ledger digest, frozen identities and exact writer.
- `current`: a separately supplied policy, frozen tuple and writer that must match.
- `fetch`: separately supplied source revision, captured configuration and endpoint digests,
  URI, FETCH runtime digest, exact Java runtime, synthetic-endpoint flag and HTTP timeout.

The writer includes installation, worker instance, epoch, run, attempt, fixed FETCH_SOURCE/0,
deterministic checkpoint and complete command digest. The frozen runtime is the CONTENT
runtime; the independent FETCH runtime is bound to the native command. Their chain is not
qualified here. Identity numbers and epochs are decimal strings bounded to nonnegative signed
Java longs, with inode/PID/epoch positive. Unknown fields, coercions, duplicate keys, invalid
UTF-8, invalid Unicode scalars, trailing tokens and noncanonical encoding fail closed.

The exact canonical historical protocol-v1 fetch command is reconstructed from those supplied
expectations and compared byte-for-byte with the sealed command. Its digest alone is not enough.
No independent value is learned from the command or ledger. Matching supplied values are still
only correspondence: this child does not query SQL, prove READY, verify installed artifacts,
acquire the installation host guard, authorize an owner or validate a registered runtime chain.

## Strict sink and native output

`StrictFetchEvidence` reconstructs all 19 ORIGINAL operation directories, deterministic UUIDs,
final and preparation slots. The whole canonical ledger and its independently supplied hash
must match. Missing markers, downgraded or alternate coherent ledgers, replaced identities,
unknown files, retained prefixes, hard links, symlinks and unsafe permissions are rejected.
Checks cover explicit CONTROL ancestry, attempt and lock identities before and after record
locking, command digest, bounded tree inventory, per-slot capacity and stable ledger readback.
No directory, ledger, command or capacity is created by the child. Java NIO checks the
pathname identity/stamp around channel opening and locking, plus channel size; it does not
provide the Python sink's separate opened-descriptor `fstat` identity check. These are
cooperating-writer checks, not isolation from arbitrary hostile same-user path replacement.

Before a possible network call the existing `result.json.preparing` is exclusively created,
file-forced and directory-forced. The original invocation retains its open channel and exact
identity. Only that invocation temporarily permits that unchanged empty prefix. The ledger
lock is released during HTTP and reacquired for every evidence operation. A competing process
or later invocation refuses the prefix without making a request. No automatic retry exists.

The child uses `HttpFeedFetcher` with its unchanged 5 MiB payload ceiling, no redirects and no
proxy, and `S3BronzeStorage.preparedMetadata` without creating an S3 client. Metadata and
descriptor bytes are bounded before the first candidate write. Payload bytes, original UUID,
instant, status and headers remain exact. `StrictFetchCandidate` independently reopens the payload, metadata and descriptor through
the bounded sink and reconstructs their complete native semantics before publishing OK while
the original empty prefix remains owned. The unchanged read-only `ManagedRssFetch.reopen`
then validates the completed candidate again. Neither reader calls the legacy execute or
legacy writer paths.

Only `payload.xml`, `metadata.json`, `descriptor.json` and `result.json` are writable, using
existing final/prefix slots, create-once bounded files, forced readback and atomic no-replace
hard-link publication. Repeated writes cannot overwrite identical or differing bytes. Neither
`complete.json` nor a new success/adoption record is produced. A result sidecar alone does not
establish a managed checkpoint, original journal commitment or permission for Bronze work.

HTTP failures produce the existing bounded failure result when possible. Other failures retain
partial evidence and the consumed prefix. An interrupted request may have reached its peer;
its failure write preserves and restores the thread's interrupt after final checks. A kill
between final-link and prefix-unlink leaves a hard-linked pair that all subsequent strict reads
refuse. No interrupted or failed state is cleaned up, repaired, adopted or refetched.

## Verification and boundaries

The new ingestion tests launch actual Java child processes against a synthetic loopback server.
They cover framing, EOF gating, deadline, exact packet and command correspondence, identity and
path rejection, payload bounds, HTTP failure, duplicate/concurrent execution, retained crash
prefixes and unchanged read-only journal validation. Sink tests cover full-plan parity, exact
caps, no-overwrite writes and explicit force/publication fault boundaries. The mandatory parity
path compares with the unchanged Python plan, which the existing backend test independently
compares with the actual `ContentOperationPlan` bytes.

```sh
mvn -B -ntp -f ingestion/pom.xml verify
mvn -B -ntp -f services/content-api/pom.xml \
  -Dtest=NarrowEvidenceSinkPlanTest,ContentOperationPlanTest test
python3 -I -B -m unittest discover -s .github/ci/tests -v
```

The current local executor has Java 21.0.13 and its compiler module but no Maven, Jackson 2 or
JUnit artifacts. New Java compilation and unit execution require hosted CI; they are not
claimed as local passes. The 47 local CI policy tests, Markdown/local-link lint, repository hygiene, PostgreSQL suite
inventory and whitespace checks pass. The unchanged Python plan probe also runs locally, but
this does not establish Java/Python parity until the Java test runs. Final hosted results are recorded
in the pull request rather than inferred from local source inspection.

Historical Java entry points, Python modules, V11 READ_ONLY_PROBE behavior, SQL schemas,
registered runtime manifests, budgets and dispatch remain unchanged. This is not a durably
authorized managed run. Native operation reservation/bootstrap integration, active-operation
heartbeat semantics, independently qualified installation/runtime facts, supervision, original
journal completion, downstream native adapters, recovery ancestry and real PostgreSQL restart
acceptance remain open. The active prefix must not cause a future parent to weaken normal
strict inspection or reuse the probe's heartbeat logic without a separate design.
