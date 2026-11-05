# Inactive ORIGINAL-only Content child sink

November 5, 2026. Additive foundation for
[issue #49](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/issues/49),
following the [strict reader](narrow-content-strict-reader.md),
[tracked admission](narrow-content-admission.md) and
[versioned projection transport](narrow-projection-transport.md).
**No production caller, launcher, runtime registration, publisher or dispatcher uses this sink.**
Ordinary Content dispatch remains disabled and Gate B0 remains incomplete.

## Independent in-memory expectations

`processing/narrow_evidence_sink.py` supplies a pure `validate_expectations` function and
an inactive `StrictSink`. The immutable inputs have no production wire or persisted format:

- `Expected` supplies installation, exact narrow policy, ORIGINAL plan kind, original run
  and attempt, source, explicit CONTROL root and its device/inode, attempt-root and lock
  device/inode, exact ledger digest, frozen selection/profile/Content-runtime digests, and
  the exact expected current writer.
- `Writer` supplies installation, worker instance, coordinator epoch, run/current attempt,
  stage, ordinal, checkpoint and full command SHA-256.
- `Current` separately supplies policy, frozen identity tuple and current writer. Every
  value must correspond to `Expected`; the current attempt must be the original attempt.

Missing or malformed expectations, a different installation/worker/epoch, alternate operation,
wrong checkpoint/command digest, recovery, RESUME and inherited attempts are refused. The
module derives checkpoint UUIDs exactly as Java `UUID.nameUUIDFromBytes` over the raw
policy/run/attempt/stage bytes. It does not use namespace-prefixed Python `uuid3`.
VERIFY_SILVER intentionally shares SILVER's checkpoint while retaining a distinct directory.

This is **correspondence checking, not provenance or authorization**. The module does not
query SQL, acquire an installation host guard, authenticate READY, load registered producer
modules, verify native dependencies, or prove that supplied current ownership is live. A future
parent must independently obtain and validate those facts. Matching values copied from the
inspected command or ledger cannot establish authority. A successful read or write does not
consume an operation or authorize a child. Original native command mode/kind and complete
runtime-chain correspondence remain the separate transport/routing boundary, not this byte sink.

## Fixed plan and read-only probe

The sink derives exactly one attempt root and one operation directory from its independent
inputs. It never discovers ancestor markers, consults process-local marker memory, initializes,
reserves, creates directories, repairs or falls back to the legacy protocol. The parent-created
operation directory and sealed command must already exist.

The complete v2 ORIGINAL ledger is reconstructed independently, including all 19 operations,
final/prefix slots and directory names. Its canonical bytes and hash must match the supplied
ledger hash and the bounded bytes actually read. No internally coherent alternate source,
plan, policy, capacity or downgraded ledger can substitute for that expectation.

The original allowance remains exactly **67,108,864 bytes and 221 entries**. Conservative
inventory serialization is checked against the existing 128 KiB narrow ceiling. The sink
also checks actual bytes, entries, depth, per-file capacities and private canonical paths.
Unknown entries, retained preparation prefixes, symlinks, hardlinks, unsafe permissions,
missing markers, unavailable evidence and identity changes fail closed.

`inspect()` is a read-only probe returning accounting totals. `read_command()` reads only the
exact operation's command, bounded by that slot, and verifies its independently supplied digest.
The bytes remain opaque here; neither method dispatches or interprets a native command.

The POSIX record lock is shared with existing compliant Java/Python writers. CONTROL ancestry,
attempt-root and lock identity are checked before opening the lock, after opening it, after
acquisition and at completion; the opened descriptor must match the expected pathname.
Lock replacement or modification while waiting is refused. Ledger bytes, identity and timestamps
are checked across each inspection. An in-process lock also serializes threads, since POSIX
record locks alone do not. These checks retain the cooperative-writer model, not filesystem
isolation against an arbitrary hostile process running as the same user. Lock acquisition has
no separate launcher deadline; no supervised production entry point is introduced.

## Create-once child writes

`write(name, bytes)` accepts only `evidence.json`, `selection.json` or `result.json` when
that name is actually reserved in the exact operation. It cannot write another operation,
parent-owned commands, process sidecars, verified anchors, native outputs or arbitrary paths.
Exact capacity is accepted; the next byte is rejected before creating a prefix. Existing final
or prefix files are retained and refused even when their bytes equal the requested output.

A new prefix is created exclusively, written, forced, reread and its directory forced. An
atomic hard link publishes the final name without replacement; the directory is forced before
unlinking the prefix, then forced again. A race cannot overwrite an existing final. A crash
between link and unlink leaves both names and a two-link inode. Those bytes remain within the
already charged final/prefix allowance, and subsequent strict probes refuse them without
repair. A failed write is not proof of process quiescence, no native effect or safe retry.
There is no durable operation-consumption record or cleanup/recovery protocol in this slice.

## Compatibility and remaining gates

`bounded_evidence.Sink`, all historical Python modules, runtime hashes, Java production code,
SQL schemas, native formats and public contracts remain unchanged. The old sink still consumes
ledger v1; this new sink is not silently selected by the existing version-2 runtime candidate.

Before any effectful integration: review bootstrap transport and its complete byte bound,
durable single-use operation allocation, semantic stage/ordinal mapping, command mode/kind,
a new runtime/entry-point identity, current-owner/READY checks around launch and supervision,
failure persistence, actual producer-module validation, native interruption and real PostgreSQL
restart tests. Recovery and RESUME need distinct current-writer/ancestry handling. This
foundation neither changes an ADR's status nor closes those gates.

## Validation

```sh
python -m unittest discover -s processing/tests -p test_narrow_evidence_sink.py -v
mvn -f services/content-api/pom.xml \
  -Dtest=NarrowEvidenceSinkPlanTest,ContentOperationPlanTest,NarrowContentEvidenceContextTest,NarrowContentEvidenceRestartTest test
```

The mandatory Java parity test invokes a fresh Python test-only probe and compares its complete
canonical ledger with the unchanged actual Java `ContentOperationPlan`, including all operation
IDs and capacities and a 128-character source. Python tests use fresh spawned processes with
independently retained test-only expectations. They cover exact/plus-one child and command
slots, all 40 permitted child slots across 19 operations, wrong writer/operation identities,
missing/downgraded/alternate ledgers, unknown entries, lock replacement during waiting,
no-overwrite races, retained partial writes and immutable existing bytes/mtimes on rejection.
Test-only subprocess transport is not a production bootstrap design.

Local validation used Python 3.12.14, DuckDB 1.4.5, Corretto JDK 21.0.13 and Maven 3.9.11:

- 28 new strict-sink Python tests passed. The focused legacy sink, narrow profile, JSON and
  projection transport regression suite also passed.
- Full content-api unit suite: 539 tests, 536 passed and three pre-existing optional corpus
  cases skipped. This includes the mandatory Java/Python full-plan comparison and existing
  actual native publisher parity. The first full run used Python without PySpark and failed
  that existing parity test; the full rerun with the pinned processing environment passed.
- 45 CI policy tests, Markdown/relative-link lint, repository hygiene and whitespace checks passed.
- Independent review identified a Python 3.12 cyclic-symlink exception-normalization gap. The
  fix and fixed-diagnostic/no-mutation regression were independently rechecked, as were strict
  absence checks that refuse permission/I/O failures instead of treating them as missing files.

These are local tests, not hosted CI, new native launch or PostgreSQL restart/recovery evidence.
