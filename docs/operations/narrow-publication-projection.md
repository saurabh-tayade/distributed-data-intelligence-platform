# Inactive temporary-marker sizing projection

November 5, 2026. Pure, inactive continuation of the
[whole-envelope qualifier](narrow-publication-envelopes.md), based on PR #102 head
`680d4ec9b311fe9d632d8442f7261a000639e6e9`.
**No managed publication hook, runtime selection, admission, sink adoption or dispatch is added.**
Gate B0 remains incomplete; normal Content dispatch remains disabled.

## What this slice establishes

`processing/narrow_publication_projection.py` has no file I/O. Its versioned functions consume
retained native capture values and immutable copies of supplied command/context data:

- `bind_context_v1` snapshots the complete original v1 PUBLISH command. It checks the command's
  runtime digest against the complete supplied runtime configuration, derives the output binding
  ID from that configuration, and compares the command's output binding with the separately
  supplied loaded binding. These inputs must come from independent authoritative loaders.
- `project_gold_v1` and `project_content_v1` validate the captured candidate marker, manifest,
  inventory and namespace correspondence, then construct an in-memory **sizing projection**.
- `compare_actual_v1` checks correspondence with separately obtained actual native verification.
  It never returns a replacement result, grants publication authority or repairs output.

The context factory does not load or validate a runtime, perform SQL checks, authenticate a
caller, or establish that a supplied binding belongs to the configuration's registration ID.
Its public dataclasses are not capabilities. Matching values or hashes alone cannot create
READY or execution authority. The factory validates supplied correspondence only.

## Exact marker projection

The source must be an actual `content_metadata.Evidence` captured with `complete.tmp` and no
`complete.json`. Root/namespace guards, ordered dataset records, names, retained metadata bytes,
lengths/hashes and marker inventory must match. No prospective inode or stamp is invented.

The projection changes exactly:

1. The marker's relative file path from `complete.tmp` to `complete.json`.
2. That entry in the publication root's directory listing, sorted as the existing capture sorts it.

Gold/Content file stamps contain device, inode, size, mtime_ns, mode and link count. They omit
ctime. Directory and ancestor records contain device/inode/mode and directory names, not
mtime. The supported same-directory rename therefore preserves all captured stamp fields.
Fresh native-consumer tests compare projected capture with actual capture after that rename.
The analytical file paths, metadata bodies, all hashes, file ordering and namespace identities
remain unchanged. Existing native inventory limits remain authoritative; no new file-count,
component-size or stamp-width cap is introduced.

A retained capture is not a live filesystem observation. The pure functions deliberately do not
call `Evidence.check`. A future integrated caller must qualify all complete projected envelopes,
then check the retained capture, dependencies and guard identities immediately before rename,
refuse any final marker, then perform actual
native verification and whole-envelope qualification before recording success. Mutation after
the snapshot is never permission to trust its projection.

## Content coverage and smoke

Content coverage is supplied as the actual native `public(coverage_totals(...))` value from the
already validated precompletion relation. Its nine totals must use the native canonical decimal
string representation within `MAX_COUNT`. The projection neither queries nor fabricates coverage.
For the future integrated path, preserve the existing all-row check **before** temporary marker
creation; this new metadata sizing step belongs after `complete.tmp` exists.

The three completed-publication smoke results cannot be claimed before completion. Their retained
schema is nevertheless exact: fixed ordered report names, a 64-character lowercase SHA-256 and
an integer row count of 0 or 1. Every legal smoke array has exactly **339 canonical UTF-8 bytes**.
The projection uses explicitly size-only representatives for those fixed-width fields. This is
a consequence of the existing schema, not a new component cap or a claim that reports ran.
No pretend completed opener is instantiated to manufacture smoke success.

After real completed-publication verification, comparison requires the actual smoke shape,
ordering, hash grammar and integer row domain, plus equal complete byte length. It normalizes
only these fixed-width smoke values for comparison; every other field, including actual coverage,
completion, capture and prepared data, must be byte-identical. The real smoke hashes/results
remain in the actual evidence and are never replaced by the sizing representatives.

`SizingProjectionV1` is a frozen, explicitly non-authoritative object and is not directly
JSON-serializable. `sizing_value()` returns a fresh hypothetical verifier-shaped value solely
for passing into the existing complete-envelope qualifier. It must never be persisted as
`evidence.json`, registered as native verification, returned as a successful operation result,
or accepted as a replacement for a real verifier. Tests use explicitly labeled `.sizing-fixture.json`
files only; these are synthetic test vectors, not execution or recovery evidence.

## Context transport remains a separate boundary

This slice chooses no serialized command extension and adds no descriptor file or ledger slot.
The original command remains complete and unchanged. The new in-memory context is not a wire
protocol and is not counted as if it were already present in a command.

The recommended later transport is a compact top-level context in a new managed command version,
not another container around every command and not an unreserved descriptor file. It must carry
its fixed policy/version/kind and output binding ID, bind them to the independently loaded runtime,
and be charged inside every full nested command/proof/recovery/RESUME envelope. New strict
constructors/readers must preserve original commands without weakening v1 readers or pinned
legacy modules. No selected new command schema or compatibility path is implemented here.

Before that integration can be admitted, further work must also establish:

- Exact full recovery receipts from the current one-fresh-source/no-retained-Silver profile,
  including original run/source correspondence and SQL/native checks; no empty receipt defaults.
- Authoritative READY reconstruction, expected fixed operation slot and parent-owned command seal.
- Child validation of independently supplied launch expectation and the v2 ledger/root/lock identities.
- Current recovery writer ownership distinct from embedded original command provenance, with the
  existing nonrecursive inherited-origin restriction preserved.
- Full packet/ancestry and lifetime capacity, complete interruption and restart tests, and safe
  retention of `complete.tmp` when sizing fails. No new retry/repair/republication permission.

Gold qualification remains stage-local. Passing it does not promise that future Content will fit.

## Tests and reproducibility

Python tests cover exact temporary-to-final capture parity, pure/no-I/O behavior, immutable
snapshots, wrong runtime/binding, malformed/partial/duplicate marker evidence, inventory mismatch,
coverage typing and fixed smoke grammar, empty publications and escaped 902-file stresses.
Retained-evidence fault injection checks mutation, mode changes and a foreign final marker before
rename; an injected qualifier exception leaves the temporary output untouched. A post-rename
same-bytes/new-inode replacement fails correspondence, and actual completed requalification is
read-only. These are deterministic fixture/fault tests, not an OS process-crash or database restart.

The fixture helper uses real DuckDB-authored Parquet and native Gold/Content consumers. Gold's
managed VERIFY shape is produced by the exact constructor expression from `managed_gold.py`,
not by running Spark, Silver provenance or `gold_publication.verify_publication`. Content's actual
`managed_content.verify` opens the completed publication and runs all three real smoke reports.
Synthetic Silver command inputs and runtime configurations are serializer scaffolding, never
submitted commands or authoritative managed qualification.

The optional Java parity test reads four fresh cases, checks projected/actual length and normalized
body equality, and passes both values through #102's unchanged whole-envelope qualifier. The small
case passes; the existing 902-file counterexamples fail their actual combined slots. No native
consumer acceptance is confused with managed admission.

```sh
PYTHONPATH=processing:processing/tests python -B -m unittest test_narrow_publication_projection -v
python -B processing/tests/publication_projection_fixtures.py --output outputs/projection-parity
mvn -f services/content-api/pom.xml -Dtest=PublicationProjectionParityTest \
  -Dprojection.parity.directory="$PWD/outputs/projection-parity" test
```

The exporter refuses an existing output directory. Without the system property, the Java corpus
case is explicitly skipped. Ordinary Java unit results are not fresh native parity evidence.
All native publishers, legacy adapters, runtime manifests, database migrations and production
routing remain unchanged. No Spark producer, restart/recovery integration or dispatch safety
claim follows from these pure tests.

Local validation used Python 3.12.14, DuckDB 1.4.5 and JDK 21.0.13, not the pinned managed
Python 3.12.15 execution gate:

- 55 Python tests passed across the new 11 projection tests and existing narrow profile,
  report and publication lifecycle regressions.
- Fresh four-case/eight-vector corpus plus the #102 qualifier suite: 14 Java tests passed,
  with no skips. An independent review reran all 11 new Python tests and generated its own
  fresh eight-vector corpus for the Java parity test; no blocking code or documentation findings.
- Full content-api unit run: 535 tests, 532 passed and three optional corpus tests skipped.
  The explicit corpus runs are reported separately; overlapping tests are not additional coverage.
- Markdown/local links, repository hygiene and whitespace checks passed.

No Spark producer, PostgreSQL or full managed-runtime integration suite was run for this slice.
