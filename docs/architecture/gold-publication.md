# Immutable Gold publication and replay (M2.4)

[gold_publication.py](../../processing/gold_publication.py) implements the publication portion of [ADR 0004](../adr/0004-gold-analytics-publication.md). It integrates the [M2.2 validated Silver boundary](silver-input-validation.md) and [M2.3 transformations](gold-transformations.md) with local Parquet writes, read-back verification and immutable replay. No dependency or infrastructure is added. [M2.5 implements standalone DuckDB analytics](duckdb-analytics.md); retained publisher-data reconciliation remains M2.6.

## Acceptance and execution

Acceptance requires two schema-correct datasets, full row-multiset equality with computed Gold, required values, unique primary keys, selected Silver provenance, reconciled observation/warning counts, unchanged selected Silver files, and a completion marker written last. Replay must reverify an existing publication without writing it. Failures leave incomplete artifacts for manual inspection and never resume or overwrite them.

Use Python 3.12, Java 21 and the existing pinned PySpark 4.0.2 dependencies. From the repository root, with Java 21 selected in `JAVA_HOME`/`PATH`:

```sh
export PYSPARK_PYTHON="$PWD/outputs/silver-python312/.venv/bin/python"
export SPARK_LOCAL_IP=127.0.0.1 TZ=UTC PYTHONPATH=processing
"$PYSPARK_PYTHON" processing/gold_publication.py \
  --silver-root outputs/bronze-to-silver \
  --gold-root outputs/gold
```

Both roots have the defaults above and are local filesystem paths. Repeat `--source SOURCE_ID` to select exact source IDs, or repeat `--marker PATH` to select exact Silver `complete.json` paths, relative to the Silver root or absolute beneath it. The two filter forms are mutually exclusive; no filter selects all completed publications in the configured root. The CLI delegates selection to M2.2. It does not glob arbitrary Silver or Gold Parquet.

The CLI prints one JSON result: `PROCESSED` or `SKIPPED` with a root-relative publication path, fingerprint, input/output counts and run diagnostics. Failures print `FAILED`, a fixed `code`, and relative `path`/structured `details` when available. Exit status is 0 for successful publication/replay, 1 for processing failures and 2 for invalid arguments. Raw article content and exception messages are excluded from these diagnostics; Spark's own logs can still contain paths and internal errors. A shutdown failure after successful publication returns `SPARK_SHUTDOWN_FAILED`; a retry verifies the already completed publication.

## Reusable interfaces

```python
from gold_publication import run, publish, verify_frames
from silver_inputs import load_inputs

result = run(spark, silver_root="outputs/bronze-to-silver",
             output_root="outputs/gold", sources=["nasa-technology"])

# Or reuse a previously validated input set:
inputs = load_inputs(spark, "outputs/bronze-to-silver")
result = publish(inputs, "outputs/gold")
```

`publish(inputs, output_root)` builds both Gold DataFrames internally and owns the full write/verification lifecycle. `run(spark, silver_root, output_root, sources=(), markers=())` first calls `load_inputs()`. Both raise `PublicationFailure` for publication errors or propagate M2.2 `InputFailure`. The exceptions expose fixed `code`, relative `path` and `details` fields.

`verify_frames(inputs, expected, actual)` performs Spark value/key/provenance, reconciliation and full-row checks and returns only two scalar output row counts. The publisher inspects every physical file schema before using this function; it is not a standalone file-integrity check. `output_inventory(directory)` inventories both exact flat dataset directories using root-relative paths, byte lengths and SHA-256 digests; it does not itself validate completion metadata.

## Layout and verification sequence

```text
outputs/gold/v1/processor_revision=1/input_set=<sha256>/
  inputs.json
  gold_articles/
  gold_source_daily/
  complete.json
```

The publisher creates the fingerprint directory exclusively. It writes `inputs.json` with M2.2's exact canonical manifest bytes and Spark writes each dataset with `errorifexists`. Gold v1 field order/types are checked against the explicit schemas before writing. Empty datasets retain schema-bearing Parquet files. Paths below the configured output root must not be symlinks; unexpected visible dataset entries and missing data files fail validation. Spark bookkeeping files beginning with `.` or `_` are excluded from the analytical inventory and never used as completion evidence.

Verification first inventories all output Parquet files and inspects each physical schema before reading with explicit schemas. It checks all required fields, valid versions, warning arrays, positive/bounded counts, timestamp consistency and primary-key uniqueness. It reconstructs each article's selected Silver row and checks membership in the validated accepted observations. Spark `exceptAll` in both directions compares complete Gold row multisets with the computed results, preserving nulls, warning order and multiplicity. This also checks distinct counts, selected winners and first/last timestamps. Global article/daily observation totals and daily warning-code totals reconcile to Silver; article rows reconcile to distinct input article IDs. Decimal aggregate totals avoid 64-bit overflow during reconciliation; out-of-range counts fail.

After all Spark actions, the publisher rechecks output hashes and manifest bytes, then calls `inputs.verify_unchanged()`. It writes `complete.tmp` exclusively and renames it to `complete.json` in that directory. A write, read-back or input-integrity failure cannot produce a completion marker. Temporary or partial files remain for inspection.

## Completion metadata v1

| Field | Content |
| --- | --- |
| `publication_version` | Integer `1` |
| `gold_contract_versions` | `gold_articles: 1`, `gold_source_daily: 1` |
| `processor_revision` | Integer `1` |
| `input_set_fingerprint` | M2.2 canonical manifest SHA-256; matches directory name |
| `input_manifest_sha256` | SHA-256 of the exact canonical `inputs.json` bytes; equal to the fingerprint |
| `input_counts` | `accepted` after replay collapse, `accepted_before_replay_collapse`, `rejected`; publication copies count once |
| `duplicates` | `duplicate_publications`, `duplicate_accepted_rows` |
| `output_counts` | Row counts for `gold_articles`, `gold_source_daily` |
| `files` | Per-dataset arrays of `path`, `byte_length`, `sha256`, ordered by UTF-8 path bytes |

Counts are nonnegative integers. Completion metadata uses the same canonical JSON serializer as the input manifest. Replay rejects malformed JSON, duplicate keys, missing/extra fields, wrong types (including booleans/floats in place of integers), unknown versions and any metadata differing from the independently computed values. It requires exact stored manifest bytes and file inventory/digests, then repeats full read-back verification and the final input-integrity check before returning `SKIPPED`.

`duplicate_selections` and discovered incomplete Silver paths are returned as run diagnostics, not stored in the completion marker. They depend on filter spelling and current discovery, whereas the canonical inventory identifies the publication. Persisting those as replay identity would incorrectly reject equivalent selections. Publication-copy and replay-row duplicates are inventory-derived and are stored and verified.

## Failure and recovery

| Code | Meaning and action |
| --- | --- |
| M2.2 input codes, including `NO_COMPLETED_INPUTS` and `INPUT_CHANGED` | Fix selection or restore immutable Silver inputs; no completed Gold output is created |
| `INCOMPLETE_OUTPUT` | Directory already exists without completion; stop writers and inspect/quarantine manually or select a fresh Gold root |
| `OUTPUT_LAYOUT_INVALID` | Missing/unexpected files, invalid directory structure or symlinks; inspect the publication |
| `INVALID_OUTPUT_MARKER`, `OUTPUT_METADATA_MISMATCH`, `OUTPUT_MANIFEST_MISMATCH` | Invalid completion metadata, differing inventory/counts or changed manifest; never replay as valid |
| `OUTPUT_SCHEMA_MISMATCH`, `OUTPUT_INVALID_VALUES`, `OUTPUT_DUPLICATE_KEY` | Physical schema, value or grain mismatch; inspect/quarantine the output |
| `OUTPUT_PROVENANCE_MISMATCH`, `OUTPUT_ROWS_MISMATCH`, `OUTPUT_RECONCILIATION_FAILED` | Read-back differs from validated Silver/computed Gold; investigate before retrying |
| `OUTPUT_CHANGED` | Output changed during verification; stop concurrent edits |
| `OUTPUT_WRITE_FAILED`, `OUTPUT_READBACK_FAILED`, `GOLD_PROCESSING_FAILED` | Local filesystem or Spark operation failed; inspect Spark logs and incomplete output |
| `INPUT_IDENTITY_MISMATCH`, `COUNT_OVERFLOW` | Inconsistent validated-input identity or unsupported count range; investigate the caller/data |

The publisher never deletes or overwrites an existing publication, repairs a marker, or labels corrupted output `SKIPPED`. A changed canonical input inventory uses a new fingerprint directory and retains earlier output. Recovery is manual; never delete Bronze or edit completed Gold to force replay. Full replay verification currently requires the selected Silver inputs to remain available.

## Tests and limits

```sh
"$PYSPARK_PYTHON" -m unittest discover -s processing/tests -p test_gold_publication.py -v
"$PYSPARK_PYTHON" -m unittest discover -s processing/tests -v
npm run check
git diff --check
```

Tests use synthetic completed Silver publications and temporary directories. They cover normal publication and byte-preserving replay, equivalent selections, replay-collapsed counts, strict metadata, corrupt Parquet, every-file schema checks, invalid values, duplicate keys, altered provenance, mismatched rows with unchanged totals, reconciliation, new fingerprints, input mutation during actions, partial writes, read-back/marker failures, empty datasets and CLI results.

Local validation passed **19 publication tests** and **64 complete processing tests** with Python 3.12.15, Java 21 and PySpark 4.0.2 in `local[2]`. Markdown/local-link checks, repository hygiene and `git diff --check` passed. The existing deliberately corrupt Silver test emitted its expected Spark checksum diagnostic; all assertions passed. No retained publisher-data reconciliation or distributed benchmark was run.

The accepted single-writer convention requires immutable Silver inputs and no concurrent manual output edits. There is no lock, transactional snapshot, multiwriter isolation, exactly-once or fsync/power-loss guarantee. Hash checks cannot detect mutations reverted between checks. Both publication and replay perform multiple Spark scans/shuffles plus file hashing; only scalar aggregates and file inventories reach Python. There is no caching policy or measured scalability claim. Parquet filenames and bytes need not be deterministic between independently produced publications; logical results and replay identity follow the contracts. M2.5 implements a [pinned completed-publication consumer and DuckDB queries](duckdb-analytics.md), validating completion metadata and file integrity before use.
