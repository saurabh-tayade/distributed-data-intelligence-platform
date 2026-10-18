# Completed-Silver input boundary (M2.2)

[ADR 0004](../adr/0004-gold-analytics-publication.md) was explicitly accepted by the repository owner on October 15, 2026. [silver_inputs.py](../../processing/silver_inputs.py) implements the read-only input boundary used by the [M2.3 Gold transformations](gold-transformations.md). It does not implement Gold publication, DuckDB, or a CLI. Existing M1 producers and schemas are unchanged.

## Acceptance criteria and selection

Acceptance requires deterministic discovery of completed M1 publications, strict marker and per-file schema checks, value/provenance validation, all-or-fail conflict detection, replay collapse, reproducible inventory fingerprints and synthetic regression coverage. No publisher requests, Bronze reads, storage services or cloud credentials are needed.

`discover(root, sources=(), markers=())` returns a `Selection`. The root defaults to `outputs/bronze-to-silver` in the convenience interface. Discovery scans only `v1/*/source_id=*/ingestion_date=*/ingestion_id=*`, not arbitrary Parquet. Exact source IDs are encoded with M1's Base64URL convention. Explicit markers can be root-relative or absolute; source and marker selection are mutually exclusive. Repeated selection paths collapse. Paths are ordered by UTF-8 bytes, independent of filter order. Missing requested sources, empty selections and missing explicit markers fail. Discovered incomplete directories are reported but excluded.

All selected markers must resolve within the root to the exact M1 namespace. Parent traversal is rejected, including traversal that would end inside the root. Symlink targets for markers, dataset directories and every data file must remain inside the root. Explicit in-root marker/directory aliases are supported; discovery does not search arbitrary alias directories. The manifest retains alias paths and records the smallest path as representative of identical logical publications. A standalone copy at a noncanonical path is rejected: M1's canonical namespace has only one path per `(bucket, metadata_key)` in one root. Copies can be explicitly selected through an in-root marker alias with sibling copied datasets; those datasets still undergo full validation and multiset comparison. Different roots are never automatically unioned.

## Validation and replay

`inventory(selection)` parses bounded markers (64 KiB), rejects duplicate JSON keys, checks exact identity/count fields, integer types (excluding booleans), v1 contract/revision, digest syntax, canonical source/UUID/date and bucket/key/path consistency. It inventories both `accepted/` and `rejected/`, including schema-bearing empty datasets. Each directory must contain Parquet files; visible non-Parquet entries or nested datasets fail. Spark's hidden housekeeping files are excluded. Exact marker bytes and all Parquet bytes are hashed with bounded streaming buffers.

`prepare(spark, selection)` fixes the session timezone to UTC and inspects every Parquet file's physical schema before reading with the existing explicit schema. Missing/extra fields and incompatible types fail. It verifies counts, required values, source-scoped article hashes, ordinals, supported warning/rejection arrays and marker/path provenance. Timestamp dates must match the UTC path partition. Required envelope fields must agree across all accepted and rejected rows of an ingestion. Rejected candidates retain nullable article fields under their rejection reasons; they are never promoted to accepted observations.

Spark performs full-row distinct/group checks, joins and multiset comparisons. Identical accepted replay rows collapse; different ordinals or ingestion IDs remain distinct. Differing records at the same replay key fail, as do conflicting ingestion provenance, article identity or accepted/rejected entry classification. A document-level rejection cannot coexist with accepted observations for the same ingestion. Identical publication copies count once; differing marker identities, counts or row multisets fail. Rejected totals are after publication-copy collapse, before any row-level replay collapse; only accepted observations are returned for Gold. The entire selected set must pass before a result is returned.

The inventory is reread and rehashed at the end of validation. New publications after discovery are not added to the frozen selection. Changes to selected files, markers or file membership fail the final check. A fresh selection of already changed, otherwise valid inputs gets a different fingerprint rather than a comparison to an unavailable historical baseline.

## Reusable Python interface

With `PYTHONPATH=processing` and an existing configured Spark session:

```python
from silver_inputs import load_inputs

inputs = load_inputs(spark, root="outputs/bronze-to-silver",
                     sources=["nasa-technology"])
observations = inputs.accepted  # validated SILVER_SCHEMA, identical replays collapsed
manifest = inputs.manifest
fingerprint = inputs.input_set_fingerprint
counts = inputs.counts
# After downstream Spark actions, check the frozen inventory again:
inputs.verify_unchanged()
```

Alternatively call `discover`, inspect its diagnostics, then call `prepare`. `counts` includes `accepted` after replay collapse, `accepted_before_replay_collapse`, and `rejected`, with publication copies counted once. `diagnostics` includes repeated selection count, duplicate publication count, collapsed accepted row count and incomplete marker paths. Invalid inputs raise `InputFailure`; no partial DataFrame is returned. `code`, `path` and `details` are structured fields and the exception string is escaped JSON. Paths are relative; source values and raw JVM errors are not included in this wrapper's messages. Spark itself may log file paths and internal errors.

`manifest` has `manifest_version`, `gold_contract_versions`, `gold_processor_revision`, `silver_contract_version`, `silver_processor_revision`, and ordered `inputs`. Each input has `marker_path`, `marker_sha256`, `identity`, `counts`, `files` and `representative_path`; file records contain `path`, `byte_length` and `sha256`. Version/revision values are 1. Gold metadata describes the accepted target contract, not an implemented Gold output. The exact [canonical serialization protocol](gold-processing.md#versioned-local-publication) is implemented by `canonical_bytes` and `fingerprint`. No absolute root, selection spelling, execution timestamp or mutable latest pointer enters the fingerprint. Reordering/repeating selections and relocating an unchanged root do not change it; adding an alias copy or rewriting equivalent Parquet does.

## Failure classifications

| Code | Meaning / response |
| --- | --- |
| `INVALID_ROOT`, `INVALID_SELECTION` | Fix root, mutually exclusive options, explicit paths or escaped symlinks |
| `SOURCE_NOT_FOUND`, `NO_COMPLETED_INPUTS` | Requested completed inputs are absent; inspect discovery scope/incomplete publications |
| `INVALID_PUBLICATION_PATH`, `MARKER_PATH_MISMATCH` | Restore the canonical M1 namespace and consistent marker |
| `INVALID_MARKER`, `UNSUPPORTED_VERSION` | Restore valid M1 metadata or explicitly implement a reviewed version |
| `MISSING_DATASET`, `INVALID_DATASET`, `INPUT_READ_FAILED` | Restore both immutable datasets/files and local read access |
| `SCHEMA_MISMATCH`, `COUNT_MISMATCH` | Restore complete, compatible Silver publication; count errors include expected/actual values |
| `PROVENANCE_MISMATCH`, `INVALID_ROW` | Investigate invalid Silver values; do not repair or discard rows silently |
| `PUBLICATION_CONFLICT`, `INGESTION_CONFLICT`, `REPLAY_CONFLICT`, `ARTICLE_IDENTITY_CONFLICT`, `ACCEPTED_REJECTED_CONFLICT` | Resolve conflicting immutable inputs before retrying the entire selection |
| `PARQUET_VALIDATION_FAILED` | Spark could not validate/read the selected dataset; inspect local Spark diagnostics |
| `INPUT_CHANGED` | Stop concurrent edits, restore immutable inputs or begin a new selection |

## Tests and limitations

Use the existing Python 3.12.15 environment, Java 21 and pinned PySpark 4.0.2; no dependencies are added. From the repository root, with Java 21 selected via `JAVA_HOME`/`PATH`:

```sh
export PYSPARK_PYTHON="$PWD/outputs/silver-python312/.venv/bin/python"
export SPARK_LOCAL_IP=127.0.0.1 TZ=UTC PYTHONPATH=processing
"$PYSPARK_PYTHON" -m unittest discover -s processing/tests -p test_silver_inputs.py -v
"$PYSPARK_PYTHON" -m unittest discover -s processing/tests -v
npm run check
git diff --check
```

Tests use synthetic M1 publications and temporary directories. They cover marker/path/schema/value errors, conflicts, copies, replay counts, UTC boundaries, rejected-only inputs, file mutations, selection equivalence, root relocation and fixed canonical fingerprint bytes/digest. Existing M1 tests remain unchanged and are included by normal test discovery and the existing processing CI job.

Completed inputs must remain immutable through downstream actions. Returned Spark DataFrames have lazy file lineage; `verify_unchanged()` must be called after downstream actions and before completion of any downstream publication. No caching, transactional snapshot, concurrent mutation protection, locks or exactly-once guarantee is provided. Hash checks cannot detect changes that are reverted between checks. Original Bronze integrity and Silver authenticity are not re-established; empty datasets supply no row provenance. File inventories live on the driver and each footer is inspected separately; full input hashing and Spark validation cost I/O and jobs. Article datasets are not collected to Python, but this is a bounded local implementation, not evidence of distributed scalability. Gold transformations are implemented in M2.3 and [publication/replay in M2.4](gold-publication.md); [standalone DuckDB analytics is implemented in M2.5](duckdb-analytics.md), and [M2.6 retained-data reconciliation passed](../m2-closeout.md).

## Local validation evidence

Validated October 15, 2026 on macOS ARM64 with Python 3.12.15, Java 21.0.7 and PySpark 4.0.2 in `local[2]`. The focused suite passed 18 tests before the final additional ingestion/warning case; the final full processing run passed **40 tests (19 new boundary tests and 21 existing tests)**. Markdown/local-link checks, repository hygiene and `git diff --check` passed. The initial sandbox run could not bind PySpark's loopback gateway; the permitted local-socket reruns executed successfully. Intermediate test failures in temporary-path handling and diagnostic serialization were corrected before the successful runs. Expected corrupt-Parquet test diagnostics, the existing native-Hadoop fallback warning and Python socket ResourceWarnings were observed; final assertions passed. No hosted CI execution, retained-data integration, publisher access, AWS or distributed-cluster validation was performed for M2.2.
