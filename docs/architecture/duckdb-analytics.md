# DuckDB analytics over a pinned Gold publication (M2.5)

The standalone consumer validates one completed [M2.4 publication](gold-publication.md), registers `gold_articles` and `gold_source_daily` in an in-memory DuckDB session, and runs bounded named reports. It requires only Python, DuckDB and that Gold directory. Spark, Java, Silver files, SeaweedFS, cloud credentials and a running database service are not needed for querying. Retained NASA/Guardian/xkcd integration remains M2.6.

Acceptance requires exact snapshot selection, strict metadata/hash/schema checks, marker row-count and cross-table reconciliation, parameterized filters, deterministic ordering, and tests proving the query path works after Spark stops and Silver is removed.

## Installation and version choice

Use Python 3.12 and the dedicated [requirements file](../../processing/requirements-analytics.txt):

```sh
python3.12 -m venv outputs/analytics-python312/.venv
outputs/analytics-python312/.venv/bin/python -m pip install -r processing/requirements-analytics.txt
export ANALYTICS_PYTHON="$PWD/outputs/analytics-python312/.venv/bin/python"
```

DuckDB is pinned to **1.4.5**, the requested candidate and an [LTS patch release](https://duckdb.org/2026/06/17/announcing-duckdb-145). Its Python 3.12 wheel and Spark-produced Gold v1 Parquet types are verified by the integration tests. Keeping the reviewed candidate provides a reproducible baseline; moving to another release requires rerunning these compatibility checks. No pandas, PyArrow or Spark dependency is introduced into this consumer. The existing processing CI installs this requirements file alongside the unchanged Spark/Boto3 requirements and includes it in its pip cache key.

## Selecting and querying a publication

```sh
"$ANALYTICS_PYTHON" processing/gold_analytics.py \
  --gold-root outputs/gold \
  --publication v1/processor_revision=1/input_set=<fingerprint> \
  --report publishers

"$ANALYTICS_PYTHON" processing/gold_analytics.py \
  --gold-root outputs/gold --publication <fingerprint> \
  --report daily --source alpha \
  --start-date 2026-10-12 --end-date 2026-10-13 --limit 100
```

Replace `<fingerprint>` with the exact 64-character lowercase SHA-256 returned by M2.4. `--publication` is mandatory and accepts either that fingerprint or the exact root-relative versioned directory path. Absolute publication paths, traversal, unsupported namespaces, glob characters and symlinks beneath the configured root are rejected. The explicitly configured Gold root is resolved locally. There is no discovery glob, latest pointer or union across snapshots.

All reports accept optional `--source` and `--limit` (default 100; range 1–1000). `daily` and `quality` also accept inclusive ISO `--start-date`/`--end-date`; date filters on the other reports are errors. A limit bounds returned rows, not the amount of validation or query work. Results may be truncated; no pagination interface is provided yet.

The CLI prints JSON with `status`, `publication`, `report`, `limit`, and `rows`. Null metadata remains JSON null. Dates use `YYYY-MM-DD`; UTC timestamps use ISO format with `Z` and six fractional digits. Errors have fixed `code` and safe `path` fields rather than underlying database exception text. Exit codes are 0 for success, 1 for publication/query failures and 2 for invalid arguments. `--help` requires no publication or Spark.

## Python interface and metric definitions

With `PYTHONPATH=processing`:

```python
from gold_analytics import open_publication

with open_publication("outputs/gold", fingerprint) as analytics:
    publishers = analytics.report("publishers", limit=100)
    daily = analytics.report("daily", source="alpha", start="2026-10-12", end="2026-10-13")
    articles = analytics.report("articles", source="alpha", limit=20)
    quality = analytics.report("quality", source="alpha")
```

`open_publication` verifies metadata, bytes, every physical file schema and SQL invariants before returning `GoldAnalytics`. Both views belong to the same pinned publication. `report` accepts only the four named queries in `REPORTS`; filters and limits are SQL parameters. File paths enter DuckDB through the relational API, not interpolated SQL. Report results are bounded lists of dictionaries; Python timestamps are naive UTC instants under the Gold contract. `close()` and context-manager exit release the in-memory connection. An unrestricted SQL method is not part of the public interface.

| Report | Grain and semantics | Ordering |
| --- | --- | --- |
| `publishers` | One source row from `gold_articles`: `article_count = count(*)`, summed `observation_count`, earliest `first_observed_at`, latest `last_observed_at`, and count of catalog rows without `published_at` | `source_id` ascending |
| `daily` | Exact `(source_id, ingestion_date)` rows from `gold_source_daily`, with all five daily metrics | Source then UTC ingestion date ascending |
| `articles` | Selected article metadata, warning codes, observation counts and all selected Silver/Bronze provenance fields | `last_observed_at` descending, then `article_id` ascending |
| `quality` | Source totals over the selected daily date range: observations, warning codes, and `warning_codes_per_observation = sum(quality_warning_count) / sum(observation_count)` | `source_id` ascending |

The quality rate counts **codes per accepted observation**, not the proportion of observations with warnings. It can exceed one. Empty inputs and absent groups produce no rows, not fabricated zeros. Missing publication timestamps do not imply warnings. There is no global distinct-ingestion field in the publisher report: summing per-article distinct-ingestion counts would double-count shared ingestions, and counting selected ingestion IDs would omit historical ingestions. Daily distinct counts retain their defined group scope. No engagement, ranking, category or cross-publisher story metric is inferred.

## Reader checks and limits of reconciliation

[gold_reader.py](../../processing/gold_reader.py) validates the exact v1/revision-1 namespace, strict completion fields and integer types, canonical `inputs.json` bytes/checksum/fingerprint, and recorded input-manifest structure and counts. Only manifest metadata is examined; no Silver path is opened. The small canonical serializer is shared with M2.2 through [publication_metadata.py](../../processing/publication_metadata.py); its original imports remain available for existing callers.

Output inventory paths must be unique, sorted, flat Parquet paths in the corresponding dataset. The reader checks lengths and hashes and rejects missing, extra or altered analytical files and symlinks. Only known Spark bookkeeping names (`_SUCCESS`, its CRC, and CRC files corresponding to listed Parquet files) may accompany the inventory. Other hidden files are not silently accepted.

[gold_analytics.py](../../processing/gold_analytics.py) checks every file's column names, order and DuckDB logical types against Gold v1 before registering views over exact file lists. Hive partition inference and schema union are disabled, so directory names cannot introduce columns and one file cannot hide another file's schema drift. Required fields, versions, count bounds, warning arrays, timestamps and primary keys are validated in SQL.

Both table row counts must match completion metadata, and each table's summed observations must equal recorded accepted input observations. Source sets, source-level observation totals, earliest observed dates and latest observed timestamps must agree across tables. Daily distinct article counts cannot exceed the source's catalog size. Selected-article warning totals cannot exceed the corresponding all-observation daily warning totals. Exact historical warning totals, per-day distinct counts and winner provenance cannot be recomputed independently without Silver; the consumer relies on M2.4's recorded publication verification for those guarantees. It does not duplicate the Gold transformations.

`verify_metadata(root, identity)` is a reusable metadata/byte checker; complete query validation requires `open_publication`. Bytes are rechecked after opening and before/after each report to detect observed changes. The publication must remain immutable for the session. These checks are not transaction isolation and cannot detect edits reverted between checks. Checksums verify consistency with the recorded publication, **not cryptographic authenticity against a trusted external authority**; someone able to replace all files and metadata can create another self-consistent publication.

## Synthetic examples and validation

The integration fixture publishes repeated Atom observations for `alpha` and one observation for `beta` through M2.4. Spark is then stopped and Silver deleted. Publisher results include:

```json
[
  {"source_id": "alpha", "article_count": 2, "observation_count": 4},
  {"source_id": "beta", "article_count": 1, "observation_count": 1}
]
```

These excerpts omit the report's timestamp and missing-publication-date fields. Daily expected metrics are:

| Source | UTC ingestion date | Observations | Distinct articles | Distinct ingestions | Warning codes |
| --- | --- | --- | --- | --- | --- |
| alpha | 2026-10-12 | 2 | 2 | 1 | 0 |
| alpha | 2026-10-13 | 2 | 1 | 1 | 4 |
| beta | 2026-10-13 | 1 | 1 | 1 | 0 |

The filtered `quality` report for alpha on October 13 returns two observations, four warning codes and a rate of `2.0`. Across both days its rate is `1.0`. Article A's selected ingestion timestamp is `2026-10-13T00:00:00.000001Z`, while its publication timestamp is null. An independently selected earlier publication contains only two observations and remains queryable explicitly.

To run producer-backed tests, use the existing Python/Spark development environment with both requirement files installed and Java 21 selected:

```sh
export PYSPARK_PYTHON="$PWD/outputs/silver-python312/.venv/bin/python"
export SPARK_LOCAL_IP=127.0.0.1 TZ=UTC PYTHONPATH=processing
"$PYSPARK_PYTHON" -m pip install -r processing/requirements-bronze.txt -r processing/requirements-analytics.txt
"$PYSPARK_PYTHON" -m unittest discover -s processing/tests -p test_gold_analytics.py -v
"$PYSPARK_PYTHON" -m unittest discover -s processing/tests -v
npm run check
git diff --check
"$ANALYTICS_PYTHON" processing/gold_analytics.py --help
```

Tests cover integrity failures, traversal and symlinks, every-file schema validation, counts/reconciliation, empty snapshots, report metrics, nulls, UTC microseconds, deterministic ties, injection-shaped filter values and CLI validation. A separate process blocks Spark/producer imports and has an invalid Java path while running all four reports. No storage services or publisher requests are used.

Local validation passed **14 analytics tests** and **78 complete processing tests** with Python 3.12.15, DuckDB 1.4.5, Java 21 and PySpark 4.0.2 for synthetic publication generation. The consumer subprocess needed neither Spark imports nor Silver. Markdown/link checks, repository hygiene, CLI help, syntax compilation and `git diff --check` passed. An initial test incorrectly assumed title ordering for tied ingestion timestamps; the assertion was corrected to the contract's article-ID ordering before the successful runs. Existing deliberate-corruption tests emitted expected Spark diagnostics. Hosted CI and retained publisher-data integration were not run.

Validation scans and hashes complete datasets; report limits do not avoid that cost. DuckDB may scan and sort more rows than it returns. Sessions use memory, disable temporary spill files and extension auto-install/loading, and may fail explicitly if resources are insufficient. No performance or scalability claim is made. M2.6 remains reconciliation against the retained publisher dataset, with dataset-specific counts and evidence recorded separately from these synthetic tests.
