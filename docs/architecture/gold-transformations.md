# Gold Spark transformations (M2.3)

M2.3 implements the accepted [Gold v1 contracts](gold-contracts.md) using PySpark 4.0.2. The input boundary remains [M2.2](silver-input-validation.md); this slice creates lazy DataFrames and no persistent Gold output. [ADR 0004](../adr/0004-gold-analytics-publication.md) remains the accepted design.

## Interface and mappings

With `PYTHONPATH=processing` and an existing Spark session:

```python
from silver_inputs import load_inputs
from gold import build_gold, GOLD_ARTICLES_SCHEMA, GOLD_SOURCE_DAILY_SCHEMA

inputs = load_inputs(spark, root="outputs/bronze-to-silver")
gold = build_gold(inputs)  # GoldFrames(articles, source_daily)
# Run downstream actions while the Spark session remains in UTC.
# After those actions, and before any future completion marker:
inputs.verify_unchanged()
```

`build_gold` requires `ValidatedInputs`, sets the session timezone to UTC, and builds both DataFrames from `inputs.accepted`. That accepted frame has passed M2.2 marker, schema, value, provenance, conflict and identity validation, with identical replay rows collapsed. The transformation module does not rediscover or revalidate Silver. `GOLD_ARTICLES_SCHEMA` and `GOLD_SOURCE_DAILY_SCHEMA` define the contract field order and Spark types; explicit projections cast every result to its declared type. As with Silver Parquet, physical nullable flags are not the value-level requiredness contract. Required input values are checked by M2.2; M2.4 must validate Gold required values on publication read-back.

`gold_articles` selects one complete observation per `article_id` with a Spark window ordered by `ingested_at DESC, ingestion_id DESC, entry_index DESC`. Its source metadata, optional article fields, warning array, and selected Silver/Bronze provenance all come from that row. It separately aggregates minimum/maximum ingestion timestamps, observation count and distinct ingestion count across **all** accepted observations of the article. Newer null optional metadata stays null. Repeated entry ordinals from different ingestions and different entry ordinals in one ingestion remain observations. There is no cross-publisher deduplication.

`gold_source_daily` groups by `(source_id, to_date(ingested_at))` in UTC. Spark built-ins compute observation count, distinct article count, distinct ingestion count, the sum of warning-array sizes, and latest ingestion timestamp. Warning totals count codes, so one observation with two warning codes contributes two. Missing groups receive no synthetic rows. Both results keep schema and column order even when the accepted input is empty. Timestamps retain Spark's microsecond precision; metrics are 64-bit integers.

The article aggregate and winner window, and the daily distinct aggregates, require Spark shuffles. A single window for all metrics would be simpler to join but Spark does not support exact `count_distinct` as a window aggregate; grouping then joining preserves the accepted distinct semantics without collecting observations to Python. This implementation makes no performance or scalability claim; the input boundary also hashes files and runs validation jobs. The plans are lazy and retain Silver file references. Call `verify_unchanged()` after all processing actions and before M2.4 marks a publication complete. This detects changes seen at the check, but does not provide a transactional snapshot or protect against concurrent mutation between checks.

## Validation

From the repository root, use Python 3.12, Java 21 and the pinned processing dependencies:

```sh
export PYSPARK_PYTHON="$PWD/outputs/silver-python312/.venv/bin/python"
export SPARK_LOCAL_IP=127.0.0.1 TZ=UTC PYTHONPATH=processing
"$PYSPARK_PYTHON" -m unittest discover -s processing/tests -p test_gold.py -v
"$PYSPARK_PYTHON" -m unittest discover -s processing/tests -v
npm run check
git diff --check
```

Synthetic tests assert exact field order/types, keys, selected metadata/provenance, first/last timestamps, distinct counts, article and daily observation reconciliation, warning-code reconciliation, UTC offset boundaries, late arrivals, ordering independence and empty outputs. Completed synthetic Silver publications exercise the real M2.2 boundary, replay collapse and rejected-only output. This is local synthetic validation, not retained publisher-data reconciliation or a distributed benchmark.

Gold publication and full Parquet read-back verification remain M2.4. DuckDB analytics remains M2.5. Retained publisher-data reconciliation remains M2.6.
