# Gold data contracts

Status: **Accepted**, version 1; documentation only for M2.1. [ADR 0004](../adr/0004-gold-analytics-publication.md) was explicitly accepted by the owner on October 15, 2026. The [processing design](gold-processing.md) defines the validated input set and publication boundary. These contracts inherit [Silver v1](silver-processing.md#accepted-schema-version-1); they do not change accepted ADRs 0001–0003.

## Shared rules

Metrics cover accepted Silver observations in one explicitly selected, completed input set, after collapsing identical replay keys. Rejected records contribute to no Gold metric; their publication counts remain visible in the input manifest. Duplicate article occurrences at different entry ordinals and observations from different retrievals remain distinct. No cross-publisher deduplication, URL canonicalization, or identifier rewriting is permitted.

Types below are Spark logical types; `long` means signed 64-bit integer. Timestamps represent UTC instants at Silver's microsecond precision; Spark sessions and DuckDB sessions use UTC. Parquet readers can expose nullable physical fields, so requiredness is validated on values. Unknown versions, missing/extra fields, incompatible types, null required values, null warning arrays/elements, invalid identifiers or count overflow fail validation, rather than being coerced or silently excluded. Nullable values are preserved without imputation. Version 1 accepts only Silver v1 and reviewed Silver processor revision 1. Future schema or metric changes require a reviewed contract version; implementation-only changes require a new processor revision and fresh publication namespace.

## gold_articles v1

Grain and primary key: **one row per `article_id`** within the selected input set.

| Field | Type | Null? | Meaning |
| --- | --- | --- | --- |
| `contract_version` | integer | No | Gold article contract version, exactly `1` |
| `article_id` | string | No | Unchanged Silver source-scoped SHA-256 identity |
| `source_id` | string | No | Stable configured source ID |
| `identifier_kind` | string | No | Silver identity kind |
| `source_article_id` | string | No | Silver publisher identifier or fallback link |
| `source_uri` | string | No | Configured feed URI from selected observation |
| `feed_format` | string | No | `rss` or `atom`, from selected observation |
| `title` | string | No | Selected observation's nonblank title |
| `url` | string | Yes | Selected article link, untrusted |
| `summary` | string | Yes | Selected summary, untrusted and unsanitized |
| `published_at` | timestamp | Yes | Selected publication instant; no fallback |
| `updated_at` | timestamp | Yes | Selected update instant; no fallback |
| `quality_warnings` | `array<string>` | No | Selected observation's ordered warning codes; empty array allowed |
| `first_observed_at` | timestamp | No | Minimum `ingested_at` over all observations of this article |
| `last_observed_at` | timestamp | No | Maximum `ingested_at` over all observations of this article |
| `observation_count` | long | No | Number of distinct replay-key observations for this article; positive |
| `distinct_ingestion_count` | long | No | Count of distinct `ingestion_id` for this article; positive |
| `selected_silver_contract_version` | integer | No | Selected input contract version, exactly `1` |
| `selected_ingestion_id` | string | No | Selected original canonical UUID |
| `selected_ingested_at` | timestamp | No | Selected ingestion instant |
| `selected_entry_index` | integer | No | Selected zero-based nonnegative ordinal |
| `selected_input_uri` | string | No | Selected original Silver input URI |
| `selected_payload_sha256` | string | No | Selected original payload digest |
| `selected_payload_byte_length` | long | No | Selected original payload length |
| `selected_bronze_bucket` | string | No | Selected Bronze bucket |
| `selected_bronze_payload_key` | string | No | Selected Bronze payload key |
| `selected_bronze_metadata_key` | string | No | Selected Bronze metadata key |

This Gold input boundary requires completed Bronze-backed Silver publications, so Bronze references are required here even though Silver's fixture contract permits nulls. The selected replay key is `(source_id, selected_ingestion_id, selected_entry_index, selected_silver_contract_version)`; its publication and file provenance are recoverable from the Gold input manifest.

Select the entire observation using `ingested_at DESC, ingestion_id DESC, entry_index DESC` within each article. Compare canonical UUID strings lexicographically with binary ordering and ordinals numerically. UUID order is a deterministic tie-breaker, not retrieval chronology. Never select columns independently, prefer non-null older metadata, or order by publication/update timestamps. `selected_ingested_at = last_observed_at`; optional fields can become null when a newer observation lacks them.

An exact ordering tie within an article implies the same replay key. Identical full Silver rows collapse before selection; any differing field, including null versus non-null or warning-array order, is an input conflict and aborts the whole publication. Validate that each article ID maps to exactly one `(source_id, identifier_kind, source_article_id)` and recomputes to the Silver hash. A conflicting identity fails even if selection would otherwise hide it. Counts describe observed occurrences, not unique stories across publishers or a publisher's total inventory.

## gold_source_daily v1

Grain and primary key: **(`source_id`, `ingestion_date`)**. Derive `ingestion_date` as `DATE(ingested_at)` with the Spark session explicitly set to UTC, never from publication time, the machine timezone, or an unchecked path partition.

| Field | Type | Null? | Meaning |
| --- | --- | --- | --- |
| `contract_version` | integer | No | Gold daily contract version, exactly `1` |
| `source_id` | string | No | Stable configured source ID |
| `ingestion_date` | date | No | UTC calendar date of observation ingestion |
| `observation_count` | long | No | Count of accepted replay-key observations in this source/day; positive |
| `distinct_article_count` | long | No | Count of distinct `article_id` in this source/day; positive |
| `distinct_ingestion_count` | long | No | Count of distinct `ingestion_id` in this source/day with at least one accepted observation; positive |
| `quality_warning_count` | long | No | Sum of `size(quality_warnings)` across these observations; nonnegative |
| `latest_ingested_at` | timestamp | No | Maximum `ingested_at` in this source/day |

An observation with both `INVALID_PUBLISHED_AT` and `INVALID_UPDATED_AT` contributes **2**, not 1, warnings. Empty arrays contribute 0. Silver v1 allows each of these codes at most once, in publication-then-update order; unexpected codes, duplicates, null arrays or null elements fail validation. Optional null article metadata does not remove observations from any metric. Invalid required source/time/identity values fail input validation rather than creating null groups. Rejections and ingestions with zero accepted observations contribute no rows, even if their completion markers are selected. No synthetic zero-count calendar rows are emitted.

Distinct counts are local to their group and are not generally additive across days or sources. Warning totals use all accepted observations, not just the selected `gold_articles` observations. Daily `observation_count` and `quality_warning_count` sum to the corresponding accepted-input totals; article `observation_count` also sums to the accepted-input total. Article row count equals global distinct article count. All distinct counts are bounded by their group's observation count.

## Scope, lateness and reconciliation

These are full recomputations over a fixed selected set, not incremental upserts. A late-arriving completed ingestion included in a later selection can add an older daily group and lower an article's first-observed time; latest metadata still follows ingestion ordering. There is no watermark, event-time exclusion, or implicit wall-clock cutoff. Removing inputs also creates a new snapshot and can reduce counts. Consumers must pin a publication and must not sum overlapping snapshots.

The retained M1 baseline is six snapshots, 118 accepted observations, 59 article identities, zero rejections and zero warnings. If exactly those unchanged inputs are selected, expected Gold article rows are 59 and summed observation counts are 118; warning totals are zero. These are dataset-specific reconciliation expectations, **not invariants or evidence of an executed Gold job**. Daily row counts require the actual UTC date distribution and must not be guessed from the snapshot count.
