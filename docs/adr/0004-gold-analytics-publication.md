# 0004: Local Gold analytics contracts and immutable publication

- Date: 2026-10-15
- Status: Accepted
- Acceptance date: 2026-10-15
- Implementation: M2.1 design; M2.2 completed-Silver discovery and validation; M2.3 Spark Gold transformations implemented. Gold publication and DuckDB integration remain unimplemented.

## Context

M1 completed local Bronze-to-Silver processing while preserving repeated article observations. Analytics needs explicit article selection, source/day metrics and a reproducible publication boundary. The retained six-ingestion baseline is evidence about that dataset, not a fixed processing invariant. Existing accepted ADRs [0001](0001-rss-raw-ingestion-contract.md), [0002](0002-bronze-object-storage.md) and [0003](0003-silver-observation-contract.md) remain unchanged.

## Decision

Adopt the version-1 [Gold data contracts](../architecture/gold-contracts.md) and [processing/publication design](../architecture/gold-processing.md). Publish `gold_articles` at article-ID grain and `gold_source_daily` at source/UTC-ingestion-date grain. Preserve source-scoped identity; select entire latest observations by `ingested_at DESC, ingestion_id DESC, entry_index DESC`. Count warning codes, not merely warning-bearing observations. Collapse identical replay keys and fail conflicting inputs before aggregation.

Consume only validated completed M1 Silver publications from one explicitly selected local root. Use Spark for deterministic full-set transformations and local Parquet publication; use DuckDB subsequently for read-only SQL over a pinned completed snapshot. Fingerprint canonical input inventories including actual Parquet bytes. Publish both products in an immutable version/revision/fingerprint directory, verify read-back and write completion metadata last. Retry verifies existing output before skipping. Changed inputs yield a separate snapshot. Incomplete outputs require explicit operator recovery.

The repository owner explicitly accepted this ADR on October 15, 2026. M2.1 defined the design and acceptance criteria; implementation status is tracked separately. No infrastructure or cloud provisioning is authorized.

## Alternatives and tradeoffs

| Alternative | Evaluation |
| --- | --- |
| DuckDB-only transformations | Simpler for the retained dataset, but would replace Spark's established processing responsibility; keep DuckDB as a consumer to demonstrate the intended boundary |
| Query Silver directly for every consumer | Avoids materialization but duplicates latest-selection/metric logic and leaves a stable product contract undefined |
| Mutable overwrite or append/upsert Gold | Reduces retained snapshots or recomputation but adds recovery and duplicate-management complexity; immutable full snapshots are easier to inspect locally |
| Marker-only input fingerprint | Cheaper, but M1 markers do not protect all Silver content; byte inventories detect changes relative to a prior fingerprint |
| Logical-row fingerprint | Could survive physically rewritten equivalent Parquet, but requires another canonical row serialization protocol; physical inventory is simpler and deliberately conservative |
| Latest by publication/update time | Describes publisher event timing, not latest observed metadata; optional/invalid dates make this unsuitable for the requested observation semantics |
| Cross-publisher deduplication | Requires unapproved canonicalization and story-identity policies; retain accepted source-scoped identities |
| Iceberg, transactional catalog or distributed locking | Broader concurrency/table-management capability with new operational scope; unnecessary for the bounded single-writer local milestone |

These are correctness and complexity tradeoffs, not measured performance rankings.

## Consequences and known limitations

Consumers receive explicit grains, metrics, provenance and snapshot identity. Gold preserves observation history through counts and retained Silver inputs, while exposing one selected article row. Rejected-only ingestions remain visible in publication metadata but do not create daily rows. Late or removed inputs cause full recomputation and can change historical metrics in a new snapshot.

Full input hashing, transformation and output verification cost I/O; retained immutable snapshots cost disk. Physically different but logically equivalent inputs create different fingerprints. Completed Silver must remain immutable; discovery is not an atomic transaction with upstream writers. M1 markers do not enable renewed Bronze integrity verification or establish original Silver content authenticity. No distributed scalability, multiwriter safety, automatic retention/recovery, fsync durability or production-grade exactly-once guarantee is claimed. This is not a transactional lakehouse.

## Validation and follow-up

M2.1 acceptance is documentation consistency: complete typed schemas, unambiguous metric/null/tie semantics, publication/failure rules, roadmap reconciliation, working local links and repository hygiene. The [implementation acceptance criteria](../architecture/gold-processing.md#implementation-acceptance-criteria) define required synthetic cases before claiming working Gold behavior. Owner acceptance is recorded above; implement the Silver selection/validation boundary and pure Spark transformations, followed by immutable publication and DuckDB reconciliation. Pin and validate any later DuckDB dependency in that implementation slice; no runtime version changes are made here.
