# Gold processing and publication design

Status: **Accepted** by the owner on October 15, 2026. M2.2 implements the [completed-Silver input boundary](silver-input-validation.md), and M2.3 implements the [Spark transformations](gold-transformations.md). M2.4 implements [immutable publication and replay](gold-publication.md); [M2.5 implements standalone DuckDB analytics](duckdb-analytics.md). See [contracts](gold-contracts.md), [ADR 0004](../adr/0004-gold-analytics-publication.md), and the [canonical roadmap](../charter.md#milestones-m0m9).

## Boundaries

```text
Completed Silver Parquet (accepted/ + rejected/ + complete.json)
  -> validate and freeze selected input inventory
  -> Spark transformations over accepted observations
  -> Gold Parquet: gold_articles + gold_source_daily
  -> read-back verification -> Gold complete.json
  -> DuckDB SQL analytics pinned to that completed publication
```

Silver owns normalization, source-scoped identities, warnings, rejections and Bronze provenance. Reuse the [M1 publication contract](bronze-to-silver.md#provenance-and-output-layout) and existing schemas without changing their producers. Gold validates Silver, selects article observations and calculates aggregates with Spark 4.0.2; it neither retrieves publishers nor re-parses Bronze. DuckDB is the local, read-only SQL consumer of Gold Parquet. M2.5 pins DuckDB 1.4.5 and provides a [standalone reader and named reports](duckdb-analytics.md); Spark retains the transformation responsibility. No new service, cloud access, Iceberg or distributed lock is needed.

## Discovery and selection

Use one explicitly configured local Silver root per run (default proposal: `outputs/bronze-to-silver`). Discover only the existing `v1/<bucket-digest>/source_id=<encoded-source>/ingestion_date=<UTC-date>/ingestion_id=<UUID>/complete.json` layout. Fixture `articles/` outputs without these markers are ineligible. No recursive glob of arbitrary Parquet or automatic union of output roots is allowed.

Support either exact marker paths or exact source-ID filters, mutually exclusive; absent filters selects all completed publications in that root. Resolve paths under the root, reject path traversal and symlink escapes, deduplicate repeated path selections, then freeze a sorted root-relative POSIX marker-path list using UTF-8 byte order. Source selection decodes the existing unpadded Base64URL path and validates it against rows. Explicit missing/incomplete selections fail; discovery reports and excludes directories without markers. Malformed markers in the selected scope fail rather than disappearing from counts. A requested source with no completed publication fails. New markers after discovery wait for a later run. There is no claim of an atomic snapshot of concurrent Silver writes; completed inputs must remain immutable throughout a run.

## Input validation and conflicts

1. Parse markers strictly, rejecting duplicate JSON keys, malformed types and unknown versions/revisions. M1 markers contain `identity` (`metadata_sha256`, `payload_sha256`, `contract_version`, `processor_revision`, `bucket`, `metadata_key`) and `counts` (`accepted`, `rejected`); they do not contain a separate marker-version field. Require contract/revision 1 and nonnegative integer counts, excluding booleans. Validate digest syntax and correspondence of bucket hash, metadata key and publication path to the M1 namespace.
2. Inventory both accepted and rejected Parquet files deterministically. Require both datasets even when empty. Inspect physical schemas before explicit-schema reads: exact field names/types from `SILVER_SCHEMA`/`REJECTION_SCHEMA`, allowing Parquet's nullable representation. Do not let an explicit reader silently fill missing columns with null. Disable schema merging and fail unreadable/missing files or count mismatches. Hash exact marker and Parquet bytes, including rejected Parquet; ignore Spark housekeeping files, never treat `_SUCCESS` as a completion marker.
3. Validate accepted values against Silver v1, including article hashes, requiredness, warning codes, UUIDs and ordinals. Validate provenance on both datasets against marker/path identity: contract, source, ingestion, bucket, payload/metadata keys, input URI and payload digest. Require source URI, ingestion instant, byte length and feed format to be consistent across all rows of an ingestion, and the UTC date to match its path. Check rejection schema/reasons and counts without turning rejected candidates into Gold observations. Empty datasets have no row provenance to validate; retain that limitation in diagnostics. M1 markers lack full original metadata, so this is internal consistency validation, not renewed Bronze checksum verification.
4. For repeated logical publication identity `(bucket, metadata_key)`, require identical marker identity and full accepted/rejected row multisets; collapse identical copies and report them. Conflicts fail. For `(source_id, ingestion_id)` require immutable envelope/payload provenance across inputs, including bucket/keys. For replay key `(source_id, ingestion_id, entry_index, contract_version)`, identical full accepted rows collapse, differing rows fail. An entry present as both accepted and rejected is a conflict. Preserve separate ordinals and retrieval IDs. Do not repair malformed Silver silently.
5. Apply the contract's article-identity consistency checks before aggregating. Reject the entire selected set on any conflict, schema/provenance error or unreadable selected input; do not publish a partial subset. Recheck inventory and byte digests before completion to detect observed input changes. Concurrent mutation remains unsupported, including changes between checks.

When duplicate publication copies exist, choose the lexicographically smallest marker path as the representative, record all copies in the manifest, and count accepted observations only once. Inventory and path order are deterministic; Spark partition order, Parquet filenames, physical row order and byte-for-byte output determinism are not promised. SQL consumers must specify `ORDER BY`.

## Versioned local publication

Implemented local publication layout, by default under ignored `outputs/`:

```text
outputs/gold/v1/processor_revision=1/input_set=<sha256>/
  inputs.json
  gold_articles/       # explicit-schema Parquet, including schema-bearing empty output
  gold_source_daily/   # explicit-schema Parquet, including schema-bearing empty output
  complete.json        # published last after both datasets pass verification
```

`inputs.json` is a deterministic manifest with `manifest_version=1`, Gold contract versions, Gold processor revision, and the sorted complete input inventory. Each input descriptor contains root-relative marker path, exact marker SHA-256, parsed M1 identity/counts, and accepted/rejected file lists of relative path, byte length and SHA-256 sorted by UTF-8 path bytes. Include all selected copies, with their chosen representative paths; also record supported Silver contract/revision. Exclude absolute roots, wall-clock execution time and CLI filter spelling from fingerprint identity. Equivalent selected inventories have the same identity regardless of how selected or where the root is mounted.

Compute `input_set` as lowercase SHA-256 of canonical UTF-8 JSON manifest bytes: recursively sorted object keys, compact separators, non-ASCII characters encoded directly as UTF-8, no NaN, no trailing newline; lists use the ordering above, integer values have no fractional form. Paths are POSIX relative paths and no Unicode normalization is applied. This exact byte protocol must have fixed-vector tests. This is a **physical input inventory fingerprint**, not a logical-row hash: rewriting equivalent Parquet, changing marker bytes, or adding/removing duplicate copies changes the fingerprint. It is deliberately stronger than trusting M1 marker counts alone, at the cost of reading all input bytes. It detects changes relative to a recorded inventory, not malicious tampering or original Silver corruption before inventory creation.

Require one writer per Gold output root, immutable selected inputs, and no concurrent manual output edits. Create the fingerprint directory exclusively; write the manifest and both datasets with overwrite disabled. Read back both datasets with schema inspection and explicit schemas. Verify full row multisets against computed frames with null-safe equality in both directions, primary-key uniqueness, required values, selected provenance and cross-table/input count and warning reconciliation. Counts alone are insufficient. Empty outputs must retain explicit schemas.

After verification, inventory output Parquet paths, lengths and SHA-256 digests. Write a temporary marker exclusively and rename it to `complete.json` in the same directory. Marker v1 records `publication_version=1`, both Gold contract versions, processor revision, input fingerprint, manifest SHA-256, accepted/rejected input totals after publication-copy collapse, accepted observation total after replay collapse, duplicate counts, both output row counts and output file inventory/digests. Marker fields are strictly validated. Readers require this marker and verify its manifest/inventory before registering the two tables from that exact directory. No implicit `latest` pointer or glob across fingerprints is provided.

## Re-execution, failure and empty inputs

| Condition | Required behavior |
| --- | --- |
| No completed inputs selected | Nonzero exit, `NO_COMPLETED_INPUTS`; publish nothing |
| Valid selected publications, zero accepted observations | Publish two schema-bearing empty tables, counts zero, preserve rejected counts; no fabricated daily rows |
| Same fingerprint, valid completed output | Verify manifest identity, file inventory/digests, schema and read-back reconciliation; report `SKIPPED`, write nothing |
| Same path, missing completion marker | `INCOMPLETE_OUTPUT`; never overwrite or automatically resume |
| Existing malformed/conflicting marker or changed output | Explicit failure; never overwrite or label it skipped |
| Different selected inventory | New fingerprint directory; full recomputation, old snapshot retained |
| Processor or contract change | Reviewed new revision/version namespace; no reuse of old markers |
| Any input/output verification failure | Nonzero exit; no completion marker for the attempted snapshot |

Diagnostics should report fixed error codes, escaped relative input/publication paths, expected/actual counts and duplicate counts. Do not print source content, credentials, raw exception payloads or article URLs. Distinguish selection, schema, provenance, input-conflict, input-changed, write and read-back failures; provide the affected path and remediation (restore immutable inputs or inspect/quarantine incomplete output). Manual recovery requires stopping writers and inspecting/quarantining the incomplete directory outside discovery, or selecting a fresh output root. Never delete Bronze or overwrite completed Gold to recover. Rejected inputs remain explicit evidence, not job failures merely because their rejection count is nonzero.

This is a local single-writer convention, **not a transactional lakehouse or production-grade exactly-once implementation**. There is no multiwriter isolation, distributed lock, fsync/power-loss guarantee, atomic cross-run catalog update, automatic retention or garbage collection. Full recomputation, byte hashing and read-back increase I/O and disk use; no performance claims are made. Hash collisions, external edits and trusted local manifest assumptions remain limitations. A consumer pins one snapshot for a query session; switching snapshots is explicit. Silver's original content authenticity is not established by Gold validation.

## Implementation acceptance criteria

Following owner acceptance of ADR 0004 on October 15, 2026, implement and test selection, validation, deterministic transformations and publication in small slices. Synthetic tests must cover ties, repeated ordinals versus replays, conflicting rows/provenance, UTC midnight and offset timestamps, null optional metadata, two warnings on one observation, rejected-only publications, no inputs, late arrivals, duplicate selection/copies, changed input, incomplete output, tampered Parquet and safe repeat execution. Verify Spark read-back and later DuckDB SQL against known expected rows and metrics. Reconcile the retained M1 dataset separately without treating its counts as universal invariants. M2.1 itself requires only documentation lint, link and hygiene checks, schema/metric review and roadmap/ADR consistency review.
