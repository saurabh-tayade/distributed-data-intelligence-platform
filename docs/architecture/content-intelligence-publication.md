# Content Intelligence publication: E1b contract

Status: **Accepted by the owner on October 20, 2026**, with the synchronous-consumer amendment in [ADR 0007](../adr/0007-content-intelligence-publication.md), for [issue #42](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/issues/42). Stage B is authorized. Implementation and actual validation evidence are recorded separately below.

## Scope and inspected reuse boundaries

E1b derives two local Parquet datasets from one explicitly selected completed [Gold v1 publication](gold-publication.md) and one [E1a snapshot](publisher-reference-snapshots.md). Preserve all accepted schemas, fingerprints, article identities, winning observations, warning codes and null semantics. Never contact publishers, reread Silver to reinterpret history, merge sources, infer article categories or substitute discovery time for publication time. Existing APIs, consumers, product specifications and M6 status remain unchanged.

| Existing interface | Safe reuse and limit |
| --- | --- |
| `gold_analytics.open_publication(gold_root, fingerprint)` | Required full accepted Gold consumer verification: both datasets, every physical schema, required values, keys, counts and cross-table reconciliation; close its context without fetching article reports |
| `gold_reader.verify_metadata(gold_root, fingerprint)` | Metadata, exact inventory and byte checks; obtain `VerifiedMetadata.paths(table)`, marker and manifest bytes; insufficient alone for initial Gold verification |
| `VerifiedMetadata.verify_unchanged()` | Rehash/revalidate and compare metadata around actions; supplement with E1b's identity/metadata/directory guards |
| `publisher_reference.open_snapshot(root, reference_id, revision, expected_fingerprint)` | Sole reference-row intake; detached immutable rows and manifest, bounded by E1a; no raw CSV/Spark CSV bypass |
| `gold.GOLD_ARTICLES_SCHEMA`, `gold.build_gold(inputs)` | Use the exact schema in the Spark-only layer; do not invoke `build_gold`, which reselects from Silver |
| `gold_publication.publish`, `verify_frames`, `output_inventory` | Gold/Silver-specific, not generic E1b helpers; follow their patterns, with new dataset-specific verification and stricter known bookkeeping inventory |
| `publication_metadata.canonical_bytes` | Reuse unchanged for new canonical JSON and SHA-256 input |

Full Gold verification here means the existing standalone consumer boundary, not proof recomputed from Silver. That boundary cannot independently reconstruct historical winner membership, exact per-day distinct counts or historical warning totals. E1b trusts Gold's completed production evidence for those claims and preserves it; its own enrichment and discovery transformation is fully compared against the verified Gold catalog. No bounded Python list of all articles is permitted.

Implemented module responsibilities are `content_dependencies.py` (bounded capture, selection, immutable descriptors and rechecks), `content_metadata.py` (Spark-free schema constants and strict metadata), `content_processing.py` (Spark frames/comparisons), `content_publication.py` (writer/CLI) and `content_analytics.py` (DuckDB verification/reports/CLI). These five modules implement the scoped boundary. Dependencies flow toward the shared metadata/dependency modules; neither imports the Content writer or Spark. The reader must not import `gold`, `gold_publication`, `silver`, `silver_inputs`, PySpark or Boto3.

The existing Gold opener sets two threads, no spill and no extension autoload but has no explicit memory-budget argument. The accepted additive keyword-only argument is `memory_limit=None`: absent preserves existing behavior; a validated non-null budget configures the DuckDB connection before any reads. E1b passes `512MiB`. Do not expose arbitrary DuckDB configuration, change SQL checks, or access private Gold connection attributes. This owner-approved compatibility change preserves existing default behavior and is covered by backward-compatibility and explicit low-memory tests.

## Dependency selection, capture and retained history

Publication inputs are all mandatory: `gold_root`, lowercase 64-hex `gold_fingerprint`, `reference_root`, E1a-valid `reference_id`, `revision`, lowercase 64-hex `reference_fingerprint`, and existing dedicated `output_root`. Only exact fingerprints are accepted, not latest, arbitrary publication paths or discovery filters. Roots are local operator configuration and do not enter product identity.

The implemented `verify_dependencies(...)` returns an internal frozen descriptor containing Gold selection, exact raw manifest/completion bytes, ordered tuples of approved file records/paths for both Gold datasets, detached E1a rows/manifest, and filesystem guards. A descriptor is not a promise of future immutability. Do not let callers supply a fabricated descriptor to bypass public verification. The public writer accepts selections and constructs the descriptor itself.

1. Validate arguments, root separation and bounded admission. Require real directories/regular files; reject traversal, symlinks at/below selected roots, hard-linked artifacts, special files and glob characters in selected file paths. Roots must not overlap each other or the Content root after trusted ancestor resolution. Ancestors above roots are trusted configuration.
2. Capture bounded Gold `inputs.json` and `complete.json`, directory identities, file stamps and approved inventories. Call `verify_metadata` for exact v1/revision-1 layout and file hashes. Deep-freeze copies; the returned object's nested dictionaries are not immutable.
3. Call `gold_analytics.open_publication` and close it after successful full verification. Do not call `report('articles')`, use `fetchall()` for article validation, or use a private connection. Recapture metadata and guards; require equality with step 2, including byte-identical completion/input manifests. Only now treat the frozen inventories as approved.
4. Guard E1a's exact four-file snapshot before opening, call `open_snapshot` with the explicit tuple, and recapture/recheck that evidence afterward. Use only its returned rows. Recheck the original four file stamps/bytes around later operations and reopen E1a; compare fingerprint, manifest and complete detached row tuple. This detects same-byte replacement and marker changes during a session as well as content changes.
5. Spark reads exactly the approved `gold_articles` absolute file list. Check each physical file against the Gold schema first; set UTC, disable schema merging and missing/corrupt-file skipping, and pass literal paths with glob characters prohibited. Do not read a directory, recurse, infer hive partitions, union schemas or cast incompatible input types into compliance. Gold daily files are also verified dependencies, although they are not transformation inputs.
6. Construct an explicit five-column Spark reference frame from the bounded E1a row tuple, with nullable optional attributes and required source/name. Recheck uniqueness and schema; header-only references use the same explicit schema. A bounded broadcast is allowed but not an identity-bearing semantic option.
7. After materializing/counting expected frames, after output read-back actions and immediately before completion rename, recheck both dependencies: metadata bytes, all approved files' streaming hashes/lengths/stamps, parent/root identities and directory inventory, followed by E1a independent opening. Spark laziness/caching never removes these checks. On replay and independent consumer opening, repeat full dependency verification before trusting previously recorded comparisons.

File stamps include device, inode, size, nanosecond mtime, mode and link count; directory guards include device/inode/type and expected membership. Access times are not promised unchanged. Once an artifact was observed, disappearance/replacement/changed bytes or metadata is `DEPENDENCY_CHANGED`; an ordinary read/close/stat failure without evidence of change is `DEPENDENCY_UNAVAILABLE` or `DEPENDENCY_INVALID`, never a fabricated mutation. Partial knowledge is not successful verification. Preserve known mutation evidence if a later I/O operation fails too.

Copy the exact selected Gold `inputs.json` and `complete.json` as `gold-inputs.json` and `gold-complete.json`. The former retains the full ordered Silver publication inventory, representative/copy relationships, marker/file hashes, counts and Bronze identity fields already recorded by Gold. Do not summarize away rejected-only inputs or silently add discovered files. The Gold article's selected ingestion/provenance columns complement that manifest; the manifest does not itself contain every historical ingestion timestamp/UUID. Do not invent missing provenance fields or claim it reconstructs unretained observations.

The selected history is precisely Gold's retained input set. No rolling window, re-fetch, re-selection, reinterpretation of rejected records or automatic retention is introduced. An earlier historical observation matters only when explicitly included in a new Gold publication. E1b reads/replay require the selected Gold Parquet, Gold metadata and E1a snapshot to remain available; they require neither Silver files nor original operator CSV/vocabulary paths. Copied evidence enables provenance inspection, not degraded dependency-free query success. Relocating identical dependencies to new roots is allowed; deleting a required dependency makes reads unavailable.

Rechecks are not leases or transactions. Mutations reverted between checks and mutations after the last check remain outside the trusted single-writer/immutable-dependency assumption.

## Exact dataset schemas and semantics

Types are Spark types: `integer` signed 32-bit, `long` signed 64-bit, `timestamp` UTC microsecond `TimestampType`, `date` UTC-derived `DateType`, and UTF-8 `string`. DuckDB mappings are `INTEGER`, `BIGINT`, `TIMESTAMP`, `DATE`, `VARCHAR` and `VARCHAR[]`. Timestamp-NTZ, nanosecond timestamps, missing/extra/reordered columns and implicit casts are not accepted. Parquet nullable flags are not requiredness evidence; inspect every physical schema and validate values. All timestamps must be finite and representable as years 0001–9999 at microsecond precision for this consumer; unsupported values fail, never round or substitute.

### `content_catalog_v1`

Grain/key: one row per unchanged Gold `article_id`. Physical column order is exactly the following table. Columns 1–27 are copied verbatim, including `contract_version` meaning Gold v1; column 28 identifies the Content contract separately. The detailed [Gold field definitions](gold-contracts.md#gold_articles-v1) remain authoritative for those preserved fields.

| Position | Column | Spark type | Required | Definition |
| --- | --- | --- | --- | --- |
| 1 | `contract_version` | integer | Yes | Preserved Gold value, exactly 1 |
| 2 | `article_id` | string | Yes | Unchanged source-scoped Gold identity |
| 3 | `source_id` | string | Yes | Exact source join key |
| 4 | `identifier_kind` | string | Yes | Preserved identity kind |
| 5 | `source_article_id` | string | Yes | Preserved native identifier/fallback |
| 6 | `source_uri` | string | Yes | Preserved configured feed URI |
| 7 | `feed_format` | string | Yes | Preserved `rss`/`atom` |
| 8 | `title` | string | Yes | Preserved selected title |
| 9 | `url` | string | No | Preserved nullable, untrusted link |
| 10 | `summary` | string | No | Preserved nullable, untrusted text |
| 11 | `published_at` | timestamp | No | Preserved nullable publisher instant |
| 12 | `updated_at` | timestamp | No | Preserved nullable publisher instant |
| 13 | `quality_warnings` | array of string | Yes | Preserved ordered Gold codes; no null elements |
| 14 | `first_observed_at` | timestamp | Yes | Preserved Gold minimum observation instant |
| 15 | `last_observed_at` | timestamp | Yes | Preserved Gold maximum observation instant |
| 16 | `observation_count` | long | Yes | Preserved positive observation count |
| 17 | `distinct_ingestion_count` | long | Yes | Preserved positive count, bounded by observations |
| 18 | `selected_silver_contract_version` | integer | Yes | Preserved value 1 |
| 19 | `selected_ingestion_id` | string | Yes | Preserved UUID |
| 20 | `selected_ingested_at` | timestamp | Yes | Preserved winning observation instant |
| 21 | `selected_entry_index` | integer | Yes | Preserved ordinal |
| 22 | `selected_input_uri` | string | Yes | Internal preserved provenance |
| 23 | `selected_payload_sha256` | string | Yes | Internal preserved provenance |
| 24 | `selected_payload_byte_length` | long | Yes | Internal preserved provenance |
| 25 | `selected_bronze_bucket` | string | Yes | Internal preserved provenance |
| 26 | `selected_bronze_payload_key` | string | Yes | Internal preserved provenance |
| 27 | `selected_bronze_metadata_key` | string | Yes | Internal preserved provenance |
| 28 | `content_contract_version` | integer | Yes | Exactly 1 |
| 29 | `discovered_at` | timestamp | Yes | Exactly `first_observed_at` |
| 30 | `discovery_date` | date | Yes | UTC date of `discovered_at` |
| 31 | `publisher_name` | string | No | Exact E1a name for matched row; null otherwise |
| 32 | `publisher_category` | string | No | Exact optional E1a publisher context, not article topic |
| 33 | `declared_language` | string | No | Exact optional declared source language |
| 34 | `country_code` | string | No | Exact optional curated source country |
| 35 | `reference_status` | string | Yes | Exactly `MATCHED` or `MISSING` |
| 36 | `reference_id` | string | Yes | Selected reference ID, including unmatched articles |
| 37 | `reference_revision` | string | Yes | Selected revision, including unmatched articles |
| 38 | `reference_fingerprint` | string | Yes | Selected snapshot fingerprint for every row |

Join exact `source_id` equality without trimming/case folding. Determine `MATCHED` from presence of the right-side source key, not a nullable attribute. A matched name is required by E1a; its three optional attributes may all be null. `MISSING` requires all four attributes null. The selected reference tuple is constant over all catalog rows. Empty catalog provenance resides in the manifest.

The left join must preserve the complete article-ID set, row count and every base value, with null-safe equality, ordered arrays and no multiplication. Unused reference rows do not create articles. No winner selection, identity calculation, warning recomputation or cross-source deduplication occurs in the transformation.

`discovered_at` is first discovery **within the selected retained history**, not first publication or first appearance on the internet. An article observed on several UTC days contributes once on its earliest retained day, whereas Gold ingestion-day metrics count its observations on those days. `published_at` can precede or follow discovery or remain null. `completed_at` is operational publication time only.

### `content_source_discovery_daily_v1`

| Order | Column | Spark type | Required | Definition |
| --- | --- | --- | --- | --- |
| 1 | `contract_version` | integer | Yes | Content discovery contract, exactly 1 |
| 2 | `source_id` | string | Yes | Unchanged catalog source |
| 3 | `discovery_date` | date | Yes | UTC catalog discovery date |
| 4 | `discovered_article_count` | long | Yes | Number of catalog articles in this source/date; positive |
| 5 | `articles_missing_reference_count` | long | Yes | Number with `reference_status = MISSING`; nonnegative |

Unique key is `(source_id, discovery_date)`. No zero-count groups. Each catalog article belongs to exactly one group. Per group, missing count is at most discovered count. Global sums equal catalog row count and missing-reference article count respectively. Sum in Spark `decimal(38,0)` / DuckDB `HUGEINT`, check 0–9,223,372,036,854,775,807 before casting to long; overflow aborts. Dataset row order/file partition order is not semantic. Reports order by binary source ascending then date ascending.

### Reference coverage

Coverage is a deterministic query, not a third Parquet dataset or redundant completion counters. Compute it from the fully verified catalog and selected E1a rows, including unused keys. Verify aggregate coverage during publication/read-back; independently recompute for consumers.

The source domain is the union of catalog source keys and reference source keys. A matched source has a reference row; a missing source appears only in the catalog. An unused reference source has zero catalog articles. “Matched sources missing optional attributes” counts distinct **used matched** sources with any null category/language/country, not articles or unused sources. Header-only reference: every article/source is missing, no unused references. Empty catalog: zero matched/missing articles and used sources; every reference key is unused. Both empty: all totals zero.

Coverage does not add Gold warnings, count rejected Silver observations as articles, or make a claim about reference correctness. Rejected counts/history remain in copied Gold metadata.

## Product identity and exact metadata

All new JSON uses unchanged `canonical_bytes`: UTF-8, keys sorted lexicographically at every object level, compact comma/colon separators, `ensure_ascii=False`, `allow_nan=False`, no BOM or trailing newline. Arrays retain specified order. Reject duplicate/extra/missing keys, floats, bool-as-integer, noncanonical encoding, unsupported versions, invalid Unicode and out-of-range integers. No optional keys except explicitly nullable fields. Digests are lowercase SHA-256 hex. Counts/lengths are integer 0–2^63−1, with positive lengths for these metadata/Parquet files.

The exact `inputs.json` schema is:

| Key | Required value/schema |
| --- | --- |
| `manifest_version` | Integer 1 |
| `product_id` | String `content-intelligence` |
| `product_contract_version` | Integer 1 |
| `processor_revision` | Integer 1 |
| `dataset_contract_versions` | Exactly `{"content_catalog_v1":1,"content_source_discovery_daily_v1":1}` |
| `gold` | Object defined below |
| `reference` | Exactly `reference_id`, `revision`, `snapshot_fingerprint`, with E1a syntax and selected values |
| `semantic_config` | Exactly the four fixed string entries below |

`gold` has exactly `publication_fingerprint`, `input_manifest`, `completion`. `publication_fingerprint` is the explicit Gold fingerprint. Each of the other two objects has exactly `file`, `byte_length`, `sha256`; `file` is respectively `gold-inputs.json` or `gold-complete.json`, never a supplied path. `input_manifest.sha256` must equal the Gold fingerprint and the actual copied input-manifest digest. `completion.sha256` hashes the exact selected Gold completion bytes, which retain both output inventories and counts. Gold's existing acceptance of completion formatting is preserved: copy/hash its raw bytes, do not reserialize them. Its input manifest remains canonical under Gold's contract.

Fixed `semantic_config`:

```json
{"discovery":"first-observed-utc-v1","gold_preservation":"all-fields-v1","reference_application":"snapshot-v1","reference_join":"exact-source-left-v1"}
```

Product fingerprint is exactly `SHA-256(canonical_bytes(inputs_object)).hexdigest()`. Store those identical bytes as `inputs.json`. There is no self-fingerprint inside `inputs.json` and no concatenation ambiguity. Gold and E1a algorithms remain unchanged. The E1a fingerprint binds its canonical manifest, CSV/vocabulary hashes and versions, authoring and validation policy. E1b does not hash E1a creation time into identity; it still rechecks that completion evidence within each operation.

Copying two Gold metadata files retains complete frozen inventory/provenance without nesting it again in the derived JSON or copying Parquet. Dependencies must remain independently retained. Exact Gold completion binding intentionally makes changed file names/physical bytes/completion formatting identity-bearing even when Gold's input-set fingerprint is unchanged. Reference revision/bytes/authoring correction changes E1a identity and therefore E1b identity.

Machine roots, invocation UUIDs, wall-clock start/completion time, logs, Spark partitions/compression and query budgets are excluded. This excludes new operational root configuration, not provenance already bound by the selected Gold publication. Those operational settings cannot change logical outputs. Semantic changes need new contract versions; implementation changes affecting production require a new processor revision. Only revision 1 is supported; unknown revisions fail, never fall back. Actual output bytes are bound by completion evidence, not by a circular output-dependent reservation fingerprint.

### Completion schema and artifact layout

```text
<content-root>/v1/processor_revision=1/publication=<product-fingerprint>/
  inputs.json
  gold-inputs.json
  gold-complete.json
  content_catalog_v1/
    part-....parquet
  content_source_discovery_daily_v1/
    part-....parquet
  complete.json
```

The root must already exist. Create only fixed version/revision parents as needed and reserve the absent fingerprint directory exclusively. Top-level completed membership is exactly the six names shown; no other files/directories. `complete.tmp` is a private pre-completion transient only; public opening rejects it even beside `complete.json`. Both datasets are flat and contain at least one schema-bearing Parquet file, including zero-row outputs.

Each dataset permits listed non-hidden `.parquet` regular files plus only `_SUCCESS`, `._SUCCESS.crc` and `.<listed-parquet-basename>.crc`. No arbitrary hidden file, `_temporary`, nested partition directory, symlink, hard link, wildcard basename or unlisted Parquet is allowed. Bookkeeping is not analytical evidence; require known names/regular types and stable membership/stamps during an operation. Do not incorporate ignored content into rows or mistake `_SUCCESS` for product completion.

`complete.json` has exactly:

| Key | Required value/schema |
| --- | --- |
| `completion_version` | Integer 1 |
| `product_id` | `content-intelligence` |
| `product_fingerprint` | Selected fingerprint, directory basename and SHA-256 of `inputs.json` |
| `manifest_byte_length` | Exact positive `inputs.json` length |
| `output_counts` | Exactly `content_catalog_v1`, `content_source_discovery_daily_v1`: nonnegative integer row counts |
| `files` | Exactly those two dataset names, each a nonempty array of file entries |
| `completed_at` | Gregorian UTC `YYYY-MM-DDTHH:mm:ss.ffffffZ`, years 0001–9999, no leap seconds |

Each file entry is exactly `{"path":relative_path,"byte_length":positive_integer,"sha256":digest}`. Paths have exactly dataset/basename, POSIX separators, no traversal, leading slash, backslash, empty segment, glob or alias. Sort each array by UTF-8 path bytes and reject duplicates. Every hash is over the entire physical file. Gold evidence copies must match both `inputs.json` descriptors and currently verified dependencies byte for byte. Completion fields are validated with independently computed counts/inventories; marker existence is insufficient.

`completed_at` is written once, preserved on replay, excluded from identity and not authenticated. A self-consistent historical timestamp change cannot be detected from the product fingerprint alone; changes during an operation are detectable. There is no latest pointer, registry, revision alias or automatic promotion.

## Complete writer verification and recovery

The public writer is `content_publication.publish(spark, *, output_root, gold_root, gold_fingerprint, reference_root, reference_id, revision, reference_fingerprint)`. Return a small immutable result with `status` (`published` or `replayed`), `product_id`, `product_fingerprint`, and both `output_counts`. No DataFrame or unchecked path is returned as verified consumer data.

1. Validate selections/paths/bounds, freeze and fully verify dependencies, construct canonical identity bytes.
2. Build expected catalog/discovery frames with explicit schemas in UTC. Execute validation/count actions before reservation: source reference uniqueness, unchanged Gold keys/count/base fields, required enrichment fields and coverage/reconciliation. Only scalar aggregates and bounded reference/file metadata reach Python.
3. If the destination exists without valid completion, fail without writes. For completed replay, require exact input manifest/evidence equality and perform all following verification steps against existing artifacts. Return `replayed` only after successful final dependency/output rechecks; preserve every byte and file/directory mtime.
4. For a new destination, reserve exclusively, write all three metadata files with exclusive create and exact-byte checks, then each dataset using Spark `errorifexists`. Check expected frame schemas before writing; never overwrite.
5. Inventory/hash output and inspect **each physical file's** names/order/types before reading exact lists with explicit schemas. Validate versions/requiredness, unique article and discovery keys, counts/overflow, valid reference tuples/statuses/attributes and exact UTC time rules.
6. Project the catalog's first 27 columns and compare the full multiset against verified Gold articles in both directions using null-safe `exceptAll`. Require equal article-ID sets and row counts separately. Compare all 38 catalog fields against the expected exact left join; this proves matched optional nulls, unmatched rows and constant version provenance without multiplication.
7. Independently group the actual catalog, compare every daily key/count against actual daily output, and compare the complete daily row multiset with expected output in both directions. Check global and per-source discovered/missing totals and coverage formulas, not only total counts. All comparisons include multiplicity and nulls. No sampling, checksum-only row validation or count-only acceptance.
8. After all Spark actions, recapture/check output files, copied metadata, inventories and directory guards; recheck dependencies. Build completion from verified values. Exclusively create/close `complete.tmp`, bounded-read it back, require exact canonical bytes/schema/counts/inventory and valid time.
9. Recheck dependencies and all output evidence/guards including temporary marker immediately before finalization. Require absent `complete.json` under the one-writer assumption; rename the same-directory temporary marker last. Return success only after rename succeeds. Do not delete partial files on failure.

Independent DuckDB verification must repeat the same semantic requirements with SQL over exact file lists, including bidirectional `EXCEPT ALL` against Gold plus detached E1a rows and daily groups, without Spark/Silver. Spark expected-frame equality alone is not independent fixture evidence; the acceptance examples below provide a separate oracle.

| Situation | Required behavior |
| --- | --- |
| Identical inputs/configuration, completed destination | Fully verify dependencies/output and replay read-only |
| Changed Gold, reference or supported semantic identity | Different fingerprint/directory; prior publication retained |
| Corrupt dependency or corrupt existing output | Fail; do not overwrite, repair or call it replayed |
| Missing marker, partial Spark write, stale temporary marker | Incomplete and blocked; no resume/automatic promotion |
| Verification or dependency/output mutation failure | Abort before completion/rows; retain evidence for investigation |
| Interruption before rename | Incomplete reservation remains |
| Interruption or lost response after rename | May be complete; explicit open/replay must fully verify; do not delete on the basis of failure reporting |
| Empty/rejected-only valid Gold | Empty schema-bearing catalog/discovery, zero article metrics, reference keys unused, copied rejection provenance retained |
| New older retained observation | Must arrive via a new Gold selection; may lower first discovery in a new product only |
| Unsupported version/revision | Reject explicitly; no schema inference or reinterpretation |

One trusted writer owns the entire Content root; Spark tasks belonging to that writer are not independent publishers. Inputs must not be edited concurrently. No locks, fsync/power-loss guarantee, distributed transactions or rollback is claimed. Manual recovery requires stopping writers and investigating/quarantining incomplete output or selecting a fresh dedicated root; the application does none of that automatically.

## Standalone read-only consumer

`content_analytics.open_publication(content_root, product_fingerprint, *, gold_root, reference_root)` derives the required dependency IDs from the strict manifest and verifies against the supplied local roots. It returns a context-managed session exposing only `report(...)` and `close()`, with immutable product/dependency identity. Callers do not pass new reference selections to an already opened product. All reports/pages pin this same fingerprint; clients must reject mismatched envelopes and explicitly reopen to change publications.

The supported report parameters are `report(name, *, source=None, start_date=None, end_date=None, reference_status=None, publisher_category=None, declared_language=None, country_code=None, limit=None, offset=0)`. `limit=None` selects the report-specific default; all other provided values follow the validation below. Unsupported non-null filters fail rather than being ignored. Return the complete detached bounded envelope, not a lazy iterator or database cursor. Closing or invalidating a session releases its connection; later calls fail with `QUERY_FAILED`.

Opening enforces bounds and exact metadata/layout/inventory, fully verifies Gold and E1a, compares copied evidence, checks every physical Content schema, registers exact approved file lists and the bounded reference rows, and runs the complete SQL value/key/base-field/join/group/coverage comparisons described above. No article rows are materialized in Python for these checks. Use in-memory DuckDB with UTC, two threads, 512 MiB configured memory, no disk spill, extension autoload/install disabled and no external fetching.

Before and after each report, rehash/recheck Content evidence and selected dependencies, including guard identities and reopening E1a. Initial full SQL validation remains applicable only while those exact bytes remain unchanged. Any verification, query or result-admission failure discards the entire pending result and invalidates the session; no partial rows escape. Invalid arguments and a concurrent BUSY rejection do not disturb an active verified request. A later invocation fully verifies afresh. Querying never writes dependency/output files, performs repairs or launches Spark. In-memory DuckDB tables/views for bounded reference rows are permitted; no persistent database is created.

### Reports, projections and filters

Only these exact report names are accepted. Every report is paged, including aggregates; there is no silently truncated unpaged mode. String ordering is binary, with no locale/case folding. Parameters are bound values; no SQL expressions, column names, order clauses or file paths come from filter text.

| Name | Row columns, exact order | Order / default page |
| --- | --- | --- |
| `catalog` | `article_id`, `source_id`, `identifier_kind`, `source_article_id`, `feed_format`, `title`, `url`, `summary`, `published_at`, `updated_at`, `quality_warnings`, `first_observed_at`, `last_observed_at`, `observation_count`, `distinct_ingestion_count`, `discovered_at`, `discovery_date`, `publisher_name`, `publisher_category`, `declared_language`, `country_code`, `reference_status`, `reference_id`, `reference_revision`, `reference_fingerprint` | `last_observed_at DESC, article_id ASC`; default 10 |
| `reference_coverage` | `source_id`, `reference_status`, `reference_usage`, `publisher_name`, `publisher_category`, `declared_language`, `country_code`, `article_count`, `articles_missing_reference_count`, `missing_optional_attributes` | `source_id ASC`; default 100 |
| `source_discovery_daily` | `source_id`, `discovery_date`, `discovered_article_count`, `articles_missing_reference_count` | `source_id ASC, discovery_date ASC`; default 100 |

Catalog omits internal `source_uri` and all `selected_*` fields; physical data preserves them for verification. All projections are fixed, including nullable `summary`; oversized responses fail rather than truncating text. Untrusted strings are plain data, never rendered HTML or fetched links.

Coverage rows use the union source domain. `reference_usage` is `USED` for referenced sources with articles, `UNUSED` for reference-only keys, `ABSENT` for catalog-only keys. Status is `MATCHED` for USED/UNUSED and `MISSING` for ABSENT. `article_count` is nonnegative; missing-reference count equals it only for ABSENT, otherwise zero. `missing_optional_attributes` is an array in the fixed order `publisher_category`, `declared_language`, `country_code`, including each null attribute of a referenced source; it is empty for ABSENT (missing reference is a different condition). Attribute types/nulls match E1a.

Coverage additionally returns a `totals` object with exactly these nonnegative integer metrics, calculated over the full source-filtered union domain **before pagination**:

| Metric | Definition |
| --- | --- |
| `article_count` | Catalog articles |
| `matched_article_count` | Catalog articles with a reference |
| `missing_article_count` | Catalog articles without a reference |
| `catalog_source_count` | Distinct sources with articles |
| `matched_source_count` | Used matched sources |
| `missing_source_count` | Catalog-only sources |
| `matched_sources_missing_optional_attributes_count` | Used matched sources with any null optional attribute |
| `reference_source_count` | Selected reference keys |
| `unused_reference_source_count` | Reference keys with no catalog article |

Require article count = matched + missing articles; catalog source count = matched + missing sources; reference source count = matched + unused references. Per-attribute absence is visible in source rows. Totals do not describe just the returned page.

All reports allow `source`, `limit`, `offset`. Source is exact `[a-z0-9][a-z0-9_-]{0,127}`; unknown syntactically valid sources yield empty rows and zero coverage totals. `catalog` additionally allows `reference_status`, `publisher_category`, `declared_language`, `country_code`, `start_date`, `end_date`. Reference filters are exact E1a v1 tokens; status is MATCHED/MISSING. Optional-attribute filters exclude nulls, combine with AND, and do not infer article attributes. No magic null token is supported. Discovery bounds apply to `discovery_date`, inclusive on both ends. `source_discovery_daily` supports the same date bounds only; `reference_coverage` rejects date/status/attribute filters so UNUSED is always defined relative to the entire selected catalog. Publication-date filtering is deferred, not silently substituted.

Dates require exact Gregorian `YYYY-MM-DD`, years 0001–9999, `start <= end`; no relative dates. Missing bounds mean unbounded on that side. Unknown/duplicate options, whitespace-normalized tokens, bool-as-integer, float, malformed digits and unsupported filter/report combinations fail before opening data.

Limits are integers 1–1000; offsets 0–1,000,000, default 0. Execute ordered `LIMIT limit + 1 OFFSET offset`, bound the extra lookahead row too, remove it and return `has_more`. Empty/beyond-end pages return no rows and false; no full count query is implied for catalog/daily. If offset reaches the cap and more rows exist, `has_more` stays true; narrowing filters is required to reach further results. Pagination does not claim sublinear scanning.

Successful envelope keys are exactly `status: "OK"`, `product_id`, `product_fingerprint`, `gold_fingerprint`, `reference` (ID/revision/fingerprint object), `report`, `filters`, `limit`, `offset`, `has_more`, `rows`, plus `totals` only for coverage. `filters` contains all applicable non-pagination filter keys with null for omitted values; coverage has only `source`, daily has `source/start_date/end_date`. Timestamp values use UTC `Z` with exactly six fractional digits, dates ISO date, nulls JSON null, warning/attribute arrays preserve order. All metric/count/offset values, including limits, are canonical nonnegative decimal **strings** in the public Python result and CLI JSON; booleans remain booleans. No IEEE-754 conversion. Contract constants live in verified metadata rather than each report row.

Local CLI forms:

```text
content_publication.py --output-root ROOT --gold-root ROOT --gold-fingerprint HEX
  --reference-root ROOT --reference-id ID --revision REV --reference-fingerprint HEX
content_analytics.py --root ROOT --publication HEX --gold-root ROOT --reference-root ROOT
  --report catalog|reference_coverage|source_discovery_daily [approved filters]
```

The writer owns/stops its Spark session in CLI mode; the library caller owns a supplied Spark session. `--help` and argument failures need no Spark initialization. CLI success uses stdout; failures use empty stdout and one fixed JSON diagnostic on stderr, at most 4 KiB. Exit 0 succeeds, 2 means invalid arguments, 1 means all integrity/availability/resource/processing failures. Writer summaries are at most 4 KiB and stringify counts. No arbitrary identifiers, paths, article text, credentials or underlying exception strings appear in errors. Spark/JVM output must stay off structured stdout; runtime logs remain local operator diagnostics, not user-facing error payloads.

### Resource and admission limits

These are accepted conservative admission policies, not capacity or latency measurements and not a change to Gold/E1a contracts. Exceeding any bound fails explicitly; never sample, truncate input, skip verification or relax a schema to fit.

| Resource | Accepted limit / behavior |
| --- | --- |
| E1a intake/rows | Existing 10 MiB CSV, 10,000 rows and all fixed E1a metadata/field caps |
| Content `inputs.json` | 16 KiB, bounded read plus one excess-byte sentinel |
| Gold `inputs.json` / evidence copy | 16 MiB each |
| Gold/Content completion metadata | 8 MiB each; cap before JSON parsing |
| Physical analytical files | At most 8,192 combined across both Gold and both Content datasets; at most 4,096 in any one dataset |
| File inventory path | At most 512 UTF-8 bytes; flat dataset-relative path rules above |
| Analytical bytes | At most 8 GiB combined Gold plus Content Parquet; streaming hashes with bounded chunks |
| Catalog rows | At most 10,000,000, checked against marker and actual counts; not a performance claim |
| DuckDB resources | Two threads, 512 MiB per connection, spill disabled; sequential Gold verification and Content verification/query connections, not overlapping budgets |
| Query execution | One active request per opened session; concurrent call fails `BUSY`; operator runs at most two consumer CLI processes, no new cross-process scheduler |
| Encoded result | At most 1 MiB UTF-8 including envelope/newline, no truncation; error payload at most 4 KiB |
| Page materialization | At most 1001 rows; preflight total UTF-8 string bytes of the entire lookahead page in SQL, at most 128 KiB, before Python fetch; then enforce actual JSON byte limit |

The owner-approved amendment keeps the consumer synchronous. Dedicated cancellable workers, hard open/report deadlines, process termination/reaping and timeout-specific tests are deferred. A simple synchronous guard permits one active request per session. The library and CLI enforce the same integrity, admission and response-size bounds. Record observed open/report durations in validation evidence; no interactive latency guarantee is made. The operator's two-process convention is not a system-wide lock or enforced multiuser quota.

The adapter must bounded-read/preflight Gold metadata and inventories before calling legacy verification, which currently reads metadata wholly into memory. Under trusted immutable inputs these caps bound admitted metadata; they are not hostile-race or peak-RSS guarantees. DuckDB's configured memory limit is not a hard process-memory limit. SQL string-byte checks before page fetch address Gold's lack of small per-cell text limits; even an individual otherwise valid Gold row can produce `RESPONSE_LIMIT`. Spark publication uses existing local resource configuration, has no 30-second interactive deadline, and aborts on configured input/output bounds or engine failures. Stage B must validate the limits on synthetic fixtures and report any feasibility problem rather than quietly raising them.

## Safe failure classes and precedence

Every class has the fixed message below. No raw dependency exception or path is forwarded. Optional `component` may only be the literal `gold`, `reference`, `catalog`, `discovery` or `metadata`. No other details are exposed.

| Code | Fixed message / condition |
| --- | --- |
| `ARGUMENT_INVALID` | `Arguments do not match the Content interface.` |
| `PATH_UNSAFE` | `A selected path violates the local path policy.` |
| `UNSUPPORTED_VERSION` | `A selected contract or processor version is unsupported.` |
| `DEPENDENCY_UNAVAILABLE` | `A selected dependency is unavailable.` — absent selection or ordinary read/close/stat failure without demonstrated mutation |
| `DEPENDENCY_INVALID` | `Dependency evidence is incomplete or invalid.` — selected completion/inventory/schema/value failure |
| `DEPENDENCY_CHANGED` | `A selected dependency changed during the operation.` — observed change only |
| `PUBLICATION_UNAVAILABLE` | `The selected Content publication is unavailable.` |
| `PUBLICATION_INCOMPLETE` | `Content publication is incomplete; owner investigation is required.` |
| `PUBLICATION_INVALID` | `Content publication evidence is invalid.` — metadata, inventory, schemas, rows or reconciliation |
| `PUBLICATION_CHANGED` | `Content publication evidence changed during the operation.` |
| `PUBLICATION_FAILED` | `Content publication failed; investigate retained evidence.` — new write/read-back/finalization I/O or Spark failure |
| `COUNT_OVERFLOW` | `A Content count exceeds its supported range.` |
| `RESOURCE_LIMIT` | `A Content resource exceeds its configured limit.` |
| `RESPONSE_LIMIT` | `The requested report exceeds its response limit.` |
| `BUSY` | `The Content session already has an active request.` |
| `QUERY_FAILED` | `The Content report could not be completed.` |

Initial missing dependencies differ from disappearance after observation. Existing invalid Content returns PUBLICATION_INVALID; an ordinary I/O error reading existing evidence also fails with that integrity class unless the selected directory itself is initially unavailable. Underlying E1a argument/path/resource/change distinctions are preserved through the corresponding Content class. A fixed Gold validation failure becomes DEPENDENCY_INVALID; an inconclusive generic Gold read failure is not evidence of change. Adapter guards establish observed mutations independently.

Precedence is deterministic within the operation: argument/filter syntax, path safety and intake bounds; then Gold verification before reference verification for publishing; then expected-frame validation and destination state. For opening an existing Content selection, validate its bounded layout/completion/manifest first to obtain dependency identities, then Gold, reference and output. Missing completion precedes reading retained artifacts. Within output verify inventory, metadata, every schema, values/keys, full row equality and aggregate/coverage reconciliation in that order. Rechecks occur at the documented boundaries and may supersede pending success, never turn failure into success. Resource/path/observed-change failures retain their classes; a new output I/O failure is PUBLICATION_FAILED. No broad mapping of every exception to a mutation class.

## Independent synthetic acceptance fixture

This is the specified synthetic fixture, exercised by the Stage B tests below; it is not collected publisher data. Use fictional RSS observations through existing Bronze/Silver/Gold fixture helpers for end-to-end tests; a transformation unit fixture may directly construct the fully specified Gold rows below. Expected results are literals and hand-calculated groups, not values obtained by calling the new enrichment code.

Observation table (`entry_index` is zero unless shown; instants UTC):

| Ingestion number | Source | Native RSS GUID | Ingested at | Entry | Title | Published at / warnings |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | alpha | shared | 2026-10-12T23:59:59.999999Z | 0 | Alpha old | 2026-10-10T12:00:00.000000Z / `[]` |
| 1 | alpha | second | 2026-10-12T23:59:59.999999Z | 1 | Alpha second | 2026-10-10T12:00:00.000000Z / `[]` |
| 2 | alpha | shared | 2026-10-13T00:00:00.000001Z | 0 | Alpha latest | null / `["INVALID_PUBLISHED_AT"]` |
| 3 | beta | shared | 2026-10-13T00:00:00.000000Z | 0 | Beta shared | null / `[]` |
| 4 | gamma | third | 2026-10-13T00:00:00.000001Z | 0 | Gamma third | 2026-10-11T00:00:00.000000Z / `[]` |

For the direct Gold-row fixture, define ingestion UUID `00000000-0000-0000-0000-NNNNNNNNNNNN` with decimal number zero-padded to 12; `identifier_kind="rss_guid"`, `feed_format="rss"`, `source_uri="https://example.invalid/<source>/feed"`, `url=null`, `summary=null`, `updated_at=null`. Identity is the existing SHA-256 of UTF-8 compact JSON `["v1",source,"rss_guid",guid]`. Selected provenance is from the winning numbered observation: `selected_silver_contract_version=1`, the above UUID/time/entry, `selected_input_uri="s3://local/<source>/<number>"`, payload digest equal to the number represented as 64 lowercase hex digits, payload length 10, bucket `local`, payload key `<source>/<number>/payload.xml`, metadata key `<source>/<number>/metadata.json`. These synthetic provenance values specify a direct row fixture, not real payload checksums. Producer-backed tests use actual fixture-generated payload/provenance and separately assert every preserved base field against that verified Gold input.

Reference CSV is exactly the following UTF-8 text with LF after every line, no BOM, canonical retained E1a vocabulary v1, `reference_id=fixture-publishers`, `revision=r1`, `maintainer_id=example-team`, `note=null`:

```csv
source_id,publisher_name,publisher_category,declared_language,country_code
alpha,Alpha Press,science,en,CA
beta,Beta Press,,,
delta,Delta Press,culture,fr,GB
```

All four catalog rows preserve the full Gold base fields just specified. Each appends `content_contract_version=1`, `discovered_at=first_observed_at`, the derived date and the same selected E1a tuple. This table specifies the remaining exact output values:

| Source / GUID | Selected title | First observed = discovered | Last observed | Observations / ingestions | Date | Reference fields (name/category/language/country) | Status |
| --- | --- | --- | --- | --- | --- | --- | --- |
| alpha / shared | Alpha latest | 2026-10-12T23:59:59.999999Z | 2026-10-13T00:00:00.000001Z | 2 / 2 | 2026-10-12 | Alpha Press / science / en / CA | MATCHED |
| alpha / second | Alpha second | 2026-10-12T23:59:59.999999Z | 2026-10-12T23:59:59.999999Z | 1 / 1 | 2026-10-12 | Alpha Press / science / en / CA | MATCHED |
| beta / shared | Beta shared | 2026-10-13T00:00:00.000000Z | 2026-10-13T00:00:00.000000Z | 1 / 1 | 2026-10-13 | Beta Press / null / null / null | MATCHED |
| gamma / third | Gamma third | 2026-10-13T00:00:00.000001Z | 2026-10-13T00:00:00.000001Z | 1 / 1 | 2026-10-13 | null / null / null / null | MISSING |

Expected discovery rows, ordered, are exactly `(1,alpha,2026-10-12,2,0)`, `(1,beta,2026-10-13,1,0)`, `(1,gamma,2026-10-13,1,1)`. Counts sum to 4, missing sums to 1. Gold still has 5 observations across 4 ingestion-day groups: alpha October 12 has 2 observations, alpha October 13 has 1, beta/gamma October 13 each have 1. E1b does not rewrite those metrics.

Literal article IDs, computed independently from the stated identity byte arrays, are:

| Source / GUID | `article_id` |
| --- | --- |
| alpha / shared | `cd3921bf8c02907ea32b983e7acdc8949150d987a082a933e3f35a85c9a7cef1` |
| alpha / second | `7a4b3986539d368dff0348f9cc3ea33be6baf80143d7edf7df8aa0a65ee58907` |
| beta / shared | `169632af4e4ba1a58d6fc493b1afbad6d647b21ec7ed5b82fef933b82fcb8076` |
| gamma / third | `3dd4843dc774fde61ebd8ffce4439d91ec199ac1a1541cba6a985b7f2c310247` |

Thus the catalog report orders gamma/third, alpha/shared, beta/shared, alpha/second. With limit 2, offset 0 returns the first two and `has_more=true`; offset 2 returns the last two and false; offset 4 returns empty and false. Reference r1's exact 158-byte CSV has SHA-256 `a95a053f01828226a4b6f5f8683597dc0c2f538d6c047bdbdbf2f649f93b3350`; its canonical 576-byte E1a manifest has fingerprint `a98795fc7537ebcb31b10f2f1e8ec1bb6a8425bbd3871b9d92b1c3c94dfcaba1`.

Expected coverage rows by source: alpha USED/MATCHED with 2 articles and no missing optional attributes; beta USED/MATCHED with 1 and all three optional attributes missing; delta UNUSED/MATCHED with 0 and none missing; gamma ABSENT/MISSING with 1 missing-reference article and an empty optional-attribute list. All other missing-reference row counts are 0. Totals in the defined key order are `4,3,1,3,2,1,1,3,1`. Filtering coverage to delta returns its unused row and totals `0,0,0,0,0,0,0,1,1`; pagination does not change either scope's totals.

Header-only reference yields the same four article IDs/base values, all attributes null, all statuses MISSING, daily missing counts `2,1,1`, and coverage totals `4,0,4,3,0,3,0,0,0`. For the rejected-only case, separately publish a fictional Atom feed containing `<entry><id>invalid</id></entry>` without a title through the existing fixture path: its selected completed Gold has accepted count 0, rejected count 1 and schema-bearing empty datasets. It yields zero Content catalog/daily rows; the three reference keys are unused and totals are `0,0,0,0,0,0,0,3,3`. A separate valid empty feed yields the same Content rows with rejected count 0; the copied Gold manifests keep those histories distinguishable.

Reference correction example: publish r2 changing only alpha's category to `technology`. All four article IDs/base values/discovery groups and coverage totals stay the same; alpha context and every row's selected reference tuple change, producing a new product. Historical-input example: a new Gold publication additionally includes alpha/shared at `2026-10-11T23:59:59.999999Z` with an older title. Its selected winner stays Alpha latest; counts become 3/3; discovery moves to October 11. Alpha now has `(October 11,1,0)` and `(October 12,1,0)` discovery groups. Global discoveries remain 4. Prior Gold/E1a/Content snapshots remain byte/mtime unchanged.

## Stage B acceptance matrix

| Case | Independent evidence required |
| --- | --- |
| B01 — dependency gate | Metadata-valid but schema/value-invalid Gold rejected through full opener; exact inventories only; E1a public opener required; no CSV bypass |
| B02 — identity/base preservation | Literal fixture article IDs, all 27 base fields, source-scoped shared GUIDs, nulls and ordered warnings preserved; no row multiplication |
| B03 — reference coverage | Present attributes, matched optional nulls, unmatched source, unused key, header-only reference and empty catalog with exact totals above |
| B04 — time and groups | Cross-day repeated observation, midnight ± one microsecond, inclusive dates, UTC regardless of host timezone, publication nulls and exact group rows |
| B05 — schemas/empty data | Inspect every physical file; wrong column/order/type/version, null required values/array elements, duplicate keys and empty schema-bearing/rejected-only Gold |
| B06 — complete equality | Alter a base value/reference attribute/date/group while preserving counts; even refreshed output hashes must fail bidirectional row comparison |
| B07 — identity vectors | Literal canonical bytes/digests below, reordered JSON keys, changed dependency/ref/revision/semantics and excluded operational settings |
| B08 — replay/change | Identical replay preserves all bytes/mtimes; changed Gold/reference creates a new namespace; prior Gold/E1a/Content and old consumers remain unchanged |
| B09 — dependency corruption | Missing/wrong selected Gold/E1a, manifest/completion/Parquet/CSV/vocabulary tampering, invalid selection and retention loss reject both writer and reader |
| B10 — output corruption | Missing/extra artifacts, stale temporary marker, malformed/duplicate-key/noncanonical metadata, false counts/digests, unexpected bookkeeping, per-file schema corruption |
| B11 — failures/finalization | Deterministic exclusive-create, partial Spark write, read-back/close/hash/rename failure and interruption before/after rename; no repair, partial success or promotion |
| B12 — observed changes versus I/O | Replace/disappear/change bytes, size, mtime, inode, parent/inventory after capture and during Spark/query actions; ordinary unchanged-file read/close/stat failure has safe non-mutation classification |
| B13 — Spark-free reader | Separate process with Spark/producer imports forbidden, invalid Java path and Silver removed; all three reports succeed while retained Gold/E1a remain verified |
| B14 — reports and pages | Literal projections/order, same-timestamp article-ID tie, first/middle/final/beyond-end pages, limit 1/1000/1001, offset 0/1,000,000/one-over, bool/float/SQL-shaped parameters, unknown source, filter combinations |
| B15 — safe/resource boundary | Decimal integers above 2^53, finite UTC microseconds, overflow, metadata/file/byte/row caps, giant string/page budget, synchronous busy guard/session cleanup, fixed diagnostics/no raw text |
| B16 — independence/regression | Hand-written expected rows/groups plus separate standard-library oracle; Spark/DuckDB agreement alone insufficient; existing Gold/E1a tests, APIs and consumer schemas unchanged |

For overflow/huge-resource cases, use bounded synthetic lower-level inputs or fault injection instead of manufacturing enormous retained datasets. No timing-dependent races or live publishers are needed. Stage B must run focused and full processing tests with the existing Python 3.12/Java 21/PySpark 4.0.2/DuckDB 1.4.5 environment, relevant existing compatibility checks and documentation/hygiene checks; preserve CI job names. Do not install anything during Stage A.

## Compatibility vectors and validation evidence

These vectors isolate canonical identity serialization from Spark's non-deterministic physical file names/encoding. Their Gold fingerprints/digests are explicitly fictional opaque test values, not claims of existing verified Parquet: a full publication verifier must reject them without matching real evidence. The complete hashing input is the literal JSON below; no external bytes or runtime-generated manifest are needed to test its expected hash. The reference descriptor uses the fully specified fixture above. Producer-backed acceptance separately generates actual valid Gold and checks dependency binding; it must not pretend these descriptor vectors are completed publications.

Vector A is exactly the following 905 UTF-8 bytes, with no final newline. Its SHA-256 is **`3ef7a9603c8d558008026aa8a4e8ca8f66966e2db445d042f00531501c770dfb`**.

```json
{"dataset_contract_versions":{"content_catalog_v1":1,"content_source_discovery_daily_v1":1},"gold":{"completion":{"byte_length":512,"file":"gold-complete.json","sha256":"1111111111111111111111111111111111111111111111111111111111111111"},"input_manifest":{"byte_length":256,"file":"gold-inputs.json","sha256":"0000000000000000000000000000000000000000000000000000000000000000"},"publication_fingerprint":"0000000000000000000000000000000000000000000000000000000000000000"},"manifest_version":1,"processor_revision":1,"product_contract_version":1,"product_id":"content-intelligence","reference":{"reference_id":"fixture-publishers","revision":"r1","snapshot_fingerprint":"a98795fc7537ebcb31b10f2f1e8ec1bb6a8425bbd3871b9d92b1c3c94dfcaba1"},"semantic_config":{"discovery":"first-observed-utc-v1","gold_preservation":"all-fields-v1","reference_application":"snapshot-v1","reference_join":"exact-source-left-v1"}}
```

| Vector | Exact transformation of A before canonical serialization | Length | Expected SHA-256 |
| --- | --- | --- | --- |
| B | Set only `reference.revision` to `r2` and `reference.snapshot_fingerprint` to `487620b9287188e8d69b6ae67f027edeff259e6b0301315a32d3bee9b39c8da4` | 905 | `8b9235817778d0fb111074b36f1cca24937a46f63e4191d70207dca5b91bdde7` |
| C | Set only `gold.completion.sha256` to 64 ASCII `2` characters | 905 | `a679f85af47810745b4d6bc223ecb4d98350d4eb602462f2ddc9b940f3615ff1` |

B's E1a fingerprint follows exactly the r2 category correction described above: 161-byte CSV, SHA-256 `851ca9e915b7a823fb25eba17f3fe6e2ca5c6f316aaf6ecfdb2eedd1230500b5`, 576-byte canonical E1a manifest with the same authoring/vocabulary and revision `r2`. C demonstrates why an unchanged Gold input-set fingerprint alone is insufficient for this derived identity.

Reversing insertion order of every JSON object must reproduce each same vector. Changing roots/logging/partitions/completion clock must leave A unchanged because none is an input field. Appending a newline to stored `inputs.json` must fail canonical validation rather than becoming a supported alternate identity. Changed semantic constants/versions must change the hash in serializer tests but remain unsupported by the revision-1 runtime until reviewed.

Evidence must be reported separately:

| Evidence class | Current state / required follow-up |
| --- | --- |
| Stage A document consistency | Literal identity/reference/article vectors calculated with standard-library JSON/SHA-256 only; documentation checks recorded after final review |
| Stage B synthetic correctness | Implemented in the focused Content tests; executed results and final counts are recorded below |
| Retained-data end-to-end | Not run for E1b; no retained publisher observations invented or altered. M2's single ingestion date cannot prove multi-day first discovery |
| Hosted CI | No E1b hosted implementation run claimed; owner publication and subsequent CI remain separate |
| Scalability benchmarks | Deferred; no local/distributed throughput, memory-capacity or AWS claim |

Stage A ran no application suites, Spark processing, live ingestion or benchmarks, installed no dependencies and provisioned no infrastructure. Accepted ADRs 0001–0006 are unchanged. The owner accepted ADR 0007's seven decisions with the synchronous-consumer amendment on October 20, 2026; Stage B is authorized.

Stage A validation on October 20, 2026: `npm run check` passed Markdown formatting and relative-link checks across 37 Markdown files, followed by repository hygiene; `git diff --check` passed. A standalone standard-library documentation check verified all three product hash vectors, both E1a fixture identities, all four article IDs, and column-name/type parity for the 27 preserved Gold fields in the 38-column catalog without importing or executing application modules. Only the new ADR, this contract, the ADR index and the minimal Content specification cross-reference were changed. No Stage B, application-test, retained-data or new hosted-CI result is claimed.

## Stage B implementation and executed validation

The owner accepted ADR 0007 on October 20, 2026 with the synchronous-consumer amendment. The implementation comprises:

| Module | Implemented boundary |
| --- | --- |
| [content_metadata.py](../../processing/content_metadata.py) | Exact 38/5-column schema mappings, strict canonical metadata, identity, bounded captures, streaming hashes and file/directory guards |
| [content_dependencies.py](../../processing/content_dependencies.py) | `verify_dependencies(...)`: full Gold opening with the optional memory limit, frozen exact inventories/Gold evidence, E1a opening and detached reference rows, dependency rechecks |
| [content_processing.py](../../processing/content_processing.py) | `build_content(spark, dependencies)` and `verify_frames(gold, expected, actual)`: source left join, UTC discovery, schema-bearing empty frames, decimal reconciliation and complete Spark comparisons |
| [content_publication.py](../../processing/content_publication.py) | `publish(spark, *, output_root, gold_root, gold_fingerprint, reference_root, reference_id, revision, reference_fingerprint)`: immutable result, reservation, exclusive metadata, error-if-exists Parquet, replay and completion last |
| [content_analytics.py](../../processing/content_analytics.py) | `open_publication(content_root, product_fingerprint, *, gold_root, reference_root)`: synchronous context-managed independent SQL verifier; `report(name, **approved_filters)` returns the bounded detached public envelope |

Only the optional `memory_limit` extension changes an existing processing module. Omitting it retains Gold's original configuration and failure behavior. Providing a valid budget configures DuckDB before reads; explicit budget exhaustion is a safe `RESOURCE_LIMIT`. No runtime dependency, service, API endpoint, frontend view or workflow/job name was added. No dedicated worker, hard deadline, termination/reaping or timeout infrastructure was implemented.

Reproduction uses the established local runtime, without installation or live ingestion:

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 21)
export PYSPARK_PYTHON="$PWD/outputs/silver-python312/.venv/bin/python"
export PYTHONPATH=processing SPARK_LOCAL_IP=127.0.0.1 TZ=UTC
"$PYSPARK_PYTHON" -m unittest discover -s processing/tests -p 'test_content*.py' -v
"$PYSPARK_PYTHON" -m unittest discover -s processing/tests -v
JAVA_HOME="$JAVA_HOME" mvn -o -B -ntp -f services/api/pom.xml verify
JAVA_HOME="$JAVA_HOME" mvn -o -B -ntp -f services/content-api/pom.xml verify -Pintegration \
  -Danalytics.python="$PYSPARK_PYTHON"
npm run check
git diff --check
```

The Maven commands reuse the existing **synthetic** `services/api/target/fixtures`; they neither fetch publishers nor regenerate retained data. The Java selector is macOS-specific local setup, not a hardcoded runtime path. Local validation uses Python 3.12.15, Java 21.0.7, PySpark 4.0.2 in `local[2]` and DuckDB 1.4.5 on macOS arm64. Spark's sandboxed gateway failed to bind its loopback socket; authorized local execution outside that restriction uses the same supported loopback settings without weakened checks. Existing Spark socket `ResourceWarning`s and incubator/native-Hadoop warnings remain non-failing.

Acceptance coverage is mapped to actual tests in [test_content_dependencies.py](../../processing/tests/test_content_dependencies.py) and [test_content_publication.py](../../processing/tests/test_content_publication.py):

| Group | Implemented assertions |
| --- | --- |
| B01 | Real metadata-valid/value-invalid Gold passes metadata-only verification but fails the full gate; public opener invocation and detached/frozen inventories |
| B02 | Literal IDs and preserved base values/nulls, distinct source identities, all 27 fields via complete comparisons, two-warning array order and corruption rejection |
| B03 | Matched, optional-null, missing and unused reference rows; exact nine coverage totals; header-only reference |
| B04 | Literal midnight/microsecond instants and discovery groups, inclusive dates, explicit non-UTC Spark configuration reset to UTC |
| B05 | Every-file schema validation, duplicate keys, required/version/count faults, schema-bearing empty and rejected-only Gold |
| B06 | Altered base, reference and discovery values with refreshed hashes still rejected by independent full-row comparisons |
| B07 | Literal vectors A/B/C and exact E1a correction fingerprint; strict canonical JSON/version failures |
| B08 | Byte/identity/mtime-preserving replay; reference-only and older-history selections produce new publications while preserving prior output |
| B09 | Missing/corrupt Gold and E1a metadata/data, unavailable selected snapshots and dependency retention loss |
| B10 | Corrupt completion and inventory, unexpected/hidden/temporary artifacts, one wrong-schema file beside correct files |
| B11 | Exclusive-create collision preserves existing bytes; write/close/read-back/marker/rename faults, partial Spark writes and lost response after final rename; incomplete reservations blocked |
| B12 | Same-stamp byte mutations, inode replacement/disappearance, root/parent/inventory changes, unchanged-file I/O/close failures, combined mutation/close errors and failed stream-construction cleanup |
| B13 | Separate process forbids Spark/Silver/producer imports, uses invalid Java path and runs all reports without Silver/original authoring inputs |
| B14 | Exact projections/order, filters, bounded lookahead, empty/beyond-end/max-offset pages, parameter rejection, decimal-string envelope and CLI exits/diagnostics |
| B15 | Metadata/file/byte/row caps; two-thread/512 MiB/no-spill/no-extension settings; explicit low-memory failure; counts above 2^53, decimal overflow, oversized lookahead and encoded-response rejection, synchronous BUSY guard |
| B16 | Hand-authored fixture results and independent standard-library discovery oracle; real synthetic Bronze/Silver-to-Gold-to-Content flow followed by Silver removal; unchanged Gold consumer and full regressions |

The producer-backed test initially expected two accepted RSS items. Inspection of the existing fictional feed established one valid item and one rejection because the other item lacks both GUID and fallback link; the test now asserts that independent expectation and retains rejection provenance. No input parser or fixture was weakened to make it pass.

These checks establish synthetic correctness for the exercised local boundary. They do not establish retained multi-day publisher history, authenticated source truth, measured scalability, power-loss durability or a query deadline. Full verification still requires retained Gold and E1a and performs whole-publication hashing/scans. The accepted Gold reader's inability to reconstruct Silver winners/history remains unchanged. Hosted CI for E1b remains unverified until the owner publishes this work and a corresponding run completes.

Final local results on October 20, 2026:

| Validation | Actual result |
| --- | --- |
| Focused E1b | **37/37 passed in 88.892 seconds**: 13 dependency/metadata tests and 24 Spark/publication/consumer tests |
| Full processing | **164/164 passed in 180.192 seconds**, including all 37 E1b tests and all 127 existing regressions |
| Relevant existing coverage within full processing | 46 E1a, 19 Gold publication, 17 Gold analytics and 5 Gold transformation tests passed, alongside Bronze/Silver regressions |
| Existing JDBC compatibility | **48/48 passed**, offline Maven build successful |
| Existing Content API compatibility | **120/120 unit/component and 20/20 HTTP/Python integration tests passed**, offline Maven build successful; no skipped tests |
| CLI help | Both new help commands exited 0 without Spark initialization or dependency selection |
| Documentation and hygiene | `npm run check` passed 37 Markdown files, relative links and repository hygiene; `git diff --check` passed |
| Acceptance status | **B01–B16 passed**, with B15's worker/deadline/timeout exclusions exactly as owner-approved; implementation checkpoints A–E complete |

One timed observation in the final focused run used four catalog articles, five accepted observations in the selected Gold scope, three discovery groups and three reference rows: initial Content publication **5.483 seconds**, independent open **0.048 seconds**, catalog report **0.008 seconds**. The final full run independently observed **4.254 / 0.050 / 0.008 seconds** for those operations. These are local test observations with uncontrolled filesystem/JVM cache state, not repeated benchmark distributions, capacity claims or interactive latency guarantees. No retained-data or scalability workload was run.

The final classification tests additionally prove that a newly observed directory entry remains `DEPENDENCY_CHANGED` even if iteration then fails, while an unchanged partial inventory with an I/O error remains `DEPENDENCY_INVALID`. This preserves the approved distinction without weakening inventory checks. A stream-construction failure closes the already opened file descriptor. No production-like retained datasets, accepted ADRs 0001–0006, dependencies or CI job names were changed. The existing staged Stage A documents were preserved in the index; implementation and acceptance updates remain working-tree changes for owner review. No Git commit, push, PR, merge, issue closure or other GitHub write was performed.
