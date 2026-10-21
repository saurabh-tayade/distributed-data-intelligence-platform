# Reliability format equivalence and benchmark contract

Status: **Accepted by the owner on October 20, 2026**, with ADR 0009’s two execution-policy amendments, for [issue #46](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/issues/46) and [ADR 0009](../adr/0009-reliability-format-equivalence-benchmarks.md). Architecture acceptance, implemented capability and actual evidence are separate. Current scope is E3 correctness and bounded regression validation only. E4a’s 100K/1M experiments and instrumentation are deferred; neither workload may run in this delivery. Accepted [E2 v1](application-reliability-publication.md) remains unchanged. The [platform plan](../product-specification.md) and [Reliability specification](../data-products/application-reliability.md) remain Proposed.

## Current owner-approved sequencing

E3 correctness is immediate; E4a performance is deferred; E5–E7 end-to-end platform delivery follows. All format/provenance/identity/schema/privacy/publication/reader decisions remain. The benchmark methods/result schemas/preflight formulas below are retained as future E4a plans, not current E3 acceptance. F01–F13 are correctness groups; F14–F17 are deferred except small configuration/resource-safety checks needed for correctness. F18 retains full regression/documentation requirements. No benchmark evidence registry, Spark event-log instrumentation, 100K/1M execution or numeric 1M preflight is required to complete E3.

The historical macOS resource screen does not determine the application architecture. Correctness uses portable Python/Spark/DuckDB/SQLite interfaces and the accepted per-operation limits. Actual resource exhaustion still fails closed. Benchmark-specific host admission and detailed monitoring belong to E4a, separately from business processing. Existing benchmark scaffolding is preserved without expanding it.

## Baseline and scope

Initial inspection: branch feature/e3-e4a-reliability-benchmarks, clean working tree and index, HEAD 257af66 (PR #45 merge). Accepted ADRs 0001–0008, E1a's I/O correction, E1b and all six E2 modules/tests are present. Issue #46 and its full acceptance criteria were reviewed read-only; there were no issue comments. Baseline merge is not new hosted-CI evidence.

Pinned processing requirements are Python 3.12.15, Java 21, PySpark 4.0.2 / Py4J 0.10.9.9 and DuckDB 1.4.5. No new dependency, infrastructure or runtime installation is authorized. SQLite is Python's standard-library module; capture its actual library version. Outputs belong under ignored outputs/reliability-benchmarks/, never Git. Normal CI discovers processing/tests with unittest, SPARK_LOCAL_IP=127.0.0.1, TZ=UTC and Java 21. Preserve all existing job names and API/JDBC compatibility checks; large experiments must not enter normal discovery.

| Existing boundary inspected | Reuse / required change |
| --- | --- |
| publication_metadata.canonical_bytes | Reuse unchanged: recursive sorted keys, compact UTF-8 JSON, no NaN/BOM/final newline |
| reliability_inputs.register / open_batch / select | JSONL-v1 only: fixed profile, data/*.jsonl, physical_line_count and batch UUID domain. Keep unchanged; v2 needs a new registrar/selection adapter |
| reliability_inputs.scan_file / parse_event | Preserve existing bounded JSONL framing, privacy, reason ordering and typed-ten equality through a narrow JSONL adapter; never use it to reinterpret Parquet |
| reliability_inputs.stage / STAGE_KEYS | Eleven fields with required line positions; cannot carry Parquet. New completed staging-v2 inventory and tagged record schema required |
| reliability_processing.normalize / aggregate / verify_frames / verify_ranks | Existing normalize, schemas and physical-key order are coupled to occurrence-v1. Implement a domain-specific v2 normalization/verification layer; preserve its typed model and accepted arithmetic, not private frame assumptions |
| reliability_publication.publish | Hard-coded v1 namespace, schema set, input manifest and verifier; not reusable by passing another extension. New v2 writer required |
| reliability_analytics.open_publication / verify_raw | Full independent raw verification is required, but the cursor joins on line_number and expects 16 columns. New version-specific reader/raw adapter required |
| service_reference.open_snapshot | Reuse unchanged; exact ID/revision/fingerprint, detached validated rows, complete metadata/identity rechecks |
| E2 evidence utilities / Content patterns | Retain identity/hash/directory/error-precedence patterns with domain tests. No accepted domain refactoring or generic ETL framework |

## Version and interface boundary

Use the accepted product contract 2 / processor revision 2 with two narrow format adapters. Domain modules: reliability_format_metadata, reliability_format_inputs, reliability_format_processing, reliability_format_publication and reliability_format_analytics. Separate reliability_benchmark_generator, reliability_benchmark_oracle and reliability_benchmark modules handle opt-in experiments. Input/reference/metadata/reader/oracle modules remain Spark-free; only generator preparation, processing and writer entry points may initialize Spark where needed. The preferred generator uses DuckDB COPY and needs no Spark preparation.

~~~text
reliability_format_inputs.register(batch_root, *, input_root, source_id, batch_id,
    input_format, files, received_at, profile)
reliability_format_inputs.open_batch(batch_root, input_format, source_id,
    batch_id, expected_fingerprint)
reliability_format_publication.publish(spark, *, batch_root, input_format,
    batches, reference_root, reference_id, revision, reference_fingerprint,
    staging_root, output_root, execution_config, observer=None)
reliability_format_analytics.open_publication(root, product_fingerprint, *,
    batch_root, reference_root, product_contract_version=2)
session.report(name, **approved_filters)
session.close()
~~~

input_format is exactly JSONL or PARQUET. batches is 1–32 exact (source_id,batch_id,batch_fingerprint) tuples. Validate then collapse identical selections; conflicting bindings fail. One publication has one format, one logical profile/generator variant and one reference snapshot. Mixed formats, mixed generator variants, legacy batch manifests, implicit latest, inference and caller-fabricated verified descriptors fail. Legacy v1 APIs/namespaces remain usable exactly as before. New reader selects v2 explicitly; no fallback to v1 or degraded metadata-only verification. Re-registration of old JSONL is an explicit copied new binding, with new fingerprints; no automatic migration or historical completion edits.

execution_config contains exactly master (implemented local[1] or local[2]; local[4] remains a future optional approved benchmark cell), shuffle_partitions=8, default_parallelism=8, driver_memory_mib=1024, executor_memory_mib=1024, output_partitions=4 and a Boolean event_log_enabled. E3 correctness permits false and requires no event-log directory or hardware preflight. Future E4a trials require true and the planned instrumentation. These execution choices do not enter product identity; they enter benchmark configuration. The isolated v2 SQL session retains UTC, TIMESTAMP_MICROS, corrected read/write calendar modes, ANSI arithmetic and accepted p95. The library requires a matching supplied Spark context; CLI validates configuration before creating it. Do not change E2/Gold/Content session defaults. observer is an optional bounded fixed-span timing sink, not arbitrary processing code or an input/output transformation; its failure fails the invocation and never relaxes publication checks.

### Registered batch v2

~~~text
<batch-root>/v2/format=jsonl|parquet/source_id=<source>/batch_id=<batch>/
  manifest.json
  complete.json
  data/<artifact_id>.jsonl|parquet
<output-root>/v2/processor_revision=2/publication=<fingerprint>/
  inputs.json
  reliability_events_v1/<listed>.parquet
  reliability_occurrences_v2/<listed>.parquet
  reliability_requests_5m_v1/<listed>.parquet
  complete.json
~~~

Roots must be existing, trusted, canonical and mutually nonoverlapping, following E2's no-symlink-at/below-root, no-hard-link/special-file/traversal/glob policy. The format path token is lowercase; the metadata enum is uppercase. Each batch has exactly its two metadata files and flat inventoried data directory. All writes use exclusive reservation/creation; completion temporary marker is verified before same-directory rename last. Incomplete reservations cannot be replayed, repaired or resumed automatically.

All metadata uses unchanged canonical_bytes. Batch fingerprint is SHA-256(manifest.json exact bytes). Manifest has exactly: manifest_version=2, batch_contract_version=2, request_schema_version=1, registrar_revision=2, input_format, source_id, batch_id, ingestion_id, received_at, profile, files. IDs and timestamps retain E2 syntax/ranges. ingestion_id is UUIDv5(URL namespace, UTF-8 text `ddip:reliability:batch-v2:<FORMAT>:<source_id>:<batch_id>`), with uppercase FORMAT. Receipt is explicit, not invocation time. Literal UUID vectors for bench-source/b00 are 4dcb761e-71ed-526f-8a63-1e9c303a02d4 (JSONL) and a581aa05-df68-566e-8ba4-f17b0e22c76e (PARQUET); format domains intentionally differ.

profile has exactly E2's nine keys plus decoder: profile_id=request-jsonl-v2 or request-parquet-v1, schema_version=1, config_revision=1, origin=synthetic, synthetic_source_id equal to source, logical_dataset_id (null or 64-hex generator-config identity for benchmarks), allowed_service_ids (1–128 sorted unique), allowed_route_ids (1–256 sorted unique), policy_revision=2, decoder. decoder is exactly {implementation:jsonl-physical-v1,revision:1} for JSONL or {implementation:duckdb-file-row-number,revision:1,duckdb:1.4.5} for Parquet. Here and below bare symbolic values in schema descriptions are JSON strings; exact schemas reject missing/extra keys. Logical dataset labels are declarations to verify, not evidence of parity.

Each file entry has exactly artifact_id, file, byte_length, sha256, record_count, physical_line_count, row_groups. file is the fixed `data/<artifact>.<format>` relative path. Sort by artifact_id, reject duplicates. JSONL uses physical_line_count=record_count and row_groups=null. Parquet uses physical_line_count=null and row_groups=[{row_group_ordinal,record_start,record_count,compressed_byte_length,uncompressed_byte_length}, ...], in footer order; ordinals/start/counts are nonnegative integers. record_start is the sum of preceding group counts. Decode all rows to confirm metadata; byte counts are the sums of column-chunk sizes, not claimed process memory. A valid zero-row Parquet file may have no groups or zero-row groups; preserve their footer ordinals, never invent records. Zero-byte Parquet is invalid. Footer metadata and actual decoded coverage must agree.

Batch completion has exactly completion_version=2, batch_fingerprint, manifest_byte_length, completed_at. It binds exact canonical manifest length and fingerprint; timestamp is representable UTC and not before receipt. Its exact bytes are retained and bound by product selection. Registration returns only a bounded immutable identity/count summary; open returns detached metadata and guarded descriptors, not event history. Full independent admission repeats on open, not just marker inspection.

## Exact Parquet input and privacy contract

The root schema has exactly these ten ordered primitive OPTIONAL fields. OPTIONAL permits typed-invalid null fixtures; all ten values are semantically required for valid events. Reject REQUIRED/mixed repetition in this narrowly reviewed input profile, nested/repeated fields, duplicates, unknown fields and schema union/coercion. Unknown/sensitive schema fields abort INTAKE_NOT_APPROVED before values are exposed. Wrong known physical types abort PARQUET_INVALID, not row-level FIELD_TYPE. No supplied schema/cast may disguise physical incompatibility.

| Position | Field | Physical / logical type | Valid value |
| --- | --- | --- | --- |
| 1 | schema_version | INT32, no annotation or consistent signed INT(32) | Exactly 1 |
| 2 | event_id | BYTE_ARRAY, STRING / UTF8 | E2 lowercase UUID |
| 3 | service_id | BYTE_ARRAY, STRING / UTF8 | Reviewed allowed technical key |
| 4 | environment | BYTE_ARRAY, STRING / UTF8 | dev, test or production |
| 5 | event_time | INT64, TIMESTAMP(MICROS,isAdjustedToUTC=true) | Strict E2 instant, years 0001–9999 |
| 6 | method | BYTE_ARRAY, STRING / UTF8 | GET, HEAD, POST, PUT, PATCH, DELETE, OPTIONS |
| 7 | route_id | BYTE_ARRAY, STRING / UTF8 | Reviewed allowed opaque token |
| 8 | status_code | INT32, no annotation or consistent signed INT(32) | 100–599 |
| 9 | duration_ms | INT64, no annotation or consistent signed INT(64) | 0–3,600,000 |
| 10 | is_synthetic | BOOLEAN | true |

Accept the explicitly listed equivalent signed-integer annotations, modern STRING and equivalent legacy UTF8 annotations; require consistent physical/logical declarations. No INT96, timezone-naive timestamp, DATE, millisecond/nanosecond timestamp, decimal/unsigned numeric substitution, binary_as_string, schema override, hive fields or unknown extension columns. Encrypted/external-column files are unsupported. Only local reviewed synthetic artifacts qualify. Initial codec is SNAPPY; record encodings/file version/created_by and reject unsupported codecs. Key-value metadata is empty or limited to ddip.generator_config_sha256 (64 lowercase hex) and ddip.logical_schema (typed-ten-v1); never copy unknown keys/values into diagnostics. No sensitive URLs, bodies, headers, identities or extension bags are allowed. Arbitrary real logs do not become safe through discarded fields.

| Condition | Required disposition |
| --- | --- |
| Unknown field/metadata, unapproved service/route string or false/operational origin | Abort intake, even if another row field is invalid |
| Bad magic/footer/UTF8 decoding, corrupt page, wrong physical schema, inconsistent row metadata, unsupported schema integer !=1 | Abort complete batch; no successful prefix |
| Null field, out-of-range status/duration, invalid allowed-type environment/method/UUID, out-of-range/nonfinite timestamp | REJECTED with E2 ordered FIELD_NULL / FIELD_VALUE / TIMESTAMP_INVALID as applicable; safe location/reasons only |
| Recognizable rejected identity repeated validly or invalidly | EVENT_IDENTITY_CONFLICT before output reservation, as E2 |
| Missing selected artifact / unchanged-file I/O / demonstrated change | Availability or integrity / operation I/O / CHANGED respectively; retain E2 precedence, never infer mutation from an OSError alone |

Parquet has no missing-key distinction: JSONL explicit null equals a nullable Parquet value and gives FIELD_NULL. A missing JSON key gives FIELD_MISSING and is tested separately. JSON syntax errors, duplicate keys, invalid UTF-8 text, fractional/quoted numeric tokens and alternate timestamp spellings are format-specific failure/rejection tests; a schema-wrong Parquet field is not their equivalent. Do not compare those reason counts as parity. A decoder exception does not become a representable row rejection. Null identity clears both recognized fields; malformed identity is never salvaged. Other rejected values are discarded, not serialized into staging or quarantine.

### Safe diagnostics and precedence

New v2 failures retain E2's fixed code/message vocabulary and availability/integrity/I/O-versus-observed-change distinctions. Additional exact messages are PARQUET_INVALID: "Parquet input evidence is invalid."; PROVENANCE_UNSUPPORTED: "Required physical provenance is unavailable."; BENCHMARK_INVALID: "Benchmark evidence is invalid."; PREFLIGHT_BLOCKED: "The benchmark plan did not pass resource admission." The last is an execution/admission diagnostic as well as the explicitly recorded preflight state; it never denotes successful processing. Early argument failures have exit 2, all operation failures exit 1.

The v2 product error envelope has exactly the E2 status/product_id/origin/label/code/message fields plus product_contract_version=2, processor_revision=2 and input_format (null only before a valid format was selected); optional conflict_summary remains counts-only after a complete conflict scan. Benchmark CLI summaries use a separate bounded envelope {status,origin,label,config_fingerprint,state,code,message}, null code/message on success and null fingerprint only before config admission. Never include raw names/values, paths, SQL, stderr or exception strings. Restricted runtime logs are not public diagnostics.

Order is arguments/versions/paths and bounded metadata; structural schema/privacy admission; complete typed validation; recognizable conflicts; destination state; output schemas/values/reconciliation; final integrity rechecks. Unknown fields/metadata or unapproved origin/service/route cannot enter ordinary rejection evidence. Initial missing intake is INPUT_UNAVAILABLE, initial missing batch BATCH_UNAVAILABLE, malformed/incomplete registered evidence BATCH_INVALID/BATCH_INCOMPLETE; selected product dependencies map to DEPENDENCY_UNAVAILABLE/INVALID. Existing product corruption/I/O is PUBLICATION_INVALID; write/finalization I/O is PUBLICATION_FAILED. Disappearance/replacement/content/directory changes observed after successful capture retain their corresponding INPUT_CHANGED/DEPENDENCY_CHANGED/PUBLICATION_CHANGED (service snapshot uses its unchanged SNAPSHOT classes). No broad exception mapping may erase already observed change evidence. Missing completion is never empty success.

## Physical provenance and capability gate

Physical location is (batch_fingerprint, artifact_id, input_file_sha256, record_ordinal), plus Parquet zero-based row_group_ordinal and row_ordinal within that group. File path is obtained only through the exact registered artifact descriptor. For JSONL preserve one-based line_number, zero-based byte_offset and physical_byte_length including ending; record_ordinal=line_number−1. Business identity remains (service_id,event_id), unrelated to physical location.

DuckDB's [documented file_row_number option](https://duckdb.org/docs/current/data/parquet/overview#parameters) supplies a file ordinal; [pinned 1.4.5 source](https://github.com/duckdb/duckdb/blob/v1.4.5/extension/parquet/reader/row_number_column_reader.cpp) initializes it from preceding group row counts. The derivation is p=file_row_number; choose the unique nonempty footer interval [start_g,start_g+n_g), then row_group_ordinal=g and row_ordinal=p−start_g. At Stage A this was a source-based feasibility inference. Small installed-runtime evidence is recorded below; target capacity remains unproved. Do not assume a newer row-group virtual column exists in 1.4.5.

Indexer protocol:

1. Capture file/root/parent identity, exact inventory, size/mtime/link count and SHA-256. Bound the footer from its final length/magic before invoking DuckDB. Inspect every physical schema and bounded footer group/column metadata; no glob or merged schema.
2. Open one exact-file DuckDB scan with file_row_number=true, hive_partitioning=false, union_by_name=false, UTC, two threads, 512 MiB, no disk spilling/extensions. Select all ten values plus ordinal; convert supported timestamps to integer microseconds only after physical schema validation. Detect invalid/nonfinite values without Python datetime conversion. No floating-point timestamp conversion.
3. Execute one explicit ORDER BY file_row_number cursor per file. Do not derive positions from cursor arrival order. Validate complete p=0..record_count−1 coverage, group membership and all typed/privacy rules. Safe fetches are <=1,024 records / 2 MiB, with SQL pre-fetch string-byte admission and actual encoded size checks. One bounded row-group decoded-size cap does not imply the whole ORDER BY is constant-memory; no-spill exhaustion aborts.
4. Emit approved tagged records incrementally; recheck hashes, identities, directory membership and all observed evidence after scan, staging, Spark actions and verification. A failure after row k invalidates the whole attempt. Never expose a successful prefix.

The plan is a single full-file scan/sort, not one rescan per fetch chunk or per row group. Footer grouping is bounded; it does not require direct row-group-only reads or claim that filter pushdown is guaranteed. A supported proven row-group-only API could replace this only after review, with identical ordinals. Spark never supplies source row numbers.

Example footer group counts [2,0,3] gives p=0,1 -> (group 0,row 0,1), and p=2,3,4 -> (group 2,row 0,1,2). Empty group 1 contributes no occurrence. File A/p=0 and file B/p=0 are different locations. Rewriting/reordering an equivalent file changes its physical hash/positions and registered binding, not the logical multiset.

Stage B checkpoint A must use hand-defined small rows, several files/groups (including empty files/groups where the writer supports them), repeated reads, one/two decoder threads, scan filters/order changes and Spark layouts. Require exact ordinal-to-value mapping, contiguous coverage, footer/actual agreement, strict annotations, pre-epoch/year-0001/year-9999 microseconds, corruption/unsupported-schema/mutation/I/O behavior, bounded fetches and plan inspection. Filtered capability probes retain original ordinals and prove their expected subset; only unfiltered full scans establish publication coverage. Physically impossible-to-emit empty-group fixtures may use reviewed tiny binary fixtures; do not claim executed coverage from an absent fixture. Prove the decoder's position semantics and DuckDB COPY writer timestamp behavior at runtime, then independently read all ten literal values through Spark under local[1]/local[2]; Spark value parity does not establish physical numbering. If exact positions, allowed schema or bounded materialization cannot be demonstrated using installed libraries, STOP for owner approval of a concrete adapter/dependency alternative. No new Arrow/JVM reader or broad fallback is implicitly authorized.

### New occurrence and staging schemas

reliability_occurrences_v2 has exactly 20 ordered columns. Types use E2 physical representations; timestamps remain UTC INT64 MICROS and reason_codes the standard three-level UTF8 list. Grain/key is (batch_fingerprint,artifact_id,record_ordinal).

| Position | Column | Spark type | Required |
| --- | --- | --- | --- |
| 1 | contract_version | integer | Yes; exactly 2 |
| 2 | batch_fingerprint | string | Yes |
| 3 | source_id | string | Yes |
| 4 | batch_id | string | Yes |
| 5 | ingestion_id | string | Yes |
| 6 | artifact_id | string | Yes |
| 7 | input_file_sha256 | string | Yes |
| 8 | input_format | string | Yes; JSONL or PARQUET |
| 9 | record_ordinal | long | Yes; zero-based |
| 10 | line_number | long | JSONL only |
| 11 | byte_offset | long | JSONL only |
| 12 | physical_byte_length | integer | JSONL only |
| 13 | row_group_ordinal | integer | PARQUET only |
| 14 | row_ordinal | long | PARQUET only |
| 15 | received_at | timestamp | Yes |
| 16 | disposition | string | Yes; ACCEPTED/DUPLICATE/REJECTED |
| 17 | service_id | string | Recognized identity only |
| 18 | event_id | string | Recognized identity only |
| 19 | semantic_sha256 | string | Valid occurrences only |
| 20 | reason_codes | `array<string>` | Yes; E2 fixed ordered unique vocabulary |

Inapplicable format positions must be null, never zero sentinels. All applicable positions are required and reconcile to raw evidence. Existing requiredness/identity/reason rules remain. Canonical ACCEPTED representative is minimum (source_id,batch_id,batch_fingerprint,artifact_id,record_ordinal); it ranks observed positions only. For JSONL this preserves line order. Every position has one disposition; P=V+R=U+D+R, ACCEPTED=U, DUPLICATE=D, REJECTED=R, delivery counts/receipts reconcile exactly.

Events/requests retain the exact E2 21/17 ordered columns, contract_version=1, business grain, reference fields and requiredness. Occurrence-v1 remains 16 columns for v1 artifacts only. New independent verification checks every physical schema and full null-safe bidirectional equality, raw-to-event lineage and group arithmetic, including independent rank qualification; no merely matching totals.

Private staging version 2 uses the E2 exact exclusive attempt layout and completed inventory pattern. stage.json has exactly staging_version=2, product_fingerprint and files. Each file descriptor has exactly file,byte_length,sha256,row_count,batch_fingerprint,artifact_id, ordered by batch/artifact. Each record has exactly these 15 fields in this documented order: batch_fingerprint, artifact_id, input_format, record_ordinal, line_number, byte_offset, physical_byte_length, row_group_ordinal, row_ordinal, state, recognized_service_id, recognized_event_id, semantic_sha256, reason_codes, event. event is null for rejection or the ten-field transport object with event_time_epoch_us. No raw rejected values, unexpected keys or unknown metadata enter staging. Preserve original captures through final stage.json verification. Spark validates all tags/positions before deriving events. Chunked preparation is local Python/DuckDB work, not distributed Spark parsing.

## Product identity, verification and compatibility

inputs.json has exactly: manifest_version=2, product_contract_version=2, processor_revision=2, product_id=application-reliability, origin=synthetic, input_format, input_contract_versions={registered_batch:2,request_event:1,service_reference:1}, dataset_contract_versions={reliability_events_v1:1,reliability_occurrences_v2:2,reliability_requests_5m_v1:1}, engine={spark:4.0.2}, batches, reference, semantic_config. batches/reference retain E2's exact selection objects and completion {byte_length,sha256} descriptors, sorted/deduplicated selection order. They bind manifests transitively through batch/snapshot fingerprints. No copied full input history or supplied CSV bypass.

semantic_config has exactly the E2 semantic_config keys/values except admission_policy_revision=2 and occurrence=format-physical-v2, plus raw_decoder equal to the selected profile decoder above. Keep event identity, typed-ten equality, recognizable-invalid conflict policy, snapshot reference, rejection vocabulary, UTC-floor window and percentile algorithm/probability/accuracy/rank policy unchanged. Product fingerprint is SHA-256 of exact canonical inputs.json bytes, no newline. Logical checksums are independently verified comparison evidence, not replacements for physical dependencies or fields in product identity.

Completion version 2 retains E2's exact completion field names, with the v2 manifest identity and output keys (events_v1, occurrences_v2, requests_5m_v1). It records actual inventory/hashes, occurrence/output counts and representable completed_at. Inspect all files, complete schemas/values/keys, raw correspondence, reference joins, exact metrics and qualifying stored p95. Write/verify complete.tmp, recheck all dependencies/output, rename last. No post-failure completion or automatic repair. Keep old completed roots usable after new failures. Replay fully verifies stored exact fields and candidate ranks without fresh approximate equality or writes/mtime changes.

New reader reports remain requests_5m, input_coverage and reference_coverage, using E2's exact row projections, filters, bounds, decimal strings and pagination. New envelope adds exactly product_contract_version=2, processor_revision=2 and input_format to the E2 envelope. No public occurrence/event/quarantine SQL or export/API is added. The year-10000 sentinel remains valid only as exclusive requests_5m end, compared through integer Gregorian epoch microseconds; start/stored/event timestamps remain years 0001–9999, maximum 31 days. Opening/reports require retained exact v2 raw batches and E2 service snapshots, 512 MiB/two threads/no spill/extensions, before/after checks, synchronous BUSY guard and no partial response. V1 response envelopes remain unchanged.

Independent raw comparison uses one ordered output occurrence-to-event cursor, matched to independently re-indexed raw files in canonical order, <=1,024 safe rows / 2 MiB. It must not rescan the complete outputs for each chunk. Prove cursor/plan coverage as in E2, now by record_ordinal and format positions. Reuse no Spark producer as a reader oracle. The shared installed decoder is a known limit, not a claim of codec-independent correctness.

## Logical equivalence and canonical comparison identities

Verify three scopes separately:

1. Valid typed occurrence multiset, including identical deliveries and every ten-field value.
2. Unique typed events after exact identity conflict checks/deduplication.
3. Enriched events, exact aggregate/report values and reference/receipt/coverage semantics.

All comparisons use source-independent business identity (service_id,event_id) and full typed equality. Across paired formats, match batch receipt policy, selected reference, valid/rejected counts, duplicate suppression, delivery summaries, statuses/durations, UTC instants, group keys/counts/errors and exact sums. Rates/means use E2 tolerance max(1e-12,abs(expected)*1e-12), finite range checks and exact numerators/denominators. Both p95 candidates must occur in the same logical group's durations and satisfy r=ceil(95N/100), b=max(1,ceil(N/10000)), L=#below+1, H=#at-or-below, displacement=max(L−r,r−H,0)<=b. Never compare approximate values as if equality were required, or average them across groups.

Report parity projects only meaningful comparable columns. requests_5m compares exact keys/counts/sums/context, tolerance-based rate/mean and independent rank qualification rather than p95 equality. reference_coverage rows/totals match exactly for the same selected snapshot. input_coverage matches on source_id/batch_id and compares receipt, file count (under the matched layout), physical/valid/accepted/duplicate/rejected counts, event ranges/delays and reason counts. Its batch_fingerprint, ingestion_id and input_byte_length are format-specific and must differ or may differ; do not demand equality or discard those provenance fields. Product/format/version envelope identities, serialized byte sizes and physical descriptors are also separate evidence, not parity columns.

Hashes use canonical_bytes on arrays with integers/booleans/nulls as JSON tokens. A valid tuple is exactly [schema_version,event_id,service_id,environment,event_time_epoch_us,method,route_id,status_code,duration_ms,is_synthetic]. Negative timestamp integers are allowed here. Sort tuple byte strings lexicographically as unsigned UTF-8 bytes, not locale or SQLite text collation; repeat equal bytes according to multiplicity. Hash these streams incrementally:

~~~text
valid:    UTF8("ddip-reliability-valid-multiset-v1\n") + (tuple_bytes + LF) for every valid delivery
unique:   UTF8("ddip-reliability-unique-multiset-v1\n") + (tuple_bytes + LF) once per unique event
rejected: UTF8("ddip-reliability-rejected-multiset-v1\n") + (safe_tuple_bytes + LF) for each rejection
~~~

A safe rejection tuple is [recognized_service_id_or_null,recognized_event_id_or_null,ordered_reason_codes]. It contains no other invalid values. Sort/repeat it the same way. Recognized IDs remain restricted evidence; public compact results contain digests/counts only. This checksum deliberately does not assert equality of discarded invalid payloads. Exact typed-invalid parity additionally checks the five prescribed defect classes/field choices, fixed expected counts and equivalent disposition/reason results. Format-only parser tests are excluded from the common rejected checksum claim and labeled separately.

The logical comparison evidence object has exactly comparison_version=1, generator_config_fingerprint (hex or null for hand fixtures), physical_occurrences, valid_occurrences, unique_valid_events, identical_duplicate_occurrences, rejected_occurrences, valid_multiset_sha256, unique_multiset_sha256, rejected_multiset_sha256. Its own identity is SHA-256 of canonical bytes. It excludes physical hashes, source roots, row positions, registered IDs, completion times and execution layouts; paired physical selections are separately recorded. Digest agreement supplements full typed/identity/group comparison, never substitutes for it.

## Generator v1: exact deterministic definition

Supported large N values are exactly 100000 and 1000000; small golden/test fixtures are separate. seed is integer 20261019. For index i and field f, define H(i,f)=unsigned-big-endian(first eight bytes(SHA256(UTF8(decimal(seed)+"/"+decimal(i)+"/"+f)))). Decimal uses no sign/leading zero for nonnegative indices, no spaces/newline. Field names are exactly environment, event_time, method, route, status_band, status_code, duration_band, duration_value. Hash labels separate assignments; this is deterministic pseudorandom mapping, not a statistical independence proof.

Unique event indices are i=0..N−1. UUID-shaped event identity uses first 16 SHA-256 bytes of UTF8(`ddip-reliability-generator-v1/20261019/event/<i>`); set byte 6=(byte6 & 15)|128 and byte 8=(byte8 & 63)|128, then lowercase hex 8-4-4-4-12. This is a custom version-8-shaped identifier, not UUIDv5 or a wall-clock UUID. Detect any full UUID or (service,event) collision and abort generation; do not resolve collisions. Invalid additions use the separate domain invalid and index j below. Actual generated IDs must never collide across those domains.

| Field | Exact base mapping |
| --- | --- |
| schema_version / is_synthetic | 1 / true |
| service_id | If i mod 10 < 6, svc00. Otherwise c=4*floor(i/10)+(i mod 10−6), service number=1+(c mod 9), formatted svc01..svc09 |
| environment | `[dev,test,production][H(i,environment) mod 3]` |
| event_time | 1790812800000000 + H(i,event_time) mod 86400000000 epoch microseconds; 2026-10-01 UTC, exact microseconds |
| method | `[GET,HEAD,POST,PUT,PATCH,DELETE,OPTIONS][H(i,method) mod 7]` |
| route_id | `<service_id>-r<two-digit H(i,route) mod 20>`; exactly 200 possible technical labels |
| status_code | t=H(i,status_band) mod 100. t<90: [200,201,204]; 90<=t<95: [400,404,429]; otherwise [500,503,599]. Choose array element H(i,status_code) mod 3 |
| duration_ms | t=H(i,duration_band) mod 100. t<80: 1+H(i,duration_value) mod 100; 80<=t<99: 101+H(i,duration_value) mod 900; otherwise 1001+H(i,duration_value) mod 59000 |

The original proposed service/status modular rules have joint counts per 100 indices hot=(54 2xx,5 4xx,1 5xx), others=(36,0,4). Thus hot 5xx rate=1/60, other rate=4/40; claiming independent 5% rates would be false. The accepted generator keeps exact service quotas and replaces status classes with the separate status_band hash. Consequently 90/5/5 and 80/19/1 are mapping probabilities, not exact observed proportions. Stage B records global/per-service status and duration-band histograms, including actual joint distribution. No p-value or independence claim follows from separate labels.

Duplicates: D=N/50. For k=0..D−1 copy all ten fields of unique i=50*k+(k mod 50), including its identity, with only delivery provenance changed. These indices are distinct and in range. There is exactly one added identical delivery for each selected identity.

Invalid additions: R=N/100, j=0..R−1. Construct base fields using the above mappings at i=N+j, substitute UUID from `invalid/<j>`, then apply exactly one defect j mod 5: 0 -> event_time=null; 1 -> status_code=600; 2 -> duration_ms=−1; 3 -> environment="invalid"; 4 -> event_id=null. Keep all ten JSON keys, including explicit nulls. Both formats yield FIELD_NULL, FIELD_VALUE, FIELD_VALUE, FIELD_VALUE, FIELD_NULL respectively; null event IDs have no recognizable identity. No invalid recognized identity is reused. Missing JSON keys, syntax errors, nonfinite/quoted/fractional tokens, identity conflicts and sensitive fields are separate failure workloads, not successful baseline records.

### Batches, layouts, reference and variants

Use source_id=bench-source, batches b00 and b01, sorted artifact IDs part-00..part-07 in each. b00 contains unique i with i mod 100<90, in ascending i; b01 contains remaining unique indices ascending, then duplicates ascending k, then invalid additions ascending j. Receipts are exactly 2026-10-02T00:05:00.000000Z and 2026-10-03T00:05:00.000000Z. All event times remain the previous fixed day. Selecting b00 alone then both is a separately labeled explicit retained-history experiment; the primary paired benchmark selects both. No prior-relative lateness inference or implicit history selection.

Partition each batch's logical physical sequence into eight files using cuts floor(P_batch*f/8), f=0..8. Preserve record order within each file for both formats. Baseline file rows: b00=11250 each, b01=1625 each; target b00=112500 each, b01=16250 each. Total 16 files, eight per batch. JSONL emits canonical object bytes with all ten keys and UTC six-digit Z timestamps, LF after every record, no BOM. Parquet preparation uses exact OPTIONAL schema, DuckDB 1.4.5, UTC, SNAPPY and requested ROW_GROUP_SIZE=8192. Record actual footer groups, encodings, writer/version, compression and byte lengths; requested grouping is not position evidence. No directory partitions or added ordinal column in the raw ten-field schema.

Preparation iterates logical records in bounded chunks, never a whole Python history. For Parquet, fill one exact-file in-memory DuckDB table in <=1,024-row/2 MiB chunks with a private ordinal, COPY the ten fields ordered by that ordinal, then release it. Construct timestamps with integer make_timestamptz(epoch_us), never double seconds. The [documented integer constructor](https://duckdb.org/docs/lts/sql/functions/timestamptz#make_timestamptzmicroseconds) is another checkpoint-A runtime gate. File-at-a-time preparation uses the same 512 MiB/no-spill SQL budget. Preparation/COPY cost is outside the timed registered-input pipeline and recorded separately. PyArrow or a SQLite-loading extension is not required/installed implicitly.

Reference fixture services-v1 uses unchanged service_reference publication: reference_id=bench-services, revision=r1, maintainer_id=example-team, E2 vocabulary v1. CSV rows in ASCII service order: svc00..svc08 plus svc-unused; names are Service 00..Service 08 and Unused Service. owner_team is null for svc01, otherwise example-platform for even service numbers and example-service for odd; service_tier is null for svc02, critical for svc00, standard for remaining numbered services. svc-unused uses example-service/experimental. Omit svc09 exactly. No reference field changes business metrics. Snapshot creation time/manifest/completion bytes are frozen once per paired experiment and explicitly selected.

Base unique service counts: svc00=0.6N; let q=floor(0.4N/9), remainder=4. svc01..svc04 each q+1, svc05..svc09 each q. Therefore missing-reference unique events are 4444 baseline / 44444 target; matched are 95556 / 955556. Duplicate missing-service deliveries are 86 / 886, so missing-reference valid deliveries are 4530 / 45330. These counts follow a 450-k duplicate-index period and arithmetic, not generated large datasets. Coverage uses unique events, not these delivery counts. Reference service count=10, used matched services=9, missing services=1, unused references=1; two used matched services lack an optional attribute.

Variants are separate generator configurations/physical selections:

- hot: change service quota to 90% svc00 (i mod 10<9), otherwise 1+(floor(i/10) mod 9); constrain time to 12:00 UTC plus H(i,event_time) mod 300000000. Derive route from the changed service. Duplicate/invalid policies stay the same. Do not reuse base coverage quotas.
- equal: base mapping except every valid duration=17. Invalid negative-duration additions still apply; singleton/all-equal p95 must be 17.
- conflict, malformed-json, invalid-utf8, unsafe-field and corrupt-parquet: small separately labeled failure fixtures only; expected abort/rejection classification, never successful benchmark throughput.

Config object has exactly generator_version=1, seed=20261019, unique_event_count=N, variant=base|hot|equal, batch_policy=two-receipts-v1, file_policy=eight-per-batch-v1, logical_schema=typed-ten-v1, reference_fixture=services-v1, field_policy=sha256-mixed-quota-v1, identity_policy=sha256-uuid8-v1. Its canonical SHA-256 is generator_config_fingerprint and the benchmark profile logical_dataset_id. Physical layout/compression/format selections have a separate benchmark configuration identity. Hash collisions or conflicting config bindings abort; no truncation-based identity.

| Successful base workload | Baseline | Conditional target |
| --- | --- | --- |
| P physical | 103000 | 1030000 |
| V valid deliveries | 102000 | 1020000 |
| U unique / ACCEPTED | 100000 | 1000000 |
| D identical DUPLICATE | 2000 | 20000 |
| R REJECTED | 1000 | 10000 |
| Each prescribed invalid class | 200 | 2000 |
| b00 / b01 physical | 90000 / 13000 | 900000 / 130000 |
| File count | 16 | 16 |

Wire JSONL line size has a conservative 320-byte bound for these fixed-length tokens/values, including LF; prove it in small generator tests. Thus target raw JSONL upper estimate 329600000 bytes, safely below 1 GiB, with largest file <=36000000 bytes. These are bounds, not measured sizes. Exact JSONL bytes are sum(len(canonical_event_object)+1); Parquet size is writer-dependent. Actual file sizes/hashes, multiset digests, status/error/duration/group histograms and per-window totals MUST be computed and independently checked in Stage B. They are intentionally not fabricated here. For any service/window, exact expected count is the sum of indicator functions of the mappings above; exact sums/errors use the same membership with independently validated field values in the oracle, not predeclared generator totals.

### Literal vectors and hand-authored golden G1

For UTF8 `20261019/0/environment`, SHA-256 is 6bb622d6efc26bda17fd24890f0196387a66d5a182180316689982faa8f2c8e9; first-eight-byte integer is 7761429314360470490 (mod 3 = 2). Generator event index 0's canonical typed tuple is:

~~~json
[1,"f3869488-a723-8658-870d-1e99b0647066","svc00","production",1790849532459480,"PUT","svc00-r11",204,68,true]
~~~

Its exact no-newline tuple SHA-256 is 06d4654f9c14ee059e9f0f5e0de0a146c9d111d492b1049955f489e4266f5820. Base N=100000 generator config canonical bytes are:

~~~json
{"batch_policy":"two-receipts-v1","field_policy":"sha256-mixed-quota-v1","file_policy":"eight-per-batch-v1","generator_version":1,"identity_policy":"sha256-uuid8-v1","logical_schema":"typed-ten-v1","reference_fixture":"services-v1","seed":20261019,"unique_event_count":100000,"variant":"base"}
~~~

Config SHA-256 is 5ac37e3b8e4609b93f5c2c37a79c29d22fdfeb08762a1a884d286136ada63545. Event 1 identity is 806bd719-6411-85e7-b7cc-92d45d6f1085, duration 82, method GET and route svc00-r03. Tests must assert literals, not derive expectations through the generator under test.

G1 is independent hand-authored evidence. Let u(n) be literal UUID `00000000-0000-0000-0000-` followed by n in 12 decimal digits. All rows have schema_version=1 and is_synthetic=true. Define the following complete business tuples by table; field order is the canonical typed order above. Repeat A once, then five invalid additions.

| Row | event_id | service / environment | event_time epoch us | method / route | status / duration |
| --- | --- | --- | --- | --- | --- |
| A | u(1) | svc00 / test | 1790812800000000 | GET / svc00-r00 | 200 / 10 |
| B | u(2) | svc00 / test | 1790812800000001 | GET / svc00-r00 | 500 / 20 |
| C | u(1) | svc01 / test | 1790813100000000 | HEAD / svc01-r00 | 404 / 7 |
| D | u(1) | svc09 / production | −1 | GET / svc09-r00 | 503 / 0 |

Invalid rows copy A except IDs u(101)..u(104) for the first four and null for the fifth. Apply respectively null event_time, status 600, duration −1, environment invalid, null event_id. Sequence is A,B,C,D,A,I0,I1,I2,I3,I4, one batch/one artifact for the tiny fixture; LF and matching Parquet records. Use matched svc00 with all attributes, matched svc01 with both optional attributes null, omit svc09, and add unused svc-unused. Freeze a small separate service snapshot; do not use this smaller reference as the large generator fixture.

Expected G1 P/V/U/D/R=10/5/4/1/5. Rejections are two FIELD_NULL and three FIELD_VALUE. Four invalid identities are recognizable but unique; the fifth has no recognized identity. Groups: svc09/production/1969-12-31T23:55 -> count 1/errors 1/sum 0/mean 0/p95 0; svc00/test/2026-10-01T00:00 -> 2/1/30/15 with p95 10 or 20 qualifying; svc01/test/00:05 -> 1/0/7/7/p95 7. Global count=4, errors=2, sum=37. Coverage=3 matched events, one missing; two used matched services, one missing service, one matched service missing optional attributes, one unused key. A's valid_delivery_count=2; other events=1. No metric counts the duplicate or rejection.

| G1 stream | Exact byte length | SHA-256 |
| --- | --- | --- |
| valid (A,B,C,D,A, byte-sorted) | 551 | 84a1b3d7acfcc43ee055a4369e8681e6f018a577a6e90468fed0cff8d34094d7 |
| unique (A,B,C,D, byte-sorted) | 447 | c0606b06d05921f88116595d95f3ce944842048d7bdc8e5dd4dfe3e32f93d909 |
| rejected safe tuples, byte-sorted | 324 | d399e551363954f697c6a020c2cef84aeeb829dee2e5fa4412a3cf0638d9c210 |

The empty valid stream is just its 35-byte header, SHA-256 b861003522d62b32471e54c2d0f3f251e34cb9a395b575ed39bf6b31836e541a. Header length is subject to the literal consistency check below. These vectors use only standard-library calculations in Stage A, not application or Parquet execution.

## Independent bounded correctness oracle

Propose reliability_benchmark_oracle, with no generator, producer, Spark or E2 aggregate/validation function imports. Implement independent strict JSON/csv/calendar/type rules from this contract and literal tests. Read retained exact raw files, not private staging or generator expected outputs. Verify raw hashes/guards and service snapshot identity; independently parse the captured reference CSV/vocabulary for joins. Reuse a canonical JSON specification, not producer expected-value calculations.

For Parquet, a separate oracle SQL adapter inspects physical schemas/footer and decodes using installed DuckDB 1.4.5 with explicit ordinals. This shares a codec, not production validation/dedup/group/rank logic. Small literal files and independent Spark value reads in checkpoint A are mandatory; they do not constitute two independent physical codecs. If that confidence boundary is insufficient, a second decoder requires owner approval, not an undisclosed dependency.

Use one owned SQLite scratch database, BINARY keys/BLOB tuple bytes, cache_size=−65536 (64 MiB), temp_store=FILE, mmap_size=0, explicit transactions <=1,024 rows, standard journaling and no extensions. The accepted scratch cap is 8 GiB including journal/temp/indexes. Configure an isolated owned SQLite temporary location before opening the oracle; verify the installed SQLite build honors it. Journal/sort/index temporary space, including unlinked temporary files, must be conservatively accounted through the isolated volume/process observations, not just a visible directory listing. If a bounded attribution/model cannot be established, block the target. Python working-state allowance is 256 MiB, one safe chunk <=2 MiB; Parquet decoding adds 512 MiB SQL, so conservatively budget 1 GiB for the oracle process. These are admission/observation budgets, not hard OS RSS enforcement. Stop if measured scratch/RSS breaches the recorded budget; no in-memory fallback or sampled correctness.

Tables hold: every safe physical occurrence keyed by selected batch/artifact/ordinal; recognized identities with valid/invalid/variant counts; unique full typed events and receipt summaries; expected/output group rows. Store full typed tuple bytes for valid equality, never only hashes. Reject valid variants or any repeated recognizable invalid identity. Stream chunks and maintain P/V/U/D/R, representatives and receipt ranges; verify all E2 equations and exact per-file coverage. No output metadata is ground truth: recompute counts, service/reference membership and every expected group from raw evidence.

Create an event index on (service_id,environment,window_us,duration_ms,event_id), where window_us uses Python integer floor division, independently of Spark SQL. Stream that index group by group with constant Python counters. A prior grouped count supplies N/r. As sorted values pass, capture the exact nearest-rank value and ties; count below/at-or-below each stored candidate and require observed membership and the fixed displacement budget. No group duration list or all-groups histogram is needed. Exact integer sums are Python integers; explicitly check DECIMAL(38,0) / signed-count ranges before comparisons. At 2M*3,600,000, duration sums also fit SQLite signed 64-bit, but validate bounds rather than relying on unchecked SQL overflow.

Import stored output projections through bounded independent DuckDB cursors into SQLite, after exact metadata/hash/physical-schema checks. Ordered merge/SQL anti-joins prove full expected/actual key sets, reference values, delivery counts, exact metrics and row counts. Rates/means use exact numerators/denominators and fixed finite tolerance. Check all groups at target size, not a sample. Stream BLOB-sorted tuple tables for the three multiset digests. Report exact p95, L/H/displacement/budget and absolute millisecond error per group in restricted oracle evidence; compact summaries contain group counts/max displacement/error and an evidence digest.

The oracle runs separately after each candidate timed publication and independently for both raw representations. Record its wall/CPU/RSS/disk/index-build time separately, outside pipeline throughput H. Full publication's own independent pending verification stays inside H. No sample is BENCHMARK_PASSED if the oracle fails or cannot complete within approved resources. Preserve original outputs and safe failure evidence.

## Measurement ledger and comparison fairness

| Boundary | Timed scope / relationship |
| --- | --- |
| A preparation | Generator field creation, JSON serialization, Parquet COPY/conversion, layout/file checks and reference setup; outside H |
| B registration | Hashing, complete format decoding/admission, immutable copies and batch completion; outside H. Record decoding/validation child spans only if genuinely instrumented |
| C processing intake | Inside H: reopen/verify frozen selections, then repeat raw indexing/validation and completed safe staging. Repeated reads are real verification work, not removed for speed |
| D normalization | Inside H: explicit staging load, validation, conflicts, dedup, receipt summaries/reference join; finish required actions/materialization before ending span |
| E aggregation | Inside H: exact groups and approximate percentile, materialized with required validation/rank work identified |
| F publication | Inside H: exact-schema writes, physical inspection, Spark read-back/multiset checks, independent pending verification, final rechecks and completion |
| G consumption | Outside H: fresh-process launch/full open, opened-session named report, raw-format SQL comparison, or replay; each separately labeled |
| H verified pipeline | From v2 publish entry with explicit already-registered selections through verified completion rename/read-only summary. Includes its own full dependency/staging/output verification; excludes A/B, Spark process startup and separate benchmark oracle |

The exclusive H ledger keys are dependency_open, staging, normalization, aggregation, output_write, spark_readback, independent_pending_verify, final_rechecks, finalize and overhead. Store integer monotonic elapsed nanoseconds. Their nonoverlapping sum equals H exactly; residual uninstrumented control time is overhead, not silently allocated to a phase. Nested decoding/hash/validation spans are labeled child observations and never added again. Where decoding and validation are interleaved, report their combined parent and null separate duration, rather than infer CPU/I/O time from subtraction. Cached expected frames/actions must not shift E work into F unnoticed; record action groups/materialization. Final read-back/rank verification work is retained.

Also report process_startup, complete process elapsed (including runtime shutdown), separate oracle, independent consumer open and report elapsed. Optional inclusive_total=A+B+H is valid only for nonoverlapping sequential wall spans over the same prepared selections; otherwise report components without a misleading sum. Record Python/JVM CPU and I/O counters independently where supported; elapsed time is not CPU time.

Pipeline throughput is P/(H_ns/1e9) physical source occurrences/s and U/(H_ns/1e9) unique valid events/s. P includes duplicates/rejections once, U excludes them. Failed/incomplete/replayed trials have no fresh-write throughput value. An optional process-inclusive throughput uses its explicitly named longer denominator; never mix it into H comparison summaries. A/B preparation throughput is separate and not called pipeline throughput.

### Required matrix and staged execution

The 100K baseline core matrix: format JSONL/PARQUET × Spark local[1]/local[2], same host, same frozen logical/reference selection and resource budgets, fixed shuffle/default parallelism 8 and four output partitions. Same 16 logical input files and record-to-file allocation. Row groups/compression differ intrinsically and are recorded; bytes are not assumed comparable. Spark consumes safe staging in both cases: this measures format-adapter-to-verified-product pipelines, NOT Spark native JSON versus native Parquet readers.

For each of four cells use one untimed-for-summary warm-up plus five measured fresh publications: 24 retained matrix publications for the 100K baseline. Qualification adds one publication per format under local[1], so the required baseline plan reserves 26 fresh product roots. The fixed query/replay plan below adds samples, not new publications. Producer processes are fresh per trial; warm-up primarily warms filesystem/runtime dependencies, not a guaranteed shared JVM JIT. Alternate format order within pairs: pair 0 JSONL then PARQUET, pair 1 reverse, continuing by parity; alternate master ordering by pair too. Run one producer/verifier/oracle at a time, without concurrent benchmarking. Record nonbenchmark contention and thermal/power mode; invalidating interference follows a documented fixed rule before viewing comparative results. Do not cherry-pick fast samples.

Every fresh trial reserves a fresh approved output root. Frozen batch/reference roots are reused read-only. For each cell, take its first measured publication (preselected, never the fastest) as the pinned replay/query artifact. Run one replay warm-up plus five measured read-only replays per cell: 24 replay samples total, without new product roots. Replays are separate labeled full-verification trials. Retain warm-ups, failed attempts and all raw samples; no deleting completed output to force another write. Five measured trials produce median/min/max/range, not a latency distribution p95. Optional local[4], hot/equal variants and compression experiments require their own complete paired cells and updated preflight; they are not core acceptance prerequisites and cannot be silently pooled. No multi-process workers or multi-host claim (E4b remains separate).

Stage the experiment: small capability/parity/oracle tests -> bounded 4,096-event pilot -> numeric baseline preflight -> one 103,000-record correctness qualification -> full baseline paired matrix -> numeric target preflight -> STOP for explicit owner go/no-go -> conditional minimum target scale validation. Qualification publications also consume storage and are included in the plan, even if excluded from timing summaries. If a matrix is too expensive, stop with partial observations and explicit unmet cells; do not reduce five samples and call it an established comparison. A completed baseline remains useful if target is PREFLIGHT_BLOCKED. Whole-issue success distinguishes complete baseline evidence from a conditional blocked target.

### Target execution amendment

The minimum target plan is one fresh independently verified publication per format under the same predeclared Spark master, preferably local[2] if admitted. Require matched logical records/reference, complete raw/typed/aggregate correctness, independent p95 ranks, resource/end-to-end observations, representative named reports and immutable replay. This is scale validation, not a repeated comparative benchmark. The minimum plan retains two fresh publications, their two oracle results, at least one replay per format and one execution of each of the three named reports per format (ten top-level samples). It does not require the baseline’s repeated raw-query study. Every actual sample remains recorded. A full repeated target study is optional and needs separate explicit resource/time approval. It must not replace or reduce the complete 100K matrix.

Numeric target PREFLIGHT_PASS is necessary but not execution authorization. Present actual baseline/machine observations, peak disk/memory, conservative target estimates/runtime interval, SQL/oracle budget conclusions and uncertainties; stop before generating target data and request owner go/no-go. Until approval, target execution is NOT_ATTEMPTED with OWNER_PENDING reason (OWNER_DEFERRED if explicitly deferred). PREFLIGHT_BLOCKED is reserved for failed/unknown numeric checks, independently of owner execution state.

### Fixed query workloads

Downstream reports use fully verified v2 publications (both stored outputs are Parquet). Required query IDs: metrics_one = requests_5m(service_id=svc00,environment=test,start=2026-10-01T12:00:00.000000Z,end=12:05 same date,limit=100,offset=0); metrics_first = all services/environments, full October 1 UTC day, limit=100,offset=0; metrics_later = same, offset=100; input = input_coverage(limit=100,offset=0); reference = reference_coverage(limit=100,offset=0). Capture all exact request parameters, rows/bytes/has_more and returned identity. They are bounded pages, not a claim to return all daily groups. No E2 event catalog, HTTP/export or browser latency is measured.

For each core cell collect one warm-up plus five fresh-process open/report samples and one warm-up plus five report-only samples per query in a pinned already-opened session, with the same parameters and mandatory before/after evidence checks. Distinguish process-start+open+report from open and session.report durations; a session report is not verification-free. Thirty or more independent samples per stated query/config/cache condition are required before reporting a latency distribution p95. Otherwise percentile fields are null with INSUFFICIENT_SAMPLES. Filesystem cache state is unknown/warm unless actually controlled; fresh process is not cold OS cache.

Run raw queries once per format, independent of Spark master: one warm-up plus five measured repetitions of each of two queries (24 raw-query samples). The default baseline plan therefore records 26 publication, 24 replay, 240 consumer and 24 raw-query samples, 314 total, including warm-ups/qualifications. Separate raw-format SQL workloads run against exact independently admitted registered files using fixed DuckDB queries, UTC/two threads/512 MiB/no spill/extensions. JSON uses an explicit ten-field schema and bounded JSON objects; Parquet has every physical schema checked. No raw query path is a substitute for strict intake. Because benchmark common invalid rows have only the five specified typed defects, a fixed full validation predicate can exclude them without coercion ambiguity; parser-specific fixtures are not timed here. raw_count returns P,V,valid-occurrence 5xx count and duration sum (duplicates INCLUDED); raw_service_day returns valid-occurrence five-minute groups for svc00/test over October 1, at most 288 rows. Independently compare these raw-scope answers with the oracle. Bind filters and exact file lists; no user SQL. Record raw admission/hash/validation separately from query-only scans and also an inclusive admission+scan measure. Do not compare these delivery-level answers directly to deduplicated product counts.

Never attribute downstream Parquet report differences to intrinsic raw JSONL/Parquet query performance. Raw-query speed, serial adapter cost, Spark typed processing and end-to-end verification are different claims. Preparation/conversion is outside the primary timed pipeline, with an optional clearly labeled inclusive view.

## Resource policy and supported instrumentation

Existing E2 caps are retained per selected publication, not multiplied into permission to exceed one operation's ceiling:

| Budget | Required enforcement |
| --- | --- |
| JSONL line / raw file | 16,384 content bytes / 256 MiB; no whole-file Python reads |
| Batch/selection | 16 files/batch, 32 batches, 128 selected files, 1 GiB selected raw bytes, 2M physical occurrences |
| Metadata | Profile 64 KiB; batch manifest 128 KiB, batch completion 4 KiB; product inputs 64 KiB, completion 1 MiB |
| Safe staging | 2 KiB/record; 64 KiB stage.json; total 4 GiB; reject a v2 record if added tags exceed its unchanged cap |
| Output | 4 GiB analytical bytes, 1,024 files total, 512 per dataset; <=2M rows per dataset |
| Reader | Two threads / 512 MiB / no spill/extensions; 1,024 records and 2 MiB raw chunks; E2 page/string/response caps unchanged (100 default/1,000 max, 64 KiB pre-fetch strings, 1 MiB encoded result) |
| New Parquet admission | Footer <=1 MiB, <=64 groups/file, <=32,768 rows/group, <=32 MiB total uncompressed column bytes/group, flat exact ten fields |
| New decoded strings | <=16 KiB UTF-8 across all string cells in one physical row before Python fetch; <=2 MiB total chunk; privacy/validity still determines disposition |
| New oracle | SQLite/journal/temp/indexes <=8 GiB, cache 64 MiB, Python allowance 256 MiB, conservative whole oracle-process budget 1 GiB including Parquet DuckDB |
| New experiment metadata | Config 64 KiB, environment 64 KiB, dataset/plan/preflight/summary 1 MiB each; benchmark completion 2 MiB / <=5,000 evidence descriptors; sample 128 KiB; <=200 publication attempts and <=4,096 query samples; compact result metadata <=64 MiB |
| Local producer budget | JVM driver heap 1 GiB, executor setting 1 GiB (same local JVM, not another heap), Python/worker allowance 1 GiB, verifier allowance 512 MiB; conservatively reserve 3 GiB total plus recorded off-heap allowance if observations demand it |

The owner accepted these new bounds in ADR 0009; they are not retroactive E2 changes. Footer/group declared sizes are admission checks, not guarantees about decoder allocations; configured SQL memory can fail first. Reject actual decoded overages/corruption, never truncate or sample. Repeated hashing/decoding/ordering is part of integrity cost. Finite caps do not establish runtime, RSS, disk quota or 1M capacity.

Instrumentation is opt-in on the new revision; no v1 changes or output-semantic settings change. Use perf_counter_ns for monotonic wall spans, process_time_ns/resource.getrusage for Python CPU/high-water marks, and supported OS PID-tree polling (100 ms requested interval) for sampled process CPU/RSS. Normalize macOS ru_maxrss bytes versus Linux KiB. Do not sum individual process maxima as if simultaneous; retain per-process high-water marks and sampled simultaneous tree peak separately. Spark's local executor is the driver JVM; do not double-count it. JVM child CPU/RSS needs PID evidence, not Python RUSAGE_SELF.

Capture OS/release/architecture, physical/logical cores, RAM total/free/available with method, filesystem type and free bytes on every distinct artifact/temp volume; runtime versions, processor/code inventory, CPU/power mode if available, Spark master/heaps/shuffle/parallelism/temp locations by root alias, SQL settings and percentile configuration. Do not dump environments, credentials, usernames or absolute operational paths into compact public evidence.

Use [Spark 4.0.2 event logs/metrics](https://spark.apache.org/docs/4.0.2/monitoring.html) for task/executor CPU/run time, shuffle read/write, memory/disk spill, task/stage attempts/failures and reported GC. Preserve uncompressed event-log segments as restricted evidence, with digest/size; parse supported pinned fields. Summed task GC/run time can overlap and is not wall time. If exposed, jstat/jcmd from the installed JDK may provide vendor-specific heap/GC snapshots; capture tool/version/interval and distinguish observations from configured heap maximum. No new Java agent, APM server or live telemetry service.

Use shutil.disk_usage and platform filesystem information; walk only owned exact trial inventories for raw/staging/output/event-log/SQLite sizes and sample temporary-directory growth. Shared/default temp deltas are not exact attribution: label estimate or isolate Spark local directories per trial and record them. I/O counters use Linux /proc or supported macOS process statistics only when reliable; otherwise null. Storage throughput is NOT_MEASURED unless a separately described bounded local probe runs outside the timed experiment.

Each optional metric has exactly {value,unit,status,method,sample_interval_ms}, status MEASURED|ESTIMATED|UNAVAILABLE|NOT_MEASURED. value is null for the latter two; method is a fixed reviewed token, not a raw exception string. A zero requires measurement, not missing data. Core wall duration/counts/bytes/config/correctness and the resource observations required for preflight are mandatory; missing mandatory data invalidates a performance pass or blocks target preflight. Optional heap/GC/I/O counters may be unavailable if disclosed; no invented values.

## Numeric preflight and state machine

Preflight is a recorded calculation, not permission to exceed admission ceilings. Run a conservative small/pilot estimate before baseline generation/processing, then mandatory observed-baseline preflight BEFORE target generation. Stage A runs neither. All quantities use bytes, nanoseconds and exact integer arithmetic (ceil divisions); no unit-ambiguous GB estimates.

1. Freeze planned cells, repetitions, warm-ups, qualification/failure attempts and retained artifacts. Select one sequential execution plan. Capture current total RAM, available/free RAM, logical/physical CPU information and per-volume free disk. Linux MemAvailable is an estimate with its method; on macOS use a conservative free+speculative-page lower bound, excluding compressed/purgeable memory unless separately justified. If available memory cannot be established, block rather than guess. Require master threads <=logical CPU capacity; optional local[4] also needs a recorded CPU budget.
2. Verify N/P/D/R, batch/file counts and all fixed caps. Before target generation estimate raw/staging/output/metadata/file counts from each format's baseline and the fixed generator layout. Let scale=ceil(P_target/P_baseline)=10. For every disk component X use U_X=max(ceil(2*scale*observed_X), independently established wire/metadata bound when larger). Include baseline failed/warm-up maxima, not just median. Where no trustworthy finite estimate exists, block. Check each U against its operation's cap; conservative over-cap estimates block pending a better proven bound, not an automatic cap increase.
3. Peak disk per volume includes already retained baseline roots; both target raw formats and registered copies; every planned warm-up/qualification/fresh-root output and private stage; every retained oracle database/journal/index; event logs; max simultaneous producer/decoder/Spark spill/temp and verification scratch; compact evidence. Avoid double-counting bytes already reflected in current free disk: report retained bytes separately and estimate additional peak. Do not assume cleanup or compression savings. Require free_bytes >= 2*additional_peak_estimate on EACH volume, not a sum across unrelated disks.
4. Establish a conservative process budget. Producer tree reserve is max(configured 3 GiB,2*observed simultaneous baseline tree peak), adding explicitly observed off-heap/worker excess. Oracle reserve is max(1 GiB,2*observed oracle peak). Sequential scheduling uses max(producer,oracle,consumer) rather than their sum; overlap, if ever approved, requires summation. Require planned reserve <= floor(0.75*total_RAM) AND reserve+ceil(0.25*total_RAM)<=available_RAM. This deliberately conservative available-memory test may block a busy personal machine; remeasure after owner action, never silently relax it. Budget each SQL connection at its unchanged 512 MiB and record concurrent count (one decoder/verifier per active phase).
5. The 512 MiB no-spill verifier is a separate risk gate, not made larger by host RAM. Estimate SQL working-state upper budget as 2*scale*observed baseline attributable peak (including joins/order/group/rank state). Use a measured isolated-process/SQL observation or a reviewed conservative model including allocator overhead; sampled whole Python RSS is only a conservative proxy, not an exact DuckDB allocation. If the bound cannot be established below 512 MiB, mark target PREFLIGHT_BLOCKED with the uncertainty/number. Raw-sort and full-output joins are both covered; do not infer success from low final file size. Likewise account for SQLite indexed sorts and its fixed scratch budget.
6. Estimate target elapsed interval from baseline maxima: [scale*Tmax,2*scale*Tmax] per phase/trial, then sum all planned attempts/oracles/queries plus preparation. This is planning extrapolation, NOT linear-scaling evidence or a deadline. Operator supplies a finite experiment time budget; if the upper estimate exceeds it, block/split the plan through review rather than running silently. Verification reads/sorts/hashes, repeated full opens and oracle work are explicitly included.
7. Record each arithmetic input/formula/limit/comparison and final reasons. Monitor free disk, owned temp sizes and process estimates at phase/chunk boundaries and with 100 ms observations where feasible. Observed budget breach, OOM/no-spill error, task failure or integrity failure aborts that attempt safely. Sampling is not a hard OS quota; remaining overshoot risk is disclosed. Passing preflight is not a guarantee.

Illustrative calculation only: total RAM 16 GiB, available 10 GiB, reserve 3 GiB gives 3<=12 and 3+4<=10, so RAM passes; available 6 GiB would block. Free disk 20 GiB and estimated additional peak 6 GiB passes 20>=12; peak 11 GiB blocks. A projected verifier 600 MiB blocks despite host RAM passing. None of these are measurements of this checkout or permission to run target.

| State | Exact meaning |
| --- | --- |
| PREFLIGHT_PASS | Every required numeric check/data item passes for the exact frozen plan; no workload success implied |
| PREFLIGHT_BLOCKED | At least one admission/resource/measurement/time-budget condition fails or is unknown; do not generate/run target |
| BENCHMARK_FAILED | An attempted sample failed correctness/integrity/resource/execution or required measurement; never included as successful throughput |
| BENCHMARK_PASSED | That attempted sample completed immutably and all independent correctness/integrity and measurement requirements passed |

Summary also records experiment_status COMPLETE|PARTIAL|BLOCKED and exact incomplete cells. A blocked target is not BENCHMARK_PASSED; the baseline may remain complete. Do not manufacture zero-duration failed samples, successful prefixes or target results from preflight arithmetic.

Cancellation uses only owned local trial processes and cooperative phase/chunk boundaries; no scheduler/deadline service. Stop writers before investigating scratch. A completion already renamed may be valid despite a lost/cancelled response; resolve by full explicit verification. Keep registered inputs, service snapshots, completed publications and result evidence immutable. Default retain all private stages/oracle/event logs through evidence review and include them in preflight. Owner may manually remove explicitly identified unreferenced benchmark scratch after preserving required summaries/digests and stopping processes; no automatic deletion, root-wide cleanup or deleting evidence to fit the next run.

## Machine-readable evidence v1

All result formats are strict versioned canonical UTF-8 JSON, no extra keys, nonfinite numbers or raw exception/payload fields. Configuration integers are JSON integers with explicit caps. Observation integer counters/bytes/nanoseconds are canonical decimal strings (signed only where documented); rates, seconds-derived throughput and CPU percentages are finite JSON numbers. Null means unknown/not applicable only where allowed. File identities are {path,byte_length,sha256}, paths relative to the owned evidence root, lengths decimal strings. No absolute machine roots or secrets enter semantic identities.

~~~text
outputs/reliability-benchmarks/v1/config=<config_fingerprint>/attempt=<local-uuid>/
  config.json
  environment.json
  plan.json
  dataset.json
  preflight-baseline.json
  preflight-target.json             # only when attempted
  samples/<six-digit-index>.json
  summary.json
  complete.json                    # complete evidence inventory, written last
  restricted/                      # raw logs, oracle DBs, code/instrument inventories
  batches/                         # v2 JSONL/Parquet roots, exact descriptors in dataset.json
  references/                      # selected unchanged service snapshot
  publications/trial-<index>/      # each owns its immutable product namespace
  staging/                         # private attempt namespaces
  scratch/                         # isolated Spark/oracle work; no auto-cleanup
~~~

The attempt UUID is operational, not config or product identity. --root always selects this exact attempt directory, never a parent/latest discovery. plan exclusively reserves the absent attempt directory under an approved existing parent; later commands require its frozen config/plan. A config can have many attempts, with no overwrites. complete.json for benchmark evidence means its selected experiment plan is terminal and its results/inventory are verified, NOT that all workloads passed: status/blocked fields remain explicit. An interrupted evidence attempt without completion is incomplete, not a fabricated failure/success. Underlying completed products keep their own independent completions. Compact reviewed evidence may later be copied into documentation by the owner; bulk outputs stay ignored.

### Config and environment

config.json has exactly config_version=1, generator_config (exact object above), layout, matrix, repetitions, queries, resources, instrumentation, source, runtime_requirements. config_fingerprint is SHA-256 of exact bytes; the fingerprint itself is not recursively stored in config.json.

| Object | Exact fields and values |
| --- | --- |
| layout | files_per_batch=8, json_encoding=canonical-utf8-lf-v1, parquet_codec=SNAPPY, parquet_row_group_target=8192, physical_profile=parquet-typed-ten-v1 |
| matrix | formats=[JSONL,PARQUET]; baseline masters=[local[1],local[2]] (optional explicit local[4] only); target SCALE_VALIDATION masters has exactly one predeclared local[1] or local[2] for both formats, preferably local[2] when admitted; separately approved target COMPARATIVE_STUDY declares its full cells. shuffle_partitions=8, default_parallelism=8, output_partitions=4 |
| repetitions | Baseline: warmups=1, measured=5, query_measured=5 (>=30 only for separately declared empirical-latency-p95 study), order=alternate-paired-v1. Minimum target SCALE_VALIDATION: warmups=0, measured=1, query_measured=1 with no comparative statistic; a larger target scope requires its own explicitly approved frozen budget |
| queries | Exact ordered ID list [metrics_one,metrics_first,metrics_later,input,reference,raw_count,raw_service_day]; parameter definitions fixed above |
| resources | driver_memory_mib=1024, executor_memory_mib=1024, producer_reserve_bytes=3221225472 (or larger explicitly recorded observed requirement within the same approved host/caps), duckdb_memory_bytes=536870912, duckdb_threads=2, oracle_reserve_bytes=1073741824, oracle_scratch_bytes=8589934592, ram_reserve_percent=25, disk_headroom_factor=2, experiment_time_budget_seconds (positive operator integer), max_publication_attempts=200, max_query_samples=4096 |
| instrumentation | version=1, polling_interval_ms=100, spark_event_log=true, jvm_probe=auto-supported (may report unavailable), spill_policy=spark-measured-duckdb-disabled |
| source | git_head (40-hex), working_tree_clean boolean, code_inventory_sha256, oracle_code_inventory_sha256; bounded participating source/requirements files have sorted relative path/byte/hash inventory in restricted evidence |
| runtime_requirements | python=3.12.15, java_major=21, pyspark=4.0.2, duckdb=1.4.5; actual vendor/patch/SQLite/OS values live in environment.json |

No arbitrary root paths, process IDs, run UUIDs, clocks or log verbosity enter config identity. They are operational observations. Changes to semantic generation, physical layout, settings, code or budgets create another config. Recheck participating source/code bytes before/after trials; changed code invalidates the attempt. Instrumentation parameters affect benchmark config, never product inputs/fingerprint. A measured need for larger host reserve requires a newly frozen plan/preflight; it does not authorize raising SQL/admission caps.

environment.json has exactly environment_version=1, captured_at, os, cpu, memory, filesystems, runtimes, spark, decoder, metric_methods. os={name,release,architecture}; cpu={logical_cores,physical_cores,power_mode}, nullable nonessential values use the metric missing-value object; memory={total_bytes,free_bytes,available_bytes,available_method}; filesystems is a sorted list {root_alias,type,free_bytes,capacity_bytes}; runtimes={python,java_vendor,java_version,pyspark,duckdb,sqlite}; spark is actual matching execution settings plus temp_root_alias; decoder={threads:2,memory_bytes:"536870912",spill_enabled:false,extension_autoload:false,extension_install:false}; metric_methods is a bounded map of the exact supported metric names below to fixed method/status declarations. No host/user/env dump. Actual free memory/disk snapshots are observations outside config identity, but preflight binds this file's digest.

dataset.json has exactly dataset_version=1, generator_config_fingerprint, logical_comparison (full comparison evidence object), formats, reference, distributions. formats has exactly JSONL/PARQUET entries with registered selections, completion descriptors, exact file descriptors including byte/record counts and actual Parquet group/codec/encoding/writer metadata, plus preparation/registration observation references. reference is the selected immutable ID/revision/fingerprint/completion descriptor. distributions contains independently verified counts by service, environment, status class, duration band, batch and group, stored inline only within the 1 MiB budget; otherwise fixed relative descriptor(s) to bounded restricted records. It is not generator output accepted as truth. Before parity is proven, logical_comparison is null and the dataset cannot qualify samples.

### Samples, preflight and summary

Every sample has exactly the following keys; sample_index is unique, ordered, 0–9999, pair_index is a nonnegative integer or null for qualification/failure trials:

| Key | Exact schema |
| --- | --- |
| sample_version / config_fingerprint / environment_sha256 / dataset_sha256 | 1 / selected canonical hashes |
| sample_index / pair_index / format / master / warmup | Integers/null as above; JSONL or PARQUET; approved master (null for raw_query, which has no Spark context); boolean |
| kind / query_id | qualification, fresh_publication, replay, consumer_fresh, consumer_session or raw_query; query ID null except query kinds |
| status / started_at / finished_at | BENCHMARK_PASSED or BENCHMARK_FAILED; exact representable UTC timestamps, no duration inference from wall clock |
| selection | {batches:[exact v2 selected objects],reference:{exact E2 selection},product_fingerprint:hex-or-null,product_root_alias:string-or-null}; no absolute paths |
| counts | null before a complete trusted scan, otherwise {physical_occurrences,valid_occurrences,unique_valid_events,identical_duplicate_occurrences,rejected_occurrences}; decimal strings |
| timing | {process_elapsed_ns,process_startup_ns,pipeline_ns,exclusive_phases_ns,child_spans_ns,oracle_ns,open_ns,report_ns,raw_admission_ns,raw_scan_ns}; nonapplicable values null; exclusive map has the ten ledger keys, child map only decode/hash/validate keys with no additive claim |
| throughput | {physical_per_second,unique_per_second,denominator}; null values for nonfresh/failed trials; denominator pipeline_ns only for primary throughput |
| resources | Fixed metric-name map to metric objects, plus process_inventory descriptor and event_log descriptor/null |
| correctness | {status:PASSED-or-FAILED-or-NOT_RUN,oracle_evidence:descriptor-or-null,valid_multiset_sha256:hex-or-null,unique_multiset_sha256:hex-or-null,rejected_multiset_sha256:hex-or-null,groups_checked:decimal-or-null,max_rank_displacement:decimal-or-null,max_absolute_p95_error_ms:decimal-or-null,replay_unchanged:boolean-or-null} |
| response | null or {query_id,parameter_sha256,publication_fingerprint,rows,encoded_bytes,has_more,result_sha256}; raw queries have publication_fingerprint=null and has_more=false |
| failure | null or {code,message}; approved fixed diagnostic strings only, no identities/paths/exception text |

Fixed metric names are python_cpu_user_ns, python_cpu_system_ns, jvm_cpu_ns, sampled_cpu_percent, python_peak_rss_bytes, jvm_peak_rss_bytes, worker_peak_rss_bytes, sampled_tree_peak_rss_bytes, jvm_heap_used_peak_bytes, duckdb_working_set_peak_bytes, jvm_gc_time_ns, shuffle_read_bytes, shuffle_write_bytes, memory_spill_bytes, disk_spill_bytes, task_failures, raw_bytes, staging_bytes, output_bytes, temporary_peak_bytes, oracle_peak_bytes, verification_read_bytes, process_read_bytes and process_write_bytes. Method/status distinguishes sampled estimates and unavailable metrics. verification_read_bytes counts actual instrumented reads only; otherwise NOT_MEASURED, never file size times an assumed scan count. Query parameters are fixed by query_id; descriptor/hash permits exact reproduction. Numeric result schemas are validated before recording success.

preflight-*.json has exactly preflight_version=1, target_unique_count, config_fingerprint, environment_sha256, baseline_sample_descriptors, planned_attempts, state (PREFLIGHT_PASS/BLOCKED), checks, assumptions, estimated_runtime_lower_ns, estimated_runtime_upper_ns, checked_at. checks is a bounded ordered list of {check_id,input_values,formula_revision,estimate_bytes_or_count,limit_bytes_or_count,comparison,passed,reason_code}; values are decimal strings/null and formula_revision is preflight-v1. Required check IDs: admission, baseline_evidence, cpu, per_volume_disk, ram_total_reserve, ram_available_reserve, verifier_memory, oracle_budget, runtime_budget. Per-volume disk entries carry root_alias. Reason codes are exactly PASS, ADMISSION_LIMIT, BASELINE_INCOMPLETE, CPU_UNAVAILABLE, CPU_BUDGET, DISK_UNAVAILABLE, DISK_HEADROOM, RAM_UNAVAILABLE, RAM_TOTAL_BUDGET, RAM_AVAILABLE_BUDGET, VERIFIER_UNBOUNDED, VERIFIER_BUDGET, ORACLE_BUDGET, RUNTIME_UNKNOWN, RUNTIME_BUDGET, CAPABILITY_UNPROVEN and STALE_EVIDENCE. PASS requires passed=true; all others require false. Missing critical observations yield passed=false, never an estimate invented as zero. assumptions are reviewed fixed tokens for cache/allocator/temp uncertainty, not freeform payloads.

summary.json has exactly summary_version=1, target_scope, target_execution_state, target_execution_reason, origin=synthetic, label="Synthetic request events — not production reliability.", config_fingerprint, dataset_sha256, experiment_status, baseline_status, target_status, sample_inventory, cells, incomplete_cells, limitations, preflight_descriptors. baseline_status is COMPLETE|PARTIAL|FAILED|BLOCKED; target_status also permits NOT_ATTEMPTED. target_scope is SCALE_VALIDATION or COMPARATIVE_STUDY (the latter separately approved); target_execution_state is NOT_ATTEMPTED, OWNER_DEFERRED, BENCHMARK_PASSED or BENCHMARK_FAILED. target_execution_reason is OWNER_PENDING, OWNER_DEFERRED, NUMERIC_BLOCKED, AUTHORIZED_EXECUTION or NOT_APPLICABLE. A passing numeric preflight with pending/deferred approval is never relabeled PREFLIGHT_BLOCKED. One verified sample per format qualifies scale evidence only; five repetitions are required for comparative statistics. Minimum scale-validation completion may be COMPLETE with one valid sample per format; its raw observations remain available, but comparative median/range/latency-p95 summaries are null with INSUFFICIENT_SAMPLES. Each cell has {format,master,kind,query_id,warmup_count,measured_count,passed_count,failed_count,sample_indices,elapsed_ns:{median,min,max,range},physical_per_second:{median,min,max},unique_per_second:{median,min,max},latency_p95_ns,latency_p95_status}. Null nonapplicable statistics; elapsed_ns.median is exactly {numerator,denominator}, with unsigned decimal-string numerator and JSON integer denominator 1 or 2; reduce integral results to denominator 1. For an even sample count use the exact mean of the middle two values, never integer truncation or floating-point nanoseconds. Other elapsed_ns fields are unsigned decimal strings; throughput statistics are finite JSON numbers. Only comparable passed samples with at least five measured repetitions qualify performance summaries; failures remain visible and a failure cannot be excluded to salvage five successes from an unapproved larger trial count. Latency empirical nearest-rank p95 needs >=30 passed samples with stated condition; otherwise null/INSUFFICIENT_SAMPLES. Raw-query cells use master=null; consumer/replay master denotes the producer configuration of the selected artifact, not DuckDB thread count. No cross-format or cross-master pooling.

Benchmark complete.json has exactly completion_version=1, config_fingerprint, summary_sha256, experiment_status, files (sorted relative descriptor list of compact evidence and restricted evidence descriptors), completed_at. Its inventory excludes mutable scratch/process files; evidence artifacts named by descriptors must be closed/immutable before finalization. Registered/product evidence keeps its own exact independent inventories, bound through dataset/sample descriptors. A result/evidence failure uses BENCHMARK_FAILED; existing successful product completion is not deleted or concealed. A partial/blocked plan can have terminal benchmark evidence with that status, never a claimed complete target.

### Full-delivery command templates (implementation pending)

These templates describe the complete accepted delivery. The full plan/run/summarize/verify-results harness is not implemented at the resource-blocked checkpoint below. Only the early resource-screen command and small v2 library/CLI boundaries are available; do not execute these templates as though the full harness exists. They define a narrow local CLI, not a service or worker scheduler. ROOT variables denote approved dedicated roots; use the established existing Python executable and Java 21. --config accepts only canonical 64 KiB config above. Source review, explicit roots and preflight precede any workload. All steps use exclusive owned destinations and fixed queries; no arbitrary SQL/script options.

~~~bash
export PYTHONPATH=processing
export SPARK_LOCAL_IP=127.0.0.1
export TZ=UTC
# Set PYTHON and JAVA_HOME to verified existing Python 3.12.15 / Java 21.
"$PYTHON" -m reliability_benchmark plan --config "$BASE_CONFIG" --root "$BASE_EVIDENCE"
"$PYTHON" -m reliability_benchmark preflight --config "$BASE_CONFIG" --root "$BASE_EVIDENCE" --tier baseline
"$PYTHON" -m reliability_benchmark_generator --config "$BASE_CONFIG" --root "$BASE_EVIDENCE" --tier baseline
"$PYTHON" -m reliability_benchmark prepare-register --config "$BASE_CONFIG" --root "$BASE_EVIDENCE" --tier baseline
"$PYTHON" -m reliability_benchmark_oracle --config "$BASE_CONFIG" --root "$BASE_EVIDENCE" --tier baseline
"$PYTHON" -m reliability_benchmark run --config "$BASE_CONFIG" --root "$BASE_EVIDENCE" --tier baseline
"$PYTHON" -m reliability_benchmark summarize --config "$BASE_CONFIG" --root "$BASE_EVIDENCE"
"$PYTHON" -m reliability_benchmark verify-results --config "$BASE_CONFIG" --root "$BASE_EVIDENCE"
"$PYTHON" -m reliability_benchmark plan --config "$TARGET_CONFIG" --root "$TARGET_EVIDENCE" --baseline-evidence "$BASE_EVIDENCE"
"$PYTHON" -m reliability_benchmark preflight --config "$TARGET_CONFIG" --root "$TARGET_EVIDENCE" --tier target --baseline-evidence "$BASE_EVIDENCE"
# Only after recorded PREFLIGHT_PASS for this exact target plan/config:
"$PYTHON" -m reliability_benchmark_generator --config "$TARGET_CONFIG" --root "$TARGET_EVIDENCE" --tier target
"$PYTHON" -m reliability_benchmark prepare-register --config "$TARGET_CONFIG" --root "$TARGET_EVIDENCE" --tier target
"$PYTHON" -m reliability_benchmark_oracle --config "$TARGET_CONFIG" --root "$TARGET_EVIDENCE" --tier target
"$PYTHON" -m reliability_benchmark run --config "$TARGET_CONFIG" --root "$TARGET_EVIDENCE" --tier target --baseline-evidence "$BASE_EVIDENCE"
"$PYTHON" -m reliability_benchmark summarize --config "$TARGET_CONFIG" --root "$TARGET_EVIDENCE"
"$PYTHON" -m reliability_benchmark verify-results --config "$TARGET_CONFIG" --root "$TARGET_EVIDENCE"
~~~

BASE_CONFIG has unique_event_count=100000; TARGET_CONFIG has 1000000 and a distinct config identity, preserving the same policies. EVIDENCE variables select exact distinct attempt directories. plan.json has exactly plan_version=1, target_scope, target_execution_authorization, config_fingerprint, baseline_evidence (null for baseline; otherwise {config_fingerprint,completion_sha256,summary_sha256}), planned_publication_attempts, planned_query_samples and tier. target_scope is null for baseline, SCALE_VALIDATION or separately approved COMPARATIVE_STUDY for target. target_execution_authorization is null until a separately recorded explicit owner decision; preflight never supplies it. An actual approval object has exactly decision=OWNER_APPROVED, scope=SCALE_VALIDATION|COMPARATIVE_STUDY, config_fingerprint (64 lowercase hex), approved_at (strict representable UTC timestamp), experiment_time_budget_seconds (positive operator integer) and owner_decision_sha256 (digest of the retained explicit owner decision). It records actual authorization; a CLI flag or generated file cannot manufacture that decision. Trial schedules/counts are frozen before measurement; include qualification, warm-ups, replay and controlled failures. Target plan/preflight/run require the exact --baseline-evidence directory and reverify its binding; no implicit latest. Other flags above are mandatory. All commands enforce tier/N and matching config. Generator/prepare/run verify that the corresponding exact-plan preflight passed and that current resource checks still hold; a stale pass is not unconditional admission.

The run command performs qualification, paired repetitions, per-candidate oracle, consumers/raw queries and result checks, failing closed. To finalize, summarize creates summary.json; verify-results checks schemas/inventories/config bindings/sample counts/statistics and underlying product/dependency integrity, then writes benchmark complete.json last. It cannot create unexecuted samples. A later verify-results invocation checks that completion read-only. Baseline completion is therefore available before target plan binds it. The source/config identity includes operator-declared time budget, optional cells and query-study sample count; command flags cannot silently amend the frozen plan.

CLI success is one bounded JSON summary; failure one fixed safe stderr envelope/exit 1 (arguments exit 2), no raw identities/paths. Benchmark success labels every summary with Synthetic request events — not production reliability. Core experiment status is distinct from shell success: preflight blocked exits 1 with its recorded state; a terminal partial summary never exits as a completed benchmark pass.

## Stage B acceptance and implementation checkpoints

All groups below are acceptance requirements; executed evidence is recorded separately. Small deterministic CI cases use unittest, literal rows, strict physical-schema fixtures, fault injection and import-blocking reader subprocesses. Existing CI job names/configuration remain unchanged; normal discovery must never launch a 100K/1M generator, benchmark, hardware preflight or timing threshold.

| Group | Independently testable behavior / required evidence |
| --- | --- |
| F01 — version compatibility | V1 literal hashes/21-16-17 schemas/replay/report envelopes unchanged; v2 rejects incompatible/mixed/version-confused selections; explicit re-registration distinct |
| F02 — Parquet positions | Literal multi-file/group values, empty files and zero-row groups when the pinned writer can emit them, explicit ordinal->value mapping across reads/threads/orders; complete range/key coverage; no arbitrary Spark numbering |
| F03 — physical types/time | Every file/column/order/repetition annotation; reject INT96/NTZ/ms/ns/coercion/unknowns; integer microseconds across epoch/year limits and sentinel query roles |
| F04 — privacy/rejections | Five typed invalid classes have exact common reasons; parser-only cases labeled separately; unknown sensitive schema/KV/values fail intake; no raw payload/diagnostic leakage |
| F05 — generator identity | Literal H/UUID/event/config vectors, exact deterministic counts/index selections, collision abort, 200-route allowlist; quota and service/status joint tables |
| F06 — logical parity | G1 literal ten-field/rejection vectors and three digests, repeated valid deliveries, format reordering independent of physical identity; unique/events/reference/receipt equality |
| F07 — identity/lineage | Cross-service ID reuse, exact variants abort, recognizable invalid repeats abort, deterministic representatives, P/V/U/D/R and full physical bijection |
| F08 — oracle independence | No producer/generator imports, separate literal arithmetic/calendar/reference tests, bounded SQLite chunks/index sort; corrupted output with refreshed hashes fails |
| F09 — groups/p95 | Every key/count/error/sum, rates/means, exact nearest rank/tie interval/membership/displacement, singleton/equal/heavy-tail/>10K and layout variation; qualifying unequal candidates allowed |
| F10 — publication/replay | Exclusive namespace and complete comparisons, schema-bearing empties, completion last, bytes/mtimes replay unchanged, changed selection/reference creates distinct product |
| F11 — integrity/I/O | Corrupt/missing/changed raw/footer/schema/metadata/reference/output; same-byte inode/parent changes, unchanged read/close faults and known mutation precedence |
| F12 — failure/recovery | Partial preparation/copy/stage/write/read-back/rename, disk/SQL caps, cancellation and post-rename ambiguity; no successful prefix/repair or damaged previous publication |
| F13 — bounded independent reader | Spark/Java-free v2 three reports, literal filters/pages/response limits, full raw comparison with one ordered output cursor rather than per-chunk full scans |
| F14 — measurement schemas | Synthetic fake-clock timing partition tests, no lazy-stage double counting, exact denominators, safe missing metrics, source/config fingerprints, result stats/sample admission |
| F15 — preflight | Small numeric pass/blocked fixtures for every formula/cap/volume/RAM/SQL/time/missing metric, retained-trial accounting; no automatic budget increases or target generation after block |
| F16 — baseline local experiment | 103000 physical: exact 102000/100000/2000/1000 reconciliation, format parity/oracle/coverage; both masters/formats, warm-up+five measured trials, all samples/resources and query scopes |
| F17 — conditional target | Before generation: numeric recorded preflight. PASS -> STOP for explicit owner approval; pending/deferred execution remains NOT_ATTEMPTED. Once separately authorized: 1030000 physical per format, minimum scale-validation publications/oracle/reports/replay; optional repeated comparison separately approved. BLOCKED -> numeric reasons, preserved baseline; executed-target evidence remains PENDING until actual success |
| F18 — regressions/evidence | Full processing/E1/E2/Gold/Content plus relevant JDBC/API compatibility, CLI help/errors, npm/diff hygiene; distinguish local, hosted and unexecuted benchmark evidence |

| Checkpoint | Scope and stop gate |
| --- | --- |
| A | Versioned physical contracts/adapters and installed-library provenance/timestamp/bounded-read feasibility; F01–F04. Stop for owner decision if capability fails |
| B | Generator, G1, checksums, small paired registration; F05–F07. No large generation yet |
| C | Independent SQLite oracle, exact/rank parity, immutable writer/reader and faults; F08–F13. Do not time a pipeline without complete correctness |
| D — deferred E4a | Instrumentation/result tests, baseline pilot/preflight, 100K qualification and complete paired baseline measurements; F14/F16 |
| E — deferred E4a | Mandatory numeric observed-baseline target preflight; F15; record PASS or BLOCKED before target generation |
| F — deferred E4a | Only after PASS AND separate explicit owner go/no-go: minimum 1M scale validation for both formats under one matching configuration, oracle/reports/replay; optional full repeated study separately approved; F17. E3 executes none of this checkpoint |
| G | Full regressions, reproducible evidence, all incomplete items and final documentation; F18; one owner-managed PR in issue #46 |

No intermediate PRs/commits are required. A consequential change to versions, position proof, decoder dependencies, numerical tolerance, budgets or mandatory sample scope requires owner review. A correctness failure is never repaired by dropping a row, changing a count, widening p95 tolerance or excluding a slow/failing sample.

## Evidence, remaining risks and owner gate

Historical Stage A execution was limited to read-only inspection, primary-source documentation/code review, small standard-library hash/quota arithmetic and Markdown checks. Historical E2 results remain in their original contract; they are not E3/E4 baseline runs. That historical evidence establishes no runtime or scale result. Current small Stage B evidence and the resource stop are recorded separately below.

Principal feasibility risks: pinned DuckDB ordinal/metadata behavior and decoded-schema details; bounded no-spill ORDER BY/joins at target size; serial preparation/indexing cost; v2 staging's unchanged 2 KiB cap; retained repeated-trial disk amplification; SQLite external-sort costs; source/measurement contamination and cache uncertainty. Shared Parquet decoding limits codec independence. The target may be blocked even below 2M rows. Local threads cannot establish E4b coordination/scaling or AWS equivalence. No linear scaling, production readiness, latency guarantee, fsync durability or multiwriter claim is made.

The owner accepted ADR 0009’s eight decision groups and limits/interfaces on October 20, 2026, with the two execution-policy amendments above. Especially review the version-2 split, actual provenance capability gate, status-distribution correction, common-invalid versus parser-only scope, oracle/codec confidence, complete baseline matrix and conservative target stop rules. This architecture is Accepted; broader specifications are not promoted.

### Executed Stage A validation

Executed locally on October 20, 2026:

| Check | Actual result |
| --- | --- |
| Repository safety/baseline | Correct branch, clean tree/index on entry; PR #45 merge at 257af66; required E2 implementation and accepted ADRs present; issue #46 read-only review |
| npm run check | Passed: 41 Markdown files, zero Markdown/local-link issues; repository hygiene passed |
| git diff --check | Passed; both new untracked documents also checked against /dev/null |
| Small standard-library vectors | Passed: both fenced canonical JSON objects and their hashes; H/event-UUID/batch-UUID/tuple/config vectors; three G1 multiset byte-length/digest vectors and empty-header digest |
| Independent small arithmetic | G1 groups (1/1/0, 2/1/30, 1/0/7), global requests/errors/sum=4/2/37; workload quotas, 450-index duplicate period, original 54/5/1 versus 36/0/4 correlation, file allocations and byte caps checked |
| Schema/acceptance consistency | Exact ten input and 20 occurrence columns; F01–F18 and checkpoints A–G complete; default plan 26 publication +24 replay +240 consumer +24 raw-query samples=314 |
| Scope/index review | Only new ADR/contract, ADR index and two minimal specification links; accepted ADRs 0001–0008, runtime/tests/dependencies/CI unchanged; empty index |

The maximum lexical generated-row example is 262 bytes including LF, below the conservative 320-byte bound; this is a small lexical calculation, not a 100K/1M size measurement. No application modules were imported by calculations. No runtime compatibility experiment, Parquet generation, large dataset, application suite, benchmark, infrastructure or dependency installation ran. At Stage A close, checkpoint A, F01–F18, 100K evidence, target preflight, 1M execution and new hosted CI were unexecuted; ADR 0009 was Proposed and awaited owner approval. The subsequent acceptance does not rewrite that historical evidence. No staging, commit, push, branch switch, PR or GitHub mutation occurred.

## Historical Stage B partial implementation and resource stop — October 20, 2026

This subsection preserves the earlier stop, before the owner sequenced E3 separately. Architecture was Accepted with both owner amendments, but implementation was **PARTIAL** and not ready for closeout. The physical capability gate and small G1 pipeline worked; the experiment runner, full acceptance/fault coverage and scale evidence were incomplete. No 4,096-event pilot, 100K dataset/matrix or 1M dataset had been generated or executed. Hosted CI was not verified. Statements and acceptance statuses in this historical subsection describe that stop; current E3 evidence follows it.

### Files and implemented boundaries

New domain modules are reliability_format_metadata, reliability_format_inputs, reliability_format_parquet (the narrow pinned physical decoder), reliability_format_processing, reliability_format_publication and reliability_format_analytics. They leave all existing domain modules unchanged. New benchmark modules are reliability_benchmark_generator, reliability_benchmark_oracle and the preliminary reliability_benchmark resource screen. Four new test modules and reliability_format_fixtures contain the small tests.

Implemented library signatures match the version/interface section: register/open_batch; homogeneous verify_dependencies; explicit completed staging; publish with execution_config/observer; v2 open_publication/report/close. Raw Parquet admission is separate from the legacy parser. JSONL uses a narrow adapter around unchanged E2 framing/parser behavior. Inputs/staging/output use the v2 namespaces, canonical identity and exact 20-column occurrence positions; events and metrics retain their original 21/17-column schemas and contract_version=1. New occurrence contract_version=2 is required. Public reader responses carry integer version 2 fields and selected format.

Publication uses explicit schemas, exact approved paths, conflict checks before reservation, immutable writes, read-back comparisons, independent raw comparison and completion-last rename. G1 publication/replay exercised both formats. This is not completion of the full fault matrix. The new reader retains three reports, resource settings and integer sentinel semantics. A Spark/producer-blocking subprocess imported it successfully; a full opening subprocess with imports blocked has not yet run.

The generator implements the reviewed field/UUID/config/batch mappings, variants, wire serialization and bounded DuckDB preparation. Config/event/G1 vectors pass. Preparation is temporarily fail-closed above 4,096 unique events until the missing frozen-plan/admission harness is implemented. The benchmark CLI currently provides only the early screen; plan, prepare-register, run, summarize, verify-results and full timing/resource result schemas are pending. This is an implementation gap, not a change to the accepted 100K requirement or permission to reduce it. The generator's 100K/1M configuration logic is testable without generating those datasets.

The SQLite oracle independently parses/re-indexes G1, checks identities, counters and logical digests, and implements output comparison/group/rank routines. The latter full output-comparison entry point has not yet been integration-qualified. Cache=64 MiB, temp_store=FILE, mmap=0 and an explicitly checked isolated temporary directory are configured. Visible scratch is checked, but unlinked temporary storage/RSS attribution and the complete 8 GiB/1 GiB envelope remain unproved. It cannot yet qualify a performance sample. No all-history Python event list is used in the oracle.

### Actual capability and small correctness evidence

Pinned local runtimes observed: Python 3.12.15, GraalVM Java 21.0.7, Spark 4.0.2 and DuckDB 1.4.5. Spark initially could not bind loopback inside the sandbox; the approved native execution environment completed the small tests. A fixture COPY with two bind parameters exposed a pinned DuckDB binder limitation; fixture preparation uses a parameterized seed table and a single COPY destination parameter. No dependency or semantic change was required.

| Executed observation | Actual result |
| --- | --- |
| Physical Parquet positions | Forward 6,149-row file groups [2048,2048,2048,5], reversed 4,101-row file groups [2048,2048,5], and schema-bearing empty file with zero groups; 1/2 decoder threads, filtered/unfiltered scans: 12 cases with exact expected ordinal/value mapping |
| Position examples in forward file | p=2048 -> (group 1,row 0,duration 4,time_us 0); p=4096 -> (group 2,row 0,duration 1,time_us 253402300799999999); p=6148 -> (group 3,row 4,duration 2,time_us 253402300799999999) |
| Bounded cursor/profile | <=1,024 rows/fetch; observed maximum chunk 66,456 encoded bytes. EXPLAIN includes READ_PARQUET and ORDER_BY. Profiled connection peak buffers 5,006,400 bytes, peak temp-directory size 0, 6,149 rows returned; connection includes fixture seed state, not isolated-process RSS or target working-state proof |
| Timestamp evidence | INT64, TIMESTAMP(MICROS,isAdjustedToUTC=true), no INT96; exact microseconds 1790812800000001, -1, 0, -62135596800000000, 253402300799999999, 1790813100000000 survived Spark -> Parquet -> DuckDB. Spark independently read all ten DuckDB-written fields with 1/2 partitions |
| Empty/corrupt/unsupported cases | Empty schema-bearing file decoded to zero rows; corrupt file rejected; unsupported column schema detected. A physical zero-row group fixture was not emitted/tested; empty-file coverage does not claim that case |
| G1 input parity | P=10,V=5,U=4,D=1,R=5; five prescribed invalid reasons match in both formats. All three literal digest vectors above pass. Raw SQLite accounting independently gives 4 requests, 2 server errors and duration sum 37 |
| G1 output/replay | Both formats published 4 events, 10 occurrences and 3 groups; report assertions include UTC five-minute boundaries, missing/nullable/unused reference context and exact counts/sums. Stored p95 candidates qualified under the rank budget. Replay preserved all output bytes and mtimes |
| Ordinary I/O versus mutation | Unchanged JSONL read and Parquet read/close failures return INPUT_UNAVAILABLE with safe messages; disappearance and same-byte replacement return INPUT_CHANGED |
| Spark-free/CLI | Producer-blocking reader import passed. Five available module CLIs returned help=0 and safe invalid-argument exit=2. This does not substitute for full Spark-free open/report integration |

The capability suite ran 3 tests in 4.799 s; its raw position subtest took about 0.076 s and profiled scan about 0.0012 s. The G1 publication/report/replay suite ran 1 test in 17.028 s. These are test/fixture observations, not pipeline H durations, benchmark samples, throughput or capacity evidence. Intake/vector/oracle tests: 9 passed (0.228 s); pure early-screen/configuration tests: 4 passed. Total new focused tests executed: **17**, all passed. Existing E1a suite: **46 passed** (0.584 s); existing E2 intake/reference suite: **17 passed** (0.180 s). Full processing, JDBC and Content API regressions have not run for this correction/checkpoint.

### Numeric early resource gate and execution state

A read-only capture at 2026-10-20T22:58:57.518447Z observed macOS 27.0.1 arm64, 10 logical/physical cores, total RAM 17,179,869,184 bytes (16 GiB), and free disk 822,186,192,896 bytes on the workspace volume. The approved conservative available-memory formula counts free+speculative pages only: 146,997,248 + 29,949,952 = **176,947,200 bytes (0.165 GiB)**. Inactive, compressed and purgeable memory are not silently counted as available.

| Early prerequisite | Calculation | Result |
| --- | --- | --- |
| Host total RAM | Producer 3,221,225,472 <= 75% host = 12,884,901,888 bytes | PASS |
| Available RAM with OS reserve | Producer 3,221,225,472 + 25% host 4,294,967,296 = **7,516,192,768 required**, versus **176,947,200 available** | **RAM_AVAILABLE_BUDGET: blocked**, shortfall 7,339,245,568 bytes |
| Finite operator experiment-time budget | Not supplied; no measured pilot/baseline runtime interval exists | **RUNTIME_UNKNOWN: blocked** |
| Additional disk/SQL/oracle amplification | Pilot and full instrumentation not yet available; free disk alone is not admission | Not evaluated as a passing full preflight |

The early screen is **PREFLIGHT_BLOCKED**, complete_preflight=false, stage=BEFORE_PILOT. Its ignored local evidence is outputs/reliability-benchmarks/checkpoint/resource-screen.json. It starts no workload. A screen PASS would still require the complete frozen-plan baseline preflight. Under the requested resource stop, no pilot or full matrix was started after these blockers were observed. No automatic budget increase, memory reclamation or deletion of retained artifacts was attempted.

Baseline matrix status: **NOT_ATTEMPTED / incomplete, zero of 26 fresh publications and zero measured benchmark samples**. No throughput, benchmark latency p95, process-tree RSS/heap/spill or target runtime/disk estimates are asserted. Target preflight is **not performed**, because it requires an actually completed baseline; do not label missing target calculations a numeric PASS. Target execution is **NOT_ATTEMPTED, OWNER_PENDING**. F17 executed-target evidence remains PENDING. Resumption needs sufficient available RAM under the accepted formula and a finite operator time budget, completion of the missing harness/verification work, pilot and full baseline, then numeric target preflight and separate owner go/no-go.

### Acceptance status at the stop

No acceptance group is represented as fully passed by partial coverage.

| Group | Status / remaining evidence |
| --- | --- |
| F01 | PARTIAL: version/schema isolation and small intake replay; full historical v1 publication/replay regression pending |
| F02 | PARTIAL: installed capability proof passed; zero-row-group and complete fault coverage pending |
| F03 | PARTIAL: UTC physical/runtime proof passed; full wrong-annotation and sentinel report matrix pending |
| F04 | PARTIAL: common typed rejects/privacy and safe I/O tests; full format/privacy precedence matrix pending |
| F05 | PARTIAL: literal generator/config/UUID vectors; distribution, collision and whole preparation tests pending |
| F06 | PARTIAL: G1 exact multiset parity and output reports; complete paired independent output qualification pending |
| F07 | PARTIAL: G1 duplicate/disposition/cardinality; complete cross-batch conflicts and lineage faults pending |
| F08 | PARTIAL: independent raw SQLite G1 accounting/digests/import tests; full output comparison and resource envelope proof pending |
| F09 | PARTIAL: G1 exact metrics and stored rank checks plus literal numerical oracle; heavy-tail/>10K/layout suite pending |
| F10 | PARTIAL: G1 completion/replay bytes/mtimes; changed selections, empty outputs and full recovery coverage pending |
| F11 | PARTIAL: ordinary read/close versus observed disappearance/replacement tests; full dependency/output/guard corruption matrix pending |
| F12 | PENDING: systematic write/read-back/finalization failure injection |
| F13 | PARTIAL: G1 reports and Spark-free import; blocked-import full open/report subprocess, access plans and response boundaries pending |
| F14 | PARTIAL: exclusive ledger implementation and CLI checks; fake-clock/result schemas, process/event-log instrumentation and samples pending |
| F15 | PARTIAL: three early RAM/time screen tests and one pre-Spark configuration test; complete numeric pilot/baseline/target formulas and all volume/resource tests pending |
| F16 | NOT_RUN: resource/time screen blocked before pilot; no 100K artifacts or samples |
| F17 | PENDING: no observed-baseline target preflight; target not generated/executed or authorized |
| F18 | PARTIAL: 63 existing Spark-free E1a/E2 tests passed; full processing/JDBC/API regressions pending. Documentation/hygiene results follow |

### Commands and review limits

Small reproducible checks use the existing Python executable, PYTHONPATH=processing and Java 21 / SPARK_LOCAL_IP=127.0.0.1 / TZ=UTC for the two Spark suites:

~~~bash
"$PYTHON" -m unittest discover -s processing/tests -p 'test_reliability_format_*.py' -v
"$PYTHON" -m unittest discover -s processing/tests -p test_reliability_benchmark.py -v
# Read-only early screen; exit 1 on failed prerequisites. Not the full preflight.
"$PYTHON" -m reliability_benchmark --root outputs/reliability-benchmarks/checkpoint
~~~

No large artifact is tracked. The raw gate/test logs and early screen remain ignored local evidence. SQLite/Parquet source decoding shares the pinned codec; business calculations do not import the producer/generator. Zero-row-group support, unlinked SQLite temp attribution, complete oracle/output qualification, process-memory instrumentation, full failure coverage and all real benchmark results remain limitations. No architecture limits or percentile tolerance were weakened. No accepted ADR 0001–0008, runtime dependency, CI job, broader product/M6 status or API was changed. No Git/GitHub write was performed.

Documentation/hygiene validation at this stop: npm run check passed (41 Markdown files, zero Markdown/local-link issues; repository hygiene passed). git diff --check passed. All 14 new untracked Python files were also parsed and checked for trailing whitespace without staging them. Final scope is three modified documentation files and 14 new Python source/test files, with an empty index. Full regression and benchmark evidence remain pending as stated above.

## E3 correctness implementation and validation — October 20, 2026

The owner revised issue #46 to E3 correctness now, E4a performance deferred, E5–E7 next. Read-only GitHub review confirmed that the issue text already reflects this sequencing. This section supersedes the historical partial correctness statuses above; it does not rewrite their evidence. ADR 0009 remains Accepted. No accepted ADR 0001–0008, legacy runtime module, dependency, CI configuration, API or wider product/M6 status changed.

### Implemented interfaces and safety

The versioned interfaces above are implemented in the six reliability_format modules, including the narrow reliability_format_parquet decoder. register/open_batch/select enforce homogeneous explicit v2 selections and exact immutable batch/profile/completion evidence. JSONL retains physical lines, byte offsets and lengths including LF/CRLF; Parquet uses zero-based record/group/within-group ordinals from the exact file and footer intervals. Inapplicable positions are null. Both adapters emit canonical 15-field typed staging records with a completed inventory. Staging validation rejects unknown fields, invalid types, positions, identities, reason ordering and evidence changes before Spark consumes the exact listed files.

The independent reader and oracle inspect every physical schema, including UTC INT64 TIMESTAMP_MICROS. Admission rejects INT96, NTZ, millisecond/nanosecond, unsigned/coerced/complex/unknown columns, unapproved metadata/field IDs and unsupported compression. Empty schema-bearing files are supported. The pinned writers did not emit a physical zero-row group; that binary case is not claimed as executed evidence. Ordinary read/close failure on unchanged evidence fails with an I/O/integrity diagnostic; observed disappearance, replacement, metadata, byte or directory changes retain the changed classification. Diagnostics carry fixed safe codes/messages and the known format without raw paths or exception text.

Spark begins at explicit typed staging. Events/metrics retain the E2 21/17-column schemas; occurrences use exactly 20 columns and contract_version=2. Full identities, deterministic representatives, valid delivery/receipt ranges, reference context, P=V+R, V=U+D and physical position bijection reconcile. Recognizable rejected identities conflict with any other occurrence of that identity. Deduplication precedes exact five-minute UTC flooring, counts, DECIMAL duration sums and percentile calculation. No v1 artifact is migrated or reinterpreted.

Publication reserves exclusively after structural/privacy/conflict gates, writes with error-if-exists, verifies every physical schema plus full expected/actual rows, independently checks raw provenance and metrics, rechecks evidence, and publishes completion by final same-directory rename. Replay independently reopens the stored result, checks qualifying stored p95 rather than fresh approximate equality, and preserves bytes/mtimes. Incomplete reservations remain incomplete. A lost response after rename is resolved by explicit verification; it is never silently repaired.

The v2 reader implements requests_5m, input_coverage and reference_coverage with bound filters, deterministic pages/lookahead, the exclusive year-10000 query-end sentinel, decimal-string counters, exact UTC microseconds and synthetic labels. Its single synchronous DuckDB session uses two threads/512 MiB/no spill/extensions disabled. It compares raw evidence to one ordered occurrence/event cursor, not one full scan per input chunk. Tests assert one ordered comparison execution, multiple bounded chunks and no partial response after failures. Pre-fetch string checks, encoded 1 MiB response enforcement, BUSY/closed handling and before/after integrity checks remain active. A separate process opens both formats and executes all reports while Spark, Java bridge and producer imports are blocked.

### Generator and independent SQLite oracle

reliability_benchmark_generator.config/event/batch_records retain the approved version-1 seed, SHA-256/UUID8 mapping, quotas, two receipt batches and sixteen-file policy. prepare(root, config) permits only bounded E3 fixtures of at most 4,096 unique events; baseline/target preparation fails before writes. Collision, reproducibility, separately hashed service/status mappings, hot-window/all-equal variants and the literal vectors above are tested. No 100K/1M data was generated.

The independent interface is:

~~~text
reliability_benchmark_oracle.verify_publication(root, fingerprint, *,
    batch_root, reference_root, scratch_root)
~~~

scratch_root must be an existing empty dedicated directory separated from inputs/references/output. The oracle independently parses and validates retained raw JSONL or Parquet, exact batch evidence, physical positions, full typed tuples, recognized invalid conflicts, deterministic deliveries and all persisted columns. SQLite stores full tuple bytes, identities and indexed durations. It compares all event/occurrence keys and every aggregate group, exact sums/counts, rates/means, reference enrichment, exact nearest-rank/tie intervals and the stored approximate candidate's rank budget. It recomputes all three logical multiset hashes and checks completion counts. Refreshed file hashes do not excuse altered values or schemas.

The oracle imports no Spark transformation, producer aggregation or generator expectation code. Its Parquet decoder reuses installed DuckDB, with independent schema/position validation and business calculations; this is not independent codec verification. SQLite uses a 64 MiB cache, mmap disabled, file-backed temporary work, a 2 GiB main-database page cap and an 8 GiB visible scratch check. Input/output chunks and string bytes are bounded; no all-history Python list is used. Full raw hashing/evidence guards remain live through comparison. Extension loading is disabled. Large-run total RSS/unlinked temporary storage attribution is explicitly NOT_MEASURED and deferred to E4a; small correctness evidence does not prove the planned large-run envelope. Optional SQLite temporary-directory facilities are not platform admission requirements.

### Executed correctness evidence

Local runtimes: Python 3.12.15, Java 21.0.7, PySpark 4.0.2 and DuckDB 1.4.5. Spark used the existing supported loopback/native execution environment. No dependencies were installed and no timestamp, schema, numerical or verification limit was weakened.

| Fixture/check | Actual assertion |
| --- | --- |
| G1 both formats, reversed Parquet source ordering | P=10,V=5,U=4,D=1,R=5; three groups; four requests, two 5xx, duration sum 37; all three literal checksums; exact reports and read-only replay |
| Moderate deterministic paired fixture | U=2,000,D=40,R=20,P=2,060,V=2,040; two batches/sixteen raw files per format; 8 FIELD_NULL and 12 FIELD_VALUE rejections; all logical digests equal; 88 missing-reference events, nine matched sources and one unused reference key |
| Timestamp/window limits | Year 0001, -1 microsecond, epoch, midnight/five-minute edges, year 9999 final microsecond and final window; integer-floor semantics and year-10000 end-only sentinel with invalid-role/alignment/span negatives |
| Physical Parquet provenance | Repeated multi-file/group scans with one/two decoder threads and filtered/unfiltered rows; literal ordinal/value maps, exact UTC annotations, empty/corrupt/unsupported cases and bounded cursor/access-plan checks |
| p95 independent numerical checks | Singleton, all-equal, ties, sparse/heavy-tail, zero/maximum duration; 10,001-event group across shuffled input and 1/3 partitions qualifies against exact rank r=9,501 and budget b=2; qualifying unequal candidates accepted; invalid membership/rank rejected |
| Corruption with refreshed hashes | Thirteen event/occurrence/reference/count/sum/rate/mean/p95/key corruptions and three physical-schema alterations rejected by both independent verifiers |
| Failure injection | Write, independent verification, Spark read-back, temporary completion write, rename-before and response-lost-after-rename; no completion on pre-finalization failure, no repair, prior completed bytes/mtimes preserved |
| CLI | Five implemented CLIs: help exit 0; invalid arguments exit 2 with safe diagnostics; actual oracle and reader CLI runs on a literal completed Parquet publication succeed |

These are bounded correctness fixtures and test runtimes, not performance samples, capacity proof or 100K/1M observations. The 33,000 repeated-row file is a structural row-group-limit rejection test; it is not a benchmark workload.

| Acceptance group | Current E3 evidence/status |
| --- | --- |
| F01 | PASS: explicit v1 re-registration remains distinct; 21/16/17 legacy schemas preserved; v2 rejects old/mixed/conflicting/version bindings; legacy full regressions recorded below |
| F02 | PASS within representable fixtures: exact multi-file/group/empty-file positions, thread/filter/order variation, bounds and corruption; physical zero-row group not emitted by pinned writers |
| F03 | PASS: physical type/annotation negatives, exact microseconds and full timestamp/sentinel boundary tests |
| F04 | PASS: typed-invalid parity, parser-only categories, duplicate keys, lexical bounds, privacy precedence, sensitive/unknown metadata and safe diagnostics |
| F05 | PASS: literal seed/H/UUID/event/config vectors, exact additions/quota, independent service/status mappings, collision/no-write and deterministic preparation |
| F06 | PASS: G1 and 2,000-event full output/oracle parity, three logical hashes and physical-layout independence |
| F07 | PASS: cross-service identities, cross-batch semantic/rejected conflicts before reservation, representatives/receipts and exact occurrence reconciliation |
| F08 | PASS for E3: independent full raw/output SQLite comparison, fixed bounds/import checks, forged-hash corruption and complete keys; large-run resource attribution deferred |
| F09 | PASS: exact metrics, independent calendar/arithmetic/rank checks, heavy-tail 10,001-row/layout/tie cases and overflow fault |
| F10 | PASS: exclusive publication, empty/rejected-only schemas, selected historical/reference changes, read-only replay and immutable prior results |
| F11 | PASS: corrupt/missing dependencies/output, annotation/metadata/inventory/identity/directory changes and unchanged read/close I/O classification |
| F12 | PASS for E3: six publication failure phases, strict incomplete/staging handling, resource/SQL/response failure and no partial success or repair |
| F13 | PASS: separate Spark/producer-free open/report process, ordered raw cursor, exact filters/pages/coverage/sentinel/bounds and session integrity |
| F14–F17 | DEFERRED: benchmark evidence/instrumentation, numeric hardware admission, full 100K matrix and 1M preflight/execution. Existing small scaffold tests do not establish these groups |
| F18 | PASS: full processing, JDBC/Content API/ingestion compatibility, CLI and documentation results recorded below |

### Small-fixture reproduction and limits

~~~bash
export JAVA_HOME=$(/usr/libexec/java_home -v 21) # macOS setup; use installed JDK 21 on Linux
export SPARK_LOCAL_IP=127.0.0.1 TZ=UTC
export PYSPARK_PYTHON="$PWD/outputs/silver-python312/.venv/bin/python"
export PYTHONPATH=processing
"$PYSPARK_PYTHON" -m unittest discover -s processing/tests -p 'test_reliability_format_*.py' -v
"$PYSPARK_PYTHON" -m reliability_benchmark_generator --help
"$PYSPARK_PYTHON" -m reliability_benchmark_oracle --help
"$PYSPARK_PYTHON" -m reliability_format_analytics --help
~~~

Use the pinned environment appropriate to the host; the macOS JDK discovery command is setup documentation, not runtime processing logic. Native Windows is untested; hosted Linux CI remains pending. Synchronous open/verification has no hard deadline. No multiwriter, hostile-filesystem, fsync/power-loss, production-data or performance guarantee is added. Raw/stage/Parquet/SQLite artifacts remain ignored/local. E4a owns hardware collectors, full scratch/RSS attribution, repeated matrix/evidence tooling, 100K/1M experiments and subsequent owner gates. No benchmark completion or numeric target preflight is claimed.

### Current local validation results

| Executed command/check | Actual result |
| --- | --- |
| Focused discovery: test_reliability_format_*.py | 49 tests passed in 113.336 s: capability 3, admission 9, generator 4, intake/vector 10, publication 8, independent verification/report 15 |
| Full processing unittest discovery | 275 tests passed in 357.060 s; includes all 49 focused tests, four preserved small benchmark-scaffold tests and 222 existing processing tests, including E1a/E1b/E2/Gold/Silver |
| Existing JDBC compatibility, offline Maven verify | 48 tests passed, zero failures/errors/skips; fresh synthetic Gold fixtures, 28 reference query scenarios and 13 deliberately damaged copies |
| Existing Content API, offline Maven verify with integration profile | 120 unit tests and 20 real HTTP/Python integration tests passed; zero failures/errors/skips; verified use of the same fresh synthetic fixture path |
| Existing Java ingestion and launcher compatibility | 29 tests passed in offline Maven verify; all five batch-launch argument regression cases passed; no live publisher calls |
| CLI checks | Help exit 0 and invalid-argument exit 2 for inputs, publication, analytics, generator and oracle; successful actual reader/oracle CLI runs against a literal completed Parquet fixture |
| npm run check | 41 Markdown files, zero formatting/local-link issues; repository hygiene passed |
| Diff hygiene and ownership | git diff --check and git diff --cached --check passed; the fourteen owner-staged Python entries remained byte-for-byte identical in the index; resumed edits and three additional test modules were left unstaged |

The initial focused run exposed an incorrect negative-test fixture projection that accidentally added an eleventh column; it was corrected to exercise the intended oversized row-group failure. The final focused and full discovery runs above passed. Native Hadoop fallback/incubator warnings and PySpark socket ResourceWarnings appeared during otherwise successful local tests. They do not establish a product performance result and were not suppressed by weakening checks.

E3 correctness is ready for owner commit/PR review. F14–F17 remain deferred, not passed. No 100K/1M generation, repeated benchmark, numeric target preflight, new hardware collector, production-data access, infrastructure or Git/GitHub write ran. Hosted CI awaits an owner-published change. Final scope is three modified Markdown files and seventeen new Python source/test files (fourteen already owner-staged on entry, three additional test modules untracked). The staged snapshot was preserved; current worktree refinements are deliberately not staged.
