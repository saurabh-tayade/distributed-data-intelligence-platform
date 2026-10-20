# Application Reliability publication: E2 contract

Status: **Accepted by the owner on October 20, 2026**, with the two amendments in ADR 0008, for [issue #44](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/issues/44) and [ADR 0008](../adr/0008-application-reliability-publication.md). Stage B is authorized. Architecture acceptance and actual implementation/validation evidence are separate. The [Reliability product specification](../data-products/application-reliability.md) and wider platform specification remain Proposed.

## Scope, compatibility and inspected boundaries

E2 combines JSONL intake, a service reference, Spark normalization/aggregation, immutable local publication and independent DuckDB consumption. It supports only reviewed synthetic sources. Every successful consumer response carries exactly this label:

> Synthetic request events — not production reliability.

Counts describe recorded completed request attempts. They do not measure availability, SLO compliance, unique users, incidents or requests that failed before emission. Environment `production` is an allowed synthetic scenario value, not a claim of production origin.

Read-only baseline inspection found clean branch `feature/e2-reliability-batch-publication` at `30153df`, the merge of PR #43, with issue #40's E1a correction in its ancestry. Issue #44's complete Stage A/B and automated acceptance requirements were reviewed. ADRs 0001–0007 are available and Accepted. No accepted ADR or runtime contract is changed here.

| Inspected boundary | Safe reuse / constraint |
| --- | --- |
| `publication_metadata.canonical_bytes` | Reuse unchanged: recursive sorted object keys, compact separators, UTF-8, no NaN or trailing newline |
| `publisher_reference.validate/publish/open_snapshot` | Mature bounded CSV, detached rows, vocabulary, exclusive revision and completion-last patterns; its fixed publisher schema and vocabulary cannot validate services |
| `content_metadata` | Evidence captures demonstrate streaming hashes, inode/metadata/directory guards and safe I/O classification; constants, schemas and helpers are Content-specific, including private functions |
| `content_dependencies.verify_dependencies` | Fully verifies Gold using `gold_analytics.open_publication` and E1a public opening; its Gold/E1a adapter is not a Reliability batch verifier |
| `content_processing` / `content_publication` | Explicit Spark schemas, exact paths, empty files, bidirectional multiset checks, exclusive reservation/replay/finalization patterns; article enrichment is not a generic ETL engine |
| `content_analytics` | Independent physical-schema/SQL verification, bounded pages, encoded size and synchronous guard patterns; report SQL and dependencies are specific to Content |
| `gold_publication` / `gold_analytics` | Accepted Spark-to-Parquet-to-DuckDB boundary; no Gold or Silver dependency belongs in E2 |
| Existing tests/tooling | unittest, temporary directories, literal synthetic fixtures, mock fault injection, separate import-blocking reader processes; Python 3.12.15, Java 21, PySpark 4.0.2, DuckDB 1.4.5; Node 24 documentation checks |

Implemented modules are `reliability_metadata`, `reliability_inputs` (registration/indexing), `service_reference`, `reliability_processing` (Spark only), `reliability_publication` and `reliability_analytics`. Metadata, intake/reference and reader modules must not import Spark, Silver, Gold/Content producers or Boto3. No generic connector framework, registry service, scheduler, worker manager, HTTP/API/React/export work, dependency installation or infrastructure is authorized by E2. A need to change accepted domain modules requires separate approval.

## Common scalar, metadata and filesystem rules

Identifiers `source_id` and `service_id` full-match ASCII `[a-z0-9][a-z0-9_-]{0,127}`. Batch/reference IDs full-match `[a-z0-9][a-z0-9_-]{0,63}`; reference revisions match `[a-z0-9][a-z0-9_-]{0,31}`. UUIDs use exact lowercase `8-4-4-4-12` hexadecimal spelling, with no surrounding whitespace; supplied event UUIDs need not be version 5. Digests are 64 lowercase hexadecimal characters. No trimming, case folding, Unicode normalization or coercion.

Timestamps require exact Gregorian `YYYY-MM-DDTHH:mm:ss.ffffffZ`, years 0001–9999, exactly six fractional digits, seconds 00–59. Reject offsets, local/naive times, leap seconds, invalid calendars, nanoseconds and alternate precision. Convert to integer epoch microseconds by integer calendar/timedelta arithmetic, never floating-point `timestamp()`. Timezone is UTC throughout Spark and DuckDB. Negative epoch microseconds are valid.

All metadata objects have exactly the keys defined below, including explicitly nullable fields. Reject duplicate/missing/extra keys at every depth, invalid Unicode scalars, noncanonical bytes, unsupported versions, boolean-as-integer and floating/exponent integer encodings. Metadata integers use canonical base-10 integer tokens, range 0–2^63−1 unless a smaller bound is stated; no negative zero. UTF-8 JSON uses unchanged `canonical_bytes`: recursive key sorting, `ensure_ascii=False`, `allow_nan=False`, separators comma/colon, no BOM/newline. Arrays retain the specified order. Fingerprints are SHA-256 of those exact bytes, not a concatenated list of hashes.

Roots are existing dedicated operator-approved local directories, mutually nonoverlapping for batches, staging, service reference and Reliability output. Resolve trusted ancestors once. Reject symlinks at/below roots, hard-linked artifacts, special files, traversal, aliases, backslashes and glob metacharacters. Input/output relative paths are fixed descendants, never arbitrary caller SQL or filenames interpreted by a glob. Raw input artifact basenames are ASCII `[a-z0-9][a-z0-9_-]{0,63}[.]jsonl`; metadata/reference names are fixed by their layouts. Exact dataset Parquet basenames allow Spark's ordinary dots/hyphens but no wildcards. No unchecked directory reads.

Capture file device/inode/type/mode/link count/size/nanosecond mtime before and after streaming reads, exact hashes/bytes as applicable, guarded root/parent identities, and exact selected-directory membership. Do not treat directory mtime changes caused by the writer's own permitted creation as dependency mutation; validate the expected phase inventory and identities. Recheck all dependency/output evidence after index completion, Spark actions, read-back, before completion rename, and before/after reports. Preserve any observed mutation even if a later close fails. An ordinary read/close/stat failure without observed change must not become a mutation code. These are checks under trusted immutable ownership, not filesystem leases or hostile-race isolation.

## Registered synthetic JSONL batches

### Review and registration boundary

The operator reviews a local synthetic producer/profile and its approved raw location before registration. A source declaration alone does not anonymize real logs. Initial origin is exactly `synthetic` and source profile exactly `request-jsonl-v1`; reject operational/mixed/unapproved sources. Raw inputs are owner-retained restricted artifacts, never collected from live applications by E2.

Implemented interface:

~~~text
reliability_inputs.register(batch_root, *, input_root, source_id, batch_id, files,
                            received_at, profile)
reliability_inputs.open_batch(batch_root, source_id, batch_id,
                              expected_fingerprint)
~~~

`input_root` is a separately approved existing incoming directory, nonoverlapping with retained batch/staging/reference/output roots. `files` is a bounded list of objects with exactly `artifact_id` and `input_file`; input_file is exactly `<artifact_id>.jsonl` immediately below input_root. Sort by artifact_id and reject duplicates. There is no glob or arbitrary absolute input path. Unselected incoming files are not consumed. Registration validates whole-file structural/privacy admission and records physical-line counts; representable row errors are allowed. It copies exact approved bytes into its absent batch namespace using exclusive creates, revalidates copies, rechecks original evidence and writes completion last. It never modifies originals. Any privacy/structural failure precedes reservation; a later copy/I/O failure may leave an incomplete reservation. Opening independently repeats structural/privacy admission, metadata, hashes, line counts and guards; a marker alone is insufficient. No compressed archives or non-JSONL format are supported.

`profile` has exactly:

| Key | Value |
| --- | --- |
| `profile_id`, `schema_version`, `config_revision` | `request-jsonl-v1`, integer 1, integer 1 |
| `origin` | `synthetic` |
| `synthetic_source_id` | Reviewed technical label, same syntax as source_id; must equal selected source_id |
| `logical_dataset_id` | Null or reviewed synthetic benchmark label using batch-ID syntax; provenance only, no E3 equivalence claim |
| `allowed_service_ids` | 1–128 distinct service IDs sorted by ASCII bytes |
| `allowed_route_ids` | 1–256 curated opaque route IDs sorted by ASCII bytes, each `[a-z0-9][a-z0-9_-]{0,63}` |
| `policy_revision` | Integer 1, fixed policies/limits in this document |

Allowlisted tokens must be reviewed technical labels, not personal identifiers disguised by a regex. Source ID is delivery provenance; it is not the event service, environment or aggregate key. Configuration corrections use a new reviewed revision/processor support, not a hidden invocation override. Revision 1 has no adjustable rejection threshold: all representable row rejections are retained and reported. Adding stricter thresholds is a future identity-bearing policy change.

Batch namespace:

~~~text
<batch-root>/source_id=<source_id>/batch_id=<batch_id>/
  data/
    <artifact_id>.jsonl
  manifest.json
  complete.json
~~~

`artifact_id` is the filename stem with the 1–64 identifier rule; `file` is exactly `data/<artifact_id>.jsonl`. A completed batch has only these two metadata files and the flat data directory containing exactly its listed files. It has 1–16 files; zero-byte files and batches containing only empty files are valid. A zero-file batch is invalid selection.

`manifest.json` has exactly:

| Key | Schema |
| --- | --- |
| `manifest_version`, `batch_contract_version`, `request_schema_version`, `registrar_revision` | Integers, all 1 |
| `source_id`, `batch_id` | Selected identifiers matching the namespace |
| `ingestion_id` | UUIDv5 under UUID URL namespace `6ba7b811-9dad-11d1-80b4-00c04fd430c8`, exact UTF-8 name `ddip:reliability:batch-v1:<source_id>:<batch_id>` |
| `received_at` | Fixed platform receipt instant, strict UTC timestamp; one receipt per batch |
| `profile` | Exact object above |
| `files` | 1–16 entries sorted by ASCII artifact_id; object schema below |

Each file entry has exactly `artifact_id`, `file`, `byte_length` (0–268,435,456), `sha256` and `physical_line_count` (0–2,000,000). No duplicate artifact/path. The canonical batch fingerprint is SHA-256 of exact manifest bytes. Receipt time is retained provenance and identity-bearing; it is not processing completion time or event time. No run/attempt UUID enters batch identity. Ingestion UUID derives from the namespace, not data; exact manifest/fingerprint prevents rebinding it to changed data.

`complete.json` has exactly `completion_version:1`, `batch_fingerprint`, `manifest_byte_length` and `completed_at`. Length/hash/identity must match independently checked bytes; completed_at is strict UTC, not earlier than received_at, recorded only after registration verification. It is outside batch identity but its exact bytes are bound by the selected product manifest. Supported batch/registrar/profile revisions are exactly 1.

Same source/batch binding plus identical manifest and a valid completed destination yields read-only replay, preserving bytes/mtimes/completion. A valid different candidate yields `BATCH_CONFLICT`; incomplete existing reservation yields `BATCH_INCOMPLETE`. No overwrite/repair. The original path is not needed after registration; retained registered raw bytes are required. Registration returns an immutable summary with status (registered/replayed), source_id, batch_id, batch_fingerprint, ingestion_id, received_at, input_file_count, input_byte_length and physical_line_count. Opening returns a detached immutable manifest/selection and exact file descriptor tuple plus integrity guards, never an unbounded event list; descriptors are not a lease and must be rechecked. The publisher constructs its own verified descriptors rather than accepting caller-fabricated ones.

### Explicit retained selection

The publisher accepts 1–32 exact `(source_id,batch_id,batch_fingerprint)` selections beneath one batch root, plus one service-reference tuple and dedicated staging/output roots. Empty history is represented by at least one valid empty batch. Validate the entire supplied bounded selection, collapse repeated identical tuples, sort unique entries by `(source_id,batch_id,batch_fingerprint)` in ASCII order, and verify each independently. Conflicting fingerprints for one source/batch binding fail `BATCH_CONFLICT`. At most 128 selected files and 1 GiB raw bytes total.

Selecting the same batch twice does not create a second physical delivery or change product identity; `duplicate_batch_selections` is an invocation-only count, not persisted lineage. Identical raw bytes registered under different batch IDs are separate deliveries and separate product inputs; semantic event deduplication still applies. Batch completion byte descriptors bind the actual retained evidence, not a fresh completion time.

New or removed history must be an explicit new selection. Never discover latest, include files created after selection, truncate history, union aggregate snapshots, use a watermark or re-fetch inputs. Older event_time is valid. A changed selected inventory creates a separate product and preserves previous publications. E2 defers comparison to a prior publication for lateness classification: `comparison_publication` is not an input and no “late” count is claimed. Event/receipt ranges, future-clock occurrence count and receipt-delay extrema remain available as descriptive provenance.

## Strict event validation and intake precedence

The exact ten required nonnull producer fields are:

| Order | Field | Accepted JSON / typed representation | Rule |
| --- | --- | --- | --- |
| 1 | `schema_version` | integer / INT32 | Exactly 1; no bool, float or string coercion |
| 2 | `event_id` | string / UTF-8 | Canonical lowercase UUID; stable completed attempt ID |
| 3 | `service_id` | string / UTF-8 | Service identifier syntax and reviewed profile membership |
| 4 | `environment` | string / UTF-8 | Exactly dev, test or production |
| 5 | `event_time` | string / UTC TIMESTAMP_MICROS | Strict timestamp above; completion time, not receipt |
| 6 | `method` | string / UTF-8 | GET, HEAD, POST, PUT, PATCH, DELETE, OPTIONS |
| 7 | `route_id` | string / UTF-8 | Opaque 1–64 token and reviewed profile membership; no raw path/URL |
| 8 | `status_code` | integer / INT32 | 100–599, terminal HTTP status |
| 9 | `duration_ms` | integer / INT64 | 0–3,600,000, whole elapsed milliseconds |
| 10 | `is_synthetic` | boolean / BOOLEAN | Exactly true, matching registered origin |

Application retries are new event IDs; repeated delivery of the same attempt retains its ID. No synthetic status 0, inferred failure, replaced time or recomputed duration. Strict JSON rejects comments, trailing data, non-object values, duplicate keys, invalid scalars and non-finite numeric tokens. Parsing must retain object pairs long enough to detect duplicate keys; never accept json's default last-key-wins. Bound nesting to 4 container levels and decoded object members to 32 total per line before constructing unbounded structures; deeper/larger input is a structural failure. A small lexical depth/member guard must be string/escape aware, not regex extraction of event values. Scalar number tokens are bounded to 64 bytes before conversion; exceeding that is structural admission failure. Fraction/exponent tokens are retained as wrong-type sentinels for integer fields, not converted/coerced through floating point; NaN/Infinity constants have a separate nonfinite sentinel. No inference or nested extension bag.

| Class | Conditions | Result |
| --- | --- | --- |
| Structural selected-batch failure | Invalid UTF-8/BOM, lone CR, overlong line/file, unreadable input, unsupported integer schema_version, excessive nesting/member/numeric-token budget, duplicate identity keys, metadata/hash/inventory mismatch | Abort selection; no product reservation or prefix success |
| Representable row rejection | Blank line, malformed JSON syntax, non-object, duplicate non-identity known keys, missing/null fields, invalid known-field types/ranges/vocabularies/calendar/precision, JSON NaN/Infinity tokens | One restricted rejected occurrence with fixed reason codes; no business row |
| Identity conflict | More than one validated semantic payload for one identity, or any recognizable-invalid identity repeated anywhere else | Abort whole product before reservation |
| Unapproved intake | Unreviewed source/profile/origin; parsed object contains any unknown key at any depth, disallowed extra sensitive field, explicit false/non-synthetic origin value, or supplied string service/route outside reviewed allowlists | `INTAKE_NOT_APPROVED`; no ordinary quarantine publication or analytical output |

Known malformed types such as null service_id may be rejected under a reviewed synthetic source. An arbitrary string in a known field is never copied to diagnostics/rejection evidence. For service/route strings, allowlist admission precedes row validity. A malformed JSON line cannot be reliably inspected for identity/unknown fields; it is a rejection only under the trusted reviewed synthetic-source assumption. It is not evidence that arbitrary malformed operational logs are safe. Investigation must use restricted originals; there is no regex-based salvage of identities.

Precedence: argument/path/metadata bounds and source approval; then streaming files in canonical order and physical lines in increasing order. For each bounded line, encoding/framing/lexical budgets precede parse. For any successfully parsed value, inspect all contained object pairs for unknown-field/origin/allowlist admission before representable row errors, unsupported schema and identity checks; wrapping an unapproved object in an array does not bypass intake. The top-level value must still be an object. Duplicate service_id/event_id keys abort structurally even when values agree: identity is ambiguous. For a parsed object with unique identity keys, other duplicate keys produce rejection; known-field validation collects fixed codes in the order below. Structural or privacy failure anywhere aborts the whole selection; previously staged safe records are private, not a successful quarantine. Only after all files pass admission does cross-occurrence identity analysis run. Report the first failure in this deterministic traversal; do not scan beyond a fatal cap to search for a different failure.

Fixed rejection-code order is `BLANK_LINE`, `JSON_SYNTAX`, `JSON_NOT_OBJECT`, `JSON_DUPLICATE_KEY`, `JSON_NONFINITE`, `FIELD_MISSING`, `FIELD_NULL`, `FIELD_TYPE`, `FIELD_VALUE`, `TIMESTAMP_INVALID`. Deduplicate codes within one occurrence. Syntax/non-object failures have just their respective code; NaN/Infinity in otherwise parseable known fields uses JSON_NONFINITE without persisting the token or adding FIELD_TYPE/FIELD_VALUE for that sentinel. A valid complete object may have several validation codes but counts as one rejection. Missing/null/type errors do not also produce dependent value/time errors for that field; invalid timestamp strings produce TIMESTAMP_INVALID rather than an additional FIELD_VALUE. Parser-controlled exceptions become fixed codes, never raw exception strings.

Unknown-field privacy checking must still run when duplicate known keys or non-finite values are present in a parseable object. Inspect all object pairs/duplicate instances for privacy, allowlist and unsupported-schema conditions, never only a first/last dictionary value. Nonstandard constants are represented by an internal sentinel solely for validation; they are never accepted numeric values. A syntax-invalid line with apparently visible IDs has no recognized identity. Duplicate non-identity keys do not prevent recognition when exactly one syntactically valid service_id and event_id occur.

## Deterministic physical occurrence indexing

### Recommended flow and exact positions

~~~text
reviewed immutable registered raw JSONL + explicit batch manifests
  -> bounded Python binary streaming indexer, one file at a time
  -> private provenance-tagged staging JSONL + exact staging inventory
  -> Spark explicit-schema read of approved staging paths
  -> typed validation, conflict gate, deduplication and reference join
  -> events + occurrence dispositions + five-minute aggregates
  -> immutable Parquet, full read-back and independent rank checks
  -> final dependency/output rechecks -> completion last
~~~

The Python pass is serial local preparation. It does not collect a file or all events into memory. Before decoding, read at most 16,384 content bytes plus ending/detection sentinels per physical line; bound whole-file bytes while hashing. A physical line is a byte span ending in LF, optionally immediately preceded by CR, or the final nonempty span at EOF. Content length excludes LF/CRLF. Count lines from one; byte_offset starts at zero; physical_byte_length includes its ending. One terminating newline creates no additional line; a second LF creates a blank rejected line. Zero bytes means zero occurrences. Mixed LF/CRLF and missing final newline are supported. A lone unescaped CR in the physical span, invalid UTF-8 or BOM at file start is structural failure. Never split at Unicode line separators.

Open each raw file without following links; compare fd/path identities before/after, hash all raw bytes including endings, and compare registered length/hash/line count. An EOF before the frozen length is not a successful readable prefix. An ordinary late read/close failure aborts; a demonstrated changed stamp/hash/identity uses DEPENDENCY_CHANGED. A hash mismatch at the first observation is DEPENDENCY_INVALID, not a claim of when the change occurred.

Example: for literal raw bytes `{}\r\n\n{"x":1}` (12 bytes), positions are:

| Physical line | byte_offset | physical_byte_length | Content | Provisional result |
| --- | --- | --- | --- | --- |
| 1 | 0 | 4 | `{}` | REJECTED, FIELD_MISSING |
| 2 | 4 | 1 | Empty | REJECTED, BLANK_LINE |
| 3 | 5 | 7 | `{"x":1}` | INTAKE_NOT_APPROVED, unknown field; abort whole selection |

Thus the third line is never a completed rejection/lineage product. For a successful mapping example replace line 3 with `{}`: exact bytes `{}\r\n\n{}` total 7, lines (1,0,4), (2,4,1), (3,5,2); all three are rejected with codes FIELD_MISSING, BLANK_LINE, FIELD_MISSING respectively. No Spark ordering establishes these positions.

Private staging lives only below `<staging-root>/attempt=<local-uuid>/`, permission-restricted to the operator, with one `stage-<batch-index>-<file-index>.jsonl` per selected raw file and canonical `stage.json`. Indices are one-based two-digit zero-padded decimals, for example stage-01-01.jsonl. This operational attempt UUID is not publication identity or event identity. Index/file order derives only from canonical input inventories. Completed stage.json has exactly `staging_version:1`, `product_fingerprint` and `files`, whose entries are exactly `file`, `byte_length`, `sha256`, `row_count`, `batch_fingerprint`, `artifact_id`, ordered by canonical batch/file order. No completed staging marker is written before every input passes admission and evidence rechecks.

Each staged line has exactly `batch_fingerprint`, `artifact_id`, `line_number`, `byte_offset`, `physical_byte_length`, `state` (VALID or REJECTED), `recognized_service_id`, `recognized_event_id`, `semantic_sha256`, `reason_codes` and `event`. Recognized IDs are both null or both validated identity strings. Event is null for rejection; otherwise it contains the ten event columns in schema order, with `event_time` replaced by signed INT64 `event_time_epoch_us`. This integer is a transport representation of the validated instant, not a changed business timestamp. Reasons are empty for VALID; semantic hash is null for REJECTED. Spark uses an explicit nested schema, not inference or raw-event parsing.

Stage records contain no unknown field names/values or raw rejected text. Write bounded canonical JSON records (at most 2 KiB each) incrementally, with fixed I/O buffering; never hold all records or a whole raw file in Python. Spark reconstructs UTC timestamps from epoch microseconds, joins immutable bounded batch descriptors for receipt/source metadata, and validates every staged field against the frozen inventory. Rehash/recheck staging before and after Spark actions; no unchecked directory glob. A zero-row stage file is valid; construct explicit schema-bearing empty frames rather than inferring its schema.

The typed ten-field event/comparison model is format-neutral. Physical occurrence v1 deliberately describes JSONL only. E3 must add an explicitly versioned Parquet profile/location contract (file, row group, row ordinal) rather than fabricate line numbers or reinterpret v1. Its adapter can feed the same typed semantic model with distinct physical provenance/fingerprints; no raw Parquet ingestion or cross-format equivalence is implemented here.

Failure halfway through any file makes the whole stage incomplete. No Spark processing or product reservation follows a failed index. Preserve partial safe staging for restricted owner investigation, never treat it as replayable input or automatically resume it. Completed private staging is disposable operational evidence, not a dependency required by consumers. The writer may remove its own successfully consumed private attempt only under an explicitly approved cleanup procedure; E2 initially leaves it for manual cleanup. Raw registered files are never deleted or replaced by staging/Parquet.

### Alternative assessment and scale limits

| Dimension | Streaming indexer (recommended) | Spark file-aware alternative | Higher-level mature reader |
| --- | --- | --- | --- |
| Physical numbering | One counter per file over exact binary LF spans; explicit offsets | Text offsets alone do not give line number; needs stable split-prefix/index algorithm | Must demonstrate byte/line semantics; no such integrated installed reader identified |
| Endings / malformed data | Explicit LF/CRLF/EOF, bounded UTF-8/JSON checks, exact failure position | Hadoop framing/decoding defaults require separate proof, including invalid bytes/CRLF splits | Library defaults often skip blanks/normalize endings or duplicate keys |
| Memory | One bounded line/parsed object and streaming buffer; bounded batch metadata | wholeTextFiles/binaryFile can materialize 256 MiB plus decoded copies; custom reader needs bounded split handling | Streaming mode could work, but must expose original byte positions without whole-file buffers |
| Multiple files | Serial deterministic traversal of at most 128; future parallel per-file indexers possible after review | Parallel split execution possible after proving numbering/guard protocol | Parallelism depends on library and still needs integrity guards |
| Half-file failure | No completed stage/product, exact safe location only | Failed tasks/retries must not produce ambiguous duplicate lineage | Must not return a successful prefix |
| Rechecks / replay | Raw hashes/stamps/line counts and private stage inventory before/after; replay re-indexes raw | Same evidence required on every worker-visible file, not just driver path | Checksums and immutable ownership still required externally |
| 100K / 1M | Finite admitted preparation, no throughput claim; serial parsing/staging may dominate | Better potential parallel parsing, substantially more engineering and split tests | New dependency/operations cost without demonstrated benefit |

Spark begins only at approved tagged staging. Spark normalization, joins, dedup and aggregation can partition/shuffle records, while the serial indexer remains a coordinator bottleneck. In E2 use the established local runtime, recording driver/executor configuration. Two local Spark threads are not distributed evidence. Multi-host use requires separately approved immutable shared staging and worker visibility checks; a driver's private path does not provide that. E3's 103,000 and E4's 1,030,000 physical-row plans fit the proposed row cap, but byte/memory/disk preflight can still block them. Neither workload has run in Stage A.

## Event identity, rejected identities and disposition equations

Semantic comparison is the complete typed tuple in ten-field order. Its canonical comparison bytes are `canonical_bytes([schema_version,event_id,service_id,environment,event_time_epoch_us,method,route_id,status_code,duration_ms,is_synthetic])`. Epoch microseconds can be negative in this event tuple; that exception does not permit negative metadata counts. Timestamp lexical spelling is already unique. JSON key order/whitespace/escapes and delivery provenance do not affect equality. Compare all typed fields, not only a potentially colliding digest; SHA-256 of these bytes is a lineage cross-check.

Recognize a rejected identity only from a parsed object containing exactly one valid, allowlisted service_id and one canonical event_id. Wrong/missing other fields do not hide that key. Malformed syntax, non-object input, absent/bad identity or duplicated identity keys must not be salvaged by regex or arbitrary key choice. Duplicated identity keys abort structurally as above.

| Selected occurrences | Disposition |
| --- | --- |
| Same key, all valid, all ten fields equal | One event; one canonical ACCEPTED occurrence; all others DUPLICATE |
| Same key, two valid typed variants | EVENT_IDENTITY_CONFLICT, including differences in environment/time/status/duration/method/route |
| Same event_id, different service_id | Distinct events, even across sources/batches |
| One rejected occurrence with recognizable key, no other occurrence with that key | REJECTED only; no event |
| Recognizable rejected key also appears valid | EVENT_IDENTITY_CONFLICT; no completed product |
| Two or more rejected occurrences with same recognizable key | EVENT_IDENTITY_CONFLICT; cannot establish semantic equality |
| Rejections with no recognizable key | Separate rejected occurrences; never deduplicated or assumed equal |
| Same complete registered batch tuple selected twice | Collapse selection before occurrence accounting; no second delivery |
| Same bytes under two registered batch bindings | Separate physical occurrences; valid semantic duplicates collapse for metrics |

Canonical ACCEPTED representative is the minimum tuple `(source_id,batch_id,batch_fingerprint,artifact_id,line_number)` by ASCII strings then integer line. This orders already observed positions, not fabricated Spark line positions. It chooses a lineage representative after exact equality, never a winning payload. Other valid identical deliveries are DUPLICATE. First/last receipt times are min/max over all valid deliveries, not the chosen representative.

For every successful publication, let P = physical occurrences, V = valid occurrences before semantic collapse, U = unique valid events, D = identical duplicate occurrences, R = rejected occurrences:

~~~text
P = V + R
V = U + D
P = U + D + R
count(ACCEPTED) = U
count(DUPLICATE) = D
count(REJECTED) = R
sum(events.valid_delivery_count) = V
sum(requests.request_count) = U
sum(requests.server_error_count) = count(unique events with 500 <= status_code <= 599)
~~~

Each parsed raw position must have exactly one occurrence row and vice versa. Per-batch/file counts reconcile to registered physical_line_count; reasons count rejected occurrences carrying each code, so summed reason counts may exceed R. Rejected identities contribute neither requests nor reference-event coverage. Duplicate/source/receipt coverage must not be confused with unique-event metrics.

After successful admission/indexing, compute all conflicting recognized keys before reservation. A failed conflict attempt can return only a bounded safe conflict_summary with exactly `conflicting_identity_count` (number of distinct keys meeting either conflict rule), `physical_occurrences`, `valid_occurrences`, `rejected_occurrences` and `complete_scan:true`, without keys/payloads. Counters are canonical decimal strings in the public envelope. No completed product exists. For early privacy/structural/I/O failures, do not claim total counts; the generic error envelope has no counts. There is no new persistent failed-run registry. Restricted input location plus physical position is sufficient for authorized manual investigation without exposing invalid identity strings.

## Immutable service-reference snapshot v1

Implemented `service_reference.validate/publish/open_snapshot` is a separate standard-library-only boundary. The public forms follow E1a's argument/result shape but return service rows:

~~~text
validate(csv_path, vocabulary_path, reference_id, revision, maintainer_id, note=None)
publish(root, csv_path, vocabulary_path, reference_id, revision, maintainer_id, note=None)
open_snapshot(root, reference_id, revision, expected_fingerprint)
~~~

Open returns detached immutable rows, manifest/provenance and selected fingerprint only after full revalidation. It works without original authoring paths, Spark, Gold or a database.

Literal unquoted first record is `service_id,service_name,owner_team,service_tier`. Four columns in exactly that order; no extra/missing/reordered/repeated/quoted header, BOM or empty file. Use the [E1a exact dialect/text/whitespace policy](publisher-reference-snapshots.md#csv-schema-and-dialect), substituting exactly four fields: comma/double-quote/doubled-quote, no escape character or whitespace skipping, LF/CRLF, optional final newline, no embedded newline/lone CR/blank records, fixed prohibited controls/scalars and fixed boundary-whitespace set. Do not delegate these rules solely to csv strict mode. Literal NULL is ordinary text, not a null sentinel. Empty optional cells, quoted or unquoted, mean null. Preserve exact original bytes.

| Field | Contract |
| --- | --- |
| service_id | Required event-compatible service ID; unique throughout snapshot, even identical duplicate rows fail whole file |
| service_name | Required 1–200 Unicode scalar values of nonblank plain text under exact inherited text policy |
| owner_team | Empty/null or exact captured token, 1–64 ASCII characters; reviewed organizational label, never a personal name/email |
| service_tier | Empty/null or critical, standard, experimental |

Approved vocabulary v1 is exactly the following canonical UTF-8 JSON, no final newline. These are fictional team tokens approved for synthetic use; adding real teams requires a reviewed new vocabulary/version and supported validator definition.

~~~json
{"owner_team":["example-platform","example-service"],"schema_version":1,"service_tier":["critical","experimental","standard"],"version":1}
~~~

Require exact keys/version/content, unique ASCII-sorted arrays, token syntax `[a-z][a-z0-9-]{0,63}` and no runtime mutation of supported v1 lists. Bounds: CSV 10 MiB, 10,000 data rows, physical record 4,096 content bytes; vocabulary 32 KiB; manifest 16 KiB; completion 4 KiB. Header-only is valid, zero rows; a selected missing/corrupt snapshot is never equivalent to it.

Snapshot layout is `<reference-root>/reference_id=<id>/revision=<revision>/{reference.csv,vocabulary.json,manifest.json,complete.json}`. Exact membership is those four regular files. Validate the whole candidate before exclusive revision reservation. Write exclusive files, bounded read-back/revalidate, recheck originals and destination guards, write/verify complete.tmp exclusively and rename completion last. A stale complete.tmp makes public opening fail even if complete.json exists. No versioned parent permits rebinding a root-wide reference/revision.

Manifest has exactly `manifest_version:1`, `csv_schema_version:1`, `validator_revision:1`, `reference_id`, `revision`, `semantic_config`, `csv`, `vocabulary` and `authoring`. Semantic config is exactly `{"csv_dialect":"service-csv-v1","null_policy":"empty-only-v1","text_policy":"scalar-text-v1"}`. CSV descriptor is exactly `file:"reference.csv"`, `byte_length`, `row_count`, `sha256`; vocabulary descriptor exactly `file:"vocabulary.json"`, `byte_length`, `sha256`, `version:1`. Authoring is exactly `maintainer_id` (non-sensitive 1–64 identifier) and `note` (null or 1–256 scalar plain text with the inherited policy). Note is reviewed provenance, not arbitrary payload/credentials.

Fingerprint is SHA-256 of exact canonical manifest bytes. Completion has exactly `completion_version:1`, `snapshot_fingerprint`, `manifest_byte_length`, `created_at` (strict UTC). Completion time is excluded from snapshot identity; product selection additionally binds exact completion bytes. Same binding/candidate yields fully verified read-only replay; differing valid identity yields REVISION_CONFLICT. Different revision produces a new snapshot. Missing/invalid files, unsupported vocabulary, identity mismatch and incomplete state fail without repair. Classify INPUT_UNAVAILABLE/INPUT_CHANGED for authoring evidence and SNAPSHOT_UNAVAILABLE/SNAPSHOT_INVALID/SNAPSHOT_CHANGED for stored evidence; ordinary read/close failure is never a fabricated change. New write/read-back/finalization I/O is PUBLICATION_FAILED.

Reference join is exact service_id after event deduplication, one row per service. MATCHED depends on right-key presence; nullable owner_team/service_tier never imply MISSING. MISSING retains the request with all three reference attributes null. A single selected tuple applies to every output row, including unmatched rows and all historical event times. No historical as-of ownership, environment inference or event-category inference. Unused keys produce coverage rows, never events/windows.

## Exact persisted Parquet contracts

Three datasets are necessary and sufficient. Coverage and quarantine are verified projections, not extra stored datasets. Raw JSONL remains a distinct immutable dependency; normalized Parquet does not replace it.

Types below are Spark types with DuckDB equivalents: integer/INTEGER, long/BIGINT, string/VARCHAR, boolean/BOOLEAN, timestamp/UTC microsecond TIMESTAMPTZ (TIMESTAMP WITH TIME ZONE), double/DOUBLE, decimal(38,0)/DECIMAL(38,0), `array<string>`/VARCHAR[]. Inspect every physical file's complete ordered schema and timestamp logical annotation; reject INT96, TimestampNTZ, milliseconds/nanoseconds, absent UTC annotation, adjusted-to-UTC=false, extra/missing/reordered fields, schema union and implicit casts. Parquet physical nullability is allowed; requiredness is enforced on every value. Arrays have nonnull elements. Use explicit schemas and flat exact file lists; disable hive inference, missing/corrupt skipping and schema merging. Every dataset has at least one schema-bearing file, even when empty.

Physical representation is INT32 for integer, INT64 for long, UTF-8 annotated BYTE_ARRAY for string, BOOLEAN for boolean, DOUBLE for double, 16-byte FIXED_LEN_BYTE_ARRAY with DECIMAL(38,0) for sums, and standard three-level LIST of UTF-8 strings for reason arrays. Every timestamp is INT64 with TIMESTAMP(MICROS,isAdjustedToUTC=true). Inspect nested array/schema structure too. E2 must use an isolated Spark SQL session with UTC, outputTimestampType=TIMESTAMP_MICROS, datetimeRebaseModeInRead=CORRECTED and datetimeRebaseModeInWrite=CORRECTED (all three latter keys prefixed spark.sql.parquet), ANSI arithmetic enabled and no legacy calendar rebasing. Do not change Gold/Content session defaults.

This explicit timestamp choice is necessary because [Spark 4.0.2 defaults to INT96 and documents corrected calendar modes](https://spark.apache.org/docs/4.0.2/sql-data-sources-parquet.html#configuration). [DuckDB documents its UTC-aware microsecond timestamp type](https://duckdb.org/docs/current/sql/data_types/timestamp.html); its pinned 1.4.5 mapping and Python UTC serialization must be demonstrated in R07/R17, including years 0001/9999 and pre-epoch values. Do not copy Content's TIMESTAMP schema constants into this new reader or silently cast a timezone-naive physical file into compliance. Internal conversion to an epoch-microsecond comparison representation is allowed only after strict physical-type validation. Stage A executed no runtime compatibility test; Stage B evidence below verifies the physical representation and primitive UTC serialization.

Dataset row/file order is not semantic. Strings compare binary without collation changes. All rows have contract_version exactly 1; no infinities/NaN. Schema/value/version failures abort the product.

### reliability_events_v1

Grain/key: one unique valid `(service_id,event_id)` across selected history. Stored internally for exact reconciliation, independent rank checking and future E3 logical equivalence. No public event-catalog report in E2. It contains all ten unchanged validated event fields, delivery summaries and snapshot context; no arbitrary “chosen source” substitutes for multi-occurrence lineage.

| Position | Column | Type | Required | Meaning |
| --- | --- | --- | --- | --- |
| 1 | contract_version | integer | Yes | 1 |
| 2 | schema_version | integer | Yes | Original event value 1 |
| 3 | event_id | string | Yes | Original canonical UUID |
| 4 | service_id | string | Yes | Original technical key |
| 5 | environment | string | Yes | Original allowed environment |
| 6 | event_time | timestamp | Yes | Original UTC completion instant |
| 7 | method | string | Yes | Original method |
| 8 | route_id | string | Yes | Original approved opaque token |
| 9 | status_code | integer | Yes | Original 100–599 |
| 10 | duration_ms | long | Yes | Original 0–3,600,000 |
| 11 | is_synthetic | boolean | Yes | Original true |
| 12 | valid_delivery_count | long | Yes | Positive count of valid occurrences for this identity, including duplicates |
| 13 | first_received_at | timestamp | Yes | Min receipt over those occurrences |
| 14 | last_received_at | timestamp | Yes | Max receipt over those occurrences; at least first |
| 15 | service_name | string | No | Exact matched name; null only when MISSING |
| 16 | owner_team | string | No | Exact optional reference attribute |
| 17 | service_tier | string | No | Exact optional reference attribute |
| 18 | reference_status | string | Yes | MATCHED or MISSING |
| 19 | reference_id | string | Yes | Selected ID |
| 20 | reference_revision | string | Yes | Selected revision |
| 21 | reference_fingerprint | string | Yes | Selected fingerprint |

Exact equality of all ten business fields is required against every valid raw delivery. Null-safe bidirectional expected/actual row comparison includes all 21 columns and cardinality. No event can exist without an ACCEPTED raw occurrence; no valid occurrence can lack its matching event.

### reliability_occurrences_v1

Grain/key: `(batch_fingerprint,artifact_id,line_number)`. Restricted operational evidence for every raw physical occurrence, including valid duplicates and rejections. All input paths, business IDs and hashes here remain operator-local; no public quarantine/catalog/download. Rejected projection is `disposition = REJECTED`; it is not a separate dataset.

| Position | Column | Type | Required | Meaning |
| --- | --- | --- | --- | --- |
| 1 | contract_version | integer | Yes | 1 |
| 2 | batch_fingerprint | string | Yes | Selected registered manifest hash |
| 3 | source_id | string | Yes | Registered delivery source |
| 4 | batch_id | string | Yes | Registered batch |
| 5 | ingestion_id | string | Yes | Independently recomputed batch UUID |
| 6 | artifact_id | string | Yes | Approved file identity within batch |
| 7 | input_file_sha256 | string | Yes | Exact retained raw file digest |
| 8 | line_number | long | Yes | One-based physical line |
| 9 | byte_offset | long | Yes | Zero-based raw byte offset |
| 10 | physical_byte_length | integer | Yes | Content plus ending, positive and at most 16,386 |
| 11 | received_at | timestamp | Yes | Exact batch receipt |
| 12 | disposition | string | Yes | ACCEPTED, DUPLICATE or REJECTED |
| 13 | service_id | string | No | Valid identity component if recognized |
| 14 | event_id | string | No | Valid identity component if recognized |
| 15 | semantic_sha256 | string | No | Canonical typed ten-field digest for valid delivery; null for rejection |
| 16 | reason_codes | `array<string>` | Yes | Empty for valid; nonempty fixed ordered unique codes for rejection |

For valid rows both identity fields/hash are required. Rejections have both identity fields null or both recognized; never store partially recognizable/invalid strings. No raw line, unknown key name, raw token, method/route payload, exception message or source path appears in rejection evidence. Contiguous positions and offsets must match a fresh binary index, not just min/max/count. Byte lengths sum exactly to file length, including all allowed endings; a zero-byte file has no occurrence rows.

There is exactly one ACCEPTED per event under the deterministic representative order; all other valid occurrences are DUPLICATE. A recognized rejected key cannot appear in any other occurrence in a successful publication. Null-identity rejected rows remain distinct by physical key.

### reliability_requests_5m_v1

Grain/key: `(service_id,environment,window_start)`. Exactly one nonempty group per deduplicated event-time window.

| Position | Column | Type | Required | Meaning |
| --- | --- | --- | --- | --- |
| 1 | contract_version | integer | Yes | 1 |
| 2 | service_id | string | Yes | Exact event service |
| 3 | environment | string | Yes | Exact event environment |
| 4 | window_start | timestamp | Yes | UTC mathematical-floor five-minute start |
| 5 | request_count | long | Yes | Positive unique-event count |
| 6 | server_error_count | long | Yes | 0–request_count; unique statuses 500–599 |
| 7 | server_error_rate | double | Yes | Finite error_count / request_count |
| 8 | duration_sum_ms | decimal(38,0) | Yes | Exact nonnegative duration sum |
| 9 | mean_duration_ms | double | Yes | Finite duration_sum / request_count |
| 10 | p95_duration_ms_approx | long | Yes | Observed allowed duration passing independent rank test |
| 11 | service_name | string | No | Exact reference name |
| 12 | owner_team | string | No | Exact optional team |
| 13 | service_tier | string | No | Exact optional tier |
| 14 | reference_status | string | Yes | MATCHED/MISSING |
| 15 | reference_id | string | Yes | Selected ID |
| 16 | reference_revision | string | Yes | Selected revision |
| 17 | reference_fingerprint | string | Yes | Selected fingerprint |

Reference attributes/status/tuple must equal the selected reference for every event/group. Check join cardinality separately and with complete comparisons. A name cannot be null for MATCHED; all three attributes must be null for MISSING. Empty and rejected-only history produces empty events/metrics, schema-bearing files and a complete occurrence dataset of zero/R rows respectively. Empty dataset provenance remains in inputs.json.

## Five-minute arithmetic and independent p95

For event epoch microseconds t, window_start is `floor(t / 300000000) * 300000000`, using exact integer/decimal mathematical floor. Do not use float division, truncation toward zero, host timezone or Spark processing time. Pre-epoch t = −1 belongs to `1969-12-31T23:55:00.000000Z`; t = 0 starts `1970-01-01T00:00:00.000000Z`. Exactly `12:05:00.000000Z` is in the next half-open group, while `12:04:59.999999Z` is in the preceding group. Year 9999's final group starts at 9999-12-31T23:55:00.000000Z and includes the event instant 9999-12-31T23:59:59.999999Z. Only representable window_start is stored. The exact exclusive query-end sentinel 10000-01-01T00:00:00.000000Z makes that final group queryable, as specified below.

Deduplicate first. Count 5xx only for 500–599, never 4xx or a missing event. Sum durations and reconciliation counts in DECIMAL(38,0), check decimal overflow explicitly with ANSI/error behavior, and check 0–2^63−1 before casting counts to long. The row cap further bounds valid maxima, but does not replace overflow guards/tests. duration_sum_ms remains decimal and is not narrowed to long. No zero-filled missing windows; no method/route/source inferred grouping.

Independent counts/sums are exact. Rate and mean use the exact numerators/denominators converted to finite doubles; validate `abs(actual - expected) <= max(1e-12,abs(expected)*1e-12)`. With the event duration bound, means lie in [0,3,600,000], rates in [0,1]. Wider-scope rates/means are recomputed from summed numerators/denominators, never an unweighted average. No report combines or averages p95 values.

The approved Spark candidate is `percentile_approx(duration_ms, 0.95, 10000)` under Spark 4.0.2. This is a release candidate to prove against the contract, not an assertion that Spark promises this exact nearest-rank displacement. See the existing [product percentile decision](../data-products/application-reliability.md#percentile-accuracy-and-validation). Algorithm, probability, accuracy, engine and rank policy are identity-bearing. Operational partition counts/input order may vary and are recorded only in local validation evidence.

For N group events:

~~~text
r = ceil(95*N/100)                     # exact integer/DECIMAL arithmetic
b = max(1, ceil(N/10000))
L = count(duration < v) + 1
H = count(duration <= v)
displacement = max(L-r, r-H, 0)
accept only if v is observed in group and displacement <= b
~~~

An unobserved value fails even if its numerical position appears plausible. Singleton/all-equal groups must return their sole observed value. Ties use the whole interval [L,H], not an arbitrary tie rank. Rank tolerance is not a percentage of milliseconds; small rank error in a heavy tail can produce large duration error. A group with durations 1–20 has r=19, b=1: values 18,19,20 pass; 17 and unobserved 19.5 fail. Do not assert candidate output is necessarily 19.

The reader can independently compute L/H/observed membership by SQL joining each event to its own aggregate key and counting comparisons to stored v. This is O(N) row examination plus grouping, without bringing durations into Python or comparing Spark results to Spark. Small fixtures use independent literal/sorted standard-library results. Larger exact nearest-rank evidence uses an independently implemented bounded external merge sort or duration histogram with cumulative integer counts per group; do not hold all groups' durations in a Python list. Disk/memory used by that future benchmark oracle must be measured separately. Production reader verification remains in-memory/no-spill and fails on resource exhaustion.

Stage B must exercise singleton, all-equal, repeated/tied, sparse, heavy-tail, zero/max-duration groups under shuffled input and multiple partition layouts, record exact p95, approximate value, L/H/displacement and absolute millisecond error. A candidate failing the fixed budget blocks checkpoint C/release and requires owner algorithm/accuracy review. No silent tolerance widening, test omission or fallback to unverifiable engine agreement.

### Approximation, fingerprint and replay

The fingerprint identifies selected inputs and the approved equivalence contract. It does not identify one uniquely determined approximate value or output encoding. First successful completion in a root freezes one qualified value per group and exact file hashes. Subsequent queries return that stored value.

Replay must reverify raw/reference dependencies, exact stored events/occurrences, exact metric keys/counts/sums/reference fields and tolerated rates/means, then independently verify the stored p95 rank. It must not overwrite aggregates or reject valid stored p95 solely because a fresh Spark computation returns a different qualifying value. Replay does not require a fresh approximate computation at all; any optional experimental recomputation is not a replay acceptance condition. For new publication read-back, expected-frame comparison excludes only p95 from exact equality and both produced/read-back candidates must independently meet the rank contract. All other fields are compared fully (rates/means by their fixed numerical tolerance).

Fresh publication under an identical fingerprint in a different root/layout may have different qualifying approximate p95 and Parquet bytes. Completion distinguishes actual outputs; input identity alone does not promise bit-identical regeneration. Within one root an existing complete result is reused read-only; never create multiple variants at the same path or silently “improve” its percentile. A client pins a root plus fingerprint/session, not a union across independently generated copies.

## Product identity, completion and immutable layout

### Canonical inputs.json v1

Supported product/processor/manifest/completion/dataset revisions are exactly 1. The product ID is `application-reliability`. inputs.json contains exactly:

| Key | Schema |
| --- | --- |
| manifest_version, product_contract_version, processor_revision | Integer 1 |
| product_id | application-reliability |
| input_contract_versions | Exactly `{"registered_batch":1,"request_event":1,"service_reference":1}` |
| dataset_contract_versions | Exactly `{"reliability_events_v1":1,"reliability_occurrences_v1":1,"reliability_requests_5m_v1":1}` |
| engine | Exactly `{"spark":"4.0.2"}` |
| origin | synthetic |
| batches | Sorted nonempty unique selection entries defined below |
| reference | Selected reference object defined below |
| semantic_config | Exact object below |

Each batch entry has exactly `source_id`, `batch_id`, `batch_fingerprint` and `completion`. Completion descriptor is exactly `byte_length` and `sha256` of the currently verified registered complete.json. Entries sort by (source_id,batch_id,batch_fingerprint). Fingerprint selects exact canonical retained batch manifest; it transitively binds its ordered file inventories/lengths/digests/line counts, source/profile/config, receipt and ingestion UUID. Product verification must follow this link and revalidate the manifest/files, not merely accept a supplied hash. No selected inventory is summarized away.

Reference object has exactly `reference_id`, `revision`, `snapshot_fingerprint` and `completion`, whose descriptor is the same two-field length/hash shape. Snapshot identity transitively binds exact CSV, vocabulary, validator policy and authoring. Bind exact completion bytes too, so the retained dependency evidence cannot silently switch while keeping its logical manifest hash.

Exact semantic_config is:

~~~json
{"admission_policy_revision":1,"event_identity":"service-event-v1","invalid_identity":"repeat-recognized-abort-v1","occurrence":"jsonl-physical-v1","percentile":{"accuracy":10000,"algorithm":"spark-percentile-approx","probability_denominator":100,"probability_numerator":95,"rank_policy":"nearest-rank-interval-v1"},"reference_application":"snapshot-service-v1","rejections":"record-safe-rows-v1","semantic_equality":"typed-ten-epoch-us-v1","window":"utc-floor-300000000us-v1"}
~~~

Product fingerprint is exactly lowercase `SHA256(canonical_bytes(inputs_object)).hexdigest()`. Store those same bytes; no embedded self-fingerprint, newline or alternate JSON formatting. Hashes of retained batch/reference manifests bind their nested field order through canonical serialization, while arrays retain prescribed inventory order. No output bytes or approximate values enter this fingerprint.

Exclude absolute machine roots, original authoring paths, operational attempt IDs, log levels, processing start/completion times, Spark partitions/compression and query page settings. Receipt times and completion-byte descriptors are dependency provenance, intentionally bound; they are not replaced by invocation clocks. A differently registered receipt/copy or physically different but semantically equal raw inventory can yield a new product. Semantic settings cannot be changed under revision 1. E3 may introduce a new supported input profile/revision while preserving the ten-field typed comparison; it must not pretend physical JSONL and Parquet fingerprints are equal.

Do not copy raw dependencies or duplicate their full manifests into every product. The small explicit selection with exact manifest and completion identities is sufficient only because all dependencies are independently retained and reopened. This trades portability for less duplication. No dependency-free mode is proposed; moving identical dependencies to another approved root is permitted, deleting them makes reads/replay unavailable.

### Layout and exact completion schema

~~~text
<reliability-root>/v1/processor_revision=1/publication=<fingerprint>/
  inputs.json
  reliability_events_v1/
    part-....parquet
  reliability_occurrences_v1/
    part-....parquet
  reliability_requests_5m_v1/
    part-....parquet
  complete.json
~~~

The root must exist. Reserve the absent fingerprint directory exclusively after admission/conflict/expected-frame gates. Each flat dataset has one or more listed Parquet files; allowed bookkeeping names are only `_SUCCESS`, `._SUCCESS.crc` and `.<listed-parquet-basename>.crc`. They must be regular, safe files with stable membership/stamps. No arbitrary hidden files, _temporary, nested partitions, symlinks, hard links or unlisted analytical files. Completed top-level inventory is exactly the five names shown. complete.tmp is permitted privately during finalization only and makes a public open fail even next to complete.json.

Completion has exactly:

| Key | Schema |
| --- | --- |
| completion_version | Integer 1 |
| product_id | application-reliability |
| product_fingerprint | Selected fingerprint, directory identity and SHA-256(inputs.json) |
| manifest_byte_length | Exact positive inputs.json length |
| occurrence_counts | Exact object: physical_occurrences, valid_occurrences, unique_valid_events, identical_duplicate_occurrences, rejected_occurrences; nonnegative INT64 integers satisfying all equations |
| output_counts | Exactly the three dataset names, independently verified nonnegative integer row counts |
| files | Exactly the three dataset names, each a nonempty sorted array of file entries |
| completed_at | Strict UTC timestamp, written after verification |

Each inventory entry has exactly `path`, `byte_length` (positive), `sha256`. Path is dataset/basename only, at most 256 UTF-8 bytes, no traversal/aliases/globs; each array sorts by UTF-8 path bytes with no duplicates. Hash entire files. Count events=U, occurrences=P, groups<=U; markers cannot override observed values. Successful conflicting identity count is necessarily zero, not an independently mutable completion field. Coverage/reason/time diagnostics are queries, not redundant completion counters.

Completion time is outside product identity, not authenticated and not an article/event timestamp. Exact byte/identity rechecks detect changes during an operation; fingerprints cannot establish historical authenticity of a self-consistent complete rewrite. Actual output hashes freeze the approximate result and physical encoding. No automatic latest, product registry, run database, transactional activation or mutable pointer.

### Writer, full verification and replay

Implemented public interface:

~~~text
reliability_publication.publish(
    spark, *, batch_root, batches, reference_root, reference_id, revision,
    reference_fingerprint, staging_root, output_root)
~~~

Return only a small immutable summary: status published/replayed, product_id, product_fingerprint, origin, label, occurrence_counts, output_counts and invocation-only duplicate_batch_selections. No unverified frame/data escapes as verified output. The library caller owns Spark; CLI owns its session and keeps runtime logs off structured stdout.

1. Validate explicit selections/paths/bounds, fully open each registered batch and selected service reference, freeze detached metadata/rows/file lists and evidence. Build identity bytes.
2. Stream/index all retained raw files to safe private staging, rechecking every dependency before/after. Spark reads only the approved stage inventory. Reject inconsistent stage transport values/schema; create exact typed candidates and occurrence frame.
3. Validate all event fields, conflict rules, representative order and reconciliation before product reservation. Build expected deduplicated enriched events and exact group fields with explicit schemas; validate reference uniqueness/cardinality, counts and overflow. For a new result compute/validate candidate p95 rank; completed replay validates stored ranks without needing a fresh approximation. Only bounded reference/file metadata and scalar counts reach Python.
4. Existing absent-completion/temporary/invalid namespace fails without writes. For completed replay, independently verify stored evidence and repeat all relevant exact comparisons/rank checks against retained dependencies. Return replayed only after final guards; do not write bytes or modify file/directory mtimes. Replay may prepare a new private staging attempt, never alter the product/dependencies.
5. For a new namespace, exclusive mkdir and exclusive inputs.json create. Spark writes three explicit-schema datasets with errorifexists. Never overwrite or clean another reservation. Check root/namespace guards before/after each action.
6. Freeze exact output inventory/hashes and inspect every physical schema before reading approved files. Check all values, requiredness, versions, keys, complete raw-position coverage, all occurrence dispositions/reasons, all ten event values, receipt summaries and selected reference context.
7. Bidirectionally compare complete occurrence/event rows against expected multisets, including nulls/array order/multiplicity. Compare exact aggregate keys/counts/sums/context in both directions; validate doubles by fixed tolerance and p95 independently against typed events, not equality to a different qualifying approximate computation. Verify no output key is missing/extra even when global totals match.
8. Run Spark-free verification of the pending read-back artifacts through the same strict core used by the independent reader, with an internally constructed draft completion descriptor. It must stream-check raw occurrences and rank tests; it cannot trust Spark's expected frames or bypass schema/inventory/dependency gates. The public opener never permits pending output.
9. Recheck all dependency bytes/identities/parents/inventories and all output metadata/files/guards after actions. Exclusively write canonical complete.tmp from independently verified counts/inventory, close/read back/validate it, then recheck all evidence immediately before final rename. Require complete.json absent under the sole-writer assumption. Rename in the same directory last.
10. Return success only after successful rename. A close/finalization/shutdown error must not cause deletion of possibly complete output. After ambiguous outcome, explicit full open/replay determines state.

No aggregate is computed before deduplication. A failed operation cannot return a partial verified page, a partial quarantine success or promote incomplete output.

| Situation | Required behavior |
| --- | --- |
| Identical completed selection | Full dependency/output verification, stored-rank verification, read-only replay |
| Changed raw batch/profile/receipt/reference selection | Different canonical identity; separate publication; prior completed publications retained |
| Missing/corrupt raw batch or reference | Unavailable/invalid dependency, never an empty substitute |
| Changed evidence after observation | DEPENDENCY_CHANGED or PUBLICATION_CHANGED, even same-byte inode replacement |
| Ordinary read/close/stat failure without observed change | Availability/integrity/I/O failure, never a mutation claim |
| Partial Spark write, missing completion or stale complete.tmp | Incomplete reservation; no resume/auto-promotion |
| Invalid schemas/rows/ranks/counts despite refreshed digests | PUBLICATION_INVALID on existing output; no repair/replay |
| New write/read-back/finalization failure | PUBLICATION_FAILED; incomplete reservation retained |
| Interruption before rename | Incomplete, owner investigation |
| Interruption/lost response after rename | Potentially complete; full explicit verification required |
| Late historical selection or reference correction | New product; old publication unchanged; no implicit adoption |
| Empty or rejected-only valid selection | Schema-bearing empty events/groups; exact zero/R occurrence rows and coverage |
| Unsupported contract/profile/processor/engine semantics | Explicit rejection, no inference/fallback |

One trusted local owner/writer per root; Spark task writers belong to that coordinator. Stop writers before manual investigation/quarantine or use a fresh approved root. No automatic deletion, repair, cleanup of retained dependencies, transaction, multiwriter isolation, fsync/power-loss durability or exactly-once guarantee. Rechecks do not detect mutations reverted between observations or after the final check.

## Standalone independent reader

Implemented interface:

~~~text
reliability_analytics.open_publication(
    reliability_root, product_fingerprint, *, batch_root, reference_root)
session.report(name, *, service_id=None, environment=None, start=None, end=None,
               source_id=None, batch_id=None, limit=100, offset=0)
session.close()
~~~

Return a context-managed pinned session; only the three named reports below are public. No event catalog, raw/quarantine report, SQL execution method, arbitrary path, HTTP endpoint, export or frontend is included. Caller-supplied alternate dependency IDs are not accepted after opening; derive them from the strict manifest and verify them under the supplied approved roots. All pages/reports share one immutable identity/session. Reopening another root/fingerprint is an explicit selection change.

Opening performs bounded metadata/layout/fingerprint/inventory checks, independent service-reference opening and raw-batch admission/hash/identity checks, every physical output schema and all SQL value/key/provenance/coverage/count/rank tests. It requires neither Spark/Java nor Silver/Gold/Content. No producer imports; small shared strict intake primitives may be Spark-free, but aggregate verification must be independent SQL and literal fixtures.

To verify raw correspondence without unbounded Python or a full duplicate raw table, stream-index registered files in canonical batch/artifact/line order and pair them with one ordered DuckDB cursor over occurrences left-joined to typed events. Fetch at most 1,024 safe rows and 2 MiB per chunk. The owner's Stage B efficiency gate permits this ordered strategy instead of the initially suggested per-chunk temporary table: it must not execute a full Parquet scan for each chunk. Validate string widths and schema/value rules in SQL before fetch; verify actual encoded chunk size too. Compare every position/disposition/reason/provenance field and every valid ten-field value, then require both streams end together. Exact per-file counts, unique keys, contiguous positions/offsets and total coverage prevent extra/missing occurrences from hiding. Independently derive representative order, delivery counts, receipt min/max and conflict rules with set-based SQL. Digest equality alone is insufficient. This serial pass is not distributed Spark processing. Full semantic verification uses a fixed set of SQL scans; raw comparison uses one join/sort/cursor, independent of chunk count.

SQL derives expected five-minute keys/counts/errors/DECIMAL sums/context from verified typed events, compares complete key sets and exact fields in both directions, checks tolerance-based rates/means, and counts ranks against stored approximate values. No unrestricted SQL or duration collection into Python. Whole-publication admission/reconciliation is mandatory even for a one-row filtered report.

Use one in-memory DuckDB connection: UTC, threads=2, memory_limit=512MiB, disabled temporary/disk spilling and extension auto-install/autoload. No network, persistent database or new worker process. The memory setting is not a hard process-RSS bound. The synchronous session has a simple nonblocking one-active-request guard; concurrent calls return BUSY, with no queue/scheduler. No hard open/report deadline or latency SLA. Measure observed durations later.

Before/after each report recheck all retained raw/reference/output evidence, exact metadata bytes, hashes, stamps, parent identities and membership. Initial complete SQL verification remains valid only while exact evidence is unchanged. Any integrity/query/resource/response failure discards all pending rows, closes/invalidates the session and returns one safe failure; later reports cannot use it. Invalid arguments/BUSY do not disturb the active valid session. No response is returned before post-query checks and encoded-size checks succeed. Closing is idempotent; calls after closing fail QUERY_FAILED.

### Reports, filters and precise projections

Every report is paged. All string ordering is binary. Bound every filter/LIMIT/OFFSET as a parameter; only fixed SQL/report identifiers occur in query templates. Unknown parameters, unsupported filter combinations, duplicate CLI options, bool/float counters and invalid types fail ARGUMENT_INVALID before opening/querying.

| Name | Exact row columns in order | Deterministic ordering |
| --- | --- | --- |
| requests_5m | window_start, service_id, environment, request_count, server_error_count, server_error_rate, duration_sum_ms, mean_duration_ms, p95_duration_ms_approx, service_name, owner_team, service_tier, reference_status | window_start ASC, service_id ASC, environment ASC |
| input_coverage | source_id, batch_id, batch_fingerprint, ingestion_id, received_at, input_file_count, input_byte_length, physical_occurrence_count, valid_occurrence_count, accepted_representative_count, duplicate_occurrence_count, rejected_occurrence_count, first_event_time, last_event_time, future_occurrence_count, min_receipt_delay_us, max_receipt_delay_us, rejected_reason_counts | source_id ASC, batch_id ASC, batch_fingerprint ASC |
| reference_coverage | service_id, reference_status, reference_usage, service_name, owner_team, service_tier, event_count, missing_reference_event_count, missing_optional_attributes | service_id ASC |

`requests_5m` requires a strict representable UTC start and either a strict representable UTC end or exactly 10000-01-01T00:00:00.000000Z as the exclusive end. Both bounds are aligned to five-minute boundaries, start < end and span at most 31 days (2,678,400 seconds). Filter window_start >= start and < end; no partial-window claim. Optional exact service_id/environment, unknown syntactically valid service returns no rows. Other filters fail. No method/route/status filtering after those dimensions were discarded; no cross-window percentile aggregation.

The sentinel is invalid as start, event_time or stored window_start; all other year-10000 strings and every larger year are invalid. Validate Gregorian day counts and microsecond spans with integer arithmetic; never construct Python datetime(year=10000). Its epoch value is exactly 253402300800000000 microseconds. Bind integer epoch-microsecond filter values and explicitly compare epoch_us(window_start) >= start_us and < end_us. Do not bind/cast/store the sentinel as a Spark or DuckDB timestamp. Ordinary bounds retain exact six-digit UTC spelling and calendar validation. R08/R15 must exercise the final window, sentinel role restrictions, invalid larger years and the exact/one-over 31-day span.

`input_coverage` permits only optional exact source_id and batch_id, both syntactically validated. Batch_id alone selects that ID under any source. Rows cover complete registered batches, not event-window subsets; no service/environment/date filters. Counts include all physical deliveries and rejected lines. Event ranges/delays use valid occurrences including duplicates; no valid occurrences means null ranges/delays. Future count uses event_time > received_at. Receipt delay is signed exact integer microseconds received_at − event_time, not a lateness classification. Rejected_reason_counts is an object with exactly all ten fixed reason-code keys and nonnegative occurrence counts, including zeros.

Input coverage totals, before pagination but after source/batch filtering, have exactly `input_batch_count`, `input_file_count`, `input_byte_length`, `physical_occurrence_count`, `valid_occurrence_count`, `accepted_representative_count`, `duplicate_occurrence_count`, `rejected_occurrence_count`, `distinct_valid_identity_count` and `duplicate_excess_in_scope`. The last is valid_occurrence_count minus distinct valid identities within that filtered batch scope. Stored DUPLICATE is defined relative to the entire publication; if the ACCEPTED representative is outside the filter, a scoped identity may have only DUPLICATE occurrences. Thus stored duplicate count need not equal duplicate_excess_in_scope for a filtered scope. Always P_scope = accepted_representative_count + duplicate_occurrence_count + R_scope. Unfiltered, distinct identities=U and duplicate_excess=D.

`reference_coverage` permits only optional service_id, not source/environment/time filters. Use union of valid event services and all selected reference keys. USED/MATCHED means reference with events; UNUSED/MATCHED means reference only; ABSENT/MISSING means events only. Event count is unique events, never deliveries/rejections. Missing-reference count equals event count for ABSENT, otherwise zero. Missing optional attributes is the ordered list owner_team, service_tier for null attributes of matched services; empty for ABSENT.

Reference totals, before pagination and after service filtering, have exactly `event_count`, `matched_event_count`, `missing_event_count`, `event_service_count`, `matched_service_count`, `missing_service_count`, `matched_services_missing_optional_attributes_count`, `reference_service_count` and `unused_reference_service_count`. Require event count=matched+missing, event services=matched+missing services, reference keys=matched+unused. Optional-missing service count includes used matched services only. Empty/header-only reference makes all events MISSING; empty events makes all reference keys UNUSED. Rejected recognizable IDs do not fabricate used services.

Default page is 100, maximum 1,000, minimum 1. Offset is integer 0–1,000,000. SQL uses LIMIT limit+1 OFFSET offset; admit the lookahead row too, remove it, return has_more. Empty/beyond-end returns []/false. At max offset has_more may remain true; narrow filters rather than silently raising the cap. Pages do not bound validation/scan work or imply stable row order in Parquet.

### Serialization, envelopes and local CLI

Success envelope has exactly `status:"OK"`, `product_id`, `product_fingerprint`, `origin:"synthetic"`, `label` (literal above), `reference` (ID/revision/fingerprint), `report`, `filters`, `limit`, `offset`, `has_more`, `rows`, plus `totals` only for the two coverage reports. Filters includes every applicable non-pagination key, with null for absent optional filters. Mandatory metrics bounds are present. No arbitrary raw source/configuration values are echoed.

All integral report values, including counters, duration sum, percentile, signed receipt delays, page limit/offset and reason counts, are canonical decimal strings (zero "0", no leading plus/zeroes, ASCII minus only for negative delay). Rates/means are finite JSON numbers; booleans remain booleans, nulls null. UTC timestamps always have six digits and Z. Arrays retain fixed order. Public rows omit internal event IDs, raw positions, raw artifact paths and rejection payloads. Reference identifiers are constant in the envelope. Mandatory origin label applies even to empty successful reports; failures use the fixed synthetic-product envelope below.

Before Python fetch, SQL sums UTF-8 bytes of all variable string cells (including array values and the entire lookahead page) and rejects above 64 KiB. Numeric/time/reason-key representation has a fixed bounded shape; after detached materialization enforce actual canonical JSON UTF-8 length plus final LF <= 1 MiB. JSON escaping and envelope overhead count. No string truncation or partial page success.

Implemented CLI forms mirror library boundaries:

~~~text
reliability_inputs.py register --root ROOT --input-root ROOT --source-id ID --batch-id ID
  --file ARTIFACT_ID [repeat] --received-at UTC --profile FILE
reliability_inputs.py verify --root ROOT --source-id ID --batch-id ID --expected-fingerprint HEX
service_reference.py validate --csv FILE --vocabulary FILE --reference-id ID
  --revision REV --maintainer-id ID [--note NOTE]
service_reference.py publish --root ROOT [same validate flags]
service_reference.py verify --root ROOT --reference-id ID --revision REV --expected-fingerprint HEX
reliability_publication.py --batch-root ROOT --batch SOURCE/BATCH/HEX [repeat]
  --reference-root ROOT --reference-id ID --revision REV --reference-fingerprint HEX
  --staging-root ROOT --output-root ROOT
reliability_analytics.py --root ROOT --publication HEX --batch-root ROOT
  --reference-root ROOT --report requests_5m|input_coverage|reference_coverage
  [approved named filters and page parameters]
~~~

SOURCE/BATCH/HEX components cannot contain slash; no arbitrary path selection is permitted. Registration takes reviewed local configuration as an operator action, not a remote client upload. --profile is a bounded canonical local profile file under trusted approved configuration ownership; capture/recheck its bytes/identity during registration, enforce the 64 KiB cap, and persist the validated object in the batch manifest. Reference authoring paths are likewise approved local operator inputs under the path policy. --help and argument failures must not initialize Spark. Success is one JSON object on stdout; failures have empty stdout and one <=4 KiB JSON stderr diagnostic; exit 0 success, 2 invalid arguments, 1 other failures. Error envelope is exactly `status:"FAILED"`, `product_id:"application-reliability"`, `origin:"synthetic"`, `label`, `code`, `message`, with optional `conflict_summary` only for a complete conflict scan as defined earlier. No traceback, raw path, event/invalid identifier, exception string or payload; registration/reference CLI uses the same envelope and fixed component-specific codes. Runtime Spark logs remain restricted operator output, never embedded in diagnostics.

Registration/reference verify CLIs print bounded identity/count summaries, never detached manifests, rows, file paths or integrity guards. Those richer immutable objects belong only to internal library consumers. All CLI non-report success summaries enforce the same 4 KiB budget.

## Privacy, retention and conservative resource policy

Approved synthetic sources must emit only the exact schema. Exclude IP addresses; user/session/device IDs; cookies, credentials and headers; raw URLs/query strings; request/response bodies; stack traces; personal names/emails. Unknown keys do not become an extension bag. Passing a regex, dropping unknown columns or setting is_synthetic=true does not make arbitrary real logs safe.

Privacy failure stops intake and prevents ordinary quarantine/analytical publication. Never copy its unexpected field names/values into logs, manifests, staged records or failure envelopes. Existing owner-supplied unapproved raw artifacts remain restricted and untouched pending an owner-reviewed retention/removal process; E2 does not silently delete or distribute them. Restricted filesystem ownership is an explicit assumption, not an implemented authentication/authorization service.

Registered raw batches, their metadata/configuration, service snapshots and complete product artifacts must be retained for every supported read/replay. No automatic expiry, history truncation, reference correction-in-place or garbage collection. Incomplete registered/staged/output artifacts require owner investigation with writers stopped. Staging is operational and may be manually cleaned after investigation; it cannot replace raw retained evidence. Quarantine access means controlled inspection of original file/line under restricted ownership, never an ordinary catalog/export.

The following additional bounds are **owner-approved E2 policies**, not inherited acceptance from Content or measured capacities. The 16 KiB line and 256 MiB file budgets originate in the Proposed Reliability specification. All limits are inclusive and fixed by policy revision 1; exceeding any is failure, never sampling/coercion/truncation.

| Resource | Accepted bound / enforcement |
| --- | --- |
| Raw line content | 16,384 bytes excluding LF/CRLF; bounded read before decode with only ending/excess sentinels |
| Raw JSONL file | 256 MiB; 1 MiB maximum hash/I/O chunk, cumulative byte check as well as initial size |
| Selected batch/file/raw bytes | 32 unique batches; 16 files per batch; 128 selected files; 1 GiB total raw bytes, also max per registered batch |
| Physical/valid/rejected occurrences | 2,000,000 physical across selection (also per batch); V/R each <=P; U/D <=V; stop at one excess sentinel |
| Schema/parser allocations | Depth <=4, <=32 decoded object members per line, number token <=64 bytes; no whole-file decode |
| Source profile / batch manifest / batch completion | 64 KiB / 128 KiB / 4 KiB; bound bytes before JSON decoding |
| Frozen selected batch manifests | At most 4 MiB combined, plus bounded completions; no event collection in Python |
| Service reference | 10 MiB CSV / 10,000 rows / 4,096-byte record; 32 KiB vocabulary, 16 KiB manifest, 4 KiB completion |
| Product inputs / completion / private stage manifest | 64 KiB / 1 MiB / 64 KiB |
| Private staging | At most 128 files, one record per occurrence, <=2 KiB encoded per safe record, <=4 GiB combined |
| Output Parquet | <=1,024 analytical files combined and <=512 per dataset; <=4 GiB combined; relative inventory path <=256 UTF-8 bytes |
| Output rows | Events/groups <=2,000,000 each; occurrences <=2,000,000; footer totals preflight before full SQL and actual counts must agree |
| Reader raw-check chunks | <=1,024 safe rows and <=2 MiB per materialization, then clear; no unbounded Python event list |
| DuckDB | Two threads, 512 MiB configured memory, no spill/extensions; one active synchronous request per session |
| Reports | Default 100, limit 1–1,000; offset 0–1,000,000; lookahead <=1,001; metrics range <=31 days |
| Response admission | SQL page variable-string bytes <=64 KiB including lookahead; actual complete UTF-8 JSON plus LF <=1 MiB; fixed error/summary <=4 KiB |

Rationale: 2M occurrences leaves row headroom above the future 1.03M physical-event target while finite byte/file caps require split workloads. At max line length the 1 GiB budget permits far fewer rows; caps are intersecting ceilings, not simultaneous promises. Small technical event fields justify smaller overall row/byte/file limits than Content's large article history, while occurrence evidence and independent rank verification justify retaining more than aggregates. Metadata budgets derive from 32 batch descriptors and 1,024 output files. A 31-day native-window report limits its selected scope, not full verification.

Spark starts with established local[2], UTC and reviewed existing runtime configuration; Stage B must record actual driver/executor heap, Python RSS and temporary-disk settings. Use an explicit local memory budget (driver heap 1 GiB, executor setting 1 GiB; in local mode tasks share the driver JVM, not a second claimed heap). Leave at least 25% host RAM for other work and require free disk at least twice a conservative estimate of simultaneous raw-copy, staging, Parquet, Spark temporary and verification space. Record numeric preflight before workloads; no new runtime/install to make it fit. These settings and Spark/DuckDB memory limits are not a hard whole-process limit.

Streaming input/stage budgets can stop before unbounded buffering. Parquet/output/Spark temporary growth is checked at write/action boundaries; it is not a disk reservation or atomic quota and can fail with disk exhaustion first. Full SQL grouping/hash joins can exhaust the no-spill budget even within row/byte ceilings. Fail closed and report unmeasured feasibility rather than promising 1M capacity, adding workers or silently lifting limits. A larger reviewed budget or alternative verifier would require a decision. No new deadline-only process management.

## Fixed failures and precedence

Errors use only the fixed messages below. No arbitrary detail/path/exception is forwarded. Conflict summary is the sole bounded optional count structure described above; normal diagnostic messages contain no IDs or data.

| Code | Exact message |
| --- | --- |
| ARGUMENT_INVALID | Arguments do not match the Reliability interface. |
| PATH_UNSAFE | A selected path violates the local path policy. |
| UNSUPPORTED_VERSION | A selected Reliability contract or processor is unsupported. |
| INTAKE_NOT_APPROVED | Input is not approved for synthetic Reliability intake. |
| INPUT_UNAVAILABLE | A selected intake file is unavailable. |
| INPUT_CHANGED | Intake evidence changed during the operation. |
| BATCH_UNAVAILABLE | The selected registered batch is unavailable. |
| BATCH_INCOMPLETE | Registered batch is incomplete; owner investigation is required. |
| BATCH_INVALID | Registered batch evidence is invalid. |
| BATCH_CONFLICT | Batch identity already binds different content. |
| CSV_INVALID | Service reference CSV is invalid. |
| VOCABULARY_INVALID | Service vocabulary is invalid or unsupported. |
| REVISION_CONFLICT | Service reference revision already binds different content. |
| SNAPSHOT_UNAVAILABLE | The selected service reference is unavailable. |
| SNAPSHOT_INVALID | Service reference evidence is invalid. |
| SNAPSHOT_CHANGED | Service reference evidence changed during the operation. |
| SNAPSHOT_INCOMPLETE | Service reference is incomplete; owner investigation is required. |
| IDENTITY_MISMATCH | Selected service reference identity does not match. |
| DEPENDENCY_UNAVAILABLE | A selected Reliability dependency is unavailable. |
| DEPENDENCY_INVALID | Reliability dependency evidence is invalid. |
| DEPENDENCY_CHANGED | Reliability dependency evidence changed during the operation. |
| EVENT_IDENTITY_CONFLICT | Selected request occurrences have an unresolved identity conflict. |
| PUBLICATION_UNAVAILABLE | The selected Reliability publication is unavailable. |
| PUBLICATION_INCOMPLETE | Reliability publication is incomplete; owner investigation is required. |
| PUBLICATION_INVALID | Reliability publication evidence is invalid. |
| PUBLICATION_CHANGED | Reliability publication evidence changed during the operation. |
| PUBLICATION_FAILED | Reliability publication failed; investigate retained evidence. |
| COUNT_OVERFLOW | A Reliability count or sum exceeds its supported range. |
| RESOURCE_LIMIT | A Reliability resource exceeds its configured limit. |
| RESPONSE_LIMIT | The Reliability report exceeds its response limit. |
| BUSY | The Reliability session already has an active request. |
| QUERY_FAILED | The Reliability report could not be completed. |

At registration, initial missing/read/close failure of unchanged originals is INPUT_UNAVAILABLE; demonstrated original disappearance/replacement/changed metadata/bytes is INPUT_CHANGED. Structural row framing/encoding/resource errors are BATCH_INVALID/RESOURCE_LIMIT. Reference authoring uses INPUT_UNAVAILABLE/INPUT_CHANGED and CSV_INVALID/VOCABULARY_INVALID; destination reads use SNAPSHOT_INVALID except initial absent selected revision, which is SNAPSHOT_UNAVAILABLE.

At product boundaries map initially absent selected batch/reference to DEPENDENCY_UNAVAILABLE; completed dependency missing an artifact or failing integrity/read/close to DEPENDENCY_INVALID; witnessed subsequent disappearance/replacement/differing bytes/stamps/inventory to DEPENDENCY_CHANGED. Preserve PATH_UNSAFE, RESOURCE_LIMIT, INTAKE_NOT_APPROVED and UNSUPPORTED_VERSION. Existing selected product initially absent is PUBLICATION_UNAVAILABLE; missing completion precedes other retained-file checks and is PUBLICATION_INCOMPLETE; ordinary existing-artifact I/O without observed mutation is PUBLICATION_INVALID. New write/read-back/close/rename I/O is PUBLICATION_FAILED. These operation-specific mappings must be tested at call sites, not a broad exception-to-CHANGED handler.

Order: arguments/filter validation, root/path safety and metadata admission; source approval and canonical batch traversal; whole-input structural/privacy gates; reference validation; recognized identity conflicts/expected values; destination state; output inventory/metadata; every schema; values/keys/complete comparisons; counts/coverage/rank reconciliation; final rechecks. Existing product opening validates bounded own metadata first to obtain dependency IDs, then batches, reference and output semantic checks. Unsupported metadata versions fail before interpretation. Recognized schema integer !=1 is fatal, not a rejected schema downgrade.

Once observed, mutation evidence survives a simultaneous ordinary I/O failure and supersedes pending success or a generic I/O diagnostic for that evidence. Unavailable information never implies success or proves mutation. When no further evidence is observable because intake already aborted, report the deterministic first fatal condition without claiming a whole-file scan. Safe local failure handling must not disclose database/Spark/OS exception text.

## Independent synthetic fixtures and expected results

These are synthetic acceptance fixtures, exercised by Stage B tests and independently calculated expectations. They are not retained operational data and require no live source. Fixture generation must not call the normalization/aggregation functions to calculate its oracle.

### F1: literal 20-request group

Use source fixture-source, batch b1, artifact events, received_at `2026-10-20T12:10:00.000000Z`. The reviewed profile allows services alpha/beta/gamma and route health; other profile values follow revision 1. Define event i for i=1..20 with all ten fields exactly:

~~~text
schema_version = 1
event_id = "00000000-0000-0000-0000-" + i as twelve zero-padded decimal digits
service_id = "alpha"
environment = "test"
event_time = "2026-10-20T12:00:00.000000Z"
method = "GET"
route_id = "health"
status_code = 500 for i=1, 503 for i=2, 200 otherwise
duration_ms = i
is_synthetic = true
~~~

Write each object as canonical UTF-8 JSON plus LF, in increasing i order. All 20 are unique. Service reference fixture-services/r1 uses exact LF-terminated CSV and the retained vocabulary above:

~~~csv
service_id,service_name,owner_team,service_tier
alpha,Alpha Service,example-platform,critical
beta,Beta Service,,
delta,Delta Service,example-service,standard
~~~

Authoring is example-team/null. Exact snapshot fingerprint is calculated from actual fixture bytes in Stage B; it is distinct from the header-only compatibility vector below.

Expected F1: P=20, V=20, U=20, D=0, R=0. One alpha/test/12:00 group: request_count=20, server_error_count=2, server_error_rate=0.1, duration_sum_ms=210, mean_duration_ms=10.5, exact nearest-rank p95=19. Candidate acceptable observed values are 18,19,20; validate rank rather than requiring 19. All 20 events MATCHED; alpha USED, beta/delta UNUSED. Every event's delivery_count=1 and receipt min=max=batch receipt. Original ten fields remain exact.

Add only another identical event 1 delivery at line 21: P=21, V=21, U=20, D=1, R=0; aggregate metrics unchanged, alpha event 1 delivery_count=2; original line 1 is ACCEPTED and line 21 DUPLICATE.

### F2: expanded successful fixture

Start with F1 and append lines below. For added valid or typed-invalid rows use the F1 template (same schema/method/route/origin/receipt), changing only listed fields. IDs refer to the same twelve-digit suffix scheme; environment remains test unless explicitly changed. Syntax-error lines are the literal content shown, with one LF. Line 27 contains all template fields but two status_code members, so its unique identity is recognizable even though the JSON object violates this contract.

| Line | Content/change | Expected disposition |
| --- | --- | --- |
| 21 | Identical alpha event 1 | DUPLICATE |
| 22 | beta, event_id suffix 1, event_time 2026-10-20T12:05:00.000000Z, duration 40, status 404 | ACCEPTED, MATCHED with null owner/tier; same native ID across services is distinct |
| 23 | gamma, suffix 21, event_time 2026-10-20T12:04:59.999999Z, duration 0, status 599 | ACCEPTED, MISSING |
| 24 | alpha, suffix 22, event_time 1969-12-31T23:59:59.999999Z, duration 3,600,000, status 200 | ACCEPTED, pre-epoch floor |
| 25 | `{"service_id":"alpha","event_id":"00000000-0000-0000-0000-000000000001",` | REJECTED JSON_SYNTAX, identity null despite apparently visible ID |
| 26 | `{` | REJECTED JSON_SYNTAX, identity null |
| 27 | alpha suffix 23, status_code appears twice with 200 then 500, duration 1 | REJECTED JSON_DUPLICATE_KEY, recognized alpha/23 |
| 28 | alpha suffix 24, duration_ms token NaN, status 200 | REJECTED JSON_NONFINITE, recognized alpha/24 |
| 29 | alpha suffix 25, duration −1, status 200 | REJECTED FIELD_VALUE, recognized alpha/25 |
| 30 | alpha suffix 26, event_time 2026-10-20T12:00:00.0000001Z, duration 1, status 200 | REJECTED TIMESTAMP_INVALID, recognized alpha/26 |
| 31 | alpha suffix 27, event_time null, duration 1, status 200 | REJECTED FIELD_NULL, recognized alpha/27 |
| 32 | alpha suffix 28, environment invalid, duration 1, status 200 | REJECTED FIELD_VALUE, recognized alpha/28 |

P=32, V=24, U=23, D=1, R=8, independently counted as 20 base + 1 duplicate + 3 new valid + 8 rejected. Occurrence output has 23 ACCEPTED, 1 DUPLICATE, 8 REJECTED. Events has 23 rows, each exactly defined by F1/changes; only alpha event 1 has delivery_count=2, all receipt extrema equal the batch receipt. Rejection reason totals: JSON_SYNTAX=2, JSON_DUPLICATE_KEY=1, JSON_NONFINITE=1, FIELD_VALUE=2, TIMESTAMP_INVALID=1, FIELD_NULL=1; other codes zero.

Expected aggregate rows in report order (all environment test):

| window_start | service | Requests | Errors | Rate | Sum ms | Mean ms | Exact p95 / acceptable approximation |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1969-12-31T23:55:00.000000Z | alpha | 1 | 0 | 0.0 | 3,600,000 | 3,600,000.0 | 3,600,000 / exactly 3,600,000 |
| 2026-10-20T12:00:00.000000Z | alpha | 20 | 2 | 0.1 | 210 | 10.5 | 19 / observed 18,19,20 |
| 2026-10-20T12:00:00.000000Z | gamma | 1 | 1 | 1.0 | 0 | 0.0 | 0 / exactly 0 |
| 2026-10-20T12:05:00.000000Z | beta | 1 | 0 | 0.0 | 40 | 40.0 | 40 / exactly 40 |

Global request sum=23, errors=3, exact duration sum=3,600,250. At native grain, each row keeps exact selected reference context. No percentile is summed globally. A metrics page from 12:00 inclusive to 12:05 exclusive contains alpha then gamma only. Limit=1 offset=0 has_more=true; offset=1 returns gamma/false; offset=2 empty/false. Querying the pre-epoch group requires its own aligned range; no interval exceeding 31 days is permitted.

Expected reference rows, ordered: alpha USED/MATCHED with 21 unique events and no missing optional attributes; beta USED/MATCHED with 1, optional list [owner_team,service_tier]; delta UNUSED/MATCHED with 0; gamma ABSENT/MISSING with 1 and empty optional list. Reference totals in the defined key order are `23,22,1,3,2,1,1,3,1`. Filtering delta yields `0,0,0,0,0,0,0,1,1`. Input coverage has one batch, P/V/representatives/duplicates/rejections=32/24/23/1/8, distinct identities=23 and duplicate_excess=1. First event time is the pre-epoch instant, last is 12:05, future occurrence count=0. No rejected identity contributes to reference coverage.

### Failed variants, history and limits

| Independent variant | Expected outcome |
| --- | --- |
| Append valid alpha event 1 with duration 99 to F2 | P=33,V=25,R=8 on complete scan; one conflicting identity; no product reservation/completion |
| Replace line 31's identity with alpha event 1 while retaining null time | P=32,V=24,R=8; recognizable-invalid/valid conflict, count 1 |
| Make lines 31/32 share alpha suffix 27 without any valid occurrence of that key | Two recognizable-invalid occurrences conflict, count 1 |
| Duplicate an identity key, even with equal values | Structural BATCH_INVALID, no choosing first/last value |
| Add unknown ip_address/header/body field to an otherwise invalid row | INTAKE_NOT_APPROVED precedes row rejection; no published quarantine |
| Invalid UTF-8, BOM, lone CR, oversize line/file, read failure halfway | Whole selection fails, never readable-prefix success |
| Infinity/−Infinity, float/bool/string integers, wrong method/environment, calendar or timestamp precision | Fixed row rejection where representable; explicit non-synthetic origin always intake failure |
| Header-only valid service reference | Same unique events/raw metrics; every event MISSING; no unused reference keys |
| Empty batch containing one zero-byte file | P=V=U=D=R=0; all three schema-bearing empty datasets; all three fixture reference keys UNUSED |
| Rejected-only file `{}\n\n` | P=R=2,V=U=D=0; two occurrence rows, no events/groups, references UNUSED |
| Separate explicit b2 with older valid alpha suffix 29 at 2020-01-01T00:00:00.000000Z, duration 7/status 200 | F2+b2: P=33,V=25,U=24,D=1,R=8; new historical group count1/sum7, new product, old unchanged; no prior-relative late count |
| Reference r2 changes only alpha team to example-service | New snapshot/product; event identities and all exact raw metric fields unchanged; fresh p95 may vary only within accepted rank bound |
| Entire b1 selection repeated | Same canonical selection/product, physical counts unchanged, invocation duplicate_batch_selections increments |
| Identical valid event delivered in separate b2 | Additional physical/valid/duplicate count only; exactly one canonical representative across sorted batches |
| Corrupt metadata/Parquet/reference/raw input or missing retained dependency | Full writer/reader verification fails, no repair or partial result |
| Replay or failure before/after marker rename | Preserve existing bytes/mtimes; incomplete state blocked; ambiguous completed state reverified |

Additional independent p95 fixtures: singleton 0; all-equal 7; ties [1,1,1,7,7,7,7,7,7,7]; sparse [0,3,600,000]; heavy tail nineteen 1s and one 3,600,000; and a group over 10,000 rows crossing the rank-budget boundary. Use integer L/H/r/b calculations independent of Spark, not an oracle copied from its expressions. Small rank tolerance can legitimately admit a very different millisecond value; record both. Different input orders and partition counts must preserve exact metrics and satisfy rank tests, not necessarily identical approximate values.

Parameter fixtures cover aligned/unaligned endpoints, pre-epoch and year endpoints, 31-day exact/one-over, absent service, all three environments, invalid/injection-shaped values, limit 1/1,000/1,001, offset 0/1,000,000/one-over, bool/float/negative values, lookahead bytes and actual JSON escaping size. Counts above 2^53 and overflow can use bounded lower-level/fault-injected verifiers rather than huge datasets; do not label forged impossible counts valid at full open.

## Literal canonical compatibility vector A

This is fully defined fictional empty-batch/header-only-reference metadata, not evidence of executed registration or a completed Parquet publication. It tests exact canonical bytes and dependency binding without importing application code. Every JSON block below is one UTF-8 line with no final newline in the hashed artifact. Metadata timestamps are fixed fictional receipt/completion provenance.

Raw artifact data/events.jsonl is zero bytes; SHA-256 is `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`. The reference CSV is exactly the 47-byte header `service_id,service_name,owner_team,service_tier` plus one LF: 48 bytes, SHA-256 `0aa647ececcc46e7f0b467da5d9b251ffcb4ca92cef5859787f88aa7a51006aa`. Vocabulary is exactly the 138 canonical bytes above, SHA-256 `d9c4f3977734e4df226180bcc86330ca465b60ad84517bfa861d828186fa4eb5`.

| Artifact | Exact byte length | SHA-256 |
| --- | --- | --- |
| vocabulary | 138 | `d9c4f3977734e4df226180bcc86330ca465b60ad84517bfa861d828186fa4eb5` |
| batch_manifest | 677 | `ad1ffe5f7a244dd8c27b832db8b728dc5e07e2e66858cb436d7f8e639c29b38d` |
| batch_completion | 183 | `274667401eb8cc37e50f744c21283cc3e156742df498f3c5d4b9ae44f6239e3d` |
| reference_manifest | 571 | `040ae62fb022e957480e9c67fbe4a01cced2dde84a9a32e16aa5b5a5b753119a` |
| reference_completion | 184 | `a68cd53421350552f58e71390925097f4e5753a8377478e79d0a2e0dac615d00` |
| product_inputs | 1387 | `d0bfa671ea0a1726de7b7bb72e5e1e157e97f13138f7f85be8bba7e1cca25996` |

The deterministic ingestion UUID is `93aa01d3-76be-5c9a-be35-66f68d660c8e`. Exact identity-bearing batch manifest:

~~~json
{"batch_contract_version":1,"batch_id":"empty-1","files":[{"artifact_id":"events","byte_length":0,"file":"data/events.jsonl","physical_line_count":0,"sha256":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"}],"ingestion_id":"93aa01d3-76be-5c9a-be35-66f68d660c8e","manifest_version":1,"profile":{"allowed_route_ids":["health"],"allowed_service_ids":["alpha"],"config_revision":1,"logical_dataset_id":null,"origin":"synthetic","policy_revision":1,"profile_id":"request-jsonl-v1","schema_version":1,"synthetic_source_id":"fixture-source"},"received_at":"2026-10-20T12:10:00.000000Z","registrar_revision":1,"request_schema_version":1,"source_id":"fixture-source"}
~~~

Exact registered batch completion:

~~~json
{"batch_fingerprint":"ad1ffe5f7a244dd8c27b832db8b728dc5e07e2e66858cb436d7f8e639c29b38d","completed_at":"2026-10-20T12:10:01.000000Z","completion_version":1,"manifest_byte_length":677}
~~~

Exact service-reference manifest and completion:

~~~json
{"authoring":{"maintainer_id":"example-team","note":null},"csv":{"byte_length":48,"file":"reference.csv","row_count":0,"sha256":"0aa647ececcc46e7f0b467da5d9b251ffcb4ca92cef5859787f88aa7a51006aa"},"csv_schema_version":1,"manifest_version":1,"reference_id":"fixture-services","revision":"r1","semantic_config":{"csv_dialect":"service-csv-v1","null_policy":"empty-only-v1","text_policy":"scalar-text-v1"},"validator_revision":1,"vocabulary":{"byte_length":138,"file":"vocabulary.json","sha256":"d9c4f3977734e4df226180bcc86330ca465b60ad84517bfa861d828186fa4eb5","version":1}}
~~~

~~~json
{"completion_version":1,"created_at":"2026-10-20T12:10:02.000000Z","manifest_byte_length":571,"snapshot_fingerprint":"040ae62fb022e957480e9c67fbe4a01cced2dde84a9a32e16aa5b5a5b753119a"}
~~~

Exact product inputs.json (1,387 bytes), fingerprint **`d0bfa671ea0a1726de7b7bb72e5e1e157e97f13138f7f85be8bba7e1cca25996`**:

~~~json
{"batches":[{"batch_fingerprint":"ad1ffe5f7a244dd8c27b832db8b728dc5e07e2e66858cb436d7f8e639c29b38d","batch_id":"empty-1","completion":{"byte_length":183,"sha256":"274667401eb8cc37e50f744c21283cc3e156742df498f3c5d4b9ae44f6239e3d"},"source_id":"fixture-source"}],"dataset_contract_versions":{"reliability_events_v1":1,"reliability_occurrences_v1":1,"reliability_requests_5m_v1":1},"engine":{"spark":"4.0.2"},"input_contract_versions":{"registered_batch":1,"request_event":1,"service_reference":1},"manifest_version":1,"origin":"synthetic","processor_revision":1,"product_contract_version":1,"product_id":"application-reliability","reference":{"completion":{"byte_length":184,"sha256":"a68cd53421350552f58e71390925097f4e5753a8377478e79d0a2e0dac615d00"},"reference_id":"fixture-services","revision":"r1","snapshot_fingerprint":"040ae62fb022e957480e9c67fbe4a01cced2dde84a9a32e16aa5b5a5b753119a"},"semantic_config":{"admission_policy_revision":1,"event_identity":"service-event-v1","invalid_identity":"repeat-recognized-abort-v1","occurrence":"jsonl-physical-v1","percentile":{"accuracy":10000,"algorithm":"spark-percentile-approx","probability_denominator":100,"probability_numerator":95,"rank_policy":"nearest-rank-interval-v1"},"reference_application":"snapshot-service-v1","rejections":"record-safe-rows-v1","semantic_equality":"typed-ten-epoch-us-v1","window":"utc-floor-300000000us-v1"}}
~~~

These literal expected hashes must be checked independently in Stage B. Reordering object insertion order before canonical serialization leaves bytes/hash unchanged; changing array order is not canonical selection and must be rejected or sorted before identity creation. Duplicate batch selections collapse before serialization. Root/attempt/log/partition changes leave this identity unchanged; changing receipt, source/profile, file bytes, selected reference or exact dependency completion bytes changes identity. Unsupported semantic changes fail instead of being silently accepted. No output file bytes or completion timestamp is fabricated by this vector.

## Stage B acceptance matrix

Every group is required. This matrix defines acceptance behavior; executed Stage B results and test mappings are recorded separately below. Use unittest, temporary fixtures and deterministic mock faults, with literal expected values independent of production expressions. No live publishers/logs or timing races.

| Group | Behavior and concrete independent verification |
| --- | --- |
| R01 — batch identity/admission | Literal manifests/UUID/vector A; exact source/profile/receipt/origin, canonical metadata, sorted explicit inventories, repeated selection collapse, changed binding conflict; byte/line/file/batch/metadata boundaries and 256 maximum-length routes within unchanged profile budget via bounded fixtures |
| R02 — stable physical provenance | Binary LF/CRLF/mixed/no-final-newline/blank/zero-byte fixtures; exact enumerated offsets/line lengths/numbers; shuffle Spark partitions and show positions unchanged; reject arbitrary row-number/monotonic-ID provenance |
| R03 — structural failures | Invalid UTF-8/BOM/lone CR, overlong lines, unsupported schema, depth/member/token caps, read/close faults mid-file; no successful prefix, completed stage or product |
| R04 — rejection/privacy precedence | Exact fixed codes and one rejection per physical line; malformed identity not salvaged; duplicate non-identity keys/nonfinite values; unknown sensitive keys on otherwise invalid rows fail intake, false origin/allowlist failure, no raw payload or exception leakage |
| R05 — identity/conflicts | F1/F2 exact tuples, same ID across services, within/across-batch duplicates/conflicts, recognized invalid-valid/invalid-invalid conflicts, unrecognizable separate rejections; exact P/V/U/D/R and representative/receipt summaries |
| R06 — service snapshots | Exact header/dialect/nulls/vocabulary/hash, duplicate key whole-file failure, header-only, immutable revision/replay, missing/corrupt/unsupported snapshots, detached rows, authoring/read/close/finalization faults; E1a unchanged |
| R07 — typed events/lineage | Every 21/16-column physical schema, required values/types/order/versions, unchanged ten fields, exact raw-position bijection and typed comparisons, safe rejection projection, duplicate disposition/lineage order |
| R08 — UTC/numeric metrics | F1/F2 exact groups/counts/sums; 4xx/5xx distinction, epoch −1/0 and boundary ±1 microsecond, all environment values, year limits and exclusive-end sentinel role/span boundaries, decimal/count overflow, rates/means tolerance and weighted recomputation |
| R09 — independent p95 | Exact nearest-rank/tie interval oracle, observed membership, fixed rank budget, singleton/all-equal/sparse/tied/heavy-tail/max/zero, >10K boundary; shuffled inputs/multiple partitions; reject refreshed-hash aggregate with invalid rank |
| R10 — complete output equivalence | Bidirectional occurrence/event and exact metric key/value comparison; alter values while keeping totals/digests self-consistent and require rejection; expected p95 differs but independently qualifies -> replay allowed without overwrite |
| R11 — immutable publication/replay | Exclusive namespace/metadata, Spark errorifexists, all three empty schemas, completion last, bytes+mtimes stable on replay, changed raw/reference/history selection creates new namespace, originals unchanged |
| R12 — failures/recovery | Mock exclusive-create, partial copy/stage/Spark write, read-back/close/hash/complete.tmp/rename failure; interruption before and after finalization; no repair, delete, auto-promotion, partial data or damage to prior completed snapshots |
| R13 — corruption and mutation | Raw/reference/product manifest/completion/Parquet/schema/inventory, unexpected hidden/temporary files, forged counts, missing dependencies; after-capture bytes/size/mtime/inode/parent/directory changes, same-byte replacement and disappearance |
| R14 — I/O classification | Initial read failure, unchanged-file recheck read/close/stat/listing failures during indexing/Spark/open/query/replay; exact availability/integrity/publication codes versus demonstrated CHANGED; known mutation survives later I/O |
| R15 — named reader reports | Literal F2 ordered rows/coverage totals/pages, exact filters and 31-day aligned bounds, scoped duplicate totals, empty/header-only/unused services, synthetic label, decimal strings, UTC precision, no raw/quarantine/event API |
| R16 — bounded session/results | Limit/offset/lookahead/string-byte/encoded-response boundaries and escaping, metadata/files/rows/bytes caps, low-memory failure, no spill/extensions, two threads, BUSY/close/invalidated-session behavior; no partial result |
| R17 — Spark-free independence | Separate process forbids Spark/producer/Silver imports, invalid Java path, no Gold/Content services; raw/reference retained; all reports and corruption/rank/raw-provenance failures checked through independent SQL/chunk verification |
| R18 — regression/evidence | Full existing M1/M2/E1a/E1b processing and relevant existing Java/API consumer compatibility; unchanged accepted schemas/fingerprints/reports, fixture bytes and CI job names; docs/hygiene and truthful local/hosted/performance evidence |

For failure injection always assert exact fixed code/message, empty success output, unchanged completed bytes/mtimes and no completion before finalization. For mutations assert observed supporting evidence. For ordinary I/O verify unchanged physical evidence before/after the injected fault; do not make tests pass by removing guards. For caps/overflow use small patched limits or lower-level bounded cases and separately verify the real configured constants. No large workload is needed to test arithmetic overflow.

### Internal implementation checkpoints

| Checkpoint | Scope / tested completion gate |
| --- | --- |
| A | Strict metadata, registered batch intake/indexer and service reference; R01–R04/R06 and direct I/O tests; no raw-JSONL Spark bypass |
| B | Explicit staged Spark schemas, valid events/occurrence/rejection lineage, conflict gate and deterministic dedup; R05/R07 and small literal fixtures |
| C | UTC aggregates, exact arithmetic and independent rank oracle; R08–R10 including partition/order variation; stop here for an algorithm decision if rank contract fails |
| D | Immutable product writer, pending independent verification core, completion/replay/fault/mutation behavior; R11–R14 and preserved prior snapshots |
| E | Bounded synchronous reports and complete independent open, R15–R18, focused/full regressions, documentation and measured observations |

After acceptance proceed through A–E within issue #44 and this branch, without intermediate PRs/review stops unless a consequential architecture change is needed. Scope is substantial: checkpoint C is a defensible tested stop if immutable writer/reader work cannot be safely completed in one implementation run; report exactly what remains, without claiming the issue complete. Do not weaken raw correspondence or rank verification to finish.

Stage B validation should use the established Python 3.12.15 / Java 21 / PySpark 4.0.2 / DuckDB 1.4.5 environment, supported SPARK_LOCAL_IP=127.0.0.1 and UTC configuration. Verify local executable paths first. Run new focused Reliability tests, full processing unittest discovery, relevant existing Java/API compatibility and npm/diff checks without changing current CI names. If a sandbox prevents loopback binding, report it and use an authorized environment where available; never weaken tests. The owner handles hosted submission/CI and all Git/GitHub writes.

## Evidence classes, feasibility and approval gate

| Evidence class | Stage A status / later requirement |
| --- | --- |
| Existing baseline | PR #43 merged at 30153df; E1b modules/fixtures and Accepted ADRs reviewed. Historical E1b test results are not rerun or relabeled as E2 evidence |
| Synthetic design arithmetic | F1/F2 hand counts/groups, canonical vectors and small standard-library calculations; no application/Spark execution |
| Stage B correctness | Implementation and executed R01–R18 results are recorded below; the historical Stage A calculations are separate evidence |
| Retained-data end-to-end | None for Reliability; no production/operational events invented, no publisher-data observations added |
| Hosted CI | Not verified for this correction/delivery; report new hosted results only after an actual corresponding run |
| Future E3/E4 | JSONL/Parquet logical equivalence, 100K/1M workloads, resource/format/scaling measurements remain future work; no workload or performance result claimed |

Key engineering risks are the indexer's lexical privacy/duplicate-key precedence, repeated raw hashing/indexing costs, private staging disk and serial preparation, exact timestamp/Parquet logical compatibility, no-spill SQL memory under skew, and qualification of Spark's candidate rank behavior across execution layouts. The streaming design makes physical positions implementable with existing standard-library primitives; it does not prove a throughput target. Independent chunk/raw and rank checks add work but close a correctness gap that a marker-only reader would leave. No installed mature file-numbering reader or generic accepted domain module currently supplies this complete boundary.

The owner accepted ADR 0008's eight decision groups on October 20, 2026, with the year-10000 exclusive query-end sentinel and 256-route amendments. Wider product/M6 status remains unchanged. Actual implementation and validation are recorded below, separately from historical Stage A evidence.

### Historical Stage A executed validation (before acceptance)

Executed locally on October 20, 2026:

| Check | Actual result |
| --- | --- |
| `npm run check` | Passed: 39 Markdown files, zero lint/relative-link issues, repository hygiene passed |
| `git diff --check` | Passed; documentation consistency review also checked the new untracked Markdown whitespace |
| Standard-library document/vector calculations | Passed: all 7 fenced JSON objects are canonical; 6 literal byte-length/SHA-256 vectors, dependency bindings, empty-file/header hashes and UUIDv5 agree |
| Schema/matrix consistency | Passed: 21/16/17 ordered output columns, all ten preserved event fields and complete R01–R18 numbering |
| Independent fixture calculations | Passed: F1 20/2/210/10.5, rank-accepted values 18–20; F2 P/V/U/D/R=32/24/23/1/8, four exact groups, errors=3, duration sum=3,600,250; physical byte-span examples checked |
| Scope/Git review | Only the proposed ADR, this contract, ADR index and minimal Reliability specification link changed; no staged changes, accepted ADR/runtime/dependency/CI files unchanged |

The small calculations used only Python standard-library JSON, hashing, UUID, calendar/integer arithmetic and document inspection, without application imports. No application suites, Spark jobs, live intake, retained-data modification, 100K/1M workload, benchmark, dependency installation or infrastructure provisioning was executed. At that Stage A checkpoint, R01–R18 and implementation checkpoints A–E were pending owner acceptance and Stage B. Hosted CI was not verified and no new hosted result is claimed. No Git/GitHub write operation was performed.

## Stage B implementation and local evidence — October 20, 2026

Architecture: Accepted with the two owner amendments. Checkpoints A–E are implemented in six domain modules, with 58 new unittest cases and a shared fictional fixture helper. Existing Gold, Content, Silver, publisher-reference, canonical serializer, runtime dependencies and CI definitions are unchanged. Stage A was already committed at e434cfd on entry; its documents were preserved. No Git index or GitHub write was performed. The broader Reliability specification remains Proposed.

### Implemented boundaries and recovery

| Module | Public boundary / responsibility |
| --- | --- |
| [reliability_metadata.py](../../processing/reliability_metadata.py) | Strict canonical formats, versions, fixed diagnostics, integer Gregorian timestamps, evidence captures, limits and exact ordered schemas; Spark-free |
| [reliability_inputs.py](../../processing/reliability_inputs.py) | register / open_batch, explicit selection, streaming physical indexer, safe private staging and dependency verification; Spark-free |
| [service_reference.py](../../processing/service_reference.py) | validate / publish / open_snapshot, detached rows and immutable four-column service snapshots; publisher_reference.py is unchanged |
| [reliability_processing.py](../../processing/reliability_processing.py) | Isolated Spark session, explicit tagged schema, full conflict scan, deterministic delivery disposition, 21-column events, 16-column occurrences and 17-column aggregates |
| [reliability_publication.py](../../processing/reliability_publication.py) | publish(spark, *, batch_root, batches, reference_root, reference_id, revision, reference_fingerprint, staging_root, output_root); immutable summary with summary() |
| [reliability_analytics.py](../../processing/reliability_analytics.py) | open_publication(reliability_root, product_fingerprint, *, batch_root, reference_root); context-managed report(name, **approved_filters) / close() |

The exact signatures, CLI flags, schemas and layouts above are implemented. Run CLI modules with the existing Python executable and PYTHONPATH=processing, for example `python -m reliability_analytics --help`; corresponding module names are reliability_inputs, service_reference and reliability_publication. CLI help and invalid options do not start Spark. All three reader CLI reports were also executed successfully. Public reports are exactly requests_5m, input_coverage and reference_coverage.

Registration freezes exact copied JSONL bytes, profile, receipt, deterministic UUID and inventory before completion. Staging is operator-private, tagged and hashed; original staging identities are retained through every check, including after stage.json creation, so a same-byte replacement cannot be hidden by a later capture. Trusted root ancestors are canonicalized before overlap checks; root/descendant symlinks, unsafe spellings, traversal and hard-linked artifacts fail admission. Spark reads only the explicit completed staging inventory. Full typed ten-field conflicts and recognizable-invalid conflicts precede product reservation; raw hashes alone do not decide semantic equality.

Product identity is the exact SHA-256 of canonical inputs.json as specified above. Completion binds the actual files, counts and timestamp. Both Spark multiset comparisons and independent DuckDB/raw checks gate completion. Every physical Parquet file is inspected. Completion temporary bytes are read back and rechecked, then renamed last. Replay verifies retained dependencies, complete raw correspondence, exact values and stored rank qualification without recomputing percentile_approx or rewriting completed files. Fault tests cover pre-completion failures and lost response after rename: the latter may leave a valid completion, resolved only by explicit verification. Incomplete reservations remain for owner investigation; no repair, resume, overwrite or deletion occurs.

Known observed changes remain INPUT_CHANGED, SNAPSHOT_CHANGED, DEPENDENCY_CHANGED or PUBLICATION_CHANGED at the corresponding boundary. Unchanged-file read/close/list/stat errors remain availability, integrity or publication-I/O failures. Tests establish unchanged bytes/stamps for injected ordinary I/O, and independently demonstrate replacement/disappearance/content/directory evidence for mutation cases. Failure envelopes expose only fixed messages and optional conflict counts.

### Timestamp compatibility and amendments

The early compatibility gate passed before full pipeline implementation, then ran again as R08. The existing Python 3.12.15, GraalVM Java 21.0.7, PySpark 4.0.2 and DuckDB 1.4.5 runtimes round-tripped nine instants: ordinary fractional microseconds, epoch −1 microsecond, epoch zero, year 0001, year 9999's last microsecond, midnight, five-minute boundary −1 microsecond, the exact next boundary and 9999-12-31T23:55:00.000000Z. Both output files were inspected physically:

~~~text
physical_type = INT64
converted_type = TIMESTAMP_MICROS
logical_type = TimestampType(isAdjustedToUTC=1,
  unit=TimeUnit(MILLIS=<null>, MICROS=MicroSeconds(), NANOS=<null>))
DuckDB type = TIMESTAMP WITH TIME ZONE
~~~

No INT96, millisecond/nanosecond conversion or loss of microseconds occurred. DuckDB epoch_us matched literal original integer instants under UTC, America/New_York and Asia/Kolkata; UTC string formatting retained six digits and years 0001/9999. The isolated Spark session uses TIMESTAMP_MICROS, UTC and corrected calendar rebasing. The base session's America/New_York/INT96 settings stayed unchanged. The early gate took 3.135 seconds, excluding runtime startup.

The environment lacks optional pytz, so direct Python materialization of DuckDB timezone-aware datetime objects is unavailable. E2 does not require it: SQL epoch_us projections and standard-library UTC formatting implement the approved public representation. The separate-process reader tests forbid pytz as well as Spark/producer imports. No dependency or timestamp range was changed to accommodate this.

Pre-epoch windows use `us - pmod(us, 300000000)` in Spark and the equivalent nonnegative-remainder integer expression independently in DuckDB. Epoch −1 maps to −300000000, not zero. The exact end sentinel is accepted only as an exclusive requests_5m end and compared as integer 253402300800000000; it is never constructed/stored as a timestamp. Tests cover the final year-9999 event/window, rejection as start/event, invalid larger/alternate years and exact/over 31-day spans. A fully populated maximum profile (128 maximum-length services, 256 maximum-length routes, maximum source/logical IDs) is 34,320 canonical bytes, below the unchanged 65,536-byte cap; 257 routes and unknown routes fail.

### Independent literal and numerical results

F1 independently expects 20 requests, two 5xx errors, duration sum 210 ms, rate 0.1, mean 10.5 ms and exact nearest-rank p95 19 ms. The observed Spark candidate was 19; qualification checked observed membership and the rank interval rather than asserting a required approximate value. F2 independently produced P/V/U/D/R = 32/24/23/1/8 and four exact groups (20, 1, 1, 1), three errors and total duration 3,600,250 ms. Its eight rejected physical lines had ordered singleton reasons JSON_SYNTAX, JSON_SYNTAX, JSON_DUPLICATE_KEY, JSON_NONFINITE, FIELD_VALUE, TIMESTAMP_INVALID, FIELD_NULL, FIELD_VALUE. The duplicate contributes lineage but no additional request. Reference coverage is 23 events: 22 matched, one missing; two used matched services, one matched service missing optional attributes and one unused reference service. Header-only/empty/rejected-only variants were independently verified.

The literal batch fingerprint ad1ffe5f7a244dd8c27b832db8b728dc5e07e2e66858cb436d7f8e639c29b38d, header-only reference fingerprint 040ae62fb022e957480e9c67fbe4a01cced2dde84a9a32e16aa5b5a5b753119a and product fingerprint d0bfa671ea0a1726de7b7bb72e5e1e157e97f13138f7f85be8bba7e1cca25996 passed their literal byte/hash assertions. These fictional compatibility vectors are independent of invocation clocks.

The numerical p95 oracle sorts bounded test values and independently calculates target rank, lower/upper tie interval, observed membership, displacement, budget and absolute millisecond error. Seven groups ran under shuffled inputs and one, two and three Spark partitions: singleton zero; all-equal sevens; repeated ties; sparse zero/3,600,000; heavy tail; literal durations 1–20; and 12,005 distinct durations 0–12,004. Small groups had displacement zero. For the large group, exact nearest-rank value was 11,404, candidate 11,403, displacement 1 within budget 2 and absolute error 1 ms, for each layout. These observations are not a proof for every possible input; every publication and reader independently enforce the same fixed budget. A self-consistent refreshed-hash p95 outside the rank budget is rejected; changing F1's stored candidate to qualifying 18 is accepted on read/replay without rewriting it.

### Reader efficiency gate and measured observations

The reader uses one ordered occurrence-to-event left join and one cursor, paired with a serial raw-file scan. SQL checks safe widths before fetch. Cursor chunks are at most 1,024 records and their actual encoded bytes are checked against 2 MiB. End-of-both-streams plus complete keys, positions, per-file counts and SQL reconciliation proves full coverage. No unbounded raw history is collected into Python and no per-chunk full Parquet scan occurs.

EXPLAIN FORMAT JSON showed two PARQUET_SCAN nodes, HASH_JOIN and ORDER_BY for that ordered comparison. A recording connection asserted one execution of the comparison SELECT even when the number of chunks changed. Other full semantic SQL checks use a fixed set of set-based scans; the number of raw chunks does not multiply them. This trades a bounded no-spill DuckDB join/sort for avoidance of a duplicate full raw temporary table. Memory exhaustion fails closed.

| Controlled reader gate | 2,051 rows | 6,149 rows |
| --- | --- | --- |
| Safe chunks | 3 | 7 |
| Ordered comparison SELECT executions | 1 | 1 |
| Parquet scan nodes in comparison plan | 2 | 2 |
| Largest actual chunk | 500,737 bytes | 501,761 bytes |
| Preparation (fixture/Parquet creation; final focused run) | 2.9009 s | 8.2297 s |
| Raw correspondence verification | 0.0777 s | 0.2431 s |
| Python process peak RSS (includes earlier test activity) | 132,907,008 bytes | 141,737,984 bytes |

These samples are from the final 58-test focused run. They are controlled correctness/efficiency checks, not scale benchmarks or CI timing thresholds. DuckDB reported two threads, 512.0 MiB memory and an empty temp_directory; extension auto-install/autoload were disabled. Python RSS is not the configured SQL memory budget or isolated component memory.

On this macOS arm64 host the operator preflight observed 16 GiB RAM, approximately 766.79 GiB free workspace/temporary disk and a conservative 512 MiB simultaneous small-fixture disk estimate (free disk must be at least twice that estimate). Each local Spark session used two threads, 1 GiB driver heap and the default 1 GiB executor setting; local tasks share the JVM. An active producer plus verifier budgets approximately 2 GiB including SQL/Python allowance; overlapping final validation processes remained below the 75% RAM ceiling. No OS RSS quota or scheduler was added. Spark used its existing local temporary directory configuration; private staging and test outputs were in temporary directories, cleaned only by their owning test fixtures.

A separate F2 observation run (32 occurrences / 23 unique events) measured the following. Registration/reference times include the small helper's fictional input-authoring step; Spark startup is outside the publication timer, but first-action initialization/JIT is inside it.

| Operation | Observed duration |
| --- | --- |
| Registered batch preparation/publication | 0.005357 s |
| Service-reference preparation/publication | 0.006224 s |
| Safe indexing/private staging | 0.001799 s |
| Full Spark publication with read-back and independent verification | 11.557183 s |
| Read-only replay with full verification | 0.079307 s |
| Independent consumer open | 0.063870 s |
| requests_5m (three rows, integrity rechecks included) | 0.007314 s |
| input_coverage (one row and totals, rechecks included) | 0.012022 s |
| reference_coverage (four rows and totals, rechecks included) | 0.009384 s |

This observation process reported Python peak RSS 102,301,696 bytes, driver heap 1g, default executor setting 1g and runtime-default Spark temporary directories. The final focused run separately observed F2 normalization/aggregation 5.344 s and publication 5.8604 s. These runs overlapped other local regression activity and had uncontrolled warm-cache/startup effects; the variation is evidence against treating tiny-fixture timings as a latency or capacity promise. No 100K/1M capacity, production throughput or scale benchmark is established.

### Executed Stage B acceptance and regression evidence

All R01–R18 groups passed locally. The final focused and complete processing results are recorded below. The following maps each required group to concrete executable evidence; no group is waived or replaced by an implementation-versus-itself oracle.

| Group | Local result | Evidence |
| --- | --- | --- |
| R01 | Passed | Inputs/Integrity: literal manifest/UUID/hash, selection/replay/conflict, metadata/file/batch/row budgets, 256 routes, canonical ancestor aliases and unsafe paths |
| R02 | Passed | Inputs/Processing: literal LF/CRLF/no-final-newline/blank/empty offsets/lengths/lines, safe staging, unchanged physical positions after Spark |
| R03 | Passed | Inputs/Integrity: UTF-8/framing/lexical caps, exact line cap, mid-file faults and no readable-prefix success |
| R04 | Passed | Inputs/Integrity: literal reasons, duplicate-key/schema/privacy precedence, malformed identity, alternate JSON spelling, no unsafe output |
| R05 | Passed | Processing/Publication: full semantic conflicts, invalid recognizable repeats, cross-service/native-ID and cross-batch delivery/scoped coverage |
| R06 | Passed | Inputs/Integrity: service vocabulary/header/nulls/hash, detached rows, header-only/replay/conflict, corruption, limits, symlinks and finalization/I/O faults |
| R07 | Passed | Processing/Publication/Verification: exact 21/16 schemas and all ten values, positions, dispositions and receipt/delivery reconciliation |
| R08 | Passed | Processing/Integrity/Publication: literal groups, UTC physical round-trip, year limits/sentinel, exact arithmetic and overflow guards |
| R09 | Passed | Processing/Verification: independent rank oracle across seven groups/three layouts and rejected refreshed-hash invalid ranks |
| R10 | Passed | Processing/Publication/Verification: bidirectional comparisons, semantic corruption with fresh digests and accepted qualified approximate variation |
| R11 | Passed | Publication: exclusive completion-last output, empty schemas, byte/mtime replay, reference/history changes and preserved originals |
| R12 | Passed | Publication/Inputs: partial copy/stage-marker/write/read-back/temporary/rename faults, incomplete reservations and ambiguous post-rename response |
| R13 | Passed | Publication/Integrity: metadata/inventory/physical-schema corruption, missing dependencies and observed file/parent/staging replacement |
| R14 | Passed | Inputs/Integrity/Publication/Verification: ordinary I/O exact classifications, unchanged evidence, known mutation survives subsequent I/O/cleanup |
| R15 | Passed | Publication: all three literal reports, coverage/scoped totals, filters/pages/order and final-year sentinel query |
| R16 | Passed | Publication/Integrity: real configured constants and bounded patched-cap tests, prefetch/encoded size, OOM/no-spill, BUSY/close/fail-closed |
| R17 | Passed | Publication: separate Spark/Java/producer/pytz-blocked reader process, successful named-report CLIs and safe help/invalid arguments |
| R18 | Passed | Full processing, JDBC and Content API compatibility; documentation/hygiene, unchanged legacy modules/dependencies/CI |

| Executed local check | Actual result |
| --- | --- |
| Focused Reliability discovery | 58 tests passed, 97.825 s |
| Complete processing discovery | 222 tests passed, 293.564 s; includes all 164 existing M1/M2/E1a/E1b cases plus 58 Reliability cases |
| Final literal schema oracle | One strengthened R07 test passed, 8.712 s, after the final test-only edit; ordered 21/16/17 columns/types are literal expectations independent of producer constants |
| JDBC compatibility | 48 tests passed, zero failures/errors/skips; Maven verify BUILD SUCCESS, 2.794 s total |
| Existing Content API compatibility/integration | 120 unit tests and 20 HTTP/Python integration tests passed, zero failures/errors/skips; Maven verify BUILD SUCCESS, 21.079 s total |
| Documentation/hygiene | npm run check passed: 39 Markdown files, zero lint/link issues, repository hygiene passed |
| Diff hygiene | git diff --check passed; new untracked Python files also checked against /dev/null |
| CLI | Help and safe invalid-argument checks passed; requests_5m, input_coverage and reference_coverage reader CLI success responses passed |

Focused/full processing used the verified repository executable outputs/silver-python312/.venv/bin/python with PYTHONPATH=processing, JAVA_HOME selected by /usr/libexec/java_home -v 21, PYSPARK_PYTHON pointing to that executable and SPARK_LOCAL_IP=127.0.0.1. Focused discovery used TZ=America/New_York and `-m unittest discover -s processing/tests -p 'test_reliability*.py' -v`; full discovery used TZ=UTC and `-m unittest discover -s processing/tests -v`. The stronger R07 oracle ran separately through unittest with processing/tests also on PYTHONPATH. Native Maven commands were `mvn -o -B -ntp -f services/api/pom.xml verify` and `mvn -o -B -ntp -f services/content-api/pom.xml verify -Pintegration` with analytics.python set to the same verified Python executable. Existing generated synthetic JDBC fixtures were reused read-only. Existing local Spark loopback binding is unavailable in the restricted sandbox; these tests use the authorized native execution environment with SPARK_LOCAL_IP=127.0.0.1. The approved runtime was verified before execution, without installing dependencies or weakening tests.

Known local warnings include the existing incubator-vector/native-Hadoop notices and PySpark socket ResourceWarnings. They did not fail assertions. No warning was suppressed to obtain success. There is no hosted CI result for this delivery. No retained Reliability dataset or production logs were collected, and E3/E4 JSONL/Parquet equivalence, 100K/1M workloads and scalability benchmarks were not executed. Raw dependencies remain required; losing them prevents consumption. One-writer/trusted-filesystem, synchronous latency and no power-loss durability limitations remain as accepted.
