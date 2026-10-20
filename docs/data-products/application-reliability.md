# Application Reliability data product

Status: **Proposed for owner review**, October 19, 2026. Part of the [bounded expansion plan](../product-specification.md). No request-event pipeline, service reference, new API or benchmark result is implemented by this specification.

The scoped E2 backend proposal is [ADR 0008](../adr/0008-application-reliability-publication.md) and its [normative proposed contract](../architecture/application-reliability-publication.md). It remains Proposed and requires owner approval before implementation; it does not approve the broader platform or E3/E4 workloads.

## User value and limits

A service owner can compare recorded request volume, HTTP 5xx responses and latency by service/environment in five-minute UTC windows, identify periods worth investigating, and export a bounded reproducible result. An operator can trace excluded records, duplicate suppression, late inputs and the selected reference/publication versions.

Start with batch JSON Lines request events and a versioned service-reference CSV. Equivalent Parquet events exercise typed compatibility and format benchmarks. Initial datasets are explicitly **synthetic**, with fixed generators and no production traffic capture. Any later operational source needs a reviewed registration/privacy contract. Counts describe recorded completed request events, not site availability, user engagement, unique users, incident causes, SLO compliance or every request that occurred. A crash before event emission can be invisible; no uptime inference is allowed.

## Request-event schema v1

Each JSONL record is one UTF-8 JSON object on one nonblank physical line. LF/CRLF line endings are accepted; a final newline is optional. Blank lines, duplicate JSON keys, non-object values, extra/missing fields, coercible strings instead of numbers and non-finite numbers are invalid. No schema inference, nested payload bag, compressed archive or arbitrary document input is supported. Proposed bounds are 16 KiB per line and 256 MiB per registered batch file; larger benchmark sets are explicitly split into bounded files. Limits are versioned configuration, not suggestions to truncate.

All ten fields below are required and nonnull. Unknown fields fail validation, including potentially sensitive ones.

| Field | JSON / Parquet logical type | Validation and meaning |
| --- | --- | --- |
| `schema_version` | integer / INT32 | Exactly `1`; boolean and fractional values rejected |
| `event_id` | string / UTF-8 string | Canonical lowercase UUID, supplied by producer and stable across retries/exports; one completed request attempt |
| `service_id` | string / UTF-8 string | `[a-z0-9][a-z0-9_-]{0,127}`; stable technical service identity, not display name |
| `environment` | string / UTF-8 string | Exactly `dev`, `test` or `production`; new values need a reviewed profile revision |
| `event_time` | string / UTC TIMESTAMP(MICROS, adjusted-to-UTC) | Request completion instant, years 0001–9999, strict `YYYY-MM-DDTHH:mm:ss.ffffffZ`; actual calendar validation, no leap seconds, local times, offsets or excess precision |
| `method` | string / UTF-8 string | Uppercase member of `GET`, `HEAD`, `POST`, `PUT`, `PATCH`, `DELETE`, `OPTIONS`; other methods require profile review |
| `route_id` | string / UTF-8 string | `[a-z0-9][a-z0-9_-]{0,63}`; curated opaque route label, not a raw URL/path/query or user identifier |
| `status_code` | integer / INT32 | 100–599 inclusive; no numeric-string or float coercion; the profile records one terminal status event, not separate interim responses |
| `duration_ms` | integer / INT64 | 0–3,600,000 inclusive, measured elapsed duration for that attempt in whole milliseconds; no negative, NaN, infinity or fractional values |
| `is_synthetic` | boolean / BOOLEAN | Required; true for all initial fixtures/benchmarks; must match the registered batch's origin label |

The proposed precision/range is deliberately bounded. Producers needing sub-millisecond duration or longer requests need a new reviewed schema/profile rather than silent rounding or clamping. Elapsed duration is producer measured, ideally using a monotonic clock; do not derive it by subtracting host wall clocks. Application-level retries are separate attempts with new `event_id`s; repeated delivery of the same logged attempt retains the ID.

Parquet must have exactly these names and logical types, with every file inspected before use. Physical nullable fields are allowed to represent rejected candidates, but nulls fail accepted-row validation. Reject timezone-naive/nanosecond timestamps, incompatible or extra columns and corrupt footers at batch level rather than coerce them. Do not derive business values from partition-directory names. JSONL and Parquet encode the same semantic records; their exact source bytes, physical record locations and hashes remain different provenance.

### Identity, provenance and invalid records

Event identity is `(service_id, event_id)` across all retained batches in one Reliability publication, independent of input format, delivery/run ID, environment and reference attributes. A producer must never reuse an event ID within a service for a different request or environment. Preserve the tuple; an optional derived hash must use a separately versioned canonical tuple encoding, not the Content article-ID algorithm.

The platform attaches an ingestion envelope, never trusting provenance fields supplied inside an event: registered `source_id`/profile/config revision, canonical ingestion UUID, UTC receipt/completion time, batch ID, approved artifact ID, byte length and SHA-256. Each occurrence records its JSONL one-based line number or Parquet file/row-group/row ordinal, plus processor/schema revision and run/attempt linkage. Parquet locations are captured at read time, not inferred from nondeterministic Spark row order. Origin and optional benchmark logical-dataset ID belong to the envelope. Raw files remain immutable under approved storage profiles.

For each occurrence, compare the canonical typed ten-field record after validation; JSON key order/whitespace and Parquet encoding are not semantic differences. Exclude ingestion provenance from semantic duplicate equality. Preserve all delivery occurrences in restricted lineage, while metrics use one canonical event per identity.

| Condition | Proposed handling |
| --- | --- |
| Same identity, identical typed event | Count one request; record each occurrence and increment identical-duplicate diagnostics. Do not count a retry delivery as another request |
| Same identity, any differing event field | `EVENT_IDENTITY_CONFLICT`; fail the whole selected publication, including conflicts across batches/formats. No last-write-wins and no choosing a preferred format |
| Invalid individual JSONL/typed row | Record location, safe reason codes and evidence reference in quarantine; exclude it from metrics. Do not echo raw values in logs |
| Invalid occurrence with a recognizable valid identity also present elsewhere | Block publication for investigation; invalid data must not conceal a possible conflicting duplicate |
| Oversize/unreadable file, unknown schema, invalid UTF-8 or corrupt Parquet | Fail the selected batch/run; no success based on the readable prefix |
| Missing reference key | Retain the valid event with explicit unmatched status; reference absence is not an invalid request |

A rejected physical line/row counts once even if it has multiple reason codes. Missing/unparseable identities cannot be deduplicated; retain their occurrence count and limitation. For a successful selection, physical row occurrences equal valid unique events plus identical duplicate occurrences plus rejected occurrences. Reconcile this equation and per-reason counts before completion. Proposed initial policy allows recorded row rejections but has no silent quality threshold; expose their count/rate with the publication. An operator can configure a stricter maximum rejection count/rate in the captured profile; exceeding it fails publication. Structural batch failures and identity conflicts always fail regardless of threshold.

Quarantine is restricted operational evidence, not an ordinary event/catalog API or export. Use stable artifact/row references and hashes instead of duplicating unexpected sensitive fields in diagnostic records. Unapproved real/sensitive inputs halt intake and follow an owner-reviewed restricted retention/removal procedure; do not automatically propagate them into analytical outputs.

## Service-reference CSV v1

Exact header: `service_id,service_name,owner_team,service_tier`. Primary key: `service_id`, unique within the version; attributes describe a service across environments, not a particular request or historical incident.

| Field | Rule |
| --- | --- |
| `service_id` | Required; same identifier syntax and exact case-sensitive equality as events |
| `service_name` | Required nonblank plain UTF-8 text, 1–200 Unicode scalar values |
| `owner_team` | Nullable token, 1–64 characters from a captured vocabulary; organizational label, no personal name/email |
| `service_tier` | Nullable member of `critical`, `standard`, `experimental`; operational context, not a calculated reliability score |

Use the same strict UTF-8 CSV dialect, whitespace/null policy, whole-file duplicate-key rejection, immutable manifest/version identity and proposed 10 MiB/10,000-row bounds as the [publisher reference](content-intelligence.md#publisher-reference-csv-v1). Capture the service vocabulary version/digest. A valid header-only reference is allowed; a missing or invalid selected reference artifact fails the run.

Left join on `service_id` after event validation/deduplication. Preserve events/aggregate counts for unmatched services with nullable reference attributes and `reference_status = "MISSING"`; a matching row with optional null attributes remains `"MATCHED"`. Report unmatched event/service counts and unused keys separately. Unique reference keys must prevent row multiplication. Reference corrections create a new snapshot-enriched publication without changing event identity or raw metrics. One reference version applies to all included event times; historical as-of ownership is not claimed.

## Five-minute aggregates and numerical contract

Proposed `reliability_requests_5m_v1` grain/key: `(service_id, environment, window_start)` within one publication. The publication is homogeneous in data origin (`synthetic` versus approved `operational`); mixed origins are rejected, so origin is a mandatory publication label rather than a hidden aggregation dimension. Method, route and individual status do not subdivide this initial aggregate.

For each valid deduplicated event, `window_start` is floor of UTC epoch microseconds divided by 300,000,000, multiplied by 300,000,000. Use mathematical floor, including pre-epoch instants. Windows are half-open `[window_start, window_start + 5 minutes)`. An event at exactly 12:05:00 belongs to the next window, never both. No processing-time window, implicit local timezone, watermark or empty-window zero filling is used.

| Field | Definition / type |
| --- | --- |
| `service_id`, `environment`, `window_start` | Required grouping values; window start is UTC microsecond timestamp aligned to five minutes |
| `request_count` | Positive signed 64-bit count of unique valid request identities in the group |
| `server_error_count` | Signed 64-bit count where `500 <= status_code <= 599`; nonnegative and no greater than request count |
| `server_error_rate` | Finite number `server_error_count / request_count`, range 0–1; label explicitly “5xx responses / recorded requests” |
| `duration_sum_ms` | Exact nonnegative decimal integer, DECIMAL(38,0) internally, supporting a weighted mean without INT64 sum overflow |
| `mean_duration_ms` | Finite nonnegative numeric value `duration_sum_ms / request_count`; no rounding of source durations |
| `p95_duration_ms_approx` | Observed integer duration returned by the approved approximate percentile method; label “approximate p95 (ms)” |
| Reference attributes/status/version | From the selected unique service-reference row; never part of the event identity |

Reject count/sum overflow before publication. REST/JSONL serialize integral counters, sums and percentile values as canonical decimal strings; rates and means are finite JSON numbers with documented rounding tolerance. For independent tests, counts/sums are exact and rates/means must agree within `max(1e-12, abs(expected) * 1e-12)`. Presentation rounding must not replace stored values. Use exact integer totals and guard chart conversions just as in the existing frontend.

The sum of window request counts equals accepted unique event count. The sum of 5xx counts equals the unique-event 5xx count. Across disjoint groups, recompute the combined error rate from summed numerator/denominator and the mean from summed duration/count; never average rates or means without their weights. **Never sum or average p95 values across windows, services or environments.** A wider-scope percentile needs a fresh calculation from the pinned deduplicated events or a separately approved mergeable sketch contract; the initial aggregate API supplies only its native-grain p95. Empty scope returns no groups and no invented zero latency/rate.

### Percentile accuracy and validation

Proposed implementation candidate: Spark `percentile_approx(duration_ms, 0.95, 10000)`, with engine version and accuracy parameter captured in the processor contract/publication. Spark documents the accuracy parameter and its `1 / accuracy` error relationship; higher accuracy uses more memory. This is a candidate to validate, not a newly accepted engine guarantee or a promise of 0.01% duration-value error. See the [pinned Spark 4.0.2 API documentation](https://spark.apache.org/docs/4.0.2/api/python/reference/pyspark.sql/api/pyspark.sql.functions.percentile_approx.html).

Define the independent exact reference for N sorted integer durations as nearest-rank p95: sorted value at one-based rank `ceil(0.95 * N)`. The proposed acceptance budget is rank displacement at most `max(1, ceil(N / 10000))`. For returned value v, compute its tie interval `[#(x < v) + 1, #(x <= v)]`; v must be an observed group value and the distance from the target rank to that interval must not exceed the budget. Require exact equality for singleton/all-equal groups and record the exact nearest-rank value alongside approximate results in validation evidence.

Check every group against independently sorted durations for small, baseline and target datasets. Include repeated values, sparse groups, heavy tails, extreme allowed durations, skew, input shuffles and multiple partition layouts. Record absolute error in milliseconds and rank displacement; relative value error is undefined when the exact p95 is zero and is not the acceptance bound. A small rank error can still mean a large latency difference in a heavy tail. If the candidate fails the budget, revise/review algorithm or accuracy before release; do not quietly widen tolerance after observing results. Approximate values can vary within tolerance between execution layouts. Completed snapshots themselves remain immutable; repeat queries over one snapshot return the stored value.

## Publication, input history and late events

Propose a separate Reliability namespace and manifest covering exact event-batch inventories, reference artifact, schema/processor revision, semantic configuration and percentile settings. Use explicit retained-input selection and full recomputation first. Keep raw occurrences/quarantine and a deduplicated event dataset with lineage, plus the five-minute aggregate and coverage diagnostics, behind one verified product completion boundary. Public queries expose only approved projections; provenance paths remain restricted.

An optional `reliability_events_v1` catalog has one row per `(service_id, event_id)` in the publication and supports reconciliation/export. Its business fields are the validated event fields, enriched reference attributes and provenance identifiers; multi-delivery evidence is retained separately at occurrence grain rather than arbitrarily choosing a delivery as the request's identity. Do not aggregate retained daily/whole-output snapshots together.

`event_time` describes request completion; `ingested_at` describes platform receipt; run/publication timestamps describe processing. Ordinary receipt delay is not itself a late revision. For revision diagnostics, label a newly included event late when its event time precedes the prior selected publication's maximum event time; record the comparison publication, and report no such classification if none exists. This diagnostic never controls acceptance. Include all valid selected events regardless of age. A future event time relative to receipt is retained with a clock-skew diagnostic, not silently rewritten; any future-time exclusion would require a reviewed contract. Publish event-time coverage, receipt-time coverage and receipt delay separately without promising complete event delivery.

Late batches with older event times enter a new selected inventory and create a new immutable publication. Recompute affected historical results as part of the full selection; preserve the prior publication and require explicit product selection/reload to adopt the new one. No watermark-based dropping, streaming checkpoint semantics or automatic latest switch is introduced. A re-delivered identical event contributes no additional request; a conflict anywhere in retained history blocks the new publication. Identical inventory replay verifies existing output and writes nothing. Different physical representations may yield different publication identities while agreeing on logical event/aggregate results within the percentile tolerance.

Before completion, inspect every output schema, validate keys and required values, reconcile occurrence/duplicate/rejection counts and aggregate sums, verify reference cardinality, compare read-back rows with expected output and recheck input hashes. Failed validation, write or recovery leaves the previous selected verified publication available. The [common run/selection contracts](../product-specification.md#asynchronous-execution-and-recovery) govern cancellation, durable history and failed-update behavior; they require future ADRs rather than retrofitting today's Gold marker.

## Consumption, privacy and operational separation

Proposed named reports filter exact service/environment and a bounded UTC window-start range with inclusive start and exclusive end, both aligned to five minutes. Reject misaligned bounds rather than pretend partial-window metrics are available. Order by `window_start ASC, service_id ASC, environment ASC`, with bounded server-side pages and a consistent publication identity. Catalog order, if exposed, is `event_time DESC, service_id ASC, event_id ASC`. Route/method filters need a separate raw-event query contract and cannot be applied to aggregates that discarded those dimensions.

Explain 5xx semantics and the denominator, approximate p95, unmatched reference coverage, record exclusions and data origin next to charts/tables. A 4xx response is still a recorded request but not a 5xx. Transport failures without a status/event are not represented by fabricated `status_code = 0` rows. Initial screens and exports must visibly say “Synthetic request events — not production reliability.” Tests/benchmarks and operational data cannot share an unlabeled publication.

Exclude IP addresses, user/session/device identifiers, cookies, credentials, headers, request/response bodies, raw query strings/URLs, stack traces and personal names/emails. Do not copy live logs and then rely on a UI to hide fields. Producers emit the approved minimal schema; unauthorized sensitive inputs are handled at the restricted intake boundary. All analytical queries, run controls, exports and artifact downloads follow [backend product-scoped authorization](../product-specification.md#authorization-exports-and-audit).

This product analyzes supplied service events. Monitoring the platform's own queue, workers, storage, verification, API and audit failures is an independent operational function, available even when this product cannot publish. An observability stack, live log collector, identity provider, orchestration framework or metadata database is not selected here.

## Acceptance fixtures and delivery dependencies

Hand-check a group of 20 unique events with durations 1–20 ms and exactly two 5xx statuses: request count 20, errors 2, rate 0.1, duration sum 210, mean 10.5 ms, exact nearest-rank p95 19 ms. The approximate result must meet the rank budget above; it is not automatically asserted to equal 19. Add an identical delivery and retain the same metrics with duplicate count incremented. Change its duration under the same identity and require a publication conflict. Add an event exactly at the next five-minute boundary and require a separate group. Omit its service reference and retain that group's metrics with explicit missing context.

Further fixtures cover invalid calendars/precision, pre-epoch flooring, maximum duration, null required fields, unsafe extra fields, byte-identical repeated batches, duplicate keys in CSV/JSON, schema mismatch, mixed origins, empty/rejected-only batches and event-time lateness. Corrupt files and missing selected references must fail the whole attempted update. JSONL/Parquet expected events must agree under logical equality before any format-performance comparison.

The [benchmark plan](../product-specification.md#benchmark-and-failure-evidence-plan) defines the seed, 100,000-event baseline, preflight-gated 1,000,000-event target, independent oracle, comparable formats, local versus multi-worker conditions, measurements and failure experiments. No dataset generation or benchmark is executed by this document. Review event bounds, privacy/intake policy, percentile algorithm, new publication manifest and retention before the first implementation slice; keep metadata-store, orchestration, worker storage, identity and observability choices at their explicit decision gates.
