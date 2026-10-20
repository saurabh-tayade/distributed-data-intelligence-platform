# ADR 0008: Application Reliability batch publication and analytics

Status: **Accepted**

Proposal date: October 20, 2026, for [issue #44](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/issues/44), combining E2a and E2b. Owner acceptance: **October 20, 2026**, covering all eight decision groups with the two amendments below. Stage B is authorized; implementation and executed evidence remain separate.

## Context and requirements

The [Reliability specification](../data-products/application-reliability.md) proposes synthetic request events, service context and five-minute analytics. E1b is now merged through PR #43; accepted ADRs 0001–0007 remain unchanged. Gold and Content demonstrate strict immutable publications and independent DuckDB verification, but their article identities, CSV schema and dependency adapters do not describe request events.

E2 must account for every physical delivery, deduplicate exactly on (service_id, event_id), block conflicts, preserve rejected-occurrence evidence without copying arbitrary payloads, and independently verify aggregate counts, sums and approximate p95 ranks. It must remain local, bounded and synthetic-only. Counts describe recorded completed requests, not uptime, SLO compliance, users or all attempted requests.

Spark's ordinary JSON DataFrame reader does not establish stable physical line numbers. Approximate percentile results may vary with execution layout, so an input-based fingerprint cannot honestly promise bit-identical approximate regeneration. These are explicit architectural choices rather than implementation details.

## Accepted decision

Adopt the [normative Reliability contract](../architecture/application-reliability-publication.md), with the owner-approved amendments below. It defines exact schemas, metadata, fingerprint vectors, limits, diagnostics, fixtures and Stage B groups R01–R18.

1. **Registered inputs:** accept only explicitly selected completed synthetic JSONL batches under approved local roots. Freeze exact file inventories, receipt provenance and reviewed source/profile configuration. No directory discovery, implicit latest, Parquet intake or live logs.
2. **Physical provenance:** use a bounded standard-library Python streaming indexer. Enumerate LF-delimited physical positions before Spark, validate strict JSON and emit only safe, provenance-tagged staging records. Spark starts at those explicit staged files, not at raw JSONL. The indexer is a sequential local preparation pass, not distributed Spark processing.
3. **Identity and privacy:** compare all ten validated typed fields; identical deliveries collapse, conflicting identities abort. A recognizable invalid identity repeated elsewhere also blocks publication. Reject unknown fields/unapproved origin at intake; representable row errors produce restricted location/reason evidence. Never copy raw rejected JSON.
4. **Service reference:** implement a separate small service-reference snapshot boundary using E1a's strict CSV and immutable-publication patterns. Retain its own exact four-column schema and service vocabulary. Do not extend or reinterpret publisher_reference.py.
5. **Three persisted datasets:** retain typed deduplicated reliability_events_v1, restricted reliability_occurrences_v1 (including rejection evidence), and reliability_requests_5m_v1. Derive coverage by verified queries. A separate quarantine dataset and public event catalog are unnecessary for E2.
6. **Metrics and p95:** deduplicate before mathematical-floor UTC windows. Use exact counts and DECIMAL(38,0) sums; independently validate rates/means. Use the approved Spark 4.0.2 candidate percentile_approx(duration_ms, 0.95, 10000), accepting only observed values within the specified nearest-rank interval budget. Failure of that budget blocks release; it does not authorize a wider tolerance.
7. **Immutable result identity:** hash canonical selected dependency/semantic metadata, not output bytes. Completion binds actual physical output. The first completed, fully verified result in a root is authoritative. Replay validates its stored p95 by independent ranks; it must not demand equality to a freshly recomputed approximate value. Exact fields remain fully reproducible.
8. **Independent consumption:** require retained raw batches and service reference. A synchronous Spark-free DuckDB reader re-indexes raw files in bounded chunks, verifies every occurrence/event and aggregate, and exposes three named paged reports. It uses two threads, 512 MiB, no spill/extensions, bounded metadata/pages/responses and before/after integrity checks. No query worker, hard deadline, scheduler, API, frontend or export service.

Accepted module boundaries are Reliability-specific metadata, batch intake/indexing, service reference, Spark processing, publication and analytics. Only publication_metadata.canonical_bytes is directly reused unchanged. Filesystem guards, fixed diagnostics and SQL verification are patterns to reproduce with domain tests, not private APIs to import from Content. No changes to accepted Gold/Content/E1a modules or new dependencies are proposed.

## Provenance alternatives

| Approach | Assessment |
| --- | --- |
| Bounded Python streaming indexer, then Spark | Recommended. Exact byte offsets and line numbers are explicit, memory is bounded by one physical line plus a small output buffer, and encoding/privacy failures stop admission. Adds serial parsing and private staging I/O; those costs must be measured separately |
| Spark file-aware reader | Hadoop text offsets can identify records, but offsets are not line numbers; stable numbering needs a per-file prefix/index protocol. wholeTextFiles/binaryFile can hold an entire 256 MiB file and inflate decoded/object memory. zipWithIndex is only meaningful with a proven stable per-file ordering and split contract. No such supported repository reader exists. A custom JVM reader is disproportionate for E2 |
| Existing mature reader | Standard-library binary I/O plus json/csv supplies the needed bounded primitives. No installed higher-level reader demonstrates exact line provenance, strict duplicate-key handling and this privacy policy together. Adding a parser/connector dependency would not remove the required integrity protocol |
| DuckDB-only pipeline | Could be simpler locally, but replaces the requested Spark normalization/aggregation boundary. Use DuckDB as an independent verifier |
| Retain raw JSON in quarantine | Easier inspection but duplicates potential sensitive values and broadens exposure. Retain only restricted locations/reasons; authorized investigation opens the original retained input |
| Omit typed deduplicated events | Saves disk, but prevents straightforward independent sums/rank validation and E3 logical comparison. Typed events have a distinct verification purpose |
| Separate rejected dataset | Duplicates occurrence locations and count reconciliation. Use a restricted REJECTED projection of the occurrence dataset |
| Exact p95 instead | Deterministic and simple to explain, but requires a different reviewed algorithm/performance contract. Keep as the fallback decision if the candidate cannot meet the approved rank bound |
| Hash output Parquet or approximate value into product identity | Binds one execution result but prevents pre-write namespace reservation and conflates semantic selection with execution layout. Bind actual files in completion instead |
| Dependency-free reads | Improves availability after retention loss but cannot prove correspondence to original occurrences. No degraded trust-marker mode |
| Prior-publication lateness comparison | Useful diagnostics, but adds a dependency and comparison scope. Defer it; preserve event/receipt times and state explicitly that E2 makes no prior-relative late classification |

## Failure, recovery and limits

Admission/privacy/structural failures and identity conflicts prevent product namespace reservation. Later failures leave incomplete private staging or reserved output for owner investigation. Never delete, repair, overwrite, resume or auto-promote it. Missing completion is not an empty result. A lost response after final rename requires full explicit verification.

All selected raw batches and reference snapshots remain immutable and retained. One trusted writer owns each root; filesystem roots and source declarations are operator-approved. No hostile-race protection, distributed transaction, multiwriter fencing, exactly-once delivery, fsync/power-loss durability, automatic retention or durable run registry is claimed. Byte/identity/inventory rechecks detect observed changes, not changes reverted between checks. Ordinary I/O failures without observed mutation use availability/integrity classes.

The accepted input/row/file caps make work finite but are not measured capacities or process-RSS guarantees. Python staging is serial; Spark normalization and aggregation can distribute only after workers can read the exact staged files. Current local-thread execution is not multi-host evidence. DuckDB full verification may exhaust its no-spill budget before the maximum admitted row count; that must fail closed. Future E3/E4 experiments may require separately reviewed budgets/storage, not weaker checks.

Checksums establish consistency, not authenticity, source truth or proof that arbitrary real data is safe. Synthetic-source review and restricted local ownership remain prerequisites. Every report must say **“Synthetic request events — not production reliability.”**

## Validation and implementation gate

Stage A permits repository inspection, small standard-library fixture/hash calculations and documentation checks only. Actual Stage A results are recorded in the contract; no application test or workload execution is implied.

After acceptance, checkpoints A–E proceed under the same issue/branch: input/reference intake; normalization/lineage/dedup; aggregates and independent oracle; publication/replay; reader/fault injection/regressions. R01–R18 must pass, including unchanged M1/M2/E1a/E1b and existing consumer compatibility. Synthetic correctness, retained-data evidence, hosted CI and future benchmarks must be reported separately. No 100K or 1M run is claimed.

If the candidate percentile fails rank validation, exact position tracking cannot meet the bounds, or consumer verification cannot be implemented safely, stop at the last tested checkpoint and request the consequential decision. Do not weaken criteria to finish the combined issue.

## Owner-approved decisions

The owner accepted these eight decision groups on October 20, 2026:

1. The synthetic-only reviewed source/profile gate, unknown-field intake failure, malformed-row investigation limits and restricted retention/access policy.
2. The streaming Python indexer, its staging format/lifecycle and serial preparation cost; raw JSONL remains retained and authoritative.
3. The recognizable-invalid-identity conflict policy, deterministic delivery representative, allowed row rejections with no adjustable threshold in revision 1, and the three exact datasets.
4. The service vocabulary v1, snapshot/revision binding and snapshot-wide ownership semantics.
5. Floor-based windows, numeric tolerances, candidate p95/rank budget and first-completed-result replay semantics allowing qualified approximate variation between fresh roots/layouts.
6. Exact canonical identity/evidence layouts, required retained dependencies, one-writer/manual recovery assumptions and deferral of prior-relative lateness comparison.
7. Every documented admission/resource limit, synchronous three-report consumer, no public event catalog and no hard latency guarantee.
8. The Stage B acceptance matrix/checkpoints, pinned existing runtimes and no new infrastructure/dependencies or changes to accepted domain modules.

The wider product specifications remain Proposed. Architecture acceptance does not establish implementation or test completion.

## Owner-approved amendments

1. Permit exactly `10000-01-01T00:00:00.000000Z` only as the exclusive end of requests_5m queries. Event times, stored windows and start bounds remain years 0001–9999. Validate spans with integer Gregorian arithmetic, preserve the 31-day maximum, and compare integer epoch microseconds without constructing/storing a year-10000 timestamp.
2. Increase allowed_route_ids to 1–256 sorted unique reviewed identifiers, preserving syntax, synthetic-only admission and all existing metadata/resource budgets. Verify the maximum vocabulary fits the 64 KiB profile cap.

## Implementation evidence

The six Reliability modules and checkpoints A–E are implemented. Executed local acceptance/regression results, physical timestamp evidence, bounded reader access-plan observations and limitations are recorded in the [Stage B evidence section](../architecture/application-reliability-publication.md#stage-b-implementation-and-local-evidence--october-20-2026). Architecture acceptance is separate from hosted CI and future E3/E4 benchmarks. No dependency, infrastructure, wider product-status or accepted ADR 0001–0007 change is included.
