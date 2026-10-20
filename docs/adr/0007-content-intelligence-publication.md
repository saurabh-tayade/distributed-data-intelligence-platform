# ADR 0007: Verified Content Intelligence publication and consumption

Status: **Accepted**

Proposal and owner acceptance date: October 20, 2026, for [issue #42](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/issues/42). The owner accepted the seven scoped decisions with the amendment below and authorized Stage B. Acceptance and implementation evidence are separate.

## Context and requirements

E1a supplies detached, validated publisher-reference rows. Gold v1 supplies source-scoped articles and observation-day aggregates over a frozen Silver inventory. E1b needs publisher context and first-discovery analytics without changing either accepted boundary. The [Content Intelligence specification](../data-products/content-intelligence.md) and [platform expansion](../product-specification.md) remain Proposed; E1b does not change M6 status.

The required result is one reproducible, immutable derived publication with a Spark writer and a Spark-free local reader. It must preserve every Gold article and base value, distinguish unmatched references from matched nullable attributes, reconcile discovery counts, reject incomplete/corrupt evidence and keep previous publications intact. Reference enrichment applies one selected snapshot to the whole catalog, not historical effective dates.

Code inspection establishes these reuse constraints:

- `gold_analytics.open_publication(root, fingerprint)` verifies metadata, inventories, every physical Parquet schema, values and cross-table invariants without Silver. `gold_reader.verify_metadata()` alone does not establish that boundary.
- `VerifiedMetadata.paths(table)` exposes the approved file inventory, but its nested metadata is mutable. A new adapter must detach/freeze its own descriptor and compare captures around full verification; it must not reach into `GoldAnalytics._metadata` or `_connection`.
- `gold.build_gold()` and `gold_publication.publish()/verify_frames()` require Silver `ValidatedInputs`, reconstruct winners and check Silver membership. They are not generic enriched-publication functions. Reusing them directly would require the wrong dependency and risk changing Gold.
- `publisher_reference.open_snapshot()` independently verifies E1a and returns immutable rows/provenance. It offers no persistent filesystem lease; E1b must capture/recheck retained evidence and reopen through that public interface.
- `publication_metadata.canonical_bytes()` is independent of Spark and suitable for new metadata. Existing producer modules import Spark/Silver and must not enter the new reader's import graph.

## Accepted decision

Adopt the complete [normative E1b contract](../architecture/content-intelligence-publication.md), with the owner-approved amendment below. Its schemas, metadata, interfaces, limits, failure matrix and acceptance fixtures are part of this accepted decision.

1. Select exactly one Gold v1/revision-1 publication and one E1a snapshot by explicit fingerprints. Fully verify both; freeze exact Gold file lists and reference rows. Retain Gold and E1a for all future E1b replay and consumption. Silver is not required by E1b.
2. Publish `content_catalog_v1` with all Gold article columns unchanged, plus discovery and reference fields. Left join exact source keys and require complete row-multiset equality with the expected enrichment.
3. Publish `content_source_discovery_daily_v1` at source/UTC-first-discovery-date grain. Each catalog identity contributes once. Obtain coverage through deterministic verified queries, without a third stored dataset.
4. Hash an exact canonical dependency/semantics manifest. Copy the exact Gold input and completion manifests as evidence; reference the independently retained E1a identity. Do not copy dependency Parquet or add roots, run UUIDs or completion time to product identity.
5. Use a dedicated version/revision/fingerprint namespace, exclusive reservation, Spark `errorifexists`, complete read-back verification and completion last. Replay verifies all dependencies and output without writes. Failed reservations require owner investigation.
6. Provide only three bounded named local reports, with explicit publication pinning, fixed projections, parameters, lookahead pagination, exact decimal-string counters and before/after integrity checks. No SQL escape hatch, HTTP API, React work, automatic selection or export service.
7. Keep new dependency/metadata, Spark transformation/publication and DuckDB reader responsibilities separate. Permit only the narrow additive Gold memory-budget hook described in the contract; preserve existing default behavior and all Gold verification semantics.

## Alternatives and tradeoffs

| Alternative | Evaluation |
| --- | --- |
| Add reference fields to Gold v1 | Breaks an accepted schema and confuses observation metrics with enrichment; use a separate derived product |
| Recompute Gold from Silver inside E1b | Could independently reconstruct historical winners, but duplicates selection and expands required retained inputs; trust the fully verified accepted Gold consumer boundary and state its limits |
| Metadata-only Gold check followed by Spark read | Cheaper, but bypasses schema/value verification; rejected |
| Generic publication engine or connector layer | Could share code later, but introduces unrelated abstractions/circular-import risk; use specific small modules and test contract parity |
| Duplicate Gold and reference data under every product | More portable when dependencies are deleted, but duplicates large data and complicates integrity/retention; retain dependencies and copy only Gold metadata |
| Bind only Gold's input-set fingerprint | Does not pin exact Gold physical output or completion evidence; also bind exact completion bytes and their inventory |
| Logical-row/Parquet-byte product fingerprint | Logical hashing needs a new row serialization; output-byte hashing complicates reservation and depends on Spark encoding. Bind verified dependencies/semantics and record actual output hashes in completion |
| Store coverage as a third dataset | Adds redundant materialization and reconciliation; fixed queries over the verified catalog and bounded reference suffice |
| DuckDB-only enrichment | Feasible at small scale, but removes the requested Spark processing boundary; use DuckDB as an independent verifier/reader |
| Output-only offline reads | Would remain available after dependency deletion but cannot independently check exact enrichment against dependencies; no degraded or trust-marker-only mode |
| Historical/as-of reference joins | Need effective dates and history rules absent from E1a; snapshot enrichment is explicit |
| Locks, database registry, transactions or automatic repair | Broader operational decisions; retain one trusted writer per root and manual investigation |

Full scans, comparisons, hashes and retained dependencies cost I/O and disk. Binding Gold completion bytes means a physically rewritten equivalent Gold publication can produce a different E1b identity even with the same Gold input-set fingerprint. Product Parquet bytes may differ on fresh regeneration in another root; identical replay within one root preserves the first completed artifacts.

## Failure, recovery and limitations

The writer owns one local root; dependencies remain immutable under trusted ownership. No distributed lock, hostile-race protection, transactional snapshot, multiwriter isolation, power-loss durability or exactly-once guarantee is proposed. Rechecks detect observed changes, not changes reverted between observations. Ordinary I/O failure without evidence of change remains an availability/integrity failure, following E1a's corrected distinction.

No incomplete output is resumed, repaired, overwritten or promoted. A lost response after completion rename requires full verification before reuse. Missing retained Gold or E1a makes E1b reads and replay unavailable; metadata copies preserve provenance but do not replace dependencies. No automatic retention or live publisher fetch is authorized.

Gold's standalone verifier cannot reconstruct original winner membership or exact historical daily metrics without Silver. E1b inherits that accepted limitation while independently proving every derived row against the selected Gold rows and E1a rows. Checksums prove consistency, not authenticity or truth of publisher metadata.

## Validation requirements and evidence

Stage A requires complete document consistency, checked literal hash vectors, Markdown/relative-link/hygiene checks and `git diff --check`. No application, Spark, retained-data or benchmark run belongs to this stage.

Stage B implements the contract's independent synthetic fixture, failure injections, corruption/schema tests, bounded consumer tests and unchanged Gold/E1a regressions; executed results are recorded in the detailed contract. Small literal expected rows and group totals must supplement Spark-versus-DuckDB comparisons. Record synthetic, retained-data, hosted CI and benchmark evidence separately. The single-ingestion-date retained M2 dataset cannot prove multi-day discovery behavior.

## Owner-approved amendment and decisions

The owner accepted all seven decisions on October 20, 2026, with this amendment: keep the consumer synchronous; defer the dedicated cancellable worker, hard 30-second deadlines, process termination/reaping, timeout tests and deadline-only execution infrastructure. Retain all integrity, admission, DuckDB memory/thread, pagination, string-byte and encoded-response checks. A simple one-active-request guard is permitted. Measure observed open/report durations without promising interactive latency. No other accepted decision changes.

The accepted decisions are:

1. **Dependency boundary and retention:** full existing Gold consumer verification, E1a public opening, retained Gold/E1a required for every read/replay, and no Silver reconstruction.
2. **Datasets and time meanings:** exact catalog columns, source-only left join, snapshot-wide reference version, UTC first-discovery groups and query-derived coverage.
3. **Identity and evidence:** canonical schema/semantic constants, exact Gold input/completion digests and copied metadata, E1a fingerprint binding, physical-dependency sensitivity and exclusion of operational clocks/roots.
4. **Publication and recovery:** dedicated namespace, one writer, complete expected/actual comparisons, completion-last protocol and fail-without-repair behavior.
5. **Consumer boundary:** exact three reports/projections, filter/pagination rules, integer/time encoding, mandatory dependency verification and no new API/UI.
6. **Admission limits:** proposed file/metadata/byte/row limits, DuckDB memory/threads, synchronous session concurrency and response-size limits; these are conservative policy proposals, not measured capacities.
7. **Implementation scope and validation:** specific modules/adapters, additive optional Gold memory-budget argument with unchanged defaults, no new dependencies/infrastructure, and the full Stage B acceptance matrix.

Stage B is authorized under this amended acceptance. Accepted ADRs 0001–0006 remain unchanged; implementation and executed validation are recorded separately in the detailed contract.
