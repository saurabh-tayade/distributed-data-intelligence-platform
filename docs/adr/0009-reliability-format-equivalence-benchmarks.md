# ADR 0009: Reliability format equivalence and local benchmarks

Status: **Proposed**

Proposal date: October 20, 2026, for [issue #46](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/issues/46), combining E3 and E4a. Owner acceptance is required before Stage B. No Parquet intake, generator, oracle or benchmark implementation is authorized by this proposal alone.

## Context and requirements

PR #45 is merged at 257af66. Accepted ADRs 0001–0008 and the JSONL E2 implementation are present. E2 registration, staging, occurrence keys and independent raw comparison explicitly depend on physical JSONL lines. Adding a filename extension would lose those guarantees. Parquet row groups and record positions require a separate representation contract.

Issue #46 needs credible comparisons of the same synthetic records, independently verified exact metrics and rank-qualified approximate percentiles. A 103,000-occurrence baseline precedes a numerically gated 1,030,000-occurrence target. Existing admission ceilings are bounds, not evidence that the target fits 512 MiB no-spill verification. Local Spark thread counts are not multi-worker evidence. No speedup or throughput threshold is predetermined.

The [normative format/benchmark contract](../architecture/reliability-format-benchmarks.md) specifies all proposed formats, interfaces, identities, generator vectors, oracle calculations, measurement files and acceptance groups. The [E2 contract](../architecture/application-reliability-publication.md) remains authoritative for v1.

## Proposed decision

1. **Version the boundary:** add product contract 2 / processor revision 2 with reviewed JSONL-v2 and Parquet-v1 input profiles, registered-batch v2 and a 20-column reliability_occurrences_v2. Keep the existing events_v1 and requests_5m_v1 schemas and numerical meaning. Use separate v2 namespaces, modules and explicit readers. Do not mutate/reinterpret v1 manifests, schemas, fingerprints, replay or consumers. A publication selects one homogeneous format; mixed-format history and automatic migration are deferred.
2. **Use narrow format adapters:** both adapters emit the same validated typed ten-field model and format-aware safe staging. Spark starts at that exact inventory. Reuse the canonical serializer and service-reference public opener unchanged. Existing domain-private writers/verifiers are patterns, not a generic framework. The legacy JSONL parser/indexer can supply its unchanged line semantics through a thin new adapter; the v1 publisher/reader cannot serve v2.
3. **Prove Parquet positions:** propose DuckDB 1.4.5 file_row_number plus bounded footer row-group prefix counts. Decode an exact file once, explicitly order by its supplied physical ordinal, and derive zero-based group/within-group ordinals. Do not trust returned scan order. Stage B must prove positions, coverage, timestamp annotations and bounded reads before implementing the rest. Failure stops for a capability/dependency decision; no invented row numbers or automatic library installation.
4. **Compare separate scopes:** hash canonical valid-occurrence, unique-event and safe-rejection multisets separately. Exact business values, deduplication, reference semantics and exact metrics must agree. Approximate p95 values may differ; both must pass ADR 0008's independent rank interval rule against the same durations. Physical identities and completion bytes remain format-specific.
5. **Freeze the generator:** use seed 20261019, indexed UUID8-shaped identities and documented SHA-256 field mappings. Preserve an exact 60% hot-service quota while replacing the correlated index-based status bands with a separately hashed status assignment. Record actual status/duration proportions; do not promise statistical independence. Keep typed-invalid parity workloads separate from parser/privacy/corruption/conflict failures.
6. **Use a disk-backed independent oracle:** standard-library SQLite holds full typed identities, safe occurrence evidence and indexed durations; a streaming Python checker calculates exact arithmetic and ranks without whole-history lists. Parquet decoding uses the installed DuckDB codec with separate oracle inspection code. This is independent business/provenance verification, not a second independent Parquet codec. Literal fixtures and the capability gate address that limitation explicitly.
7. **Measure comparable work:** compare both formats under the new revision, local[1]/local[2], fixed resources and matched file policies. Record preparation outside processing, exclusive timing spans inside verified publication, raw-format queries separately from downstream Parquet reports, and replay separately from fresh writes. Use one warm-up and five measured paired repetitions per required cell. Preserve all samples and report median/range; a latency p95 claim requires at least 30 samples.
8. **Gate resource use:** preflight the baseline, then use observed baseline storage/RSS/heap/spill and verification work to gate target generation. Preserve E2 caps, two DuckDB threads/512 MiB/no spill, at least 25% host RAM reserve and twice conservative peak experiment disk free. Include every retained trial, oracle and private stage. Block rather than raise budgets. No infrastructure, multi-worker, API, export or scheduler work.

## Alternatives and tradeoffs

| Alternative | Assessment |
| --- | --- |
| New reviewed input/processor revision with format-aware provenance | Recommended compatibility boundary. More metadata/tests, but old artifacts retain their meaning |
| Standalone Parquet adapter feeding the typed model | Recommended within revision 2. It cannot safely feed occurrence-v1 or pretend its output was raw JSONL |
| Experimental direct Spark Parquet benchmark only | Useful later for isolated engine studies; it cannot establish complete E2 physical occurrence/rejection/publication equivalence. Not the primary E3 delivery |
| Add Parquet to v1 / encode row ordinal as line number | Rejected: changes accepted provenance, schema interpretation and replay |
| Rewrite v1 into a generic connector framework | Unnecessary domain refactoring and compatibility risk. Keep a narrow new implementation; reconsider sharing only after evidence |
| Spark partition order / monotonically increasing IDs | Does not prove physical positions; rejected |
| Bounded row-group reads with a new Arrow/JVM library | Plausible fallback, but adds a dependency or custom reader. Requires a separate decision if the pinned DuckDB gate fails |
| Exact p95 equality between executions | Inconsistent with Accepted ADR 0008. Independently qualify both stored candidates instead |
| In-memory duration lists or all-groups histograms | Poorly bounded under skew. SQLite external indexes trade temporary disk/I/O for bounded Python state |
| Full required matrix before any correctness run | Too risky without observations. First establish small correctness and a baseline pilot; never relabel reduced samples as a complete comparison |

## Compatibility, failure and recovery

Old JSONL registration/open/publish/report APIs remain unchanged and receive regression tests. New v2 APIs reject legacy bindings rather than silently converting them. Explicit re-registration copies retained JSONL under the new profile and creates distinct batch/product identities; it is not an in-place migration. The three new-reader report names/projections remain familiar, with explicit version/format envelope fields.

All writes remain exclusive and completion-last. Structural/privacy failures, conflicts and provenance failures prevent product reservation. Later failure leaves incomplete evidence; no automatic repair, resume, overwrite or promotion. Full raw/reference retention and read-only replay remain required. An interrupted final rename has the existing ambiguous-response resolution: independently verify the selected namespace.

Performance evidence is invalid if correctness, integrity or mandatory instrumentation fails. A blocked target preserves the completed baseline. The benchmark harness is an opt-in local command, not a durable run registry or execution service. It must never automatically delete retained inputs, completed publications or recorded evidence. No hostile-race, multiwriter, fsync/power-loss or production-readiness claim is added.

## Feasibility and evidence

Read-only source review found that [DuckDB 1.4.5's row-number reader](https://github.com/duckdb/duckdb/blob/v1.4.5/extension/parquet/reader/row_number_column_reader.cpp) initializes an ordinal from preceding row-group row counts. This supports the proposal; it does not establish runtime behavior for our fixtures, schema mapping, pruning, empty groups or memory budget. Those remain the mandatory Stage B checkpoint A gate. Do not rely on newer DuckDB-only row-group virtual columns.

Stage A calculations cover only small literal hashes, index quotas/correlation and resource formulas. No Spark/Parquet experiment, large dataset, application suite or benchmark is executed. Local documentation validation is recorded separately in the contract. Hosted CI and scale results are unverified.

## Owner decisions required before Stage B

Acceptance must explicitly approve:

1. Product/batch/processor v2, the exact 20-column occurrence contract, homogeneous formats, separate modules/readers and explicit re-registration policy.
2. The proposed DuckDB position proof and stop condition; additional footer/group/string bounds and typed Parquet rejection/privacy policy.
3. The generator's quota/hash combination, corrected status distribution, UUID8-shaped identity domain, fixed batches/files/reference and failure variants.
4. The three logical checksums and literal vectors; SQLite oracle envelope and its shared-codec limitation.
5. Required baseline/target matrix, five repetitions, cache disclosures, fixed report/scan scopes and opt-in local-only benchmarks.
6. Timing/instrumentation/result schemas, missing-metric rules and source/configuration fingerprints without changing product semantics.
7. Conservative numeric preflight, staged stop rules, manual cleanup/retention and no automatic resource increases.
8. Checkpoints A–G and acceptance groups F01–F18 under this issue/branch, followed by one owner-managed PR. A blocked target may close the experimental scope only with truthful baseline/blocked evidence, never a fabricated target pass.

This ADR remains Proposed. Accepted ADRs 0001–0008 and wider product/M6 status are unchanged.
