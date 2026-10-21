# ADR 0009: Reliability format equivalence and local benchmarks

Status: **Accepted**

Proposal and owner acceptance date: October 20, 2026, for [issue #46](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/issues/46), combining E3 and E4a. The owner approved all eight decision groups, subject to the two execution-policy amendments below. The current owner-approved sequencing is E3 correctness now; E4a performance experiments deferred; E5–E7 end-to-end delivery next. No 100K/1M execution is authorized in the current E3 task.

## Context and requirements

PR #45 is merged at 257af66. Accepted ADRs 0001–0008 and the JSONL E2 implementation are present. E2 registration, staging, occurrence keys and independent raw comparison explicitly depend on physical JSONL lines. Adding a filename extension would lose those guarantees. Parquet row groups and record positions require a separate representation contract.

Issue #46 needs credible comparisons of the same synthetic records, independently verified exact metrics and rank-qualified approximate percentiles. A 103,000-occurrence baseline precedes a numerically gated 1,030,000-occurrence target. Existing admission ceilings are bounds, not evidence that the target fits 512 MiB no-spill verification. Local Spark thread counts are not multi-worker evidence. No speedup or throughput threshold is predetermined.

The [normative format/benchmark contract](../architecture/reliability-format-benchmarks.md) specifies the versioned formats, interfaces, identities, generator vectors, oracle calculations, measurement files and acceptance groups. The [E2 contract](../architecture/application-reliability-publication.md) remains authoritative for v1.

## Decision

1. **Version the boundary:** add product contract 2 / processor revision 2 with reviewed JSONL-v2 and Parquet-v1 input profiles, registered-batch v2 and a 20-column reliability_occurrences_v2. Keep the existing events_v1 and requests_5m_v1 schemas and numerical meaning. Use separate v2 namespaces, modules and explicit readers. Do not mutate/reinterpret v1 manifests, schemas, fingerprints, replay or consumers. A publication selects one homogeneous format; mixed-format history and automatic migration are deferred.
2. **Use narrow format adapters:** both adapters emit the same validated typed ten-field model and format-aware safe staging. Spark starts at that exact inventory. Reuse the canonical serializer and service-reference public opener unchanged. Existing domain-private writers/verifiers are patterns, not a generic framework. The legacy JSONL parser/indexer can supply its unchanged line semantics through a thin new adapter; the v1 publisher/reader cannot serve v2.
3. **Prove Parquet positions:** use DuckDB 1.4.5 file_row_number plus bounded footer row-group prefix counts. Decode an exact file once, explicitly order by its supplied physical ordinal, and derive zero-based group/within-group ordinals. Do not trust returned scan order. Stage B must prove positions, coverage, timestamp annotations and bounded reads before implementing the rest. Failure stops for a capability/dependency decision; no invented row numbers or automatic library installation.
4. **Compare separate scopes:** hash canonical valid-occurrence, unique-event and safe-rejection multisets separately. Exact business values, deduplication, reference semantics and exact metrics must agree. Approximate p95 values may differ; both must pass ADR 0008's independent rank interval rule against the same durations. Physical identities and completion bytes remain format-specific.
5. **Freeze the generator:** use seed 20261019, indexed UUID8-shaped identities and documented SHA-256 field mappings. Preserve an exact 60% hot-service quota while replacing the correlated index-based status bands with a separately hashed status assignment. Record actual status/duration proportions; do not promise statistical independence. Keep typed-invalid parity workloads separate from parser/privacy/corruption/conflict failures.
6. **Use a disk-backed independent oracle:** standard-library SQLite holds full typed identities, safe occurrence evidence and indexed durations; a streaming Python checker calculates exact arithmetic and ranks without whole-history lists. Parquet decoding uses the installed DuckDB codec with separate oracle inspection code. This is independent business/provenance verification, not a second independent Parquet codec. Literal fixtures and the capability gate address that limitation explicitly.
7. **Measure comparable work:** compare both formats under the new revision, local[1]/local[2], fixed resources and matched file policies. Record preparation outside processing, exclusive timing spans inside verified publication, raw-format queries separately from downstream Parquet reports, and replay separately from fresh writes. Use one warm-up and five measured paired repetitions per required baseline cell; target scope follows the amendment below. Preserve all samples and report median/range; a latency p95 claim requires at least 30 samples.
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

## Owner-approved decision groups

The owner accepted these eight decision groups:

1. Product/batch/processor v2, the exact 20-column occurrence contract, homogeneous formats, separate modules/readers and explicit re-registration policy.
2. The proposed DuckDB position proof and stop condition; additional footer/group/string bounds and typed Parquet rejection/privacy policy.
3. The generator's quota/hash combination, corrected status distribution, UUID8-shaped identity domain, fixed batches/files/reference and failure variants.
4. The three logical checksums and literal vectors; SQLite oracle envelope and its shared-codec limitation.
5. The full baseline matrix with five repetitions, amended target scale-validation scope, cache disclosures, fixed report/scan scopes and opt-in local-only benchmarks.
6. Timing/instrumentation/result schemas, missing-metric rules and source/configuration fingerprints without changing product semantics.
7. Conservative numeric preflight, staged stop rules, manual cleanup/retention and no automatic resource increases.
8. Checkpoints A–G and acceptance groups F01–F18 under this issue/branch, followed by one owner-managed PR. A blocked target may close the experimental scope only with truthful baseline/blocked evidence, never a fabricated target pass.

## Owner-approved execution-policy amendments

1. **Proportional target execution:** the full 100K matrix remains mandatory: JSONL/Parquet, local[1]/local[2], one warm-up and five measured fresh publications per cell, oracle, replay, consumer and raw-query measurements, retaining every sample. Minimum 1M scope is scale validation: one independently verified fresh publication per format under one predeclared matching Spark configuration (prefer local[2] when admitted), complete raw/typed/aggregate correctness and p95 qualification, resource observations, representative reports and immutable replay. A full repeated 1M comparative study is optional and needs separate resource/time approval. A single target sample per format establishes processing and verification of that workload, not a statistically supported speedup or scaling trend.
2. **Explicit target go/no-go:** PREFLIGHT_PASS is necessary but never authorizes target generation or execution. Present actual machine/baseline evidence, peak memory/disk, conservative target estimates, runtime interval, verifier/oracle budgets and uncertainties; then stop for explicit owner go/no-go. Target execution is NOT_ATTEMPTED with OWNER_PENDING or OWNER_DEFERRED reason after a passing preflight. PREFLIGHT_BLOCKED means failed or unknown numeric conditions. BENCHMARK_PASSED means a completed independently verified sample. F17 executed target evidence remains pending until separately authorized and successful.

Acceptance does not establish implementation, benchmark or hosted-CI completion. Accepted ADRs 0001–0008 and wider product/M6 status remain unchanged. Capability failure still stops checkpoint A for owner review.

## Historical partial Stage B evidence and resource stop

At the earlier stop, the installed-runtime capability gate and small G1 v2 intake/publication/report/replay tests had passed; oracle qualification and acceptance/fault coverage were still partial. An early read-only resource screen observed 176,947,200 available bytes under the approved free-plus-speculative formula, below the required 7,516,192,768 bytes on this 16 GiB host; no finite operator experiment-time budget was supplied. Work stopped before the 4,096-event pilot or any 100K/1M generation/execution. The [historical evidence](../architecture/reliability-format-benchmarks.md#historical-stage-b-partial-implementation-and-resource-stop--october-20-2026) preserves that result. It is not the current E3 acceptance status. No complete baseline, target preflight, benchmark pass or new hosted-CI result is claimed.

## Owner-approved sequencing amendment — E3 correctness now

The owner revised issue #46 to JSONL/Parquet Format Equivalence & Correctness. Original format, provenance, identity, privacy, schemas, immutable publication, independent verification and percentile decisions remain Accepted. E3 completes correctness and regressions with small/moderate bounded synthetic fixtures. E4a’s 100K/1M methodologies above remain planned, with no historical result implied. F14–F17 performance experiments, sample registry, event-log instrumentation and numeric hardware preflights are deferred; they are not E3 closure gates.

The earlier macOS memory screen was a benchmark-specific observation, not an application architecture decision or a requirement to reject small correctness work. Business processing retains portable schema/admission limits, bounded DuckDB/SQLite verification and fail-closed handling of actual resource exhaustion. Benchmark hardware admission/collectors remain separate future E4a work. E3 does not require Spark event logging, a performance plan or an operator experiment-time budget. Accepted ADRs 0001–0008 and wider product status remain unchanged.

## E3 implementation evidence

The v2 registrar, format-aware staging, Spark transformations, immutable publisher, independent DuckDB reader, deterministic bounded generator and independent SQLite correctness oracle are implemented. The [current E3 validation record](../architecture/reliability-format-benchmarks.md#e3-correctness-implementation-and-validation--october-20-2026) separates functional evidence from deferred benchmark work. Physical provenance, exact types/time, full raw/output correspondence, rejection/conflict rules, approximate-rank qualification, replay and failure handling are correctness requirements. Detailed resource collectors, repeated performance experiments and target preflight remain deferred. No 100K/1M dataset or benchmark was executed by this delivery.
