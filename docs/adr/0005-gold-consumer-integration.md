# 0005: Gold consumer integration for M6

- Date: 2026-10-18
- Status: **Accepted**
- Owner acceptance: October 18, 2026.
- Scope: M6.1 feasibility findings and the accepted bounded Python CLI adapter architecture for M6.2. Acceptance authorizes implementation of the adapter, not deployment of the unverified Java JDBC POC.

## Context and requirements

M1 and M2 supply immutable Gold v1 Parquet publications and a standalone Python/DuckDB consumer. M6 will add Spring Boot APIs and a React/TypeScript dashboard. [ADR 0004](0004-gold-analytics-publication.md) remains Accepted; this proposal changes neither Gold contracts nor publication layout, transformation ownership or integrity semantics. No API, frontend, broker, persistent database, cloud resource or paid service is implemented here.

The current Python consumer is a validation boundary, not a thin SQL wrapper. It checks explicit identity, supported versions, strict completion metadata, canonical manifest and hashes, exact file inventory, every physical schema, required values, keys, counts, cross-table reconciliation and observed changes. The [reviewed responsibility matrix](../architecture/consumer-integration.md#reference-responsibilities-reviewed-before-the-experiment) records the exact behavior before the JDBC experiment. Production integration must retain it without requiring Spark, Silver or SeaweedFS at query time.

Consumers must preserve source-scoped article identity, complete selected-observation metadata, nulls, UTC microseconds, warning lists, deterministic ordering and bounded parameterized filters. Observations are not page views; warning totals count codes; global distinct ingestions cannot be obtained by summing per-article counts. Both products must belong to one explicitly pinned publication, with no mutable latest pointer.

## Evaluated architectures

| Concern | A: Java + DuckDB JDBC + full Java verification | B: Spring Boot invokes existing Python CLI | C: Persistent Python analytics worker |
| --- | --- | --- | --- |
| Correctness | Feasible, but requires a complete independently tested port of metadata, canonicalization, hashing and SQL checks; current POC is insufficient | Reuses current verifier and named reports in the same process; wrapper must preserve arguments, results and errors | Can reuse verifier, but long-lived sessions must retain all change checks and isolate requests |
| Implementation and maintenance | Largest validation effort; two implementations and SQL copies need conformance tests or a deliberate future migration | Small process adapter and deployment packaging; one analytical validation implementation | Protocol, lifecycle, readiness, restart, backpressure and concurrency add scope |
| Runtime dependencies | JVM, bundled native JDBC library and JSON library; no Python at query time | JVM plus pinned Python 3.12 and DuckDB 1.4.5; no Spark, Boto3 or storage service at query time | Same languages plus a supervised worker/service lifecycle |
| Performance characteristics | Embedded calls can reuse a connection; hashing and scans still required; cold native load costs observed locally | Process launch per request and repeated validation; bounded local startup measured, throughput not measured | Potential amortization of startup; benefit not measured and cannot justify omitting validation |
| Failure modes | Native loading/ABI errors, JVM resource pressure, drift in ported checks, lifecycle leaks | Timeout, oversized output, process exit, environment mismatch; wrapper must drain both streams and reap children | All subprocess risks plus stale state, protocol failure, queue overload and restart coordination |
| Operations | One application runtime, but platform-native library and temp-directory requirements | Two pinned runtimes in one local deployment; explicit executable/module/config paths, no separate listening service | Additional supervision and persistent process state |
| Portability | macOS ARM64 and Linux amd64 tested; other platforms require validation | Existing macOS and hosted Linux Python evidence; wrapper/deployment still to be built | Python portability plus protocol/process supervision tests |

A Java-only deployment could be attractive later. The experiment demonstrates that SQL and type mapping are practical; it does not demonstrate that a verifier port is cheaper or safer. A one-time Python precheck followed by unverified Java queries is not an adequate substitute: it splits the boundary and loses per-query change and result validation. No worker implementation was built because there is no workload evidence justifying it.

## Experimental evidence and compatibility

The isolated [Java POC](../../services/api/README.md) uses Java 21, `org.duckdb:duckdb_jdbc:1.4.5.0`, Jackson 2.22.3 already used in the repository, and JUnit 6.1.3 for tests. No Spring Boot or connection pool was needed. [DuckDB's JDBC documentation](https://duckdb.org/docs/lts/clients/java) describes the embedded JDBC interface; [Maven Central's artifact metadata](https://repo.maven.apache.org/maven2/org/duckdb/duckdb_jdbc/1.4.5.0/duckdb_jdbc-1.4.5.0.pom) and the executed driver confirm the pinned artifact. JDBC reports database version `v1.4.5`, matching the Python 1.4.5 engine baseline.

M2.4 generated full, earlier and empty synthetic publications using the existing test fixture builder, now shared between Python and JDBC tests. Python validated the publications and produced 28 reference report/filter cases. All complete result arrays matched Java, including all article provenance fields, ordering, null dates, microseconds, warning arrays and earlier/empty selection. No transformation or producer was changed.

**48 Java tests passed** on macOS ARM64 (GraalVM Java 21.0.7) and in an offline Linux amd64 container (Temurin 21.0.12.1, emulated on the ARM64 host). Linux execution mounted only the experiment, synthetic Gold and cached Maven dependencies; no Spark, Silver, Python or storage service was present. This is local Linux evidence, not an executed GitHub Actions run. CI now runs fixture generation and JDBC tests after the existing processing suite; hosted execution of this change remains pending owner publishing.

The JAR bundles macOS universal ARM64/x86_64 and Linux amd64/ARM64 native libraries. Its inspected driver bytecode targets Java 8, but this experiment requires and tests Java 21; other JVM versions are not certified. Writable temporary space permitting native library mapping is required. The first hardened Linux attempt failed with `/tmp` mounted `noexec`; the same tests passed with executable temporary storage. No external DuckDB installation or native compilation was required.

JDBC `INTEGER` and `BIGINT` map to `Integer` and `Long`, and aggregate `HUGEINT` values preserve integer precision through JDBC/JSON rather than floating-point conversion. `TIMESTAMP` is read as `LocalDateTime`, interpreted under the Gold UTC contract and serialized with exactly six fractional digits plus `Z`; `DATE` is `LocalDate`. Nulls remain null. `java.sql.Array` is copied into detached lists and freed while result sets are open. Tests passed with the JVM timezone set to Honolulu. Try-with-resources closes result sets, statements and connections, including query failures. Fixed SELECTs and parameterized paths/filters do not write published data; before/after file checks passed. This is not a general JDBC read-only sandbox.

## Publication integrity assessment

The POC intentionally returns **`UNVERIFIED_POC`**. It implements only explicit fingerprint/path selection, completion presence, selected version/identity checks, unique flat inventory paths, no globbing and rejection of missing files or symlinks along selected paths. SQL initialization and query failures produce fixed diagnostics without underlying exception text.

It does **not** implement full completion/manifest validation, canonical manifest verification, file lengths/hashes, unexpected-file rejection, every-file schema enforcement, required-value/key/count validation, cross-table reconciliation or change detection. Merely opening the same bytes successfully is not sufficient verification. The [coverage matrix and damaged-copy results](../architecture/consumer-integration.md#integrity-coverage-and-demonstrated-gaps) distinguish implemented Java protection from gaps. Six corruption cases are deliberately accepted by Java and rejected by Python; tests label these as missing protections. A separate test demonstrates Java's failure to detect a manifest change after opening.

There is no identified JDBC blocker to porting the checks: Java NIO/MessageDigest and strict Jackson parsing can implement filesystem/metadata checks, while per-file `DESCRIBE` and SQL can implement schema/value/key/reconciliation checks. Per-file schema inspection was exercised successfully. A production port would still require canonical serialization compatibility vectors, strict integer/range/Unicode/path tests, full negative-test parity, and before/after inventory verification. Those are additional implementation and review work, not verified capabilities of this POC.

All architectures retain the existing limitations: immutable single-writer assumptions, no transactional snapshot or exactly-once guarantee, no protection against edits reverted between checks, and consistency hashes rather than cryptographic authenticity. Historical winner/metric correctness relies on M2.4 publication verification; consumers do not reprocess Silver.

## Preliminary performance observations

On this macOS ARM64 machine, five fresh-process `publishers` calls over three catalog rows, three daily rows and five accepted observations had median elapsed times of **1,167.97 ms** for the Java POC and **56.07 ms** for the Python CLI. Java ranged 1,152.28–1,385.31 ms; Python ranged 55.81–107.30 ms. Calls alternated, used warm filesystem caches after testing, captured JSON and checked reference equality. Timing includes process startup, native initialization, query and serialization; Java omits verification work performed by Python. No isolated SQL latency, memory, throughput or concurrency measurement was made.

These are preliminary local observations, not production benchmarks or a fair engine-speed comparison. They provide no evidence that a persistent worker or connection pool is necessary. See the [experiment guide](../architecture/consumer-integration.md#validation-evidence-and-measurement-limits) for method, regressions and separate retained-data results.

## Accepted decision and consequences

**Accept B for M6.2: a bounded Spring Boot adapter invoking the existing Python analytics CLI.** Retain its full validation/query boundary. Configure a fixed Python executable, module location and Gold root; pin the fingerprint in operator configuration and accept only validated filters/limit from API requests, selecting fixed report names in application code. Pass arguments with `ProcessBuilder` as an argument list, never through a shell. Do not expose filesystem roots or arbitrary SQL as request parameters.

This recommendation minimizes new correctness logic and services at the cost of packaging Python alongside Java and paying process startup per request. The owner accepted this architecture on October 18, 2026; the historical experiment findings above remain unchanged. Implementation and validation evidence are tracked in the [M6.2 API guide](../../services/content-api/README.md). Keep the JDBC experiment isolated; do not route production requests through it. Reconsider A only with a fully tested verifier port and a concrete deployment need. Reconsider C only after measured request rates and latency show that process reuse justifies the added operations.

## Required M6.2 follow-up

1. Owner acceptance recorded October 18, 2026; implement the bounded Python CLI adapter, keeping the unverified JDBC POC isolated.
2. Define API report DTOs, null/UTC/list serialization, bounded limits and publication selection; address JavaScript's safe integer range without changing analytical meanings.
3. Build the process adapter with a fixed executable/environment/working directory, separate bounded stdout/stderr draining, JSON envelope validation, timeout, cancellation, child cleanup and bounded concurrency. Preserve structured error codes and redact internal diagnostics.
4. Test argument injection, missing runtimes, malformed/oversized output, nonzero exit, timeout/cancellation, process startup failure and resource cleanup, alongside existing corruption tests. Never return partial output as successful analytics.
5. Package only Python/DuckDB consumer dependencies for runtime; keep Spark and fixture-generation dependencies in development/CI. Validate macOS/Linux packaging and run hosted CI after owner publication.
6. Define read-only filesystem permissions and operating limits; validate behavior under concurrent requests and input mutation while preserving existing checks. Do not add a latest pointer or weaken validation through unreviewed caching.
7. Limit M6.2 to the publisher API foundation; keep remaining endpoints and UI work scoped to later issues. Treat titles, summaries and URLs as untrusted publisher data; define rendering, URL policy and pagination before a dashboard ships.
