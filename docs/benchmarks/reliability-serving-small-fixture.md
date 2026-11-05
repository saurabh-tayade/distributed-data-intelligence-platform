# Reliability consumer: bounded serving evidence

This closes the measurement/documentation gap in [#58](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/issues/58)
for the implemented [Reliability consumers](../architecture/reliability-analytics-api.md).
It measures the unchanged verified native reader, native CLI and real loopback Spring HTTP
endpoint. It is a small synthetic fixture experiment, not a throughput, scale, production,
Spark-producer or hosted-deployment benchmark. The original request-window experiment and later coverage extension below preserve native contracts.

## Workload and environment

Measured November 4, 2026, starting at 18:32:03 UTC, on source commit
`24a05405d9ea41f2d2e566b43b743d65e9f2acca` (merged #89). The
[harness](../../scripts/measure-reliability-serving.py) uses the existing independently
hand-authored [G1 verifier fixture](../../processing/tests/reliability_format_fixtures.py).
It does not generate expected analytical metrics with the production processor.

Each input format has one batch/file with ten physical occurrences: five valid deliveries,
four unique valid events, one identical duplicate and five rejected occurrences. Published
tables contain four events, ten occurrences and three five-minute metric groups. The reference
contains three services: two used matches and one unused entry; an event service is unmatched.
The measured interval is `[2026-10-01T00:00:00.000000Z, 2026-10-01T00:10:00.000000Z)`.
It selects two rows with `limit=1`: offset 0 returns svc00 (two requests, sum 30 ms),
offset 1 returns svc01 (one request, sum 7 ms, null optional reference attributes).
The third, pre-epoch metric group is outside this interval. Both input formats publish Parquet
analytical tables; `JSONL`/`PARQUET` here distinguishes their retained raw input format.

| Retained data | JSONL bytes / files | Parquet bytes / files |
| --- | ---: | ---: |
| Completed publication, including manifests and three tables | 14,336 / 5 | 14,313 / 5 |
| Batch dependency, including manifest and completion marker | 3,459 / 3 | 2,984 / 3 |
| Reference dependency, including vocabulary and metadata | 1,057 / 4 | 1,057 / 4 |
| Total selected publication and dependencies | 18,852 / 12 | 18,354 / 12 |
| Raw event file alone, already included above | 2,439 / 1 | 1,817 / 1 |

Unselected fixture-construction copies are excluded from these sizes. The harness records
SHA-256, size and modification time for every fixture file and requires the inventory to be
unchanged after all reads. Publication fingerprints are:

- JSONL: `c9562eecc14454a3a64df23308704d679e79f682dc7ddc4a0830de35a0339752`.
- Parquet: `1e1057fe67b56ffa83fa4f251ea09aeafccf5eefa214a112e053b8de9fa7dfd9`.

These identify this run. Regenerated dependency completion timestamps can change publication
fingerprints on reruns even when the logical fixture and its expected analytical rows match.

Actual environment: Linux 6.18.57 x86_64, glibc 2.41; AMD EPYC 9V74 80-Core Processor
reported by the host, **nine visible/affinity CPUs**, and `/proc/meminfo` reporting 10,206,472 kB
total memory. These are visible sandbox facts, not a claim of dedicated 80-core hardware.
Cgroup CPU/memory limits were unavailable at the standard paths. Python 3.12.14, DuckDB 1.4.5,
Amazon Corretto Java 21.0.13+11-LTS, Maven 3.9.11; Spring Boot 4.1.1 from the unchanged POM.
DuckDB retains two threads, 512 MiB memory limit, no spill directory and disabled extension
autoload/install. The API retains the default two-process admission limit, 30-second native
timeout, 1,048,576-byte stdout and 65,536-byte stderr budgets.

The shared environment was not isolated from host activity, and CPU frequency/governor,
storage latency and cache state were not controlled. The harness issued requests serially,
with no concurrent load generator. No other local build was intentionally overlapped with
measurement. It runs JSONL then Parquet, so format comparisons can include time/order effects.

## Method and timing boundaries

There are five samples per format and path, no discarded samples or explicit warmup pass.
Fixture construction and JAR compilation are outside request measurements. Fixture generation
and prior reads warm filesystem caches; **fresh process does not mean cold filesystem cache**.
Wall-clock duration uses `perf_counter_ns()`. Results below are median with observed min–max,
not confidence intervals, p95/p99 estimates or service-level objectives.

1. **Native session:** each sample is a fresh Python harness child. Native module import is
   timed separately, excluding Python/interpreter and standard-library harness startup.
   `open_publication()` is then timed through full verification and report preparation.
   Local timing wrappers around existing functions report its dependency-verification,
   DuckDB-connection, table-registration, relational-validation, raw-correspondence and report-view
   phases. Wrappers do not skip or replace verification. These phases are **inside** the open
   total, not additional costs; residual work includes publication capture/hash and final checks.
2. **Native query/report:** two `report('requests_5m', ...)` calls use that same verified
   session in offset order. Timing includes filter/limit validation, byte-budget query, result
   SQL, projection, response-size serialization check, and publication/dependency rehashes
   before and after execution. It is the usable report-call latency, **not SQL-only latency**.
   Reusing a session avoids repeated schema/metric/raw-semantic verification but does not avoid
   rereading retained bytes. There is no claim that work is proportional only to the page size.
3. **Native CLI:** each page starts a new, uninstrumented Python CLI process. Timing covers
   process launch, imports, full open/verification, report, output serialization and exit through
   captured stdout. Five independent first/later page pairs are measured; they share no reader.
4. **HTTP:** each sample starts a fresh packaged analytics-role JVM, bound only to loopback,
   with an explicit fixture publication and no configured Gold publication or database.
   Process start through health readiness is reported separately (50 ms readiness polling).
   The first analytical request uses a fresh HTTP/1.1 connection; the subsequent offset-1
   request uses that connection. Each HTTP request still launches a separate native child and
   performs full verification. Timing ends after complete body receipt, before client JSON
   decoding/assertions; server validation and serialization are included. First-request JVM/MVC
   initialization and connection setup can contribute to the first/later difference.

All measured report envelopes must agree across native session, CLI and HTTP field-for-field,
including JSON primitive types, exact counters, timestamps, nulls, ordering and `hasMore`.
The HTTP projection follows the existing contract; the benchmark does not supply its own SQL.
This is 20 native report calls, 20 fresh CLI calls and 20 HTTP calls across ten JVMs.
Do not subtract independently sampled medians to claim a precise HTTP-overhead component.

## Observed timings

Milliseconds, median (minimum–maximum), **n=5 per cell**:

| Timed boundary | JSONL | Parquet |
| --- | ---: | ---: |
| Native module import | 94.3 (80.0–117.3) | 71.6 (70.5–82.5) |
| Open fully verified native session | 118.1 (106.0–141.4) | 148.4 (132.3–168.3) |
| First native report in that session | 9.2 (8.7–14.1) | 8.9 (8.5–9.6) |
| Later native report in that session | 8.9 (8.7–12.9) | 8.1 (7.7–9.0) |
| Fresh CLI, first page | 267.2 (250.3–274.8) | 284.9 (278.9–337.4) |
| Fresh CLI, later page | 260.9 (240.6–262.4) | 273.4 (263.4–299.8) |
| HTTP, first analytical request | 308.5 (293.0–318.0) | 358.7 (322.2–479.9) |
| HTTP, later page | 278.0 (265.0–278.3) | 296.7 (278.7–440.1) |
| JVM start through health readiness, excluded above | 2,811.4 (2,792.4–3,005.7) | 3,080.1 (2,855.2–3,218.6) |

Nested native-open phase timings, also median (minimum–maximum) ms, n=5:

| Existing function boundary | JSONL | Parquet |
| --- | ---: | ---: |
| Retained dependency verification | 15.5 (12.6–21.1) | 33.1 (29.9–37.4) |
| Main analytics DuckDB connection/configuration | 9.0 (7.8–9.3) | 6.8 (6.3–12.3) |
| Register all three publication tables and validate schemas | 10.1 (9.5–12.0) | 9.6 (8.7–11.0) |
| Relational/count/metric/reference validation | 68.8 (63.0–80.6) | 62.3 (56.7–80.3) |
| Raw occurrence/event correspondence | 3.8 (3.5–10.0) | 21.3 (19.8–21.9) |
| Prepare report views | 2.7 (2.6–3.3) | 2.5 (2.4–2.7) |

The connection row measures only the main analytics connection, not connections used inside
dependency/raw format readers. Sub-phase medians need not sum to the total median. HTTP bodies
were 950/928 bytes for JSONL first/later pages and 952/930 for Parquet; CLI bodies were
1,005/983 and 1,007/985 bytes, respectively, including the CLI newline. These tiny bodies do not
measure worst-case 1,000-row responses. Parquet's smaller retained bytes did not imply faster
verification for this fixture; this is not a general format-performance conclusion.

## Consumer choice and freshness

**Direct verified DuckDB session:** useful for trusted local analytical clients that can hold
a native session and access all retained dependencies. It exposes existing bounded native reports,
not a generic HTTP SQL service. This fixture shows initialization/verification costs substantially
above subsequent report-call costs. Reusing verification may help repeated local queries, but
per-report file rehashes remain, and sessions reject concurrent use with `BUSY`. Do not bypass
`open_publication()` by reading Parquet directly when claiming verified-consumer semantics.

**Bounded REST:** keeps trusted paths, runtime and verification on the operator side and exposes
only the versioned projection. Every page pays process/import/full verification cost; small limits
bound response size, not verification or scan cost. It provides a language-neutral boundary,
safe failures and bounded process admission without a persistent reader lifecycle. The measured
subsecond loopback requests do not establish remote-network latency, concurrency capacity,
large-publication behavior or production readiness. Authorization under #50 remains required
before restricted multi-user analytical capabilities are publicly exposed.

**Materialized PostgreSQL projection:** not implemented or benchmarked. There is no demonstrated
latency/concurrency requirement or representative workload here that justifies another store.
A future evaluation should compare verified ingest/refresh cost, exact numeric/null/timestamp
parity, atomic publication selection, storage and operational overhead under an agreed workload.
The present measurements do not establish a benefit, so neither PostgreSQL nor a reader cache
is introduced. Coverage reports were optional follow-on scope at the time of this original measurement; the
implemented extension and separate measurements are recorded below. No new serving store is added.

Freshness and consistency are publication-scoped:

- The operator selects one exact immutable fingerprint at service startup. No latest lookup,
  polling, fallback, automatic refresh or switch based on run success occurs.
- Native source/event/receipt timestamps keep their original meaning. Query time, successful
  health checks and publication completion time do not establish that the source is current.
  This endpoint supplies no live-data/freshness SLA or newly invented watermark.
- New events, late data or reference corrections require a new verified publication. Making it
  visible requires an operator configuration change/restart. Existing direct sessions remain
  tied to their original fingerprint; they do not become sessions over the new publication.
- Every REST page independently verifies its configured publication and dependencies. Matching
  identity and immutable retained files yield stable ordering, but there is no multi-request
  database transaction. Missing/mutated dependencies fail closed without partial results.
- Preserve filters and publication while paging. The UI pins identity for its browser session
  and blocks a mismatch until full reload. Other clients must also compare publication identity;
  a rolling operator switch must not silently combine pages from differently configured servers.

## #58 acceptance reconciliation

| Issue criterion | Implemented evidence and boundary |
| --- | --- |
| Additional documented consumer | Reliability `GET /api/v1/reliability/request-windows`, [versioned contract](../architecture/reliability-analytics-api.md). Native `requests_5m` is authoritative. |
| No arbitrary filesystem selection | Strict request parameter allowlist and operator-only roots/fingerprint; `ReliabilityWebTest` rejects invalid input before child execution, `ReliabilityApiIT` rejects publication/report overrides. |
| Verified publications only | Unchanged `open_publication()` performs dependency, schema, semantic and metric verification. Actual HTTP/native parity and corruption tests exercise that path. |
| Bounded pages and responses | Limit 1–1,000, offset 0–1,000,000; actual first/final/empty/max-offset parity cases. Native byte-budget regressions now explicitly cover `requests_5m` for both formats; shared `BoundedProcessRunnerTest` tests stdout/stderr limits, timeout and admission. |
| Independent native reconciliation | Hand-authored G1 fixture, independent verifier/oracle tests, and `ReliabilityApiIT` comparing all fields/types against actual native CLI output. The measurement harness also requires native/CLI/HTTP agreement. |
| Explicit missing/corrupt failure | Twelve damaged-fixture cases across both formats, queried with populated and empty filters, fail closed through `ReliabilityApiIT`; no guessed diagnostic classification or fallback. |
| Performance and tradeoffs documented | Reproducible bounded measurements above; no materialized-store benefit or scale claim inferred. |
| Gold v1 backward compatibility | No Gold endpoint, DTO, query, publication selection, process contract or native module is changed by this evidence slice. Existing Gold tests remain in the aggregate profile; this experiment is not a new Gold HTTP qualification. |

## Reproduce and validate

From the repository root, with `JAVA_HOME` pointing to Java 21, Maven 3.9+ and Python 3.12 available:

```sh
python3.12 -m venv outputs/serving-python/.venv
export ANALYTICS_PYTHON_EXECUTABLE="$PWD/outputs/serving-python/.venv/bin/python"
"$ANALYTICS_PYTHON_EXECUTABLE" -m pip install -r processing/requirements-analytics.txt
mvn -B -ntp -f services/content-api/pom.xml package
"$ANALYTICS_PYTHON_EXECUTABLE" -B scripts/measure-reliability-serving.py \
  --java "$JAVA_HOME/bin/java" --output outputs/serving-evidence-new --samples 5
"$ANALYTICS_PYTHON_EXECUTABLE" -B -m unittest discover \
  -s scripts/tests -p test_measure_reliability_serving.py -v
PYTHONPATH=processing:processing/tests "$ANALYTICS_PYTHON_EXECUTABLE" -B \
  -m unittest discover -s processing/tests -p test_reliability_format_verification.py -v
PYTHONPATH=processing:processing/tests "$ANALYTICS_PYTHON_EXECUTABLE" -B \
  services/content-api/tests/generate_reliability_fixtures.py \
  --output services/content-api/target/reliability-fixtures
mvn -B -ntp -f services/content-api/pom.xml -Pintegration verify \
  -Dit.test=ReliabilityApiIT -DskipUnitTests=true \
  -Danalytics.python="$ANALYTICS_PYTHON_EXECUTABLE"
npm run check
git diff --check
```

Both fixture generators require new, nonexisting output directories and never overwrite prior
evidence. Choose fresh paths, or intentionally remove disposable prior fixtures before rerunning.
The harness accepts 1–20 samples, uses bounded child/readiness timeouts, stops its JVMs and writes
`results.json` only after successful comparisons and read-only inventory checks. The JSON retains
all raw samples, environment/source/JAR/harness identities, file inventories, native reference
pages and summaries. Generated outputs and logs remain ignored and must not be committed.
The harness guard tests use the explicit local command above; hosted processing CI discovers
`processing/tests`, not `scripts/tests`, and does not run this benchmark automatically.

The local run passed 406 Spring unit/web tests, 16 native verifier tests (including the added
request-window response-budget case), 16 `ReliabilityApiIT` cases (34 native parity scenarios and
12 damaged fixtures), all 23 script tests (seven new harness guards), and all 45 CI-policy tests.
Markdown/relative-link, repository-hygiene and whitespace checks passed. The measured consumers
passed all 60 page comparisons across native/CLI/HTTP and left their fixture inventories unchanged.
Full Gold/Spark, PostgreSQL, frontend/browser, hosted CI, large-data and load testing are separate
scopes; these timings do not substitute for them.

## Coverage extension: November 5, 2026

The same harness now accepts `--report input_coverage` or `--report reference_coverage`.
These are separate five-sample runs for both raw input formats, using the new fixed HTTP
endpoints and the unchanged native reports. Source base is `47fb83f896698b7e5576de1581452341e149d1de`
plus the reviewed, uncommitted coverage slice. Each raw result records the built JAR and harness
SHA-256, exact publication identity, envelopes, timings and retained file inventory.

The [coverage fixture](../../services/content-api/tests/reliability_coverage_fixtures.py) extends
hand-authored G1 to five batches/files: 14 physical occurrences, 8 valid deliveries, 4 unique
events, 4 stored duplicates and 6 rejected occurrences. It includes an empty batch, a rejected
occurrence with two reasons, duplicate-only scope, zero delay and negative delay beyond the
JavaScript exact-integer range. Three metric groups and three selected reference keys remain.
Fixture construction retains the original G1 seed publication; raw `retained_sizes.output`
includes both seed and selected publications and must not be called selected-only output size.
Only the selected publication is queried and verified. All retained hashes, sizes and mtimes
remain unchanged after every measured path.

Input uses no filters, `limit=1`, offsets 0 and 1, yielding batches b00 and b01. Both pages
have more rows and identical complete-scope totals (14 physical / 8 valid / 4 distinct events).
Reference uses no service filter, `limit=1`, offsets 0 and 1, yielding svc-unused then svc00.
Both pages have more rows and the same total of 4 unique events. These queries have no time
or environment filter. Every session, CLI and HTTP envelope matches exactly, including reason
keys, signed strings, nulls, identity and types. Two reports together execute 40 native reports,
40 CLI calls and 40 HTTP calls across 20 fresh JVMs.

The environment, measurement boundaries and no-scale-claim limitations above still apply:
Python 3.12.14 / DuckDB 1.4.5, Corretto 21.0.13, Linux x86_64, 9 visible CPUs and approximately
10 GB reported host memory. Five serial samples have no discarded warmup; filesystem caches
are warm and uncontrolled. This task ran no builds/tests during timing, but the shared host
was not isolated. Results are observed medians/ranges, not throughput, tail latency or an SLA.

Milliseconds, median (minimum–maximum), **n=5 per cell**:

| Boundary | Input JSONL | Input Parquet | Reference JSONL | Reference Parquet |
| --- | ---: | ---: | ---: | ---: |
| Native import | 52.7 (47.2–58.8) | 54.6 (49.4–68.8) | 54.8 (50.9–56.8) | 54.1 (47.7–58.2) |
| Fully verified session open | 112.0 (104.0–135.6) | 274.3 (253.6–298.4) | 130.4 (104.6–132.0) | 271.7 (269.8–306.4) |
| First native report | 18.9 (17.4–19.4) | 19.5 (18.2–21.6) | 14.5 (11.7–19.1) | 15.4 (15.0–16.7) |
| Later native report | 18.1 (16.8–19.7) | 19.5 (18.0–22.8) | 14.6 (10.8–18.5) | 13.1 (12.9–16.5) |
| Fresh CLI, first page | 257.3 (250.7–267.1) | 395.2 (388.1–428.7) | 264.0 (234.5–280.2) | 394.2 (382.3–486.0) |
| Fresh CLI, later page | 275.1 (243.9–280.3) | 419.3 (378.7–453.9) | 259.3 (243.7–267.3) | 409.1 (399.1–458.0) |
| HTTP, first request | 304.4 (284.7–311.8) | 501.9 (442.0–570.4) | 311.5 (272.5–312.7) | 456.0 (427.7–506.6) |
| HTTP, later page | 267.8 (255.6–286.5) | 440.8 (402.8–490.6) | 268.2 (236.0–319.0) | 432.1 (407.5–443.3) |
| JVM health readiness, separate | 3006.4 (2845.3–3194.5) | 3005.5 (2784.2–3736.3) | 3278.5 (3169.2–3413.2) | 2903.5 (2855.3–3121.4) |

Nested open-phase medians, milliseconds; these are already included in session open:

| Existing phase | Input JSONL | Input Parquet | Reference JSONL | Reference Parquet |
| --- | ---: | ---: | ---: | ---: |
| verify_dependencies | 12.6 | 93.2 | 13.8 | 93.6 |
| connection | 8.0 | 7.5 | 9.3 | 7.2 |
| register_table | 10.0 | 10.3 | 10.5 | 9.2 |
| validate_tables | 67.0 | 70.0 | 75.0 | 70.4 |
| verify_raw | 4.5 | 79.9 | 5.0 | 82.2 |
| prepare_reports | 2.4 | 2.4 | 2.9 | 2.8 |

These tiny-fixture results continue to show verification/startup dominating repeated same-session
reports. They do not justify caching verification, introducing PostgreSQL projections, weakening
dependency checks or making a general raw-format performance claim. No browser/remote-network
latency, concurrency, worst-case response or production workload was measured.

Reproduce after the API build, using a new output directory for each run:

```sh
python scripts/measure-reliability-serving.py --report input_coverage \
  --output outputs/reliability-input-serving --samples 5 --java "$JAVA_HOME/bin/java"
python scripts/measure-reliability-serving.py --report reference_coverage \
  --output outputs/reliability-reference-serving --samples 5 --java "$JAVA_HOME/bin/java"
python -I -B -m unittest discover -s scripts/tests -p test_measure_reliability_serving.py -v
```
