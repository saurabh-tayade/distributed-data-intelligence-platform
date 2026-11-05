# Content catalog: small-fixture serving evidence

This experiment measures the [Content catalog HTTP boundary](../architecture/content-catalog-api.md) for issue #58. It is a local four-article synthetic fixture experiment, not a scale, load, production, retained-data, or Spark-producer benchmark. It does not establish an interactive-latency guarantee.

## Workload and method

The [fixture generator](../../services/content-api/tests/generate_content_fixtures.py) publishes literal Gold rows through the existing Spark Content producer and pins the existing reference publisher's snapshot. Four articles cover three sources: alpha has matched attributes, beta is matched with null optional attributes, and gamma has no reference. The reference also has an unused delta row. The two measured pages use `limit=1`, offset 0 (gamma) and offset 1 (alpha); both have `hasMore=true`. All readers use the same selected immutable Content, Gold and reference artifacts.

The [measurement harness](../../scripts/measure-content-serving.py) runs five samples per path, serially, with no discarded samples or explicit warmup. Generation and JAR compilation are excluded. Prior verification and generation warm filesystem caches: a fresh process does not mean a cold filesystem cache. It records environment/runtime versions, source commit, harness/JAR hashes, publication fingerprint, retained byte/file counts, raw timings, exact expected native pages and a file inventory. SHA-256, sizes and nanosecond modification times must remain unchanged after all measurements.

Timing boundaries use `perf_counter_ns()`:

1. **Native import:** fresh Python child, timed native-module import; excludes interpreter and standard-library harness startup.
2. **Initialization and verification:** `open_publication()` through complete dependency and publication verification. Nested function timings identify dependency verification, main connection creation, physical schema registration, and relational validation. Nested phases are part of the total, not additive overhead.
3. **Native report:** first/later `report('catalog')` on the same verified session. Includes argument validation, string-byte preflight, lookahead query, projection, encoded-response size check and retained-evidence checks before and after the query.
4. **Query only:** diagnostic SQL on that already verified session after its full report. Includes parameterized native catalog projection, order, limit-plus-one, offset and fetch. Excludes integrity checks, byte preflight and public serialization. This is not a supported serving path or permission to bypass verification; this timing intentionally benefits from prior open/report work and caches.
5. **Fresh CLI:** separate native process per page, including launch, imports, complete verification, report, output serialization and exit.
6. **HTTP:** fresh packaged analytics-role JVM per sample, loopback only. Health readiness is timed separately; the first analytical request opens an HTTP/1.1 connection and the later page reuses it. Each HTTP request starts a fresh native process and fully verifies all evidence. Timing ends after the complete response body, before client JSON decoding. First-request MVC/JVM initialization and connection establishment may affect first/later differences.

All native session, CLI and HTTP envelopes must agree field-for-field and JSON type-for-type, including exact decimal strings, six-digit UTC timestamps, nulls, warning order, row order and pagination. Query-only timing uses the unchanged native projection/filter definitions; it is separately labeled and never supplies the HTTP result. No projection cache or verification bypass is introduced.

## Observed local results

Measured November 5, 2026, starting 04:44:43 UTC, on the issue #58 working tree based on
`f0fdc13fe25c3653192660ed69031aab94d18232`. The measured JAR SHA-256 was
`acc43f78e325ee8dacd1ebdc242e34fbc4ead82c2ed50c562a81608464f76c20`;
the harness SHA-256 was `5c519a5965cc14f4a9e8eddecfbd0514371e155c0c879a82e801326a1afc988f`.
This identifies the executed artifact, not an assertion that these changes were already committed.

Environment: Linux 6.18.57 x86_64, glibc 2.41; host-reported AMD EPYC 9V74 80-Core Processor,
**nine visible/affinity CPUs**, 10,206,472 kB total memory reported by `/proc/meminfo`.
These are sandbox observations, not dedicated hardware allocations. Standard cgroup CPU/memory
limit paths were unavailable. Python 3.12.14, DuckDB 1.4.5, Amazon Corretto Java 21.0.13+11-LTS,
Maven 3.9.11 and Spring Boot 4.1.1. No other local build was intentionally overlapped with this
measurement; shared-host activity was uncontrolled. The native reader retains its two-thread,
512 MiB, no-spill DuckDB configuration and disabled extension autoload/install. The API uses
the unchanged shared runner defaults.

Selected publication: `04a90262b8b6911a25a612e34a6a3813002c1d8ff1980ea6f6350d9a0283c97c`.
The catalog contains four rows and the discovery-daily table three rows. Retained on-disk sizes,
including Spark sidecar files and completion/manifest evidence, were:

| Root | Bytes | Files |
| --- | ---: | ---: |
| Content | 19,740 | 12 |
| Gold | 15,032 | 10 |
| Publisher reference | 1,101 | 4 |
| Total | 35,873 | 26 |

The fixture-construction CSV/vocabulary copies outside these roots are excluded from this table.
Fresh fixture generation can change fingerprints and Parquet sizes because exact dependency
completion bytes and physical output are bound; the logical workload remains fixed.

Milliseconds, median (observed minimum–maximum), **n=5 per cell**:

| Timed boundary | First page | Later page |
| --- | ---: | ---: |
| Complete native report on an already verified session | 14.1 (13.6–16.2) | 14.0 (13.5–15.1) |
| Diagnostic query only after that report | 2.8 (2.6–4.2) | 2.8 (2.6–3.2) |
| Fresh native CLI | 265.2 (252.2–294.8) | 285.3 (246.1–295.5) |
| HTTP analytical request | 290.4 (276.4–305.3) | 256.4 (246.3–313.0) |

| Initialization boundary | Median (minimum–maximum), ms |
| --- | ---: |
| Native module import | 50.8 (48.3–55.2) |
| Open fully verified session | 137.0 (131.6–147.1) |
| ↳ Retained dependency verification | 39.0 (33.2–40.0) |
| ↳ Main analytics connection | 6.9 (6.6–7.4) |
| ↳ Schema registration, all three relations | 9.4 (8.3–9.4) |
| ↳ Relational validation | 75.2 (69.9–82.4) |
| JVM start through health readiness, separate from requests | 2,791.8 (2,749.4–3,084.6) |

Nested phases do not include every capture/hash operation and their medians need not sum to the
open median. HTTP bodies were 1,355 bytes for the first page and 1,389 bytes for the later page.
All ten HTTP responses and ten CLI responses matched the native session envelopes exactly;
retained file hash/size/mtime checks passed. The eight independent harness unit tests also passed.

## Reproduce

Generate the integration fixtures as documented in the [API contract](../architecture/content-catalog-api.md), build the API JAR with Java 21, then run from the repository root:

```sh
.venv/bin/python scripts/measure-content-serving.py \
  --fixture services/content-api/target/content-fixtures \
  --java /absolute/path/to/java21/bin/java \
  --jar services/content-api/target/content-api-0.1.0-SNAPSHOT.jar \
  --samples 5 --output /tmp/content-serving-new-run
```

The output directory must not exist. The harness writes raw `results.json` and JVM logs there; no fixture or source modification is required. `python -m unittest discover -s scripts/tests -p test_measure_content_serving.py` tests harness safeguards independently of the explicit measurement run.

## Interpretation and limits

Report initialization, query-only, complete report, CLI and HTTP separately. Do not subtract independently sampled medians to claim a precise server-overhead component. Five samples support descriptive minimum/median/maximum only, not p95/p99 estimates or confidence bounds.

Direct verified DuckDB sessions can amortize initialization for trusted local consumers, but complete native reports still recheck retained evidence. HTTP deliberately starts a fresh verifier for each request and retains stronger isolation at additional startup and scan cost. Any projected serving cache would need a separately designed freshness, invalidation, admission and provenance contract; this slice makes no such change.

The bounded fixture has only four articles and small files. It says nothing about throughput, concurrency, large responses, maximum admitted publication sizes, large reference snapshots, retention failures under load, remote storage, or hosted performance. This shared environment is not a dedicated host; CPU frequency, disk latency and filesystem cache state are not controlled. Full verification costs are not proportional only to the returned page size.
