# Content API — verified Gold analytics (M6.2 and M6.4)

This Java 21 module implements `GET /api/v1/publishers`, `/api/v1/articles`, `/api/v1/daily` and `/api/v1/quality` over one explicitly pinned Gold publication. The owner [accepted ADR 0005](../../docs/adr/0005-gold-consumer-integration.md) on October 18, 2026. The [JDBC POC](../api/README.md) remains isolated and is not a runtime dependency. The separate [M6.3 React dashboard](../../frontend/README.md) consumes this publisher endpoint. M6.4 adds the three analytical endpoints and deterministic article pagination. `/api/v1/publication` is not implemented. The frontend continues to consume publishers only; M6.5 remains planned.

## Acceptance and structure

Acceptance requires reference-equivalent results for all four reports, explicit publication selection, exact integer and UTC microsecond serialization, safe failure responses, bounded subprocess resources, and automated real HTTP/Python integration on synthetic publications. Startup and ordinary Maven unit tests must work without Gold, Spark or Python.

The dependency flow is `Controller -> Service -> GoldAnalyticsAdapter -> BoundedProcessRunner -> gold_analytics.py`. `PublisherController`/`PublisherService` remain the publisher path; `AnalyticsController`/`AnalyticsService` handle the three new endpoints using `ArticleQuery`/`DateQuery` and report-specific typed responses. Controllers validate only their explicitly supported parameters before invoking any process. The service maps typed analytical rows to typed REST records. The adapter chooses a fixed report enum, constructs arguments and validates the CLI envelope. The runner owns admission, process/stream lifecycle and cleanup. There is no SQL, Parquet reader, JDBC connection, cache, worker service or verifier port in this module.

Spring Boot **4.1.1** manages dependency and plugin versions; Java is **21**. Runtime starters are `spring-boot-starter-webmvc`, `spring-boot-starter-actuator`, and `spring-boot-starter-validation`. Tests use `spring-boot-starter-webmvc-test`, which includes the general Boot test starter and MVC test support. Artifact names were checked against the release POMs and Spring's [MVC documentation](https://docs.spring.io/spring-boot/reference/web/index.html), [testing documentation](https://docs.spring.io/spring-boot/reference/testing/), and [managed dependencies](https://docs.spring.io/spring-boot/appendix/dependency-versions/coordinates.html). The resolved build uses Spring MVC 7.0.9, Jackson 3.1.5, Tomcat 11.0.24, Hibernate Validator 9.1.3.Final and JUnit 6.0.3. There are no dependency version overrides or new non-framework runtime libraries.

Python runtime requirements are Python **3.12** (locally tested with 3.12.15) and only `duckdb==1.4.5` from [requirements-analytics.txt](../../processing/requirements-analytics.txt). Keep `gold_analytics.py`, `gold_reader.py` and `publication_metadata.py` together. Spark/PySpark and Boto3 are fixture-generation dependencies, not query dependencies. No Silver reads, SeaweedFS, AWS credentials or publisher requests are required.

## Local startup

From the repository root, with Java 21, Maven 3.9+ and Python 3.12 installed:

```sh
python3.12 -m venv outputs/analytics-python312/.venv
outputs/analytics-python312/.venv/bin/python -m pip install -r processing/requirements-analytics.txt
mvn -B -ntp -f services/content-api/pom.xml verify
export ANALYTICS_PYTHON_EXECUTABLE="$PWD/outputs/analytics-python312/.venv/bin/python"
# Set the exact lowercase 64-character fingerprint returned by Gold publication.
export ANALYTICS_PUBLICATION='<your-completed-publication-fingerprint>'
java -jar services/content-api/target/content-api-0.1.0-SNAPSHOT.jar
```

Replace the placeholder before starting. The script and Gold-root defaults below are relative to the JVM's startup working directory. These commands deliberately start the packaged JAR from the repository root. When launching from elsewhere, set `ANALYTICS_SCRIPT` and `ANALYTICS_GOLD_ROOT` explicitly. Stop the foreground service with Ctrl-C. The JAR does not bundle Python or Gold data.

For a liveness-only start, leave `ANALYTICS_PUBLICATION` unset. Health is UP; analytical queries return `503 PUBLICATION_UNAVAILABLE`. A malformed nonempty fingerprint fails configuration validation at initialization. A missing, incomplete or corrupt selected publication fails the query explicitly. There is no latest discovery or fallback selection. Runtime executable/script availability is checked on queries so basic application startup does not depend on Python installation.

## Configuration

Spring Boot properties support environment variables or command-line `--analytics.*` arguments. Configuration is operator-controlled, never taken from HTTP parameters.

| Property | Environment variable | Default and constraints |
| --- | --- | --- |
| `analytics.python-executable` | `ANALYTICS_PYTHON_EXECUTABLE` | `python3`; trusted path or name resolved once from startup PATH; use an explicit virtualenv executable for reproducibility |
| `analytics.script` | `ANALYTICS_SCRIPT` | `processing/gold_analytics.py`; normalized absolute path at initialization |
| `analytics.gold-root` | `ANALYTICS_GOLD_ROOT` | `outputs/gold`; normalized absolute path at initialization |
| `analytics.publication` | `ANALYTICS_PUBLICATION` | Empty/unconfigured; otherwise exactly 64 lowercase hexadecimal characters; no directory paths |
| `analytics.timeout` | `ANALYTICS_TIMEOUT` | `30s`; 1ms–5m, covers process execution and stream draining |
| `analytics.max-processes` | `ANALYTICS_MAX_PROCESSES` | `2`; 1–32; per application instance, immediate rejection when full |
| `analytics.max-stdout-bytes` | `ANALYTICS_MAX_STDOUT_BYTES` | 1048576; 128–67108864; hard byte cap before JSON parsing |
| `analytics.max-stderr-bytes` | `ANALYTICS_MAX_STDERR_BYTES` | 65536; 128–1048576; counted and discarded, never logged |
| `server.port` | `SERVER_PORT` | `8080` |
| `server.address` | `SERVER_ADDRESS` | `127.0.0.1`; local-only binding by default |

Timeout, concurrency, byte bounds and publication syntax are validated at initialization through Bean Validation. Paths are trusted operator configuration; this is not an executable sandbox. Use read-only permissions for the Python code and completed Gold data in deployment. The default HTTP connector also bounds worker threads (32), connections (64), and socket accept backlog (16); there is no adapter request queue.

## HTTP contract

The [M6.4 API contracts](../../docs/architecture/analytics-api-contracts.md) define the complete article, daily and quality schemas, null handling, ordering and metrics, plus compatibility requirements recorded before implementation. Publisher behavior below is unchanged.

```sh
curl --fail-with-body 'http://127.0.0.1:8080/api/v1/publishers'
curl --fail-with-body 'http://127.0.0.1:8080/api/v1/publishers?sourceId=alpha&limit=10'
curl --fail-with-body 'http://127.0.0.1:8080/api/v1/articles?sourceId=alpha&limit=20&offset=0'
curl --fail-with-body 'http://127.0.0.1:8080/api/v1/daily?from=2026-10-12&to=2026-10-13'
curl --fail-with-body 'http://127.0.0.1:8080/api/v1/quality?sourceId=alpha&from=2026-10-13'
curl --fail-with-body 'http://127.0.0.1:8080/actuator/health'
```

`sourceId` is an optional exact, case-sensitive catalog ID matching `[a-z0-9][a-z0-9_-]{0,127}`, consistent with the ingestion source catalog. No trimming, SQL expressions, wildcard matching or paths are accepted. `limit` defaults to 100 and must be a decimal integer from 1 through 1000. Blank, unknown, duplicate or invalid parameters return HTTP 400. A valid ID with no rows returns an empty array.

The response has `publication`, `data`, and `meta` (`returned` and the requested `limit`). Each publisher has:

| Field | JSON representation and meaning |
| --- | --- |
| `sourceId` | Nonblank string; source identity |
| `articleCount` | Decimal string; catalog article count |
| `observationCount` | Decimal string; sum of accepted article observations |
| `firstObservedAt` | UTC timestamp with exactly six fractional digits and `Z`; null preserved |
| `lastObservedAt` | Same timestamp/null representation |
| `articlesWithoutPublishedAt` | Decimal string; articles with null publication time |

All counters are parsed as nonnegative integral signed 64-bit values and serialized directly as decimal strings, including values beyond JavaScript's safe integer range. No floating-point conversion occurs. The verified Gold publisher query normally produces nonnull observation timestamps; the JSON boundary preserves explicit nulls rather than inventing values. Python's source-ascending UTF-8 ordering is preserved and checked, with no duplicate source rows. Empty input returns `data: []`; no synthetic zero groups are invented. A limit bounds output, not verification work. Publisher, daily and quality metadata contains only `returned` and `limit`; there is no global total or pagination token. Articles additionally return `offset` (default 0, maximum 1,000,000) and `hasMore`. Advance by the returned count while `hasMore` is true, preserving publication identity and source filter. No lookahead row is exposed.

Daily and quality accept optional inclusive UTC `from`/`to` dates mapped to the existing Python start/end filters. Distinct counts are local to their daily groups and are not additive. `warningCodesPerObservation` is a finite nonnegative JSON number, potentially greater than 1.0, measuring codes per accepted observation rather than the percentage of records containing warnings. Article warning lists describe only the selected observation. Article URLs and summaries are unchanged untrusted nullable strings, not sanitized HTML or safe navigation targets. No Bronze keys or local input provenance are exposed by the article DTO.

## Errors and liveness

Errors contain only a stable code, for example `{"code":"PUBLICATION_UNAVAILABLE"}`. Subprocess messages, local paths and Python error details are never forwarded.

| HTTP status | Code | Meaning |
| --- | --- | --- |
| 400 | `INVALID_ARGUMENT` | Invalid, duplicate or unsupported HTTP parameter |
| 503 | `PUBLICATION_UNAVAILABLE` | No configured fingerprint, or Python rejects the selected publication |
| 503 | `ANALYTICS_RUNTIME_UNAVAILABLE` | Missing executable/script, inaccessible working directory, or process startup failure |
| 503 | `ANALYTICS_CAPACITY_EXCEEDED` | All process permits are in use; no waiting for a slot |
| 504 | `ANALYTICS_TIMEOUT` | Process/draining deadline exceeded |
| 500 | `ANALYTICS_EXECUTION_FAILED` | Nonzero exit without a recognized publication failure, stream I/O failure, interruption or unexpected execution failure |
| 500 | `ANALYTICS_RESPONSE_INVALID` | Oversized stdout/stderr, invalid JSON/UTF-8, wrong envelope/identity/types/order, or malformed timestamps |

A missing Python dependency may fail after process startup without a structured CLI envelope; this is `ANALYTICS_EXECUTION_FAILED`. The adapter does not infer classifications by inspecting traceback text. Operators should verify the configured environment with `python -m pip check` and the documented standalone CLI.

Only Actuator health is exposed, with no details or components. The framework's automatic liveness/readiness groups are explicitly disabled to avoid implying publication readiness. `GET /actuator/health` checks basic process liveness; it does not scan Gold or assert query readiness. Each analytical request independently verifies readiness by opening and querying the configured publication. No startup publication scan or cached readiness can bypass those checks. Internal execution logs include only fixed report name, generated execution ID, elapsed milliseconds and stable outcome code. They exclude filters, result rows, subprocess streams, environment contents and configured paths.

## Process and integrity boundary

The explicit `ProcessBuilder` list contains the configured executable, `-E -s -B -X utf8`, the configured script, `--gold-root`, `--publication`, `--report` with a fixed application enum, `--limit`, and optional `--source`. Articles always add `--offset`; daily/quality may add `--start-date` and `--end-date`. No report name or additional flag comes from HTTP. There is no shell. The working directory is the script's parent. Python ignores Python environment overrides, disables user-site imports and bytecode writes, and uses UTF-8. The child environment is cleared and receives only controlled `PATH=/usr/bin:/bin`, `TZ=UTC`, and `LANG=C.UTF-8`; macOS may add its own encoding variable. AWS credentials, `PYTHONPATH` and parent environment customizations are not inherited.

A semaphore admits up to the configured process count with `tryAcquire`; excess requests fail immediately. Two owned virtual threads drain stdout/stderr concurrently. Each stream has an independent hard byte budget. Standard input is closed. There is no executor pool or persistent worker. Success requires complete stream draining, exit code zero, exact `OK` envelope fields, matching configured identity/report/requested limit, and fully validated rows. Partial results never survive timeout, overflow, interruption or nonzero exit.

Cleanup forcibly terminates the child and observed descendants, reaps the direct child with a bounded wait, closes streams and interrupts reader threads. Interruption is restored after cleanup. Cleanup can add up to three seconds of bounded joins after the execution deadline under normal OS process/pipe behavior. Descendant enumeration/termination is best effort where permitted by the OS: restrictions or a child that detaches/exits between observations can prevent discovery. The trusted Python consumer does not spawn children; this adapter is not general process-tree containment. No OS-level CPU or RSS limit is claimed; timeout, admission and stream caps bound different resources.

[The existing Python verifier](../../docs/architecture/duckdb-analytics.md#reader-checks-and-limits-of-reconciliation) remains authoritative for publication metadata, canonical manifest/checksums, exact file inventory, Parquet schemas, required values/keys, cross-table reconciliation, and observed publication changes before and after reports. Source filtering uses its parameterized SQL. Java validates only the process/result boundary. No historical metric recomputation, weakened verifier, one-time precheck or unverified JDBC query is introduced.

The inherited immutable single-writer assumptions remain: no transactional snapshot, cryptographic authenticity, protection against edits reverted between checks, or exactly-once guarantee. Repeated full verification and fresh-process startup cost work per query; throughput and capacity have not been benchmarked. No authentication, TLS, public hosting, automatic retry, publication refresh or cache is implemented. Bind locally until deployment controls are designed.

## Automated validation

Ordinary Maven verification needs Java 21 and dependency availability, but no Python, Spark or publication:

```sh
mvn -B -ntp -f services/content-api/pom.xml verify
```

Failure-path tests use deterministic JVM children for success/nonzero exit, both pipes, output overflow, timeout, interruption, capacity and descendant cleanup. Strict parser/controller/service tests cover argument handling, identity/envelope validation, nulls, microseconds, counts above 2^53, errors, liveness and invalid startup configuration. Test-only Mockito uses the subclass mock maker to avoid self-attaching a JVM agent; no production mocking dependency is added.

The integration profile requires the existing M6.1 fixture output. Generate it once when absent (the generator deliberately refuses to overwrite an existing output):

```sh
# Development environment only: Spark/Boto3 are used to build synthetic publications.
export PYSPARK_PYTHON="$(command -v python3.12)"
"$PYSPARK_PYTHON" -m pip install -r processing/requirements-bronze.txt -r processing/requirements-analytics.txt
PYTHONPATH=processing:processing/tests SPARK_LOCAL_IP=127.0.0.1 \
  "$PYSPARK_PYTHON" services/api/tests/generate_fixtures.py --output services/api/target/fixtures
mvn -B -ntp -f services/api/pom.xml verify
mvn -B -ntp -f services/content-api/pom.xml verify -Pintegration \
  -Danalytics.python="$ANALYTICS_PYTHON_EXECUTABLE"
```

Set `ANALYTICS_PYTHON_EXECUTABLE` to a Python environment with the analytics requirements, as in local startup. A custom fixture location can be passed as `-Dgold.fixture=/path/to/generated/fixtures`. The profile fails explicitly if fixtures are missing; it does not silently skip integration. Real loopback HTTP calls run through Spring, the process adapter, and Python/DuckDB. Fixtures contain two publishers, repeated observations, microseconds and missing publication times. The generator stops Spark and removes its synthetic Silver inputs before queries. All 13 damaged copies are rejected, including the known JDBC integrity gaps. CI reuses one fixture generation for JDBC and this API; it retains the ingestion, processing, documentation and hygiene jobs. Dependency bootstrap needs network access; test data and queries need no external services.

## Historical M6.2 local evidence

Executed October 18, 2026 on macOS ARM64 with GraalVM Java 21.0.7, Maven 3.9.16, Python 3.12.15 and DuckDB 1.4.5:

| Check | Result |
| --- | --- |
| Content API Maven verification | 85 unit/component tests and 17 real HTTP/Python integration tests passed; no failures/errors/skips |
| Synthetic HTTP reference comparison | Full (alpha: 2 articles/4 observations; beta: 1/1), earlier and empty publications matched Python; exact source filters, limits and absent-source results passed |
| Synthetic corrupt/missing publications | All 13 damaged copies plus missing fingerprint returned safe HTTP 503; no partial data |
| Existing Python processing suite | 78 passed in 105.468 seconds; expected corruption diagnostics and existing socket ResourceWarnings appeared |
| Existing ingestion / JDBC POC | 29 ingestion tests and 48 JDBC tests passed |
| Existing batch launch regression | All five argument cases passed |
| Documentation and hygiene | `npm run check` and `git diff --check` passed |
| Hosted CI / other OS API packaging | Configured, not executed for this change; pending owner publication and platform validation |

The initial sandboxed regression attempts could not open local fixture sockets or enumerate child processes. Authorized runs with those local permissions passed. Cleanup was hardened so unavailable descendant enumeration never prevents direct-child termination. Initial checks also exposed the macOS-added encoding environment variable and Boot's default health groups; tests and configuration now cover those platform/framework behaviors.

Optional retained-data validation is separate from synthetic tests. The packaged API matched the existing Python reference for fingerprint `195999b03c48ca9a5828f5a67c1e00c312efe6f52e0916c180188f8bb9bdf185`: guardian-world **45 articles / 90 observations**, nasa-technology **10 / 20**, and xkcd **4 / 8**, totaling **59 / 118**. The NASA filter also matched. All ten Gold files retained identical byte lengths, SHA-256 digests and modification times. No completed Gold/Silver publication was modified and no replacement publisher data was fetched. Local ignored evidence is in `outputs/m6-2/retained-validation.json`.

Hosted execution of these changes remains pending owner publication; local tests do not establish hosted CI success or production scalability.

## M6.4 validation and follow-up

M6.4 extends the same adapter with fixed article/daily/quality reports and report-specific DTO validation. `BoundedProcessRunner`, publisher controllers/services/DTOs, Gold v1, the verifier and accepted ADRs are unchanged. Python `--offset` is the only CLI extension; existing calls retain their original envelopes. No dependency versions or runtime requirements changed. Article pages may exceed the configured stdout cap because metadata and CLI provenance can be large; reduce the requested limit or deliberately adjust the operator-owned cap. No partial data is returned after overflow.

Executed October 18, 2026 on the same local Java 21/Python 3.12/DuckDB 1.4.5 toolchain, with Node.js 24.19.0 for frontend regression checks:

| Check | Result |
| --- | --- |
| API unit/component tests | 120 passed, including all existing publisher and process lifecycle tests |
| Real HTTP/Python integration | 20 passed: all reports against full/earlier/empty reference fixtures, source/date filters, multiple article pages, empty/beyond-end pages, 13 corruption cases and a missing publication through every endpoint |
| Synthetic immutability | Fixture file hashes, sizes and modification times identical before/after HTTP tests |
| Existing ingestion / JDBC POC | 29 ingestion tests and 48 JDBC tests passed |
| Existing Python/Spark processing | 81 passed in 115.188 seconds, including 17 analytics tests; expected deliberate-corruption diagnostics and existing PySpark socket ResourceWarnings appeared |
| Batch launch regressions | Five cases passed |
| Frontend compatibility | 72 mocked tests, strict type checking, lint and production build passed; existing bundle-size warning remains |

Documentation/link checks, repository hygiene and `git diff --check` passed. Hosted CI remains pending owner publication. The existing API integration CI step already runs the expanded suite using the same generated fixtures; its label now reflects all analytics reports. All existing jobs remain intact.

Optional retained-data evidence is separate from synthetic CI: the exact retained M2 fingerprint `195999b03c48ca9a5828f5a67c1e00c312efe6f52e0916c180188f8bb9bdf185` returned all **59 identities** in nine pages (eight pages of seven, then three), matching Python order without duplicates or omissions. Daily returned **3 rows / 118 observations**; quality returned **118 observations / 0 warning codes**. Source filtering, beyond-end pagination and publisher regression checks passed. All ten Gold files retained identical hashes, sizes and modification times; the temporary API was stopped. No publisher data was fetched or Gold/Silver regenerated. Ignored local evidence is `outputs/m6-4/retained-validation.json`.

M6.5 remains planned. Its UI work can consume these documented contracts: preserve the fingerprint across pages, retain exact counters, distinguish selected article warnings from historical warning counts, and design safe URL navigation and summary rendering before displaying untrusted article metadata. No article UI or automatic publication switch is included in M6.4.
