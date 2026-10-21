# Content API — verified Gold analytics (M6.2 and M6.4)

This Java 21 module implements `GET /api/v1/publishers`, `/api/v1/articles`, `/api/v1/daily` and `/api/v1/quality` over one explicitly pinned Gold publication. The owner [accepted ADR 0005](../../docs/adr/0005-gold-consumer-integration.md) on October 18, 2026. The [JDBC POC](../api/README.md) remains isolated and is not a runtime dependency. The separate [M6.3 React dashboard](../../frontend/README.md) consumes this publisher endpoint. M6.4 adds the three analytical endpoints and deterministic article pagination. `/api/v1/publication` is not implemented. The M6.5 frontend now consumes all four endpoints; final dashboard browser acceptance and M6 closeout remain pending.

## Acceptance and structure

Acceptance requires reference-equivalent results for all four reports, explicit publication selection, exact integer and UTC microsecond serialization, safe failure responses, bounded subprocess resources, and automated real HTTP/Python integration on synthetic publications. Startup and ordinary Maven unit tests must work without Gold, Spark or Python.

The dependency flow is `Controller -> Service -> GoldAnalyticsAdapter -> BoundedProcessRunner -> gold_analytics.py`. `PublisherController`/`PublisherService` remain the publisher path; `AnalyticsController`/`AnalyticsService` handle the three new endpoints using `ArticleQuery`/`DateQuery` and report-specific typed responses. Controllers validate only their explicitly supported parameters before invoking any process. The service maps typed analytical rows to typed REST records. The adapter chooses a fixed report enum, constructs arguments and validates the CLI envelope. The runner owns admission, process/stream lifecycle and cleanup. The analytics path has no SQL, Parquet reader, JDBC connection, cache, worker service or verifier port. E5 B1 adds a separately selected management persistence foundation below.

Spring Boot **4.1.1** manages dependency and plugin versions; Java is **21**. Original analytics starters are `spring-boot-starter-webmvc`, `spring-boot-starter-actuator`, and `spring-boot-starter-validation`. Tests use `spring-boot-starter-webmvc-test`, which includes the general Boot test starter and MVC test support. Artifact names were checked against the release POMs and Spring's [MVC documentation](https://docs.spring.io/spring-boot/reference/web/index.html), [testing documentation](https://docs.spring.io/spring-boot/reference/testing/), and [managed dependencies](https://docs.spring.io/spring-boot/appendix/dependency-versions/coordinates.html). The resolved build uses Spring MVC 7.0.9, Jackson 3.1.5, Tomcat 11.0.24, Hibernate Validator 9.1.3.Final and JUnit 6.0.3. There are no dependency version overrides; the E5 B1 persistence additions are documented below.

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

M6.5 now consumes these contracts in the separate frontend, preserving the fingerprint across views/pages, exact counters and warning meanings, and rendering article text with HTTP(S)-only explicit navigation. See the [frontend guide](../../frontend/README.md#m65-validation-and-remaining-closeout) for implementation and remaining browser acceptance. No backend capability or automatic publication switch is added by M6.5.

## E5 B1 persistence and runtime roles

[ADR 0010](../../docs/adr/0010-durable-execution-coordination-and-recovery.md) is Accepted on October 20, 2026; only B1 is implemented here. PostgreSQL schema/migrations, startup isolation and coordinator transaction primitives are available. Source/run HTTP operations, dispatch, imports, processing supervision, host writer guards, recovery handoff, cancellation/retry/reconciliation and Operations UI remain B2–F work. An idle worker is not an execution service. Existing analytical endpoints and publication identities are unchanged.

PostgreSQL **17.11**, a maintained 17 patch release, is pinned to the official multi-platform `postgres:17.11-bookworm` image at digest `sha256:3645570cccdfa447589da9f57dd740faa29b30938e861289a5574b6ca6b03826`. Local validation used its Linux ARM64 image. See PostgreSQL's [version policy](https://www.postgresql.org/support/versioning/). Spring Boot 4.1.1 manages Spring JDBC, PostgreSQL JDBC **42.7.13**, Flyway core/PostgreSQL **12.4.0** and HikariCP **7.0.2**; no version overrides, JPA, Testcontainers or second database abstraction are added. Flyway is invoked explicitly, without automatic Boot migration configuration.

### Local database setup

Use Docker Compose, Java 21 and Maven 3.9+. From the repository root, copy `.env.example` to ignored `.env` only if it does not already exist. Preserve existing Bronze settings. Set local `DDIP_DB_USER`, a nonblank private `DDIP_DB_PASSWORD`, `DDIP_DB_NAME`, `DDIP_DB_PORT` and matching `DDIP_DB_URL`. Generate `DDIP_INSTALLATION_ID` once (canonical UUID), retain it privately and reuse it across restarts. Do not regenerate it to bypass an installation mismatch. The URL is restricted to an explicit loopback host/port/database, with no URL query/credentials. PostgreSQL initialization credentials/database apply only to a new volume; changing `.env` does not change an existing database's password or identity.

```sh
# Review/fill .env first. Export it only from a trusted operator-controlled shell file.
set -a
. ./.env
set +a
docker compose --env-file .env -f infrastructure/local/compose.yaml up -d --wait postgres
mvn -B -ntp -f services/content-api/pom.xml verify
DDIP_ROLE=migrate java -jar services/content-api/target/content-api-0.1.0-SNAPSHOT.jar
```

The new PostgreSQL host port is bound to `127.0.0.1`; `pg_isready` checks service readiness. Application readiness additionally checks authentication, server major 17, exact known Flyway versions/checksums, no pending/future migrations, required tables and the singleton installation binding. No role creates a missing database. A repeated migration does not recreate a missing coordinator. If setup is interrupted after DDL commits but before singleton initialization, retain the database and require explicit operator inspection; there is no automatic bootstrap repair. Migration is explicit, with API/worker stopped: schema `ddip_management`, migrations V1–V5, no baseline, clean, repair, out-of-order upgrade or automatic replacement. An unknown commit/startup outcome requires inspection and a repeat of the explicit validated operation; it does not authorize dropping data. Flyway history validation is not a defense against a database administrator altering arbitrary schema objects.

Compose preserves the existing SeaweedFS image, credentials, Bronze volume and default Bronze-only startup. PostgreSQL is in the optional `management` profile; targeting `postgres` explicitly enables that service. Missing database variables do not prevent the existing SeaweedFS-only configuration from resolving. PostgreSQL initialization refuses an empty password on a new volume, and application database roles validate all their own settings. PostgreSQL uses the explicitly named persistent volume `ddip-management-postgres17-data`. Start/stop PostgreSQL independently; do not start ingestion. Use `docker compose --env-file .env -f infrastructure/local/compose.yaml stop postgres` for a non-destructive stop, then the same `up -d --wait postgres` to restart. Never use `down -v`, delete volumes or replace an operational database as a startup workaround. Retain native output roots and restricted journals independently; database persistence is not a cross-resource backup or certified restore guarantee. Manual retention/recovery and comprehensive backup automation remain as specified in ADR 0010.

### Role commands and bounds

Run the packaged JAR from the repository root; shell variables above must be exported. Secrets belong in the ignored operator environment, not command arguments, repository files or logs.

```sh
# Default/explicit legacy analytics: no database, migrations or worker infrastructure.
DDIP_ROLE=analytics java -jar services/content-api/target/content-api-0.1.0-SNAPSHOT.jar
# Database validation plus existing analytics; management mutations are not implemented.
DDIP_ROLE=api java -jar services/content-api/target/content-api-0.1.0-SNAPSHOT.jar
# Non-web, database/runtime-path validation, idle only; Ctrl-C stops it.
DDIP_ROLE=worker java -jar services/content-api/target/content-api-0.1.0-SNAPSHOT.jar
# Non-web, explicit migrations/installation initialization, then exits.
DDIP_ROLE=migrate java -jar services/content-api/target/content-api-0.1.0-SNAPSHOT.jar
```

`DDIP_ROLE` maps to `ddip.role`: exactly `analytics` (default), `api`, `worker`, `migrate`. Unknown/import roles fail; import belongs to B2. Worker requires absolute `DDIP_WORKER_PYTHON`, `DDIP_WORKER_JAVA_HOME`, `DDIP_WORKER_PROCESSING_ROOT`, an executable Python/Java path and the fixed Content/Reliability-v2 publication scripts. B1 checks path availability only; it does not probe child versions, acquire filesystem ownership, register an active worker or claim/launch anything. C must qualify exact runtime/profile/root identity before processing. Worker/migration force a non-web context even if a web type is supplied, excluding analytical controllers, properties and process pools. The worker has one idle lifecycle thread; no polling scheduler is present.

Database roles use one Hikari pool: API maximum 4, worker/migration maximum 2, minimum idle 0. Flyway needs a second connection for its coordination; a one-connection migration pool is insufficient. Connection, SQL statement, lock and explicit transaction waits are bounded to 3 seconds; Flyway lock/connection retries are disabled; idle-in-transaction timeout is 5 seconds. Sessions use UTC. No database transaction encloses external processing. Startup errors use fixed `MANAGEMENT_CONFIGURATION_INVALID`, `MANAGEMENT_UNAVAILABLE` or `MANAGEMENT_INCOMPATIBLE`; conditional conflicts return false or `STATE_CONFLICT`. Native driver/pool/Flyway logging is disabled to avoid connection-detail leakage. These are internal B1 diagnostics, not new management REST envelopes.

`CoordinatorRepository.read()`, `compareAndSetIdleMode(expectedVersion, mode)` and `isCurrentOwner(attemptId, instanceId, epoch)` are the B1 repository boundary. `ManagementTransactions.required(...)` supplies explicit bounded transactions; initialization is called only by migration. The conditional mode update works only on an idle slot with matching installation/version. Ownership reads are snapshots, not launch authorization or proof of filesystem exclusivity. B2/C must add lock-ordered guarded source/run/claim transactions; no takeover method is provided in B1.

### Schema and test commands

The five SQL migrations are in [db/migration](src/main/resources/db/migration): V1 profiles/storage/coordinator/worker instances; V2 sources/revisions/artifacts/dependencies; V3 runs/captured sources/inputs/attempts/stages/artifact links; V4 RSS references/controls/idempotency/audit; V5 immutable evidence/identity/version/terminal-outcome guards. Nineteen operational tables plus Flyway history use bounded fields, exact bytea/digest checks, foreign keys and history/claim indexes. JSONB is used only for lossless row-identity comparison in triggers; canonical bytes are never derived from JSONB. B2 must implement the fixed canonical DTO encoder and semantic configuration validation before accepting requests. DB JSON-object/size/hash checks alone do not implement that encoder.

Deferred composite foreign keys permit atomic initial source/revision insertion and run/attempt/coordinator binding; final committed links cannot point at a different run, instance or epoch. Unique partial indexes enforce one active run/attempt. Evidence and captured selections reject ordinary UPDATE/DELETE/TRUNCATE; mutable rows require exactly incremented versions and immutable identities. Terminal attempt/stage/control outcomes and successful runs cannot be rewritten. Superuser/DDL access remains trusted; these are application integrity guards, not E6 authorization. Cross-row admission limits, full state transitions, safe audit DTO contents, artifact-cycle insert serialization and actual handoff/quiescence protocol are B2–F responsibilities.

Real PostgreSQL integration tests require a **dedicated disposable test service** and credentials allowed to create/drop isolated test databases. They create random `ddip_b1_*` databases, reset only their own schema, and drop only their own database afterward. Never point test variables at the operational installation. CI uses its own service. No H2 substitute or absent-service skip is allowed.

```sh
export DDIP_TEST_DB_URL=jdbc:postgresql://127.0.0.1:15432/ddip_test
export DDIP_TEST_DB_USER=ddip_test
# Export DDIP_TEST_DB_PASSWORD from private test configuration.
export DDIP_TEST_PYTHON="$PWD/outputs/silver-python312/.venv/bin/python"
mvn -B -ntp -f services/content-api/pom.xml verify -Ppostgres
# Independently run existing HTTP/Python compatibility against existing synthetic fixtures.
mvn -B -ntp -f services/content-api/pom.xml verify -Pintegration \
  -Danalytics.python="$PWD/outputs/silver-python312/.venv/bin/python"
mvn -B -ntp -f services/api/pom.xml verify
npm run check
git diff --check
```

Use the two Maven profiles separately. Ordinary `verify` needs no PostgreSQL. The `postgres` profile fails explicitly when test-service settings are absent. Existing CI job names are unchanged; the processing job adds a digest-pinned PostgreSQL service and the real DB tests. Hosted execution for B1 remains pending.
