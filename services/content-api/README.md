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

[ADR 0010](../../docs/adr/0010-durable-execution-coordination-and-recovery.md) is Accepted on October 20, 2026; B1 is implemented here; the subsequently authorized source-only B2a implementation is documented below. PostgreSQL schema/migrations, startup isolation and coordinator transaction primitives are available. Run HTTP operations, dispatch, imports, processing supervision, host writer guards, recovery handoff, cancellation/retry/reconciliation and Operations UI remain B2b–F work. An idle worker is not an execution service. Existing analytical endpoints and publication identities are unchanged.

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

The new PostgreSQL host port is bound to `127.0.0.1`; `pg_isready` checks service readiness. Application readiness additionally checks authentication, server major 17, exact known Flyway versions/checksums, no pending/future migrations, required tables and the singleton installation binding. No role creates a missing database. A repeated migration does not recreate a missing coordinator. If setup is interrupted after DDL commits but before singleton initialization, retain the database and require explicit operator inspection; there is no automatic bootstrap repair. Migration is explicit, with API/worker stopped: schema `ddip_management`, migrations V1–V7 (V1–V5 foundation, V6 sources, V7 ownership), no baseline, clean, repair, out-of-order upgrade or automatic replacement. An unknown commit/startup outcome requires inspection and a repeat of the explicit validated operation; it does not authorize dropping data. Flyway history validation is not a defense against a database administrator altering arbitrary schema objects.

Compose preserves the existing SeaweedFS image, credentials, Bronze volume and default Bronze-only startup. PostgreSQL is in the optional `management` profile; targeting `postgres` explicitly enables that service. Missing database variables do not prevent the existing SeaweedFS-only configuration from resolving. PostgreSQL initialization refuses an empty password on a new volume, and application database roles validate all their own settings. PostgreSQL uses the explicitly named persistent volume `ddip-management-postgres17-data`. Start/stop PostgreSQL independently; do not start ingestion. Use `docker compose --env-file .env -f infrastructure/local/compose.yaml stop postgres` for a non-destructive stop, then the same `up -d --wait postgres` to restart. Never use `down -v`, delete volumes or replace an operational database as a startup workaround. Retain native output roots and restricted journals independently; database persistence is not a cross-resource backup or certified restore guarantee. Manual retention/recovery and comprehensive backup automation remain as specified in ADR 0010.

### Role commands and bounds

Run the packaged JAR from the repository root; shell variables above must be exported. Secrets belong in the ignored operator environment, not command arguments, repository files or logs.

```sh
# Default/explicit legacy analytics: no database, migrations or worker infrastructure.
DDIP_ROLE=analytics java -jar services/content-api/target/content-api-0.1.0-SNAPSHOT.jar
# Database validation, existing analytics and B2a source management; no execution.
DDIP_ROLE=api java -jar services/content-api/target/content-api-0.1.0-SNAPSHOT.jar
# Non-web C1 passive readiness; requires the private C1 bindings below; Ctrl-C stops it.
DDIP_ROLE=worker java -jar services/content-api/target/content-api-0.1.0-SNAPSHOT.jar
# Non-web, explicit migrations/installation initialization, then exits.
DDIP_ROLE=migrate java -jar services/content-api/target/content-api-0.1.0-SNAPSHOT.jar
```

`DDIP_ROLE` maps to `ddip.role`: exactly `analytics` (default), `api`, `worker`, `migrate`, `operator-import`. Unknown roles fail. The Spark-free import role is described in [the current operator contract](#spark-free-native-input-imports-b2b-1b-a). Worker requires absolute `DDIP_WORKER_PYTHON`, `DDIP_WORKER_JAVA_HOME`, `DDIP_WORKER_PROCESSING_ROOT`, an executable Python/Java path and the fixed Content/Reliability-v2 publication scripts. B1 originally checked path availability only. [C1 passive worker readiness](#c1-passive-worker-and-durable-claim-foundation) now also requires an approved execution-profile ID, deployment digest and the existing installation CONTROL binding. It acquires that guard, validates private roots and registers a worker, with one bounded heartbeat task and no production claim/dispatch loop. Worker/migration force a non-web context even if a web type is supplied, excluding analytical controllers and query process pools. C2 must qualify executable/native versions before any execution capability is enabled.

Database roles use one Hikari pool: API maximum 4, worker/migration maximum 2, minimum idle 0. Flyway needs a second connection for its coordination; a one-connection migration pool is insufficient. Connection, SQL statement, lock and explicit transaction waits are bounded to 3 seconds; Flyway lock/connection retries are disabled; idle-in-transaction timeout is 5 seconds. Sessions use UTC. No database transaction encloses external processing. Startup errors use fixed `MANAGEMENT_CONFIGURATION_INVALID`, `MANAGEMENT_UNAVAILABLE` or `MANAGEMENT_INCOMPATIBLE`; conditional conflicts return false or `STATE_CONFLICT`. Native driver/pool/Flyway logging is disabled to avoid connection-detail leakage. These are internal B1 diagnostics, not new management REST envelopes.

`CoordinatorRepository.read()`, `compareAndSetIdleMode(expectedVersion, mode)` and `isCurrentOwner(attemptId, instanceId, epoch)` are the B1 repository boundary. `ManagementTransactions.required(...)` supplies explicit bounded transactions; initialization is called only by migration. The conditional mode update works only on an idle slot with matching installation/version. Ownership reads are snapshots, not launch authorization or proof of filesystem exclusivity. B2a adds lock-ordered source transactions; B2b/C must add guarded run/claim transactions; no takeover method is provided in B1.

### Schema and test commands

The original five B1 SQL migrations are in [db/migration](src/main/resources/db/migration): V1 profiles/storage/coordinator/worker instances; V2 sources/revisions/artifacts/dependencies; V3 runs/captured sources/inputs/attempts/stages/artifact links; V4 RSS references/controls/idempotency/audit; V5 immutable evidence/identity/version/terminal-outcome guards. Nineteen operational tables plus Flyway history use bounded fields, exact bytea/digest checks, foreign keys and history/claim indexes. JSONB is used only for lossless row-identity comparison in triggers; canonical bytes are never derived from JSONB. B2a now implements the fixed source canonical encoder and source semantic validation; run DTOs remain B2b work. DB JSON-object/size/hash checks alone do not implement that encoder.

Deferred composite foreign keys permit atomic initial source/revision insertion and run/attempt/coordinator binding; final committed links cannot point at a different run, instance or epoch. Unique partial indexes enforce one active run/attempt. Evidence and captured selections reject ordinary UPDATE/DELETE/TRUNCATE; mutable rows require exactly incremented versions and immutable identities. Terminal attempt/stage/control outcomes and successful runs cannot be rewritten. Superuser/DDL access remains trusted; these are application integrity guards, not E6 authorization. B2a adds source admission, revision/gate transitions and fixed source audit details. Run admission/state transitions, artifact-cycle insert serialization and actual handoff/quiescence protocol remain B2b–F responsibilities.

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

## E5 B2a source management

The owner authorized B2a only after committing B1. The [normative B2a contract](../../docs/architecture/durable-execution.md#stage-b2a-source-management-contract-and-evidence) defines exact fields, replay, pagination, safe failures and local security. No run/artifact/import/worker/UI workflow is implemented. `DDIP_ROLE=api` exposes source controls; analytics/worker/migrate roles do not instantiate these controllers, allowlists or request filter. Existing `/api/v1/` analytical behavior is unchanged.

Run the existing **explicit migration** command with API/worker stopped before starting this version. V6 adds immutable source creation sequence, durable public command-result snapshots and private endpoint identity/digest evidence to existing tables; V1–V5 are unchanged. An upgrade cannot bootstrap a missing old coordinator. Do not repair/reset an operational schema to bypass a failure.

### Operator reference setup

Source registration requires reviewed references, not client-supplied paths/URLs. Configure an absolute ignored `DDIP_MANAGEMENT_CATALOG` file (<=64 KiB), and existing B1 operator-provisioned immutable storage bindings with matching UUID, installation, product and BRONZE/BATCH role. Root/ownership/native-storage qualification is not performed by source CRUD and remains required before later processing. This checkpoint adds no storage-provisioning/import HTTP route. Do not use the tests' placeholder bindings for an operational installation.

Example catalog below is fictional configuration syntax, not proof of a real registered profile or storage root. Replace IDs/digest/URI with independently reviewed installation references. Native Reliability profile hashes must come from the existing validated synthetic profile and canonical bytes, not from a filename or label.

```json
{
  "catalogVersion": 1,
  "endpoints": [
    {
      "endpointId": "20000000-0000-4000-8000-000000000001",
      "sourceId": "demo-feed",
      "uri": "https://feeds.example.org/one"
    }
  ],
  "reliabilityProfiles": [
    {
      "sourceId": "demo-requests",
      "profile": "request-jsonl-v2",
      "dataOrigin": "synthetic",
      "registeredProfileSha256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    }
  ],
  "storageProfiles": [
    {"profileId": "30000000-0000-4000-8000-000000000001", "productId": "content-intelligence", "role": "BRONZE"},
    {"profileId": "30000000-0000-4000-8000-000000000002", "productId": "application-reliability", "role": "BATCH"}
  ]
}
```

Keep catalog files private under an ignored operator root such as local-data/e5. Startup reads the catalog without fetching feeds or resolving DNS. Changing a used endpoint's URI requires a **new endpoint UUID** and source revision; V6's retained private digest prevents new configurations from rebinding an old endpoint ID. Historical source inspection and exact command replay remain possible after removing a catalog entry. An empty/blank catalog approves no new references. Native profile, raw evidence and filesystem verification remain independent prerequisites for B2b/C.

```sh
# Export from private operator configuration. The catalog path must be absolute.
export DDIP_MANAGEMENT_CATALOG="$PWD/local-data/e5/source-catalog.json"
export DDIP_MANAGEMENT_ORIGINS='http://127.0.0.1:8080,http://127.0.0.1:5173,http://127.0.0.1:4173'
# Generate once and retain privately if page cursors must survive application restart.
export DDIP_MANAGEMENT_CURSOR_KEY="$(python3 -c 'import base64,secrets; print(base64.urlsafe_b64encode(secrets.token_bytes(32)).decode().rstrip("="))')"
DDIP_ROLE=api java -jar services/content-api/target/content-api-0.1.0-SNAPSHOT.jar
```

The signing key is not authentication. With it unset, a process-local random key invalidates old cursors on restart; source and command state remain durable. API binds to loopback, accepts only configured local Host/Origin values and rejects preflights. Mutations require JSON, allowed Origin and `X-DDIP-Local-Request: 1`. For Vite, use its existing same-origin `/api/` proxy with preserved Host, and explicitly list dev/preview origins. No permissive CORS or React Operations is added. Other local processes can imitate these headers; E6 authentication/authorization remains deferred.

### Source commands and responses

After provisioning the fictional example's references with real reviewed values:

```sh
curl --fail-with-body 'http://127.0.0.1:8080/api/management/v1/capabilities'
curl --fail-with-body -X POST 'http://127.0.0.1:8080/api/management/v1/sources' \
  -H 'Origin: http://127.0.0.1:8080' -H 'X-DDIP-Local-Request: 1' \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: demo-create-001' \
  --data '{"productId":"content-intelligence","sourceId":"demo-feed","displayName":"Reviewed feed","enabled":true,"config":{"configVersion":1,"profile":"rss-atom-v1","dataOrigin":"publisher","feedEndpointId":"20000000-0000-4000-8000-000000000001","registeredProfileSha256":null,"inputStorageProfileId":"30000000-0000-4000-8000-000000000001"}}'
curl --fail-with-body 'http://127.0.0.1:8080/api/management/v1/sources/content-intelligence/demo-feed'
curl --fail-with-body -X PUT 'http://127.0.0.1:8080/api/management/v1/sources/content-intelligence/demo-feed/enabled' \
  -H 'Origin: http://127.0.0.1:8080' -H 'X-DDIP-Local-Request: 1' \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: demo-disable-001' \
  -H 'If-Match: "0"' --data '{"enabled":false}'
curl --fail-with-body 'http://127.0.0.1:8080/api/management/v1/sources?productId=content-intelligence&limit=20'
curl --fail-with-body 'http://127.0.0.1:8080/api/management/v1/sources/content-intelligence/demo-feed/revisions?limit=20'
```

Use the exact GET/command ETag for a new conditional operation. Revision POST uses `{displayName,config}` at the revisions path, with the same mutation headers/If-Match. Initial registration creates revision 1/version "0". Changed configuration/name appends; identical current configuration/name is a no-op. Reverting appends another revision. Enablement changes only live gate/version/time, never old revisions. No identity deletion, rename or profile-family switch exists.

Every command needs a durable Idempotency-Key (1–128 ASCII alphanumeric/underscore/hyphen). Same operation/source/key and normalized body/If-Match return the original committed status/data/ETag, including after later changes or a lost response. Different content conflicts. Object-key order and JSON escape spelling do not change identity. New stale requests return 412; missing If-Match returns 428. A replay returns historical data, so GET the source for its current state. Correlation IDs identify each HTTP request independently.

Responses are `{apiVersion:1,data,meta:{correlationId}}`; lists add limit, hasMore, nextCursor. Source detail includes only public six-field config, its hash, identity, display name, current revision, enabled flag, decimal-string rowVersion and UTC microsecond creation/update times. Lists omit config; revision history includes old safe configurations. Failures have exactly apiVersion, code, message, correlationId, retryable, with fixed messages. No resolved URI, credential, private storage settings, SQL exception or arbitrary request data is returned.

Source pagination is product/source ascending, default 20/max 100 with one-row lookahead and creation high-water. Revision pagination is ascending with revision high-water. Follow signed nextCursor with the same endpoint/filters; no offset/arbitrary sort exists. Gate-filter membership can change between pages; no cross-request database snapshot is promised. Invalid/tampered/expired cursors fail 400. Limits are 64 KiB request, 1 MiB encoded response, 128 sources and 1,000 revisions/source; failed admission does not leave bindings/audit or partial source rows.

### B2a validation

The existing service-backed CI Maven profile includes SourceManagementIT as well as PersistenceIT, without changing CI job names or adding services/dependencies. Run with the existing dedicated PostgreSQL service/test settings:

```sh
mvn -B -ntp -f services/content-api/pom.xml test
mvn -B -ntp -f services/content-api/pom.xml verify -Ppostgres
mvn -B -ntp -f services/content-api/pom.xml verify -Pintegration \
  -Danalytics.python="$PWD/outputs/silver-python312/.venv/bin/python"
npm run check
git diff --check
git diff --cached --check
```

The `postgres` command requires DDIP_TEST_DB_URL/USER/PASSWORD and, in CI, DDIP_TEST_PYTHON as documented for B1. Tests create/drop only isolated random test databases. No live feed, production data, Spark execution or cloud service is required. Current executed counts/evidence are in the supporting B2a contract; B1 figures above remain historical. Hosted CI awaits owner publication. B2b must add run DTOs/capture/imports; C–F must add ownership/supervision/adapters/recovery/UI. None is implemented by registering an enabled source.

## E5 B2b-1 import review checkpoint

Historical checkpoint, preceding the B2b-1A ownership implementation below: B2b-1 verified artifact imports were authorized on October 21, 2026 with an explicit stop for an incompatible ownership model. Inspection reached that stop: [ADR 0011](../../docs/adr/0011-artifact-import-maintenance-ownership.md), now accepted for B2b-1A, describes the required durable maintenance owner and quiescence-checked abandoned-import closure. The current coordinator represents only run-attempt ownership; STOPPING plus a host lock does not bind or recover an import operation. No importer, host guard, native verification wrapper or V7 migration was added. Existing analytics/API/migrate commands and the idle worker retain their B1/B2a behavior.

Historical B2b-1 inspection (superseded by [B2b-1B-A](#spark-free-native-input-imports-b2b-1b-a)): there was **no runnable import invocation or approved import catalog/profile format yet**. At that checkpoint, `DDIP_ROLE=operator-import` failed with the fixed MANAGEMENT_CONFIGURATION_INVALID diagnostic. Do not use a fake run/attempt, change coordinator mode manually or clear ownership to make an import possible. Do not insert unverified artifacts directly into PostgreSQL. No import HTTP endpoint exists.

After owner approval, B2b-1 must provide the fixed non-web role in the existing packaged JAR, exact invocation, strict private catalog/runtime/storage configuration, identity-checked persistent installation guard and read-only native qualification. Only v2 Reliability JSONL/Parquet-profile batches, publisher/service snapshots and explicitly selected completed Silver history are in scope. Spark is needed only for bounded read-only Silver qualification; batches/references have Spark-free readers. The importer must use the existing artifact/dependency tables, compare immutable native/evidence identities, reverify every import and atomically store its result/audit. Historical registry qualification never guarantees current native readability.

The [checkpoint contract and local validation](../../docs/architecture/durable-execution.md#stage-b2b-1-inspection-and-owner-review-checkpoint) describe the inspected interfaces and remaining test matrix. Existing synthetic reader/regression commands validate the baseline; they do not execute an import:

```sh
export JAVA_HOME="$(/usr/libexec/java_home -v 21)"
export PATH="$JAVA_HOME/bin:$PATH"
export PYSPARK_PYTHON="$PWD/outputs/silver-python312/.venv/bin/python"
export SPARK_LOCAL_IP=127.0.0.1 TZ=UTC PYTHONPATH=processing
mvn -o -B -ntp -f services/content-api/pom.xml test
"$PYSPARK_PYTHON" -m unittest discover -s processing/tests -p test_reliability_format_inputs.py -v
"$PYSPARK_PYTHON" -m unittest discover -s processing/tests -p test_publisher_reference.py -v
"$PYSPARK_PYTHON" -m unittest discover -s processing/tests -p test_reliability_inputs.py -v
"$PYSPARK_PYTHON" -m unittest discover -s processing/tests -p test_silver_inputs.py -v
# Dedicated PostgreSQL test-service variables are required as documented above.
mvn -o -B -ntp -f services/content-api/pom.xml verify -Ppostgres
mvn -o -B -ntp -f services/content-api/pom.xml verify -Pintegration \
  -Danalytics.python="$PYSPARK_PYTHON"
npm run check
git diff --check
git diff --cached --check
```

The JDK selector above is macOS-specific; on other hosts set JAVA_HOME to the installed Java 21 runtime. Use an already provisioned Python 3.12.15/PySpark 4.0.2/DuckDB 1.4.5 environment and the existing synthetic API fixtures. No packages are installed by these checks. B2b-2 still needs completed import ownership/verification/persistence, opaque registered selections, exact source/profile capture and use-time revalidation. Run submission, processing/publication, worker execution and E5 completion remain pending separate authorization.

## E5 B2b-1A ownership foundation

Historical B2b-1A scope and evidence, before the separately authorized [B2b-1B-A implementation](#spark-free-native-input-imports-b2b-1b-a): accepted ADR 0011 authorized the internal maintenance ownership foundation only. V7 and the internal InstallationHostGuard/ImportOwnershipRepository are implemented. There is no working operator-import role, import endpoint or native qualification/registration workflow. The worker remains idle and analytics-only mode still needs no PostgreSQL. B2b-1B requires separate owner authorization.

Run the existing explicit migrate command with API/worker stopped before starting this version. V7 preserves existing coordinator identity/mode/epoch/version and V1–V6 checksums. It adds artifact_import_operations, coordinator.active_import_id, an import audit link, exclusion/state/identity guards and scoped artifact uniqueness. Runtime requires V7; it never applies migrations automatically.

V7 refuses duplicate existing native scopes or existing Reliability rows with an unresolved legacy format. Fixed SQL diagnostics are ARTIFACT_NATIVE_IDENTITY_CONFLICT / ARTIFACT_NATIVE_FORMAT_UNRESOLVED; application startup reports the existing safe MANAGEMENT_INCOMPATIBLE class. All migration changes roll back. Preserve the database and obtain owner review for these cases; there is no automatic backfill/deduplication. Do not disable evidence guards or delete rows to force an upgrade. Native format is now explicit JSONL/PARQUET for Reliability kinds and null otherwise; identity uniqueness also includes product, kind, contract version and processor revision.

The internal guard requires separately provisioned, operator-reviewed local POSIX files: a canonical private control directory with mode 0700, an existing empty host.guard file and installation.id file with mode 0600, all owned by the current OS user. installation.id is the retained installation UUID followed by one newline. Binding.descriptor() canonically records bindingVersion=1, bindingId, installationId, root, and rootDevice/rootInode, guardDevice/guardInode, installationDevice/installationInode as decimal strings. Store those exact bytes/hash in an immutable CONTROL storage binding linked to the selected immutable execution profile. Provisioning/profiles remain trusted private setup, not HTTP input; this checkpoint supplies no provisioning CLI. Do not copy test bindings into an operational installation or replace a guard after a crash. Device/inode/root changes fail closed and require reviewed binding reconciliation.

For future integration, acquire the guard before reserve; qualify only after reserve commits; finish only after definitive safe cleanup; resolve uncertain commits using lookup with the same operation ID and exact canonical request. Request/result evidence is bounded to 64 KiB and must pass the shared strict canonical encoder. It is internal metadata, never raw native payload or an HTTP import contract. Coordinator mode is unchanged: active_import_id owns maintenance, and STOPPING remains shutdown state. Current coordinator idle-mode mutations and DB active-attempt/run admission reject occupied import ownership. All future writer implementations must also hold this same guard and compare the coordinator pointer/epoch before claim.

Closing a process/guard/connection or observing an old heartbeat never releases the reservation. Recovery needs actual old-owner/descendant/unmanaged-writer quiescence, explicit operator confirmation and checks that observable matching owner processes are stopped. The internal handoff seals the old operation INTERRUPTED and atomically binds a new ACTIVE RECOVERY owner at epoch+1, with no idle interval. Inspect the exact successor after an uncertain response; do not repeat blindly with another ID. Finalize the recovery owner separately after a definitive safe result. Neither handoff nor closure launches native verification or processing. No operator-facing closure command is provided in B2b-1A.

Run the bounded coordination/regression checks from the repository root, using Java 21 and the dedicated PostgreSQL test-service variables documented above:

```sh
mvn -o -B -ntp -f services/content-api/pom.xml test
mvn -o -B -ntp -f services/content-api/pom.xml verify -Ppostgres -Dit.test=ImportOwnershipIT
mvn -o -B -ntp -f services/content-api/pom.xml verify -Ppostgres
mvn -o -B -ntp -f services/content-api/pom.xml verify -Pintegration \
  -Danalytics.python="$PWD/outputs/silver-python312/.venv/bin/python"
npm run check
git diff --check
git diff --cached --check
```

These tests create/drop only random dedicated test databases and private temporary guard fixtures. Child JVM fixtures exercise competing locks, abrupt owner death, observable live-owner refusal and repeated recovery. A test connection wrapper loses acknowledgment only after a real PostgreSQL commit for reservation/result/handoff, then verifies exact lookup without duplicate mutations. V6 upgrade tests preserve timestamps/bytes/checksums and occupied recovery ownership; conflicts leave no partial V7 schema. No fixture qualifies or registers a native artifact through an importer. Existing CI's postgres profile includes the new suite; no new service/dependency is needed.

The [current coordination evidence](../../docs/architecture/durable-execution.md#stage-b2b-1a-durable-import-ownership) records actual results and limitations. B2b-1B must add the strict private catalog/runtime resolver, complete native qualification/evidence retention, verifier containment/cleanup and artifact/dependency writes inside the finalization transaction. Historical SUCCEEDED/verified_at metadata cannot replace fresh native verification. Native imports, run submission/dispatch, publication, UI and complete E5 acceptance remain unexecuted here.

## Spark-free native input imports (B2b-1B-A)

The owner authorized this checkpoint separately from B2b-1A. `operator-import` now provides a fixed, non-web, local operator command for existing completed inputs. It instantiates neither analytics controllers/query pools nor the idle worker lifecycle. No import HTTP route exists. That checkpoint excluded Silver, which is now separately supported by the [B2b-1B-B extension](#verified-silver-imports-b2b-1b-b). Derived publications remain excluded. No processing run, source intake, batch registration or publication is performed by this command.

### Private installation setup

Use the existing migrated V7 installation and its private [host guard](#e5-b2b-1a-ownership-foundation). Stop cooperative writers and resolve any occupied execution/recovery/import ownership first. Do not clear coordinator fields manually. Provision reviewed immutable `execution_profiles`, `storage_bindings` and `execution_profile_storage` through the installation's existing trusted operator database setup; the importer creates none of them. A binding must belong to this installation and selected product, have BATCH or REFERENCE role, and be explicitly associated with the import execution profile. B2a Reliability source revisions must already contain the exact native registered-profile digest and BATCH binding UUID with the matching profile family. Historical disabled sources/revisions can be imported; enablement is a later submission policy.

The CONTROL binding uses the existing `InstallationHostGuard.Binding.descriptor()` unchanged. Its private canonical root and pre-existing `host.guard`/`installation.id` must have the ownership, mode and device/inode identities required by B2b-1A. Native roots must be existing canonical absolute local directories. Store this exact canonical JSON as each native storage binding's `configuration_bytes`, with its SHA-256 in `configuration_sha256` (device and inode are **decimal strings**, from `InstallationHostGuard.identity(root)`):

```json
{"importStorageVersion":1,"root":"/private/approved/batches","rootDevice":"123","rootInode":"456"}
```

Runtime profile configuration has the following exact fields. Paths are installation-owned configuration, never catalog fields. Provision canonical bytes and their digest; module hashes must match the reviewed checkout. Root inode identity changes or module changes require explicit operator review and new immutable configuration, not automatic repair.

```json
{
  "importProtocolVersion": 1,
  "python": "/private/approved/venv/bin/python",
  "processingRoot": "/private/approved/checkout/processing",
  "pythonVersion": "3.12.15",
  "duckdbVersion": "1.4.5",
  "timeoutSeconds": 120,
  "modules": {
    "artifact_import_verifier.py": "<reviewed SHA-256>",
    "publication_metadata.py": "<reviewed SHA-256>",
    "reliability_format_inputs.py": "<reviewed SHA-256>",
    "reliability_format_metadata.py": "<reviewed SHA-256>",
    "reliability_format_parquet.py": "<reviewed SHA-256>",
    "reliability_inputs.py": "<reviewed SHA-256>",
    "reliability_metadata.py": "<reviewed SHA-256>",
    "publisher_reference.py": "<reviewed SHA-256>",
    "service_reference.py": "<reviewed SHA-256>"
  }
}
```

`timeoutSeconds` is 1–120 per verifier/recheck invocation. Use the pinned analytics Python dependencies already in [processing requirements](../../processing/requirements-analytics.txt); Spark is neither imported nor started. Install dependencies/build the Java application **before** invoking the import role. The command never executes Maven, pip, npm, a shell or packaging. The runtime executable and installed dependencies remain trusted operator resources; module hashes and runtime version probes do not attest a hostile runtime installation.

### Catalog and invocation

Create an ignored, private UTF-8 JSON file (regular file, canonical absolute address, no symlink, owned by the current operator, no group/other permissions). Set mode `0600`. Catalog bytes are at most 64 KiB, depth 16, with 1–16 explicit selections; unknown/duplicate fields, floating-point values, duplicate native keys and duplicate locations fail. Sixteen is this checkpoint's smaller operational limit within ADR 0011's 128-selection ceiling. The normalized canonical selection order is deterministic, independent of supplied array ordering.

Example syntax uses fictional identities/digests; it is not a usable approved binding:

```json
{
  "catalogVersion": 1,
  "operationId": "40000000-0000-4000-8000-000000000001",
  "executionProfileId": "native-import-v1",
  "controlBindingId": "installation-control",
  "artifacts": [
    {
      "kind": "RELIABILITY_BATCH_V2",
      "productId": "application-reliability",
      "storageBindingId": "30000000-0000-4000-8000-000000000002",
      "relativeAddress": "v2/format=jsonl/source_id=demo-requests/batch_id=b00",
      "contractVersion": 2,
      "processorRevision": 2,
      "identity": {
        "sourceId": "demo-requests",
        "sourceRevision": 1,
        "batchId": "b00",
        "inputFormat": "JSONL",
        "registeredProfileSha256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
      },
      "fingerprint": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    }
  ]
}
```

Supported selections are exact native namespaces:

| Kind | Product; versions | Identity and relative address | Authoritative reader |
| --- | --- | --- | --- |
| RELIABILITY_BATCH_V2 | application-reliability; contract/registrar 2/2 | sourceId, sourceRevision, batchId, inputFormat JSONL/PARQUET, registeredProfileSha256; `v2/format=jsonl` or `parquet` `/source_id=…/batch_id=…` | `reliability_format_inputs.open_batch` |
| PUBLISHER_REFERENCE | content-intelligence; contract/validator 1/1 | referenceId, revision; `reference_id=…/revision=…` | `publisher_reference.open_snapshot` |
| SERVICE_REFERENCE | application-reliability; contract/validator 1/1 | referenceId, revision; `reference_id=…/revision=…` | `service_reference.open_snapshot` |

All entries require `fingerprint`, `storageBindingId`, `relativeAddress`, `kind`, `productId`, `contractVersion`, `processorRevision`, `identity` exactly. References have only `referenceId` and `revision` inside identity. Parquet means Parquet-v1 input profile under Reliability product contract v2. Legacy v1 registrations are never reinterpreted. Native manifests supply fingerprints; an E5 evidence digest never replaces one. There is no latest selection, glob, scan or arbitrary absolute artifact selection.

From the repository root, with Java 21 and the same private database/installation variables used by migration:

```sh
# Build outside any import operation.
mvn -o -B -ntp -f services/content-api/pom.xml -DskipTests package
export DDIP_ROLE=operator-import
export DDIP_IMPORT_CATALOG=/private/approved/import-catalog.json
# DDIP_DB_URL, DDIP_DB_USER, DDIP_DB_PASSWORD, DDIP_INSTALLATION_ID: private installation configuration.
java -jar services/content-api/target/content-api-0.1.0-SNAPSHOT.jar \
  --spring.main.banner-mode=off --logging.level.root=OFF
```

One bounded JSON summary identifies the operation and opaque artifacts/evidence digests; it contains no source payload, manifests, private root paths, URLs or native exception text. Exit 0 means VERIFIED, definitive RECOVERY_CLOSED, or an explicitly labeled HISTORICAL_RESULT; exit 1 means a known safe rejection/NOT_FOUND; exit 2 means unresolved ownership requiring lookup/inspection. A historical result can describe a prior failure/interruption; inspect its `state` and nested `result`. Startup/schema failures occur before command execution and use existing fixed database diagnostics/nonzero exit.

### Verification, evidence and idempotency

After strict catalog validation and host-guard acquisition, V7 reservation commits the operation request/profile/host binding, epoch and exclusive owner before native verification. All native verification runs outside database transactions. The supervisor invokes only the approved Python executable with `-I -B -X utf8` and the fixed `artifact_import_verifier.py`; the environment is cleared to fixed PATH/TZ/LANG. Stdin/stdout are bounded to 512 KiB, stderr to 64 KiB and discarded. Streams drain concurrently, launch/result identities and exit codes must agree, and protocol schema/evidence inventories are checked. Duration and cleanup are bounded. Observed descendants or uncertain enumeration/termination require explicit recovery even if the direct child exits.

The adapter captures exact completion bytes, all selected namespace file lengths/SHA-256 hashes, native inventory and retained dependency descriptors, directory inventories and filesystem identities/timestamps. It invokes the mandatory public native reader, captures again, and performs a separate final recheck. Missing/incomplete namespaces, unexpected files, unsupported versions/provenance, mismatched identity/profile/fingerprint, invalid schemas/counts, symlinks/hard-linked files, corruption and observed changes fail closed. No original file, marker, schema or fingerprint is written. Unavailable, invalid, changed, unsupported and resource-limit classifications remain distinct; allowlisted native failure codes are retained in failed results.

A final short coordinator-first transaction checks the exact operation/process/epoch/coordinator/operation versions and selection identity. It inserts/reuses immutable artifacts, stores the result and result-digest audit, then releases this owner atomically. Any conflict or SQL/conditional failure rolls back all registry changes. These four leaf inputs have no independently registered artifact dependencies: registered raw files and reference CSV/vocabulary identities are exact descriptors in artifact evidence. `artifact_dependencies` remains available for real operational-artifact relationships; no synthetic UUIDs or batch-to-service edges are invented.

A **new operation ID** always fully requalifies the selection. Identical evidence returns existing artifact IDs; different evidence under the same scoped native key or incompatible identity at a registered binding/address conflicts. Evidence includes filesystem identities and stamps, so an in-place rewrite or replacement can conflict even if logical values/bytes are restored. The native identity key excludes fingerprint and source revision: version/product/kind/format are explicit V7 scopes. Qualification timestamp is database metadata and does not change immutable evidence identity on repeat verification. Registry `verified_at` records initial qualification, not permanent readability. Future run submission must revalidate native evidence and retained dependencies.

Reusing an **existing operation ID with the exact normalized request** performs historical reconciliation only, never relaunches verification. It returns HISTORICAL_RESULT with `revalidated:false`, or RECOVERY_REQUIRED for ACTIVE. A changed request under that ID is rejected. This prevents lost responses from causing a second blind operation.

### Interruption and explicit import recovery

Known safe failure after confirmed child cleanup atomically records FAILED/result/audit and releases ownership. Database outage, unknown commit, stale token, unexpected failure, uncertain child cleanup or abrupt importer termination leaves durable ownership blocked unless the definitive commit already succeeded. Closing the file lock, parent exit, heartbeat expiry or lost connection never clears it. Use the original catalog and stable operation ID for lookup:

```sh
DDIP_IMPORT_ACTION=inspect java -jar services/content-api/target/content-api-0.1.0-SNAPSHOT.jar \
  --spring.main.banner-mode=off --logging.level.root=OFF
```

Only after actual installation-level old-owner/descendant/unmanaged-writer quiescence is established, use an explicit new recovery ID and confirmation:

```sh
DDIP_IMPORT_ACTION=recover \
DDIP_IMPORT_RECOVERY_ID=40000000-0000-4000-8000-000000000002 \
DDIP_IMPORT_QUIESCENCE_CONFIRMED=true \
java -jar services/content-api/target/content-api-0.1.0-SNAPSHOT.jar \
  --spring.main.banner-mode=off --logging.level.root=OFF
```

Recovery acquires the same host guard, refuses an observable surviving old process, atomically transfers occupied ownership to a fenced RECOVERY successor, then separately closes that recovery safely. It never qualifies/registers/repairs native artifacts. Confirmation is a trusted operator assertion after actual inspection, not automated proof of orphan cessation. An interrupted recovery remains occupied; set `DDIP_IMPORT_TARGET_ID` to its operation UUID and supply a new recovery UUID. Reusing the same recovery UUID looks up its exact historical result; uncertainty keeps blocking. Processing-run RECONCILE controls are not involved.

### Synthetic reproduction and remaining work

Run focused native tests and the PostgreSQL profile against a dedicated disposable test service with create/drop-database credentials (never an operational database):

```sh
export PYTHONPATH=processing:processing/tests
export DDIP_TEST_PYTHON="$PWD/outputs/silver-python312/.venv/bin/python"
"$DDIP_TEST_PYTHON" -B -m unittest processing/tests/test_artifact_import_verifier.py
# Fixtures use actual native registrars/reference publishers solely to prepare tiny test inputs.
# Existing test DB variables: DDIP_TEST_DB_URL, DDIP_TEST_DB_USER, DDIP_TEST_DB_PASSWORD.
mvn -o -B -ntp -f services/content-api/pom.xml verify -Ppostgres
mvn -o -B -ntp -f services/content-api/pom.xml verify -Pintegration \
  -Danalytics.python="$DDIP_TEST_PYTHON"
npm run check
```

[Fixture preparation](../../processing/tests/artifact_import_fixtures.py) creates four small synthetic native inputs; [ImportWorkflowIT](src/test/java/platform/contentapi/importing/ImportWorkflowIT.java) provisions isolated private bindings/sources and invokes real readers and the non-web role. It drops only its randomly named test database. Actual executed counts and interruption/restart evidence are recorded in the [implementation contract](../../docs/architecture/durable-execution.md#b2b-1b-a-local-validation).

Historical follow-up from B2b-1B-A, now implemented separately below: B2b-1B-B required the owner-reviewed read-only Spark qualification boundary for Silver, retained Bronze checks, complete accepted/rejected Parquet schema/provenance/count verification, stable before/after inventories and a clearly distinguished E5 inventory identity. B2b-2 must select opaque registry IDs and freshly verify retained dependencies. Neither checkpoint was implemented/authorized by that Spark-free change. Worker execution and full E5 remain incomplete. Checksums/cooperative locks establish consistency on trusted local storage, not authenticity against a hostile filesystem administrator, distributed fencing, backup/restore certification or proof that every orphan is stopped. Hosted CI and other-host behavior remain pending observation.

## Verified Silver imports (B2b-1B-B)

The owner separately authorized the Silver extension after the Spark-free checkpoint. The same non-web `operator-import` role now supports one additional fixed kind, `SILVER`, for explicitly selected Content Intelligence retained history. No transformation, source fetch, publication, HTTP import route or processing run is introduced. The Spark-free kinds and their protocol remain supported.

### Exact Silver selection and private bindings

Catalog version remains 1; each Silver entry has the following exact fields and **no native `fingerprint` field**. This fictional example illustrates syntax only:

```json
{
  "catalogVersion": 1,
  "operationId": "40000000-0000-4000-8000-000000000001",
  "executionProfileId": "silver-import-v1",
  "controlBindingId": "installation-control",
  "artifacts": [
    {
      "kind": "SILVER",
      "productId": "content-intelligence",
      "contractVersion": 1,
      "processorRevision": 1,
      "storageBindingId": "approved-silver",
      "relativeAddress": "v1/25bf8e1a2393f1108d37029b3df5593236c755742ec93465bbafa9b290bddcf6/source_id=ZGVtbw/ingestion_date=2026-10-12/ingestion_id=50000000-0000-4000-8000-000000000001/complete.json",
      "identity": {
        "sourceId": "demo",
        "ingestionId": "50000000-0000-4000-8000-000000000001",
        "bronzeStorageBindingId": "approved-bronze",
        "bronzeBucket": "local",
        "bronzeMetadataKey": "bronze/rss/source_id=ZGVtbw/ingestion_date=2026-10-12/ingestion_id=50000000-0000-4000-8000-000000000001/metadata.json",
        "bronzePayloadKey": "bronze/rss/source_id=ZGVtbw/ingestion_date=2026-10-12/ingestion_id=50000000-0000-4000-8000-000000000001/payload.xml"
      }
    }
  ]
}
```

Source encoding is native unpadded Base64URL of the exact UTF-8 source ID. The UUID is canonical, date is valid ISO UTC partition date, bucket hash is SHA-256 of the exact bucket name, and payload/metadata/marker paths must agree exactly. The native marker, not a current source revision/feed URL, establishes the original retained ingestion. Sources are nonblank scalar strings bounded to 512 UTF-8 bytes with no controls; supported approved bucket names match `[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]`. Marker addresses are bounded to 1024 bytes. Traversal, aliases, symlinks and unapproved bindings fail. There is no source scan or newest-file selection.

Provision immutable, installation/profile-associated Content Intelligence SILVER and TEMP bindings using the local-root descriptor from the Spark-free guide. SILVER denotes the root above `v1/`; TEMP denotes a private canonical `0700` scratch root, disjoint from the Silver and installation CONTROL roots. Java/Python validate root device/inode identities. Native roots are never chmodded, repaired or cleaned. Scratch has at least 1 GiB free before Spark starts. Per-invocation private scratch directories are retained for operator inspection; Spark performs its ordinary runtime cleanup inside them. Manual retention remains the operator's responsibility.

The BRONZE binding has this exact private configuration:

```json
{"importBronzeVersion":1,"endpoint":"http://127.0.0.1:8333","region":"us-east-1","bucket":"local","credentialsFile":"/private/approved/bronze-import-credentials.json"}
```

The credential file is canonical, regular, owned by the operator, without symlinks or group/other permissions; use `0600`. It contains exactly `{"accessKey":"<explicit local key>","secretKey":"<explicit local secret>"}` (at most 8 KiB). Credentials are read from this reviewed private location, passed only in bounded child stdin and never stored in artifact evidence/results. The existing `bronze_to_silver.local_client` still forces explicit HTTP loopback endpoints, explicit credentials/region, path-style addressing, disabled proxies/redirect escape and bounded GETs. No ambient AWS credentials, S3A, LIST/PUT/DELETE, bucket initialization or live publisher request is used. Any failed/unauthorized GET is unavailable evidence, never proof of object absence.

Use an explicitly reviewed **new immutable runtime profile**, with `importProtocolVersion:2`. Preserve the existing Python/processing paths, exact Python 3.12.15/DuckDB 1.4.5 fields and module map, adding:

```json
{
  "spark": {
    "javaHome": "/private/approved/jdk21",
    "javaVersion": "21",
    "pysparkVersion": "4.0.2",
    "scratchBindingId": "approved-silver-temp"
  }
}
```

The map must hash all nine original modules **and** `silver_import_qualification.py`, `silver.py`, `silver_inputs.py`, `bronze_to_silver.py`. Use existing pinned [Bronze requirements](../../processing/requirements-bronze.txt) and [analytics requirements](../../processing/requirements-analytics.txt), installed before invocation. `timeoutSeconds` is 1–900 for Silver qualification; final rechecks stay bounded to at most 120 seconds. Version-1 profiles keep their original fields and 120-second ceiling. Updating the adapter/module bytes requires a reviewed new profile/digests; no immutable database profile is rewritten automatically.

### Verification extent and operational identity

The fixed protocol version 2 invokes existing `silver_inputs.load_inputs(spark, root, markers=[exact_marker])`, its per-file/schema/row/count/replay checks and `verify_unchanged()`. Additional checks independently reopen the exact original metadata and payload using `bronze_to_silver.metadata/read_object/local_client`, verify original raw metadata digest, payload digest/length and exact receipt/marker/key identity, and compare every accepted/rejected row's source URI, original timestamp (native Python/Spark microsecond representation), payload length and RSS/Atom format with Bronze. The existing native rules also check source/ingestion IDs, bucket/keys/input URI, payload hash, supported warning/rejection arrays, article identity, required values and accepted/rejected conflicts.

Native schemas declare nullable fields; requiredness is enforced through native row predicates. Spark infers nullable Parquet fields even when native producers write required literal columns. Qualification checks the native logical schema and required nonnull values/array elements, preserving that native compatibility; it does not invent a new universal physical OPTIONAL/REQUIRED footer rule. Empty datasets have schema/count/marker/Bronze evidence but no row provenance. These checks **do not reconstruct article semantics from Bronze XML** or prove that a self-consistent artifact was never changed before import. A missing original empty partition cannot be inferred from the original count-only marker; observed disappearance after capture is rejected.

The managed capture hashes the original marker, every accepted/rejected Parquet file and native-ignored hidden bookkeeping, including bounded hidden directories. Hidden bookkeeping retains the native `.`/`_` policy; it is captured/rechecked, not treated as analytical data or assigned new CRC semantics. Visible unsupported files/nested datasets, glob metacharacters in analytical filenames, symlinks, hard-linked files, missing datasets, malformed evidence and observed substitution/membership changes fail. Admission is at most 128 analytical files per dataset, 512 captured entries, hidden nesting depth 6, 1 GiB total captured bytes per selected ingestion, 64 KiB marker and 16 MiB per bookkeeping file. Original native contracts are unchanged; these are explicit E5 resource limits.

The distinct identity is **`E5_SILVER_INVENTORY_V1 = SHA256(canonical_bytes(value))`**, where `value` contains exactly:

- `qualificationPolicy: "ddip-e5-silver-import"`, `qualificationRevision: 1`, `productId: "content-intelligence"`, `contractVersion: 1`, `processorRevision: 1`, exact `sourceId` and `ingestionId`.
- `nativeCompletion: {relativeAddress, byteLength, sha256}` over original marker bytes.
- `files`: every captured non-marker file, sorted by UTF-8 relative path, each `{path,dataset,byteLength,sha256}`; dataset is accepted, rejected or bookkeeping.
- `bronze: {bucket,metadataKey,metadataByteLength,metadataSha256,payloadKey,payloadByteLength,payloadSha256}` over retained exact objects.

Canonical encoding is the existing E5 integer-only, sorted-key, compact UTF-8 JSON encoding. Operation IDs, database IDs, qualification times, absolute paths and filesystem identity/stamps do not enter this digest. Stamps remain separate immutable capture evidence for mutation/reimport checks. Java independently verifies this fixed representation/digest before registration. The digest is never labeled a native Silver/Gold/Content publication fingerprint.

Two literal compatibility vectors are in [SilverInventoryVectorTest](../../processing/tests/test_silver_import_qualification.py). These serialize metadata, not fake qualified Parquet. Base vector: source `demo`, UUID `50000000-0000-4000-8000-000000000001`, date 2026-10-12, bucket `local`, paths as above; marker length 2/hash `44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a`; accepted/a.parquet length 1/hash 64 `a` characters; rejected/r.parquet length 2/hash 64 `b` characters; Bronze metadata length 2/hash 64 `c` characters and payload length 3/hash 64 `d` characters. Its expected digest is `e7d22d80f101997d6d9e010f09dd3d13b92630688c6f6ccf8dfa9fd61123f411`. Adding accepted/_SUCCESS as bookkeeping, length 0/hash `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`, yields `4f17688a978d6e5ad6dee7463797a7a3d99696e80e751a22a858de05a92f8f32`. Ordering or machine/stamp changes do not change these values; bytes/lengths/inventory/Bronze evidence changes do.

### Supervision, registration and recovery

Invocation is unchanged: select `DDIP_ROLE=operator-import` and the private `DDIP_IMPORT_CATALOG`, then run the built Content API jar. Silver VERIFY alone starts fixed `local[2]` Spark 4.0.2 with approved Java 21, 512-MiB driver, 16-MiB result limit, UTC, disabled UI/event log and private scratch working/temp/warehouse locations. No native root is a writable runtime location. There is no persistent Spark service or parallel qualifier pool. Protocol stdout remains at most 512 KiB, Spark stderr is counted/discarded at 16 MiB, input is bounded to 512 KiB, and process cleanup waits are bounded. Non-Spark imports retain their existing budgets.

The adapter stops Spark, shuts down Py4J, closes its gateway stdin and waits for JVM exit before reporting confirmed shutdown. Java tracks/rechecks observed descendants with a count ceiling. A timeout, overflow, interruption, malformed/ambiguous Silver result, surviving descendant or unavailable cleanup evidence retains blocked ownership; process exit alone is insufficient. Trusted fixed runtime plus explicit shutdown and observed quiescence qualify normal completion, not arbitrary descendants/hostile runtime attestation. Spark-free RECHECK recaptures Silver and reopens Bronze without launching a JVM; it must agree exactly before registry commit.

V7 and the existing reservation/finalization transaction are unchanged. SILVER's native namespace key is canonical `{bucketSha256,metadataKeySha256}` (hashes of exact native bucket/key UTF-8), keeping the full bounded approved key evidence out of the 512-character relational identity field; full identities remain in immutable qualification evidence. This namespace-key encoding is separate from the operational inventory digest. Same native namespace/incompatible location/evidence conflicts; fresh identical operations requalify and reuse one artifact ID. Bronze is an exact native retained-dependency descriptor, not a fabricated operational artifact/edge. Artifact/result/audit commit atomically under exact owner/process/epoch/versions. Historical same-operation lookup is not current-readability proof. Existing explicit recovery, lost-ack lookup and no automatic takeover remain unchanged.

### Silver reproduction and remaining boundary

Use synthetic fixtures only for tests, with Java 21 selected and the pinned environment:

```sh
export JAVA_HOME="$(/usr/libexec/java_home -v 21)"
export PATH="$JAVA_HOME/bin:$PATH"
export PYSPARK_PYTHON="$PWD/outputs/silver-python312/.venv/bin/python"
export DDIP_TEST_PYTHON="$PYSPARK_PYTHON"
export SPARK_LOCAL_IP=127.0.0.1 TZ=UTC PYTHONPATH=processing:processing/tests
# Gate A, before DB integration:
"$PYSPARK_PYTHON" -B -m unittest processing/tests/test_silver_import_qualification.py \
  processing/tests/test_artifact_import_verifier.py
mvn -o -B -ntp -f services/content-api/pom.xml -Dtest=ImportBoundaryTest test
# Gate B and foundation regressions; private dedicated test PostgreSQL credentials as above:
mvn -o -B -ntp -f services/content-api/pom.xml verify -Ppostgres
npm run check
```

The [E5 evidence record](../../docs/architecture/durable-execution.md#b2b-1b-b-local-validation) records actual counts, regressions, crash/restart probes and limitations. Tests use actual public native readers and PostgreSQL; Bronze integration uses a read-only loopback S3 HTTP fixture with the actual boto3 client, not a claim of newly observed SeaweedFS/AWS compatibility. Native Silver/Bronze publishers/readers remain unchanged.

B2b-2 still needs opaque-ID submission, frozen selections, aggregate admission and independent fresh verification at submission/worker consumption. Historical `verified_at` is insufficient. No run submission, worker execution or full E5 completion is included. Hosted CI, other-host process behavior, hostile storage/runtime authenticity, exact XML-to-row semantic reconstruction and comprehensive backup/power-loss guarantees remain unverified.

## Durable queued run submission (B2b-2A)

B2b-2A accepts durable selections for CONTENT_RSS_V1 and RELIABILITY_V2. Every new run is QUEUED. No worker claim, attempt, feed retrieval, native verifier, Spark/Python child or publication is launched by submission. The queue cannot drain in this checkpoint. A registered input's historical qualification is checked as database metadata; its files and retained Bronze dependencies must be independently revalidated by the future worker before consumption.

### Reviewed submission profiles

Use the existing V7 API installation, [source catalog](#operator-reference-setup) and completed operator imports. Provision a new, immutable UUID-named execution profile through trusted private database setup, with exact canonical UTF-8 configuration bytes and their SHA-256. Import-verifier profiles are incompatible with submission. A version-1 admission profile has exactly:

```json
{"runProfileVersion":1,"pipeline":"CONTENT_RSS_V1","pipelineVersion":1,"sourceProfile":"rss-atom-v1","runtime":{"javaVersion":"21","pythonVersion":"3.12.15","pysparkVersion":"4.0.2","deploymentSha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}}
```

The digest above is fictional. Use the reviewed deployment's real immutable release digest. RELIABILITY_V2 profiles use sourceProfile=request-jsonl-v2 or request-parquet-v1, one format per profile. Unknown fields, other versions and import-only profiles fail. The configuration digest and immutable profile ID identify its revision; profiles are never edited in place. This format is an admission descriptor, **not a runnable command specification**. Worker implementation must independently resolve and match the pinned deployment and supply its approved fixed adapters, commands, limits and runtime checks before execution can be authorized. No executable or arbitrary Spark settings are accepted over HTTP.

Associate the profile with reviewed `execution_profile_storage` rows belonging to this installation. Required roles are BRONZE/SILVER/REFERENCE/GOLD/CONTENT/CONTROL/STAGING/TEMP for Content and BATCH/REFERENCE/RELIABILITY/CONTROL/STAGING/TEMP for Reliability. Bindings must belong to the selected product; the shared CONTROL binding may belong to the other product. Each selected source input, artifact and Silver Bronze dependency binding must be an authorized member with its correct role. At most 128 bindings are captured. Submission checks canonical database descriptors and installation membership, without opening roots, checking free disk space or contacting S3. Root/runtime readiness belongs to the future worker.

Source revisions must be current and enabled for new admission. RSS endpoint evidence is reconstructed from the reviewed in-memory catalog and must match the immutable digest captured by B2a. The exact descriptor is copied into the run, so later catalog changes do not rewrite accepted work. Reliability profiles remain synthetic-only and must match the source revision, batch format, registered-profile digest and storage binding. Every selected source must have a selected batch. Historical Silver source provenance need not be one of the selected fetch sources.

### Requests and safe inspection

Start the packaged API with the existing private DDIP_DB_URL/USER/PASSWORD/INSTALLATION_ID settings and B2a catalog/cursor configuration; the [role setup](#role-commands-and-bounds) is unchanged:

```sh
java -jar services/content-api/target/content-api-0.1.0-SNAPSHOT.jar \
  --ddip.role=api --server.address=127.0.0.1 --server.port=8080 \
  --ddip.management.catalog="$DDIP_MANAGEMENT_CATALOG" \
  --ddip.management.cursor-key="$DDIP_MANAGEMENT_CURSOR_KEY"
```

POST `/api/management/v1/runs` requires application/json, an approved Host/Origin, X-DDIP-Local-Request: 1 and an ASCII Idempotency-Key of 1–128 letters/digits/underscore/hyphen. No query parameters or If-Match are accepted. Bodies are bounded to 64 KiB. These local protections do not authenticate a person; requester scope remains local-operator.

Exact Content shape (fictional previously registered IDs):

```json
{"pipeline":"CONTENT_RSS_V1","executionProfileId":"10000000-0000-4000-8000-000000000001","fetchSources":[{"sourceId":"demo-feed","revision":1}],"retainedSilverArtifactIds":[],"publisherReferenceArtifactId":"20000000-0000-4000-8000-000000000001"}
```

Exact Reliability shape:

```json
{"pipeline":"RELIABILITY_V2","executionProfileId":"10000000-0000-4000-8000-000000000002","sources":[{"sourceId":"demo-requests","revision":1}],"batchArtifactIds":["30000000-0000-4000-8000-000000000001"],"serviceReferenceArtifactId":"20000000-0000-4000-8000-000000000002"}
```

Content requires 1–8 unique fetch sources and 0–128 unique qualified Silver artifacts; Reliability requires 1–32 unique sources and 1–32 unique v2 batches of one format. Each reference must have the right kind/product/version. Revision integers range from 1 to 1,000. UUIDs are canonical lowercase. Unknown, missing, duplicate or wrongly typed fields/IDs fail; clients cannot submit paths, endpoints, configuration, raw data or fingerprints. Metadata also enforces existing aggregate limits: Silver analytical bytes <=1 GiB; Reliability <=128 raw files, <=1 GiB raw bytes and <=2M occurrences. This does not substitute for worker-side native limits/rechecks.

```sh
curl --fail-with-body -i http://127.0.0.1:8080/api/management/v1/runs \
  -H 'Origin: http://127.0.0.1:8080' -H 'X-DDIP-Local-Request: 1' \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: reviewed-selection-001' \
  --data-binary @private/run-selection.json
```

HTTP 202 is returned only after commit, with relative Location `/api/management/v1/runs/{runId}`, ETag and no-store. Single envelopes are exactly `{apiVersion:1,data,meta:{correlationId}}`. Minimal run data contains runId, productId, pipeline, state, currentStage, rowVersion, requestedAt, finishedAt, sourceRevisions (`{sourceId,revision}`), inputArtifactIds, outcomeCode and retryEligibility. Reliability adds dataOrigin=synthetic. Row versions are decimal strings; timestamps are UTC with six fractional digits. Nullable persisted fields are null. Input IDs include the reference and are ordered by role/ordinal; the DTO exposes no private evidence, canonical configuration, locations or native fingerprints.

GET the returned Location to inspect that persisted selection without Python/Spark. It is operational history, not a current-readability certificate. Richer lists, attempts/stages/control hints and result browsing remain B2b-2B. No run control or HTTP import route is implemented; B2a capabilities remain the source-catalog response, not a complete run-profile/artifact discovery API.

Errors retain `{apiVersion,code,message,correlationId,retryable}` with fixed messages: INVALID_ARGUMENT/400, LOCAL_BOUNDARY_REJECTED/403, NOT_FOUND/404, IDEMPOTENCY_CONFLICT or STATE_CONFLICT or SOURCE_DISABLED/409, MANAGEMENT_CAPACITY_EXCEEDED/429, and MANAGEMENT_UNAVAILABLE or COORDINATOR_RECOVERY_REQUIRED/503. No SQL/private exception text is returned. Response encoding is bounded to 1 MiB. Unknown commit acknowledgment returns 503; repeat the **same key and full request**, never infer failure or submit another key automatically.

### Frozen evidence, admission and retry of a request

One short transaction locks coordinator, resolves scoped idempotency first, checks readiness/capacity, locks sources in sorted order, validates metadata and inserts run/source/input/audit rows atomically. The existing runs uniqueness on `(product_id,principal_scope,idempotency_key)` is the binding; there is no second queue or command-binding copy. Identical replay returns the existing run's current persisted state even after source edits, disablement, a full queue or a coordinator stop/import/recovery. Changed normalized content conflicts. A source-update race either captures the exact locked revision or rejects the stale request. A failed transaction leaves no accepted run, links or audit. Submission never changes either coordinator owner pointer or creates an attempt.

Source config objects/digests, original reviewed endpoint evidence, observed enabled/gate version, execution profile configuration/digest, approved storage descriptors, exact registered artifact identity/qualification bytes/digests and qualification time are frozen in canonical captured_selection and run_inputs. Native dependency descriptors remain inside qualification evidence; current imported kinds are native leaves without invented registry edges. Source FKs and artifact FKs retain linkage; V5 immutability guards protect snapshots independently of HTTP. The 16 MiB aggregate evidence limit is enforced before insertion. Request/selection SHA-256 values are operational digests, never publication fingerprints; Silver retains its separately labeled E5_SILVER_INVENTORY_V1 identity.

Operational request normalization reuses B2a's UTF-8 canonical encoder: exact fields, sorted object keys, integer-only values, sorted ASCII source IDs/revisions and lexical UUID sets. Duplicates are rejected before sorting. Native dependency serializers retain their own order/formula later. Literal independent Python-stdlib vectors for [RunCommandsTest](src/test/java/platform/contentapi/management/RunCommandsTest.java), with source demo/revision 1, profile `10000000-0000-4000-8000-000000000001`, reference `20000000-0000-4000-8000-000000000001`, empty Content history or Reliability batch `30000000-0000-4000-8000-000000000001`:

- Content: `ccb5b21efeca0723add5093f11a1d88bfa7a7eb412cda9d03d4f87326b294de8`.
- Reliability: `d7bcaa53a98d8a24872119379c64b60417982309e3fca8977207480b03e0a1b2`.

At most 20 unfinished runs globally (QUEUED/RUNNING/CANCEL_REQUESTED/RECOVERY_REQUIRED) and 10,000 retained runs are admitted. A healthy active execution slot can coexist with up to 19 queued runs. STOPPING/RECOVERY_ONLY, any import owner, recovery work, inconsistent execution ownership or stale supervision block new admission; none frees ownership. Historical replay bypasses these new-work checks. Queued rows do not grant native write permission.

Audit admission conservatively reserves 4,096 future events per unfinished run and three per active import before accepting new metadata work within the existing 1M-event limit. Source commands and new import/recovery reservations honor this reserve. Existing import finalization uses its reserved capacity; capacity rejection never clears an uncertain owner. Future lifecycle/control implementation must stay within the reserve and continue checking history bounds. No purge or automatic queue draining is supplied.

### Reproduction and remaining boundary

Use a dedicated PostgreSQL 17 test service and the existing DDIP_TEST_DB_URL/USER/PASSWORD variables. Submission fixtures are tiny synthetic database qualification descriptors, not mock persistence or fresh native qualifications. They intentionally need no native files or Python/Spark. The full postgres regression also runs the existing import suites with their separate synthetic native fixtures and DDIP_TEST_PYTHON prerequisite.

```sh
mvn -B -ntp -f services/content-api/pom.xml test \
  -Dtest=RunCommandsTest,AuditCapacityTest
mvn -B -ntp -f services/content-api/pom.xml verify -Ppostgres \
  -Dtest=RunCommandsTest,AuditCapacityTest -Dit.test=RunSubmissionIT
mvn -B -ntp -f services/content-api/pom.xml verify -Ppostgres
mvn -B -ntp -f services/content-api/pom.xml verify -Pintegration \
  -Danalytics.python="$PWD/outputs/silver-python312/.venv/bin/python"
npm run check
git diff --check
git diff --cached --check
```

For an actual dedicated database restart, pass `-Dddip.test.submission-restart-signal=/private/tmp/ddip-run-restart`. The test writes READY, waits at most 45 seconds, then requires a changed PostgreSQL postmaster start time and exact surviving IDs, sequence, selection bytes and input evidence. An external local harness must restart **only the dedicated test container**, wait for readiness and write `/private/tmp/ddip-run-restart.done`; it must not restart an operational installation. Omit the property for ordinary CI; that tests fresh connections/application restart without claiming a server restart.

Current executed counts and failure-injection evidence are recorded in the [B2b-2A contract](../../docs/architecture/durable-execution.md#stage-b2b-2a-durable-run-submission). Hosted CI remains pending owner publication. B2b-2B requires safe bounded run-history/attempt/event inspection and approved public DTO expansion. Worker claiming, source gate checks at dispatch, deployment/runtime readiness and native dependency revalidation remain later authorized work. No accepted submission has been executed; E5 remains incomplete.

## Bounded run history and inspection (B2b-2B)

The API role now supplies read-only history over the existing PostgreSQL records. These GETs do not fetch feeds, open native artifacts, requalify inputs, acquire ownership, create attempts or append audit events. Existing submission remains available; newly accepted runs still stay QUEUED. Worker dispatch and run controls remain unavailable.

### Routes and filters

| Route | Exact query parameters | Ordering and data |
| --- | --- | --- |
| GET `/api/management/v1/runs` | productId, sourceId, state, limit, cursor; all optional | Descending immutable submission sequence; public run summaries |
| GET `/api/management/v1/runs/{id}` | None | Persisted public run detail and captured source/input IDs |
| GET `/api/management/v1/runs/{id}/attempts` | limit, cursor | Ascending attempt_no; actual attempts and stage counts |
| GET `/api/management/v1/runs/{id}/events` | limit, cursor | Ascending event_seq; actual run-linked audit events |

Product is content-intelligence or application-reliability. sourceId uses the existing lowercase ASCII source syntax, at most 128 characters. State is exactly QUEUED, RUNNING, CANCEL_REQUESTED, SUCCEEDED, FAILED, CANCELLED or RECOVERY_REQUIRED. Filters combine with AND. Source filtering uses immutable run_sources membership, including its product association, without reading current source settings or guessing from retained artifact provenance. EXISTS prevents source/input join fanout from duplicating runs.

Pages default to 20, permit 1–100 and use one-row lookahead. Unknown, duplicate, empty or wrongly typed query parameters fail; there is no OFFSET or arbitrary sort. IDs are canonical UUIDs. A valid absent run returns NOT_FOUND for detail and either subresource; a present unclaimed run returns an empty attempts page.

```sh
curl --fail-with-body 'http://127.0.0.1:8080/api/management/v1/runs?productId=content-intelligence&state=QUEUED&limit=20'
curl --fail-with-body "http://127.0.0.1:8080/api/management/v1/runs/$RUN_ID"
curl --fail-with-body "http://127.0.0.1:8080/api/management/v1/runs/$RUN_ID/attempts?limit=20"
curl --fail-with-body "http://127.0.0.1:8080/api/management/v1/runs/$RUN_ID/events?limit=20"
```

GETs require an approved Host and enforce the existing Origin policy when Origin is supplied. They do not require Origin, Idempotency-Key or X-DDIP-Local-Request. Mutation protections remain unchanged. The API is trusted-loopback-only; these controls and opaque IDs are not authentication or authorization. No permissive CORS, polling service, SSE or WebSocket was added.

### Public DTOs

Single envelopes retain exactly `{apiVersion:1,data,meta:{correlationId}}`. Page meta adds limit (JSON integer), hasMore (boolean), nextCursor (string or null). Responses remain application/json and Cache-Control: no-store; detail has an opaque row-version ETag. Encoding is capped at 1 MiB before writing, including on serialization failure. No partial successful response is returned.

Run summaries contain runId, productId, pipeline, state, currentStage, rowVersion, requestedAt, finishedAt, outcomeCode, retryEligibility and result. Reliability adds dataOrigin=synthetic. Detail adds sourceRevisions (`{sourceId,revision}`), inputArtifactIds, attempts (`{count,currentAttemptId}`), latestControl and counts. Captured source revisions/input IDs come from run_sources/run_inputs, never current configuration. Detail reads at most 32 source IDs and 129 input IDs, rejecting oversized persisted selections safely. Counts, sequence IDs and row versions are canonical nonnegative decimal strings; bounded attempt numbers/revisions remain integers. All timestamps use UTC with exactly six fractional digits.

Example QUEUED detail data (fictional IDs/time):

```json
{
  "runId": "70000000-0000-4000-8000-000000000001",
  "productId": "content-intelligence",
  "pipeline": "CONTENT_RSS_V1",
  "state": "QUEUED",
  "currentStage": null,
  "rowVersion": "0",
  "requestedAt": "2026-10-21T00:00:00.123456Z",
  "finishedAt": null,
  "sourceRevisions": [{"sourceId": "demo-feed", "revision": 1}],
  "inputArtifactIds": ["20000000-0000-4000-8000-000000000001"],
  "attempts": {"count": "0", "currentAttemptId": null},
  "latestControl": null,
  "result": null,
  "counts": null,
  "outcomeCode": null,
  "retryEligibility": false
}
```

An attempt has exactly attemptId, attemptNo, kind, state, startedAt, finishedAt, outcomeCode and stageCount. Kind is EXECUTE/RECONCILE; state is CLAIMED/RUNNING/SUCCEEDED/FAILED/CANCELLED/INTERRUPTED. stageCount counts persisted stage_invocations for that attempt, not planned stages or verified-stage claims. No stage commands, PID/process evidence, output addresses or raw logs are exposed. Detailed stage arrays/routes are not included in this checkpoint.

An event has exactly eventSeq, timestamp, action, beforeState, afterState, beforeVersion, afterVersion, code and context. Context has nullable attemptId/controlId; supplied links must belong to the requested run. Only explicitly run-linked audit rows are read, in immutable sequence order. General source/import audit rows and heartbeat updates are not merged into a run's lifecycle. Private details_bytes are never fetched. Actions/states/outcome codes use the fixed [inspection classifications](src/main/java/platform/contentapi/management/RunInspectionValues.java); unrecognized stored classifications fail with fixed INTERNAL_FAILURE rather than exposing arbitrary text. Checkpoint C must coordinate any additional public classifications before writing them.

latestControl is null or exactly `{controlId,type,state,reason,requestedAt,appliedAt,rowVersion}` from the latest persisted control, ordered by requested time then UUID. It reports history only: no control POST endpoint or actionable control hints are supplied. retryEligibility is the stored eligibility observation, not authorization to invoke a future retry.

counts is null when no counters were recorded. A present map accepts only accepted, rejected, articles, occurrences, events, windows, rows, files and bytes, each a canonical nonnegative signed-64-bit decimal string. Unsupported fields/types fail safely; counters are not reconstructed from input manifests or defaulted to zero. These are operational counter labels, not a change to native product schemas or count limits.

result is null or exactly `{artifactId,nativeFingerprint,productContractVersion,processorRevision,verificationStatus,verifiedAt}`. A public successful result requires the proper Content v1/revision-1 or Reliability v2/revision-2 artifact, its original 64-hex native publication identity and a VERIFIED VERIFY_RESULT stage/artifact link belonging to the run. A database success flag or artifact row alone is insufficient. The API reads that existing native identity; it does not hash evidence into a new native fingerprint. Silver inventory identities, references and intermediate Gold artifacts cannot be presented as final publications. verificationStatus is exactly VERIFIED_AT_EXECUTION: historical persisted verification, **not current readability, authenticity or activation**. Full private evidence is never loaded to render these summaries; future workers must persist the approved native identity and verification linkage correctly.

### Cursor, query and restart policy

Run pages use B2a's existing SourceCursor HMAC-SHA256 signer and `ddip.management.cursor-key`; no second key, signing mechanism, pagination table or session is introduced. RunPageCursor validates only the run-specific payload. Tokens have version 1, endpoint kind, a canonical scope digest, immutable high-water sequence, last-returned position and fixed 24-hour expiry. Scope binds endpoint, filters, ordering and (for attempts/events) run ID. The scope digest avoids putting long source filters or private data in tokens. Tokens are signed, not encrypted; clients should treat them as opaque.

Use an unchanged private configured signing key for API restart continuity. Changing/loss of that key, tampering, unsupported versions, wrong endpoint/run/filters, invalid bounds, expiry or tokens over 512 encoded bytes return INVALID_ARGUMENT. The original expiry is preserved across pages. Page size may change within 1–100 without changing selection scope. Existing B2a source cursors retain their previous lifetime policy. An omitted key remains process-local as before; its cursors do not survive restart.

New sequence identities above the traversal's high-water are excluded. A static dataset traverses without duplicates/skips. **States are mutable and pagination is not snapshot isolation:** a run that leaves a state filter may disappear from later pages, while a run entering that filter after its position was passed may not appear in that traversal. States/results/counts can reflect later commits. Start a new traversal to refresh membership; no long-lived PostgreSQL snapshot is retained.

History reads use short transactions with no coordinator/owner row lock. Run pages issue two SQL statements (high-water and bounded projection); detail uses three bounded projections; attempt/event pages use an existence check, indexed high-water and bounded page query. Aggregates/result/control summaries are SQL projections, without per-row JDBC queries. Existing submission-sequence uniqueness, state/sequence, source-membership, attempt, stage, control and run/event indexes suffice; no migration was added. Private request/selection/artifact evidence and audit-detail blobs are not selected for history pages.

GET errors are fixed INVALID_ARGUMENT/400, LOCAL_BOUNDARY_REJECTED/403, NOT_FOUND/404, MANAGEMENT_UNAVAILABLE/503, or INTERNAL_FAILURE/500 for invalid persisted public metadata/response overflow. SQL, private diagnostics, roots, secrets and native payloads are absent from both success and failure responses. Database outage is not an empty successful page.

### Validation and B2 readiness

Focused reproduction uses the existing dedicated PostgreSQL variables and pinned host JDK 21; no Python or Spark is needed for RunHistoryIT:

```sh
mvn -B -ntp -f services/content-api/pom.xml test -Dtest=RunInspectionTest
mvn -B -ntp -f services/content-api/pom.xml verify -Ppostgres \
  -Dtest=RunInspectionTest -Dit.test=RunHistoryIT,RunSubmissionIT
mvn -B -ntp -f services/content-api/pom.xml verify -Ppostgres \
  -Dit.test=PersistenceIT,SourceManagementIT,ImportOwnershipIT,ImportWorkflowIT,RunSubmissionIT,RunHistoryIT
mvn -B -ntp -f services/content-api/pom.xml verify -Pintegration \
  -Danalytics.python="$PWD/outputs/silver-python312/.venv/bin/python"
npm run check
git diff --check
git diff --cached --check
```

The regression command includes the existing Spark-free import tests and their DDIP_TEST_PYTHON prerequisite. The existing 23 Spark-backed SilverImportIT cases are deliberately not selected for this read-only checkpoint; their previous results remain historical. The default postgres CI profile includes the new history suite alongside existing suites; no new dependency/service/job was added.

For an actual dedicated DB restart test, pass `-Dddip.test.history-restart-signal=/private/tmp/ddip-history-restart`. An external harness waits for READY, restarts only the dedicated test container, waits for readiness, then writes `/private/tmp/ddip-history-restart.done` within 45 seconds. The test requires a later PostgreSQL postmaster start time and identical cursor continuation/events after API restart. Never use this procedure against an operational installation. Without the property, ordinary CI checks fresh API/connection continuity without claiming a server restart.

Current executed evidence is in the [B2b-2B contract](../../docs/architecture/durable-execution.md#stage-b2b-2b-bounded-run-history-and-inspection). The implemented B2 source management, verified import, durable queued submission and requested history/inspection surfaces are ready for owner review. Full capability discovery/HTTP artifact listing is not added by this checkpoint. The queue cannot drain yet. Checkpoint C still needs owner authorization, fixed runnable deployment/adapters, host-guard/fenced claiming, live source gates, native revalidation, bounded supervision, atomic stage/result/audit writes and failure/recovery qualification. Later run controls, Operations UI and E5 completion remain deferred.

## C1 passive worker and durable claim foundation

**C1 does not execute processing runs. Production dispatch remains disabled until the
Reliability adapter is implemented and approved in C2.** A READY worker means passive
coordination readiness, not native processing capability. Accepted runs remain QUEUED;
the twenty-unfinished-run queue cannot drain. No worker/claim/recovery HTTP endpoint exists.

### Private startup contract

Provision immutable database profiles and the existing private installation guard first;
startup never creates or repairs them. Use a B2 run-profile descriptor and its exact reviewed
deployment SHA. All worker/import profiles for the installation must associate the **same
CONTROL binding**, with the original private root/device/inode identities. A different
historical worker/import host-binding digest fails closed. Other local bindings use the
existing importStorageVersion=1 root/device/inode schema; TEMP/STAGING are private writable
roots disjoint from native roots, CONTROL and each other. Content Bronze uses the existing
loopback-only importBronzeVersion=1 descriptor/private credentials file. Startup does not
contact Bronze or verify imported artifacts.

In addition to the existing ignored DDIP_DB_URL/USER/PASSWORD and DDIP_INSTALLATION_ID,
export these private values; do not put credentials on command lines:

```sh
export DDIP_WORKER_EXECUTION_PROFILE_ID='<reviewed-run-profile-UUID>'
export DDIP_WORKER_CONTROL_BINDING_ID='<existing-installation-control-binding>'
export DDIP_WORKER_DEPLOYMENT_SHA256='<exact-deploymentSha256-from-that-profile>'
export DDIP_WORKER_PYTHON='<absolute-approved-python-executable>'
export DDIP_WORKER_JAVA_HOME='<running-JDK-21-home>'
export DDIP_WORKER_PROCESSING_ROOT='<canonical-repository-processing-directory>'
DDIP_ROLE=worker java -jar services/content-api/target/content-api-0.1.0-SNAPSHOT.jar
```

The role forces a non-web context even if servlet mode is requested. It has no Tomcat,
analytics controllers/query pool, Flyway migration, claim polling or processing child.
It validates the current Java 21 JVM/Java home, script/executable availability, PostgreSQL 17
and all seven migration checksums, installation, canonical profile digest and storage roots.
Python/PySpark versions and deployment hash are reviewed metadata pins here, **not executed
version probes or an authenticity guarantee**; C2 must qualify the actual fixed adapter/runtime.
There is deliberately no dispatch-enabled property or injectable production test executor.

The guard is acquired before registration and retained throughout the worker lifetime.
READY requires no unresolved ownership. A live guard owner prevents startup; a free guard
with a durable occupied slot registers RECOVERY_ONLY and never adopts/releases that slot.
STOPPING is respected by stopping the new idle worker without clearing the coordinator mode.
Invalid schema/runtime/configuration fails with fixed safe diagnostics.

### Claims, heartbeats and interruption

The internal WorkerRepository claim boundary is exercised only with a capability on the
**test classpath**. C1 production has NONE. A future real capability must match the exact
configured execution-profile ID/digest and supported fixed pipeline. No fake capability
can be selected through a request, catalog, property or Spring bean.

Claims lock coordinator, selected sources in deterministic key order, then run/attempt.
The oldest immutable submission sequence is checked; live source disablement produces
FAILED/SOURCE_DISABLED without an attempt, leaving frozen revisions untouched. Incompatible
metadata/profile produces FAILED/RUNTIME_MISMATCH without silently skipping an entry forever.
Compatible test-only claims commit RUNNING plus EXECUTE/CLAIMED, monotonic attempt number,
new epoch, worker/attempt pointer and RUN_CLAIMED/ATTEMPT_CLAIMED audits atomically. No stage,
counts, result or SUCCEEDED evidence is fabricated. Captured request/configuration/input bytes,
canonical hashes and registered qualification descriptors are checked in PostgreSQL only.
C2 must independently reopen/reverify native dependencies before consumption.

Owner tokens compare installation, worker, attempt, epoch, expected states and all applicable
row versions. Zero-row/stale/failed database updates stop further authorization without
refreshing a token or clearing ownership. The five-second heartbeat task uses bounded short
transactions; occupied-slot heartbeats update worker/coordinator timestamps and versions,
without audit spam or lifecycle transitions. Thirty seconds is a stale indication, never a
lease expiry or takeover permission. An idle RECOVERY_ONLY observer cannot heartbeat the old
owner's slot. Connection/statement/lock/transaction bounds remain three seconds.

Clean idle Ctrl-C/context shutdown stops scheduling, records STOPPED and closes the guard;
queued selections stay unchanged. Known pre-launch failure can close FAILED/release only
while CLAIMED, before any stage intent or side-effect authorization. Missing PID information
alone never proves quiescence. Explicit interrupt records INTERRUPTED and RECOVERY_REQUIRED
while retaining occupied ownership. Ambiguous commit, database failure or abrupt death can
leave RUNNING/CLAIMED occupied; a restart remains blocked. Lookup by the exact attempted UUID
returns a historical receipt, not a new authorization token or permission to launch again.
C1 has no reconciliation, automatic retry, orphan cleanup or native process supervisor.

Existing GET run/attempt/event APIs display these persisted states and actual audit records;
currentStage/counts/result stay null until real evidence exists. History reads remain independent
of the coordinator lock. No public API contract/classification was expanded for C1.

### Reproduction and C2 boundary

Use only the dedicated disposable PostgreSQL service, pinned JDK 21 and existing test Python
runtime. Each suite creates/drops its own synthetic database; never target operational data.

```sh
mvn -q -f services/content-api/pom.xml verify -Ppostgres \
  -Dit.test=WorkerCoordinationIT -Dgroups=C1_GATE_A \
  -Dtest=NoUnitTests -Dsurefire.failIfNoSpecifiedTests=false
mvn -q -f services/content-api/pom.xml verify -Ppostgres \
  -Dit.test=WorkerCoordinationIT -Dgroups=C1_GATE_B \
  -Dtest=NoUnitTests -Dsurefire.failIfNoSpecifiedTests=false
mvn -q -f services/content-api/pom.xml verify -Ppostgres
mvn -q -f services/content-api/pom.xml verify -Pintegration \
  -Danalytics.python="$PWD/outputs/silver-python312/.venv/bin/python"
npm run check
```

The full postgres profile includes the 23 small Spark-backed Silver import cases; these are
synthetic read-only import regressions, not worker execution. For an actual dedicated server
restart, add `-Dddip.test.worker-restart-signal=/private/tmp/ddip-c1-worker-restart` and/or the
existing history restart signal. An external harness must wait for READY, restart only the
dedicated test container, wait for database readiness and write the matching `.done` file within
45 seconds. Tests require a newer PostgreSQL postmaster start time and identical persisted
claim/selection/history identities. Ordinary CI without these properties checks fresh
connections/API restart and does not claim an actual database-server restart.

Current local counts and exact commands are recorded in the [C1 evidence](../../docs/architecture/durable-execution.md#c1-local-validation-and-remaining-work).
C2 requires the fixed Reliability adapter, actual deployment/native dependency preflight,
stage INTENT/launch recheck/process-identity persistence, bounded process supervision, independent
publication verification and atomic result registration before enabling dispatch. Content,
RSS journaling, run controls and explicit execution-owner reconciliation remain later work.
C1 does not complete Checkpoint C or E5. Hosted CI remains pending owner publication/observation.

## C2a Reliability read-only preflight

**Production dispatch is still disabled.** No command, environment flag or HTTP request enables
C2a as an executor. Starting the existing worker remains passive; real runs remain QUEUED.
`ReliabilityPreflight.verify(guard, owner, executionRuntimeId, checkpointId)` is an internal seam
for C2b and isolated test-owned attempts, not a new operator entry point. It cannot publish,
reserve an output namespace, register a result artifact or finish a run successfully.

Only RELIABILITY_V2 with homogeneous request-jsonl-v2 or request-parquet-v1 is supported.
Preflight uses exact frozen source revisions, batch artifact IDs and one service-reference ID.
It reopens the selected native namespaces with the authoritative import/native readers and
joint `reliability_format_inputs.verify_dependencies`, checks original evidence before/after,
and preserves the native dependency fingerprint. There is no latest selection, directory scan,
fallback registration or replacement source revision. The native reader requires one approved
batch root/reference root and compatible native profiles for the whole selected set.

### Execution-only runtime profile

Provision an immutable row in the existing `execution_profiles` store with this exact shape;
this example is descriptive, not a management API request or runnable production configuration:

```json
{
  "executionRuntimeVersion": 1,
  "pipeline": "RELIABILITY_V2",
  "runProfileId": "<frozen-run-profile-id>",
  "runProfileSha256": "<64-lowercase-hex>",
  "python": "/approved/python312/bin/python",
  "javaHome": "/approved/jdk21",
  "processingRoot": "/approved/pinned-deployment/processing",
  "deploymentSha256": "<frozen-deployment-sha256>",
  "deploymentRevision": "<40-lowercase-hex-build-revision>",
  "bindings": {
    "batch": "approved-batches",
    "reference": "approved-reference",
    "output": "approved-reliability",
    "staging": "approved-staging",
    "temp": "approved-temp",
    "control": "approved-control"
  },
  "verificationSeconds": 900
}
```

The unchanged RunProfile v1 and captured selection pin `runProfileSha256` and deployment SHA.
Associate the execution-only profile with the same six reviewed storage bindings and installation.
Each frozen canonical descriptor/root device/inode must match; roots must be disjoint, and
CONTROL must equal the installation host guard. Staging/temp/control require private ownership
and permissions. All writer roots must be writable with >=1 GiB usable space per volume.
An old opaque deployment hash cannot be replaced in a queued run to make it executable.

The deployment includes canonical `e5-reliability-deployment.json`:
`{deploymentRevision,versions,spark,modules}`. Its SHA-256 equals the frozen deployment SHA;
`modules` is the exact fixed filename/hash map listed by `ReliabilityRuntime.MODULES` and
`reliability_execution_preflight.MODULES`. The helper `deployment_manifest(revision, root)`
is for explicit reviewed provisioning/test setup only; preflight never generates or repairs it.
Use canonical bytes from the existing `publication_metadata.canonical_bytes` for this manifest.
The fixed tuple is Java 21, Python 3.12.15, PySpark 4.0.2 and DuckDB 1.4.5. The future Spark
settings are local[2], shuffle/default parallelism 8, 1024 MiB driver/executor, four output
partitions, event log false. Java checks its actual JVM; the authorized Python verifier checks
actual Python/package versions. No Spark session or gateway starts. Dependencies are never
installed automatically. Package versions and pinned local module hashes are consistency checks,
not cryptographic authentication of the interpreter, third-party wheels or storage administrator.

### Stage and process evidence

CHECK_INPUTS INTENT is committed before launching the fixed verifier. A separate fenced
transaction consumes its single command authorization after checking controls and exact
worker/run/attempt/epoch/row versions. Actual child PID/start/instance identity is persisted
once observed. No SQL transaction remains open during native verification. The command is an
explicit argument list: approved Python, `-I -B -X utf8`, fixed deployed
`reliability_execution_preflight.py`, private `command.json`. It receives no shell or ambient
AWS/proxy/Python override environment. Stdin is closed; stdout/stderr are discarded with
independent 16 MiB bounds. Deadline <=15 minutes, cleanup <=5 seconds graceful +5 forced
+5 final drain. Any observed descendant is conservative recovery uncertainty.

Private 0700 CONTROL directories contain exclusive 0600 command/selection/result/evidence
sidecars and parent `verified.json`. Command/result/validation anchor <=64 KiB each;
selection/evidence <=16 MiB each. The parent verifies protocol version, run/attempt/stage,
checkpoint/selection/runtime identities, exit/status agreement, native evidence and exact
decimal-string counts. The stage result SHA anchors `verified.json`, which hashes the exact
command, result, evidence and frozen selection and records observed clean cleanup. Keep these
private descriptors for explicit reconciliation; C2a adds no automatic retention/cleanup job.
No raw event/CSV/Parquet payload or private descriptor is returned by management inspection.

Native files are opened read-only and checked for unchanged bytes/mtimes. The 32-batch,
128-file, 1 GiB and 2M-occurrence native budgets and independent DuckDB limits are unchanged.
Free-space checks are cooperative admission, not quotas. CHECK_INPUTS VERIFIED leaves the
test-owned run/attempt RUNNING and ownership occupied, with no publication result. A known
clean read-only failure records stage FAILED only. Invalid results, interruption or uncertain
cleanup require UNKNOWN/RECOVERY_REQUIRED when a fenced database commit is possible;
database/ownership failure retains the existing occupied evidence. C2a cannot release or
recover an execution owner. No heartbeat expiry, process exit or free guard permits takeover.

`inspectCheckInputs` resolves an exact checkpoint to historical INTENT_COMMITTED,
LAUNCH_UNCERTAIN, CHILD_RECORDED, HISTORICAL_VERIFIED, CONFIRMED_ROLLBACK or UNRESOLVED.
It grants no launch authority. A lost intent/authorization acknowledgment never causes an
automatic relaunch. Existing run/attempt/event GETs show persisted stages and the fixed
STAGE_LAUNCH_AUTHORIZED/RESOURCE_LIMIT classifications without private process information.

### Synthetic reproduction and remaining boundary

Use the pinned local runtimes and only a dedicated disposable PostgreSQL test service. The
existing PostgreSQL test environment variables are required, plus an absolute DDIP_TEST_PYTHON
pointing to the approved Python environment. Tests create/drop isolated databases and native
fixtures; fixture authoring is setup, not part of preflight.

```sh
JAVA_HOME=$(/usr/libexec/java_home -v 21) \
PYTHONPATH=processing:processing/tests \
outputs/silver-python312/.venv/bin/python -B -m unittest \
  processing/tests/test_reliability_execution_preflight.py -v

mvn -q -f services/content-api/pom.xml verify -Ppostgres \
  -Dit.test=ReliabilityPreflightIT -Dtest=VerifierProcessTest
mvn -q -f services/content-api/pom.xml verify -Ppostgres
mvn -q -f services/content-api/pom.xml verify -Pintegration \
  -Danalytics.python="$PWD/outputs/silver-python312/.venv/bin/python"
npm run check
```

The literal one-batch native dependency SHA is
`3b9354d578f017cefea8e9ace2556c6ed474afec606722ee01e9fba8e4a39a7a`
(one file, 2439 bytes, ten physical rows). The fixture pins native original timestamps and
checks deterministic selection order; operational IDs are excluded from the native identity.
Current Gate A/B and regression counts are recorded in the
[C2a implementation contract](../../docs/architecture/durable-execution.md#c2a-validation-and-c2b-boundary).

**Implemented:** internal read-only preflight, fenced stage evidence and bounded supervision.
**Tested:** local synthetic/native/PostgreSQL evidence in the contract; hosted CI is pending.
**Planned for separately authorized C2b:** revalidate the retained command/bundle/evidence
(`recheck` calls full qualification again), construct exact native publisher arguments, implement
publication stage/process lifecycle, independently verify output and atomically register verified
results before enabling a production capability. Fresh preflight is point-in-time evidence, not
permanent readability, a filesystem snapshot or proof of future aggregate correctness.
**Deferred:** all production publication/dispatch, Content execution, automatic recovery/retry,
new run-control endpoints, activation and E5 completion. STOP at C2a.
