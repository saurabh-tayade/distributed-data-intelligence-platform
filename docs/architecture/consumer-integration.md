# M6.1 consumer integration experiment

This is an integration feasibility experiment for [issue #27](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/issues/27), not a production API or verified replacement for the Python consumer. Spring Boot and React are outside M6.1. Accepted ADRs 0001–0004 and Gold v1 remain unchanged.

## Reference responsibilities reviewed before the experiment

The reference is [gold_reader.py](../../processing/gold_reader.py) plus [gold_analytics.py](../../processing/gold_analytics.py), not just their report SQL. A production consumer must preserve the following behavior:

| Boundary | Existing behavior |
| --- | --- |
| Selection | Explicit fingerprint or exact supported v1/revision-1 path; no latest pointer or arbitrary Parquet glob; both tables belong to the same publication |
| Paths and layout | Reject traversal, symlinks below the configured root, wildcard paths, missing/extra analytical files and unexpected directory entries; allow only named Spark bookkeeping files |
| Completion | Require `complete.json`; reject duplicate JSON properties, wrong types including boolean/float integers, unknown/missing fields and unsupported versions; check fingerprint and count relationships |
| Input manifest | Require canonical UTF-8 JSON bytes, SHA-256 matching identity, exact versioned metadata structure, sorted unique paths, representative relationships and input/duplicate counts; never open Silver |
| Output inventory | Exact listed paths only; ordered unique flat Parquet entries, integer lengths and lowercase SHA-256; verify every length and digest |
| Physical schemas | Inspect every Parquet file independently, with exact Gold field order/names/logical types; disable hive inference and schema union |
| Values and keys | Required/nonblank fields, supported versions, count bounds, warning code arrays/order, finite and consistent required timestamps, article and source/day uniqueness |
| Reconciliation | Both row counts match completion; both observation totals match accepted inputs; source sets/totals, first UTC dates and latest times match across products; daily article cardinality and selected-warning bounds |
| Change detection | Recheck metadata and inventory after opening and before/after every report; do not expose results after an observed change |
| Queries | Four bounded named reports, parameterized values, source filtering, inclusive daily/quality date filters, deterministic ordering, limits 1–1000, no unrestricted SQL interface |
| Results and errors | Preserve nulls, UTC microseconds and ordered warning lists; fixed safe diagnostics, no raw database exception text; close connections on success and failures |

The Python reader verifies recorded consistency, not cryptographic authenticity. It cannot independently recompute historical warning totals, distinct counts or winner provenance without Silver; M2.4 verifies those at publication time. It does not provide transactions or detect changes reverted between checks. The [Gold contracts](gold-contracts.md), [publication design](gold-publication.md) and [Python consumer guide](duckdb-analytics.md) remain authoritative.

Observations are not page views. Identities remain source-scoped. Warning totals count codes, and the quality rate is codes divided by accepted observations; it can exceed one. Missing publication dates do not automatically warn. Distinct ingestion counts cannot be summed into a global publisher metric. Daily dates reflect UTC ingestion time, not publication time.

## Experiment acceptance

Before recommending an integration architecture, demonstrate Java 21/JDBC loading, queries over M2.4-produced synthetic Parquet, full result parity with Python for all four reports and filters, null/date/microsecond/list mappings, deterministic cleanup, and safe failures. Explicitly test and document integrity gaps rather than counting them as Java protections. Test generation may use local Spark; the Java query process must need only the generated Gold directory. Keep retained-data evidence separate and all generated data ignored.

The experiment below passed its compatibility acceptance checks. [ADR 0005](../adr/0005-gold-consumer-integration.md) was subsequently **Accepted** on October 18, 2026 for the existing Python CLI boundary used in [M6.2](../../services/content-api/README.md); the JDBC POC remains unverified for production.

## POC interfaces and runtime

[services/api](../../services/api/README.md) is an isolated Maven Java 21 module. `JdbcReports.open(Path goldRoot, String fingerprint)` owns one in-memory JDBC connection; `report(name, source, start, end, limit)` returns detached lists/maps; `close()` releases the connection. It supports the four existing report names and source filters; only daily/quality accept inclusive dates. Limits are 1–1000. `Main` exposes this interface as a local CLI with JSON output and status `UNVERIFIED_POC`, never `OK` or a claim of integrity verification.

`PocPublication` is a selection guard, not a port of `gold_reader.py`. It accepts only a lowercase 64-character fingerprint beneath `v1/processor_revision=1/input_set=...`; unlike Python it does not accept a root-relative publication path. It reads exact listed file paths and checks selected path safety and versions. Its 1 MiB marker bound is an experimental resource limit, not a new Gold contract. Configured root aliases are resolved, while symlinks below that root along selected paths are rejected.

The POC depends on DuckDB JDBC **1.4.5.0**, whose engine reports **v1.4.5**. Jackson **2.22.3** and JUnit **6.1.3** reuse existing repository baselines; DuckDB JDBC adds no transitive Maven dependencies. No Spring Boot, HTTP server, connection pool, external DuckDB binary or native build toolchain is needed. The inspected JAR SHA-256 is `5f11500c4fe8af47cfb15b36bb92e22597ad2109556c1564823e2db1a210072e`; the JAR contains macOS universal, Linux amd64/ARM64 and Windows amd64 native libraries. Only macOS ARM64 and Linux amd64 were executed.

Writable temporary space that permits native library loading is required; a `noexec` mount failed the Linux experiment. This applies even though the DuckDB session disables temporary query spill files. The connection also disables automatic extension installation/loading and uses two threads. Only fixed SELECTs run against Parquet, with paths, source, dates and limit bound as JDBC parameters. There is no public arbitrary SQL method, but the in-memory connection is not itself an OS-enforced read-only security boundary. Reports do not write Gold; query-only file invariance is tested.

## Reproducing synthetic compatibility tests

Prerequisites: JDK 21, Maven 3.9+ (local run used 3.9.16), Python 3.12.15 and the existing pinned processing and analytics requirements. Dependency installation needs Maven/Python package access. Test execution is offline and needs no publisher, SeaweedFS, AWS or running Spark service. Fixture generation starts Spark `local[2]` temporarily, then stops it and removes only its own temporary synthetic Silver.

From the repository root, with Java 21 and Maven on `PATH`:

```sh
export PYSPARK_PYTHON="$PWD/outputs/silver-python312/.venv/bin/python"
export SPARK_LOCAL_IP=127.0.0.1 TZ=UTC
export PYTHONPATH=processing:processing/tests
# On macOS, if Java 21 is not already selected:
export JAVA_HOME=$(/usr/libexec/java_home -v 21)
export PATH="$JAVA_HOME/bin:$PATH"
"$PYSPARK_PYTHON" -m pip install -r processing/requirements-bronze.txt -r processing/requirements-analytics.txt
mvn -B -ntp -f services/api/pom.xml -DskipTests package
"$PYSPARK_PYTHON" services/api/tests/generate_fixtures.py --output services/api/target/fixtures
mvn -B -ntp -f services/api/pom.xml verify
```

Generation requires a fresh output directory and never overwrites an existing one. For another run, choose a new ignored directory with `--output` and pass its absolute path via `mvn ... -Dgold.fixture=/absolute/path`. `mvn clean` removes `target/`, so generate fixtures again afterward. Do not point this generator at retained Gold. Generated Parquet, reference JSON, damaged copies and Maven artifacts stay ignored.

The shared [fixture builder](../../processing/tests/gold_fixtures.py) was extracted from the existing Python analytics test without changing producer code or fixture semantics. The [comparison generator](../../services/api/tests/generate_fixtures.py) first validates each completed publication with Python, then stores full expected report results. Java tests need only these generated fixtures and Java dependencies; Python reference generation is not a Java integrity check. Fixed snapshots include earlier and rejected-only/empty selections, null publication/update dates, an offset crossing UTC midnight, two warnings per observation, repeated entry indices and deterministic article selection.

Run the Java CLI using the generated publication fingerprint:

```sh
PUBLICATION=$("$PYSPARK_PYTHON" -c 'import json; print(json.load(open("services/api/target/fixtures/reference.json"))["publications"]["full"].split("input_set=")[1])')
java -cp 'services/api/target/classes:services/api/target/dependency/*' platform.consumer.Main \
  --gold-root "services/api/target/fixtures/gold's snapshots" \
  --publication "$PUBLICATION" --report publishers --limit 100
java -cp 'services/api/target/classes:services/api/target/dependency/*' platform.consumer.Main \
  --gold-root "services/api/target/fixtures/gold's snapshots" \
  --publication "$PUBLICATION" --report quality --source alpha \
  --start-date 2026-10-13 --end-date 2026-10-13 --limit 100
```

The CLI uses exit 0 for successful **unverified** query results, 2 for argument errors and 1 for selection/JDBC failures. Diagnostics contain fixed codes such as `INCOMPLETE_OUTPUT`, `UNSUPPORTED_VERSION`, `INVALID_LAYOUT`, `JDBC_INITIALIZATION_FAILED` and `QUERY_FAILED`, without database messages or path contents. `--help` needs no publication. A missing completion marker is rejected; a corrupt Parquet file fails when the report scans it. A successful report does not imply that the other table was scanned or validated.

## Analytical compatibility results

All **28** complete result comparisons passed, not just row counts. They cover all four reports on full, earlier and empty publications, source filtering, injection-shaped source values, limits and inclusive dates. Snapshot selection remains explicit and both report families resolve the same fingerprint.

| Synthetic source | Catalog articles | Observations | All-observation warning codes |
| --- | --- | --- | --- |
| alpha | 2 | 4 | 4 |
| beta | 1 | 1 | 0 |

Daily rows are alpha/October 12 (2 observations, 2 articles, 1 ingestion, 0 warnings), alpha/October 13 (2, 1, 1, 4), and beta/October 13 (1, 1, 1, 0). Alpha's quality rate is `1.0` overall and `2.0` for October 13. The article selected for A is the later ordinal from the second ingestion, with null publication/update timestamps, two ordered warning codes and selected ingestion time `2026-10-13T00:00:00.000001Z`. Earlier A has publication time `2026-10-10T00:00:00.000000Z`. Earlier and empty publications remain independently queryable.

| Gold / SQL type | JDBC handling demonstrated |
| --- | --- |
| `INTEGER`, `BIGINT` | Nullable boxed `Integer`/`Long`; no primitive defaulting of nulls |
| Aggregate integer (`HUGEINT`) | Preserve integral `getObject` result and JSON integer precision; no conversion through double |
| `TIMESTAMP` | `getObject(..., LocalDateTime.class)`; Gold UTC interpretation; six fractional digits and `Z` in JSON |
| `DATE` | `getObject(..., LocalDate.class)`; ISO date independent of JVM default timezone |
| `VARCHAR[]` | `java.sql.Array` copied to a detached list before result close; array freed; empty list and warning order preserved |
| Optional nulls | Java null / JSON null, including absent publication/update times and optional metadata |

A non-UTC JVM timezone test passed. Per-file JDBC `DESCRIBE` matched the complete Python Gold schema mapping; this demonstrates a usable validation mechanism, not enforcement by the POC. Statements/results/connections use try-with-resources; repeated reports, query failure and use-after-close tests exercise cleanup. There is no load-test or native-memory-leak guarantee.

## Integrity coverage and demonstrated gaps

| Responsibility | Java POC today | Feasible production implementation |
| --- | --- | --- |
| Explicit fingerprint, supported path, no globs/latest | Implemented | Retain with configured trusted root |
| Completion presence, selected identity/version fields | Implemented; not full structure/type validation | Strict Jackson keys/types/ranges/duplicate handling and conformance tests |
| Selected flat paths, duplicates, missing files, symlinks | Implemented for listed paths | Add exact layout/allowed housekeeping/inventory ordering checks |
| Canonical manifest, checksum and metadata relationships | **Not implemented** | Java NIO, strict JSON and canonical UTF-8 serializer with Python compatibility vectors |
| File lengths, SHA-256, unexpected files | **Not implemented** | Streaming `MessageDigest`, full inventory comparison |
| Every-file schema enforcement | **Not implemented**; `DESCRIBE` mechanism tested | Compare every name/order/type before exposure, no hive inference/schema union |
| Required values, keys, row counts, cross-table checks | **Not implemented** | Execute equivalent SQL validations over both exact table inventories |
| Changed publication detection | **Not implemented** | Recheck metadata/inventory before and after each report and opening |
| Full historical winner/metric recomputation | Neither consumer should do this | Remains M2.4 producer responsibility; do not add Silver dependency |

Damaged copies were tested separately from valid report comparisons:

| Damage | Java POC result | Python reference result |
| --- | --- | --- |
| Missing marker | `INCOMPLETE_OUTPUT` | `INCOMPLETE_OUTPUT` |
| Unsupported revision | `UNSUPPORTED_VERSION` | `UNSUPPORTED_VERSION` |
| Invalid marker JSON | `PUBLICATION_READ_FAILED` | `INVALID_METADATA` |
| Missing listed data | `INVALID_LAYOUT` | `INVENTORY_MISMATCH` |
| Inventory path traversal | `INVALID_LAYOUT` | `INVALID_METADATA` |
| Listed file is external symlink | `INVALID_LAYOUT` | `INVALID_LAYOUT` |
| Unreadable corrupt Parquet | `QUERY_FAILED` on scan | `FILE_MISMATCH` before query |
| Altered manifest bytes | **Accepted: known gap** | `MANIFEST_MISMATCH` |
| Incorrect completion row count | **Accepted: known gap** | `COUNT_MISMATCH` |
| Unexpected output file | **Accepted: known gap** | `INVALID_LAYOUT` |
| Modified but readable Parquet values | **Accepted: known gap** | `FILE_MISMATCH` |
| Extra column, with refreshed recorded hash | **Accepted: known gap** | `UNSUPPORTED_SCHEMA` |
| Null required title, with refreshed recorded hash | **Accepted: known gap** | `INVALID_VALUES` |

An additional Java test changes `inputs.json` after opening and still receives results, explicitly documenting absent change detection. Python's existing suite tests rejection of changes during a query session. These passing gap assertions must never be reported as passing Java integrity checks. Byte hashes are consistency evidence, not signatures against a trusted authority. A full Java port would need the entire responsibility matrix, including Unicode/canonicalization and negative-test parity; inspecting a Parquet schema alone is insufficient.

## Validation evidence and measurement limits

Executed October 18, 2026 on macOS ARM64: Python 3.12.15, PySpark 4.0.2, DuckDB Python 1.4.5, GraalVM Java 21.0.7 and Maven 3.9.16. The exact JAR's engine version and both complete Gold schemas were checked at runtime.

| Check | Actual result |
| --- | --- |
| Java POC Maven verify | 48 tests passed; 28 report comparisons, 13 damaged-copy cases and 7 mapping/lifecycle/CLI/selection/schema/invariance/change-gap tests |
| Linux amd64 Java test run | Same 48 passed in Temurin 21.0.12.1 container, network disabled; no Python, Spark or Silver mounted |
| Existing Python processing suite | 78 passed in 131.536 seconds |
| Existing ingestion Maven verify | 29 passed, no failures/errors/skips |
| Existing shell regression | All 5 argument cases passed |
| CLI help and syntax checks | Passed: Java help, Python compilation and Maven POM parsing |
| `npm run check` and `git diff --check` | Passed: Markdown/local links, repository hygiene and whitespace |
| Hosted CI | New comparison step configured; execution of these changes not yet verified |

Final path review added a regression for wildcard filenames containing multiple newlines and bounded marker reads before parsing. The first Java run assumed JDBC's version string lacked a `v` prefix; the assertion was corrected to the observed `v1.4.5` before passing. One Maven run was blocked by local cache permissions, then succeeded with access. The first Linux attempt failed on `noexec` temporary storage; changing only the mount to executable allowed the same tests to pass. Existing synthetic corruption tests emitted expected Spark diagnostics and existing socket ResourceWarnings.

The Linux container used `eclipse-temurin@sha256:35501d3f93173b420ac9001658eb68157e813316b865087e345c187fca422c4c`, `--platform linux/amd64`, `--network none`, a read-only root and `--tmpfs /tmp:rw,exec,nosuid`. It mounted the experiment read-only, its generated `target/` writable, Maven tooling and its cache read-only, and executed the offline Surefire goal with the fixture path set inside the container. Docker amd64 emulation on ARM64 is not a hosted-runner performance measurement. Containers were removed after execution; no service was introduced.

Five alternating fresh-process `publishers --limit 100` calls to each CLI used the same three-article/five-observation synthetic publication, captured JSON, and asserted reference equality. `time.perf_counter_ns()` measured total subprocess wall time with a 30-second timeout per call. Filesystem caches were already warm from tests. Java samples (ms): 1385.31, 1167.97, 1163.23, 1169.47, 1152.28; Python samples: 107.30, 57.64, 55.89, 55.81, 56.07. Medians were 1167.97 ms and 56.07 ms respectively. This includes startup, native extraction/loading, querying and serialization; Java performs fewer checks. It is not isolated SQL performance, a production benchmark or grounds to weaken validation. Warm persistent JDBC request latency, resource capacity and concurrent service behavior were not measured.

Optional retained-data comparison separately used the already completed M2 fingerprint `195999b03c48ca9a5828f5a67c1e00c312efe6f52e0916c180188f8bb9bdf185`. Python verified it first; all four full Java result arrays matched Python: 3 publishers, 59 articles, 3 daily rows and 3 quality rows, with 118 observations and zero warnings. All 10 Gold files retained identical lengths, SHA-256 and modification times. No retained data, publisher requests or Silver access is needed by the automated tests. Runtime logs and measurement JSON are local ignored evidence under `outputs/m6-1/`.

## Regression and CI commands

```sh
PYTHONPATH=processing "$PYSPARK_PYTHON" -m unittest discover -s processing/tests -v
mvn -B -ntp -f ingestion/pom.xml verify
bash scripts/test-run-batch.sh
mvn -B -ntp -f services/api/pom.xml verify
java -cp 'services/api/target/classes:services/api/target/dependency/*' platform.consumer.Main --help
npm run check
git diff --check
```

The existing [CI workflow](../../.github/workflows/ci.yml) retains its documentation, ingestion and processing checks. One step after the processing suite generates the synthetic fixtures and runs Maven JDBC tests with runner Java 21. The same pinned Python requirements/cache remain unchanged. Dependency bootstrap needs network access; fixtures/tests themselves stay deterministic and service-independent. A hosted result must be recorded after the owner publishes the changes.

No production reader, API, frontend or new analytical metric shipped. This describes the historical M6.1 experiment. The owner subsequently [accepted ADR 0005](../adr/0005-gold-consumer-integration.md) on October 18, 2026. The separate [M6.2 API module](../../services/content-api/README.md) implements the bounded Python adapter and publisher endpoint, reusing these fixtures. It does not deploy this JDBC POC.
