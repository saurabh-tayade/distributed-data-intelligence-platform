# Reliability request-window analytics API

This is the narrow #58 read-only vertical slice over the existing, independently verified
Reliability v2 `requests_5m` report. It does not activate run results, change native contracts,
or add a publication registry, scheduler, export, arbitrary query, path input or Content dispatch.
The native [Reliability contract](application-reliability-publication.md) and
[v2 format contract](reliability-format-benchmarks.md) remain authoritative.

## Acceptance and boundaries

Acceptance requires invalid requests to fail before child execution, exact native report parity
for both JSONL and Parquet, pinned publication identity, explicit synthetic labeling, bounded
pagination, safe errors and UI tests for interrupted/stale requests and publication changes.
The Spring adapter runs the fixed `reliability_format_analytics.py` CLI through the existing
shared `BoundedProcessRunner`. Python remains responsible for all dependency, publication,
schema, raw-data correspondence and metric verification and SQL. Java validates and projects
the process response; React never queries artifacts or interprets execution success as activation.

The fixed native CLI is reused directly rather than introducing a second report implementation
or a wrapper that captures its diagnostics. Native failure diagnostics go to stderr, which the
existing runner counts and discards. Consequently all nonzero native exits use the safe generic
`500 ANALYTICS_EXECUTION_FAILED`; the adapter does not guess a cause from paths or traceback text.
This endpoint has its own operator-configured publication, independent of Content's Gold selection.
Both products share process admission, timeout and stdout/stderr budgets.

## HTTP request

`GET /api/v1/reliability/request-windows`

| Parameter | Contract |
| --- | --- |
| `start`, `end` | Required UTC instants with exactly six fractional digits and `Z`, aligned to five-minute boundaries. Select `window_start >= start AND window_start < end`. The interval must be positive and no more than 31 days. No rounding or date imputation. |
| `end` sentinel | Native exclusive `10000-01-01T00:00:00.000000Z` is accepted only as the upper bound. Ordinary years are 0001–9999. |
| `serviceId` | Optional exact `[a-z0-9][a-z0-9_-]{0,127}` service identity, not an ingestion-source filter. |
| `environment` | Optional exact `dev`, `test` or `production`. The word `production` is a synthetic event attribute, not evidence of production traffic. |
| `limit` | Default 100; canonical decimal integer 1–1000. |
| `offset` | Default 0; canonical decimal integer 0–1,000,000. |

Unknown, duplicate, blank, malformed or unsupported parameters return `400 INVALID_ARGUMENT`
before a process starts. Clients cannot supply a publication, report name, SQL, filesystem root,
raw events or reference selection. Limits bound returned rows, not full native verification cost.

## HTTP response

The response contains exactly:

- `publication`: configured native product fingerprint, 64 lowercase hex characters.
- `productId`: `application-reliability`; `productContractVersion: 2`; `processorRevision: 2`.
- `inputFormat`: `JSONL` or `PARQUET`, copied from the native publication.
- `origin: synthetic`; `label: Synthetic request events — not production reliability.`
- `reference`: `referenceId`, `revision`, `snapshotFingerprint`, all native reference identities.
- `filters`: exact `start`/`end` and nullable `serviceId`/`environment`.
- `data`: the rows below; an empty result is `[]`, never fabricated windows.
- `meta`: numeric `returned`, `limit`, `offset`, and boolean `hasMore`.

| Row field | Representation and meaning |
| --- | --- |
| `windowStart`, `serviceId`, `environment` | UTC window/service/environment grain, sorted ascending in this order. UTC timestamp retains six fractional digits. |
| `requestCount` | Positive decimal string: unique valid synthetic request events in the group, not delivery/occurrence count. |
| `serverErrorCount` | Decimal string: events with status 500–599. |
| `serverErrorRate` | Native finite floating-point ratio `server_error_count / request_count`, 0–1, not a percentage. |
| `durationSumMs` | Exact native DECIMAL(38,0) rendered directly as a decimal string, never through floating point. |
| `meanDurationMs` | Native finite floating-point mean, not recomputed by the adapter or UI. |
| `p95DurationMsApprox` | Exact decimal string holding the native approximate p95 estimate in milliseconds. The estimate is explicitly labeled approximate. |
| `serviceName`, `ownerTeam`, `serviceTier` | Untrusted reference text, nullable as in the native report; rendered as text, never HTML. |
| `referenceStatus` | `MATCHED` or `MISSING`; a missing reference retains null metadata, not invented values. |

The native `limit`/`offset` decimal strings become bounded HTTP metadata integers only. Versions
remain integers. Every analytical integer value remains a string. The adapter checks exact envelope
fields, identity/version/format, filters, pagination, row order and uniqueness, timestamp ranges,
primitive types, metric bounds and reference-null consistency before returning any rows.

Pagination uses the existing native `LIMIT limit + 1 OFFSET offset`; Java never slices data.
To continue, add the returned count to offset while `hasMore` is true, preserving filters and
publication. At offset 1,000,000 traversal stops even if `hasMore` is true. No global totals,
zero-filled windows, cross-window percentile or cross-page snapshot transaction are implied.
Every request independently reopens and fully verifies the same configured immutable publication
and retained raw/reference dependencies.

The [bounded serving evidence](../benchmarks/reliability-serving-small-fixture.md) records native
initialization/verification, same-session reports, fresh CLI and first/later HTTP page timings,
consumer tradeoffs, freshness semantics and the #58 acceptance reconciliation. Its tiny synthetic
fixture measurements are not production or scale benchmarks.

## UI behavior

The Reliability route is separate from Content Analytics. Users enter an explicit UTC interval
before the first request. Filters are applied together and reset pagination; page requests keep
the same filter scope. Loading, empty, failure and invalid-response states never show old rows
as current. Stale and aborted completions are ignored.

The first successful response pins the Reliability publication for the browser session, independent
of Content's pin. Navigation away and back preserves that identity. A later mismatch clears rows
and blocks the view until a full dashboard reload. The UI never offers an in-session publication
switch or uses an Operations run result to replace the configured publication. Reference identity,
input format and the synthetic label remain explicit; no production-readiness claim is made.

## Safe failures and configuration

Failures contain only `{ "code": "..." }`. Existing shared codes apply: 400 invalid arguments;
503 unconfigured publication, unavailable runtime or exhausted process capacity; 504 timeout;
500 invalid consumer response or generic native execution failure. Corrupt or missing retained
artifacts fail closed with no partial rows and no fallback publication. No raw child diagnostics,
paths, event content or reference text are logged. Successes use `Cache-Control: no-store`.

Properties are operator-owned, using normal Spring Boot binding:

| Property | Default |
| --- | --- |
| `reliability.analytics.python-executable` | `python3`; resolved once from startup PATH; use an absolute Python 3.12 virtualenv executable. |
| `reliability.analytics.processing-root` | `processing`; the script basename is fixed, not configurable. Keep native consumer dependencies alongside it. |
| `reliability.analytics.root` | `outputs/reliability` |
| `reliability.analytics.batch-root` | `outputs/reliability-batches` |
| `reliability.analytics.reference-root` | `outputs/service-reference` |
| `reliability.analytics.publication` | Empty (503); otherwise the exact 64-hex native v2 fingerprint. |

Paths resolve from JVM startup working directory. The JAR does not bundle Python or native data.
This endpoint is available only in the existing analytics-enabled roles; it creates no database or
worker dependency. An unconfigured Reliability publication does not prevent Content queries or health.
The existing `analytics.timeout`, `analytics.max-processes`, `analytics.max-stdout-bytes` and
`analytics.max-stderr-bytes` bound the shared runner. Do not raise limits automatically after failure.

## Reproduction and test scope

From the repository root, with Java 21, Maven, Node 24 and Python 3.12 available:

```sh
python3.12 -m venv outputs/reliability-analytics/.venv
export ANALYTICS_PYTHON_EXECUTABLE="$PWD/outputs/reliability-analytics/.venv/bin/python"
"$ANALYTICS_PYTHON_EXECUTABLE" -m pip install -r processing/requirements-analytics.txt
PYTHONPATH=processing:processing/tests "$ANALYTICS_PYTHON_EXECUTABLE" \
  services/content-api/tests/generate_reliability_fixtures.py \
  --output services/content-api/target/reliability-fixtures
mvn -q -f services/content-api/pom.xml test
mvn -q -f services/content-api/pom.xml -Pintegration verify \
  -Dit.test=ReliabilityApiIT -DskipUnitTests=true \
  -Danalytics.python="$ANALYTICS_PYTHON_EXECUTABLE"
PYTHONPATH=processing:processing/tests "$ANALYTICS_PYTHON_EXECUTABLE" \
  -m unittest discover -s processing/tests -p test_reliability_format_verification.py -v
npm --prefix frontend ci --ignore-scripts --no-audit --no-fund
npm --prefix frontend run typecheck
npm --prefix frontend run lint
npm --prefix frontend test
npm --prefix frontend run build
npm run check
git diff --check
```

The literal fixture generator is Spark-free and uses the existing independently authored native
verification fixture, not a Java-generated report. HTTP integration compares actual CLI envelopes
for JSONL and Parquet, including first/final/beyond-end pages, exact service/environment filters,
exclusive boundaries, pre-epoch UTC, no-match intervals, nullable references and missing reference
context. Corrupt publication and raw/reference dependency copies fail closed. File hash/mtime
inventories verify that reads leave selected data unchanged. The complete integration profile also
requires the existing Gold fixtures; its generation command remains in the [API guide](../../services/content-api/README.md).

This does not constitute a new producer/Spark correctness qualification, full PostgreSQL execution
regression, browser screenshot audit, benchmark, retained production-data validation or hosted-CI pass.
Those scopes are distinct from this bounded native-reader/API/UI change.

## Local validation evidence

On November 4, 2026, the final local code passed 406 Spring Boot unit/web tests, 16 HTTP/native
integration tests (34 JSONL/Parquet parity scenarios and 12 damaged-fixture cases), and all 15
existing native format-verifier tests. Frontend typecheck, lint, build and 1,170 tests across 30
files passed. All 45 CI-policy tests passed, preserving the unchanged 26-class PostgreSQL inventory
across five partitions and requiring both analytics HTTP suites. Markdown/link checks, repository
hygiene and whitespace checks passed.

The environment used Python 3.12.14 with DuckDB 1.4.5, a complete Java 21 JDK, Maven 3.9.11 and
Node 24.22.1. The frontend build retains the existing >500 kB chunk warning (main bundle 754.60 kB,
216.91 kB gzip). Native production modules and pinned dependencies were unchanged. Full Gold/Spark
and PostgreSQL integration profiles, a live browser visual audit and hosted CI were not run locally.
