# Verified enriched Content catalog HTTP boundary

Issue #58 adds an analytics-role, read-only view of the accepted E1b publication. It does not change [ADR 0007](../adr/0007-content-intelligence-publication.md), Gold or Content production, native verification, reference snapshot semantics, or the native named reports. The execution-role PostgreSQL catalog remains a separate product boundary.

## Request and configuration

`GET /api/v1/content/catalog` invokes only `content_analytics.py --report catalog` through the shared bounded process runner. There is no client-selected publication, root, report, SQL, automatic latest selection, fallback to Gold, or cached verified session. Every request opens and fully verifies the selected Content publication and retained Gold and publisher-reference dependencies, even for a one-row or empty page.

Configuration is under `content.analytics`:

| Property | Default / meaning |
| --- | --- |
| `python-executable` | `python3`; pinned analytics environment executable |
| `processing-root` | `processing`; directory containing the fixed native entry point |
| `root` | `outputs/content` |
| `gold-root` | `outputs/gold` |
| `reference-root` | `outputs/publisher-reference` |
| `publication` | Empty by default; explicit 64-character lowercase hex product fingerprint |

Paths are operator configuration, never request parameters. Relative paths resolve from the service working directory. Analytics serving needs DuckDB and the native analytics modules, not Spark or Silver. Fixture generation below uses Spark separately.

| Query parameter | Contract |
| --- | --- |
| `sourceId` | Optional exact source; `[a-z0-9][a-z0-9_-]{0,127}` |
| `from`, `to` | Optional inclusive UTC **first-discovery dates**, canonical `YYYY-MM-DD`; if both supplied, from ≤ to |
| `referenceStatus` | Optional `MATCHED` or `MISSING` |
| `publisherCategory` | Optional `business`, `culture`, `general-news`, `science`, `technology` |
| `declaredLanguage` | Optional `en`, `es`, `fr` |
| `countryCode` | Optional `CA`, `GB`, `US` |
| `limit` | Default 100, range 1–1000 |
| `offset` | Default 0, range 0–1,000,000 |

Filters combine with AND. Unknown, duplicated, blank, malformed, or out-of-range parameters fail before process execution. Pagination numbers are canonical nonnegative decimal integers, without signs or leading zeroes. Dates are discovery dates, not publication or observation-day filters. The HTTP default 100 is passed explicitly to native code, whose catalog default is 10.

## Exact response

Top-level fields are exactly `publication`, `productId`, `goldPublication`, `reference`, `filters`, `data`, `meta`. `productId` is `content-intelligence`. `reference` contains `referenceId`, `revision`, `snapshotFingerprint`. `filters` always contains `sourceId`, `from`, `to`, `referenceStatus`, `publisherCategory`, `declaredLanguage`, `countryCode`, with JSON null for absent filters.

Each data row contains exactly:

- `articleId`, `sourceId`, `identifierKind`, `sourceArticleId`, `feedFormat`, `title`, `url`, `summary`
- `publishedAt`, `updatedAt`, `qualityWarnings`, `firstObservedAt`, `lastObservedAt`
- `observationCount`, `distinctIngestionCount`, `discoveredAt`, `discoveryDate`
- `publisherName`, `publisherCategory`, `declaredLanguage`, `countryCode`
- `referenceStatus`, `referenceId`, `referenceRevision`, `referenceFingerprint`

Native decimal-string counters remain strings, including values greater than 2^53. UTC timestamps preserve all six fractional digits and `Z`; nullable fields remain null; warning-array order is preserved. A matched publisher can have null optional attributes. A missing publisher is `MISSING` with all four publisher attributes null. Neither case is silently filled from other data.

Rows remain in native `lastObservedAt DESC, articleId ASC` order. `meta` contains JSON integers `returned`, `limit`, `offset` and boolean `hasMore`, derived from native lookahead. Final/beyond-end pages do not fabricate a total count. No projection into PostgreSQL, cursor store, or export is added.

## Failure and resource boundaries

The response body on failure is only `{"code":"..."}`. Invalid HTTP input is 400 `INVALID_ARGUMENT`. An unset configured publication is 503 `PUBLICATION_UNAVAILABLE`; a missing executable or fixed entry point is 503 `ANALYTICS_RUNTIME_UNAVAILABLE`. A native nonzero exit, including missing/corrupt retained dependencies, is 500 `ANALYTICS_EXECUTION_FAILED`, never a partial catalog. Invalid successful native output is 500 `ANALYTICS_RESPONSE_INVALID`. Shared runner busy/deadline/output bounds remain applicable. Native stderr, paths, and raw dependency diagnostics are not returned.

The adapter strictly validates the fixed native envelope and projects its fields without changing the authoritative native verifier. Full scans and hashes remain deliberate per-request costs. The [small-fixture measurement](../benchmarks/content-serving-small-fixture.md) is evidence about local overhead, not scale capacity or an interactive-latency guarantee.

## Reproducible parity evidence

From the repository root, with the pinned processing environment and Java 21:

```sh
PYTHONPATH=processing:processing/tests .venv/bin/python \
  services/content-api/tests/generate_content_fixtures.py \
  --output services/content-api/target/content-fixtures
cd services/content-api
mvn -Pintegration -Dit.test=ContentApiIT \
  -Danalytics.python=/absolute/path/to/.venv/bin/python verify
```

Generation uses existing literal Gold fixtures, the existing Spark Content producer, and the existing reference publisher. It forces UTC for Python's naive-datetime conversion. CLI and HTTP reads remain Spark-free. Expected envelopes come from actual native CLI subprocesses, supplemented by literal ordering, warning, large-count, null, and microsecond assertions.

`ContentApiIT` sends actual loopback HTTP requests through Spring and the real bounded child process. Sixty parity cases span standard, empty, and >2^53-count publications; first/final/at-end/beyond-end pages; each filter; combinations; inclusive date bounds; no matches; matched optional nulls and missing references. Eight damaged copies cover metadata, Gold, reference, and Content evidence, including semantic corruption on the final catalog row with refreshed hashes. First-page, empty-filter, and beyond-end requests must all fail closed. Fixture hashes, sizes, and modification times must remain unchanged throughout HTTP tests.

## Final local validation and remaining boundaries

On November 5, 2026, the combined Maven `verify -Pintegration` build passed with **550 unit
cases** (three existing opt-in measurement/projection cases skipped) and **49 HTTP integration
cases**: unchanged Gold 20, Reliability 16, and Content 13. Content covers the 60 parity scenarios
and 24 corrupted-fixture requests described above. All 24 native Content publication/reader
acceptance tests passed, including response-byte limits and oversized lookahead rejection.
The CI inventory/policy suite passed all 45 tests and still requires every existing PostgreSQL
partition plus all three separate HTTP suites. Eight measurement-harness safeguard tests passed.

Frontend typecheck, lint, production build and all **1,257 tests across 32 files** passed after
integration. Independent review additionally passed all 60 real native fixture envelopes through
the actual frontend parser after HTTP field projection. The client preserves the native warning
and identifier domain rather than reconstructing Silver rules. Repository Markdown/link/hygiene
and whitespace checks passed. The existing frontend >500 kB chunk warning remains.

The independently pinned route is `#/content`; its [frontend guide](../../frontend/README.md#enriched-content-catalog)
describes interrupted requests, identity changes, page transitions and safe text/link handling.
Visual polish and browser walkthrough remain deferred. No hosted CI result or new PostgreSQL
integration run is claimed here. The endpoint retains the service's existing local-only default
binding and analytics-enabled roles; no public multi-user authorization, deployment, Content
worker dispatch or publication activation is added.
