# M6.4 analytics API compatibility and contracts

## Compatibility requirements and acceptance criteria

Reviewed before implementation: preserve the publisher endpoint, its decimal-string counters and nullable microsecond timestamps; preserve all four Python report names, existing `report()` results and CLI envelopes when `--offset` is absent. Keep accepted ADRs 0004/0005 and Gold v1 unchanged. Python remains the authoritative verifier and SQL implementation. Reuse the existing bounded process runner without changes, fixed report selection, failure mappings and synthetic fixtures. No frontend analytical capabilities are added.

Acceptance requires parameter rejection before process execution, strict report-specific output validation, deterministic DuckDB article pages, real HTTP/Python reference parity on full/earlier/empty publications, corruption rejection, existing regressions and separate read-only retained-data evidence when available.

## Shared HTTP contract

All four endpoints return `{ "publication": "<configured fingerprint>", "data": [], "meta": {} }`. Source filters are exact IDs matching `[a-z0-9][a-z0-9_-]{0,127}`. `limit` defaults to 100 and accepts decimal integers 1–1000. Unknown, duplicate, blank or malformed parameters fail with `400 INVALID_ARGUMENT`; publication, paths and SQL cannot be supplied by clients. Empty results are arrays. Clients must compare publication identities across requests; no server-side session or snapshot switching is provided.

All analytical integer counts are nonnegative signed 64-bit values serialized directly as decimal strings. Required observation/distinct counts are positive for returned article/daily/quality groups. Dates are `YYYY-MM-DD`; timestamps use UTC ISO strings with exactly six fractional digits and `Z`. Nullable fields remain JSON null. No count passes through floating point. Out-of-range consumer counts fail explicitly. Publisher behavior is unchanged.

## Articles

`GET /api/v1/articles?sourceId=alpha&limit=20&offset=0`

`offset` defaults to 0 and accepts decimal integers 0–1,000,000. The fixed cap bounds the accepted request space, not the cost of Gold verification or DuckDB sorting. Rows are ordered by `lastObservedAt DESC, articleId ASC`. Pagination applies after the exact source filter. `meta` contains `returned`, `limit`, `offset`, and boolean `hasMore`. There is no global total, cursor or next-offset token. To continue, add the returned count to the offset while `hasMore` is true, keeping the same source, limit and publication. At the maximum permitted offset, further traversal is unavailable even if `hasMore` is true. Empty/beyond-end pages have `returned: 0` and `hasMore: false`.

| Row field | Representation and meaning |
| --- | --- |
| `articleId` | Existing source-scoped lowercase SHA-256 identity; never regenerated |
| `sourceId` | Nonblank source identity |
| `title` | Selected observation's nonblank title; untrusted text |
| `url`, `summary` | Unchanged nullable strings; untrusted, unsanitized publisher metadata |
| `publishedAt`, `updatedAt` | Nullable selected timestamps; no fallback or imputation |
| `firstObservedAt`, `lastObservedAt` | Required minimum/maximum accepted observation timestamps |
| `observationCount`, `distinctIngestionCount` | Positive decimal strings at article grain; distinct ingestions cannot exceed observations |
| `qualityWarnings` | Selected observation's ordered codes: `[]`, `["INVALID_PUBLISHED_AT"]`, `["INVALID_UPDATED_AT"]`, or both in that order; not historical warning totals |

Bronze keys, input paths, selected-ingestion provenance and feed internals are omitted. JSON transport does not make URLs safe to navigate or summaries safe to insert as HTML; browser consumers must apply their own rendering/link policy.

The Python CLI's optional `--offset` is supported only for `articles`; its presence requests pagination metadata even at zero. The normal envelope adds numeric `offset` and boolean `has_more`; `rows` contains only visible rows. DuckDB performs `LIMIT limit + 1 OFFSET offset`, and Python removes only that lookahead row. Java never slices results or implements SQL. Calls without `--offset` retain the original envelope and article fields, including the CLI's historical provenance. The new Python `article_page()` method returns `rows`, `offset` and `has_more`; existing `report()` remains unchanged.

## Daily

`GET /api/v1/daily?sourceId=alpha&from=2026-10-12&to=2026-10-13&limit=100`

`from` and `to` are optional strict dates (years 0001–9999), mapped to Python `--start-date` and `--end-date`. Either bound can stand alone; reversed ranges fail. Bounds are inclusive UTC ingestion dates, not publication dates. Rows are ordered by `sourceId ASC, ingestionDate ASC`. No absent calendar groups are fabricated. `meta` contains `returned` and `limit`; it does not claim a global total or completeness when capped.

| Row field | Representation and meaning |
| --- | --- |
| `sourceId`, `ingestionDate` | Source and UTC date defining the group |
| `observationCount` | Decimal string; accepted observations in this source/day |
| `distinctArticleCount`, `distinctIngestionCount` | Decimal strings; distinct identities within this group, not additive across date groups |
| `qualityWarningCount` | Decimal string; all warning codes across accepted observations in this group |
| `latestIngestedAt` | Required UTC timestamp of the latest observation in this source/day |

## Quality

`GET /api/v1/quality?sourceId=alpha&from=2026-10-13&to=2026-10-13&limit=100`

Filters and `meta` match daily. Rows are ordered by source ascending. Fields are `sourceId`, decimal-string `observationCount`, decimal-string `qualityWarningCount`, and numeric `warningCodesPerObservation`. The rate is the existing DuckDB result `quality_warning_count / observation_count`: a finite, nonnegative number of **warning codes per accepted observation**, not a percentage of records with warnings. It may exceed 1.0. Counts stay exact; the rate intentionally preserves the existing floating-point analytical contract.

## Failures and operational limits

The existing safe error body contains only `code`: 400 invalid arguments; 503 publication/runtime unavailable or exhausted process capacity; 504 timeout; 500 execution failure or invalid consumer output. No raw stderr, paths, publisher content or exception diagnostics are exposed or logged. Exact envelope fields, report/publication/limit identity, pagination metadata, types, counts, timestamps, filters and ordering are checked before returning any data.

All endpoints share the existing process concurrency, timeout and stream budgets. Article metadata/provenance can reach the stdout cap before 1000 rows; reduce the limit or deliberately configure an operator-owned byte budget. Overflows fail without partial success. Each page reopens and verifies the whole immutable publication. Offset pagination is deterministic for one unchanged publication but is not a scalability improvement, a transaction spanning HTTP calls or protection against concurrent operator reconfiguration.

See the [API startup and test guide](../../services/content-api/README.md) and [Python consumer guide](duckdb-analytics.md) for reproduction commands and validation evidence.
