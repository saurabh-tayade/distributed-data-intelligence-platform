# Verified Content discovery and reference coverage

Issue #58 extends the [enriched Content catalog boundary](content-catalog-api.md) with two
fixed, read-only reports from the same accepted E1b publication. The authoritative
[native report contract](content-intelligence-publication.md#reports-projections-and-filters)
and [ADR 0007](../adr/0007-content-intelligence-publication.md) are unchanged.

## Fixed requests and selection

| Endpoint | Native report | Supported parameters |
| --- | --- | --- |
| `GET /api/v1/content/source-discovery-daily` | `source_discovery_daily` | `sourceId`, `from`, `to`, `limit`, `offset` |
| `GET /api/v1/content/reference-coverage` | `reference_coverage` | `sourceId`, `limit`, `offset` |

Both default to limit 100 and offset 0. Limits are canonical integers 1–1000 and offsets
0–1,000,000. The source is exact `[a-z0-9][a-z0-9_-]{0,127}`. Discovery dates are exact
Gregorian `YYYY-MM-DD`, years 0001–9999, inclusive UTC bounds with from ≤ to; an omitted
bound is open. Source/date values are never trimmed or normalized. Unknown, duplicate,
blank, malformed or unsupported parameters fail with 400 `INVALID_ARGUMENT` before any
child process starts. Coverage rejects date, status and publisher-attribute filters.

Every request invokes the fixed `content_analytics.py` with the operator-configured
`content.analytics` roots and publication used by the catalog. The shared bounded process
runner enforces admission, timeout and output limits. Each request fully reopens and
verifies the Content publication, retained Gold selection and publisher-reference snapshot,
including when the requested page is empty. There is no arbitrary SQL, generic report
parameter, client-selected path/publication, activation, export, cache or materialized
PostgreSQL copy. Existing Gold, Reliability and catalog endpoints are unchanged.

## Exact response and meanings

Both responses have exactly `publication`, `productId`, `goldPublication`, `reference`,
`filters`, `data`, `meta`; coverage additionally has `totals`. The identities and `meta`
shape match the catalog. `reference` has `referenceId`, `revision`, `snapshotFingerprint`.
`meta` has integer `returned`, `limit`, `offset` and boolean `hasMore`. Counts in rows and
coverage totals are canonical nonnegative signed-64-bit decimal strings, never floating
point. Nullable reference attributes remain JSON null. Successful responses are `no-store`.

Discovery `filters` contains `sourceId`, `from`, `to`, null when absent. Each row has exactly
`sourceId`, `discoveryDate`, `discoveredArticleCount`, `articlesMissingReferenceCount`.
Ordering is source ID ascending, then discovery date ascending, with binary string ordering.
Discovery means first observed in the selected history, not article publication, readership,
first appearance on the internet or observations on that day. Every catalog article belongs
to exactly one source/day group. Only positive discovery groups exist. The response is a
bounded page; no global total is fabricated or inferred from overlapping publications.

Coverage `filters` contains only `sourceId`, null when absent. Its source domain is the
union of catalog sources and all keys in the selected reference snapshot, ordered by source
ID ascending. Each row has exactly `sourceId`, `referenceStatus`, `referenceUsage`,
`publisherName`, `publisherCategory`, `declaredLanguage`, `countryCode`, `articleCount`,
`articlesMissingReferenceCount`, `missingOptionalAttributes`.

- `USED` / `MATCHED`: referenced source with articles; missing-reference count is zero.
- `UNUSED` / `MATCHED`: reference-only source, with zero articles and missing-reference count.
- `ABSENT` / `MISSING`: catalog-only source, with all reference attributes null and
  missing-reference count equal to article count.
- `missingOptionalAttributes` uses the native ordered names `publisher_category`,
  `declared_language`, `country_code` for null matched attributes. It is empty for ABSENT,
  because a missing reference differs from an incomplete matched reference.

Coverage `totals` describes the **entire source-filtered union domain before pagination**:

| Field | Meaning |
| --- | --- |
| `articleCount` | Catalog articles |
| `matchedArticleCount` | Articles with a reference |
| `missingArticleCount` | Articles without a reference |
| `catalogSourceCount` | Sources with articles |
| `matchedSourceCount` | Used matched sources |
| `missingSourceCount` | Catalog-only sources |
| `matchedSourcesMissingOptionalAttributesCount` | Used matched sources with any null optional attribute |
| `referenceSourceCount` | Keys in the selected reference snapshot |
| `unusedReferenceSourceCount` | Reference keys without articles |

Article = matched + missing articles; catalog sources = matched + missing sources;
reference sources = matched + unused reference sources. Adapter and client validate these
identities, page bounds against the union size, row/null/usage invariants, ordering,
uniqueness, filter echo and page contributions to totals. They preserve native totals rather
than summing just visible rows. An unknown source returns empty rows and zero totals. An
empty catalog can still have UNUSED reference rows. A beyond-end page can have no rows but
nonzero whole-domain totals. `hasMore` retains native lookahead semantics at the offset cap;
narrowing filters, not a silently truncated count, is required for inaccessible later rows.

## Client state and safe failures

The existing `#/content` workspace provides three report views. One mounted Content session
pins all five identity components across report changes and navigation away/back. A changed
Content, Gold, reference ID, reference revision or reference fingerprint blocks all Content
requests until a full reload. Only the visible report fetches. Filter, page, refresh and report
changes discard stale visible results. Aborted or superseded responses cannot pin the session,
replace newer results or trigger the identity latch. Switching reports resets filters and
offset, preventing unsupported catalog/date filters from leaking into coverage.

Daily groups and coverage sources use plain-text tables. All nine coverage totals have
explicit scope labels and remain visible on a valid empty page. Counts use exact integer
formatting. Loading, invalid filter, malformed response, missing publication, capacity,
timeout, execution failure and no-result states remain separate. Metadata never renders as
HTML or triggers resource fetches. Browser abort does not guarantee immediate server-child
cancellation; the existing process budgets still apply.

Failure bodies and resource behavior are the [catalog's safe failure contract](content-catalog-api.md#failure-and-resource-boundaries).
In particular, native missing/corrupt evidence gives generic `ANALYTICS_EXECUTION_FAILED`,
not raw paths or partial rows. A malformed successful native envelope gives
`ANALYTICS_RESPONSE_INVALID`. No unverified fallback is available.

## Reproducible validation

Run the [catalog fixture and Maven commands](content-catalog-api.md#reproducible-parity-evidence)
with Java 21 and the pinned processing Python environment activated. If it is not activated,
set both `-Danalytics.python` and `-Dtransport.python` to that environment’s Python executable;
the mandatory native publisher-transport unit test also needs its Spark dependencies.
The expanded generator produces 150 native CLI scenarios: 60 unchanged catalog, 54 daily and
36 coverage, across standard, empty and large-observation-count publications. Literal checks
independently establish daily groups, USED/UNUSED/ABSENT rows, optional nulls and totals.
`ContentApiIT` compares all response fields and JSON types through actual loopback HTTP,
Spring and the bounded native process. Eight damaged copies are queried through all three
reports at first page, absent-source and maximum offset: 72 failures must remain closed.
Retained hashes, sizes and modification times must not change.

The generated `http-reports.json` is a deterministic HTTP projection of those actual native
CLI envelopes, not a separately invented frontend fixture. The actual frontend parsers must
admit every scenario:

```sh
DDIP_CONTENT_HTTP_FIXTURES=../services/content-api/target/content-fixtures/http-reports.json \
  npm --prefix frontend test -- contentNativeParity.test.ts
```

The analytics HTTP CI job runs this check after Maven/native parity, with pinned frontend
dependencies. Changes to the Content parser, its shared Gold/date parser or this cross-runtime
test select that job. Default standalone frontend runs skip only this generated-fixture test;
the ordinary parser and UI regression tests always run. Unit tests reject malformed native
and HTTP envelopes, mixed identities, ordering/count/date/filter violations and unsafe error
contents. UI tests exercise stale completions, interrupted report/navigation requests and
all five identity changes. See [the bounded measurement evidence](../benchmarks/content-serving-small-fixture.md#discovery-and-coverage-extension)
for verification, complete-report, query-only, CLI and HTTP costs.

Browser walkthrough and visual polish remain deferred to the joint UI review. This slice does
not activate publications, enable managed Content dispatch, deploy publicly or complete
multi-user authorization. Local tests and measurements do not claim hosted CI success.

## Final local validation

On November 5, 2026, the final combined Maven `verify -Pintegration` passed **561 unit
cases** (three existing opt-in measurement/projection cases skipped) and **50 actual HTTP
integration cases**: Gold 20, Reliability 16 and Content 14. Content includes the 150 native
parity scenarios and 72 damaged-publication requests, plus strict HTTP input and runtime
failure checks for all three endpoints. Fixture inventories remained unchanged.

Independent review passed frontend typecheck, lint, production build and **1,322 tests** with
the generated native-fixture check enabled (35 files). That check also enforces the full
60/54/36 scenario inventory. The existing frontend chunk-size advisory remains. CI policy
and PostgreSQL inventory tests passed all 45 cases; the measurement harness passed 10 guard
tests. Markdown/relative-link and whitespace checks passed. This slice did not rerun the
separate PostgreSQL integration partitions or browser visual acceptance locally; their
existing CI checks and the later joint walkthrough remain in scope for their own validation.
