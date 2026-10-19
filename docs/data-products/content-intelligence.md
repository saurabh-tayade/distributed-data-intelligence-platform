# Content Intelligence data product

Status: **Proposed for owner review**, October 19, 2026. This is the Content product contract within the [bounded expansion plan](../product-specification.md), not an amendment to accepted Gold v1 or a claim of M6.5 acceptance.

## Users, questions and boundaries

A content analyst can find source-scoped articles, understand when they first appeared in the retained observations, filter by publisher context and export a bounded reproducible catalog. An operator can explain missing enrichment, selected input history and changes between publications.

Useful questions are: which articles did this source contribute, which identities were first discovered on a given UTC date within this inventory, and which catalog rows lack publisher-reference context? Counts do not measure readership, engagement, unique stories across publishers or every article a publisher has ever produced.

Inputs are live public RSS/Atom using the existing registered fetch path, and one versioned publisher-reference CSV. Automated tests use fictional feeds; a live run is a separate, deliberate operator action under existing source-usage constraints. Do not fetch linked article pages/images, infer topics from text, canonicalize URLs, or create cross-publisher story identities.

## Existing behavior preserved

The [batch catalog](../architecture/batch-ingestion.md#source-catalog), [Silver observation contract](../architecture/silver-processing.md), [Gold contracts](../architecture/gold-contracts.md), [publication rules](../architecture/gold-publication.md) and [REST contracts](../architecture/analytics-api-contracts.md) remain authoritative. In particular:

- Keep `article_id = SHA-256` of the existing compact JSON array `["v1", source_id, identifier_kind, source_article_id]`. Do not replace it with a URL/title hash or change it when reference data changes.
- Keep a new ingestion UUID for each retrieval, even if its bytes match an earlier feed. Identical replay-key observations collapse; conflicting replay keys abort; different ordinals/retrievals remain observations.
- Keep Gold's whole-row winner order `ingested_at DESC, ingestion_id DESC, entry_index DESC`, including nullable metadata from the winning observation.
- Keep `gold_source_daily` at source/UTC ingestion-date grain. Its counts and warning codes are not first discoveries or engagement. Daily distinct counts are not globally additive.
- Keep quality warnings separate from new enrichment diagnostics. A missing reference or publication date must not be added to Gold v1's warning-code list.

Propose a separate enriched publication derived from one fully verified Gold v1 publication and one fully validated reference snapshot. Existing outputs and APIs remain usable and unchanged. New consumer reports/filters require separately versioned API review; they are not silently added to `/api/v1/articles`.

## Publisher-reference CSV v1

One row describes one configured feed source, which may be narrower than a publishing organization. Primary key: `source_id`, unique within a reference version. Multiple feeds from one organization remain separate keys; matching display names do not merge their articles.

Exact header and order:

```text
source_id,publisher_name,publisher_category,declared_language,country_code
```

| Field | Type and validation | Meaning |
| --- | --- | --- |
| `source_id` | Required string matching `[a-z0-9][a-z0-9_-]{0,127}` | Exact join to configured source/Gold source ID; no case folding or trimming |
| `publisher_name` | Required nonblank UTF-8 text, 1–200 Unicode scalar values | Curated display name; plain text, not HTML |
| `publisher_category` | Nullable token, 1–64 characters, from a captured versioned vocabulary | Publisher/feed context only, such as a curated subject focus; not an article classification |
| `declared_language` | Nullable token, 1–32 characters, from the captured vocabulary | Reference author's description of the source; not language detection for each article |
| `country_code` | Nullable token from a captured reviewed two-letter uppercase code list | Curated source association; not article location, author nationality or story geography |

CSV dialect: UTF-8 without BOM, comma separator, required single header, double-quote escaping by doubling, LF or CRLF records, no embedded CR/LF/control characters in cells. Optional empty cells map to null; literal `NULL` is text and fails a token vocabulary unless explicitly allowed. Leading/trailing whitespace in nonempty cells is invalid rather than silently normalized. Reject extra/missing headers, wrong field counts, malformed quoting, duplicate keys (including byte-identical duplicate rows), invalid tokens or oversized cells. Reject the entire reference version on any error to prevent ambiguous/multiplying joins. A valid header-only reference is permitted and yields all rows unmatched. Proposed reference bound: 10 MiB and 10,000 rows, recorded in configuration and tested at its boundary.

Each immutable reference artifact has an external manifest with `reference_id`, schema version `1`, human revision label, byte length, SHA-256, row count, vocabulary version/digest and authoring/provenance metadata. A label is not an identity check. Reusing a reference ID/revision with changed bytes is a conflict; corrections create a new version. Retain both exact CSV bytes and manifest. Only non-sensitive, curated source context belongs here; this task adds no CSV data.

## Enrichment and version semantics

Validate reference uniqueness before a left join on exact `source_id`. The join must preserve the base catalog row count and article-ID set. Unmatched sources retain all original article fields and null reference attributes with `reference_status = "MISSING"`; matched sources use `"MATCHED"`, including when their optional reference attributes are null. Never invent an `unknown` source ID or drop articles via an inner join. UI labels may display “Reference unavailable” without changing stored nulls.

A reference row with no article is allowed and appears only in reference-coverage diagnostics, not as a fabricated article or zero-count daily group. Coverage records distinguish unmatched sources/articles, matched rows with missing optional attributes, and unused reference keys. These diagnostics are not content-quality warnings or rejected articles.

One selected reference version applies consistently to the entire derived publication. This is snapshot enrichment, not a historical/as-of join: correcting a publisher category changes the context attached to older discoveries in the new derived publication only. Do not claim the reference value was true when an article was published or discovered. Keep the prior enriched publication available. Effective-dated reference history would require a future contract and ADR.

## Output datasets and time meanings

Proposed `content_catalog_v1` grain/key: one row per unchanged `article_id` within one enriched publication. Its manifest binds `product_id = "content-intelligence"`, product contract/revision, base Gold fingerprint and reference identity. It preserves the full base catalog's validated semantics; a public projection omits internal Bronze/Silver paths just as the current article API does.

| Output fields | Contract |
| --- | --- |
| Existing article identity, source, title, URL, summary, publication/update times, observation counts and selected warning codes | Same values/nulls as the selected base Gold article; no re-selection or reinterpretation |
| `first_observed_at`, `last_observed_at` | Base Gold minimum/maximum observation instants in the frozen retained-input scope |
| `discovered_at` | Exactly `first_observed_at`, explicitly labeled “first discovery in retained inputs” |
| `discovery_date` | UTC date of `discovered_at`; required |
| `publisher_name`, `publisher_category`, `declared_language`, `country_code` | Nullable snapshot reference attributes with the meanings above |
| `reference_status`, `reference_id` | Required match diagnostic and selected reference version identity, including for unmatched rows |

Proposed `content_source_discovery_daily_v1` grain/key is `(source_id, discovery_date)` within the same enriched publication. `discovered_article_count` is a positive signed 64-bit count of catalog identities assigned to that UTC date. `articles_missing_reference_count` is a nonnegative count no greater than that group's discovered count. No zero-count rows are fabricated. Each identity contributes exactly once in a publication, so summing discovery counts over all disjoint source/day groups equals the catalog row count. Displaying only a filtered/limited subset must identify that returned scope. Do not sum discovery totals from overlapping publications.

| Time | Meaning and forbidden substitution |
| --- | --- |
| `published_at` / `updated_at` | Nullable publisher-supplied instants from the selected observation; neither determines identity nor discovery |
| `ingested_at` | Time the feed retrieval completed, carried through Bronze/Silver; repeated fetches create repeated observations |
| `discovered_at` | Earliest accepted observation in the selected retained inventory; not the first time the internet or publisher saw the story |
| `last_observed_at` | Latest accepted observation in that inventory; not proof the article remains available now |
| Product `completed_at` | Operational time of verified publication completion in run metadata; never an article timestamp or fingerprint input |

Timestamps retain UTC microseconds; missing publisher timestamps remain null and do not become discovery time. A late-added old ingestion can move an identity's discovery date earlier in a new publication. Removing historical inputs can move it later. Both changes require an explicit new inventory and scope label; neither edits the old publication. The retained M2 example has one actual ingestion date, so it cannot demonstrate multiday trends; use separately labeled synthetic fixtures.

## Input inventory and publication verification

The derived manifest pins the base Gold fingerprint, exact base completion/input-manifest digests and output inventory, plus the publisher-reference artifact and semantic configuration/processor revision. Retain the base manifest's full ordered inventory of completed Silver inputs and their accepted/rejected file hashes, original ingestion UUIDs, observation coverage and Bronze references. This inventory defines discovery history; “all available data” or a directory glob is not a reproducible selection.

First verify the entire base Gold publication through the accepted consumer boundary; never join a reference against an unverified Parquet glob. Compute the derived outputs with the approved Spark boundary, write to a new product namespace and verify schemas, exact article-ID preservation, null-safe base-field equality, join cardinality, reference membership and discovery reconciliation before completion. Recheck input/reference bytes after processing. Queries must verify the complete derived publication and its recorded dependency identity; the exact dependency verification protocol needs the new-product ADR, without weakening current Gold verification.

Identical inventory/configuration replay verifies and reuses the completed derived publication without writes. Changed reference bytes, base inventory or semantic processor revision create a different publication. Missing/corrupt references, changed inputs, invalid base Gold or failed read-back publish nothing. An absent reference row is allowed; an absent selected reference file is a failure. The last selected verified publication remains available according to the [common availability contract](../product-specification.md#publication-selection-availability-and-freshness).

## Catalog, filters and exports

The proposed enriched catalog offers bounded server-side pagination, exact source filtering, reference-status filtering and exact reference category/language/country filters. Labels must say “publisher category,” “declared source language” and “source country”; they must not imply article classification. Optional discovery-date bounds are inclusive UTC dates; a separately named publication-date filter excludes null publication times and explains that scope. No free-form SQL, full-text relevance ranking or implicit date substitution is introduced. Filter vocabulary and parameters require a new API contract before implementation.

Default catalog ordering remains `last_observed_at DESC, article_id ASC`, with a modest page size and exact returned pagination metadata. Discovery aggregates order by source then UTC discovery date. All pages, facets, summaries and exports must pin the same enriched publication; facet counts must come from the defined full filtered scope or explicitly say they describe returned rows, never the current page presented as the whole catalog.

Show readable title/source/time fields with expandable summary, identity, counts, reference status/version and selected-warning details. Keep untrusted title/summary text escaped, allow only validated HTTP(S) links on explicit navigation and use protected new-tab attributes. Never fetch external content automatically. Large integers remain decimal strings at REST/export boundaries; use exact arithmetic and safe chart fallbacks. Existing daily distinct counts and warning rates retain their meanings.

An authorized export queries the pinned filtered catalog or discovery aggregates within [common export limits](../product-specification.md#authorization-exports-and-audit). Include source-scoped identity, selected reference version, filter/time semantics and publication provenance. Default catalog JSONL omits summary unless explicitly selected to bound size; no raw feed, internal paths or linked content is exported. Source-usage restrictions may disable content-bearing exports. These capabilities are proposed and are not supported by today's article endpoint.

## Acceptance examples and open decisions

Before implementation, review small expected-result fixtures covering these cases:

| Case | Required evidence |
| --- | --- |
| Same source article observed on two days | One catalog identity, two observation occurrences, one discovery contribution on the earlier day |
| Same native identifier under two sources | Two existing source-scoped article IDs; no story merge |
| Missing reference, missing optional attributes, unused key | Catalog identity/counts unchanged; distinct explicit diagnostics |
| Duplicate reference key or missing selected file | Entire derived publication fails; no join multiplication or silent unenrichment |
| Reference correction only | Same article IDs/base counts, new derived fingerprint and context; old publication unchanged |
| Older ingestion added | New base/derived publications, possible earlier discovery; old publications unchanged |
| Null/invalid publisher date | Preserve existing null/warning semantics; discovery still derives from accepted observations |
| Filter/page/export boundaries | Deterministic ordering, no missing/duplicate identities, exact scope and common publication pin |
| Rejected-only base input | Schema-bearing empty catalog/aggregates with explicit rejection provenance; no fabricated dates |

Owner decisions remain the reference vocabulary/governance, default history retention, exact new manifest/API schema, reference correction policy and permitted exports under publisher terms. The [delivery plan](../product-specification.md#delivery-slices-and-decision-gates) separates these additions from M6 acceptance; no accepted ADR or current publication-selection behavior changes here.
