# Visual 1 — Data Product Journey

A portfolio illustration of DDIP's two implemented data-product paths, grounded in owner commit `217a35c` (README refresh) and E5 C3b commit `847511e`. Architecture approval, implemented capability and recorded test evidence are distinct. This illustration is not an application interface or a benchmark report.

## Files and preview

- [Editable SVG](data-product-journey.svg): 1080 × 1350 viewBox, live text and vector shapes, with a descriptive accessibility title and text alternative.
- [Rendered PNG](data-product-journey.png): 1080 × 1350 pixels, rendered directly from that SVG.

![Two journeys: standalone Content batches produce Gold analytics and enriched Content reports; managed Reliability runs produce independently verified immutable Parquet. Shared controls retain frozen selections and execution history. Managed Content and native recovery resolution remain unfinished.](data-product-journey.png)

## Composition and factual references

Navy marks the standalone Content lane; teal marks managed Reliability. Both read from left to right, with downward processing steps. The two Content output branches deliberately separate existing Gold REST/React consumption from enriched Content's standalone DuckDB reports. The shared metadata foundation is visually quieter. Dashed amber styling identifies planned managed Content execution.

| Visual assertion | Committed source |
| --- | --- |
| RSS/Atom → Java ingestion → SeaweedFS Bronze → Spark Silver → Spark Gold | [Root overview](../../README.md#inputs-outputs-and-consumers), [Bronze-to-Silver](../architecture/bronze-to-silver.md) and [Gold contract](../architecture/gold-contracts.md) |
| Gold supplies article catalogs, publisher/daily metrics and quality diagnostics through DuckDB, REST and existing React views | [Analytics API contracts](../architecture/analytics-api-contracts.md), [API guide](../../services/content-api/README.md#http-contract), [frontend implementation](../../frontend/README.md), [accepted ADR 0005](../adr/0005-gold-consumer-integration.md) |
| Gold plus a versioned publisher CSV snapshot produces a separate enriched catalog and discovery publication, with standalone DuckDB reports/CLI | [Content implementation](../architecture/content-intelligence-publication.md#stage-b-implementation-and-executed-validation), [publisher snapshots](../architecture/publisher-reference-snapshots.md), [accepted ADR 0007](../adr/0007-content-intelligence-publication.md) |
| Synthetic JSONL v2 and Parquet v1 inputs use registered Reliability v2 artifacts and one selected service-reference snapshot | [Implemented v2 format contract](../architecture/reliability-format-benchmarks.md#e3-correctness-implementation-and-validation--october-20-2026), [service-reference boundary](../architecture/application-reliability-publication.md#immutable-service-reference-snapshot-v1), [accepted ADR 0009](../adr/0009-reliability-format-equivalence-benchmarks.md) |
| Durable submission → fixed worker/Spark → independent verification → immutable Parquet, request/error counts, five-minute aggregates, latency and duplicate/rejection diagnostics | [Active worker guide](../../services/content-api/README.md#c2b-2-active-reliability-worker), [publication and verified completion](../architecture/durable-execution.md#c2b-1-fixed-reliability-publication-and-verified-completion), [v2 native reader contract](../architecture/reliability-format-benchmarks.md#version-and-interface-boundary) |
| PostgreSQL metadata, frozen selections, idempotency, run/attempt history, independent verification and one globally active managed execution | [E5 invariants](../architecture/durable-execution.md#boundary-and-invariants), [ADR 0010](../adr/0010-durable-execution-coordination-and-recovery.md), [additive ADR 0011](../adr/0011-artifact-import-maintenance-ownership.md) |
| C3b handoff is internal; native reconciliation and final recovery disposition are unfinished | [C3b implementation boundary](../architecture/durable-execution.md#c3b-fenced-execution-recovery-handoff) and [operator limitations](../../services/content-api/README.md#c3b-internal-recovery-handoff) |

The formats represent the same typed event model; they do not share physical provenance or native fingerprints. Each Reliability publication uses one format, and each worker startup pins one reviewed profile. JSONL and Parquet require separate startups, never concurrent workers. Reliability is not represented as Bronze/Silver/Gold and has no completed React analytics dashboard. Content observations are not readership/engagement, and discovery is scoped to selected retained history. Approximate p95 is independently rank-qualified; it is not claimed to be an exact percentile. See the linked contracts for precise validation boundaries.

## Rendering and review

The SVG uses only local vector paths and standard Arial/Helvetica/sans-serif fonts. It has no scripts, external font files, network assets or vendor logos. The line icons are original vector paths. No application dependency or build infrastructure was added.

The PNG was produced with the already-installed Google Chrome 155.0.8059.85 headless renderer on macOS. To reproduce from the repository root with a local Chrome installation:

```sh
visual_profile=$(mktemp -d "${TMPDIR:-/tmp}/ddip-visual1.XXXXXX")
"/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \
  --headless=new --disable-gpu --disable-background-networking \
  --disable-extensions --no-first-run --no-default-browser-check \
  --hide-scrollbars --force-device-scale-factor=1 \
  --window-size=1080,1350 --user-data-dir="$visual_profile" \
  --screenshot="$PWD/docs/visuals/data-product-journey.png" \
  "file://$PWD/docs/visuals/data-product-journey.svg"
```

Use the equivalent installed executable path on another operating system. Temporary browser profiles belong in OS scratch space. Edit the SVG as the source of truth and rerender the PNG after changes. Browser/font differences can slightly alter text metrics; the supplied PNG fixes the reviewed appearance.

Local review checks SVG text bounds and text-to-text overlap, PNG dimensions, self-contained assets and the actual rendered layout. A reduced 540 × 675 preview is used to check feed-size readability. Small status captions benefit from opening the full-resolution image on narrow phones; this is a fixed illustration, not a responsive layout. The SVG description and this source guide provide a text alternative.

On October 22, 2026, the final source passed browser measurements for all 67 text labels: zero bounds violations, zero text overlaps and zero external asset references. The actual 1080 × 1350 PNG and its reduced preview were visually inspected. `npm run check` passed for 45 Markdown files with no lint/link issues and successful repository hygiene; both Git diff whitespace checks passed. No application source, root README, configuration, tests or CI files were changed.

Documentation validation uses `npm run check`, `git diff --check` and `git diff --cached --check`. Native/application test suites are not rerun for this visual-only task, and historical checkpoint counts are not new evidence. Only Visual 1 is included; native recovery resolution, future product visuals and application/UI changes are outside scope.
