# Project charter

## Objectives

Build a public, portfolio-quality, local-first Content Intelligence & Analytics Platform inspired by a large content portal. Demonstrate heterogeneous ingestion, distributed transformations, trustworthy analytical layers, SQL exploration, and multiple consumers with reproducible evidence.

M1 delivered Java 21 local RSS/Atom batch ingestion, SeaweedFS Bronze persistence, Spark Silver normalization and verified Bronze-to-Silver integration with local Parquet. The full batch/streaming, Gold and consumer objectives below remain the long-term MVP direction. See the [M1 closeout record](m1-closeout.md).

## MVP scope

- Content metadata from a documented batch source and engagement events from a reproducible event source, using synthetic or appropriately licensed inputs introduced in a later milestone.
- Batch ingestion and a bounded streaming use case with documented event-time and replay behavior.
- Bronze raw records, Silver validated records, and Gold content/engagement aggregates in object storage.
- Spark batch transformations and incremental processing; original RSS/Atom payloads (JSON where appropriate for other sources) and Parquet analytical outputs.
- SQL analysis, a read API, and a small analytics dashboard as distinct consumers.
- A Docker Compose environment that runs locally, with documented resource requirements established through measurement.
- Tests for functional changes, data quality checks, failure/replay validation, and a reproducible benchmark.

## Explicitly deferred

Production hosting, high availability, multi-region operation, enterprise identity, comprehensive governance, recommendation/ML systems, web-scale scraping, paid data feeds, and full CDC are outside the MVP. Iceberg is deferred until a concrete table-management need justifies it. Cloud validation is optional and follows the local MVP; no AWS infrastructure is currently authorized.

## Engineering principles

Develop locally first; add services only when justified. Prefer small, understandable components, explicit contracts, repeatable runs, and incremental delivery. Define quality and failure behavior before claiming reliability. Preserve raw inputs for replay within documented retention limits. Never infer production scalability from a small local test. Document meaningful tradeoffs in ADRs and keep secrets and runtime data out of Git.

## Milestones M0–M9

| Milestone | Deliverable and exit evidence |
| --- | --- |
| M0 — Foundation | M0.1: repository layout, charter, architecture proposal, conventions, and Git on main; later setup work is tracked separately |
| M1 — Completed Bronze-to-Silver pipeline | Complete: local batch ingestion, Bronze storage, Spark Silver normalization and verified integration |
| M2 — Gold Analytics & Data Products | M2.1: proposed contracts, architecture and publication design (documentation only); later implementation must reconcile Spark Gold and DuckDB SQL against known inputs |
| M3 — Retired milestone slot | Original local Silver scope delivered in M1; no separate active deliverable |
| M4 — Retired milestone slot | Original Gold/SQL scope moved to M2; no duplicate active deliverable |
| M5 — Streaming | Planned: events reach analytical layers; demonstrate late events, checkpoints, restarts and duplicate handling |
| M6 — Consumers | Planned: read API and dashboard consume documented analytical outputs alongside SQL exploration |
| M7 — Reliability | Planned: end-to-end validation, operational signals, failure recovery and runbooks |
| M8 — Benchmarks | Planned: reproducible workloads, environment details, correctness checks and measured results |
| M9 — Optional AWS validation | Planned only with explicit approval: bounded experiment, cost report and verified teardown |

This is the canonical milestone roadmap; other documents link here rather than maintaining separate milestone tables. M0.1 and M0.3 are implemented, and M1 is complete. The original M2 Bronze and M3 Silver objectives were delivered in M1. M2 now covers Gold Analytics & Data Products, moving the former M4 Gold/SQL scope forward. M3 and M4 identifiers are retained as retired historical slots to avoid renumbering M5–M9. Broker selection and resource benchmarking remain deferred. Milestones express intended sequencing, not delivery dates. Gold architecture remains Proposed under [ADR 0004](adr/0004-gold-analytics-publication.md); M2.1 documentation does not mark M2 implementation complete.

## Success criteria

- A fresh checkout can eventually run the MVP using documented local prerequisites without cloud credentials.
- At least one batch source and one event source can be traced through Bronze, Silver, and Gold.
- Replays and restarts produce results consistent with documented duplicate and late-event semantics.
- Quality checks and aggregate reconciliation catch deliberately invalid test inputs.
- SQL, API, and dashboard consumers use documented data contracts and consistent metric definitions.
- Automated checks and a benchmark provide repeatable evidence; measured limitations are published.
- Repository documentation accurately separates implemented functionality from plans.

These are full MVP exit criteria; completing M1 does not satisfy the remaining streaming, Gold, consumer and benchmarking goals.

## Cost constraints

The local MVP must have no required paid services or AWS usage. Development uses existing hardware and free tooling. Per-object read bounds and local setup are documented. Aggregate CPU, memory, disk, retention sizing and resource benchmarking remain deferred; no measured hardware capacity is claimed.

AWS spend is not authorized. Before M9, agree on a numeric budget, region, service scope, experiment lifetime, resource tags, and teardown verification. Use budget alerts while recognizing that alerts do not impose a hard spending cap. Avoid persistent cloud resources and paid subscriptions by default.
