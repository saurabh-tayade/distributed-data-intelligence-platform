# Project charter

## Objectives

Build a public, portfolio-quality, local-first Content Intelligence & Analytics Platform inspired by a large content portal. Demonstrate heterogeneous ingestion, distributed transformations, trustworthy analytical layers, SQL exploration, and multiple consumers with reproducible evidence.

Foundation documentation and the first Java 21 RSS/Atom retrieval contract are implemented. The complete pipeline capabilities in this charter remain planned.

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
| M1 — Local environment and contracts | Select initial sources, schemas, broker, and compatible versions; document local startup and resource needs |
| M2 — Batch Bronze | Repeatable ingestion with provenance, failure reporting, and replay/idempotency tests |
| M3 — Silver processing | Spark validation, normalization, deduplication, and handling of rejected records with tests |
| M4 — Gold and SQL | Defined aggregate grains and metrics; SQL results reconciled against known test inputs |
| M5 — Streaming | Events reach analytical layers; late events, checkpoints, restarts, and duplicate handling are demonstrated |
| M6 — Consumers | Read API and dashboard consume documented analytical outputs alongside SQL exploration |
| M7 — Reliability | Automated end-to-end validation, operational signals, failure recovery, and runbooks |
| M8 — Benchmarks | Reproducible workload, hardware/configuration details, correctness checks, and measured results |
| M9 — Optional AWS validation | Explicitly approved bounded experiment, local/cloud comparison, cost report, and verified teardown |

M0.1 is complete; M1 has begun with the RSS/Atom contract; M2–M9 are not started. Milestones express intended sequencing, not delivery dates.

## Success criteria

- A fresh checkout can eventually run the MVP using documented local prerequisites without cloud credentials.
- At least one batch source and one event source can be traced through Bronze, Silver, and Gold.
- Replays and restarts produce results consistent with documented duplicate and late-event semantics.
- Quality checks and aggregate reconciliation catch deliberately invalid test inputs.
- SQL, API, and dashboard consumers use documented data contracts and consistent metric definitions.
- Automated checks and a benchmark provide repeatable evidence; measured limitations are published.
- Repository documentation accurately separates implemented functionality from plans.

These are future MVP exit criteria; M0.1 alone does not satisfy them.

## Cost constraints

The local MVP must have no required paid services or AWS usage. Development uses existing hardware and free tooling; local CPU, memory, disk, and retention bounds will be documented at M1 and refined from measurements. No hardware capacity is assumed yet.

AWS spend is not authorized. Before M9, agree on a numeric budget, region, service scope, experiment lifetime, resource tags, and teardown verification. Use budget alerts while recognizing that alerts do not impose a hard spending cap. Avoid persistent cloud resources and paid subscriptions by default.
