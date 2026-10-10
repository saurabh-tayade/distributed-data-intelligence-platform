# Distributed Data Intelligence Platform

An MSN-inspired Content Intelligence & Analytics Platform exploring how heterogeneous content metadata and engagement events can become reliable analytical data products.

**Current status: Foundation (M0.1).** Implemented: repository layout, project charter, architecture proposal, agent guidelines, and development hygiene. No applications, ingestion jobs, datasets, containers, infrastructure, or deployments exist yet. All pipeline capabilities below are planned.

## Engineering objectives

- Ingest heterogeneous sources through batch and streaming paths.
- Transform data with Apache Spark into Bronze, Silver, and Gold layers.
- Demonstrate data quality, replay, idempotency, and observable processing.
- Serve SQL analytics and specialized downstream consumers.
- Build a reproducible local environment and measure performance before making scaling claims.
- Validate selected workloads temporarily on AWS only after explicit approval.

## Planned technology stack

| Area | Candidates |
| --- | --- |
| Services and ingestion | Java 21, Spring Boot; Python where appropriate |
| Distributed processing | Apache Spark, PySpark, Spark SQL |
| Operational storage | PostgreSQL |
| Event transport | Kafka or Redpanda (selection deferred) |
| Object storage | MinIO locally; Amazon S3 for approved cloud validation |
| Data formats | JSON, Parquet; Iceberg deferred |
| SQL analytics | DuckDB locally; Athena as a cloud candidate |
| Frontend | React, TypeScript |
| Local orchestration | Docker Compose |

These are directions, not installed dependencies or compatibility guarantees.

## Planned high-level pipeline

```text
Content metadata / publisher exports --> Batch ingestion ------+
Engagement events --------------------> Event transport -------+--> Bronze (raw)
                                                                       |
                                                               Spark transformations
                                                                       |
                                                                 Silver (validated)
                                                                       |
                                                               Spark aggregations
                                                                       |
                                                                  Gold (curated)
                                                                       |
                                                    SQL analytics / APIs / dashboard
```

Batch processing comes first; streaming is introduced incrementally. Consumers may also read Silver where their use case requires record-level data.

## Repository layout

| Path | Purpose |
| --- | --- |
| `docs/` | Charter, architecture, decision records, and future benchmarks |
| `ingestion/` | Future source adapters and ingestion jobs |
| `processing/` | Future Spark transformations and data quality checks |
| `services/` | Future APIs and application services |
| `frontend/` | Future analytics UI |
| `infrastructure/local/` | Future local environment definitions |
| `infrastructure/aws/` | Reserved for explicitly approved cloud validation |
| `tests/` | Future integration and end-to-end tests |
| `scripts/` | Future development and operational helpers |

Empty directories contain `.gitkeep` placeholders so the layout survives a future clone.

## Milestone roadmap

| Milestone | Planned outcome |
| --- | --- |
| M0 | Repository foundation and development conventions (M0.1 complete) |
| M1 | Source contracts and reproducible local environment |
| M2 | Batch ingestion into Bronze |
| M3 | Spark transformations into Silver |
| M4 | Gold models and SQL analytics |
| M5 | Streaming ingestion and incremental processing |
| M6 | Consumer APIs and analytics dashboard |
| M7 | Reliability, observability, and automated validation |
| M8 | Reproducible benchmarks and optimization |
| M9 | Optional, approved temporary AWS validation and teardown |

Only M0.1 is implemented. M1–M9 are not started; M9 is outside the local MVP.

## Getting started

Start with the [charter](docs/charter.md), [architecture proposal](docs/architecture/overview.md), and [agent guidelines](AGENTS.md). No installation or runtime command is available at this stage. `.env.example` is a documented placeholder without credentials.

Record consequential choices using the [ADR process](docs/adr/README.md). This project is inspired by a content portal domain and is not affiliated with MSN or Microsoft.
