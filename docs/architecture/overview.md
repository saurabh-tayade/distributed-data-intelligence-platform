# Architecture overview

**Status: proposed architecture.** Only repository foundation files are implemented. No component described below is running or provisioned.

## Sources and ingestion

Planned sources include publisher/content metadata delivered as files or API responses and engagement events such as content views or clicks. Initial contracts, licenses, identifiers, schemas, timestamps, and retention policies will be defined in M1. No real MSN integration or business dataset exists.

Batch adapters will land inputs with source identifiers, ingestion timestamps, and provenance. Streaming adapters will publish events through either Kafka or Redpanda; the broker choice remains open. A future Spark Structured Streaming path will persist/process events with explicit checkpoint, replay, watermark, and late-event policies. A broker is transport, not the long-term analytical record.

## Data layers

| Layer | Planned responsibility | Candidate representation |
| --- | --- | --- |
| Bronze | Preserve raw payloads and provenance for audit and replay | JSON in local MinIO object storage |
| Silver | Validate schemas, normalize types, deduplicate by defined keys, and isolate rejected records | Parquet with documented schemas |
| Gold | Publish curated content and engagement aggregates with explicit grains and metric definitions | Parquet consumed through SQL or serving projections |

Partitions, schema evolution, retention, write atomicity, and replay semantics require explicit design before implementation. Parquet alone does not provide transactions or upserts. Iceberg is deferred; it may be reconsidered if table-management requirements justify it.

## Distributed processing

Apache Spark, with PySpark and Spark SQL, is the planned transformation engine. Start with small local workloads and introduce multiple workers only when a milestone needs distributed execution evidence. Spark support does not itself prove that a workload has run across workers or scales effectively. Java 21 is the intended service runtime; compatible Spark, Python, JVM, and connector versions must be verified before dependencies are selected.

Batch delivery precedes streaming. No exactly-once guarantee is claimed: source offsets, checkpoint recovery, sink behavior, duplicate handling, and event-time policies must be designed and tested together.

## Specialized consumers

- DuckDB will support local SQL exploration of analytical Parquet outputs; it is not the distributed transformation engine.
- Spring Boot services may expose stable read contracts, using PostgreSQL for operational metadata or selected serving projections where justified.
- A React/TypeScript dashboard will consume service APIs for content and engagement metrics.

Object storage is the analytical system of record. PostgreSQL is not intended to duplicate every data layer. Consumer refresh, freshness, and ownership contracts remain to be defined.

## Local-first deployment direction

Docker Compose is planned for the local services actually needed at each milestone: object storage, broker, PostgreSQL, and processing/consumer components. No Compose definitions or containers are introduced in M0.1. Keep data and service volumes outside version control. Establish startup, health checks, resource limits, and teardown instructions incrementally. Local execution must not depend on AWS access.

## Candidate AWS mappings

These mappings are options for an approved, temporary validation exercise, not deployment decisions or committed equivalence with local behavior.

| Local role | AWS candidate | Validation consideration |
| --- | --- | --- |
| MinIO object storage | Amazon S3 | Verify APIs, authentication, and storage behavior used by jobs |
| Kafka-compatible broker | Amazon MSK | Evaluate cost and operational fit before selecting |
| Spark processing | Amazon EMR or AWS Glue | Check runtime, library, and connector compatibility |
| PostgreSQL | Amazon RDS for PostgreSQL | Provision only if the validation requires serving state |
| DuckDB SQL exploration | Amazon Athena | Verify SQL dialect, catalog, and data-layout requirements |
| Services and UI | ECS or static hosting, if needed | May be omitted from a data-only cloud experiment |

No AWS infrastructure may be introduced without explicit approval. See the [charter](../charter.md) for budget and teardown requirements.

## Decisions and open choices

The foundation establishes local-first development, incremental delivery, a layered analytical architecture, and an honest distinction between plans and working features. Broker selection, initial sources, runtime versions, orchestration details, and cloud services remain open. Record consequential implementation decisions using the [ADR process](../adr/README.md).
