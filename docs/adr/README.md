# Architecture decision records

Use an ADR for consequential choices such as the event broker, data contracts, storage/table format, delivery semantics, or an approved cloud experiment. Routine implementation details do not need an ADR.

Name records `NNNN-short-title.md`, starting with `0001`. Each record should include:

- Title and date
- Status: Proposed, Accepted, Rejected, or Superseded
- Context and requirements
- Decision and alternatives considered
- Consequences and tradeoffs
- Validation evidence or follow-up work

A proposed decision is not an implemented feature. Accepted records describe an agreed choice; implementation status must be reported separately. Preserve historical records and link a replacement when superseding a decision.

[ADR 0001](0001-rss-raw-ingestion-contract.md) is **Accepted** and records the RSS/Atom raw ingestion boundary and bounded buffering design. The [charter](../charter.md) establishes project constraints; the [architecture overview](../architecture/overview.md) describes the planned design and open choices.

[ADR 0002](0002-bronze-object-storage.md) is **Accepted** and records Bronze S3 persistence, local SeaweedFS, and the Maven build introduced for external Java dependencies.

[ADR 0003](0003-silver-observation-contract.md) is **Accepted** and records source-scoped Silver article identity and repeated-observation semantics, exercised by both the fixture prototype and local Bronze-to-Silver integration.

[ADR 0004](0004-gold-analytics-publication.md) is **Accepted** and defines M2 Gold contracts, Spark-to-Parquet-to-DuckDB boundaries and immutable local snapshot publication. Owner acceptance was recorded on October 15, 2026. M2.2 implements the [Silver input boundary](../architecture/silver-input-validation.md); Gold products, immutable publication and standalone Python/DuckDB analytics are implemented; retained-data reconciliation is recorded in the [M2 closeout](../m2-closeout.md).

[ADR 0005](0005-gold-consumer-integration.md) is **Accepted** by the owner as of October 18, 2026. M6.1 verifies Java/DuckDB JDBC feasibility and documents remaining integrity gaps. [M6.2](../../services/content-api/README.md) implements the accepted bounded adapter around the existing verified Python analytics CLI and exposes publisher analytics. Acceptance does not authorize deployment of the unverified JDBC POC; the [M6.3 React publisher dashboard](../../frontend/README.md) consumes the existing API, while [M6.4](../architecture/analytics-api-contracts.md) extends the same API boundary to articles, daily and quality. M6.5 implements the complete analytical dashboard and article explorer; remaining browser acceptance and final M6 closeout are pending.

[ADR 0006](0006-publisher-reference-snapshots.md) is **Proposed** for E1a Stage A ([issue #38](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/issues/38)). It specifies strict publisher-reference validation, retained vocabularies and immutable local reference/revision snapshots in the [detailed contract](../architecture/publisher-reference-snapshots.md). Owner approval is required before Stage B implementation; E1b enrichment and existing Gold/consumer behavior are outside this decision.
