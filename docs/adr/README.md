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
