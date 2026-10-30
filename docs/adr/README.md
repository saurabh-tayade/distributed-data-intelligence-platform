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

[ADR 0006](0006-publisher-reference-snapshots.md) was proposed and **Accepted** by the owner on October 19, 2026, for the scoped E1a boundary ([issue #38](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/issues/38)). It specifies strict publisher-reference validation, retained vocabularies and immutable local reference/revision snapshots in the [detailed contract](../architecture/publisher-reference-snapshots.md), which separately records implementation and validation evidence. Approval covers the local validator, writer, independent opener, CLI and focused tests/documentation; E1b enrichment, the broader expansion and existing Gold/consumer behavior are outside this decision.

[ADR 0007](0007-content-intelligence-publication.md) is **Accepted** by the owner on October 20, 2026 for [issue #42](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/issues/42), E1b Verified Content Catalog & Discovery Analytics. Its [detailed contract](../architecture/content-intelligence-publication.md) defines verified Gold/E1a dependencies, Spark enrichment/discovery, immutable derived publication and a bounded standalone reader. The accepted amendment keeps the consumer synchronous and defers dedicated workers and hard query deadlines; Stage B is authorized. Implementation evidence is recorded separately. Accepted ADRs 0001–0006 and wider product/M6 status are unchanged.

[ADR 0008](0008-application-reliability-publication.md) is **Accepted** by the owner on October 20, 2026, for [issue #44](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/issues/44), combined E2a/E2b Application Reliability. Its [detailed contract](../architecture/application-reliability-publication.md) defines registered synthetic JSONL, deterministic physical occurrence indexing, service references, deduplicated events, five-minute metrics, rank-verified approximate p95, immutable publication and a synchronous independent reader. Approval covers all eight decisions with an exclusive year-10000 query-end sentinel and a 256-route vocabulary cap, preserving other budgets. Stage B is authorized; executed implementation evidence is recorded separately. Accepted ADRs 0001–0007 remain unchanged.

[ADR 0009](0009-reliability-format-equivalence-benchmarks.md) is **Accepted**, October 20, 2026, for [issue #46](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/issues/46). Its [format and benchmark contract](../architecture/reliability-format-benchmarks.md) defines versioned Parquet provenance, logical equivalence, a deterministic generator, independent oracle and planned local benchmarks. The owner approved its eight decisions and two execution amendments: the future full 100K matrix, minimum 1M scale-validation scope and separate explicit owner go/no-go after numeric target preflight. Acceptance is separate from implemented and executed evidence. Accepted ADRs 0001–0008 remain unchanged.

The owner has since sequenced ADR 0009 as **E3 correctness now, E4a performance deferred, E5–E7 next**. E3 implements the v2 registrar, staging, Spark pipeline, immutable publication, independent reader, bounded generator and SQLite correctness oracle. See the [current correctness evidence](../architecture/reliability-format-benchmarks.md#e3-correctness-implementation-and-validation--october-20-2026), including regressions and remaining limitations. Benchmark methodology remains planned; machine-specific resource observations do not define application correctness or architecture. No completed 100K baseline, target preflight, 1M result or new hosted-CI pass is claimed.

[ADR 0010](0010-durable-execution-coordination-and-recovery.md) is **Accepted — October 20, 2026**, including the Stage A.1 amendments and seven decision groups, for E5 Durable Execution & Source Controls. Its [supporting contract](../architecture/durable-execution.md) defines PostgreSQL/Spring JDBC coordination, one host worker, immutable source revisions and guarded cancellation/retry/reconciliation. B1 persistence/runtime-role isolation is implemented; the owner subsequently authorized B2a source management. Its exact source/revision/idempotency API and local evidence are recorded in the supporting contract. Native imports/submission, worker processing/control workflows and Operations UI remain planned and require separate authorization; the subsequent B2b-1A ownership approval is recorded below. Accepted ADRs 0001–0009 and their data/publication/consumer semantics remain unchanged.

The owner accepted [ADR 0011](0011-artifact-import-maintenance-ownership.md) on **October 21, 2026**, additive to ADR 0010, and authorized **B2b-1A durable import ownership only**. The implementation uses an exclusive coordinator import pointer, a durable operation/recovery chain and atomic fenced recovery handoff; STOPPING remains shutdown state. Native identity uniqueness is scoped by product/kind/version/format. [Current evidence](../architecture/durable-execution.md#stage-b2b-1a-durable-import-ownership) distinguishes coordination tests from actual imports. B2b-1B native qualification/functional imports, B2b-2 submission and worker execution remain pending separate authorization.

[ADR 0012](0012-recovery-verification-provenance.md) records the bounded C3c.2 implementation strategy for separate recovery ownership and original native provenance under ADR 0010. It is an implementation record, not a new owner-accepted architecture decision. C3c.2 supplied internal verification; the C3c.3 implementation record below covers internal final disposition.

[ADR 0013](0013-recovery-finalization-evidence.md) records the bounded C3c.3 finalization strategy, conservative native-error interpretation, fresh qualification and atomic disposition/release under ADR 0010. It is an implementation record, not an owner-accepted ADR. C3c.4 supplies the local operational invocation described below.

[ADR 0014](0014-controlled-operational-recovery.md) records C3c.4's trusted local non-web operator role,
fresh challenge-bound process/evidence confirmation, guarded lifecycle and explicit recovery-chain
handling. It is an implementation record, not an owner-accepted ADR. The
[operator procedure](../operations/reliability-recovery.md) and
[current evidence](../architecture/durable-execution.md#c3c4-controlled-operational-recovery) supersede
earlier operational-activation limitations only to the extent implemented and validated there.

[ADR 0015](0015-managed-silver-receipt-adapter.md) records D3's exact D2 receipt adapter,
fenced Silver invocation and reuse of native Silver qualification and artifact registration.
It is an implementation record with owner acceptance pending. Production Content dispatch,
Content enrichment and operational Content recovery remain disabled or deferred; D4 extends
its historical Gold deferral with the bounded checkpoint below.

[ADR 0016](0016-managed-gold-frozen-dependencies.md) records D4's complete frozen Silver set,
reuse of native Gold publication/read-only verification, and durable intermediate Gold registration.
It is an implementation record with owner acceptance pending. ADR 0015's approval status is unchanged.
