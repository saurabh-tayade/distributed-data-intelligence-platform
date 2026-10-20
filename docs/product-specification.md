# Data products and bounded platform expansion

Status: **Proposed for owner review**. Prepared October 19, 2026 for [issue #36](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/issues/36). This document specifies future behavior; it approves no architecture, dependency, deployment or implementation. Product details are in [Content Intelligence](data-products/content-intelligence.md) and [Application Reliability](data-products/application-reliability.md).

## Purpose and release boundary

Provide two inspectable data products: a catalog that explains which publisher articles were discovered, and request-event aggregates that explain recorded service failures and latency. Both must expose their input scope, quality exclusions, reference version and immutable publication identity. Operators must be able to reproduce an output and diagnose a failed update without losing the last usable publication.

The proposed release is local-first, batch-first and limited to registered RSS/Atom, two reference CSV schemas, request-event JSON Lines, and equivalent request-event Parquet. It extends the existing Java ingestion, Spark transformation, immutable Parquet and verified DuckDB consumer boundaries where compatible. A single deployment is assumed. No generic workflow designer, arbitrary SQL/code execution, multitenancy, document ingestion, scraping, article interpretation, engagement analytics, cloud or paid services are included. Streaming remains a later M5 decision.

The acceptance criteria for this documentation slice are explicit schemas and grains, unambiguous time/duplicate/reference semantics, bounded operational and access contracts, a reproducible early scale experiment, compatibility review, and passing documentation checks. No runtime evidence is produced by writing these specifications.

## Evidence and compatibility baseline

Repository review used the clean `docs/data-product-specifications` checkout at `edbe1083b23eb8cbc81564e10079a037cf9231e5`. Read-only GitHub inspection on October 19, 2026 confirmed [PR #35](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/pull/35) merged and [issue #34](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/issues/34) closed; issue #36 was open. PR #35 still lists unchecked acceptance items and had no conversation comments recording manual acceptance when inspected. Administrative closure is not evidence that those checks ran.

| Evidence class | What can be stated | What remains separate |
| --- | --- | --- |
| Merged implementation | M1/M2 ingestion, Silver, Gold and consumers; M6 publisher/daily/quality/article APIs and four-view frontend are in this checkout | Merge does not establish every acceptance criterion or production readiness |
| Recorded local validation | [M2 closeout](m2-closeout.md) reports 118 observations, 59 articles and three daily rows; [frontend evidence](../frontend/README.md#m65-validation-and-remaining-closeout) reports 184 frontend tests, retained-data client checks and unchanged Gold files | These historical runs are not rerun or independently certified by this documentation task |
| Unresolved acceptance | Frontend documentation and PR #35 identify remaining browser/manual review; final M6 closeout is pending | No inference of M6.5 acceptance or full M6 completion; hosted check results were not inspected in this task |
| Proposed expansion | Reference enrichment, reliability events, async management, authorization, exports and scale experiments below | No implementation or measurement is claimed |

The following accepted decisions remain authoritative:

| Contract | Compatibility requirement |
| --- | --- |
| [ADR 0001](adr/0001-rss-raw-ingestion-contract.md), [RSS retrieval](architecture/rss-ingestion.md) | Preserve bounded raw bytes, retrieval UUIDs and explicit failures; registration does not turn fetching into arbitrary URL access |
| [ADR 0002](adr/0002-bronze-object-storage.md), [Bronze storage](architecture/bronze-storage.md) | SeaweedFS is the accepted local Bronze store; preserve immutable input and single-writer, metadata-last semantics; no implied AWS approval |
| [ADR 0003](adr/0003-silver-observation-contract.md), [Silver](architecture/silver-processing.md) | Preserve source-scoped article hashes, repeated observations, nulls, warnings and rejections |
| [ADR 0004](adr/0004-gold-analytics-publication.md), [Gold v1](architecture/gold-contracts.md), [publication](architecture/gold-publication.md) | Do not add reference columns to v1, alter its fingerprint formula, replace observation metrics, or mutate completed publications |
| [ADR 0005](adr/0005-gold-consumer-integration.md), [consumer](architecture/duckdb-analytics.md) | Retain full Python verification per query through the bounded Spring adapter; the unverified JDBC POC is not an alternative production path |
| [Current APIs](architecture/analytics-api-contracts.md), [API operations](../services/content-api/README.md), [frontend](../frontend/README.md) | Existing operator-configured fingerprint, supported parameters, exact counters, UTC precision and fail-closed browser pin remain unchanged |

New products need new versioned contracts and publication namespaces after ADR approval. An enriched Content publication references one verified base Gold publication; it is not a rewritten Gold v1 snapshot. Reliability has its own event identities and cannot be inserted into the article schemas. Existing `/api/v1/` endpoints do not acquire new filters, reference joins, export operations or client-selected fingerprints through this proposal.

## Registration and captured configuration

Registration is an allowlist of reviewed adapter/profile combinations, not a plugin marketplace. Proposed IDs below describe contracts, not implemented endpoint names.

| Product/profile | Permitted input | Boundary |
| --- | --- | --- |
| Content / `rss-atom-v1` | Public RSS/Atom via existing catalog and bounded fetcher | Source IDs retain current syntax and identity; no linked-resource retrieval |
| Content / `publisher-reference-v1` | Exact publisher CSV schema | Immutable version, unique source key, complete-file validation |
| Reliability / `request-jsonl-v1` | Registered request-event batches | Exact event schema, explicit ingestion envelope and rejection accounting |
| Reliability / `request-parquet-v1` | Equivalent typed batches | Same logical event rules; no schema inference or union of incompatible files |
| Reliability / `service-reference-v1` | Exact service CSV schema | Immutable version, unique service key, complete-file validation |

A registered source has a stable `source_id`, product, adapter/profile and schema version, configuration revision, enabled state, storage-profile IDs, resource limits, reference-version selection and data-origin label (`publisher`, `synthetic`, or approved `operational`). The registry rejects unknown combinations and duplicate IDs before I/O. Format comes from the registered profile and validated content, not a filename alone. An identifier change is a new source, not an invisible rename. Updates create immutable configuration revisions; UI display labels can change without rewriting source identity.

Capture the resolved configuration when a run is accepted: source revisions, parser/processor versions, reference digests and vocabulary version, input-selection policy, time bounds, rejection policy, resource budgets, percentile settings and storage-profile revisions. Configuration edits affect future runs only. Record runtime versions and code revision for reproduction. Credentials are resolved outside manifests by controlled references; secret values and token-bearing URLs never enter configuration snapshots, logs or exports.

### Storage and retained-input inventory

Only an operator-approved storage profile may resolve input, staging, output and export locations. Initial candidates are the existing local SeaweedFS Bronze profile and dedicated local filesystem roots for analytical artifacts. Approval must record ownership, permissions, quotas, retention and teardown. There is no newly approved shared-worker, S3A, cloud or public-download profile. Client requests carry registered object/profile IDs, never filesystem paths, buckets, endpoints, credentials, glob patterns or executable locations. Resolve membership server-side; reject traversal, symlink escapes and unregistered destinations.

Each run freezes an explicit sorted inventory at the discovery boundary: product/profile/schema, ingestion identities, approved relative artifact IDs, sizes, SHA-256 digests, completion-marker digests, reference versions and resolved configuration digest. Content additionally retains its base Gold fingerprint and the base manifest's completed Silver inventory and Bronze provenance; Reliability records every event batch and reference artifact. New inputs discovered afterward wait for another run. Missing selected inputs fail; an integrity failure is not an empty dataset. Verify selected bytes and membership again before publication.

The proposed initial history policy is cumulative over an explicitly retained inventory, with no automatic sliding window or deletion. Extending history creates a reviewed new selection; removing inputs is an explicit new scope, not a silent retention side effect. Never union complete output snapshots as though they were disjoint input batches. Preserve required raw inputs, references, manifests and configuration for replay; if retention later removes a dependency, mark replay unavailable and keep the verified output's documented scope. Retention/garbage collection is a later decision and must respect active query/export pins.

New product fingerprints should cover product/contract/revision plus canonical input and semantic-configuration inventories, excluding run UUIDs, wall-clock start times, secrets and machine-specific root paths. Physical JSONL and Parquet inventories will have different fingerprints even when logical events match. Exact canonical bytes, compatibility vectors and completion schema require the publication ADR gate; the accepted Gold v1 fingerprint algorithm is not changed.

## Asynchronous execution and recovery

Proposed management requests validate authorization/configuration, durably record a run and return its opaque ID without holding an HTTP request open for ingestion and Spark processing. A fixed pipeline performs intake, input verification, normalization, product transformation, output read-back verification and completion. There is no user-defined DAG, shell command or uploaded executable. Read-only analytical queries keep their current bounded query path; they do not run ingestion.

Run states are `QUEUED`, `RUNNING`, `PAUSE_REQUESTED`, `PAUSED`, `CANCEL_REQUESTED`, `SUCCEEDED`, `FAILED`, `CANCELLED`, and `RECOVERY_REQUIRED`. The current stage is recorded separately. `SUCCEEDED` means a verified completed publication exists, including a verified identical replay; it does not mean a new consumer selection was activated. A run with allowed record rejections reports them prominently alongside success.

| Control or failure | Required proposed behavior |
| --- | --- |
| Enable/disable source | Enable permits new work. Disable prevents new dispatch, including queued runs using that source; those terminate with `SOURCE_DISABLED`. Already running work keeps its captured configuration unless explicitly paused/cancelled. Historical data stays queryable |
| Pause | A queued run pauses immediately. Running work records `PAUSE_REQUESTED` and stops only after the current durable stage boundary; a single long fetch/Spark action is not instantly suspended. No promise of sub-stage resume |
| Resume | Revalidate checkpoint inventory and ownership, then continue the same captured run. Missing/changed dependencies enter recovery or fail; do not substitute newer configuration |
| Cancel | Remove queued work or request bounded termination of owned running work. After cleanup, retain diagnostics and incomplete output without completion evidence. Cancellation does not undo an already verified publication |
| Cancel/completion race | Serialize finalization under the run owner. If completion won, record success and a too-late cancellation; if cancellation won, no completion marker may be published. Uncertain ownership/outcome enters recovery |
| Retry | New attempt linked to the original run, with the same frozen input/configuration and idempotency identity. Reconcile complete artifacts first; matching output is verified and reused. A changed reference/input selection is a new run |
| Lost retrieval before durable receipt | Do not reconstruct the old feed response from a new fetch. Reuse an intact retained raw receipt if available; otherwise fail that attempt and require a new retrieval identity/run |
| Process crash/restart | Recover persisted stage/attempt state and verify artifacts. Do not infer success from exit code, file existence or a missing heartbeat alone. Establish the previous writer is stopped before restart; unresolved ownership blocks writes |
| Incomplete/conflicting output | Preserve it for investigation. No silent overwrite, deletion or marker repair. Operator-authorized quarantine or a fresh approved staging location follows the existing recovery discipline |
| Failed update | Keep the last selected verified publication and show its age plus the failed attempt. Never serve partial new data; no previous verified publication means unavailable |

History must survive application restart: run/attempt IDs, requester, trigger and idempotency key, captured revisions, inventory, stage transitions/times, counts and rejection codes, checkpoints, sanitized failure code, publication result and control acknowledgements. Repeated submission with the same product/principal/idempotency key and identical payload returns the same run; a changed payload conflicts. A durable state store, atomic state transitions and writer ownership protocol are prerequisites, not implemented features. A history-store failure must prevent acknowledging newly accepted work or promotion; monitoring reports it independently.

The initial worker is one trusted local dispatcher with one writer per product output root. A durable queue must have admission limits; overload returns an explicit busy outcome rather than an unbounded backlog. Proposed starting budgets are one active publication run per product, two globally and 20 queued runs globally, adjustable only by reviewed configuration after measurement. CPU/RAM/disk limits remain subject to hardware preflight. No transactional exactly-once, multiwriter fencing or fsync guarantee is inherited from today's CLI. A restart/recovery ADR must establish those guarantees that the implementation actually needs.

## Publication selection, availability and freshness

**Current behavior remains operator-configured `ANALYTICS_PUBLICATION`.** No automatic latest lookup, mutable Gold pointer or browser-selected filesystem location is introduced. An explicit per-product operator configuration is the first proposed extension: Content and Reliability independently name verified immutable publications. Changing a selection is a management action, not part of an ordinary query.

A future product registry/selection API requires a separate ADR and owner approval. If adopted, it must verify a candidate before activation, durably record old/new selections and actor, and change only that product's selection. Completion and activation are separate outcomes; failure to activate leaves the previous selection intact. Rollback is an explicit audited selection of a still-verified retained publication. No automatic fallback to an arbitrary older or newer publication is allowed.

Every query/page/export resolves and pins `(product_id, contract_version, publication_id)` at its start. Multi-request consumers must retain that identity, cancel obsolete requests and reject mismatches; a user explicitly reloads to adopt another publication. If historical pinning is later supported, only authorized IDs in the product registry are eligible. Content and Reliability may have different identities and freshness; there is no cross-product transaction. Display publication completion time, input coverage, last observation/event time, last attempted update and failure separately. Completion time is processing freshness, not proof of source completeness. Corruption of the selected publication makes queries unavailable even if an earlier success was recorded.

Proposed new reports start with 100 rows by default and 1,000 maximum; metadata-heavy catalogs use 10 by default. A 30-second execution deadline and 1 MiB serialized-response budget fail explicitly rather than returning a truncated success. Share a two-process interactive-query budget across products initially, preserving the current adapter's bounded behavior; publication workers and exports have separate admission/resource budgets. Server-side pagination must return actual limit/offset/has-more metadata, with offsets capped at 1,000,000 unless a separately reviewed cursor contract replaces them. These are limits on output/admission, not on full-publication verification cost. All new API schemas and bounds require review; existing endpoint defaults remain unchanged.

## Authorization, exports and audit

These are future backend requirements. The present loopback-only API has no authentication; it must not be described as enforcing these permissions. Choose the authentication/identity-provider integration and policy persistence before exposing new management/export/query surfaces to other users. A local operator CLI can exercise early data/benchmark slices without inventing a public authorization service.

| Product-scoped capability | Enforcement boundary |
| --- | --- |
| `query` | Every analytical request and publication-metadata read; reject IDs outside the authorized product |
| `manage` | Register/revise sources and profiles, start/pause/resume/cancel/retry runs, inspect restricted run history and explicitly activate a publication |
| `export` | Create an export and view its status; requires query permission for the same product/publication |
| `download` | Every artifact download, including completed exports; require current product query/export permission and artifact membership. Knowledge of an artifact ID is not authorization |

Permissions are explicit; management does not automatically grant export/download. Deny by default. Server-side checks apply to direct API calls, not just hidden UI buttons. Reject unauthorized enumeration and arbitrary storage addressing; never expose raw Bronze/quarantine/internal provenance through a catalog download. Backend-managed short-lived download access, revocation and authentication failure behavior need tests. Identity provider, role vocabulary and policy engine remain decision gates; no enterprise/multitenant IAM is implied.

Exports are on-demand background jobs over one verified product publication and one validated filter/projection, not a concatenation of the currently loaded browser pages. Initial proposed ceilings: 100,000 rows, 50 MiB encoded output, five minutes execution, one active export per principal and two globally, with 24-hour artifact expiry. The first exceeded bound fails the whole export with a safe limit error; no partial success or silent truncation. These are starting admission limits to validate, not capacity claims. Export jobs have separate bounded resources so they cannot consume all interactive query slots.

Use fixed schemas: JSONL initially for catalogs/events and CSV for small aggregate tables, with an accompanying provenance manifest. Record product/contract/publication, reference versions, filters/order/projection, row count, byte length, artifact hash, generation time, data-origin label and expiry. Authorize on submission, execution and download, including after revocation. Store only under an approved export profile; complete and verify artifact/manifest before making either downloadable. Retain the pinned publication while an export is running. Cleanup removes only expired temporary exports under an approved retention policy, never their input publications.

JSONL preserves text and exact decimal-string integers. CSV must use fixed quoting/encoding/null rules and neutralize formula-leading untrusted cells, documenting any presentation escaping in its manifest; the underlying product is unchanged. Prefer JSONL when exact untrusted strings or null/empty distinctions matter. Product ownership/usage rights can prohibit exports even when technical access exists; public RSS availability is not redistribution permission.

Platform monitoring is independent of the Reliability product's request-event datasets. A broken ingestion pipeline must still be observable. Record run queue depth, stage durations, liveness versus query readiness, failures/retries, rejected/duplicate/conflicting inputs, publication age, disk budgets, query capacity/timeouts and export failures using bounded labels. Do not use article IDs, event IDs, URLs or arbitrary exception messages as metric labels. Persist audit events for registration/configuration changes, run controls, selection changes, export/download decisions and authorization denials, with UTC time, actor, product, target ID, correlation/run ID and safe outcome code. Exclude payloads and secrets. Define access, retention and failure alerts for this independent operational channel before selecting an observability stack; no tamper-proof audit guarantee is claimed.

## Benchmark and failure-evidence plan

This is an experiment plan, not a result. The [Reliability contract](data-products/application-reliability.md#percentile-accuracy-and-validation) defines the mathematical oracle and tolerance. Content uses small fictional RSS/Atom and reference fixtures; live feeds and the retained M2 dataset are not scale generators or automated test dependencies.

The scoped [ADR 0009 proposal](adr/0009-reliability-format-equivalence-benchmarks.md) and [format/benchmark contract](architecture/reliability-format-benchmarks.md) refine E3/E4a without accepting this wider plan or authorizing workload execution.

### Workloads and independent expectations

| Tier | Dataset and acceptance purpose |
| --- | --- |
| Small deterministic fixtures | Hand-authored events spanning UTC boundaries, repeated durations, missing references, null/invalid fields, duplicate/conflict cases, empty batches and late arrivals; exact expected keys/counts/sums and sorted p95 references |
| Baseline | 100,000 distinct valid events, plus 2,000 identical duplicate occurrences and 1,000 representable invalid rows: 103,000 physical rows; measure end-to-end processing before administrative features |
| Target after preflight | 1,000,000 distinct valid events, plus 20,000 duplicate occurrences and 10,000 representable invalid rows: 1,030,000 physical rows; same contract, distributions and correctness gates |

Version the generator, seed `20261019`, canonical logical dataset checksum and expected counts. Use a specified deterministic algorithm with golden vectors, not runtime-default random seeds: hash UTF-8 `seed/row-index/field-name` with SHA-256, interpret the first eight bytes unsigned big-endian and map by documented integer modulo. Derive stable UUID-shaped event IDs from a separate indexed namespace; never use wall-clock UUIDs/times. Record collisions as generator failures. Persist generated data under ignored local outputs, never Git.

Proposed generator-v1 distribution: ten services, three environments, 20 route IDs per service and a fixed 24-hour UTC range beginning `2026-10-01T00:00:00.000000Z`. Assign 60% of valid events to one service using `index mod 10 < 6`; spread the rest deterministically over nine services. Map 90%/5%/5% to 2xx/4xx/5xx using `index mod 100`, and duration bands of 80% at 1–100 ms, 19% at 101–1,000 ms, 1% at 1,001–60,000 ms using a separate field hash with recorded actual proportions. Hash event times/methods/routes independently; include an additional hot-window/skew variant and an all-equal-duration variant. Do not assert independent distributions when modular choices correlate.

Select duplicate indexes deterministically; reuse every semantic event field, vary only ingestion provenance. Select invalid additions evenly across null required time, out-of-range status, negative duration, invalid environment and missing event ID. These variants are representable in both typed formats. Omit one service-reference key to exercise retained unmatched events. Mark 10% of valid events for delivery in a second batch while preserving earlier event times, to test a changed publication. Conflicting duplicates, malformed JSON syntax, invalid UTF-8 and damaged Parquet are separate failure workloads; a failed job is not counted as successful throughput.

Generate JSONL and Parquet from the same logical records, including identical duplicate occurrences and typed-invalid values. Validate equivalent logical row multisets, not byte equality, input filenames or Parquet physical order. Pair a canonical batch/time/provenance manifest for comparisons and record each format's physical hashes separately. Malformed syntax has no equivalent typed Parquet row; report those parser tests separately. Pin JSONL encoding, Parquet compression/row-group settings, file counts/partition sizes and output settings. Do not hide JSONL-to-Parquet conversion inside one format's timing; record preparation separately.

Build an independent standard-library reference checker, separate from Spark expressions and the generator: validate records, canonicalize identities, collapse duplicates, compute exact group counts/5xx counts/integer duration sums, and sort durations per group for rank checks. Store small expected outputs under review. At the target, process groups or use bounded local external sorting if necessary; never remove the oracle because Spark output looks plausible. Expected accepted/duplicate/rejected totals are N, 0.02N and 0.01N; every grouping key and metric must reconcile. No timing passes acceptance if correctness or verification fails.

### Execution conditions and measurement

Before any target run, record OS/architecture, physical/logical cores, RAM, free disk, filesystem/storage throughput, Java/Python/Spark/DuckDB versions, executor/driver limits, thread/worker counts, partitions, shuffle settings and percentile accuracy. Estimate simultaneous raw, normalized, output, oracle and spill space using the baseline. Require at least twice the estimated peak disk requirement free and a configured process-memory budget leaving at least 25% host RAM for the OS/other applications. Record the actual numeric budgets and stop on their breach. If preflight fails, retain the 100,000-event baseline and report the target blocked by resources; do not install runtimes or provision workers implicitly.

Measure separately: input registration/hash inventory, parsing/validation/quarantine, deduplication, reference join, aggregation, Parquet write, read-back/full verification and completion; also total wall-clock time from fixed-input acceptance through verified publication. Report physical input rows/s and accepted unique events/s with their denominators, CPU time, peak driver/Python/executor RSS and heap/GC where available, shuffle read/write, memory/disk spill, input/output/quarantine bytes, compression and file counts. Mark unavailable instrumentation explicitly. Report ingestion/network work separately from fixed-file processing and dependency/generator setup separately from both.

For consumers, measure fixed representative verified queries: one service/window, full aggregate overview, first and later catalog/event pages, and bounded export. Include verification and process startup in user-visible latency; optionally report query-only time separately. Use a fresh consumer process for cold-process trials, then repeat with warm filesystem cache. Run one warm-up and at least five measured repetitions per format/topology/configuration; alternate JSONL/Parquet order. Record all samples, median and range. For a latency distribution claim, collect at least 30 query samples and disclose sample count; five trials do not establish a stable p95 latency. Do not claim cold OS caches unless actually controlled; otherwise label them unknown/warm. Identical replay is a separate verification workload, not a fresh-write throughput trial.

Fresh-write repetitions use isolated approved output roots with the same frozen inputs and settings; do not benchmark the existing publication's `SKIPPED` path as a new write or delete retained output to force work. Budget the resulting disk copies in preflight. Early E3/E4 consumer measurements use the existing verified CLI or a narrowly scoped reviewed product report, including direct artifact serialization where an HTTP/export interface is not yet implemented. Repeat HTTP and authorization/export-job latency checks after E6; never label CLI timing as browser/API performance or delay the first scale experiment for those interfaces.

First compare `local[1]`, `local[2]` and a bounded higher thread count on the same machine. These are one-JVM local-thread results, not distributed Spark evidence. A separate multi-worker trial requires approved runtime/storage design, evidence of tasks on at least two worker processes, recorded worker hosts/resources and accessible immutable input/output locations. Same-host worker processes demonstrate coordination only, not multi-host scaling. Keep total CPU/RAM comparable when testing speedup and publish differences when not comparable.

Workers must read the exact inventoried bytes from approved shared storage or verified identical read-only mounts; a driver's private path or `127.0.0.1` service is not automatically accessible on other hosts. Stage/verify worker visibility before timing. One designated publication coordinator remains the sole writer/finalizer for each product root; distributed tasks writing partition files do not authorize concurrent publication jobs. Test worker loss and coordinator restart before claiming recovery. No predetermined speedup, linear scaling, production-scale readiness or AWS equivalence is assumed.

Failure/replay evidence must include: identical input replay with unchanged complete files; conflicting duplicate abort; missing/corrupt reference and input bytes; schema mismatch; worker/process death before and after data writes; disk/output-limit exhaustion; pause/cancel at each durable boundary; completion-versus-cancel race; restart after completion but before history acknowledgement; consumer corruption rejection; late events creating a new publication; and continued serving of the prior selected publication after an update failure. An approximate p95 may vary within its validated rank tolerance across execution layouts; exact counts and sums may not.

## Delivery slices and decision gates

These `E` labels are proposal-local planning labels, not new milestones or replacements for M0–M9. Existing M6 browser acceptance, review and closeout remain their own track. Do not make enrichment, authorization or benchmarking retroactive M6 acceptance requirements, or use this plan to mark M6 complete.

| Slice and dependency | User value | Independently reviewable engineering evidence |
| --- | --- | --- |
| E0 — Review contracts and ADR gates | Shared meanings before new data is exposed | Owner review of these Proposed specifications; small expected-result examples and version boundaries; no application changes |
| E1a — Publisher reference validation | Identify publisher context without losing unmatched articles | CSV schema/duplicate-key tests; version/hash capture; no changes to base Gold |
| E1b — Enriched catalog and discovery | Explore source context and first discoveries | Left-join cardinality, retained inventory, discovery reconciliation and changed-reference/new-publication tests; operator-local execution |
| E2a — Reliability JSONL/reference normalization | Inspect accepted and rejected request events | Explicit schema, provenance, duplicate/conflict and missing-reference fixtures; privacy field rejection |
| E2b — Reliability aggregates and verified publication | Compare failures and latency by service/window | Independent counts/mean/p95 checks, late-event snapshots, publication corruption/replay tests; fixed local commands |
| E3 — Parquet equivalence and 100,000-event baseline, after E2b | Know correctness and resource cost before building controls | Logical multiset parity, exact reference oracle, stage/RSS/spill evidence, local-thread comparisons; no management UI required |
| E4a — 1,000,000-event target after E3 preflight | Establish a bounded local workload envelope | Recorded hardware budget, repeated measurements, failure/replay and consumer latency; stop/revise scope if preflight fails |
| E4b — Multi-worker experiment after storage/runtime ADR | Determine whether worker distribution is useful | Worker task placement and shared-storage verification, equal-resource comparison, coordinator/worker-loss evidence; separately labeled from E4a |
| E5a — Durable fixed run executor | Start work and inspect outcomes without holding a terminal | Metadata-store/orchestration ADR; persisted idempotent submissions and stage history; restart recovery and single-writer tests |
| E5b — Source and run controls | Enable, disable, pause, cancel and retry with clear outcomes | Captured-version isolation, admission bounds, control-race tests; minimal controls over fixed pipelines |
| E6a — Backend authorization and product selection | Restrict operations and choose a verified product scope | Identity/policy and selection ADRs; deny-by-default query/management tests, cross-product denial and activation-failure tests |
| E6b — Bounded exports and downloads | Reuse a reproducible filtered result | Separate budgets, provenance, spreadsheet-safe CSV, revocation/download authorization and expiry tests |
| E7 — Minimal product UI and operational closeout | Inspect products, run history and failure/freshness together | Desktop/keyboard/error review, permissions tested through direct APIs, independent monitoring/audit and recovery demonstration |

E1 and E2 can be delivered independently after their contract decisions. E3/E4 deliberately precede the administrative feature sequence and inform its budgets. Monitoring/log evidence starts with the first executable slice; E7 assembles the operational experience rather than postponing instrumentation. E5 is initially operator-local; E6 authorization is mandatory before management/export features are exposed through a shared web interface. Streaming, scheduled automation and generalized orchestration are not prerequisites for these batch experiments.

| Future ADR gate | Alternatives and tradeoff to evaluate before implementation |
| --- | --- |
| New product contracts/reference snapshots | Separate derived publications versus a Gold version upgrade; favor preserving v1, but review manifest dependencies, reference corrections and replay costs |
| Reliability percentile/precision | Existing Spark approximate aggregate versus exact order statistics; establish rank tolerance, memory cost and reproducibility without promising an engine guarantee |
| Product selection and publication registry | Explicit operator mapping versus a managed registry/promotion protocol; any change to operator-configured fingerprint selection needs approval and a compatibility path |
| Orchestration and durable metadata | Small fixed executor plus durable store versus established orchestration tooling; compare recovery burden, atomic transitions and operational cost. SQLite/PostgreSQL or an orchestrator are not selected here |
| Worker runtime/storage | Local threads versus multiple worker processes/hosts and shared mounts versus object-storage connectors; demonstrate visibility and preserve one finalizer before distributed claims |
| Identity and authorization | Local authenticated operator mechanism versus an external identity provider; choose principal validation, policy persistence, revocation and download enforcement before exposure |
| Exports/retention | Bounded materialization versus streaming delivery; quotas, provenance, safe text, expiry, usage restrictions and pinned-input retention |
| Monitoring/audit and later streaming | Built-in structured signals versus a dedicated stack; independence from product pipelines, retention and access first. Broker, watermark and checkpoint choices remain future M5 work |

Accepted ADRs 0001–0005 are not edited by this specification. If a later decision substantially changes one, create a reviewed superseding ADR and migration/rollback evidence. Do not silently reinterpret historical wording or treat these proposed limits as owner-approved architecture.

## Open risks and review questions

The owner must review the proposed event duration/range limits, reference vocabularies, input retention, percentile tolerance and export budgets. Reference metadata can be stale or incomplete; enrichment coverage must stay visible. RSS discovery is limited to retained observations, not complete publisher history. Request logs can omit failed or unrecorded requests, so the Reliability product is not an uptime or SLO guarantee. Physical fingerprints and approximate percentiles constrain claims of bit-identical regeneration. Full verification, Spark shuffles and exported metadata can dominate cost; the early experiment must inform limits. Durable recovery, product activation and backend access control are new responsibilities whose storage/tool choices remain unresolved.

Release acceptance requires product fixtures, unchanged M1/M2/M6 regressions, reviewed new ADRs, measured bounds, corruption/failure tests, independent monitoring, access tests and reproducible local teardown. Those are future execution requirements. This documentation task runs only formatting, local-link, hygiene and whitespace checks and leaves all new specifications Proposed.
