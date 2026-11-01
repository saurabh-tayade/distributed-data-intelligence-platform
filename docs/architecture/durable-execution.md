# Durable execution and source controls

Status: **Accepted — October 20, 2026**, with the Stage A.1 refinements, under [ADR 0010](../adr/0010-durable-execution-coordination-and-recovery.md). The owner accepted additive [ADR 0011](../adr/0011-artifact-import-maintenance-ownership.md) on October 21. Architecture acceptance alone passes no E5 test group. Current implementation and authorization are recorded below; earlier checkpoint status paragraphs and counts remain historical. Accepted ADRs and native data contracts are unchanged.

Previous checkpoints established D1–D5a publication/finalization, D5b-1 reconciliation and [D5b-2a explicit RESUME](#d5b-2a-explicit-resume-of-recovered-content). The current [D5b-2b inherited recovery increment](../operations/d5b2b-inherited-recovery-validation.md) reconciles interrupted continuations through their recorded original prefixes. Managed Content and Execution Recovery remain **Partially implemented**. Ordinary Content worker dispatch is blocked by the absence of a proved aggregate recovery-evidence envelope; cancellation execution, guarded retry, automatic recovery/resume and Operations UI remain deferred. [ADR 0020](../adr/0020-inherited-content-reconciliation.md) is pending owner acceptance; earlier ADR statuses and historical checkpoint/test records are unchanged. The fixed Reliability worker and C3c recovery retain their existing paths.

## Boundary and invariants

Gate B0's [completion investigation](../operations/content-recovery-envelope-completion.md)
distinguishes native data from private sidecars and documents an additional small-workload
interruption gap: a durable publisher intent can precede creation of its required command.
The current 64 MiB / 4096-entry capture, 1 MiB inventory/packet limits and per-file readers
are not a pre-write execution envelope. No approved operational Content budget profile is
implemented. [ADR 0021](../adr/0021-content-evidence-producer-reservations.md) proposes narrow
versioned producer reservations and recoverable sidecar persistence; it is not accepted or
implemented. Existing dispatch gating, native identities and earlier ADR statuses are unchanged.

E5 manages one local installation, one trusted operator and one worker. PostgreSQL is the operational authority; existing files/object receipts and independent verifiers remain the data authority. The API never performs processing in an HTTP request or treats a database artifact row as verification. There is no DB/filesystem/S3 distributed transaction. No exactly-once execution, automatic failover, multiwriter isolation, hostile-filesystem protection or artifact power-loss durability is claimed.

The following are accepted Stage B requirements, not blanket claims of implemented capabilities:

1. A submission/control is acknowledged only after its transaction commits. Lost acknowledgments are retried with the same key and full normalized request, not guessed from an HTTP error.
2. At most one managed execution/reconciliation/import is active globally. Metadata fencing and host ownership are separate requirements. Expired heartbeats do not free the slot.
3. Source/configuration selections are immutable per run. Explicit dependency inventories are frozen before their processing stage. No latest lookup, directory expansion, implicit source discovery or new fetch on retry.
4. A stage intent commits before its child can create product effects. Completion requires full verification, not stdout, exit status, filename existence or a DB success flag.
5. Completed artifacts and their original fingerprints/metadata remain unchanged. Existing incomplete analytical namespaces are never overwritten, resumed, repaired or deleted. Private new staging is permitted only under a fresh attempt namespace with retained earlier evidence.
6. Retry/reconciliation first establish old-writer quiescence. A completed result wins over an uncertain control acknowledgment; partial output never becomes a successful product.
7. Management never activates a publication. Existing ANALYTICS_PUBLICATION and /api/v1/ Gold analytics behavior remain unchanged. Previous results remain selected after all run outcomes.

The Proposed platform plan's two globally active runs are narrowed to one. Its stronger completion-versus-cancel guarantee cannot be provided by current standalone publishers; the explicit request/outcome semantics below are proposed instead. Stage A.1 removes pause/resume from mandatory E5 scope and defers it entirely: no corresponding states, control endpoints/enums, worker behavior or UI are required. Its generic operational source/export permissions are not implemented by E5. Accepted ADRs 0001–0009 are unchanged.

## Components, roles and deployment

~~~text
React Operations -> loopback management API -> PostgreSQL
                                               ^
host worker-only Spring Boot process ----------+
  -> fixed stage supervisor -> Java RSS adapter / pinned Python adapter
  -> existing loopback SeaweedFS + immutable local roots
  -> existing full independent verifiers -> PostgreSQL result/audit
Existing React analytics -> existing /api/v1/ -> existing query supervisor
~~~

Use the existing services/content-api codebase with explicit roles api, worker, migrate and operator-import. These are fixed application entry modes, not submitted job types or dynamically loaded components. Legacy analytics-only mode keeps management disabled and requires no DB. With management enabled, API startup validates compatible schema/runtime configuration before exposing management readiness. Worker mode uses a non-web Spring application, no controllers/query pools and no embedded HTTP listener. Boot/JDBC/Flyway wiring must be conditional so adding JDBC does not break legacy startup or existing tests without a DataSource.

Recommend PostgreSQL 17 in Docker Compose, on 127.0.0.1:5432, with its own persistent named volume. Keep existing pinned SeaweedFS, credentials, bucket and volume unchanged. Exact maintained PostgreSQL patch/image digest and Boot 4.1.1-compatible JDBC/Flyway artifacts must be pinned in B. Proposed dependencies are Spring JDBC, PostgreSQL JDBC, Flyway core and its PostgreSQL database support. No JPA, message broker or generic scheduler is required. Stage A installs none of them.

Launch the API and worker on the host with the pinned JDK/Python environment. Existing Java LocalS3 and Python bronze_to_silver.local_client reject non-loopback endpoints; container DNS seaweedfs and host.docker.internal cannot be substituted. Container-local localhost also cannot reach the host's published endpoint. Do not add host networking, relax those factories or add S3A to work around this. An eventual container-worker deployment needs a separately approved endpoint/storage/process-containment contract and tests. PostgreSQL/SeaweedFS containers are infrastructure services, not additional processing workers.

Build/package in an explicit setup step; jobs never invoke scripts/run-batch.sh, Maven, npm, pip or shell command text. Worker resolves prebuilt ingestion classes/JAR, exact Python executable and fixed adapter scripts from an immutable runtime profile. Capture build revision, actual version tuple, profile digest and worker instance in attempts. A changed deployment version cannot silently retry an old run; allow read-only reconciliation through a compatible verifier or require the original runtime/new run. Do not insert run IDs or runtime operational settings into product fingerprints.

### Proposed startup and shutdown responsibilities

| Step | Responsibility |
| --- | --- |
| Configure | Operator supplies ignored local DB credentials, approved feed endpoint catalog, immutable root/runtime profile bindings and required existing BRONZE_S3 settings; never upload secrets through management |
| Start services | Explicit Compose startup of PostgreSQL and SeaweedFS; authenticated readiness, existing bucket setup and volumes verified; no automatic bucket/database replacement or deletion |
| Migrate | Explicit migrate role, worker stopped; versioned SQL/checksums through Flyway, bounded migration lock wait; forward migrations only, no automatic rollback/repair or ORM schema updates |
| Start API | Loopback-only, schema/profile compatibility validated, fixed mutation protections; management readiness distinct from liveness and existing analytical availability |
| Start worker | Acquire installation host guard, validate root identities/permissions/runtime/profile and DB slot; register instance; enter READY only if no unresolved previous owner. Otherwise RECOVERY_ONLY with no processing launch |
| Stop | Stop admissions/dispatch, request controlled child termination, reap/verify, checkpoint clean outcome or retain RECOVERY_REQUIRED. Release guard only after cleanup; shutdown failure cannot report clean recovery |
| Manual retention/recovery | Retain persistent volumes and restricted journals; document operator inspection/retention without automatic deletion. If the operator restores copies, keep dispatch disabled until installation/profile/artifact bindings and nonterminal claims are verified; backup/restore automation and an untested restore guarantee are out of scope |

Stage B should provide reproducible checked commands for these roles, using the packaged Spring Boot JAR and an explicit operator configuration file. The exact Maven artifact path is a packaging detail to settle in B; no nonexistent command is represented as runnable Stage A evidence. Compose down must retain named volumes; no routine command uses down -v. No publisher request or existing retained dataset is part of setup/CI.

## Source, profile and dependency selection

### Persistent source model

Stable source_id syntax is the existing `[a-z0-9][a-z0-9_-]{0,127}`; identifiers are never renamed or recycled. Key sources by (product_id, source_id); product is content-intelligence or application-reliability. A configuration change creates an increasing immutable revision and changes the current pointer under optimistic version control. Old revisions, captured runs and artifacts remain available. Display labels are non-sensitive text of at most 128 UTF-8 bytes; rendered as text, never HTML.

Each revision's exact configuration object has configVersion=1, profile, dataOrigin, feedEndpointId, registeredProfileSha256 and inputStorageProfileId; inapplicable fields are explicitly null. RSS uses profile=rss-atom-v1, origin=publisher, a reviewed endpoint ID, null registeredProfileSha256 and a Bronze storage-profile ID. Reliability uses request-jsonl-v2 or request-parquet-v1, origin=synthetic, null feedEndpointId, an exact reviewed registered-profile digest and a v2 batch storage-profile ID. The source ID must match underlying registered batch/receipt provenance. Unknown keys/combinations fail. There is no operational/real-log Reliability intake in E5.

Endpoint IDs resolve through a reviewed server-side catalog to public HTTP(S) feed URIs satisfying SourceCatalog/RssSource policy, including no query, credentials or fragment. Requests cannot introduce arbitrary URLs; adding a new reviewed endpoint is an operator configuration action. No redirects, linked-content fetching or new application retries are added. Only isolated test profiles permit synthetic loopback feed URLs. This policy avoids presenting the current fetcher as a general SSRF-safe URL service.

enabled is a live source dispatch gate with its own row version/audit history. It is not a product semantic field or mutable edit to an old revision. Capture its observed version/value at submission. Disable blocks queued dispatch for selected delivery sources; active work uses captured revisions until explicitly cancelled. Historical Silver/raw dependencies and already completed products are not deleted or silently excluded by disablement. A retained artifact's source provenance does not itself authorize a new fetch. Retry checks current enablement for the originally selected delivery sources, but never replaces their captured configuration with the new current revision.

### Storage/runtime profiles and artifacts

Profile IDs identify immutable server-reviewed revisions, not paths supplied by requests. A storage binding records role, product, canonical local root or local Bronze bucket/endpoint reference, ownership and expected root identity, permitted versions and retention policy. Paths/secrets are private operator configuration. Restore/remount changes require explicit binding reconciliation; do not silently trust a root replaced at the same path. Runtime profiles freeze exact commands, adapters, execution configuration, environment whitelist, deadlines and cooperative resource budgets. They cannot override product semantics or increase accepted parser/verification limits.

An artifact ID maps to one fully verified completed registered batch/reference/Silver/Gold/Content/Reliability binding, its contract/processor versions, native identity, exact completion/input evidence and approved relative address. Registration does not authenticate source truth. Required raw/Gold/reference dependencies must remain retained; loss makes a future run/replay unavailable. No degraded verification mode is introduced.

Completed existing inputs can be imported only through an explicit operator-import mode with a reviewed local catalog of exact identities/relative addresses, one host guard and the same coordinator maintenance slot. Stop the worker before this mode can acquire its lifetime guard; import refuses an unresolved occupied slot. It calls the correct full verifier, stores the observed evidence, never scans for latest and never mutates the imported artifact. Limit import to completed v2 Reliability batches, publisher/service references and explicit completed Silver history needed for Content. Gold/derived output records are produced by managed execution, not a generic artifact registry API. No HTTP path/upload/reference-authoring interface is included. Existing v1 runs/readers remain usable outside E5; E5 does not migrate them.

### Qualification of imported completed Silver

Original Silver completion is evidence of the existing publication protocol, not a complete content-hash inventory. An imported marker alone therefore cannot qualify a managed input. Within the existing artifact/evidence records, the operator-import adapter must:

1. Resolve only the explicitly selected completed Silver namespace beneath its approved root. Validate the root/namespace identity, original path layout and source/ingestion binding using the existing Silver selection/marker validation; reject path aliases, missing completion, incomplete state and incompatible versions. Never infer or invent an ingestion UUID.
2. Retain the exact original completion bytes, their byte length and SHA-256, plus parsed original identity/count/provenance fields. Capture complete namespace/directory membership and a file inventory of the marker, every selected accepted/rejected Parquet file and permitted bookkeeping, with approved relative path, size, SHA-256 and filesystem identity/metadata for each. Distinguish bookkeeping from analytical files under the existing layout policy; it is still captured/rechecked as managed evidence. Do not use a Parquet glob, alter the marker or treat its counts as an integrity proof.
3. Call the existing silver_inputs.load_inputs/prepare boundary with the exact marker selection. It verifies every selected physical schema, required values, accepted/rejected keys, replay/cardinality and source/ingestion/Bronze provenance against the original marker. Bind that verified selection to the independent file capture and original completion evidence. No E5 schema coercion, marker upgrade or weaker metadata-only alternative is permitted.
4. Recheck marker bytes, full inventory, file hashes/identities and directory/root guards after all validation actions, then commit the artifact/evidence registration. The capture provides a managed baseline from import time, not proof that the original data was never altered before import or that an author is authentic.
5. Reopen and compare that exact managed baseline before downstream CHECK_INPUTS/PREPARE_GOLD and recheck it after downstream actions, alongside the existing verifier's unchanged-input boundary. Retain the exact marker set and resulting Gold input manifest; later files or same-path replacements cannot silently enter selection. Missing input is unavailable, malformed/corrupt evidence is invalid, and demonstrated post-capture identity/metadata/byte/inventory changes are changed evidence. Ordinary I/O prevents verification without itself proving mutation. A broad native unchanged-check failure is not independent proof of mutation: retain its native code, use managed capture evidence for management classification and always fail closed without changing the legacy helper.

Any failed qualification/revalidation aborts import or downstream processing without a successful result or repair. Preserve original Silver namespaces, completion markers, schemas and replay behavior. This strengthens managed evidence capture, not the historical Silver data contract, and adds no new public endpoint, data format or persistence table.

### Freeze points

Submission captures the exact source revisions, reference artifact binding, input artifact IDs, execution/storage profile revisions and normalized submission bytes. Full file/Spark verification is deferred to the worker; synchronous validation only proves registered membership/version and syntactic bounds, not intact data. Dispatch reopens/rechecks every selected dependency. New arrivals after submission are never included.

RSS submission explicitly names retained Silver history and the sources to fetch once. After successful journaled fetch/Bronze verification, record exact resulting Bronze receipts. Freeze the Gold input as the union of those resulting completed Silver ingestions and the submitted retained Silver markers, deduplicated by the existing Silver selection rules. Persist that exact inventory/manifest before Gold publish. Never discover all files under a source or union complete Gold outputs as new observations. Gold's original Silver/retained-history provenance and Content's exact Gold completion evidence remain authoritative.

Reliability submission selects completed v2 batch IDs of one homogeneous format and a service-reference artifact. Capture exact batch manifest/completion/profile evidence; use verify_dependencies and publish unchanged. Registration/staging/record positions, identity conflicts, 20/21/17 columns and rank-qualified p95 remain ADR 0009 semantics. E5 does not generate or register raw data implicitly.

## Fixed stage adapters and machine boundary

| Pipeline / durable stage | Operation and reuse | Checkpoint / recovery proof |
| --- | --- | --- |
| CONTENT_RSS_V1 / CHECK_INPUTS | Verify source/profile/ref and submitted retained Silver inventory | Captured reference/selection evidence; no network discovery |
| FETCH_SOURCE, one source at a time in UTF-8 source-ID order | Existing HttpFeedFetcher -> original RawFeed; private journal before any S3 PUT | Complete original spool + UUID/instant/expected metadata/keys; stage is not complete until journal descriptor commits |
| STORE_BRONZE, per journaled source | Existing S3BronzeStorage.write with unchanged source/RawFeed; independent GET/read-back | Both original objects verified, exact receipt and metadata digest retained |
| SILVER, per exact resulting Bronze key | bronze_to_silver.process_one or run with only explicit keys; existing XML validation/normalization and output | Exact Silver marker/files and provenance validated; no partial source success accepted as whole-run success |
| PREPARE_GOLD | silver_inputs.load_inputs with only frozen marker list | Exact existing canonical manifest/fingerprint/input file inventory retained; no Gold namespace reservation |
| PUBLISH_GOLD | gold_publication.publish with unchanged validated input semantics; recompare prepared inventory before invocation | Completed Gold identity/input manifest/inventory; no repair of incomplete output |
| PUBLISH_CONTENT | content_publication.publish with the exact verified Gold and publisher-reference selection | Completed Content result, native fingerprint and required dependency links |
| VERIFY_RESULT | content_analytics.open_publication and named bounded smoke query | Full retained-dependency/output verification, independent of child success envelope |
| RELIABILITY_V2 / CHECK_INPUTS | reliability_format_inputs.verify_dependencies with exact batches/reference | Frozen inputs.json identity and completion evidence before publisher launch |
| PUBLISH_RELIABILITY | reliability_format_publication.publish; exact existing isolated Spark configuration | Native completion and identity; its staging/normalization/aggregation/read-back remain one non-resumable execution stage |
| VERIFY_RESULT | reliability_format_analytics.open_publication and bounded named reports | Full independent raw physical correspondence, counts/sums/reference/rank verification |

These are operational checkpoints, not changes inside accepted publishers. Internal Spark actions do not become resumable jobs. PREPARE_GOLD and CHECK_INPUTS can be rerun read-only to re-establish frozen identity; changed evidence fails. A source fetch/store/Silver failure blocks downstream Gold/Content; earlier successful inputs/results remain retained. No partial fresh selection is quietly published. A no-fetch refresh from an existing Gold is a possible future fixed adapter, not part of the two initial types.

### Narrow additive interfaces needed in Stage B

Add an execution-specific Java RSS CLI/adapter around existing fetch/storage classes. Do not parse BatchIngestionCli's human summary, use the Maven-building shell wrapper or modify the legacy runner's per-source failure behavior. The adapter supports one fixed source/stage, journals the original RawFeed and returns a versioned safe machine result. A narrowly additive prepared-write helper may expose the existing Bronze key/metadata serializer for exact journal/recovery comparison; it must delegate to the existing implementation, preserve emitted bytes/defaults and pass legacy vectors. It does not change the Bronze schema or retry algorithm.

Add fixed Python execution wrappers that call the public processing/verification functions above, validate captured input identity and emit safe machine results. No arbitrary module/command/SQL/working-directory flags. CLI argument lists reference only server-generated absolute paths after root validation; requests supply IDs. Existing CLIs, result streams and exit meanings remain unchanged. Content currently writes safe failures to stderr; Silver can emit per-input records; wrappers normalize those differences without changing processors or assuming all stdout is JSON.

The private command descriptor contains protocolVersion=1, runId, attemptId, ownerEpoch, stage, itemOrdinal, capturedConfigSha256 and exact selected artifact/profile descriptors. It is at most 64 KiB; larger native prepared input manifests are separate bounded evidence files (at most 16 MiB), hashed and addressed only from that descriptor. Private result.json has exactly protocolVersion, runId, attemptId, stage, itemOrdinal, status, code, evidenceId, counts; status is OK or FAILED, code null or fixed enum, evidenceId null or an opaque sidecar ID, and counts a fixed per-stage decimal-string map. Maximum encoded result is 64 KiB. It is completed/read back outside native publication directories. Neither file changes product fingerprints.

The worker accepts a result only if identities/schema/status/exit agree, expected evidence and full verifier agree, and child ownership is still current. Store raw sidecars privately; public API uses an approved summary. Do not infer a business failure code by parsing arbitrary stack traces. Missing/bad results or shutdown errors trigger reconciliation even with exit 0. Native publisher diagnostics are retained as allowlisted code strings and mapped to management classes without losing observed-change versus ordinary-I/O distinctions.

## Run, attempt, stage and control states

| Run transition | Preconditions / durable outcome |
| --- | --- |
| submit -> QUEUED | Idempotent committed request/configuration/input snapshot and admission slot |
| QUEUED -> RUNNING | Source enablement, host guard and clear coordinator; atomically create CLAIMED attempt/owner epoch before launch |
| QUEUED -> FAILED | Dispatch gate disabled/incompatible/missing registered configuration; fixed code, no child |
| QUEUED -> CANCELLED | Cancel commits/applies before claim; no child or product effects |
| RUNNING -> CANCEL_REQUESTED | Record request before supervisor acts; no following stage is authorized |
| CANCEL_REQUESTED -> CANCELLED | All owned writers proven stopped; no completed final product after full inspection; record retained partial effects and retry eligibility |
| RUNNING or CANCEL_REQUESTED -> SUCCEEDED | Final intended product fully verified; cancel outcome TOO_LATE when applicable; history records publication, never activation |
| active -> FAILED | Known failure plus proven writer quiescence and known artifact state; incomplete output flag can block retry without blocking unrelated later runs |
| active -> RECOVERY_REQUIRED | Lost heartbeat/parent/DB acknowledgment, unresolved child termination or ambiguous artifact state; active slot remains blocked |
| FAILED or CANCELLED -> QUEUED | Explicit eligible retry, no unresolved ownership, immutable dependencies intact and no incomplete/corrupt intended namespace; at most configured attempts |
| RECOVERY_REQUIRED -> SUCCEEDED / FAILED / CANCELLED | Explicit RECONCILE attempt after quiescence proof and occupied-slot handoff; complete final result -> success, otherwise close the proven outcome with guarded retry eligibility. An earlier checkpoint alone is not whole-run success |

The run states are exactly QUEUED, RUNNING, CANCEL_REQUESTED, SUCCEEDED, FAILED, CANCELLED and RECOVERY_REQUIRED. No direct RECOVERY_REQUIRED -> RUNNING or automatic retry exists. Reconciliation first produces a safe outcome; retry is a separate guarded control. SUCCEEDED has no outgoing transition. FAILED/CANCELLED are closed outcomes unless an explicit retry reopens the run; the prior attempt/control history remains append-only. All omitted transitions fail STATE_CONFLICT.

Attempt transitions: CLAIMED -> RUNNING, FAILED or INTERRUPTED; RUNNING -> SUCCEEDED, FAILED, CANCELLED or INTERRUPTED. Only a known pre-launch failure is FAILED from CLAIMED; uncertain launch is INTERRUPTED. These outcomes are immutable. A RECONCILE attempt follows the same claim/run/final lifecycle; it may succeed as a reconciliation operation while leaving its run FAILED/CANCELLED. Its result records the resulting run state explicitly. An old INTERRUPTED attempt never becomes falsely SUCCEEDED later; append a recovered-result link instead.

Stage states are INTENT, RUNNING, VERIFIED, FAILED and UNKNOWN. An INTENT records selected evidence/expected namespace before child start. RUNNING additionally records PID, start instant and worker instance; a crash between start and PID recording leaves UNKNOWN, not proof that no child exists. VERIFIED requires evidence checkpoint plus process cleanup. Stage invocation rows remain distinct by attempt/stage/item, with guarded state transitions and preserved terminal outcomes; an eligible retry may reuse only a reverified prior VERIFIED checkpoint. This is retry from immutable evidence, not a pause/resume feature. No continuation inside an incomplete publication/staging file is allowed.

Control types are exactly CANCEL, RETRY and RECONCILE. States are REQUESTED, APPLIED, TOO_LATE and REJECTED. Return a control ID on commit; HTTP 202 means requested, not cancelled/succeeded. Repeated same control key/body returns that control. A failed precondition creates no applied effect. Every terminal acknowledgment has a fixed reason. Repeated retry/reconcile controls cannot launch duplicate attempts. Terminal late cancellation may be recorded as TOO_LATE without altering the run. PAUSE and RESUME are unsupported/deferred, not reserved accepted enum values.

### Cancellation races

Under the coordinator/run lock, check controls immediately before launch authorization. If cancel has already committed, do not start the stage. Once authorized/in flight, the processor does not consult PostgreSQL before completion rename; cancellation can race even with an earlier committed HTTP response. The supervisor requests termination and verifies actual artifacts. Complete valid final result -> SUCCEEDED and TOO_LATE; absent/known incomplete final result with stopped writers -> CANCELLED; uncertainty -> RECOVERY_REQUIRED. Never delete a completed result to make cancellation appear to win. Intermediate completed Bronze/Silver/Gold can remain after cancellation of a Content chain; report them without claiming the final product succeeded.

E5 implements no process suspension or pause/resume transition. Stage boundaries support persistent evidence, cancellation admission checks and guarded retry only. A worker cannot release ownership while its writers remain uncertain.

## PostgreSQL persistence contract

Use a dedicated management schema and versioned SQL migrations. UUIDs are opaque operational IDs, not publication/ingestion identities. UTC timestamptz fields record operational times only; do not load year-9999 event histories into management rows or reinterpret timestamps. SQL bigint sequences/versions and public counts use decimal strings; bounded version numbers remain JSON integers. Canonical objects use the unchanged Python canonical_bytes definition (sorted UTF-8 object keys, compact JSON, no floats/BOM/newline, array order explicit). A small fixed-DTO Java encoder must pass shared compatibility vectors; JSONB storage/order is not a substitute. Reject duplicate/unknown keys, trailing JSON, invalid Unicode and coercion.

| Table | Keys / required content and constraints |
| --- | --- |
| coordinator | Singleton id=1; installation_id, mode READY/STOPPING/RECOVERY_ONLY, epoch>=0, active_attempt_id nullable FK, owner_instance_id nullable, heartbeat_at, row_version. Owner/active fields set together; never clear by heartbeat expiration |
| worker_instances | PK instance_id; installation/runtime-profile IDs and digest, host installation binding, PID/start instant, started_at/heartbeat_at, READY/STOPPING/RECOVERY_ONLY/STOPPED status. Do not claim PID alone identifies a process |
| sources | PK (product_id,source_id), immutable product/profile family, current_revision FK, enabled, row_version, created_at/updated_at. No deletion/reuse of referenced source IDs |
| source_revisions | PK (product_id,source_id,revision>0); display_name, config_version=1, exact canonical config bytea <=64 KiB, sha256, created_at, actor. FK source; identical current config/display name is a no-op, while reverting to older bytes creates a new increasing revision; UPDATE/DELETE denied for ordinary runtime role |
| execution_profiles | PK immutable profile_id; configuration bytes/digest, permitted products/adapter/runtime versions, storage binding IDs and limits; server-provisioned, no management HTTP mutation |
| artifacts | PK artifact_id; kind/product/native versions/native identity, storage binding ID and private relative address, exact input/completion descriptors and dependency links, verified_at. Unique binding/address; native identity immutable; complete artifacts only, no success inferred from registry |
| artifact_dependencies | PK (artifact_id,role,ordinal), dependency_artifact_id FK; UNIQUE(artifact_id,role,dependency_artifact_id); retained exact dependency links, no cycles or mutable latest selection |
| runs | PK run_id; unique submission_seq; product/pipeline/version, principal_scope=local-operator, idempotency_key (1–128 ASCII token), normalized_request bytes <=64 KiB/hash, immutable captured configuration/selection descriptors <=16 MiB, execution_profile_id FK, state/current_stage/current_attempt_id, row_version, requested_at/finished_at, result_artifact_id nullable FK, safe outcome/counts, retry_eligibility. UNIQUE(product_id,principal_scope,idempotency_key) |
| run_sources | PK (run_id,product_id,source_id); exact source revision FK, captured gate version/enabled; source snapshots immutable. Captures only selected delivery sources, not an invented revision for legacy artifact provenance |
| run_inputs | PK (run_id,role,ordinal); artifact_id FK, frozen native identity/completion/input evidence descriptors, unique run/role/artifact. Retained history order is explicit; no latest pointer |
| attempts | PK attempt_id; FK run, positive attempt_no, UNIQUE(run_id,attempt_no), kind EXECUTE/RECONCILE, instance_id/owner_epoch, state, times, reused_checkpoint_id nullable, safe failure. One active CLAIMED/RUNNING attempt globally via unique partial constant index; retry checkpoint reuse does not introduce pause/resume |
| stage_invocations | PK (attempt_id,stage,item_ordinal); pipeline-approved ordered stage enum, state, input evidence hash, intended namespace/identity, private command/result evidence addresses/digests, child PID/start/instance, times, output artifact links, safe outcome/counts. At most 512 invocations/run through guarded transaction |
| rss_deliveries | PK (run_id,source_id); captured revision, owning attempt, journal descriptor/hash, original ingestion UUID and original_ingested_at_text, expected keys/length/hash, FETCH_JOURNALED/BRONZE_WRITING/BRONZE_VERIFIED, verified metadata digest/receipt. UNIQUE(ingestion_id); preserve the exact Java Instant string, including up to nine fractional digits, as text/bytes rather than truncating it through SQL timestamptz; no body in SQL; journaled fields immutable once set |
| controls | PK control_id; FK run, type, key, normalized request/hash, expected version, state/reason, requester/requested_at/applied_at. UNIQUE(run_id,key); same key/different body conflicts |
| command_bindings | PK (scope,idempotency_key); normalized request bytes/hash, committed response object ID/status and creation time for source mutations. Transactional binding precedes response; complete fixed schemas only |
| audit_events | PK event_seq bigint; time, actor_scope, run/source/control/attempt references as applicable, action, before/after state/version, safe code and fixed details <=8 KiB. Append-only; no payload, feed URLs, article/event identities, commands or raw stack traces |

Use a stage_artifacts join keyed by (attempt_id,stage,item_ordinal,role,ordinal) with artifact FK for the stage table's result links. Creation of a native artifact record, its dependency links, stage VERIFIED outcome and audit checkpoint commits atomically after full verification. A lost commit response is resolved through that stage identity; it does not reinsert a second logical artifact.

Use FK/restrict-on-delete for history and identity linkage; deferred initial source/current-revision FK creation can support atomic initial revision insertion. Source/config/artifact/captured-request bytes are immutable. Runtime can update only state/control/checkpoint fields through fixed repository methods, never arbitrary JSON patches. CHECK enums, counter ranges, digest syntax and state-dependent nullability; SUCCEEDED requires result_artifact_id, observed completed evidence and finished_at. Application transaction tests cover cross-row invariants that CHECK cannot express.

Indexes: runs(state,submission_seq), runs(product_id,requested_at DESC,run_id), run_sources(product_id,source_id,run_id), attempts(run_id,attempt_no), stage_invocations(attempt_id,stage,item_ordinal), controls(run_id,requested_at,control_id), audit_events(run_id,event_seq), and artifacts(kind,native_identity). Add a unique partial constant run index over RUNNING/CANCEL_REQUESTED/RECOVERY_REQUIRED to back the global active slot. All metadata mutations that change admission/ownership lock coordinator first, then source rows in key order, then run/attempt/control rows; avoid inverse lock ordering. History GET uses bounded indexed reads without the coordinator lock.

No JSONB arbitrary document/workflow store, outbox broker or notification system is needed. Database rows and local descriptors are not a cross-resource commit. Persistent local volumes and manual retention/recovery documentation must preserve their linkage. Comprehensive backup automation and restore certification are deferred. No automatic history purge or pruning of artifacts is included; admission stops at configured bounds pending owner maintenance.

### Submission and claim transactions

Submission at READ COMMITTED: lock coordinator; look up idempotency binding first. Identical normalized request returns the original run even if current sources changed or it is terminal; a different normalized request conflicts. For a new binding, check queue/admission and compatible enabled source revisions; lock selected sources, capture exact artifact/profile membership, insert run/links/audit, commit, then return 202. Unknown commit outcome returns 503; caller can safely repeat the same key. Never start a child in the API transaction.

Worker claim: acquire host guard before DB operations. Lock coordinator; require READY, no occupied slot and no unresolved ownership. Select lowest immutable submission_seq QUEUED run FOR UPDATE; recheck selected delivery sources' live enabled flags and runtime compatibility. Disabled run -> FAILED/SOURCE_DISABLED with audit, no child. Otherwise increment epoch, create CLAIMED attempt, bind coordinator/run/attempt/instance, commit. Only after that confirmed commit can worker proceed. No SKIP LOCKED is needed with one claimant; no transaction remains open over stage work.

Before each child launch, commit the stage INTENT, then a bounded pre-start ownership/control check/authorization. Launch exactly one process, close stdin, record its identity as soon as observable. A launch/DB gap is uncertain and blocks recovery rather than inferring absence. All subsequent owner writes use WHERE attempt_id/owner_epoch/instance/state/row_version match and require exactly one affected row; zero means stop, never retry the write using a newer token. State transition and audit commit together. Heartbeat updates are coalesced, not a new history event every five seconds.

### Transactional recovery handoff of an occupied coordinator

This protocol transfers the existing occupied slot to read-only reconciliation; it never makes that slot temporarily available to the ordinary queue. It uses the existing coordinator, run, attempt, stage, control and audit records, not another queue, lease service or public takeover endpoint.

Let R be the affected run, A its occupied attempt, E its current owner epoch, W its old worker instance, and K a committed RECONCILE control targeting A with the expected run version. The recovering instance N must hold the installation's host guard and be in RECOVERY_ONLY. N may be the original supervisor only after its former execution handlers and owned writers have ceased; otherwise it is a replacement worker. Neither possession of that guard nor any PID, heartbeat or database lease independently proves quiescence. Recording this control's REQUESTED state stores the checked attempt/run-version precondition without changing R's state/version or the occupied binding; transfer applies those changes. An intervening run change makes the request stale rather than silently changing its target.

1. **Establish quiescence before transfer.** Require the existing explicit operator confirmation bound to A, its installation and recorded process evidence, plus supervisor checks of all observable original writers/descendants. A known live writer vetoes transfer. Unresolved termination, an uninspected orphan, changed installation or unavailable ownership evidence keeps the slot blocked. This confirmation must remain valid at transfer; it is not a filesystem fencing mechanism. No processing child, Bronze PUT or publisher invocation may start while quiescence is unresolved.
2. **Lock and revalidate in one short transaction.** Lock coordinator first, then R, A and K in the established order. Resolve an already transferred/completed K idempotently before applying pre-transfer checks. For a new transfer, check coordinator=(A,E,W), R.current_attempt_id=A, K's captured expected run version, the coordinator version just read under lock, the installation/profile binding, and K=REQUESTED/RECONCILE targeting A. R must be RUNNING, CANCEL_REQUESTED or RECOVERY_REQUIRED with that occupied slot. Check the existing attempt/admission/history bounds. A mismatched binding/version rejects the stale request, without retargeting it to a newer attempt. The control's rejection may be recorded in a separate guarded metadata transaction; failure to record it never transfers or clears ownership.
3. **Close the old active attempt atomically.** If A is CLAIMED or RUNNING, conditionally update it to INTERRUPTED with a fixed interruption reason and the time the interruption was recorded (not an invented process-death time). If A is already INTERRUPTED, validate and retain that outcome. Its unresolved INTENT/RUNNING stages become UNKNOWN; preserve VERIFIED/terminal stage evidence. A terminal FAILED reconciliation attempt that still occupies a RECOVERY_REQUIRED slot can also be retained for another explicit read-only reconciliation; never rewrite its terminal outcome. Other inconsistent terminal bindings fail closed. Updating an active A before inserting its successor satisfies the existing unique active-attempt index within the same transaction.
4. **Create exactly one reconciliation attempt.** Set the new attempt UUID equal to K.control_id, using an already durable, unique control identity instead of allocating another attempt on every transaction retry. Allocate the next attempt_no under the locked run; insert kind=RECONCILE, state=CLAIMED, owner_epoch=E+1 and instance_id=N. The cross-table reuse of this opaque UUID changes no native publication identity. An existing row with that UUID must match K/R/kind exactly; otherwise roll back and retain recovery. This control creates at most one attempt; another reconciliation after its failure needs a new control targeting the then-occupied attempt.
5. **Transfer the occupied binding, never clear it.** Conditionally update coordinator from exactly (A,E,W,expected_version) to the new attempt/N/E+1, mode=RECOVERY_ONLY and fresh heartbeat/version. Conditionally update R from the checked old binding/version to RECOVERY_REQUIRED with the new current_attempt_id and incremented version. Preserve pending cancellation and frozen inputs. Append interruption/handoff/claim audit records linked to K. Any affected-row count other than one or any constraint/audit failure rolls back the whole transaction, including the old attempt's transition. There is no committed state with an empty slot between A and its successor.
6. **Confirm commit before read-only execution.** Only after commit is confirmed may the new owner advance CLAIMED -> RUNNING and launch bounded independent verification under its new token. Every heartbeat, stage/control/result mutation and associated audit write must lock/check the live coordinator and match R/new attempt/E+1/N/expected state/version; no stale execution callback may bypass that guard. A zero-row conditional update stops that owner. Ordinary queue claiming and all fresh publication/ingestion remain disabled throughout reconciliation, including when only an earlier checkpoint is valid.
7. **Resolve and release conditionally.** After quiescence and full artifact/dependency inspection, commit the reconciliation attempt's terminal outcome, the control acknowledgment, run outcome/result/retry eligibility, audit and slot release together under the new binding/version. Release to READY only for the matrix's resolved SUCCEEDED/FAILED/CANCELLED outcomes and no outstanding stop condition. Reconciliation never changes a run directly to QUEUED/RUNNING. Further execution of R after a resolved FAILED/CANCELLED outcome requires a separate guarded retry and ordinary claim; unrelated queued work may be claimed normally once the slot is safely released. Verification/ownership ambiguity keeps R=RECOVERY_REQUIRED, coordinator=RECOVERY_ONLY and the occupied binding. A known failed read-only reconciliation can close its attempt FAILED/control REJECTED while retaining that occupied slot; another explicit control is required. A reconciliation process crash leaves its attempt subject to this same handoff protocol after fresh quiescence confirmation.

**Failed or ambiguous transfer transactions:** a definite rollback leaves A/E/W and the prior run/attempt/stage/control records unchanged; the worker starts nothing and can revalidate the same K. After a lost commit acknowledgment, no verification child or fresh transfer starts until PostgreSQL is reachable and a bounded read transaction locks the coordinator and reads K plus attempt_id=K.control_id. The lock ensures a still-pending transfer has finished; lock timeout/outage means unknown, not rollback. A matching committed new binding/epoch/attempt/audit confirms the one transfer and permits its recorded next read-only step. A completed K returns its recorded result without another attempt. Absence of that attempt together with the exact old binding and unchanged REQUESTED K proves rollback and permits reattempt of the same transaction/identity after revalidation. Any newer/different owner, mismatched evidence or inconclusive read remains blocked; do not increment the epoch again, clear the slot or infer success from absence in an unlocked snapshot. Recovery of a newer interrupted owner uses a new explicit control, not the old K.

Checkpoint B/C tests must use real PostgreSQL to demonstrate an occupied active attempt -> INTERRUPTED plus one new reconciliation claim, partial-index enforcement, epoch increment, same-control deduplication, rollback after each mutation, lost-acknowledgment resolution, concurrent/stale transfer rejection, and rejection of old-owner heartbeats/checkpoints/completion/launch authorizations after transfer. Process/quiescence tests separately prove that no lock/PID/lease/heartbeat-only shortcut authorizes transfer or publishing. These are proposed/unexecuted tests; SQL atomicity does not fence external filesystem writers.

## Ownership, heartbeat and process supervision

Use a dedicated persistent host FileChannel lock file under the installation's private control root, held for the lifetime of the worker/import process. Validate root/file identities; never unlink/recreate the lock to bypass a live owner. All managed roots bind to that installation. This guards cooperative workers only: arbitrary legacy CLI writers must be stopped before managed execution. Never use shared/network filesystem locks or a lease as a multi-host solution.

Heartbeat every 5 s with the current epoch; older than 30 s is STALE/UNKNOWN supervision, not proof of death. A second process cannot dispatch even if DB says stale and a crashed parent's file lock has been released: the occupied persistent slot keeps RECOVERY_ONLY. A local lock does not survive a parent's crash as a guarantee about orphan children. No automatic takeover, process killing by reused PID or machine-restart assumption is permitted.

The execution-specific supervisor reuses the query runner's argument-list, environment-isolation, concurrent draining and safe-error patterns, but not its 30 s query timeout or in-memory semaphore as a durable queue. It owns one direct child and observed descendants, records process instance/start identity, requests graceful termination, escalates and waits. On Java's current best-effort descendant enumeration, complete orphan containment is not proven. If descendants disappear from observation, cannot be reaped or cannot be attributed, retain RECOVERY_REQUIRED and require local operator confirmation that the former parent/Python/JVM tree and unmanaged writers are stopped. Never auto-release the slot because destroyForcibly returned or the direct parent exited. Unix-specific containment may be isolated later, but is not a new cross-platform guarantee or silently required daemon.

Child environment is an explicit stage whitelist: pinned runtime paths, UTC, UTF-8, loopback Spark address, private scratch and required configured Spark settings. Pass local BRONZE_S3 settings only to stages using them. No ambient AWS credentials/proxies, arbitrary inherited environment, executable interpolation or shell. Raw child streams are not public logs. Drain continuously with bounded counters; parse only the small fixed result file. Discard raw stderr by default; optional restricted diagnostics require an explicit local policy and remain unserved. On output/deadline/cleanup failure, no partial successful response is emitted.

### Database outage

Any failed ownership/heartbeat transaction stops new stage authorization immediately. Request active-child termination through the supervisor; do not continue by caching a lease. A publisher may already be in flight or finalize before termination. Preserve the stage intent/spool/result locally, retain the occupied slot in DB and reconcile after connectivity returns. PostgreSQL unavailability makes source/run mutations/history unavailable with fixed 503, not an empty queue or successful acceptance. Known reader queries over existing immutable artifacts retain their independent behavior and need no DB connection. API liveness must not be presented as worker/database readiness.

## RSS journal and recovery protocol

The journal is a restricted per-run/source directory outside all native output roots, owned by the local worker, mode 0700 where supported. It contains exact payload bytes plus a strict version-1 descriptor of captured source revision/config hash, original RawFeed ingestion UUID/instant/status/contentType/declared length, payload length/hash and expected Bronze keys/metadata bytes. Unknown fields/oversize descriptors fail. Payload <=5 MiB per source; descriptor <=64 KiB, excluding the separately bounded metadata byte file. No XML parsing/rewriting or timestamp truncation occurs in the journal. It is not a new Bronze schema or a public data product.

Protocol: commit FETCH intent -> fetch once -> exclusive private payload/descriptor write, read-back/hash validation and completion-last journal marker -> commit immutable journal/ingestion/key descriptor in rss_deliveries -> commit STORE_BRONZE intent -> call existing writer -> independent bounded GET verification against the original prepared bytes -> commit verified receipt. The prepared metadata uses the same serializer as S3BronzeStorage, not Java reserialization of arbitrary parsed metadata. Recheck spool/source binding before any storage retry. No raw payload goes to DB/audit/API.

If fetch succeeds but crashes before the durable journal/DB checkpoint, there was no authorized S3 PUT. Do not fetch again in that run or invent the previous UUID; report FETCH_EVIDENCE_LOST and require a new run. A complete orphan journal can be inspected explicitly during reconciliation, but is not automatically adopted. A known committed journal permits read-only reconciliation of exact keys; zero objects or a matching payload-only orphan permits an explicit retry using the identical original RawFeed under ADR 0002. Matching metadata with missing payload, mismatches or changed source/date fields are conflicts, never repairable published records. A 404 other than exact NoSuchKey, read failure or absent bucket is not proof of object absence.

If receipt acknowledgment is lost but both objects match, persist that recovered receipt without another fetch/PUT. After every selected source has a verified receipt, Silver consumes only those exact metadata keys. DB loss after an object write or after journal rename remains a recoverable ambiguity, not a DB rollback of S3. Journal/objects are retained for owner investigation; no automatic cleanup is introduced. The journal is additional restricted raw retention with explicit storage budgets and documented manual retention/recovery; comprehensive backup automation is deferred. Preserve original byte/UUID/instant evidence throughout.

## Reconciliation and retry decision matrix

Reconciliation is an explicit durable control, executed by the sole worker under the host guard/global slot. Require operator quiescence attestation (fixed boolean and audit code, not a text assertion presented as automated proof), bound to the old attempt and process evidence. The service must also check all observable owned process identities; a live known writer vetoes attestation. Unobservable descendants keep uncertainty unless the trusted local operator has actually stopped/inspected the installation. Merely setting a DB flag is not filesystem fencing. A restarted/replaced host needs equivalent explicit installation-level proof before dispatch.

| Observation after quiescence and full inspection | Action |
| --- | --- |
| Expected final publication complete, exact identity/dependencies/full verification pass | Record SUCCEEDED/replayed-or-recovered and native result linkage; outstanding cancel TOO_LATE; do not rewrite output |
| Only earlier durable stage completed | Reverify/checkpoint its original evidence; close the interrupted run as FAILED (or CANCELLED for a confirmed pending cancellation), recording retry eligibility only after all required checks. A separate guarded retry may reuse verified evidence; reconciliation does not publish the next stage |
| Intended analytical namespace absent, complete frozen inputs retained | Known failed/cancelled outcome can be closed with retry eligible; explicit retry may invoke same stage/selection |
| Intended analytical namespace exists without valid completion, including complete.tmp | FAILED or CANCELLED with INCOMPLETE_OUTPUT, retry ineligible; preserve it. Owner investigation outside E5 is required |
| Complete output corrupt or selected evidence mismatches | FAILED/DEPENDENCY_INVALID or CHANGED/PUBLICATION_INVALID or CHANGED; no repair or fallback to an unselected artifact |
| Dependency unavailable / verification I/O fails | Cannot establish success; retain recovery/blocked control for retry of read-only verification. Do not classify ordinary I/O as observed mutation |
| Raw RSS evidence lost before journal checkpoint | FAILED/FETCH_EVIDENCE_LOST; require new run/retrieval; no same-run refetch |
| Journal intact; exact orphan/absent Bronze state allowed by ADR 0002 | Explicit storage-only retry eligible, same UUID/instant/bytes/metadata; no automatic PUT during read-only reconciliation |
| Any possible old writer or outcome remains unresolved | Keep RECOVERY_REQUIRED and block all new managed writes globally |

Normal child failures can close FAILED/CANCELLED once quiescence and artifact state are known, releasing the slot for unrelated runs even if the failed fingerprint is incomplete. That run's retry remains blocked. Recovery controls do not move/delete/quarantine artifacts or change captured output roots. Selecting a new approved root/reference/history/runtime is a new run after owner investigation, never a hidden retry workaround. Repeated eligible attempts are at least once; they are not a claim that HTTP fetches or process execution occur exactly once.

## Management REST contract

Namespace /api/management/v1, separate controllers/advice/DTOs from unchanged /api/v1/. Strict UTF-8 JSON; reject unknown/duplicate/missing fields, coercions and extra/duplicate query parameters. Request bodies <=64 KiB; responses <=1 MiB encoded, verified before writing. No raw stdout/stderr, SQL, commands, internal paths, event/article IDs or payload download. IDs are UUID strings except established source IDs, native digests and fixed enums. ETags/If-Match are opaque quoted row-version strings; missing conditional preconditions -> 428, stale -> 412.

| Endpoint | Exact operation/body |
| --- | --- |
| GET /capabilities | Enabled fixed pipelines/profile IDs, approved endpoint display IDs, runtime compatibility, operational limits and coordinator readiness; no credentials/paths |
| GET /sources | productId/enabled optional filters plus bounded page; current public source summaries |
| POST /sources | Idempotency-Key required; {productId,sourceId,displayName,enabled,config}; initial immutable revision, 201 only after commit |
| GET /sources/{productId}/{sourceId} | Current summary, revision/config hash, row version; safe profile/endpoint IDs, never resolved secret settings |
| POST /sources/{productId}/{sourceId}/revisions | If-Match and Idempotency-Key; {displayName,config}; append revision atomically; identical current configuration/name returns the current revision, not an old revision number |
| PUT /sources/{productId}/{sourceId}/enabled | If-Match and Idempotency-Key; {enabled}; audit live dispatch gate, no change to historical revisions |
| GET /sources/{productId}/{sourceId}/revisions | Bounded immutable revision summaries, ascending revision; no mutation/delete route |
| GET /artifacts | kind/productId/sourceId filters and bounded page of registered compatible completed evidence; no path lookup or import/upload through HTTP |
| POST /runs | Idempotency-Key; one exact discriminated body below; 202 with run ID and Location after commit. Identical repeat returns same run/state, including terminal state; no new claim |
| GET /runs | productId/sourceId/state optional filters, deterministic descending submission sequence, bounded page |
| GET /runs/{id} | Public run DTO below, source revisions/input/artifact IDs, attempts summary, allowed control hints and current version; hints are not authorization or a substitute for server checks |
| GET /runs/{id}/attempts | Ascending attempt_no page, stage summaries and safe outcomes; no arbitrary log route |
| GET /runs/{id}/events | Ascending event_seq page, bounded typed lifecycle/control/audit events; no heartbeat firehose |
| POST /runs/{id}/controls | If-Match and Idempotency-Key; {action,attemptId,quiescenceConfirmed}; action exactly CANCEL/RETRY/RECONCILE, attemptId and boolean null except exact occupied old attempt UUID/true for RECONCILE; 202 with control ID/state. A still-active old attempt becomes INTERRUPTED only in the guarded handoff transaction; pause/resume values are rejected |
| GET /runs/{id}/controls/{controlId} | Durable requested/applied/too-late/rejected outcome and reason, current run version |

Source create/revision/gate idempotency records use the same durable full-request comparison rule as runs, stored in command_bindings (unique scope/key); this is not a generic executable job system. Retain original response IDs/status for committed commands. Look up a matching binding before rejecting an old If-Match on a retransmission, including controls. A new command with stale If-Match still fails. Serialize all source mutation/capture under the documented lock order.

CONTENT_RSS_V1 submission has exactly pipeline, executionProfileId, fetchSources, retainedSilverArtifactIds, publisherReferenceArtifactId. fetchSources is 1–8 {sourceId,revision} entries; retained history is 0–128 registered Silver artifact UUIDs. RELIABILITY_V2 has exactly pipeline, executionProfileId, sources, batchArtifactIds, serviceReferenceArtifactId; sources is 1–32 {sourceId,revision}, batches is 1–32 completed v2 IDs. References must have the correct product/kind. Selected source/profile/format membership must agree with all batch artifacts. No extra runtime/settings/URL/path keys. Reject duplicate IDs; canonicalize set-valued selections by UTF-8 source/revision or UUID order before idempotency comparison. The worker's native dependency serializer preserves its existing ordering/fingerprint formula.

Example request (fictional registered IDs, not a runnable prepared environment):

~~~json
{"pipeline":"RELIABILITY_V2","executionProfileId":"10000000-0000-4000-8000-000000000001","sources":[{"sourceId":"demo-requests","revision":1}],"batchArtifactIds":["20000000-0000-4000-8000-000000000001"],"serviceReferenceArtifactId":"30000000-0000-4000-8000-000000000001"}
~~~

Successful single responses have exactly apiVersion=1, data, meta={correlationId}; lists add limit, hasMore, nextCursor in meta. Public run data has runId, productId, pipeline, state, currentStage, rowVersion, requestedAt, finishedAt, sourceRevisions, inputArtifactIds, attempts, latestControl, result, counts, outcomeCode, retryEligibility. Nullable values are null, not invented. result is null or {artifactId,nativeFingerprint,productContractVersion,processorRevision,verificationStatus,verifiedAt}; verificationStatus=VERIFIED_AT_EXECUTION is historical, not proof of current readability or activation. Reliability data also carries the exact synthetic label. Attempt/stage detail is paged rather than an unbounded array. Operational timestamps use strict UTC microseconds; IDs/control versions and integer counts stay precise as strings where required.

List limit defaults 20, range 1–100. Use validated opaque version-1 keyset cursors bound to endpoint/filters/order/high-water sequence, max 512 encoded bytes, one-row lookahead and parameterized SQL. Lists are consistent in ordering/identity but may reflect updated states; never promise a database snapshot spanning HTTP requests. No arbitrary sort/SQL/offset. Artifacts default to identity summaries; histories/configuration blobs/native manifests are not copied into every response.

Failure envelope is exactly {apiVersion:1,code,message,correlationId,retryable}; message is fixed for the code and retryable is an operational hint, never automatic permission to refetch/repair. Fixed classes: INVALID_ARGUMENT/400, LOCAL_BOUNDARY_REJECTED/403, NOT_FOUND/404, IDEMPOTENCY_CONFLICT or STATE_CONFLICT or RETRY_NOT_SAFE or SOURCE_DISABLED/409, PRECONDITION_FAILED/412, PRECONDITION_REQUIRED/428, MANAGEMENT_CAPACITY_EXCEEDED/429, MANAGEMENT_UNAVAILABLE or COORDINATOR_RECOVERY_REQUIRED/503, INTERNAL_FAILURE/500. Async execution outcome codes additionally include INPUT_UNAVAILABLE/INVALID/CHANGED, PUBLICATION_UNAVAILABLE/INVALID/CHANGED, INCOMPLETE_OUTPUT, FETCH_FAILED, FETCH_EVIDENCE_LOST, BRONZE_CONFLICT, STAGE_TIMEOUT, STAGE_OUTPUT_LIMIT, PROCESS_START_FAILED, TERMINATION_UNCONFIRMED and RUNTIME_MISMATCH. Preserve native safe diagnostic code separately; do not flatten unavailable/invalid/changed distinctions or include native diagnostic paths. No HTTP 200 or successful stage result includes partial data on error.

## Security and proposed admission limits

E5 has no identity provider or product permissions. Actor scope local-operator is an honest label for this trust boundary, not an authenticated person. Loopback prevents remote binding, not access by other local processes. Reject unapproved Host headers, CORS and unexpected Origin; allow only explicitly configured local API/dev/preview origins. For mutations require application/json, an allowed Origin and X-DDIP-Local-Request: 1; same-origin frontend/proxy and explicit local operator client send these. The header is an anti-simple-request/CSRF measure, not a credential. No public binding or multiuser exposure is permitted before E6 authorization. Read-only legacy API semantics remain unchanged.

| New E5 starting limit | Proposed value / effect |
| --- | --- |
| Managed execution and queue | 1 active globally; 20 unfinished runs total including active/recovery. With an active run, at most 19 wait. Recovery-required ownership blocks new dispatch/admissions |
| Source management | 128 registered sources; 1,000 immutable revisions/source; no automatic deletion at the cap |
| Run/attempt/control/history | 20 attempts/run including retry/reconcile; 128 controls/run; 512 stage invocations/run; 10,000 retained runs, 200,000 source command bindings and 1,000,000 audit events. Heartbeats update in place; reserve lifecycle/control/recovery event capacity for admitted work before admitting new work, rather than exhausting history capacity mid-finalization |
| HTTP/history metadata | 64 KiB request/command/result; 1 MiB encoded response; list 20 default/100 maximum; cursor <=512 bytes; 8 KiB fixed audit detail; captured native evidence <=16 MiB/run excluding raw payloads |
| Content selection | 1–8 feeds, <=5 MiB each under existing fetch limit; <=128 retained Silver ingestions and <=1 GiB selected Silver analytical files as an E5 adapter admission cap, checked before Spark consumption |
| Reliability selection | Existing stricter native limits: 32 batches/128 raw files, 256 MiB/file, 1 GiB raw selection, 2M occurrences, 4 GiB stage/output; format/group/string/profile limits unchanged |
| Runtime | One fixed local[2] Spark child at a time, approved existing settings; independent DuckDB uses 2 threads/512 MiB/no spill/extensions disabled. No host-RAM formula changes accepted verification budgets |
| Time/child output | Existing HTTP timeouts unchanged; processing stage deadline 30 min, independent result verification 15 min, overall active attempt 2 h. Raw stdout/stderr counted separately, cap 16 MiB/stream, raw content discarded; result file <=64 KiB |
| Cleanup and database waits | Graceful stop 5 s, forced stop wait 5 s, final reap/drain wait 5 s; any unconfirmed termination -> recovery. DB connection/statement/lock waits <=3 s; heartbeat every 5 s, stale after 30 s |
| File/disk safety | Explicit approved roots, bounded input/stage byte/file admission and native artifact limits, plus >=1 GiB minimum available space on each worker-writable artifact/journal/staging/temp volume checked before each stage. Stop new launches on reserve breach, and request safe termination if a cooperative in-flight check observes exhaustion. No automatic budget increase or cleanup; no OS-specific hard quota requirement |

Deadlines are process-supervision budgets, not guarantees a result will complete within them. Count raw stream bytes without retaining an unbounded buffer; on excess stop the stage and preserve uncertainty until verification. Early bounded stage/input admission is preferred. Cooperative free-space/admission checks and existing artifact limits do not provide hard disk limits: other processes and in-flight writes can consume space between observations, and legacy Gold/Silver have no comprehensive in-flight output quota hook. OS-specific filesystem quotas are not an E5 completion gate. Do not add a quota framework or benchmark collector. On disk-full/short-write/temporary-marker failure, fail closed, stop new stages, terminate/reap as required, inspect completion through the normal recovery protocol and preserve all incomplete evidence. Block retry of an incomplete analytical namespace; do not free space by deleting it or promote it into success. A fully completed result found after a lost acknowledgment still requires independent verification.

Sources, raw inputs, restricted RSS journals, stages and completed publications are retained, private and ignored by Git. Use persistent local PostgreSQL/SeaweedFS volumes and explicit local artifact roots, with documented manual inspection, retention and recovery procedures. Operator startup/maintenance also checks minimum free space for those service-managed volumes; the host worker cannot infer their capacity from its own filesystem, so storage/DB exhaustion there follows the fail-closed I/O/outage protocol. No default deletion/TTL/purge. Comprehensive backup automation/restore certification is deferred, with no untested restore or end-to-end power-loss guarantee. An operator-initiated restore/remount keeps dispatch disabled until retained identity/dependency evidence and writer quiescence are revalidated. PostgreSQL, JVM, Python/Spark and existing query processes contend for resources; these bounds are not a measured RAM/latency/capacity claim. Event/status counters and monotonic stage durations suffice for E5; no APM, Spark benchmark logs, 100K/1M generator or E4a resource preflight is required.

## Stage B failure injection and acceptance matrix

Every group E5-01–E5-18 below is **Proposed / unexecuted**, not an implemented feature or Stage A/A.1 application test result. Use real PostgreSQL transactions and small deterministic synthetic HTTP/S3/JSONL/Parquet/reference fixtures. H2/mock-only SQL tests cannot establish PostgreSQL locking/unique-index behavior. Normal CI must not run live feeds, retained-data mutation or benchmark workloads.

| Group | Independently testable acceptance |
| --- | --- |
| E5-01 — migrations/roles | Empty/populated PostgreSQL upgrades/checksums, wrong schema and DB outages fail closed; no DB in legacy analytics-only mode; worker-only process exposes no HTTP/controller |
| E5-02 — source revisions | Create/update/gate concurrency, optimistic preconditions, immutable old revisions, source ID/profile/URL allowlists and captured revision isolation through edits/disable/retry |
| E5-03 — submission/idempotency | Concurrent identical/different bodies/keys, lost commit response, normalization vectors, unfinished-run/queue capacity; exactly one persisted run binding and guarded retry |
| E5-04 — ownership | Real PostgreSQL occupied-slot handoff: old active -> INTERRUPTED, epoch+1, exactly one new RECONCILE attempt, atomic index replacement/rollback, concurrent/stale control and old-owner mutation rejection; no lock/PID/heartbeat/lease-only takeover |
| E5-05 — supervisor | Start failure, malformed/oversized result, noisy stdout/stderr, deadlines, interruption, denied descendant enumeration/reused PID/orphan JVM; unknown termination blocks dispatch |
| E5-06 — Reliability | Registered JSONL/Parquet small fixtures, exact selected batches/reference, native 20/21/17 schemas/counts/ranks, safe rejections and producer/independent verification unchanged |
| E5-07 — RSS evidence | Crash before/after fetch/journal/DB intent and every S3 PUT; original UUID/instant/bytes retained, exact receipts/read-back, no same-run refetch or hidden payload disclosure |
| E5-08 — Content history | Synthetic RSS/Atom, explicit Bronze keys/Silver markers, imported Silver original marker bytes/namespace/source-ingestion binding plus full inventories/hashes/schema/provenance and use-time revalidation; reject missing/corrupt/changed evidence without rewriting markers. Exact retained history, Gold/Content values and no partial fresh selection |
| E5-09 — controls | Durable queued/running cancellation, guarded retry/reconciliation, finalization races, late cancel success/TOO_LATE, rejected stale controls and idempotent acknowledgments; pause/resume enums are rejected and no suspension UI/behavior is required |
| E5-10 — DB outage | Before/after claim, child PID record, stage/publication and each handoff mutation/commit; real PostgreSQL definite rollback/lost-acknowledgment resolution uses the same control/attempt identity; no blind second transfer or new stage without confirmed ownership |
| E5-11 — reconciliation | Confirm old-writer quiescence, read-only occupied-slot transfer/verification, complete/absent/incomplete/corrupt outputs, final rename/response-loss and failed reconciliation retry; no fresh publishing until resolved, complete valid result reused and incomplete never promoted |
| E5-12 — retry | Same source/config/input/reference/runtime bindings across attempts, stored p95 qualification, receipt retry policy, blocked partial namespace, changed selection/root/runtime requires new run |
| E5-13 — integrity classes | Missing/dependency corruption, same-byte inode/directory/metadata changes and unchanged read/close errors; native safe classes preserved without exception/path leakage |
| E5-14 — resource/security | Request/page/result/attempt/queue/stream/time caps, bounded stage admission, native artifact bounds, minimum-free-space refusal and disk-full/short-write/finalization faults; preserve partial artifacts/block unsafe retries. Parameterized SQL, injection/Host/Origin checks; no hard quota or E6 authorization claim |
| E5-15 — Operations UI | Source list/revision/enable, exact artifact selection/run submission, run/attempt stages, cancel/retry/reconcile outcomes and quiescence confirmation, reload/restart/history, accessibility/raw-data exclusion; no pause/resume controls |
| E5-16 — deployment/restart | Explicit local migration/API/worker/Compose lifecycle, DB/worker restart with persistent local volumes and retained restricted journals/roots, manual retention/recovery procedure, missing/mismatched bindings; no destructive teardown or untested backup/restore guarantee |
| E5-17 — independent result/audit | Child exit zero/bad artifact rejected; success/no activation, prior selected product unchanged; transactional append-only safe lifecycle/control/source audit and bounded error responses |
| E5-18 — unchanged regressions | Full processing E1/E2/E3/Gold/Silver, ingestion/launcher, JDBC/Content API, frontend unit/build/type/lint and docs; pinned runtime, legacy hashes/bytes/replay/report envelopes unchanged |

Checkpoints B–F remain independently reviewable. B1 foundation is authorized; B2 and C–F remain planned/unexecuted. Full acceptance groups remain unexecuted; the current B1 evidence below records only tested subsets. B specifies migrations/API/transaction tests including occupied-slot transfer before C integrates actual supervision and Reliability writers. D adds qualified Silver imports, exact RSS journal recovery and the Content chain. E tests cross-pipeline cancellation/guarded retry/reconciliation, stale owners and disk exhaustion; F adds the minimal Operations UI and reproducible local lifecycle/full regressions. No pause/resume or hard-filesystem-quota implementation is required. A consequential incompatibility, unresolved writer ownership or correctness failure stops at a tested checkpoint for owner review, never weaker verification.

In Stage B CI, retain existing required job names. Add small PostgreSQL service-backed integration tests in the appropriate existing job, one worker test process and isolated roots; use a synthetic S3/HTTP test double for ordinary CI. Opt-in local Compose verification can test actual SeaweedFS after explicit setup, without publishers. Do not introduce Testcontainers unless separately justified; an explicit CI PostgreSQL service and existing test tools suffice. Stage A does not change CI or run these suites.

## Minimum React Operations scope and deferred work

Add a small Operations entry alongside the existing analytics workspace, with its own management DTO decoder/client/state. Reuse current React/Vite/testing stack and text-safe components; no router/state-management dependency is justified. Show source config revision/enablement, registered input/reference selectors, bounded run history, current stage, attempts, counts/rejections, publication candidate identity and separate last-control outcome. Offer only cancellation, guarded retry and explicit reconciliation; pause/resume is deferred and has no E5 UI requirement. Poll only while visible (proposed 2 s), one request at a time, abort on navigation and do not label a 202 response completed. Reload reconstructs state from PostgreSQL. Actions use fresh version/key, disable duplicate clicks and handle 412/reload explicitly. Reconciliation requires the trusted operator's actual process-stop confirmation, with clear warnings about its meaning.

Do not expose environment editors, raw URLs, SQL, log streaming, arbitrary executables, uploads, downloads or raw/quarantine/event catalogs. Completed candidate is not automatically selected; existing analytics pin remains unchanged. E6 owns authentication/product-scoped policy, selection/activation and exports. E7 owns product report integration, broader navigation/visual polish and release acceptance. E4a owns full benchmark tooling/experiments and detailed performance admission. No broad platform/M6 completion is implied.

## Risks and implementation estimate

Highest risk is external-effect ownership: a database epoch cannot fence an existing filesystem publisher, and the current Java supervisor cannot prove all orphan descendants are gone. Explicit confirmed quiescence plus the atomic occupied-slot handoff is required; real PostgreSQL/process tests are still unexecuted. RSS journaling adds restricted raw retention and byte-exact metadata/UUID recovery; incorrect refetch would alter observations. Imported Silver must satisfy the managed evidence qualification above; its original marker alone cannot establish content integrity. Cooperative disk admission cannot prevent every full-disk race, and manual retention is not automated backup or tested restore. Management JDBC boot wiring and Jackson 2 ingestion versus Jackson 3 Spring DTO dependencies require packaging/isolation tests.

This is a substantial but bounded integration: two process roles, a transactional operational schema, two fixed adapters and a small UI. Planning estimate for one experienced contributor is roughly 3–5 engineering weeks after decisions/initial feasibility, with C/D/E carrying most uncertainty; this is a scope estimate, not a delivery commitment or measured productivity. Persistence/API alone is not a safe usable execution checkpoint. The first meaningful runnable checkpoint is tested C with basic stop/reconcile; do not declare completion if D/E/F are missing.

## Historical Stage A evidence

Read-only inspection covered clean prepared branch/empty index at merge 63133acf3cb25bc420966cbb4c9d690054914b35, AGENTS.md, accepted ADRs 0001–0009, the Proposed platform specification, Java ingestion/Bronze/process supervision, Python explicit processing/publication/verifier boundaries, React/API wiring, Compose/environment/requirements and CI. PostgreSQL locking and Spring migration documentation were reviewed; no external service was provisioned or dependency installed. This proposal is repository-grounded analysis, not executed E5 feasibility evidence.

Executed local Stage A validation:

| Check | Actual result |
| --- | --- |
| npm run check | Passed: 43 Markdown files, zero formatting/local-link issues, repository hygiene passed |
| git diff --check and git diff --cached --check | Passed; new documents also checked directly for trailing whitespace/final newline |
| Architecture consistency review | Accepted ADRs 0001–0009 and existing data interfaces remain unchanged; one host worker/DB-versus-filesystem ownership, explicit raw UUID recovery and honest cancellation semantics checked against source |
| Scope/index review | Only two new Markdown documents and one ADR-index modification; initial empty index preserved; no implementation/configuration/test file changed |
| Application/hosted/scale evidence | Not run in Stage A; all E5-01–E5-18 remain proposed acceptance, not executed results |

Only ADR 0010, this supporting contract and the ADR index are changed. No application code/tests, accepted ADR, native manifest/schema/fingerprint/reader, CI job, dependency or service configuration is modified. No application regression suite, Spark workload, ingestion, benchmark, container/database startup or retained-data change was performed. No staging, commit, push, branch switch, PR/issue update or other Git/GitHub write was performed; repository Git inspection was read-only.

## Historical Stage A.1 refinement and validation

Following owner architecture review, this documentation-only refinement removes mandatory pause/resume, specifies the guarded occupied-coordinator recovery transfer, requires complete managed Silver evidence qualification, and narrows disk/retention scope to bounded cooperative safety with persistent volumes and manual procedures. PostgreSQL, one host worker, two fixed adapters, source revisions, artifact selection, history, cancellation, guarded retry/reconciliation and minimal Operations remain. No persistence table, API endpoint or acceptance group is added; existing attempt/control bindings implement the handoff. All checkpoints B–F and E5-01–E5-18 remain Proposed/unexecuted.

Local validation: npm run check passed with 43 Markdown files, zero formatting/local-link issues and repository hygiene passed. git diff --check and git diff --cached --check passed. Document consistency review checked the seven run states, reduced control enums, atomic handoff/ambiguous-commit rules, unchanged native Silver semantics and all eighteen proposed acceptance groups. No application or PostgreSQL integration tests ran.

Only this supporting contract and ADR 0010 were refined. The existing ADR-index change and the two owner-staged document snapshots were preserved; refinements remain unstaged. Accepted ADRs 0001–0009, application code/tests, dependencies, native data contracts, CI and infrastructure remain unchanged. No staging, commit, push, branch switch or GitHub mutation occurred. ADR 0010 remains **Proposed — awaiting owner approval**; Stage B has not begun.

## Stage B1 implemented foundation and local evidence

The owner accepted ADR 0010's seven decision groups, including all Stage A.1 refinements, on October 20, 2026 and authorized **B1 only**. Inspection at B1 entry found branch `feature/e5-durable-execution-source-controls` at `9a87cc5`, with the merged E3 baseline present and a clean tree/index: the earlier owner-staged documentation had already been committed by the owner. No owner work was discarded. The historical Stage A/A.1 evidence above retains its original proposal status; this section records the subsequent implementation.

**Implemented:** PostgreSQL 17.11 Compose service pinned by multi-platform digest, explicit persistent volume/loopback port/health check; Boot-managed JDBC/Flyway/PostgreSQL dependencies; V1–V5 SQL migrations; explicit analytics/API/worker/migration roles; schema/checksum/install validation; bounded connections/transactions; coordinator initialization, snapshot and idle compare-and-set primitives. Exact commands, environment variables, restart/retention rules and table inventory are in the [service operational guide](../../services/content-api/README.md#e5-b1-persistence-and-runtime-roles). PostgreSQL is opt-in through the management Compose profile or explicit service target, preserving default SeaweedFS-only startup and its original credentials/image/volume.

| Migration | Operational tables / constraints |
| --- | --- |
| V1 — profiles and coordinator | storage_bindings, execution_profiles, execution_profile_storage, worker_instances, coordinator; singleton/installation/profile-digest constraints, bounded canonical byte evidence |
| V2 — sources and artifacts | sources, source_revisions, artifacts, artifact_dependencies; immutable identities, deferred current revision FK, exact byte/digest evidence and dependency-cycle check |
| V3 — runs and attempts | runs, run_sources, run_inputs, attempts, stage_invocations, stage_artifacts; seven states, captured immutable evidence, global active indexes, deferred composite ownership links and history/queue indexes |
| V4 — controls and audit | rss_deliveries, controls, command_bindings, audit_events; exactly three controls, exact original RSS Instant text, idempotency/FK bounds and append-only evidence |
| V5 — guards | Immutable UPDATE/DELETE/TRUNCATE guards, fixed identity columns, exact version increments and sealed terminal outcomes across those nineteen tables |

Canonical request/configuration encoding and semantic DTO validation remain B2 work; B1 stores exact supplied bytes and verifies bounds/JSON-object representation/digests, without defining identity through JSONB. Audit safe-detail construction, full source/run/control transitions, total admission counts and serialized artifact-edge insertion also remain future fixed-repository responsibilities. Schema readiness verifies supported history/checksums/tables/installation, not arbitrary hostile administrator DDL. A superuser can defeat database integrity guards and remains inside the trusted operator boundary.

**Role behavior tested:** analytics needs no DB, even with invalid database settings; API validates PostgreSQL without migration and retains the existing analytical beans; worker forces non-web startup even when servlet mode is supplied, creates no analytics controllers/query runner/attempt/worker registration, and remains idle; migration is non-web and exits after explicit setup. Worker path checks are not child runtime-version qualification or filesystem ownership. Only a migration that applies initial/pending schema files may initialize an absent coordinator; repeated migration cannot recreate a lost singleton. A crash after DDL commits but before bootstrap initialization therefore requires operator inspection rather than automatic repair. No dispatch, takeover, processor launch or native artifact mutation is implemented.

Local validation ran October 20–21, 2026 on macOS ARM64, GraalVM Java 21.0.7/Maven 3.9.16, Spring Boot 4.1.1 and official PostgreSQL 17.11 (Debian/Linux ARM64) in an isolated Docker service. Existing HTTP tests used Python 3.12.15/DuckDB 1.4.5 with previously generated synthetic Gold fixtures. No live publisher, retained personal data, AWS, large dataset or Spark processing run was needed.

| Executed command / check | Actual result |
| --- | --- |
| mvn -B -ntp -f services/content-api/pom.xml verify -Ppostgres, with dedicated DDIP_TEST_DB_* settings | **136 unit/component + 28 real PostgreSQL integration cases passed**, zero failures/errors/skips; final run 22.469 s, DB integration 12.96 s |
| PostgreSQL assertions | Clean/repeated migrations; checksum/future/pending/missing schema rejection; source/byte/digest/identity immutability; required unique/FK constraints; singleton/install validation; exactly one concurrent CAS winner; occupied-token/epoch rejection; deferred-constraint rollback with no partial rows; sealed terminal/audit evidence; unsupported states/controls and unsafe addresses rejected |
| Role/configuration assertions | Database-free analytics; API refuses empty/incompatible DB without creation; non-web idle worker/migrator; runtime path rejection; UTC sessions, pool/wait bounds; connection failure and secret-safe fixed diagnostics |
| Packaged migration and real Docker database restart | Migration exited 0; five SQL checksums and installed timestamps plus installation/mode/epoch/version were **identical** after restart and repeated command; persisted STOPPING/version 1 retained |
| Packaged API with test PostgreSQL stopped | Exit 1, fixed MANAGEMENT_UNAVAILABLE; checked absence of password, JDBC URL and native driver exception/refused-connection text |
| mvn -B -ntp -f services/content-api/pom.xml verify -Pintegration -Danalytics.python=the-existing-Python-3.12-executable | **136 unit/component + 20 HTTP/Python integration tests passed**, all four endpoints; synthetic output bytes/sizes/mtimes preserved |
| mvn -B -ntp -f services/api/pom.xml verify | **48 existing JDBC tests passed** |
| mvn -B -ntp -f ingestion/pom.xml verify; bash scripts/test-run-batch.sh | **29 ingestion tests and 5 launch-argument cases passed**; no live ingestion |
| Compose config checks | Both default Bronze-only service selection and management profile validated with dummy credentials; no operational service/data volume changed |
| npm run check; git diff --check; git diff --cached --check | Passed; 43 Markdown files, zero formatting/local-link issues, repository hygiene passed |

Tests caught and corrected the one-connection Flyway migration-pool deadlock, unqualified trigger search path and a possible lost-singleton reinitialization. The final migrations use a two-connection migration pool and explicit dependency-trigger schema resolution. Repeated testing counted unique cases above, not repeated executions as additional coverage. Expected invalid-configuration/failure logs, existing dynamic JVM-agent warnings and nonfatal HTTP 401 warnings from Flyway's upstream optional Maven metadata feeds appeared; all required dependencies resolved and builds passed. Native DB/driver exception chains are not exposed by B1 diagnostics.

**Tested subsets, not full E5 acceptance:** E5-01 persistence, E5-04 conditional-token/schema predicates, E5-10 unavailable-connection failure, E5-16 isolated restart and E5-18 affected Java/HTTP compatibility now have B1 evidence. None of E5-01–E5-18 is declared fully passed; actual source/run workflow, recovery transfer/lost-acknowledgment/process and full end-to-end acceptance remain unexecuted. Full Python/React suites were not run for this Java persistence-only checkpoint; their modules/contracts are unchanged. Hosted B1 CI remains pending an owner-managed PR run. CI preserves required job names and adds the real PostgreSQL service/test step to the existing processing job.

**Planned B2–F:** fixed canonical DTO encoder/source revisions and management transactions/APIs, qualified artifact imports, claims/host ownership/supervision, two processing adapters, RSS evidence, cancellation/retry/read-only reconciliation, complete failure injection and Operations UI. **Deferred:** pause/resume, hard filesystem quotas, comprehensive backup automation/restore certification, E4a benchmarks, E6 authorization/exports, E7 polish and any multi-worker/cloud system. Database ownership still does not prove filesystem writer termination. B1 is ready for owner review; no B2 behavior is authorized or implemented.

The dedicated test container is stopped; its separate `ddip-b1-postgres-test-data` volume and restart evidence are retained. The operational `ddip-management-postgres17-data` volume and existing SeaweedFS volume were not created, started, replaced or deleted by validation. Test reports/builds remain ignored; no secret/data/log artifact is added to Git. The owner-prepared index is unchanged and no staging, commit, push, branch operation or GitHub write was performed.

## Stage B2a source-management contract and evidence

B2a implements only persistent source management under the accepted architecture. It adds no run submission, artifact import, dispatcher, processing, RSS delivery, cancellation/retry/reconciliation workflow, activation or UI. The worker remains the B1 idle non-web role. Native publishers/readers, fingerprints, retained data and ADRs 0001–0009 are unchanged. Historical B1 results above are not counted as new B2a execution evidence.

### Fixed source DTOs and references

The six source endpoints and source-only capabilities endpoint in the REST table above are implemented in the `api` role. No other management capability is advertised. Request bodies remain exactly:

| Operation | Required body fields |
| --- | --- |
| POST /sources | productId, sourceId, displayName, enabled, config |
| POST /sources/{productId}/{sourceId}/revisions | displayName, config |
| PUT /sources/{productId}/{sourceId}/enabled | enabled |

productId is exactly content-intelligence or application-reliability. sourceId is the established ASCII identifier; displayName is nonblank text of 1–128 UTF-8 bytes, without control characters. Labels are untrusted display text, never interpreted as HTML, paths or executable content. Operators are responsible for choosing non-sensitive labels; there is no automated personal-data classifier. enabled is a JSON boolean. Missing, duplicate, extra, invalid-Unicode or coerced fields fail before mutation. Request bodies are bounded to 65,536 bytes including whitespace; only UTF-8 JSON is accepted, with no floats, BOM or trailing JSON. Read endpoints accept no body-driven selection.

config has exactly configVersion, profile, dataOrigin, feedEndpointId, registeredProfileSha256, inputStorageProfileId. configVersion is integer 1. The three supported profiles retain the accepted combinations above; inapplicable fields must be explicit JSON null. Endpoint/storage IDs are canonical lowercase UUID strings. Native profile hashes are exactly 64 lowercase hex characters. No URL, path, secret, extension bag, SQL or command field is accepted. Revisions cannot change product/source/profile-family identity; selecting JSONL versus Parquet for a different family requires a distinct source identity.

Full source data has exactly productId, sourceId, displayName, enabled, currentRevision, rowVersion, configSha256, createdAt, updatedAt, config. Creation returns 201; revision/gate commands and reads return 200. currentRevision is an integer 1–1000; rowVersion is a canonical decimal string, initially "0". Single source responses include ETag with the quoted rowVersion, for example `"0"`. Timestamps are UTC with exactly six fractional digits. GET /sources returns these summaries without config. Revision history entries have exactly productId, sourceId, revision, displayName, configSha256, createdAt, config; revision is an integer, not a live gate version. History provides old public configurations without resolved URLs/settings.

Success envelopes remain exactly apiVersion=1, data, meta={correlationId}; lists add limit, hasMore, nextCursor. Errors remain the fixed envelope/classes above. Correlation IDs are generated per HTTP request. There is no public exception detail, command record, raw endpoint URI or private binding address. Every encoded response is checked against 1 MiB before writing, with Cache-Control: no-store; no partial successful response is emitted.

### Reviewed server catalog and immutable endpoint meaning

The operator may configure an absolute DDIP_MANAGEMENT_CATALOG path to a private, bounded 64 KiB UTF-8 JSON file. It is loaded once at API startup, outside transactions; restart to apply reviewed changes. No file/network access occurs inside source mutation transactions. Blank configuration is a valid empty allowlist, preserving B1 startup; new source admission then fails until references are provisioned. Source/history inspection and matching durable command replay do not depend on current catalog membership.

The exact catalog object has catalogVersion=1, endpoints, reliabilityProfiles and storageProfiles. Each array has at most 128 entries. Entries have exactly:

| Array | Entry fields / policy |
| --- | --- |
| endpoints | endpointId, sourceId, uri; unique UUID endpoint ID, bound technical source ID, reviewed public HTTP(S) URI with host/valid port and no credentials, query or fragment. Literal IP/loopback/private local host names are not admitted; no test-only loopback feed profile is implemented here |
| reliabilityProfiles | sourceId, profile, dataOrigin, registeredProfileSha256; profile exactly request-jsonl-v2 or request-parquet-v1, origin exactly synthetic, hash of the exact independently reviewed native profile bytes under the existing canonical convention |
| storageProfiles | profileId, productId, role; unique UUID; role BRONZE for Content or BATCH for Reliability |

Catalog URI syntax follows RssSource/SourceCatalog restrictions and adds the reviewed public-host boundary. It performs no DNS resolution or fetch and does not claim general SSRF protection. The trusted operator must review host resolution and source policy. Reliability catalog entries attest an already reviewed, native-validated synthetic profile; this metadata is not raw-batch registration or proof of input integrity. Later imports/selection must independently verify the actual native source/profile hash/origin and retained evidence. Filename extensions never authorize a format.

A storage reference must also match an existing immutable storage_bindings row's ID, installation, product and role. B2a neither provisions roots through HTTP nor establishes filesystem/root readiness. Operator provisioning uses the private reviewed B1 installation bindings; absent/mismatched bindings reject admission. Do not insert dummy test bindings into an operational installation. Native storage qualification and artifact import remain B2b/C responsibilities.

Used RSS endpoint IDs have private immutable evidence: SHA-256 of canonical `{endpointId,sourceId,uri}` bytes in source_revisions. Under the coordinator lock, every newly admitted configuration checks all prior uses of that endpoint ID; rebinding it to different URI/source bytes fails STATE_CONFLICT. A correction needs a new reviewed endpoint ID and a new source revision. The URI itself is not copied into public source configuration, command snapshots or audit. Matching historical command replay remains historical, not permission to use a rebound endpoint. B2b/C must capture/recheck this private endpoint evidence against the reviewed catalog before authorizing a fetch. Legacy pre-B2a rows retain null endpoint evidence; no original URI is invented or old revision rewritten. Such rows alone are not qualified fetch evidence.

### Canonical identity, transactions and replay

SourceJson encodes only the bounded E5 object/array/string/integer/boolean/null trees. Object keys sort by Unicode code-point/UTF-8 order, strings use the Python-compatible JSON escapes, UTF-8 is not ASCII-escaped, arrays retain order, and there is no BOM/newline/float/Unicode normalization. Decoding explicitly rejects malformed UTF-8 and unpaired surrogates. SHA-256 covers exactly those bytes. Spring DTO defaults and PostgreSQL JSONB reserialization are not identity authorities. Existing publication algorithms are not changed.

Literal compatibility vectors, independently calculated with standard-library Python json.dumps(sort_keys=True,separators=(',', ':'),ensure_ascii=False,allow_nan=False), are checked by Java unit tests:

~~~json
{"configVersion":1,"dataOrigin":"publisher","feedEndpointId":"20000000-0000-4000-8000-000000000001","inputStorageProfileId":"30000000-0000-4000-8000-000000000001","profile":"rss-atom-v1","registeredProfileSha256":null}
~~~

The exact UTF-8 bytes above, excluding fence/newline, hash to `621880a6619c9d7dc570a9fd3bfba86909d6628872503d7d7cba3d30cc489f86`. The second literal vector is `{a:{U+E000:1,U+1F600:2},enabled:false,nullable:null,z:[backspace,newline,U+96EA,U+1F600]}` with those actual Unicode keys/values and standard JSON control escapes; exact canonical bytes are embedded in SourceValidationTest and hash to `c53c663783ba3275e52b23a16cfaa7c4163a7f8323557966c876b72e7ca0f885`. It exercises non-BMP key ordering, nulls, booleans, controls and preserved array order.

Each mutation uses B1's bounded ManagementTransactions: lock/validate coordinator installation first; look up durable command binding; then lock an existing source, check If-Match, validate current reviewed references and bounds, append/update source state, store response snapshot and append audit, all in one commit. No network/filesystem I/O or process launch is inside the transaction. Concurrent identical commands serialize to one logical mutation. Concurrent different revision commands against one version yield one winner and one 412. Source changes can occur while execution ownership is occupied; they never release/change ownership or alter a previously captured run revision. B2b source capture must use the same lock order and immutable revision FK.

Idempotency-Key is 1–128 ASCII alphanumeric/underscore/hyphen characters. Scope is SHA-256 of canonical `{action,productId,sourceId}` with action CREATE, REVISE or ENABLE, avoiding scope-length/cross-source/cross-operation collisions. Normalized request bytes add expectedVersion (decimal string or null for creation) and the exact validated body. The full bytes, not just their digest, are compared. A matching binding is resolved before checking whether the old If-Match version is stale or current references were removed. Changing If-Match is changed normalized content for the same key. Missing If-Match yields 428; malformed/weak/wildcard/multiple versions yield 400; genuinely stale new commands yield 412. A key with different normalized content yields 409 IDEMPOTENCY_CONFLICT.

Replay returns the original committed status, public data snapshot and ETag, even after later source changes, restart or lost response. meta.correlationId identifies the new HTTP request. Use GET to inspect the current source; a historical replay is not a fresh state observation. Snapshot schema/canonical bytes/config digest are validated before returning it. V5 UPDATE/DELETE guards protect bindings; V6's snapshot field permits null only for legacy records, which cannot be fabricated into successful source replay.

Creation appends revision 1/version 0. A changed name/config appends currentRevision+1 and increments rowVersion once. Identical current name/config is a no-op with unchanged timestamps/revision/version, but a new valid command still records its durable binding and audit. Reverting creates a new revision. Changed enablement increments only the live row version/time; same-value enablement is a no-op. Source count is globally capped at 128 and revisions at 1,000/source; no-op current commands remain permitted at the revision cap. New command/audit bounds are 200,000/1,000,000; future run admission must additionally reserve its recovery capacity. No history deletion/recycling is introduced.

Audit actions are SOURCE_CREATED, SOURCE_REVISED, SOURCE_UNCHANGED, SOURCE_ENABLED, SOURCE_DISABLED, SOURCE_GATE_UNCHANGED. Codes are CREATED, UPDATED or UNCHANGED. Rows contain local-operator scope, source/product, previous/resulting gate/version and UTC database time. Fixed details have commandId, correlationId, changed, previousRevision, resultingRevision; commandId links to command_bindings.response_object_id. There is no raw body, label, URL, credential or exception text in audit. Matching replay adds no duplicate audit. Any constraint/audit/commit failure rolls back the related source/revision/binding/audit together; an uncertain acknowledgment can be resolved only by repeating the same key/content when PostgreSQL is available.

### Pagination and local request policy

GET /sources allows only productId, enabled, limit, cursor; enabled is exactly true/false. GET revision history allows only limit/cursor; individual GETs and mutations accept no query fields. Duplicated/extra/blank/invalid parameters fail 400. Limit defaults to 20, range 1–100, with one-row lookahead. Source order is productId/sourceId in PostgreSQL C order, bounded by a captured immutable source_seq high-water. Later sources cannot enter an existing page chain; live gate changes may affect filtered membership, not promise a cross-request snapshot. Revision order is ascending revision, bounded by the first page's currentRevision; later revisions do not enter that chain.

Opaque version-1 cursors are <=512 bytes and bind endpoint/filter/order/position/high-water. HMAC-SHA256 detects tampering and wrong endpoint/filter use. DDIP_MANAGEMENT_CURSOR_KEY may supply a private base64url 32-byte key retained across restarts; blank uses a fresh process-local random key, so old cursors fail safely after restart while all database source/command state persists. The key signs pagination state only, not operators or commands; it is not authentication. No offset/arbitrary ordering/query language is implemented.

API management startup rejects non-loopback server.address. DDIP_MANAGEMENT_ORIGINS defaults to the configured API port on `http://127.0.0.1`; configure up to eight exact local HTTP(S) origins with explicit ports. The management filter checks one allowed Host, rejects unexpected Origin and all preflights, and requires an allowed Origin plus X-DDIP-Local-Request: 1 and application/json (optional UTF-8 charset only) for mutations. It rejects duplicate security headers and content encodings. No permissive CORS is enabled. The existing Vite /api/ same-origin proxy retains Host (changeOrigin:false); explicitly allow its 5173/4173 authority/origin when used. Direct cross-origin browser requests remain unsupported. These protections do not authenticate other local processes; E6 is still required for shared exposure. Existing /api/v1/ responses/policies remain unchanged.

### Migration and validation boundary

V6 only adds immutable source_seq, immutable command response_data, and private endpoint_id/endpoint_sha256 revision evidence plus its index/pair constraint. V1–V5 are unchanged; nineteen tables remain. Explicit migration is required before API startup; no automatic migration is added. SchemaLifecycle recognizes V6 and initializes a coordinator only for a new schema, never merely because a later migration was applied. Tests cover a healthy V5 upgrade preserving checksums/timestamps/coordinator state and an incomplete V5 installation whose missing singleton must not be recreated by V6.

The following executed evidence covers B2a, separately from historical B1 results. This is partial evidence for E5-01/E5-02/E5-14/E5-17/E5-18, not completion of those whole groups. B2b/C–F and hosted CI remain pending.

### Executed B2a validation — October 21, 2026

Local environment: macOS ARM64, Java 21.0.7, Maven 3.9.16, Spring Boot 4.1.1 and dedicated official PostgreSQL 17.11 Linux ARM64 container. Existing synthetic HTTP fixtures use Python 3.12.15/DuckDB 1.4.5. No live RSS, raw-batch import, retained production-like data modification, Spark or benchmark workload was used.

| Exact command/check | Actual local result |
| --- | --- |
| mvn -B -ntp -f services/content-api/pom.xml verify -Ppostgres, with isolated DDIP_TEST_DB_* settings | **180 unit/component tests + 63 real PostgreSQL integration cases passed**, zero failures/errors/skips. Includes all 136 B1/legacy unit cases and 28 B1 PG cases; 44 new source-validation unit cases and 35 source-management integration cases. Final run 37.217 s; source integrations 15.51 s, B1 PG 12.03 s |
| Source/canonical correctness | RSS and both synthetic formats; independent literal Python-compatible hashes above; exact field/type/null/Unicode/UTF-8 limits; profile/source/storage catalog membership; source creation/history/no-op/revert/gate; profile-family immutability; immutable DB evidence and captured results |
| Transaction/concurrency/replay | Concurrent identical commands commit once; two revisions against one version yield 200/412; audit-trigger failure rolls back all four row groups; discarded committed response can be replayed; replay after later revisions/catalog removal/restart preserves original result; operation/source scopes and conditional content conflict correctly |
| HTTP/security/resource cases | Real loopback HTTP and MockMvc; fixed envelopes; Host/Origin/custom-header/JSON/preflight/duplicate-header checks; Vite-preserved Host; 64 KiB body and actual 1 MiB response bound; 128-source/1,000-revision bounds; 100-row lookahead; source/revision high-water/tamper/filter checks and signed cursor across restart; corrupt stored config/snapshot never leaks private fixture values |
| PostgreSQL outage/upgrade | Live isolated test DB temporarily refuses connections; GET and mutation return safe 503 without partial rows. V5-to-V6 upgrade preserves old checksums/timestamps and coordinator state; missing prior singleton is never recreated. Used endpoint URI rebinding fails 409, while a newly reviewed ID/revision succeeds |
| mvn -B -ntp -f services/content-api/pom.xml verify -Pintegration -Danalytics.python=the-existing-absolute-Python-3.12-path | **180 unit/component + 20 existing real HTTP/Python analytics tests passed**; all four endpoints and corrupt-publication cases, synthetic publication bytes/sizes/mtimes unchanged |
| mvn -B -ntp -f services/api/pom.xml verify | **48 JDBC compatibility tests passed** against existing synthetic publications |
| npm run check; git diff --check; git diff --cached --check | Passed after fixing one documentation bare-URL lint issue; 43 Markdown files, zero formatting/local-link issues; repository hygiene passed |

Independent test expectations include literal hashes, exact status/data/count/audit assertions, database-side immutability and real concurrent transactions, not mock-only repository outcomes. Initial testing corrected a test assertion to exercise the source sequence's DEFAULT update (PostgreSQL itself rejects direct identity assignment earlier). Review also added the upgrade bootstrap guard, immutable endpoint binding evidence and stored response/schema checks. No applied B1 migration was edited and no test was weakened to accommodate resources.

The existing CI postgres profile automatically includes SourceManagementIT; no CI job names, service pins or dependencies changed. Hosted CI remains **pending owner publication**, not observed as passed. Nonfatal existing optional Maven metadata-feed 401 and JVM agent warnings remain; negative tests intentionally emit fixed startup-failure messages. Full Python/Spark/React suites were not run for this Java-only metadata/API checkpoint; their code/contracts are unchanged. The actual Vite server/browser UI was not exercised; its preserved-Host proxy contract was tested directly through the backend.

B2a is implemented and locally tested; no whole E5 acceptance group is claimed complete. B2b still owns run schemas/submission, exact source/revision/private-endpoint capture and qualified artifact imports; C–F own host ownership, native processing, RSS recovery, controls and Operations. Catalog/native source truth and non-sensitive labels remain trusted operator responsibilities. Root readiness is not established by source metadata. Legacy null endpoint evidence is not fetch-ready; no automated repair/backfill is performed. Default ephemeral cursors expire on restart unless a persistent signing key is configured. E6 authentication/exports, E4a benchmarks and automatic scheduling remain deferred.

## Stage B2b-1 inspection and owner-review checkpoint

Historical inspection checkpoint, before the subsequent B2b-1A approval/implementation below. At that point the scope was verified imports of completed native inputs only, authorized October 21, 2026. Repository entry was clean, including the index, at `b0ffd78eb8e9597d74241c679c2b473facec12ac` on the existing owner-prepared `feature/e5-durable-execution-source-controls` branch. Historical B1/B2a results above are separate from the checks recorded here.

**Implemented at this checkpoint:** inspection documentation and [Proposed ADR 0011](../adr/0011-artifact-import-maintenance-ownership.md). No import application role, catalog parser, Python wrapper, host guard, registry repository or migration has been implemented. B2b-1 and durable execution are incomplete.

### Coordination blocker and proposed correction

The accepted boundary requires one durable maintenance/import owner in the same coordinator slot. Current V1–V6 can bind only a run attempt: V1 pairs worker owner/attempt fields, V3 adds the composite attempt/instance/epoch FK, V4 makes recovery controls run/attempt-specific, and V5 seals the coordinator field set. B1's idle-mode update checks only `active_attempt_id IS NULL`, so STOPPING without an attempt is still considered idle. A mode flag/host lock alone cannot bind an import identity or resolve its abandoned ownership. Creating a synthetic run/attempt would violate this checkpoint.

ADR 0011 proposes a forward V7 with one import-operation intent/result table, an exclusive import-owner pointer on the existing coordinator, conditional terminalization and explicit quiescence-checked local closure of abandoned imports. It retains existing coordinator modes, run/attempt/control states, short transactions and the existing artifact registry. This adds an ownership/recovery family beyond the accepted attempt-only transaction contract, so it is proposed for owner approval before implementation. The accepted ADR and V1–V6 are unchanged. No SQL migration is supplied or applied by this checkpoint.

An additional persistence requirement is native-identity uniqueness: V2's native-identity index is nonunique, while location uniqueness and immutable evidence guards already exist. The proposal specifies an additive native key constraint, failing on existing duplicates without automatic repair. Registry commit must serialize dependency insertion under the coordinator and commit the import result/audit in the same transaction.

### Native verification feasibility and boundaries

| Selected input | Existing authoritative interface | Requirements for the future importer |
| --- | --- | --- |
| Reliability JSONL v2 registered batch | `reliability_format_inputs.open_batch(root, 'JSONL', source_id, batch_id, expected_fingerprint)` | Spark-free. V2 manifest/completion, exact source/batch/format, reviewed v2 profile, inventory/hashes/lengths and actual physical record counts are verified. Match registered profile bytes/hash to the approved binding; never route a v1 registration through this interface |
| Parquet-v1 registered batch under Reliability v2 profile | Same `open_batch` with `'PARQUET'`; `reliability_format_parquet` | Spark-free; requires pinned DuckDB 1.4.5. Checks literal typed schema, DuckDB writer provenance, decoder/profile, footer/row-group limits and raw physical records, rather than a filename extension |
| Publisher-reference snapshot | `publisher_reference.open_snapshot(root, reference_id, revision, expected_fingerprint)` | Spark-free. Verifies original manifest/completion, exact identity, CSV schema/count/hash and retained vocabulary compatibility; no authoring/upload interface |
| Service-reference snapshot | `service_reference.open_snapshot` with the same identity arguments | Spark-free. Independent complete snapshot and retained CSV/vocabulary validation; no service-reference creation by imports |
| Completed Silver | `silver_inputs.load_inputs(spark, root, markers=[exact_relative_marker])`, then `verify_unchanged()` | Requires Spark, including physical schema checks and full row/provenance/replay/count validation. No source discovery or globs. Add bounded read-only capture of original marker bytes, every accepted/rejected file and permitted bookkeeping, directory/file identities, lengths and hashes before/after qualification. Reject aliases/symlinks and extra files at the stronger managed boundary |

Silver's current standalone reader permits in-root aliases and excludes hidden Spark bookkeeping from its analytical inventory. Managed import must capture/recheck bookkeeping and reject path aliases, rather than silently claiming the existing reader already enforces the stronger E5 boundary. Its broad INPUT_CHANGED failure can include inability to reread; use independent managed observations to distinguish demonstrated mutation from unavailable/invalid evidence and retain the native code.

Under accepted ADRs 0003/0004 the Silver reader validates retained Bronze provenance in the marker/rows; it does not reopen original Bronze objects or prove their current readability/authenticity. No such Bronze verification was implemented or claimed here. Required retained dependencies must be explicit per native contract; embedded snapshot vocabulary/raw batch files must remain available. Do not invent batch-to-reference edges when a native registered batch has no reference dependency. Where a selected contract requires retained external evidence, its absence must fail before import commit.

The existing `storage_bindings`/`execution_profiles` tables store immutable bounded private configuration bytes, not an implemented import root/runtime resolver. B2a's reviewed source catalog approves RSS endpoints, Reliability profiles and BRONZE/BATCH IDs; it does not validate filesystem root readiness. The future importer needs fixed versioned private binding/catalog schemas, installation/root/file identity checks, exact selections and bounded subprocess protocol. The existing analytical process runner has query-specific limits and best-effort descendant cleanup; it is not proof of safe Spark cleanup or durable import ownership.

### Planned import contract after ownership approval

Acceptance is the requested B2b-1 matrix: full native qualification before trust, immutable native identity/evidence, idempotent fully reverified imports, native/location conflicts, atomic dependency/result/audit persistence, fail-closed missing/corrupt/changed/I/O evidence, bounded operator summaries and crash-safe exclusion. Re-import must compare exact immutable evidence and dependencies and reuse the existing opaque artifact ID only after fresh verification. Historical `verified_at` is qualification history, never permanent readability or integrity. A Silver E5 inventory digest must be labeled operational evidence, not an original Silver publication fingerprint.

Use only local non-web `operator-import` from the existing packaged Spring application, private operator-supplied exact catalog, fixed kinds and server-owned storage/runtime bindings. No client commands/absolute artifact paths, HTTP import endpoint, directory/latest scanner, package installation or shell interpolation. Verification remains outside SQL transactions. The [operational checkpoint](../../services/content-api/README.md#e5-b2b-1-import-review-checkpoint) distinguishes the intended role from currently runnable commands. Approval/testing must precede documenting a working import invocation.

### Executed validation and remaining scope

Local environment: macOS ARM64; Java 21.0.7 explicitly selected for Maven/Spark (the shell's Maven default would otherwise use Java 27); Maven 3.10.0; Python 3.12.15; DuckDB 1.4.5; PySpark 4.0.2; official PostgreSQL 17.11 in the existing isolated `ddip-b1-postgres-test` container on loopback port 15432. Node 26.12.0 ran the installed documentation checks; the repository's constrained Node 24 environment was not requalified. No dependencies were installed or changed.

| Actual command/check | Outcome |
| --- | --- |
| `mvn -o -B -ntp -f services/content-api/pom.xml test` | Passed: 180 unit/component tests, zero failures/errors/skips. Includes database-free analytics startup and deliberate rejection of the unavailable import role |
| `mvn -o -B -ntp -f services/content-api/pom.xml verify -Ppostgres` | Passed: 180 repeated unit/component tests plus 63 real PostgreSQL tests (28 PersistenceIT, 35 SourceManagementIT), zero failures/errors/skips. Tests create/drop only their own random databases |
| Disposable V1–V6 PostgreSQL ownership probe | Passed 6 diagnostic assertions: no import-owner column; deferred attempt-owner FK exists; B1 idle predicate can change STOPPING back to READY; invented MAINTENANCE mode rejected; owner without an attempt rejected; same native identity/different location and evidence accepted by current SQL constraints. Probe transaction rolled back and only its generated database was dropped. These observations confirm current limitations, not successful import guarantees |
| `mvn -o -B -ntp -f services/content-api/pom.xml verify -Pintegration -Danalytics.python="$PWD/outputs/silver-python312/.venv/bin/python"` | Passed: 180 repeated unit/component tests plus 20 HTTP/Python compatibility tests, zero failures/errors/skips; existing synthetic fixtures used without regeneration |
| Python `-m unittest discover -s processing/tests -p test_reliability_format_inputs.py -v` | Passed: 10 existing tests, actual JSONL v2/Parquet profile registration/opening, format/provenance/physical positions and error boundaries |
| Same discovery with `test_publisher_reference.py` | Passed: 46 existing tests, actual reference opening, vocabulary/schema/identity/retention and byte/mtime preservation/error boundaries |
| Same discovery with `test_reliability_inputs.py` | Passed: 17 existing tests, including actual service-reference opening/replay/corruption and legacy v1 compatibility; v1 acceptance here does not authorize v1 E5 import |
| Same discovery with `test_silver_inputs.py` | Passed: 19 existing Spark tests, actual completed Silver counts/schema/provenance/replay/inventory and mutation checks. Synthetic fixtures only; no stronger managed-import capture is claimed |
| Same discovery with `test_reliability_format_verification.py` | Passed: 15 existing independent verifier/retained-dependency/read-only replay tests. Completed output fixtures remain native reader regression evidence, not authorization to import those output kinds |
| `npm run check` | Passed: 44 Markdown files, zero lint/local-link issues; repository hygiene passed |
| `git diff --check`, `git diff --cached --check` | Passed; index unchanged and empty |

Python commands used the existing `outputs/silver-python312/.venv/bin/python`, `PYTHONPATH=processing`, and, for Spark, JAVA_HOME/PATH for Java 21, PYSPARK_PYTHON pointing to that executable, SPARK_LOCAL_IP=127.0.0.1 and TZ=UTC. The [operational guide](../../services/content-api/README.md#e5-b2b-1-import-review-checkpoint) gives the reproduction commands. Final test commands passed; the first restricted Maven invocation failed before running tests because its cache lock directory was outside the writable sandbox. The same command passed with authorized cache/local-socket access; no assertion or runtime setting was weakened. Existing expected negative-startup/Spark diagnostics did not fail tests.

The six database probe observations can also be inspected in a disposable V1–V6 test database. This fragment demonstrates the consequential idle predicate and native index properties; roll back all mode changes and never run a diagnostic mode update against an operational installation:

~~~sql
BEGIN;
SELECT column_name FROM information_schema.columns
WHERE table_schema='ddip_management' AND table_name='coordinator'
ORDER BY ordinal_position;
SELECT pg_get_constraintdef(oid) FROM pg_constraint
WHERE conrelid='ddip_management.coordinator'::regclass
  AND conname='coordinator_attempt_owner_fk';
SELECT indexdef FROM pg_indexes WHERE schemaname='ddip_management'
  AND indexname='artifacts_native_identity_idx';
-- Fresh synthetic coordinator starts READY/version 0 with no attempt.
UPDATE ddip_management.coordinator SET mode='STOPPING',row_version=row_version+1
WHERE singleton_id=1 AND row_version=0 AND active_attempt_id IS NULL
RETURNING mode,row_version;
UPDATE ddip_management.coordinator SET mode='READY',row_version=row_version+1
WHERE singleton_id=1 AND row_version=1 AND active_attempt_id IS NULL
RETURNING mode,row_version;
ROLLBACK;
~~~

All 107 native-reader tests above are existing baseline cases; repeated Maven unit suites are not counted as additional unique coverage. No import ownership, native import acceptance, registry import idempotency, process exclusion or import crash guarantee has been tested because those features remain unimplemented. Full processing/ingestion/frontend regressions were not run at this documentation-only stop. Hosted CI remains unobserved.

Only four documentation files changed: this contract, the service operational guide, Proposed ADR 0011 and the ADR index. No production code, V1–V6, dependency, CI, native contract, retained personal dataset or Git index was changed. Logs/build outputs remain ignored under outputs/target. The dedicated PostgreSQL test container was restored to its original stopped state with its volume retained; operational PostgreSQL, unrelated containers and SeaweedFS were not started or altered. Docker Desktop was started for the local test service. No Git/GitHub mutation, live publisher request, AWS resource, benchmark workload or operational processing run occurred.

**Planned B2b-1:** owner-approved maintenance protocol/migration, role/catalog/profile resolver, bounded native qualification/evidence capture, transactional registry identity/result/audit repository and the complete synthetic/native/PostgreSQL/process failure matrix. **B2b-2 dependencies:** completed B2b-1; opaque artifact-ID selection/read APIs as authorized, exact source/profile/private endpoint capture, use-time revalidation of registry evidence and dependencies, and admission/idempotency transactions. Do not accept arbitrary filesystem selections or treat historical qualification as fresh verification. **Deferred:** run submission/dispatch, managed publication/worker execution, run controls, Operations UI and all other exclusions in the B2b-1 request. No whole E5 acceptance group or hosted CI result is claimed.

## Stage B2b-1A durable import ownership

Owner approval: October 21, 2026, recorded in accepted additive ADR 0011. Entry was clean on feature/e5-durable-execution-source-controls at `58b4e5f966d231f318a981b4b8262f2af644a1fa`. This checkpoint implements coordination only. No native verifier, import command, HTTP import route, importer artifact insertion, processing submission or dispatch was added. The operator-import role remains deliberately unavailable; worker remains idle. Historical inspection/native-reader results above are not B2b-1A execution evidence.

### Schema and compatibility

V7 adds one operational table, artifact_import_operations, bringing the schema to twenty operational tables. It stores opaque operation/process IDs; IMPORT/RECOVERY kind; unique predecessor link; installation/profile/host-binding identity; PID/start instant; epoch; immutable canonical request bytes/hash (64 KiB maximum); state/version; timestamps and terminal result bytes/hash (64 KiB maximum). States are exactly ACTIVE, SUCCEEDED, FAILED, INTERRUPTED. Initial state/version must be ACTIVE/0; only version-incremented ACTIVE-to-terminal transitions are allowed. Identity, terminal results and history reject mutation/deletion/truncation. A unique partial index permits one ACTIVE import globally.

The singleton gains active_import_id with a deferred operation/installation/epoch FK. Attempt and import ownership are mutually exclusive. Immediate guards serialize import/active attempt/active run admission on the coordinator; deferred guards require the final ACTIVE operation and pointer to agree. An INTERRUPTED operation must have a successor. These permit an atomic occupied-slot transfer without permitting orphan ACTIVE imports or direct clearing of a live import. Existing attempt-owner FK, run states/control enums, coordinator installation/mode/epoch/version and old evidence remain intact. STOPPING keeps its original shutdown meaning; the pointer owns maintenance independently of mode.

Audit events gain an optional import_id FK/index. Fixed actions are IMPORT_RESERVED, IMPORT_FINISHED, IMPORT_INTERRUPTED and IMPORT_RECOVERY_RESERVED. Details contain only protocolVersion and, for handoff, relatedOperationId; fixed codes/states, opaque IDs, versions and DB timestamps complete the audit. Requests/results and private root/process configuration are not copied into audit. Reservation, result and handoff audit failures roll back their related operation/pointer changes.

Artifacts gain native_format and UNIQUE NULLS NOT DISTINCT `(product_id,kind,contract_version,processor_revision,native_format,native_identity)`. JSONL/PARQUET is required for RELIABILITY_BATCH_V2/RELIABILITY_V2; other kinds require null. Same identity text can recur in distinct native namespaces. Location uniqueness and immutable evidence guards remain. This is metadata integrity, not native qualification or authorization for derived-output imports.

V7 first checks existing rows: duplicate non-format scopes raise ARTIFACT_NATIVE_IDENTITY_CONFLICT. Existing Reliability rows have no reliable V6 format field and raise ARTIFACT_NATIVE_FORMAT_UNRESOLVED rather than guessing from strings, extensions or native manifests. Either condition aborts the entire migration, retaining rows/history and leaving no partial V7 table/column. Such installations require owner review; no backfill, deletion, merge or guard disabling is supplied. V1–V6 remain byte-for-byte unchanged. Migration remains explicit with application processes stopped; missing prior coordinator evidence is never bootstrapped by upgrade. Runtime schema validation now requires all seven migrations and the new table.

### Internal protocol and integration interfaces

InstallationHostGuard.acquire(binding) requires a pre-provisioned private local POSIX control root (0700), host.guard and installation.id (0600, regular, single-link files). The binding fixes installation UUID, absolute canonical root and device/inode identities of root/guard/installation file. installation.id contains exactly the canonical UUID plus newline. The code never creates, unlinks or recreates these entries. It checks owner/permissions, rejects symlinks/hard links/path aliases, takes a lifetime FileChannel tryLock and rechecks identities. Closing the guard never changes PostgreSQL ownership. The corresponding immutable CONTROL storage row must contain the exact canonical Binding.descriptor() bytes/hash and belong to the installation; execution_profile_storage must authorize it for the exact immutable execution-profile revision. Earlier import/worker host evidence cannot silently move to another guard. There is no new provisioning command or imported catalog parser.

ImportOwnershipRepository is an internal JDBC bean in existing DB roles, without a controller, lifecycle task, scheduler or command-line role. Its calls reject an enclosing transaction so external qualification/process inspection cannot be wrapped in a long outer SQL transaction:

1. reserve(guard, Reservation) checks the held guard/installation, locks coordinator, requires READY plus no execution/recovery/import owner, verifies approved profile/control binding, inserts the new immutable intent, increments epoch, binds the pointer and appends audit. It returns Token only after commit. Coordinator mode is unchanged. No native work occurs.
2. Token carries operation ID, process-instance ID, epoch, operation version and coordinator version. finish requires these exact fields and held host identity, plus the caller's independently established safe-to-release confirmation. It atomically seals result/audit and releases this pointer, preserving mode/epoch. A zero match/stale token stops; it is never refreshed/retried automatically. A false cleanup confirmation leaves ACTIVE ownership intact. IMPORT permits SAFE_COMPLETION/SUCCEEDED or SAFE_FAILURE/FAILED; RECOVERY permits only RECOVERY_CLOSED/SUCCEEDED. Those outcomes describe coordination, not qualified artifacts in this checkpoint.
3. lookup(operationId, exactRequestBytes) is a historical observation for lost acknowledgment. It compares the full canonical request, returns retained state/result and never launches or resumes anything. A completed operation ID cannot be rebound. A fresh future import requires a fresh operation ID and complete native revalidation; it may reuse artifact IDs only through B2b-1B's verified identity comparison.
4. Actual artifact/dependency writes are absent. B2b-1B must add them inside the fenced finish transaction, before result/audit/release; calling the current finish and then inserting artifacts in another transaction would violate the accepted atomicity contract. B2b-1B also owns strict catalog semantics/count limits, native evidence capture, bounded verifier supervision and proof of cleanup before requesting release.

### Abandoned ownership and fenced recovery

A crash, lost DB session/acknowledgment, expired heartbeat or free host guard leaves ACTIVE ownership occupied. No automatic heartbeat/timeout takeover or shutdown hook clears it. If a write commits but its acknowledgment fails, callers query the same operation ID/request to determine whether reservation/result/handoff committed; unknown connectivity continues blocking.

After actual installation-level old-writer quiescence, handoff requires the same guard, explicit trusted-operator confirmation and an observable process-identity check outside SQL transactions. A live PID with matching start instant, unavailable live-process start identity or denied process inspection vetoes transfer. A reused PID is not identified as the old process. Absence of the parent is insufficient by itself: operator confirmation must cover orphan descendants and unmanaged writers. This is an explicit trust boundary, not automated process containment.

One short transaction locks/rechecks the exact old operation/coordinator token, seals old state INTERRUPTED with successor evidence, inserts an ACTIVE RECOVERY successor retaining the original request/profile/host binding, increments epoch, replaces the occupied pointer and appends both audits. No intermediate idle pointer is written or exposed. Rollback leaves the old owner ACTIVE. An uncertain commit is resolved by the same successor ID; a second blind successor cannot be created. Stale owners cannot finalize. A separate safe recovery finalization releases only the successor. If recovery itself is abandoned, the same fenced handoff can append another successor; history is retained and no native reconciliation/publication occurs.

### B2b-1A validation and limits

Executed locally on macOS ARM64, Java 21.0.7, Maven 3.10.0 and the existing dedicated official PostgreSQL 17.11 Linux ARM64 test container on 127.0.0.1:15432. HTTP compatibility used the existing Python 3.12.15/DuckDB 1.4.5 environment and synthetic fixtures without regeneration. Documentation checks used installed Node 26.12.0; the constrained Node 24 environment and hosted CI were not observed in this checkpoint. No dependency/service was added or package installed.

| Exact command/check | Actual result |
| --- | --- |
| `mvn -o -B -ntp -f services/content-api/pom.xml test` | Passed: 189 unit/component tests, including 9 new guard/request tests; zero failures/errors/skips |
| `mvn -o -B -ntp -f services/content-api/pom.xml verify -Ppostgres -Dit.test=ImportOwnershipIT` | Passed: 42 new real PostgreSQL tests plus 189 repeated unit/component tests; zero failures/errors/skips |
| `mvn -o -B -ntp -f services/content-api/pom.xml verify -Ppostgres` | Passed: 106 PostgreSQL tests (42 ownership, 28 B1 persistence, 36 B2a source management) plus 189 repeated unit/component tests; zero failures/errors/skips |
| `mvn -o -B -ntp -f services/content-api/pom.xml verify -Pintegration -Danalytics.python="$PWD/outputs/silver-python312/.venv/bin/python"` | Passed: 20 existing HTTP/Python compatibility tests plus 189 repeated unit/component tests; zero failures/errors/skips |
| `npm run check` | Passed: 44 Markdown files, zero lint/local-link issues and repository hygiene passed |
| `git diff --check`, `git diff --cached --check` | Passed; index unchanged and empty |
| Byte comparison against entry commit | Accepted ADRs 0001–0010 and V1–V6 unchanged (16 files) |

Commands explicitly selected Java 21 via JAVA_HOME/PATH. PostgreSQL credentials were loaded privately from the existing dedicated test container into test environment variables; no secret appeared in command arguments or reports. The guide supplies the equivalent DDIP_TEST_DB_URL/USER/PASSWORD/PYTHON setup. All databases use random test-only names and are dropped by their fixtures. The dedicated container was restored to its original stopped state with its volume retained; operational PostgreSQL, SeaweedFS and unrelated containers were not started/altered.

Coverage includes clean and populated V6 upgrades (including occupied recovery ownership), unchanged migration checksums/timestamps/evidence bytes, conflicting migration rollback, both directions of execution/import exclusion and concurrent admission, exact owner/version/epoch rejection, immutable terminal results, request/result bounds/digests, audit rollback, DB outage with retained owner, actual competing JVM locks, abrupt owner death, live-owner refusal despite a free guard, explicit quiescence refusal, atomic occupied-to-occupied handoff, repeated recovery and lost acknowledgment. A connection wrapper throws SQLSTATE 08006 only after a real PostgreSQL write commit during reservation/finalization/handoff; exact lookup observes that commit and duplicate mutations remain blocked. Concurrent readers observed only occupied ACTIVE owner bindings during transfer. The new B2a case proves source creation/revision/idempotent replay leaves maintenance ownership unchanged. Historical repeated unit executions are not counted as additional distinct tests.

Initial new-test failures were assertion issues: Flyway includes a schema-history creation row without a version, and Java byte-array values in JDBC maps require content comparison. Tests now compare the seven versioned migrations and recursively compare exact retained evidence; no migration/ownership constraint or acceptance condition was weakened. The historical V5 upgrade fixture now uses V5 SQL to construct its old coordinator before running the V7 repository. Expected negative-startup and intentionally rejected migration diagnostics appeared; final commands passed.

Coordination fixtures contain synthetic metadata only, including explicitly labeled direct-SQL registry rows for uniqueness tests. No importer inserts unverified native artifacts. No import-native qualifier, Spark workload, publisher request, operational processing run or cloud resource was used. Existing HTTP regressions still exercise their native Gold reader. Existing Python/native contracts are unchanged. Native-import qualification, child-verifier cleanup/containment, filesystem integrity under actual qualification, full ingestion/processing/frontend regressions and hosted CI remain unexecuted for B2b-1A.

The guard supports trusted cooperative local POSIX hosts, tested here on macOS; Linux/other-host execution remains unobserved until CI/owner validation. It is not hostile-filesystem protection, distributed fencing or proof of orphan termination. Process observation plus an operator flag cannot automate installation-level quiescence; actual inspection remains mandatory. Unknown evidence/cleanup/acknowledgment keeps ownership occupied. Existing unscoped Reliability registry rows require owner review before V7 can apply. Comprehensive backup/restore and power-loss durability remain outside this checkpoint.

B2b-1A is implemented and locally tested. B2b-1B remains pending owner review: provide a strict private catalog/runtime/storage resolver, actual native qualification and before/after capture, bounded child supervision/cleanup, current-readability checks and atomic artifact/dependency insertion inside the fenced finalization transaction. Keep the same guard, opaque operation identity and exact-request lookup. B2b-2 and all processing/dispatch remain unauthorized; no whole E5 acceptance group is marked complete.

### B2b-1A changed files

- [docs/adr/0011-artifact-import-maintenance-ownership.md](../../docs/adr/0011-artifact-import-maintenance-ownership.md)
- [docs/adr/README.md](../../docs/adr/README.md)
- [docs/architecture/durable-execution.md](../../docs/architecture/durable-execution.md)
- [services/content-api/README.md](../../services/content-api/README.md)
- [services/content-api/pom.xml](../../services/content-api/pom.xml)
- [services/content-api/src/main/java/platform/contentapi/execution/CoordinatorRepository.java](../../services/content-api/src/main/java/platform/contentapi/execution/CoordinatorRepository.java)
- [services/content-api/src/main/java/platform/contentapi/execution/DatabaseConfiguration.java](../../services/content-api/src/main/java/platform/contentapi/execution/DatabaseConfiguration.java)
- [services/content-api/src/main/java/platform/contentapi/execution/ImportOwnershipFailure.java](../../services/content-api/src/main/java/platform/contentapi/execution/ImportOwnershipFailure.java)
- [services/content-api/src/main/java/platform/contentapi/execution/ImportOwnershipRepository.java](../../services/content-api/src/main/java/platform/contentapi/execution/ImportOwnershipRepository.java)
- [services/content-api/src/main/java/platform/contentapi/execution/InstallationHostGuard.java](../../services/content-api/src/main/java/platform/contentapi/execution/InstallationHostGuard.java)
- [services/content-api/src/main/java/platform/contentapi/execution/JdbcCoordinatorRepository.java](../../services/content-api/src/main/java/platform/contentapi/execution/JdbcCoordinatorRepository.java)
- [services/content-api/src/main/java/platform/contentapi/execution/SchemaLifecycle.java](../../services/content-api/src/main/java/platform/contentapi/execution/SchemaLifecycle.java)
- [services/content-api/src/main/resources/db/migration/V7__import_maintenance_ownership.sql](../../services/content-api/src/main/resources/db/migration/V7__import_maintenance_ownership.sql)
- [services/content-api/src/test/java/platform/contentapi/execution/ImportOwnershipIT.java](../../services/content-api/src/test/java/platform/contentapi/execution/ImportOwnershipIT.java)
- [services/content-api/src/test/java/platform/contentapi/execution/InstallationHostGuardTest.java](../../services/content-api/src/test/java/platform/contentapi/execution/InstallationHostGuardTest.java)
- [services/content-api/src/test/java/platform/contentapi/execution/OwnershipFixtures.java](../../services/content-api/src/test/java/platform/contentapi/execution/OwnershipFixtures.java)
- [services/content-api/src/test/java/platform/contentapi/execution/OwnershipTestChild.java](../../services/content-api/src/test/java/platform/contentapi/execution/OwnershipTestChild.java)
- [services/content-api/src/test/java/platform/contentapi/management/SourceManagementIT.java](../../services/content-api/src/test/java/platform/contentapi/management/SourceManagementIT.java)

## B2b-1B-A implementation acceptance

Authorized scope: Spark-free imports of completed Reliability v2 JSONL/Parquet registered batches and publisher/service reference snapshots only. Before calling this checkpoint implemented, actual native readers must qualify synthetic fixtures, the fixed non-web operator role must use reviewed immutable bindings, re-import must reverify exact evidence, and real PostgreSQL must prove atomic registry/result/audit persistence under V7 ownership. Qualification/rechecks must occur outside SQL transactions. Native payloads and full manifests must not enter protocol responses or the registry. Uncertain child cleanup/commit must preserve ownership; exact operation lookup and explicit fenced recovery must remain available. Silver, derived publications and run submission remain excluded. Current executed evidence will be recorded after implementation/testing.

### B2b-1B-A implemented boundary

The owner separately authorized Spark-free native import implementation. The current [operator guide](../../services/content-api/README.md#spark-free-native-input-imports-b2b-1b-a) supersedes historical statements above that the import role/catalog are unavailable. Accepted ADRs 0010/0011 and V1–V7 remain unchanged; no additional migration or alternate registry is required. The seven run states and processing-run controls remain untouched.

`operator-import` forces a non-web context without analytics controllers, query pools or worker lifecycle. A private, versioned, strict 64-KiB catalog selects 1–16 exact approved namespaces. Canonical normalized order and a caller-chosen UUID bind the durable command. The server resolves immutable installation/profile/storage/source-revision bindings; native paths and executables cannot be supplied in catalog entries. A module-hash-bound Python 3.12.15/DuckDB 1.4.5 adapter uses the actual public native readers for JSONL-v2/Parquet-v1-under-v2 registered batches and the separate publisher/service reference implementations. It never imports Spark or runs a producer.

Before/after capture includes exact completion bytes, all required file lengths/hashes, original registered inventories/physical counts/Parquet groups, reference CSV/vocabulary descriptors, directories and filesystem identity/stamps. These are bounded metadata, not native payloads or full manifests. A second supervised recheck precedes finalization. Java validates the fixed response identity/schema/exit agreement and evidence consistency. Native invalid/unavailable/changed/unsupported/limit classifications remain distinct; safe native codes are allowlisted. Runtime/root changes, missing or unexpected files, symlinks, substituted evidence and ambiguous I/O fail closed.

The existing host guard and V7 reserve/lookup/finish/handoff own coordination. Verification has no surrounding SQL transaction. `finishVerified` locks the coordinator, checks the exact operation/process/epoch/versions and frozen selections, then atomically registers/reuses artifacts, stores opaque IDs/evidence digests, appends a result-digest audit and releases ownership. A later conflict rolls back earlier inserts. The four supported kinds are leaf artifacts: retained raw files and reference CSV/vocabulary dependencies have exact descriptors in immutable evidence, without invented operational artifact IDs/edges. Real registered dependency edges remain for later kinds where native semantics require them.

V7's native uniqueness remains `(product,kind,contract,processor,native_format,native_identity)` with NULLS NOT DISTINCT. Batch native keys are canonical sourceId/batchId; reference keys are referenceId/revision. Fingerprint and file evidence are compared separately, never used to evade native identity conflicts. The existing binding/address key also protects location. Fresh import commands reverify before reuse; same-ID retries only return explicitly historical results (`revalidated:false`) or blocked ACTIVE ownership. `verified_at` is first qualification history. A rewritten/replaced file's observed stamps can cause conflict even if its content returns to the same bytes; the importer never repairs registry/file identity.

The supervisor uses a fixed argument list and cleared environment, bounded input/output/stderr, a per-call 1–120-second deadline and bounded direct-child/observed-descendant cleanup. Observed descendants or uncertain cleanup preserve blocked ownership. Known safe verifier failures finalize FAILED; SQL/ownership/unknown outcomes do not perform a compensating release. Lost committed responses are resolved from the exact request/operation. Explicit import recovery holds the same guard, checks observable old-process identity and actual operator-confirmed quiescence, performs the existing occupied-to-occupied fenced transfer and separately closes the recovery. No native qualification, mutation or processing occurs during recovery.

This implements a trusted local-operator boundary, not hostile-runtime/filesystem attestation, orphan-process proof or distributed fencing. Keeping exact evidence in the existing registry avoids a second authority; retaining native descriptors instead of phantom artifact edges preserves the accepted native dependency model. Limiting catalogs to sixteen selections and hashing a fixed small module set keeps resource/protocol bounds reviewable without a generic connector or task framework.

### B2b-1B-A local validation

Executed locally October 21, 2026 on the existing owner-prepared E5 branch, starting at `1a1b11e6faf592fbb2e68ed5b341dcf7b9ecdd6a` with clean working tree/index. Java 21.0.7, Maven 3.10.0, Python 3.12.15, DuckDB 1.4.5 and dedicated PostgreSQL 17.11 Docker test service were used. Native inputs were small synthetic fixtures only. No live feed, private retained dataset, Spark transformation, benchmark or AWS resource was used. Node 26.12.0 executed documentation tooling locally; the repository's Node 24 engine/hosted runner remains unobserved for this change.

| Exact command / local environment | Result |
| --- | --- |
| `JAVA_HOME=$(/usr/libexec/java_home -v 21) mvn -o -B -ntp -f services/content-api/pom.xml verify -Ppostgres` with private dedicated `DDIP_TEST_DB_URL/USER/PASSWORD` and absolute `DDIP_TEST_PYTHON` | Passed: **210 unit/component**, **144 real PostgreSQL** tests (28 B1, 36 B2a, 42 ownership, 38 import workflow), zero failures/errors/skips |
| `JAVA_HOME=$(/usr/libexec/java_home -v 21) mvn -o -B -ntp -f services/content-api/pom.xml verify -Pintegration -Danalytics.python="$PWD/outputs/silver-python312/.venv/bin/python"` | Passed: **20 real HTTP/Python** compatibility tests plus 210 repeated unit/component tests; no failures/errors/skips |
| `PYTHONPATH=processing:processing/tests outputs/silver-python312/.venv/bin/python -B -m unittest processing/tests/test_artifact_import_verifier.py processing/tests/test_reliability_format_inputs.py processing/tests/test_publisher_reference.py processing/tests/test_reliability_inputs.py processing/tests/test_reliability_format_verification.py` | Passed: **112** tests: 24 new import qualification, 10 v2 format inputs, 46 publisher reference, 17 v1/service input, 15 v2 completed verifier; no failures/errors/skips |
| `mvn -o -B -ntp -f services/content-api/pom.xml verify -Ppostgres -Dit.test=ImportWorkflowIT#registeredIdentitySurvivesFreshConnectionAndOptionalActualRestart -Dddip.test.restart-signal="$PWD/outputs/e5-b2b1ba-restart.signal"` with the same Java/test environment | Passed: **1 controlled actual PostgreSQL restart** probe; the normal suite repeats this test using fresh connections only |
| `npm run check`; `git diff --check`; byte-for-byte migration comparison with HEAD | Passed: Markdown/local links (44 files), tracked/untracked hygiene and whitespace; **V1–V7 unchanged**; index remains clean |

The dedicated restart probe writes READY only after real native verification and registration commit. A separate local test orchestrator then ran `docker restart ddip-b1-postgres-test`, waited for `pg_isready`, and created the signal's `.done` companion. The test asserted an increased `pg_postmaster_start_time()` and identical artifact IDs, evidence digests, verified timestamps and terminal operation result after reconnecting. Reproduction requires an exclusively controlled **test** service; never restart an operational database as part of this probe. Only the random test database was dropped afterward. The existing dedicated service was returned to its original stopped state; its volume was retained. Repeated suite executions are not additional distinct tests.

The 38 workflow cases use actual PostgreSQL and actual authoritative native readers for acceptance. Coverage includes four-kind atomic import, new-command requalification/reuse, historical lookup without a current-readability claim, changed valid completion bytes/native evidence conflict, incompatible location rollback, result/audit rollback with no trusted partial artifact, simultaneous whole-command imports and concurrent conditional finalization, stale ownership and cross-operation evidence refusal, missing bindings and symlink escape, real connection refusal before reservation and after verification, committed result with injected lost acknowledgment, abrupt JVM death before/after qualification and explicit guarded recovery, malformed/oversized/identity/schema/exit-disagreeing verifier responses, safe failure versus uncertain cleanup and both Spring-context/packaged-jar non-web commands. Actual commit acknowledgment failure is injected only **after** PostgreSQL commits; exact lookup proves committed artifact/result/audit persistence. Connection-outage cases temporarily disable new connections only to their own disposable database, restoring access in `finally`.

Process-boundary unit tests launch disposable JVM children to verify output/stderr budgets, timeout, startup failure, pre-existing/during-child interruption, descendant uncertainty and cleanup. Injected verifier failures exercise transactional service decisions without pretending to be native acceptance. Existing 42 ownership cases independently prove writer/recovery admission exclusion in both directions, competing process guards, no idle interval during fenced recovery, immutable state/evidence, stale tokens, abandoned-owner blocking, V6 upgrade/checksums, native scope uniqueness and lost reservation/handoff responses. Native before/after byte/mtime comparisons cover all four artifact kinds. No processing run/attempt was created by imports, the worker stayed idle and no import HTTP route was added. The existing unit suite still proves database-free analytics startup; existing B2a API/revision/idempotency regressions passed.

Initial sandbox-only Maven compilation could not use its existing cache lock; authorized local execution passed. No dependency installation occurred inside import. Hosted CI has **not** been observed after publication; the existing pinned Python/PostgreSQL CI profile now discovers the new tests through its expanded Maven include and Python discovery, without new services/dependencies. Linux/other-host process and filesystem behavior, hard power-loss durability, hostile administration, comprehensive backup/restore and proof of unobservable orphan quiescence remain unverified.

**Implemented:** this Spark-free checkpoint and its bounded operator boundary. **Tested:** the commands/cases above, separately from historical B2b-1A results. **Planned subject to owner review:** B2b-1B-B must add full read-only Silver qualification (accepted/rejected Parquet schema/provenance/counts, retained Bronze evidence, stable inventories, an explicitly E5-only inventory identity and bounded Spark qualification supervision), reusing the same ownership and atomic registry finalization. Its fixed descriptor/protocol extension requires separate authorization; current catalogs reject Silver. **Deferred:** B2b-2 opaque-ID run submission/fresh dependency verification, worker execution, derived output registration, complete E5 and other excluded functionality. No further implementation was started.

### B2b-1B-A changed files

- [docs/architecture/durable-execution.md](../../docs/architecture/durable-execution.md)
- [processing/artifact_import_verifier.py](../../processing/artifact_import_verifier.py)
- [processing/tests/artifact_import_fixtures.py](../../processing/tests/artifact_import_fixtures.py)
- [processing/tests/test_artifact_import_verifier.py](../../processing/tests/test_artifact_import_verifier.py)
- [services/content-api/README.md](../../services/content-api/README.md)
- [services/content-api/pom.xml](../../services/content-api/pom.xml)
- [services/content-api/src/main/java/platform/contentapi/ContentApiApplication.java](../../services/content-api/src/main/java/platform/contentapi/ContentApiApplication.java)
- [services/content-api/src/main/java/platform/contentapi/execution/ImportOwnershipRepository.java](../../services/content-api/src/main/java/platform/contentapi/execution/ImportOwnershipRepository.java)
- [services/content-api/src/main/java/platform/contentapi/execution/RuntimeEnvironment.java](../../services/content-api/src/main/java/platform/contentapi/execution/RuntimeEnvironment.java)
- [services/content-api/src/main/java/platform/contentapi/execution/RuntimeRole.java](../../services/content-api/src/main/java/platform/contentapi/execution/RuntimeRole.java)
- [services/content-api/src/main/java/platform/contentapi/importing/ImportBindings.java](../../services/content-api/src/main/java/platform/contentapi/importing/ImportBindings.java)
- [services/content-api/src/main/java/platform/contentapi/importing/ImportCatalog.java](../../services/content-api/src/main/java/platform/contentapi/importing/ImportCatalog.java)
- [services/content-api/src/main/java/platform/contentapi/importing/ImportEvidence.java](../../services/content-api/src/main/java/platform/contentapi/importing/ImportEvidence.java)
- [services/content-api/src/main/java/platform/contentapi/importing/ImportFailure.java](../../services/content-api/src/main/java/platform/contentapi/importing/ImportFailure.java)
- [services/content-api/src/main/java/platform/contentapi/importing/ImportJson.java](../../services/content-api/src/main/java/platform/contentapi/importing/ImportJson.java)
- [services/content-api/src/main/java/platform/contentapi/importing/NativeImportProcess.java](../../services/content-api/src/main/java/platform/contentapi/importing/NativeImportProcess.java)
- [services/content-api/src/main/java/platform/contentapi/importing/NativeImportVerifier.java](../../services/content-api/src/main/java/platform/contentapi/importing/NativeImportVerifier.java)
- [services/content-api/src/main/java/platform/contentapi/importing/OperatorImportConfiguration.java](../../services/content-api/src/main/java/platform/contentapi/importing/OperatorImportConfiguration.java)
- [services/content-api/src/main/java/platform/contentapi/importing/OperatorImportService.java](../../services/content-api/src/main/java/platform/contentapi/importing/OperatorImportService.java)
- [services/content-api/src/main/java/platform/contentapi/importing/QualifiedArtifact.java](../../services/content-api/src/main/java/platform/contentapi/importing/QualifiedArtifact.java)
- [services/content-api/src/main/resources/application.properties](../../services/content-api/src/main/resources/application.properties)
- [services/content-api/src/test/java/platform/contentapi/execution/RuntimeRoleTest.java](../../services/content-api/src/test/java/platform/contentapi/execution/RuntimeRoleTest.java)
- [services/content-api/src/test/java/platform/contentapi/importing/ImportBoundaryTest.java](../../services/content-api/src/test/java/platform/contentapi/importing/ImportBoundaryTest.java)
- [services/content-api/src/test/java/platform/contentapi/importing/ImportProcessChild.java](../../services/content-api/src/test/java/platform/contentapi/importing/ImportProcessChild.java)
- [services/content-api/src/test/java/platform/contentapi/importing/ImportWorkflowChild.java](../../services/content-api/src/test/java/platform/contentapi/importing/ImportWorkflowChild.java)
- [services/content-api/src/test/java/platform/contentapi/importing/ImportWorkflowIT.java](../../services/content-api/src/test/java/platform/contentapi/importing/ImportWorkflowIT.java)

## B2b-1B-B acceptance and inspection

Authorized scope is explicit completed Silver import only, reusing V7 ownership and the existing operator role. Gate A must pass actual read-only Spark qualification with synthetic Bronze/Silver before any Silver registry integration is added. Gate B then requires actual PostgreSQL atomicity, conflicts, interruption/recovery and durability checks. Existing native publishers, schemas, marker bytes and V1–V7 remain unchanged.

Inspection at `7fe6ae08d9b2494f319fedcbb18f0c392bd29f36` on the owner-prepared E5 branch found a clean tree/index. Native `silver_inputs.load_inputs` already checks exact per-file logical types, required values, accepted/rejected quality/replay conflicts, counts and marker provenance; it does not reopen Bronze or inventory hidden Spark bookkeeping. `bronze_to_silver.metadata`, `read_object` and `local_client` provide the accepted bounded metadata/key/payload and loopback-only S3 policies. Additional qualification must capture bookkeeping and filesystem guards, independently reopen original Bronze and compare its provenance across every row. These checks do not reconstruct article semantics from the original XML; that limitation must remain explicit. Spark infers Parquet fields nullable even when producers write required literals; the native schemas declare nullable fields and enforce requiredness through row rules. Qualification must preserve that compatibility rather than invent a universal physical OPTIONAL/REQUIRED footer policy.

Gate A passed locally before Silver database integration began: 30 new Python Silver qualification/vector tests plus 24 existing import-verifier tests, and 25 Java process-boundary tests. Actual native M1 fixtures cover RSS, Atom, accepted/rejected/empty datasets, exact Bronze GET verification through the existing boto3 client against a read-only loopback S3 fixture, corruption/provenance/count/schema/path failures, file/marker mutation, literal inventory vectors and unchanged bytes/mtimes. A real protocol subprocess stopped Spark, closed the gateway stdin and waited for its JVM to exit before success. Process tests cover bounded output, timeout, interruption and descendant uncertainty. This establishes current qualified evidence, not the original pre-import Parquet membership or semantic equivalence to normalized XML. Early failures exposed the PySpark gateway stdin lifetime, removal of an empty native partition (which original counts cannot detect), and native conflict classifications; fixes preserve native behavior. Database registration for Silver was still absent at this gate.

### B2b-1B-B local validation

B2b-1B-B is implemented and locally tested on the existing owner branch. [The Silver operator contract](../../services/content-api/README.md#verified-silver-imports-b2b-1b-b) defines catalog/runtime/Bronze configuration, exact verification extent, E5-only digest formula and two literal vectors. V7 already supports atomic registration; no migration or native publication contract change was required. V1–V7, `silver.py`, `silver_inputs.py` and `bronze_to_silver.py` match the entry commit byte for byte. Accepted ADRs 0002–0004/0010/0011 are unchanged.

**Implemented:** one fixed SILVER kind and protocol-2 adapter, approved SILVER/BRONZE/TEMP/runtime bindings, independent retained Bronze verification, native read-only Spark qualification, complete managed capture/rechecks, explicit E5 inventory identity and the existing fenced atomic artifact/result/audit workflow. Native Bronze dependencies are exact descriptors, not fake registry artifacts. Native namespace uniqueness uses the exact bucket/key hash encoding documented in the guide, independently of the inventory digest. Fresh reimports requalify; changed native/location/evidence conflicts. Same-operation lookup is historical only. Uncertain Spark/process/protocol/DB ownership remains blocked; explicit fenced recovery is unchanged.

**Gate A:** passed before any Silver registry integration: 54 Python cases (30 new qualification/vector plus 24 existing adapter) and 25 focused Java boundary cases at that point. Final read-only refinements passed **34 Silver Python cases**, adding marker-only forgery, mutation between capture and marker parse, native hidden-bookkeeping subtrees and rejection of glob metacharacters in analytical filenames. Native schemas declare nullable fields; Spark required literals/nullable footer inference remain compatible, while native required scalar/array values are enforced. Original completion lacks a file inventory: a pre-import missing empty partition cannot be inferred from counts; observed disappearance/change against the E5 baseline fails. No XML-to-row semantic reconstruction or original authorship claim is made.

**Gate B:** **23 real PostgreSQL Silver integration cases passed**, using the actual fixed Python/Spark reader and original Bronze GET validation against a read-only loopback S3 HTTP fixture with the actual boto3 client. Acceptance is not mocked. Tests prove successful import, fresh requalification/ID reuse, valid changed completion evidence conflict, later corruption and Bronze changes, atomic artifact/result/audit rollback, simultaneous import admission, stale fencing, pre-Spark unavailable dependencies/bindings, credential symlink refusal, post-Spark dependency mutation, uncertain timeout/malformed results, actual DB connection refusal after Spark, committed write with injected lost acknowledgment, abrupt JVM-owner death before/after qualification, explicit/repeated recovery and packaged non-web invocation. Acknowledgment injection throws only after real PostgreSQL commits. SQL/ownership uncertainty never triggers a compensating release. The existing 144 PostgreSQL cases additionally cover execution/recovery exclusion, occupied-to-occupied handoff, immutable state/constraints, identity/location scopes, source APIs and idempotency.

Actual local commands and outcomes (October 21, 2026; Java 21.0.7/Maven 3.10.0, Python 3.12.15, PySpark 4.0.2, DuckDB 1.4.5, PostgreSQL 17.11 on macOS ARM64):

| Command | Outcome |
| --- | --- |
| `JAVA_HOME=$(/usr/libexec/java_home -v 21) mvn -o -B -ntp -f services/content-api/pom.xml verify -Ppostgres` with dedicated private `DDIP_TEST_DB_URL/USER/PASSWORD` and absolute `DDIP_TEST_PYTHON` | **224 Java unit/component + 167 PostgreSQL** (144 existing + 23 Silver), no failures/errors/skips |
| Same environment: `mvn -o -B -ntp -f services/content-api/pom.xml verify -Ppostgres -Dit.test=SilverImportIT -Dddip.test.silver-restart-signal="$PWD/outputs/e5-b2b1bb-restart.signal"` | Final helper refinements: **23 repeated Silver PostgreSQL + 224 repeated unit/component**, all passed, including an **actual PostgreSQL container restart** |
| `JAVA_HOME=$(/usr/libexec/java_home -v 21) mvn -o -B -ntp -f services/content-api/pom.xml verify -Pintegration -Danalytics.python="$PWD/outputs/silver-python312/.venv/bin/python"` | **20 HTTP/Python** analytical compatibility + 224 repeated unit/component, all passed |
| `PYTHONPATH=processing:processing/tests outputs/silver-python312/.venv/bin/python -B -m unittest processing/tests/test_silver_import_qualification.py processing/tests/test_artifact_import_verifier.py processing/tests/test_silver.py processing/tests/test_bronze.py processing/tests/test_silver_inputs.py processing/tests/test_reliability_format_inputs.py processing/tests/test_publisher_reference.py processing/tests/test_reliability_inputs.py processing/tests/test_reliability_format_verification.py processing/tests/test_gold.py processing/tests/test_gold_publication.py processing/tests/test_gold_analytics.py processing/tests/test_content_dependencies.py processing/tests/test_content_publication.py` | **262 passed** (32 Silver checks present at this broad run + 230 unchanged regression cases), no failures/errors/skips |
| Same native environment: `outputs/silver-python312/.venv/bin/python -B -m unittest processing/tests/test_silver_import_qualification.py` | Final **34 qualification/vector cases passed**; overlaps the prior 32, so **264 distinct Python cases** are covered across these executions, not 296 |
| `npm run check`; `git diff --check`; byte comparison of migrations/native modules with entry commit | Passed: 44 Markdown files/local links, tracked/untracked hygiene and whitespace; no migration/native module changes |

Native commands also set `JAVA_HOME`/PATH for Java 21, `PYSPARK_PYTHON` to the pinned absolute Python, `SPARK_LOCAL_IP=127.0.0.1`, `TZ=UTC`; the operator itself uses its reviewed controlled runtime profile. No retained personal dataset, live publisher, AWS resource or benchmark was accessed. Gold/Content publication appears only inside existing small synthetic regression fixture preparation, never the import operation.

The controlled restart signal is written only after real Silver qualification/registration commits. An external local test orchestrator then ran `docker restart ddip-b1-postgres-test`, waited for `pg_isready` and wrote the `.done` companion. The test observed a later `pg_postmaster_start_time()`, identical artifact ID/native identity/evidence digest/verified timestamp, and the same committed operation state after reconnecting. The probe requires exclusive control of a dedicated test service, never an operational database. The dedicated container was restored to its initial stopped state with its volume retained. Repeated tests are not additional unique tests.

Readonly checks compare exact bytes/mtimes across all native Silver files, including bookkeeping; the final integration case also preserves the backing synthetic Bronze object files. The S3 fixture asserts signed GET-only access; no Bronze PUT/DELETE or source fetch occurs. Spark was stopped/gateway reaped before normal result success. Temporary Spark runtime directories are confined to the approved private scratch root and retained for manual inspection; native artifact roots are never cleanup targets. No processing run/attempt was created by imports. Existing analytics-only startup, B2a APIs, idle worker and non-web import behavior passed regressions.

Initial Gate B had one test-setup error: it attempted to delete an immutable profile/storage association. The replacement provisions a new profile referencing a missing scratch binding; no SQL guard was weakened. Gate A refinements preserve the native hidden-directory exclusion policy while capturing its contents as bookkeeping, distinguish observed changes around parsing, and keep Python/JVM temp locations private. The old Spark-free test's global `sys.modules` assertion was moved into a fresh isolated process so discovery of unrelated Spark tests cannot invalidate it. Expected corrupt-input Spark diagnostics, native-Hadoop fallback and existing socket ResourceWarnings appeared; final assertions passed.

**Known limits / unverified:** checksum and cooperative-guard consistency is not hostile filesystem/runtime authenticity or an atomic S3 snapshot; mutation reverted between checks may be unobserved. Physical footer requiredness is not a new Silver contract; requiredness follows native logical schema/row rules. Empty datasets have no row-level provenance evidence, and full XML semantic equivalence is not reconstructed. Tests do not newly certify SeaweedFS/AWS behavior: retained-object integration uses the actual client against a synthetic loopback S3 protocol fixture. Hard power-loss, comprehensive restore, other-host/Linux process behavior and complete orphan-process quiescence remain unverified. Explicit recovery confirmation still requires actual operator inspection beyond parent PID/lock/connection liveness. Hosted CI has not been observed; existing pinned Python/PostgreSQL CI discovery now includes the new tests through the Maven profile, with no new services/dependencies. Documentation tooling ran on local Node 26.12.0; the repository's Node 24 engine remains a hosted verification item.

**Planned, requiring owner authorization:** B2b-2 must expose/select opaque registry IDs, freeze exact selections, enforce aggregate admission and validate registered Silver/Bronze metadata at submission and independently revalidate native dependencies at worker consumption (including post-action rechecks). Existing `verified_at` or historical import result is insufficient. **Deferred:** run submission, queue claiming/worker execution, processing controls, derived publications, React Operations, authorization/exports, activation, automatic cleanup/backup, distributed workers, AWS and E4a. No B2b-2/worker implementation has begun; E5 remains incomplete.

### B2b-1B-B changed files

- [docs/architecture/durable-execution.md](../../docs/architecture/durable-execution.md)
- [processing/artifact_import_verifier.py](../../processing/artifact_import_verifier.py)
- [processing/silver_import_qualification.py](../../processing/silver_import_qualification.py)
- [processing/tests/silver_import_fixtures.py](../../processing/tests/silver_import_fixtures.py)
- [processing/tests/test_artifact_import_verifier.py](../../processing/tests/test_artifact_import_verifier.py)
- [processing/tests/test_silver_import_qualification.py](../../processing/tests/test_silver_import_qualification.py)
- [services/content-api/README.md](../../services/content-api/README.md)
- [services/content-api/pom.xml](../../services/content-api/pom.xml)
- [services/content-api/src/main/java/platform/contentapi/execution/ImportOwnershipRepository.java](../../services/content-api/src/main/java/platform/contentapi/execution/ImportOwnershipRepository.java)
- [services/content-api/src/main/java/platform/contentapi/importing/ImportBindings.java](../../services/content-api/src/main/java/platform/contentapi/importing/ImportBindings.java)
- [services/content-api/src/main/java/platform/contentapi/importing/ImportCatalog.java](../../services/content-api/src/main/java/platform/contentapi/importing/ImportCatalog.java)
- [services/content-api/src/main/java/platform/contentapi/importing/ImportEvidence.java](../../services/content-api/src/main/java/platform/contentapi/importing/ImportEvidence.java)
- [services/content-api/src/main/java/platform/contentapi/importing/ImportJson.java](../../services/content-api/src/main/java/platform/contentapi/importing/ImportJson.java)
- [services/content-api/src/main/java/platform/contentapi/importing/NativeImportProcess.java](../../services/content-api/src/main/java/platform/contentapi/importing/NativeImportProcess.java)
- [services/content-api/src/main/java/platform/contentapi/importing/NativeImportVerifier.java](../../services/content-api/src/main/java/platform/contentapi/importing/NativeImportVerifier.java)
- [services/content-api/src/main/java/platform/contentapi/importing/OperatorImportService.java](../../services/content-api/src/main/java/platform/contentapi/importing/OperatorImportService.java)
- [services/content-api/src/main/java/platform/contentapi/importing/SilverImportEvidence.java](../../services/content-api/src/main/java/platform/contentapi/importing/SilverImportEvidence.java)
- [services/content-api/src/test/java/platform/contentapi/importing/ImportBoundaryTest.java](../../services/content-api/src/test/java/platform/contentapi/importing/ImportBoundaryTest.java)
- [services/content-api/src/test/java/platform/contentapi/importing/ImportProcessChild.java](../../services/content-api/src/test/java/platform/contentapi/importing/ImportProcessChild.java)
- [services/content-api/src/test/java/platform/contentapi/importing/SilverCatalogTest.java](../../services/content-api/src/test/java/platform/contentapi/importing/SilverCatalogTest.java)
- [services/content-api/src/test/java/platform/contentapi/importing/SilverImportIT.java](../../services/content-api/src/test/java/platform/contentapi/importing/SilverImportIT.java)

No Git/GitHub write commands were run. The entry index was clean; six new files were later observed staged during the session. Those index entries were left untouched.

## Stage B2b-2A durable run submission

Authorized on October 21, 2026: queued-only CONTENT_RSS_V1/RELIABILITY_V2 submission and minimal persisted run inspection. Acceptance requires strict discriminated requests/canonical vectors, immutable source/profile/input capture, real PostgreSQL atomicity/concurrency/idempotency/capacity/restart evidence, local HTTP protection and no processing launch. B2b-2B, worker execution and full E5 remain unauthorized. Accepted ADRs 0010/0011, native contracts and migrations V1–V7 are unchanged.

### Implemented boundary

POST `/api/management/v1/runs` and GET `/api/management/v1/runs/{id}` use the existing API role, boundary filter, bounded canonical JSON, envelope and fixed safe advice. Exact bodies remain those in the normative REST section. [The operator guide](../../services/content-api/README.md#durable-queued-run-submission-b2b-2a) defines actual profile setup, commands, DTO fields, error mappings and synthetic reproduction. Minimal GET intentionally omits the unimplemented history/attempt/control/result expansions. Existing B2a source capabilities remain scoped to that catalog.

The existing runs row is the scoped durable idempotency binding. Coordinator-first lookup returns the original run before current revision, enablement, catalog, readiness or capacity checks. New work validates a fixed versioned admission profile, locks sources in ASCII order, requires current enabled revisions and approved endpoint/profile associations, and validates registered artifacts with the existing import protocol's pure shape/consistency checks. This helper never opens roots, native data, S3 or a child process. Full native freshness checks remain worker-side, consistent with the normative freeze points above. No native publication fingerprint is computed.

New version-1 admission profiles pin pipeline/version, sourceProfile and exact JDK/Python/PySpark/deployment revision metadata in immutable execution_profiles. IDs over this HTTP boundary are UUIDs; import-only profiles retain their existing token IDs and schemas. Storage associations must authorize the selected native inputs and each fixed pipeline role. A profile revision is its immutable ID/configuration digest. This metadata descriptor deliberately does not invent a runnable worker adapter; C/D must match the reviewed deployment and implement fixed runtime/command readiness before execution is possible.

Canonical captured_selection stores exact source config/digest and original reviewed endpoint descriptor, gate observation, execution/storage profile configurations/digests and registered artifact identities/qualification evidence/time. Exact copies are linked through immutable run_sources/run_inputs, within the 16 MiB aggregate cap. Existing imported kinds are native leaves; retained raw/vocabulary/Bronze evidence stays in exact native descriptors, without fabricated artifact edges. Changes to current source/catalog configuration cannot rewrite the snapshot. Operational request/selection hashes remain distinct from all native fingerprints and Silver's E5 inventory identity.

One short transaction inserts QUEUED run, sources, inputs and RUN_SUBMITTED audit. SQL uniqueness/FKs/digest/immutability constraints are reused; no new table or migration is required. Every conditional failure rolls back accepted work. Commit uncertainty maps to 503 and is resolved by exact scoped key/request replay. Request sets are normalized with B2a's UTF-8 encoder; native serialization order is untouched. Independent literal request SHA-256 vectors are `ccb5b21efeca0723add5093f11a1d88bfa7a7eb412cda9d03d4f87326b294de8` (Content) and `d7bcaa53a98d8a24872119379c64b60417982309e3fca8977207480b03e0a1b2` (Reliability), with full inputs in the guide/tests.

Admission limits remain 20 unfinished/10,000 retained runs. New work reserves 4,096 audit events per unfinished run and three per active import against the 1M bound; source commands and import/recovery reservations respect that reserve. Already admitted import finalization is not denied by a new admission check. These are conservative logical reservations, not a new ledger/table or disk quota. Future lifecycle writers must honor their per-run budget. All counts/admissions serialize on the existing coordinator. Healthy execution can retain its slot while more work queues; stale/unresolved/recovery/import ownership blocks new admission without alteration. No source operation or submission releases either owner. There is no queue draining, attempt creation, worker claim or HTTP control/import endpoint.

### Current local validation

All following results were executed locally on October 21, 2026 at this checkpoint; historical results above are not counted again. The full commands used the host JDK 21, dedicated PostgreSQL 17.11 test service and existing pinned Python 3.12.15/PySpark 4.0.2 fixture environment. DB credentials were supplied privately through DDIP_TEST_DB_URL/USER/PASSWORD; DDIP_TEST_PYTHON named the existing ignored virtual environment. No live publishers, retained personal datasets or cloud services were used.

| Exact Maven/check command (repository root) | Current result |
| --- | --- |
| `mvn -q -f services/content-api/pom.xml -Dtest=RunCommandsTest test` | Initial focused gate: 26 cases passed; two additional selection-bound cases subsequently passed in the complete commands below |
| `mvn -q -f services/content-api/pom.xml -Ppostgres -Dtest=RunCommandsTest,AuditCapacityTest -Dit.test=RunSubmissionIT verify` | Focused validation/submission iterations passed before the final history-cap and selection-bound additions; final counts come from the complete run below |
| `mvn -q -f services/content-api/pom.xml -Ppostgres -Dddip.test.submission-restart-signal=/private/tmp/ddip-e5-submission-restart verify` | **254 Java unit/component + 211 real PostgreSQL cases passed**, zero failures/errors/skips; external harness restarted only the dedicated test container at the READY barrier |
| `mvn -q -f services/content-api/pom.xml verify -Pintegration -Danalytics.python="$PWD/outputs/silver-python312/.venv/bin/python"` | **20 HTTP/Python analytics compatibility cases passed**, with the same 254 unit/component cases also passing; repeats are not additional distinct tests |
| `npm run check` | 44 Markdown files/local links and repository hygiene passed |
| `git diff --check` and `git diff --cached --check` | Passed; no index writes |

Gate A comprises **30 new pure cases**: 28 RunCommandsTest and two AuditCapacityTest. They cover strict DTO/type/duplicate/unknown-field rules, both pipeline vectors, reordered sets, changed selections, revision/source/artifact bounds, profile compatibility and audit reservation boundaries. Literal digests were independently calculated with Python stdlib sorted compact UTF-8 JSON plus hashlib, rather than PostgreSQL JSONB or the Java encoder. The extra 224 unit/component cases preserve existing runtime/management/import/query behavior.

Gate B comprises **44 new real PostgreSQL RunSubmissionIT cases**. They cover committed QUEUED HTTP acceptance and GET, both Reliability formats, Content empty/retained history, frozen endpoint/config/input evidence, exact relational/audit atomicity, concurrent same/different-key submissions, replay after revision/disablement/terminal state/catalog removal, full 20-run queue and populated 10,000-run retained history, disabled/stale source rejection, wrong profiles/formats/membership and malformed registered metadata. Trigger-injected input/audit failures roll back the entire run. A JDBC wrapper throws SQLSTATE 08006 only after a real write commit; exact-key replay observes that one committed run. Real connection refusal is tested at repository and HTTP boundaries with fixed safe diagnostics.

Source mutation races use explicit latches; the capture-first case additionally observes PostgreSQL's actual lock wait before releasing the admission transaction. Update-first rejects the obsolete revision; admission-first keeps the original endpoint/configuration after the update commits. Tests prove that healthy execution allows queueing while its pointer/epoch/version remain identical, recovery/stale supervision blocks admission, and active/abandoned import ownership is never changed even when the host guard becomes free. FKs and V5 guards reject evidence/link mutation. Source and artifact records/qualification bytes remain unchanged after submission; production submission has no filesystem/network/native-reader or process supervisor dependency.

The actual database restart test asserts a later pg_postmaster_start_time and exact surviving run IDs, source revisions in canonical selection bytes, input evidence bytes/artifact IDs, hashes and submission sequence through new connections. Application-context restart and minimal GET are separately tested. No processing attempts, stages, receipts or worker instances are created by ordinary submission; tests that simulate an active slot create it explicitly as database setup, never via dispatch.

All **167 pre-existing PostgreSQL cases** also passed: 28 PersistenceIT, 36 SourceManagementIT, 42 ImportOwnershipIT, 38 ImportWorkflowIT and 23 SilverImportIT. Because shared audit admission changed, the existing import suites ran their small actual native-reader/DuckDB/Spark/Bronze-loopback regression fixtures, separately from metadata-only run acceptance. Their read-only evidence, lost-acknowledgment and ownership/process recovery checks remain valid. No new Spark workload, native publisher workload, benchmark or native verification inside submission was introduced.

Ignored local logs: outputs/e5-b2b2a-gate-a.log, e5-b2b2a-gate-b.log, e5-b2b2a-all-postgres.log, e5-b2b2a-http.log and e5-b2b2a-hygiene.log. The dedicated test PostgreSQL service was initially stopped and was returned to stopped with its volume retained. No Git/GitHub writes occurred. Hosted CI is **unobserved/pending owner publication**; the existing postgres Maven profile now includes RunSubmissionIT, without new services, dependencies or CI jobs.

Limits of evidence: synthetic registered metadata is not fresh native qualification and does not prove current artifact readability. The 1M audit bound/reservation formula is tested at literal boundaries without generating a million audit rows. Actual abrupt API-process death during a commit, host power loss, disk restore certification and future worker lifecycle-budget enforcement are not independently certified by this checkpoint. DB rollback/lost-ack/restart evidence is not an exactly-once execution guarantee.

### Planned and deferred

Historical B2b-2A closeout (superseded for history by the B2b-2B section below): B2b-2B requires bounded history/attempt/event reads and richer public inspection, with the same local trust boundary and frozen evidence. It has not begun. C/D must independently revalidate native files/dependencies, resolve exact approved adapters/runtime/storage readiness, enforce reserved history budgets and live source gates, and implement fenced attempts/stage freeze points. Accepted QUEUED work has no native write authority. Cancellation/retry/reconciliation endpoints, worker dispatch, publication/activation, UI, authorization/exports, schedulers and AWS remain deferred; no E5 completion claim is made.

### B2b-2A files changed

- [docs/architecture/durable-execution.md](../../docs/architecture/durable-execution.md)
- [services/content-api/README.md](../../services/content-api/README.md)
- [services/content-api/pom.xml](../../services/content-api/pom.xml)
- [services/content-api/src/main/java/platform/contentapi/execution/ImportOwnershipFailure.java](../../services/content-api/src/main/java/platform/contentapi/execution/ImportOwnershipFailure.java)
- [services/content-api/src/main/java/platform/contentapi/execution/ImportOwnershipRepository.java](../../services/content-api/src/main/java/platform/contentapi/execution/ImportOwnershipRepository.java)
- [services/content-api/src/main/java/platform/contentapi/importing/RegisteredImportEvidence.java](../../services/content-api/src/main/java/platform/contentapi/importing/RegisteredImportEvidence.java)
- [services/content-api/src/main/java/platform/contentapi/management/AuditCapacity.java](../../services/content-api/src/main/java/platform/contentapi/management/AuditCapacity.java)
- [services/content-api/src/main/java/platform/contentapi/management/ManagementConfiguration.java](../../services/content-api/src/main/java/platform/contentapi/management/ManagementConfiguration.java)
- [services/content-api/src/main/java/platform/contentapi/management/ReviewedSourceCatalog.java](../../services/content-api/src/main/java/platform/contentapi/management/ReviewedSourceCatalog.java)
- [services/content-api/src/main/java/platform/contentapi/management/RunCommands.java](../../services/content-api/src/main/java/platform/contentapi/management/RunCommands.java)
- [services/content-api/src/main/java/platform/contentapi/management/RunController.java](../../services/content-api/src/main/java/platform/contentapi/management/RunController.java)
- [services/content-api/src/main/java/platform/contentapi/management/RunProfile.java](../../services/content-api/src/main/java/platform/contentapi/management/RunProfile.java)
- [services/content-api/src/main/java/platform/contentapi/management/RunRepository.java](../../services/content-api/src/main/java/platform/contentapi/management/RunRepository.java)
- [services/content-api/src/main/java/platform/contentapi/management/SourceAdvice.java](../../services/content-api/src/main/java/platform/contentapi/management/SourceAdvice.java)
- [services/content-api/src/main/java/platform/contentapi/management/SourceFailure.java](../../services/content-api/src/main/java/platform/contentapi/management/SourceFailure.java)
- [services/content-api/src/main/java/platform/contentapi/management/SourceRepository.java](../../services/content-api/src/main/java/platform/contentapi/management/SourceRepository.java)
- [services/content-api/src/test/java/platform/contentapi/management/AuditCapacityTest.java](../../services/content-api/src/test/java/platform/contentapi/management/AuditCapacityTest.java)
- [services/content-api/src/test/java/platform/contentapi/management/RunCommandsTest.java](../../services/content-api/src/test/java/platform/contentapi/management/RunCommandsTest.java)
- [services/content-api/src/test/java/platform/contentapi/management/RunFixtures.java](../../services/content-api/src/test/java/platform/contentapi/management/RunFixtures.java)
- [services/content-api/src/test/java/platform/contentapi/management/RunSubmissionIT.java](../../services/content-api/src/test/java/platform/contentapi/management/RunSubmissionIT.java)

No migrations or accepted ADRs were modified. The entry working tree/index was clean; the owner retains all Git/GitHub operations.

## Stage B2b-2B bounded run history and inspection

Authorized October 21, 2026, starting from clean owner-committed E5 branch `b36d2ae111139d34e4f2295e277b3ca6cc6c25a1`. Acceptance criteria are bounded deterministic read-only run/attempt/audit inspection, source/product/state filtering, reuse of signed cursors, safe exact DTOs, real PostgreSQL and HTTP gates, restart continuity and unchanged submission/ownership/native contracts. Later lifecycle states are SQL test fixtures only. No worker, dispatch, control or native execution is authorized here.

### Implemented contract and queries

Implemented GET `/api/management/v1/runs`, expanded GET `/runs/{id}`, GET `/runs/{id}/attempts` and GET `/runs/{id}/events`. Exact filters, field sets, examples, safe errors, counter labels, publication qualification projection and commands are in the [operator guide](../../services/content-api/README.md#bounded-run-history-and-inspection-b2b-2b). Existing POST submission receives the same additive persisted detail DTO, preserving B2b-2A's committed response/Location and replay behavior. No mutation/control/import endpoint was added.

RunHistoryRepository reads the original runs, run_sources, run_inputs, artifacts, attempts, stage_invocations/stage_artifacts, controls and audit_events, without another history model/table or denormalization. Run pages sort unique submission_seq descending; source membership uses indexed EXISTS, not a fanout join or current-source configuration. Attempts sort attempt_no ascending with persisted stage counts. Events sort event_seq ascending and require explicit run_id linkage; general source/import audits and heartbeat updates are excluded. Context attempt/control FKs are checked against the same run. Event private details are not loaded or synthesized.

All SQL uses explicit columns, bound values and fixed predicates/orderings. Run pages use two statements; per-run pages and detail use three. Control/result/attempt/stage summaries use SQL subqueries/joins without per-row JDBC calls. Selection/configuration/evidence blobs are absent from history projections. Detail's source/input lists have bounded lookahead and reject oversized metadata. Counts are an allowlisted nullable decimal-string map, not invented zeros. No query locks coordinator or obtains host ownership. Existing sequence, state/sequence, source, attempt/stage, control and audit indexes suffice; **no migration or index change** was needed. V1–V7, maintenance ownership and accepted ADRs/data contracts remain unchanged.

A successful public result requires the proper final native product kind/version, original native 64-hex publication identity and VERIFIED VERIFY_RESULT linkage for that run. Its verificationStatus=VERIFIED_AT_EXECUTION is historical evidence, not live native verification, authenticity or activation. No publication fingerprint is derived from E5 metadata. Checkpoint C must write correct native identities, verification links and approved public classifications; a registry/flag alone cannot create a verified DTO. Unknown/private stored classifiers or malformed counters fail with fixed INTERNAL_FAILURE before any partial response.

RunPageCursor is a scoped payload validator over the unchanged B2a SourceCursor HMAC signer/configuration. Version-1 tokens bind endpoint/filter/order/run scope, high-water, last position and a fixed 24-hour expiry retained through traversal. Tokens remain <=512 encoded bytes, with one-row lookahead and no OFFSET. A configured unchanged key supports restart continuity; wrong key, expiry, incompatible payload/scope or tampering fails 400. Source cursor semantics are unchanged. Page size may change. Static traversal has no duplicates/skips; mutable state-filter membership is not snapshot-isolated across requests. No persistent cursor sessions or long SQL snapshots are used.

The existing trusted-loopback Host/Origin/CORS boundary and response envelope/no-store/ETag conventions remain. GETs need no mutation-only header. At most 100 rows/1 MiB encoded bytes are returned; large integers remain decimal strings and timestamps preserve UTC microseconds. Private paths/configuration/commands/process internals/native payloads and raw audit detail are not exposed. Opaque IDs are not authorization. Analytics routes remain separate.

### Current validation

Executed locally October 21, 2026 using host JDK 21, the existing dedicated PostgreSQL 17.11 service and isolated random test databases. Credentials remained private environment variables; DDIP_TEST_PYTHON used the existing Python 3.12.15/DuckDB environment for the older Spark-free import suite. Synthetic later-state fixtures obey database constraints and do not represent executed workers or native publication qualifications.

| Exact command, repository root | Final observed result |
| --- | --- |
| `mvn -q -f services/content-api/pom.xml -Dtest=RunInspectionTest test` | **5** focused pure cursor/classification/encoding-limit cases passed |
| `mvn -q -f services/content-api/pom.xml -Ppostgres -Dtest=RunInspectionTest -Dit.test=RunHistoryIT,RunSubmissionIT verify` | Focused iterations passed; final two attempt/event page-boundary additions are included in the complete run below |
| `mvn -q -f services/content-api/pom.xml -Ppostgres -Dit.test=PersistenceIT,SourceManagementIT,ImportOwnershipIT,ImportWorkflowIT,RunSubmissionIT,RunHistoryIT -Dddip.test.history-restart-signal=/private/tmp/ddip-e5-history-restart verify` | **259 Java unit/component + 233 real PostgreSQL integration cases passed**, zero failures/errors/skips; external harness performed an actual dedicated DB restart at the READY barrier |
| `mvn -q -f services/content-api/pom.xml verify -Pintegration -Danalytics.python="$PWD/outputs/silver-python312/.venv/bin/python"` | **20 HTTP/Python analytics compatibility cases passed**, zero failures/errors/skips; the same 259 Java cases also passed, without double counting repeats |
| `npm run check` | **44** Markdown files/local links and repository hygiene passed |
| `git diff --check` and `git diff --cached --check` | Passed; index unchanged |

**Gate A:** all **45 new RunHistoryIT cases** use the actual PostgreSQL schema and persisted fixtures; 42 of those also exercise real loopback HTTP. Direct read-model cases and HTTP-backed cases together cover empty/queued/history order, product/source/state/combined filters, fanout avoidance, default/maximum bounds, static traversal, cursor tamper/scope/key/expiry checks, new arrivals, mutable-state membership, source revision history, actual attempts/stage counts, all seven run states, result verification linkage, controls, precise UTC timestamps and decimal values above JavaScript's safe range. Sequence/version/count fixtures exercise `9007199254740993` without floating point. A 5,001-run metadata fixture, including a 2 MiB private selection blob, confirms bounded projection; JDBC observation records exactly two page statements, and EXPLAIN confirms the submission-sequence index. No history projection loads the private blob. No speculative migration/index was introduced.

**Gate B:** **42 of the same 45 cases** send real HTTP through an actual loopback Spring server and real PostgreSQL, not MockMvc-only or mock persistence. They verify exact envelopes/DTOs, malformed/duplicate queries, 200/400/403/404/500/503 mappings, paging and scope checks, null/empty observed evidence, Host/Origin protection without mutation headers on GET, stage/attempt/event counts, public native result metadata and unsafe-metadata rejection. A test-only interceptor outside the production component scan injects an oversized response into the actual response encoder; HTTP receives only the fixed 500 envelope, with no successful/partial data. Ordinary large metadata-history pages remain within 1 MiB. Private audit details/commands/locations and malformed counter keys never enter responses. No production fixture/response-injection endpoint exists.

Read-only assertions preserve run states/versions, audit count and exact coordinator state. Actual history requests complete while another connection holds coordinator FOR UPDATE, demonstrating no ownership-row lock dependency. Event tests include persisted submission and attempt-linked audit, exclude unrelated source/other-run events, and reject mismatched attempt context. Attempts include EXECUTE/RECONCILE and only actual stored stage counts. Checkpoint C state fixtures insert metadata directly into disposable databases; no processing run is claimed or stage executed by the application.

The restart harness stops/restarts only the dedicated test container, waits for readiness and signals completion. The test requires a strictly later pg_postmaster_start_time and identical persisted cursor continuation/events after API restart with the same configured key. Separate tests cover same-key API restart, changed-key rejection, state transitions between pages, actual connection refusal across all four reads and source updates that leave captured history unchanged.

**Existing regressions:** 28 PersistenceIT, 36 SourceManagementIT, 42 ImportOwnershipIT, 38 Spark-free ImportWorkflowIT and 44 RunSubmissionIT passed (**188** older PostgreSQL cases), plus the new 45 = 233. Existing 254 Java cases plus five new = 259. Submission remains atomic/idempotent and QUEUED, its additive detail matches GET, analytics-only remains DB-independent, import fencing/host guard/recovery remains intact and worker mode remains idle. Native publication/verification code was not modified. The existing **23 Spark-backed SilverImportIT cases were explicitly not selected**, respecting this checkpoint's no-Spark boundary; they are exclusions, not reported JUnit skips or new successes. No live feed, Spark workload, retained personal dataset, benchmark or AWS experiment ran. Hosted CI remains **unobserved/pending owner publication**; only the existing postgres Maven test include gains RunHistoryIT.

Intermediate failures were test expectations/instrumentation: the first changed-key fixture accidentally reused the same zero key, query observation initially omitted the plain high-water statement, and an old B2a assertion still expected GET /runs to be absent. These were corrected; the final complete commands above passed without weakening production validation. Ignored logs are outputs/e5-b2b2b-gate-a-unit.log, e5-b2b2b-focused.log, e5-b2b2b-regressions.log, e5-b2b2b-http-analytics.log and e5-b2b2b-hygiene.log. The dedicated PostgreSQL container was initially stopped and was returned to stopped with its volume retained. No Git/GitHub write operation occurred.

### B2 closeout and Checkpoint C boundary

**Implemented:** revisioned sources and durable source commands; verified operator artifact imports; durable bounded queued submission/frozen selections; the requested bounded run/attempt/event history and safe detail APIs. These B2 checkpoints are ready for owner review. Broader capability discovery/HTTP artifact listing is not supplied by this history checkpoint. No whole-E5 acceptance group is declared complete.

**Planned, requiring separate owner authorization:** Checkpoint C must resolve pinned runnable deployment profiles/fixed adapters; implement host-guard/coordinator-fenced queue claiming; recheck source gates, native inputs/dependencies and runtime/storage readiness; persist real attempts/stage intents and observed outcomes atomically; honor admission/audit budgets; supervise cleanup and qualify interruption/recovery. New run IDs, attempts, native fingerprints and Silver operational inventory identities remain distinct. Existing queued runs have no native write authority.

**Deferred:** worker dispatch/execution/publication, processing controls/reconciliation, React Operations, activation, product authorization/exports, scheduling/distributed workers, cloud and benchmarks. Native GET requalification is intentionally absent. Spark-backed Silver qualification regressions were not selected under this checkpoint's no-Spark boundary. Historical verification does not certify present readability. New public classifier/counter/native-result mappings must be coordinated with future worker writers; arbitrary private metadata will not be passed through as public history. E5 is incomplete; stop after B2b-2B.

### B2b-2B files changed

- [docs/architecture/durable-execution.md](../../docs/architecture/durable-execution.md)
- [services/content-api/README.md](../../services/content-api/README.md)
- [services/content-api/pom.xml](../../services/content-api/pom.xml)
- [services/content-api/src/main/java/platform/contentapi/management/ManagementConfiguration.java](../../services/content-api/src/main/java/platform/contentapi/management/ManagementConfiguration.java)
- [services/content-api/src/main/java/platform/contentapi/management/RunController.java](../../services/content-api/src/main/java/platform/contentapi/management/RunController.java)
- [services/content-api/src/main/java/platform/contentapi/management/RunHistoryRepository.java](../../services/content-api/src/main/java/platform/contentapi/management/RunHistoryRepository.java)
- [services/content-api/src/main/java/platform/contentapi/management/RunInspectionValues.java](../../services/content-api/src/main/java/platform/contentapi/management/RunInspectionValues.java)
- [services/content-api/src/main/java/platform/contentapi/management/RunPageCursor.java](../../services/content-api/src/main/java/platform/contentapi/management/RunPageCursor.java)
- [services/content-api/src/main/java/platform/contentapi/management/RunRepository.java](../../services/content-api/src/main/java/platform/contentapi/management/RunRepository.java)
- [services/content-api/src/test/java/platform/contentapi/management/RunApiFixtures.java](../../services/content-api/src/test/java/platform/contentapi/management/RunApiFixtures.java)
- [services/content-api/src/test/java/platform/contentapi/management/RunHistoryIT.java](../../services/content-api/src/test/java/platform/contentapi/management/RunHistoryIT.java)
- [services/content-api/src/test/java/platform/contentapi/management/RunInspectionTest.java](../../services/content-api/src/test/java/platform/contentapi/management/RunInspectionTest.java)
- [services/content-api/src/test/java/platform/contentapi/management/RunSubmissionIT.java](../../services/content-api/src/test/java/platform/contentapi/management/RunSubmissionIT.java)
- [services/content-api/src/test/java/platform/contentapi/management/SourceManagementIT.java](../../services/content-api/src/test/java/platform/contentapi/management/SourceManagementIT.java)
- [services/content-api/src/test/java/platform/historyfixture/OversizedInspectionConfiguration.java](../../services/content-api/src/test/java/platform/historyfixture/OversizedInspectionConfiguration.java)

The entry branch/index/working tree was verified clean. All changes remain for the owner; no staging, commits, branch changes or GitHub operations were performed.

## C1 worker coordination acceptance boundary

The owner authorizes C1 coordination only. Acceptance requires real PostgreSQL proof of
atomic FIFO claims, source enablement gates, epoch/version fencing, rollback and exact
claim lookup; process tests must prove cooperative guard exclusion. Worker startup must
be non-web, validate the installation and pinned metadata, and retain the host guard.
Heartbeats are indications of liveness, never leases. Shutdown or uncertain acknowledgment
must preserve occupied ownership. Tests may inject a capability in isolated databases.
**Production dispatch remains disabled until the fixed Reliability adapter is implemented
and approved in C2.** C1 must leave production QUEUED runs untouched and create no native
processes, stage invocations or publication-success evidence.

### C1 implemented protocol and integration seam

C1 reuses V1–V7, the existing singleton coordinator, worker/attempt/run rows,
[InstallationHostGuard](../../services/content-api/src/main/java/platform/contentapi/execution/InstallationHostGuard.java)
and immutable B2 selections. No migration, dependency, HTTP route or native contract changes.
[WorkerRepository](../../services/content-api/src/main/java/platform/contentapi/execution/WorkerRepository.java)
has no process-launch or publication-success method. Its production constructor fixes
WorkerExecutionCapability.NONE; the test capability exists only under src/test. No property,
Spring bean, request or catalog entry can supply an executor. The passive lifecycle never
polls or claims queued work. The existing 20-unfinished-run admission bound therefore still
fills without draining.

Startup validates PostgreSQL 17/schema V7 without migrating, the current Java 21 JVM,
configured executable/script availability, canonical immutable run-profile bytes/digest,
the configured deployment digest, installation identity and associated storage descriptors.
It acquires the existing private CONTROL guard before worker registration, retaining it
through shutdown. Local roots reuse importStorageVersion=1 root/device/inode descriptors;
TEMP/STAGING are private, writable and disjoint from native roots and each other/control.
Bronze reuses the loopback-only importBronzeVersion=1 descriptor/private credentials validation,
without contacting S3. Profile-required roles, product/installation membership and at most
128 associated bindings are checked. All profiles must reuse the historical installation
CONTROL binding: worker and import history digests cannot silently change. No directories,
guard files, profiles or schema are created or repaired at startup.

With READY and no unresolved owner/run/attempt/import, registration records READY. Occupied
or RECOVERY_ONLY registration records a new RECOVERY_ONLY observer, retaining the old binding;
it cannot adopt the old attempt. A live cooperative owner prevents guard acquisition first.
STOPPING records/stops the new idle instance and starts no heartbeat loop. Invalid configuration
or schema fails closed. Runtime version strings/deployment SHA are reviewed admission pins;
C1 does **not** probe Python/PySpark, hash a packaged deployment or qualify a runnable adapter.
Those C2 preflights must precede enabling the capability.

A claim with a test-only available capability takes one short transaction in this order:

1. Lock coordinator; compare installation, READY mode, exact session epoch/coordinator and
   worker row versions, and absence of execution/recovery/import ownership or unresolved rows.
2. Select the lowest QUEUED submission_seq. Lock its current source rows in product/source
   order, then lock/recheck the run. No SKIP LOCKED or in-memory queue is used.
3. Check live enablement while retaining the captured revision. Disabled sources produce
   FAILED/SOURCE_DISABLED with SOURCE_DISABLED_AT_DISPATCH and no attempt. Incompatible/invalid
   pinned metadata produces FAILED/RUNTIME_MISMATCH and RUN_FAILED, so an incompatible head is
   observable rather than silently skipped. C1 uses an exact configured profile ID/digest match.
4. Revalidate canonical request/selection digests, exact run_sources/run_inputs copies,
   immutable source revisions, artifact metadata qualification shapes/native identities,
   profile/binding associations and homogeneous Reliability source/batch correspondence.
   [FrozenRunSelection](../../services/content-api/src/main/java/platform/contentapi/management/FrozenRunSelection.java)
   performs database metadata checks only; no filesystem hash, S3 GET or native verifier runs.
5. Increment epoch, insert EXECUTE/CLAIMED with a caller-allocated attempt UUID and monotonically
   increasing attempt_no (maximum 20), change run to RUNNING/current attempt, bind coordinator
   worker/attempt/epoch and append RUN_CLAIMED plus ATTEMPT_CLAIMED. Commit before returning a token.

The run's current stage, counts and result remain null; no stage or success is invented.
Existing inspection APIs expose those persisted records with their approved classifications.
Ownership operations reject an outer transaction. Guard/root checks occur before SQL;
no transaction spans external qualification or execution. Audit additions stay within the
existing one-million-event bound and submission-reserved lifecycle budget.

Owner tokens include installation, instance, attempt, epoch, coordinator/worker/run/attempt
versions and expected states. Every mutation checks all applicable fields under coordinator-first
locks; conditional writes require exactly one row. Heartbeat updates worker and occupied-slot
UTC timestamps/versions atomically, without changing attempt/run state or appending audit.
The fixed interval is five seconds; older than thirty seconds is an uncertainty indication,
not release authority or a measured timing guarantee. Manual tick and database timestamp
fixtures make tests deterministic. Idle observer heartbeats never update the old owner's slot.
A database failure or stale token latches off further authorization in that repository;
no method refreshes/adopts a token. Database failure retains the occupied persisted condition.

Internal markRunning commits ATTEMPT_STARTED without launching anything. stageIntentTransaction
is a JDBC-only C2 seam joining ownership/control checks and future stage INTENT metadata in one
transaction; its callback must never perform external I/O. It is not a stage executor or a launch
authorization. C2 must add fixed stage intent/process-identity/outcome persistence, check controls
again before launch, supervise actual processes and verify effects before any success. Known
pre-launch failure is allowed only from CLAIMED with no stage intent and the trusted protocol's
proof that no side effect was authorized; absence of a PID alone is insufficient. It atomically
closes FAILED, records attempt/run audit and clears only this owner. No retry eligibility is asserted.

Explicit interrupt records INTERRUPTED plus RECOVERY_REQUIRED/RECOVERY_ONLY while preserving the
occupied attempt pointer. Abrupt JVM death or lost claim acknowledgment can instead leave the
original RUNNING run/CLAIMED attempt occupied; both block a restart. Exact attempt-ID lookup
returns a **historical receipt only**, never a new token or relaunch permission. C1 provides no
reconciliation/handoff/retry executor. Clean idle stop records STOPPED, closes heartbeat scheduling
and releases the guard with queued captures unchanged. Uncertain stop does not clear ownership;
C1 has no native child to clean up and makes no orphan-descendant termination guarantee.

### C1 local validation and remaining work

Local baseline: clean owner-prepared feature/e5-durable-execution-source-controls at
78d32f537b0e82502ce0368e33680dcc596019a9. The existing index was preserved. Java 21.0.7,
PostgreSQL 17.11, Python 3.12.15/PySpark 4.0.2 and the already provisioned isolated test runtime
were used. Docker operations targeted only ddip-b1-postgres-test, initially stopped; no volumes,
retained native datasets or operational database were removed. Hosted CI is unobserved.

Set JAVA_HOME to the pinned JDK 21 and DDIP_TEST_DB_URL/USER/PASSWORD to the dedicated disposable
service; set DDIP_TEST_PYTHON to the approved existing test interpreter. Exact Maven invocations
from the repository root (test wrappers supplied credentials privately, never printed them):

| Command | Observed result |
| --- | --- |
| `mvn -q -f services/content-api/pom.xml verify -Ppostgres -Dit.test=WorkerCoordinationIT -Dgroups=C1_GATE_A -Dtest=NoUnitTests -Dsurefire.failIfNoSpecifiedTests=false` | **29 Gate A PostgreSQL cases passed**, no skips/failures/errors |
| `mvn -q -f services/content-api/pom.xml verify -Ppostgres -Dit.test=WorkerCoordinationIT -Dgroups=C1_GATE_B -Dtest=NoUnitTests -Dsurefire.failIfNoSpecifiedTests=false` | **26 Gate B PostgreSQL/lifecycle/process cases passed**, no skips/failures/errors |
| `mvn -q -f services/content-api/pom.xml -Ppostgres -Dddip.test.worker-restart-signal=/private/tmp/ddip-c1-worker-restart -Dddip.test.history-restart-signal=/private/tmp/ddip-c1-history-restart verify` | **261 Java + 311 PostgreSQL cases passed**, no skips/failures/errors; external harness performed both actual dedicated server restarts |
| `mvn -q -f services/content-api/pom.xml verify -Ppostgres -Dit.test=WorkerCoordinationIT,PersistenceIT` | Final worker-path/STOPPING/shutdown changes: **261 Java + 83 PostgreSQL cases passed**, no skips/failures/errors; repeated cases are not added to distinct totals |
| `mvn -q -f services/content-api/pom.xml verify -Pintegration -Danalytics.python="$PWD/outputs/silver-python312/.venv/bin/python"` | **20 existing HTTP/Python compatibility cases + the same 261 Java cases passed**, no skips/failures/errors |

The 311 PostgreSQL cases comprise 28 persistence, 36 source management, 42 import ownership,
38 Spark-free imports, **23 Spark-backed Silver imports**, 44 submission, 45 history and 55 C1.
The 261 Java cases comprise the existing 259 and two internal-evidence encoding/boundary checks.
Gate A/B tags partition the 55 C1 cases; they are not additional tests. The default postgres
Maven/CI profile now includes WorkerCoordinationIT; no CI job/service/dependency was replaced.

Gate A uses real row locks, constraints, deferred FKs and commits. It covers all three
pipeline/profile shapes with test capability, FIFO, concurrent winner, monotonic attempt number,
rollback at attempt/run/coordinator/audit insertion and after the first claim audit, both source
update/claim orderings, disabled sources, coherent immutable revisions, invalid frozen evidence,
profile rejection, exact lost-ack lookup and stage-intent/control fencing. Concurrency barriers
and observed PostgreSQL coordinator lock waits establish ordering. Namespace snapshots compare
all selected fixture-root entries/bytes/mtimes around claim/inspection; no stage/process/result
is created. Intentionally injected failures were corrected during development before these final
passes; no failing/skipped case is counted as passing.

Gate B exercises actual non-web Spring startup, guard-bound identities and heartbeats, idle stop,
STOPPING, configuration/schema/installation mismatch, import exclusion, unresolved prior attempts,
stale epoch/version, database outage, historical guard pinning and outer-transaction rejection.
Separate JVMs demonstrate cooperative guard exclusion. A deliberately killed **test-only** claim
owner leaves its attempt/slot occupied; another worker cannot claim it. This observes local JVM
exit, not all hypothetical orphan descendants. Claim receipts and frozen run bytes survive a
strictly later PostgreSQL postmaster start time; exact receipts authorize nothing. Production
capability remains absent even when unknown fake-executor/dispatch properties are supplied.
No test capability is present in the packaged production JAR. Existing GET inspection shows
actual CLAIMED/RUNNING/INTERRUPTED/RECOVERY_REQUIRED records without fabricated results.

The full regression included the 23 Silver cases excluded at B2b-2B. Their synthetic fixture
setup/read-only qualification is distinct from C1 worker processing. No live publisher, new
production transformation, benchmark, retained personal dataset or AWS operation ran. The
native Python/verifier modules, fingerprints, accepted ADRs and V1–V7 remain byte-for-byte
unchanged against the committed baseline. A C1 sentinel also retains its exact bytes/mtime.
`npm run check` passed for all **44 Markdown files**, local links and repository hygiene.
`git diff --check` and `git diff --cached --check` passed; V1–V7 were independently
compared byte-for-byte with HEAD, and the initially empty index remains empty. The dedicated
PostgreSQL test container was restored to its initial stopped state after testing.

**Implemented:** passive worker readiness/guard lifetime, metadata claim repository and exact
historical lookup, epoch/version fencing, bounded heartbeats, known pre-launch closure,
uncertain occupied interruption and C2 JDBC intent seam. **Tested:** the local gates/regressions
above; production dispatch has no executor. **Planned for separately authorized C2:** a fixed
Reliability adapter, executable deployment/native dependency preflight, fixed stage intent and
process identity/outcome persistence, launch control recheck, bounded child supervision and
independent final result verification/registration. The current exact-profile match is deliberately
conservative; supporting multiple compatible pinned profiles requires explicit adapter preflight,
not substituting current configurations. **Deferred:** Content/RSS execution, controls, execution
reconciliation/handoff, automatic retries, Operations UI and E5 completion.

Known limits: PostgreSQL fences metadata, not stale filesystem writers; the guard is cooperative.
No heartbeat, free guard, JVM exit or lost connection proves quiescence. Uncertain ownership
requires explicit future reconciliation, and C1 supplies no operator execution-owner recovery
command. No packaging/runtime authenticity, hostile-storage, other-host orphan cleanup,
power-loss/backup restore or hosted CI guarantee is inferred. Native inputs remain subject to
fresh worker-side revalidation before consumption; historical qualification is not readability.

## C2a Reliability preflight acceptance boundary

The owner authorizes C2a read-only runtime/native preflight and CHECK_INPUTS persistence only.
Production WorkerExecutionCapability.NONE stays fixed; no queued production run may be claimed.
Gate A must qualify actual small native JSONL/Parquet/reference selections and the pinned runtime,
without native writes. Only after Gate A passes may Gate B integrate real PostgreSQL intent,
fenced single launch, process identity, result/cleanup evidence and failure injection.
No publication, output namespace reservation, final result verification or run success is included.
V1–V7 already represent the fixed stage; accepted ADRs and native contracts remain unchanged.

### Fixed runtime and frozen native selection

C2a implements the internal `ReliabilityPreflight.verify` operation, not a production runner,
HTTP endpoint or selectable executor. `WorkerExecutionCapability.NONE` remains the production
capability. Real submissions stay QUEUED; a READY worker still cannot drain the queue. Only
isolated test-classpath C1 capabilities can claim an attempt and invoke this operation.
Successful CHECK_INPUTS is neither a successful run nor a verified publication.

`RunProfile` v1 remains unchanged and metadata-only. A separately provisioned immutable
`execution_profiles` row has `executionRuntimeVersion=1`, `pipeline=RELIABILITY_V2`, the frozen
`runProfileId`/`runProfileSha256`, trusted `python`, `javaHome`, `processingRoot`,
`deploymentSha256`, `deploymentRevision`, `bindings` and `verificationSeconds` (1–900).
The six binding keys are exactly batch/reference/output/staging/temp/control, with existing
BATCH/REFERENCE/RELIABILITY/STAGING/TEMP/CONTROL roles. Each is associated with this execution
profile and installation and must equal the corresponding frozen descriptor. Roots are
canonical, identity-pinned, disjoint, readable, and writer roots are writable; staging/temp/control
are private. CONTROL is exactly the held host binding. Deployment files are outside these roots.
This is an execution-only compatibility boundary, not another profile store or client setting.
Old run profiles are never rewritten to make them runnable; incompatible pins require a new
reviewed profile and submission.

The approved deployment contains canonical `e5-reliability-deployment.json` with exactly
`deploymentRevision` (40 lowercase hex), `versions`, `spark` and `modules`. Its SHA-256 must
equal the frozen run profile's deployment SHA and the execution-only descriptor's SHA.
`modules` maps each fixed filename in `ReliabilityRuntime.MODULES` to its exact file SHA-256;
the same allowlist is checked in Python. It includes the fixed preflight and authoritative native
readers/validators, not a client-selected module. Java and the child check manifest/module bytes
before and after qualification. These hashes establish deployment consistency, not authenticity
against a trusted operator modifying both code and pins.

Actual Java must be 21; the authorized read-only Python child checks Python **3.12.15**,
PySpark **4.0.2** and DuckDB **1.4.5**. Importing PySpark does not start a Spark session/gateway.
The pinned future Spark configuration is local[2], shuffle/default parallelism 8, driver/executor
1024 MiB, output partitions 4 and event logging false, validated with the native configuration
validator. No dependency install, runtime fallback, Spark normalization or publication occurs.
The environment is cleared and rebuilt with only PATH, TZ, LANG, JAVA_HOME, TMPDIR and fixed
BLAS/OpenMP thread limits. No ambient AWS credentials, proxies or Python executable overrides
are inherited. The exact trusted executable and fixed script use an argument list, never a shell.

The immutable canonical captured selection is revalidated under C1 fencing. Its profile,
synthetic-only source revisions, registered evidence digests, exact source/batch correspondence,
homogeneous JSONL-v2 or Parquet-v1 input profile and service reference must agree. Current source
enablement remains the C1 claim gate; a later source edit does not replace frozen configuration.
Historical registry qualification alone is insufficient. Existing import namespace/capture helpers
and full native `reliability_format_inputs.open_batch` / `service_reference.open_snapshot` checks
are mandatory, followed by **one joint** `reliability_format_inputs.verify_dependencies` and
`Dependencies.check`. Native profiles and selected roots must satisfy that reader's compatibility
rules; there is no multi-root union, latest lookup or substitution.

The child compares fresh completion bytes, exact file inventory/hash/length and filesystem stamps
to imported evidence, invokes format-specific physical/provenance verification, then recaptures
all selected evidence after the joint verification. The unchanged native dependency manifest and
its native fingerprint are recorded privately. Literal synthetic JSONL dependency vector:

~~~text
SHA256(canonical(nativeDependencies)) =
3b9354d578f017cefea8e9ace2556c6ed474afec606722ee01e9fba8e4a39a7a
files=1, bytes=2439, rows=10
~~~

`reliability_preflight_fixtures.py` pins the original fixture timestamps and native evidence; the
test checks this literal independently of generated operational IDs. Multi-batch ordering is
deterministic. This is an existing native dependency identity, not a newly published Reliability
fingerprint. No operational UUID, timestamp, root or worker identity is added to it.

Existing native budgets remain authoritative: 32 batches, 128 raw files, 1 GiB raw selection,
2 million physical occurrences and the existing file/row-group/metadata limits. DuckDB uses
its independent two-thread, 512 MiB, no-spill/extension-install policy. At least **1 GiB available
space on each relevant writer volume** is checked before authorization and at heartbeat/final
checks. Staging/output/scratch are fixed but no output namespace is reserved. These are
cooperative admission checks, not quotas or a guarantee that space stays available.

### CHECK_INPUTS commit, launch and private evidence

No SQL transaction spans runtime filesystem checks or native verification. The flow is:

1. Revalidate the exact claimed owner and frozen selection; resolve and check approved runtime.
2. Write bounded private command/selection sidecars under the held CONTROL root.
3. Atomically mark CLAIMED → RUNNING when needed, insert CHECK_INPUTS/ordinal 0 INTENT, set
   current stage and append ATTEMPT_STARTED/STAGE_INTENT. Commit before launch.
4. Recheck runtime, exact command/selection bytes, host guard, worker/attempt/epoch, applicable
   versions, RUNNING state and outstanding controls. In a separate short transaction bind the
   immutable intended command digest/address and append STAGE_LAUNCH_AUTHORIZED.
5. Launch once only after confirmed authorization. Persist observable PID/start instant/instance
   identity with STAGE_STARTED, then run bounded read-only verification outside transactions.
6. Validate exit/result agreement, exact identities, fresh bounded evidence and confirmed observed
   process cleanup. Write a parent validation anchor; conditionally commit STAGE_VERIFIED and
   stage result/counts. Run/attempt remain RUNNING and coordinator ownership stays occupied.

The existing C1 coordinator-first fencing and repository failure latch apply to every mutation;
zero-row CAS never adopts a new token. SQL statements only run inside short transactions.
INTENT's `intended_identity` is the command digest for this read-only stage, not a product
fingerprint. `command_address` is initially null; binding it consumes the single authorization.
The state stays INTENT until the observed child identity can be committed. Bound command with
no recorded child is **LAUNCH_UNCERTAIN**, not proof that no child started. No new stage state,
run state, maintenance owner, queue or migration is introduced.

Private directory: `runs/<run>/attempts/<attempt>/check-inputs/<checkpoint>` under CONTROL.
Directories are 0700 and files 0600, created exclusively, with no-follow/identity/stability checks.
The fixed sidecars are `command.json`, `selection.json`, `result.json`, `evidence.json` and
`verified.json`. Canonical UTF-8 JSON rejects duplicate fields, floats, noncanonical bytes and
unsupported protocol shapes. Command/result/anchor are <=64 KiB each; selection/evidence are
<=16 MiB each. No raw JSONL/Parquet/CSV payload is stored. Native metadata descriptors are private.

Command v1 binds run/attempt/epoch, CHECK_INPUTS/ordinal 0, checkpoint UUID, canonical captured
selection SHA, execution-runtime profile ID/SHA/configuration, fixed selection filename and bundle
SHA. Result v1 binds run/attempt/stage/ordinal, status OK or FAILED, fixed code, evidenceId and
decimal-string counts. Exit 0 requires OK with checkpoint evidenceId; exit 1 requires an
allowlisted FAILED result. The private evidence binds the same identities plus policy
`E5_RELIABILITY_PREFLIGHT_V1`, actual runtime tuple, deployment SHA, exact imported evidence
digests/captures, native dependency manifest/fingerprint and aggregate files/bytes/rows.

The parent verifies those identities and counts, then writes `verified.json` binding the command,
result, evidence and captured-selection SHA values, checkpoint, native dependency fingerprint and
`OBSERVED_CLEAN_EXIT`. The stage's result address/SHA anchors this wrapper. Successful
CHECK_INPUTS creates no artifact, stage-artifact link, publication fingerprint or run result.
Existing history shows currentStage CHECK_INPUTS, persisted stage count/events and no invented
result/counts at run level. The only public allowlist additions are STAGE_LAUNCH_AUTHORIZED
and RESOURCE_LIMIT; private descriptors, commands, raw streams and roots remain absent.

### Supervision, interruption and acknowledgment ambiguity

The dedicated supervisor closes stdin, concurrently drains/discards stdout/stderr with independent
16 MiB bounds, and enforces a configured deadline <=15 minutes. Cleanup allows up to 5 seconds
graceful, 5 seconds forced and 5 seconds final drain. Heartbeats are short fenced operations every
5 seconds, without heartbeat audits; database failure authorizes no further work. The fixed reader
is not expected to spawn children. Any observed descendant is conservatively uncertain, even if
identity-aware termination succeeds. PID/start identity is checked before killing observed handles.
No universal macOS/Linux process-tree containment or proof about unobserved descendants is claimed.

Known, cleanly terminated read-only failures can close the **stage only** as FAILED with fixed
INPUT_UNAVAILABLE/INPUT_INVALID/INPUT_CHANGED/RUNTIME_MISMATCH/RESOURCE_LIMIT or process-bound
codes. The run/attempt/slot remain occupied; C2a does not supply terminal pipeline failure/release.
Invalid result protocol, interruption or uncertain termination marks stage UNKNOWN, interrupts the
attempt and moves the run/coordinator to RECOVERY_REQUIRED/RECOVERY_ONLY when the current
owner and database can commit that change. If ownership/database finalization is uncertain,
the existing occupied state is retained instead. No timeout, heartbeat age, JVM exit or free host
guard releases ownership. Recovery remains explicitly authorized future work.

Intent, authorization and finalization use stable checkpoint identity. A lost acknowledgment stops
the call without another launch. `inspectCheckInputs` returns historical classification only:
CONFIRMED_ROLLBACK, INTENT_COMMITTED, LAUNCH_UNCERTAIN, CHILD_RECORDED,
HISTORICAL_VERIFIED or UNRESOLVED. It cannot refresh ownership or authorize continuation.
Even a confirmed historical verification does not establish current dependency readability.

### C2a validation and C2b boundary

Gate A completed before PostgreSQL integration: **30 native tests passed**, using actual JSONL,
Parquet and service-reference readers, joint dependency verification, literal/deterministic identity,
runtime/module/root mismatch, missing/corrupt/changed evidence, disk limits and exact unchanged
native bytes/mtimes. Gate B passed **36 real PostgreSQL cases and 11 supervisor unit cases**.
All fixtures are small, synthetic and isolated; fixture authoring is test setup, never preflight.

Final local validation on October 21, 2026 used Java 21.0.7, Python 3.12.15, PySpark 4.0.2,
DuckDB 1.4.5 and the dedicated PostgreSQL 17.11 test container. Exact underlying commands
(DB credentials supplied only through the existing private test environment):

~~~sh
# Gate A, before PostgreSQL integration: 30 passed
JAVA_HOME=$(/usr/libexec/java_home -v 21) PYTHONPATH=processing:processing/tests \
outputs/silver-python312/.venv/bin/python -B -m unittest \
  processing/tests/test_reliability_execution_preflight.py -v

# Gate B: 11 Java + 36 PostgreSQL passed
mvn -q -f services/content-api/pom.xml verify -Ppostgres \
  -Dit.test=ReliabilityPreflightIT -Dtest=VerifierProcessTest

# Full regression: 272 Java + 347 PostgreSQL passed, no skips/failures/errors
mvn -q -f services/content-api/pom.xml -Ppostgres \
  -Dddip.test.worker-restart-signal=/private/tmp/ddip-c1-worker-restart \
  -Dddip.test.history-restart-signal=/private/tmp/ddip-c1-history-restart verify

# Additional real HTTP allowlist case added after the full run: 1 passed
mvn -q -f services/content-api/pom.xml verify -Ppostgres \
  '-Dit.test=RunHistoryIT#preflightAuthorizationAndResourceFailureAreSafePublicEvents' \
  -Dtest=NoUnitTests -Dsurefire.failIfNoSpecifiedTests=false

# 85 native tests passed (includes the same 30 C2a cases, not 115 distinct cases)
JAVA_HOME=$(/usr/libexec/java_home -v 21) PYTHONPATH=processing:processing/tests \
outputs/silver-python312/.venv/bin/python -B -m unittest \
  processing/tests/test_reliability_execution_preflight.py \
  processing/tests/test_reliability_format_inputs.py \
  processing/tests/test_reliability_format_admission.py \
  processing/tests/test_reliability_format_verification.py \
  processing/tests/test_reliability_inputs.py \
  processing/tests/test_reliability_verification.py -v

# 272 Java + 20 HTTP/Python compatibility cases passed
JAVA_HOME=$(/usr/libexec/java_home -v 21) \
mvn -q -f services/content-api/pom.xml verify -Pintegration \
  -Danalytics.python="$PWD/outputs/silver-python312/.venv/bin/python"
npm run check
git diff --check
git diff --cached --check
~~~

Across the full PostgreSQL regression and the additional HTTP case, **348 distinct PostgreSQL
tests passed**: the existing 311, 36 C2a stage cases and one safe-event HTTP case. The 23
Spark-backed Silver import regressions are included, with no C2a Reliability publication.
The external restart harness observed READY signals, restarted only the disposable server and
required later postmaster start times at both C1/history barriers. This tests existing claim/history
durability; C2a does not claim a separate power-loss or process-tree containment guarantee.

Gate B uses actual native JSONL/Parquet fixtures and actual controlled JVM children. It proves
intent visibility before launch, observed PID persistence, cancellation/version/epoch fencing,
audit rollback, real commit followed by injected acknowledgment loss at all three boundaries,
safe database refusal before/after child launch, native changes after import, child startup/nonzero
exit/timeout/stream bounds, malformed results, interruption and observed-descendant uncertainty.
Every case asserts occupied ownership, no SUCCEEDED run, no publication result or stage-artifact
link. The HTTP case checks only persisted safe classifications, not fabricated native execution.
Production NONE is tested before sidecar/stage creation; existing C1 production-startup tests
also leave real queued selections unchanged. Native read-only tests snapshot all selected roots'
entries/bytes/mtimes and reject any staging/publication call.

Markdown lint (44 files), relative links and repository hygiene passed. V1–V7 and accepted ADRs
remain unchanged, no migration or new dependency is introduced, and the initially empty index
remains empty. The dedicated PostgreSQL test container was restored to its initial stopped state.
Local logs are ignored outputs; hosted CI remains pending owner publication and observation.
No live feed, retained personal data, AWS resource or large benchmark was used.

**Implemented:** internal runtime/native qualification, durable fenced CHECK_INPUTS, fixed bounded
read-only supervision, private result/evidence anchoring and historical acknowledgment inspection.
**Tested:** actual local evidence listed here and in the operator guide; no native processing claim
is inferred. **Planned for separately authorized C2b:** consume/recheck this same pinned command,
bundle and evidence (`recheck` requalifies and requires exact agreement), implement the fixed
publisher argument construction, stage intent/launch/process/result lifecycle, independent native
output verification and transactional verified publication registration before enabling a real
production capability. A successful preflight is a point-in-time check, not a filesystem snapshot;
C2b must revalidate before consumption. No Spark normalization, aggregate equivalence or output
correctness is established here. **Deferred:** production dispatch/publication, retry/recovery
takeover, cancellation endpoints, Content/RSS execution, UI, activation and E5 completion.

## C2b-1 fixed Reliability publication and verified completion

C2b-1 implements an internal Reliability-specific adapter for isolated test-owned runs only.
**Production WorkerExecutionCapability.NONE and passive WorkerIdleLifecycle remain unchanged.**
No queue loop, activation property, fake packaged executor, management execution endpoint or
analytics-publication selection is introduced. C2b-2 requires separate owner authorization.

### Profile eligibility and immutable runtime

C1 inspection found that claiming the oldest run without eligibility filtering could fail a valid
Content run or a run pinned to another format profile. Claims now select the oldest QUEUED run
**within the current worker's exact immutable execution profile and supported fixed pipelines**.
Other valid profile/pipeline selections stay QUEUED. Source disablement and invalid captured
metadata still reject a genuinely eligible selection under the existing coordinator-first lock order.
This preserves submission order among executable runs without rewriting captured configurations.
There is no SKIP LOCKED, secondary queue or in-memory scheduling authority.

One worker instance remains pinned to one RunProfile ID/SHA and deployment SHA. Both Reliability
formats are supported by the same fixed adapter, but approved JSONL and Parquet profiles remain
distinct. An idle worker may later be stopped and a separately reviewed instance started for the
other profile; C2b-1 does not implement profile switching or a production policy for choosing among
profiles. Broader multi-profile readiness/fairness belongs to C2b-2 review, not silent compatibility
coercion. No accepted ownership model or native profile is changed.

The execution-only runtime descriptor remains version 1. The approved deployment manifest's exact
module map now also includes `reliability_execution.py`. Its digest still must equal both frozen
RunProfile and execution-runtime pins. Existing deployment pins are never rewritten; deploying this
new wrapper requires a newly reviewed matching deployment/profile, not a fallback for old runs.
Java 21/Python 3.12.15/PySpark 4.0.2/DuckDB 1.4.5 and the C2a fixed local Spark/DuckDB/resource
policies remain unchanged. No package installation or machine-specific setting enters native identity.

### Retained preflight and three fixed stages

The internal entry is `ReliabilityExecution.run(guard, verifiedPreflight, runtimeId)`. The supplied
C2a CHECK_INPUTS must be VERIFIED for the exact current run/attempt. Parent code reopens the
original private command/result/selection/evidence/validation anchor and checks their exact
hashes against the persisted checkpoint and captured selection. It revalidates runtime, host guard,
storage binding identities, private permissions and disk reserve before continuing. A historical flag
or caller-supplied fingerprint alone cannot qualify publication.

A bounded private `retained.json` carries those original descriptors to the fixed child. Before
invoking the native publisher, the child performs full `reliability_execution_preflight.recheck`
with the authoritative batch/reference and joint dependency readers. The exact original native
dependency fingerprint must still match. Stage authorization permits this fixed wrapper only;
its write path is gated behind that fresh native qualification. No substitute input, profile, root,
new registration, latest discovery or operational identifier enters publisher arguments.

The only new native wrapper stages are PUBLISH_RELIABILITY and VERIFY_RESULT, ordinal zero.
Each follows INTENT → separate single-use launch authorization → observed-child RUNNING →
verified disposition. Prior-stage VERIFIED and current run stage enforce their order. Immutable
`input_evidence_sha256` binds the canonical stage command (including retained-evidence SHA);
`intended_identity` is the **native** fingerprint and `intended_namespace` is exactly
`v2/processor_revision=2/publication=<native fingerprint>`. Private command/result addresses
remain in CONTROL. Command/result/parent anchor are <=64 KiB, retained/evidence <=16 MiB.

Parent sidecars use the existing private no-follow/exclusive-file policy under
`runs/<run>/attempts/<attempt>/publish-reliability/<checkpoint>` or
`runs/<run>/attempts/<attempt>/verify-result/<checkpoint>`. The existing protocol-v1 result fields
and decimal-string counts are unchanged. Every mutation checks installation, worker, attempt,
epoch, expected state and applicable versions. No PostgreSQL transaction spans native I/O or Spark.
Bound command with uncertain launch acknowledgment is never a permission to launch again.

PUBLISH_RELIABILITY constructs arguments from the frozen native dependency manifest and approved
roots and calls **unchanged** `reliability_format_publication.publish`. Its 20/21/17-column
contracts, rejection/identity accounting, five-minute UTC arithmetic and rank-qualified approximate
p95 remain native responsibilities. A completed identical namespace takes native read-only replay
without starting Spark or recalculating approximation. Incomplete namespaces are never repaired.
The native publisher's own completion/read-back checks do not replace VERIFY_RESULT.

VERIFY_RESULT is a distinct Spark-free child using **unchanged**
`reliability_format_analytics.open_publication`. It verifies all retained dependencies, completion
and output file inventories/hashes, exact physical schemas/provenance, independent raw-to-output
reconciliation, counts/arithmetic and p95 rank qualification. It executes three bounded native
reports (requests_5m over one observed five-minute window, input_coverage and reference_coverage,
limit 1), then rechecks native evidence. Report rows are not stored in the operational registry.

### Spark supervision and cleanup

The default C2a supervisor still refuses every observed descendant and retains its <=15-minute
read-only limit. An explicit internal Spark mode permits bounded observed descendants and a
<=30-minute processing limit; it cannot be selected through configuration or HTTP. Both modes
use closed stdin, explicit argument lists, a cleared allowlisted environment, independent 16 MiB
stream bounds and 5-second graceful/forced/final-drain cleanup phases. Stage deadlines are also
capped by remaining time to the attempt's two-hour deadline; authorization and heartbeats check
that deadline in PostgreSQL. These are operational limits, not measured timing guarantees.

The wrapper starts the fixed local Spark session only for a new native namespace. JVM local/temp
and warehouse files use approved private scratch; native staging/output are used only by the
native publisher. Scratch and incomplete native artifacts are retained, without automatic deletion.
It stops Spark, shuts down Py4J, closes the gateway's stdin and waits up to five seconds for its
actual child to exit. The parent requires the reaped gateway PID to match observed descendants,
tracks start identities, and requires every observed descendant to be gone before clean completion.
Live descendants discovered after apparent parent completion make the outcome uncertain even
when subsequent identity-aware cleanup succeeds. Unknown/reused identities are never killed
blindly. No universal macOS/Linux containment or proof about unobserved descendants is claimed.

### Atomic result, replay and immediate failure

The final independent reader produces a bounded immutable qualification descriptor: native
product/kind/version/format/fingerprint, approved binding and native relative namespace, original
completion/inputs bytes, exact sorted file lengths/hashes (including native bookkeeping), and safe
counts. It contains no run/attempt timestamp or machine path in the native fingerprint. A parent
anchor binds the exact command, retained bundle, child result, qualification evidence and observed
clean cleanup. Historical verification is not current readability or analytics activation.

Under the current fenced coordinator, one short transaction persists/reuses the RELIABILITY_V2
artifact (contract/processor 2), exact retained batch/reference dependency links, VERIFIED
VERIFY_RESULT and stage_artifacts RESULT linkage, SUCCEEDED attempt/run, final counts/audits
and conditional coordinator release. Existing V7 native-format uniqueness and location constraints
remain authoritative. Identical artifact reuse requires exact immutable evidence/location/dependency
agreement; conflicts do not overwrite anything. Any artifact/link/audit failure rolls back all final
trusted state. No V8 is needed; V1–V7 are unchanged.

Definitive pre-write runtime/evidence failure with known ownership, failed process startup before
publication, or fixed native failure with proven quiescence and freshly qualified ABSENT/INCOMPLETE
output may close FAILED and release the slot atomically. Incomplete output is retained with
INCOMPLETE_OUTPUT and retry ineligible. C2b-1 conservatively leaves all such failures retry ineligible;
it implements no retry control. Raw native classifications stay private and map to fixed safe codes.
A dependency I/O failure, invalid result, unconfirmed gateway, timeout/interruption, stale fencing or
ambiguous database outcome does not establish a successful result or permission to release.
Where possible UNKNOWN/INTERRUPTED/RECOVERY_REQUIRED is persisted; otherwise the already
occupied state remains. C3's explicit reconciliation/handoff is not implemented here.

A completed native result after a lost native response can be reopened without another writer and
must still pass the separate VERIFY_RESULT stage. A lost final PostgreSQL acknowledgment is
resolved using the exact known run/attempt and persisted result/stage linkage through historical
inspection; the old repository's failure latch prevents another launch. No automatic retry,
namespace cleanup, output overwrite or publication activation occurs.

### C2b-1 validation and remaining activation boundary

Gate A completed before database acceptance: eight native cases passed using actual small Spark
publishers for JSONL/Parquet, compatible multi-batch delivery, independent reader processes,
exact accounting/rank qualification, immutable replay, corrupt/incomplete output refusal, runtime
and retained dependency mutation, and proven-quiescent native identity-conflict failure. Database
and integrated regression results are recorded after the final local runs below.

**Implemented:** fixed publication/independent-reader wrapper, retained preflight validation,
fenced ordered stages and single-use launch, bounded Spark cleanup evidence, atomic verified
result registration/completion, native immutable replay and conservative immediate failure.
**Tested:** only the actual local gates/regressions recorded here; hosted CI remains pending.
**Planned for separately authorized C2b-2:** review deployment/profile eligibility and starvation
policy, wire a real fixed capability to the passive worker, handle returned lifecycle tokens safely,
prove production startup/admission/dispatch and shutdown, then explicitly approve queue draining.
**Deferred:** production activation, automatic retries/recovery takeover, C3 controls/handoff,
Content/RSS execution, UI, analytics activation and E5 completion. STOP after C2b-1.

#### Local C2b-1 validation evidence

The final local environment was Java 21.0.7, Python 3.12.15, PySpark 4.0.2, DuckDB 1.4.5 and
PostgreSQL 17.11. The full regression passed **274 Java and 375 PostgreSQL cases**, with no
failures/errors/skips, including **26 C2b lifecycle**, **36 C2a preflight**, **56 C1 worker** and
**23 Spark-backed Silver** cases. Three external harness barriers restarted only the dedicated
server: claim durability, history/cursor durability and actual verified Reliability result linkage.
Each required a newer postmaster start time, not merely another connection. Actual result, attempt
and event GETs were checked after the Reliability restart with unchanged public DTOs.

A subsequent cleanup-hardening run passed **275 Java cases** (including the new denied-descendant
observation test) and **37 PostgreSQL cases** (36 C2a plus retained-preflight failure). The known
parent is now terminated even when descendant enumeration is denied; uncertainty still blocks
ownership release. Counts across repeated runs are not added as new distinct coverage.

Native qualification passed eight Gate A cases; Spark-free C2a/native input/reader regressions
passed 85; isolated E2 processing/publication and E3 publication regressions passed 8/17/8.
That is **126 distinct native cases**, including the same 30 C2a preflight cases within the 85.
These cover literal counts, duplicate/rejection and p95 rank policies, real both-format native
publication, multi-batch delivery, separate independent-reader processes, native replay bytes/mtimes,
completion/output schema corruption, missing/changed dependencies and actual gateway reaping.
The refreshed-inventory schema failure and wrong-result-fingerprint PostgreSQL cases cannot
produce a verified artifact. Existing HTTP/Python analytics compatibility passed **20 cases**.

An initial combined legacy Spark invocation was invalid: it selected the host Python 3.9 worker
for a 3.12 driver and left an incompatible Spark context after setup failure (16 tests reported,
14 subtest failures/11 errors). A first isolated harness also resolved the venv executable symlink
outside its environment (three loader errors). Neither attempt is counted as passing. Preserving
the venv executable path, explicitly pinning PYSPARK_PYTHON/PYSPARK_DRIVER_PYTHON and running
legacy suites separately produced the clean E2/E3 passes above. The new managed wrapper already
sets its approved worker executable and was unaffected; no native contract was changed to fix tests.

Exact underlying commands, with private existing DDIP_TEST_DB_* variables and absolute
DDIP_TEST_PYTHON supplied by the local test harness, were:

~~~sh
# Gate A (eight native cases)
JAVA_HOME=$(/usr/libexec/java_home -v 21) PYTHONPATH=processing:processing/tests \
outputs/silver-python312/.venv/bin/python -B -m unittest \
  processing/tests/test_reliability_execution.py -v

# Focused Gate B, expanded to 26 cases in the full regression
mvn -q -f services/content-api/pom.xml verify -Ppostgres \
  -Dit.test=ReliabilityExecutionIT -Dtest=VerifierProcessTest

# Full regression; an external dedicated-container restart harness handles the barriers
mvn -q -f services/content-api/pom.xml -Ppostgres \
  -Dddip.test.worker-restart-signal=/private/tmp/ddip-c1-worker-restart \
  -Dddip.test.history-restart-signal=/private/tmp/ddip-c1-history-restart \
  -Dddip.test.reliability-restart-signal=/private/tmp/ddip-c2b1-result-restart verify

# Final shared-supervisor hardening: all 275 Java and 37 PostgreSQL cases
mvn -q -f services/content-api/pom.xml verify -Ppostgres \
  '-Dit.test=ReliabilityPreflightIT,ReliabilityExecutionIT#retainedEvidenceChangedBeforePublicationFailsAndReleasesKnownOwner'

# Spark-free regression: 85, including C2a's 30
JAVA_HOME=$(/usr/libexec/java_home -v 21) PYTHONPATH=processing:processing/tests \
outputs/silver-python312/.venv/bin/python -B -m unittest \
  processing/tests/test_reliability_execution_preflight.py \
  processing/tests/test_reliability_format_inputs.py \
  processing/tests/test_reliability_format_admission.py \
  processing/tests/test_reliability_format_verification.py \
  processing/tests/test_reliability_inputs.py \
  processing/tests/test_reliability_verification.py -v

# Separate processes: eight E2 processing, 17 E2 publication, eight E3 publication cases
export JAVA_HOME=$(/usr/libexec/java_home -v 21)
export PYTHONPATH=processing:processing/tests
export PYSPARK_PYTHON="$PWD/outputs/silver-python312/.venv/bin/python"
export PYSPARK_DRIVER_PYTHON="$PYSPARK_PYTHON"
export SPARK_LOCAL_IP=127.0.0.1
"$PYSPARK_PYTHON" -B -m unittest processing/tests/test_reliability_processing.py -v
"$PYSPARK_PYTHON" -B -m unittest processing/tests/test_reliability_publication.py -v
"$PYSPARK_PYTHON" -B -m unittest processing/tests/test_reliability_format_publication.py -v

# Existing analytics compatibility: 20 cases; already-tested Java tests omitted here
JAVA_HOME=$(/usr/libexec/java_home -v 21) \
mvn -q -f services/content-api/pom.xml verify -Pintegration \
  -Dtest=NoUnitTests -Dsurefire.failIfNoSpecifiedTests=false \
  -Danalytics.python="$PWD/outputs/silver-python312/.venv/bin/python"
npm run check
git diff --check
git diff --cached --check
~~~

The processing CI job keeps its existing jobs/dependencies and now allows 25 minutes instead of
15 for the added bounded native/transaction cases. This is a timeout allowance, not a performance
claim or observed hosted success. Local logs are ignored outputs. No E4 experiment, 100K/1M
workload, live feed, retained personal dataset, cloud credential or AWS operation was used.

A successful `Completed` result returns the exact committed released worker token. A definitive
safe failure returns its fixed classification and committed released token through `DefinitiveFailure`;
an ambiguous commit returns no token. These let C2b-2 continue only from confirmed lifecycle
transitions, without adopting a newer epoch/version. The current adapter consumes an already
verified C2a token; C2b-2 must still integrate C2a failure handling and passive-worker scheduling.
Replayed artifact reuse preserves the artifact's original `verified_at`; fresh stage timestamps/audits
record the later verification. Neither timestamp is permanent readability assurance.

Known limits remain cooperative host/filesystem trust, no universal orphan containment, no hard disk
quotas or deployment authenticity, and no power-loss/backup guarantee. Forced process-death and
late-control/reconciliation matrices remain C3 work; only existing control checks before launch are
operational here. No cancellation endpoint is enabled. Production activation is **not** inferred from
isolated adapter success; profile fairness, lifecycle/failure-token wiring and activation/shutdown
acceptance require separately authorized C2b-2 work.

Changed implementation files are `processing/reliability_execution.py`, the operational preflight
module allowlist, `ReliabilityExecution.java`, `WorkerRepository.java`, `ReliabilityRuntime.java`,
`ReliabilityPreflight.java`, `PreflightFiles.java` and `ReadOnlyVerifierProcess.java`. Test changes
are `test_reliability_execution.py`, `reliability_preflight_fixtures.py`, `ReliabilityExecutionIT.java`,
shared `ReliabilityExecutionFixtures.java`, the existing preflight/supervisor/worker tests and the
test-only `PreflightCatalogFixture` bridge. The content-api pom, this contract, service guide and
existing CI timeout are updated. Authoritative native publishers/readers, accepted ADRs and V1–V7
remain byte-identical to the committed baseline. No Git staging/commit/push/branch operation was
run. Staged additions appeared during work outside these commands; their entries were left intact.

The final affected lifecycle rerun passed all **26 PostgreSQL cases** after the confirmed-release
interface change, using this command (no unrelated Java cases were repeated):

~~~sh
mvn -q -f services/content-api/pom.xml verify -Ppostgres \
  -Dit.test=ReliabilityExecutionIT -Dtest=NoUnitTests -Dsurefire.failIfNoSpecifiedTests=false
~~~

The final standalone Gate A also passed all **eight cases**, including publication directory and
file bytes/mtimes on replay. The production JAR was inspected: test-only capabilities, process
fixtures and fixture bridges are absent. NONE/passive-worker code, accepted ADRs and V1–V7
were compared with HEAD. Markdown lint (44 documents), local links, repository hygiene and
both working-tree/staged whitespace checks passed. The initial dedicated database state was
stopped; the container was restored to that state after local validation.

Exact changed-file inventory:

- `.github/workflows/ci.yml`
- `docs/architecture/durable-execution.md`
- `processing/reliability_execution.py`
- `processing/reliability_execution_preflight.py`
- `processing/tests/reliability_preflight_fixtures.py`
- `processing/tests/test_reliability_execution.py`
- `services/content-api/README.md`
- `services/content-api/pom.xml`
- `services/content-api/src/main/java/platform/contentapi/execution/PreflightFiles.java`
- `services/content-api/src/main/java/platform/contentapi/execution/ReadOnlyVerifierProcess.java`
- `services/content-api/src/main/java/platform/contentapi/execution/ReliabilityExecution.java`
- `services/content-api/src/main/java/platform/contentapi/execution/ReliabilityPreflight.java`
- `services/content-api/src/main/java/platform/contentapi/execution/ReliabilityRuntime.java`
- `services/content-api/src/main/java/platform/contentapi/execution/WorkerRepository.java`
- `services/content-api/src/test/java/platform/contentapi/execution/ReliabilityExecutionFixtures.java`
- `services/content-api/src/test/java/platform/contentapi/execution/ReliabilityExecutionIT.java`
- `services/content-api/src/test/java/platform/contentapi/execution/ReliabilityPreflightIT.java`
- `services/content-api/src/test/java/platform/contentapi/execution/VerifierProcessTest.java`
- `services/content-api/src/test/java/platform/contentapi/management/PreflightCatalogFixture.java`
- `services/content-api/src/test/java/platform/contentapi/management/WorkerCoordinationIT.java`

## C2b-2 production Reliability worker

The packaged non-web worker now uses the fixed RELIABILITY_V2 capability and the existing real
preflight/publisher/independent-verifier adapters. No fake capability is configurable or packaged.
This supersedes the passive-worker implementation status in the historical C1/C2a/C2b-1 sections;
it does not change ADR 0010/0011, the seven run states, native product contracts or V1–V7.
There is no new migration, persistence model, public endpoint or analytics activation.

### Pinned readiness and eligible ordering

Each host startup pins one RunProfile ID/SHA, execution-runtime ID/SHA and deployment digest.
JSONL request-jsonl-v2 and Parquet request-parquet-v1 use separately reviewed startups, with the
same installation CONTROL guard. Only RELIABILITY_V2 QUEUED rows with that exact run-profile ID
are considered, in increasing immutable submission sequence. A Content run or another approved
profile stays QUEUED. The operator stops the worker and starts another reviewed profile to service
that queue. Unsupported queued work still consumes the global twenty-unfinished-run capacity.
There is no cross-profile fairness or simultaneous format worker.

Startup validates schema without migration, the installation guard, actual JDK 21, immutable
profile/storage associations, disjoint root identities/permissions and the fixed deployment manifest.
The legacy worker python/java-home/processing-root properties must equal the execution-runtime
fields. A fixed `--runtime-check` mode of reliability_execution_preflight.py then checks actual
Python 3.12.15/PySpark 4.0.2/DuckDB 1.4.5 and module/deployment hashes. This is a bounded 30-second
Spark-free probe with a private canonical request/result digest under CONTROL; it has no run,
attempt, native selection or publication authority. Only its own scratch files are removed.
No dependency installation, arbitrary module or shell command is supported.

Only after readiness does registration inspect the coordinator. STOPPING/RECOVERY_ONLY, an
occupied import or unresolved execution register a non-dispatching historical worker and close
it without clearing the old pointer. A free guard or old heartbeat never authorizes takeover.
Invalid startup fails closed before claims. Runtime/root/free-space checks continue at stage
boundaries and heartbeats; each writable volume requires at least 1 GiB free, not a hard quota.
Changed deployment files require a new reviewed profile and submission, never rewritten captures.

`ddip.worker.runtime-profile-id` / DDIP_WORKER_RUNTIME_PROFILE_ID selects the immutable execution
runtime descriptor already defined by C2a. Polling is `ddip.worker.poll-millis` /
DDIP_WORKER_POLL_MILLIS, default 1000 ms and accepted range 250–5000 ms. This is cooperative
bounded polling of PostgreSQL, not an execution deadline or measured scheduling guarantee.

### One owner for heartbeat, claim and completion tokens

One dispatcher thread owns the live session token. Its short idle heartbeat (five-second policy)
and coordinator-first claim transactions are serialized. Sources lock in deterministic order;
claim gates still validate enablement and exact frozen source/profile/input membership. Invalid
eligible selections and SOURCE_DISABLED fail observably without a processing attempt. Other
pipelines/profiles are excluded by eligibility, not misclassified as invalid work.

After claim, only the native supervisor path updates attempt/coordinator/worker versions. There
is no concurrent idle-heartbeat scheduler. The loop installs a session only from a confirmed claim
with no attempt, `Completed.worker()` or a typed definitive-failure result. Stage heartbeats pass
their updated owner into later stages and finalization. Inspection snapshots and wake requests
cannot mutate tokens. No callback refreshes a stale token from the database. A lost acknowledgment
or failed condition stops the loop; it never retries with newer authority. No Java monitor or SQL
transaction spans native work, and shutdown can signal that work concurrently.

The fixed sequence remains CHECK_INPUTS → PUBLISH_RELIABILITY → VERIFY_RESULT. All existing
stage intents, single-use launch authorizations, process identities, retained-evidence rechecks,
30/15-minute stage limits, two-hour attempt limit and independent 16 MiB stream caps remain.
Only independently verified native output can atomically link the result/stage/dependencies,
finish attempt/run SUCCEEDED and release this owner. A distinct identical-selection run repeats
verification and uses native read-only replay, reusing the operational artifact only with identical
evidence/links. No publication is activated for analytics.

### Definitive closure, uncertainty and shutdown

Production uses `ReliabilityPreflight.verifyForExecution`. A typed pre-INTENT qualification failure
proves no child was authorized. A known clean verifier failure retains its fixed private anchor;
one fenced transaction seals CHECK_INPUTS FAILED, finishes attempt/run FAILED (retry ineligible),
appends audits and releases this owner. It returns only the confirmed released session. The older
internal C2a `verify` seam retains its stage-only behavior for isolated checkpoint tests. Neither
path converts an arbitrary RuntimeException, malformed result, database error or uncertain cleanup
into proof of quiescence. A rollback leaves no terminal partial closure; a lost commit response
returns no authority even when historical rows prove that finalization committed.

C2b-1 known quiescent publication failures use the same confirmed-token continuation rule.
Incomplete namespaces remain untouched and retry ineligible. The dispatcher can service another
eligible run after a confirmed safe failure or success; it never retries the failed run automatically.
Unknown launch, child cleanup, stale ownership or DB outcome stops polling and never clears an
occupied coordinator. If a terminal transaction actually committed before its response was lost,
its atomic release remains committed, but the old worker still stops without inventing a session.
Established adapter paths persist RECOVERY_REQUIRED/UNKNOWN where possible;
an unavailable database may leave RUNNING/CLAIMED evidence occupied instead. Neither is idle.

Idle stop signals the loop, records worker STOPPED using its current token and closes the guard.
Active stop sets a persistent stop signal and interrupts the owner thread. The supervisor checks
both before authorization/launch and while supervising; a cleared interrupt cannot enable a later
stage. Existing identity-aware graceful/forced/final cleanup runs before the dispatch thread closes
the guard. Shutdown waits up to 45 seconds outside the lifecycle monitor. If the thread has not
finished, it retains its guard and reports uncertainty; no shutdown callback fabricates release.
An independently verified final commit may win the shutdown race. A stop observed before ProcessBuilder.start can close as PROCESS_START_FAILED with no child.
Interrupted work with an observed child is conservatively recovery-required, not automatically
CANCELLED or successful.

A crashed JVM may free the OS guard while its durable occupied slot remains. Replacement startup
refuses that ownership even if the old parent is gone. Local tests observe parent/PID and bounded
observed-descendant cleanup; they do not establish universal OS process-tree containment. C3 must
supply explicit execution-owner quiescence/reconciliation. There is no automatic orphan cleanup,
recovery, retry, cancellation API or claim of exactly-once native execution.

### C2b-2 verification and remaining boundary

The small production-style fixtures use the real fenced operator importer, HTTP source creation
and POST submission, packaged worker lifecycle, Spark publisher and a distinct native independent
verifier. Existing GET detail/attempts/events expose persisted lifecycle, current stage, stage count,
fixed stage events and VERIFIED_AT_EXECUTION result linkage; detailed private stage/process evidence
is not added to public DTOs. Historical verification still does not establish current readability.

Local validation on October 21, 2026 used Java 21.0.7, Python 3.12.15, PySpark 4.0.2,
DuckDB 1.4.5 and the isolated PostgreSQL 17.11 test container. Final distinct evidence:

| Suite | Passed | Scope |
| --- | ---: | --- |
| Java unit/component | 278 | Includes fixed capability and persistent shutdown-signal checks |
| PostgreSQL integration, union of runs below | 401 | B1/B2/C1 plus 41 preflight, 27 publication and 20 production-worker cases; includes all 23 Spark-backed Silver imports |
| Spark-free native | 87 | Existing 85 plus two actual startup-probe cases; includes 32 preflight cases |
| Native C2b publication/independent reader | 8 | Both formats, joint selection, replay, corruption and cleanup |
| Native E2/E3 | 33 | Eight processing, 17 publication and eight format-publication cases, in separate processes |
| Analytics HTTP/Python | 20 | Existing loopback analytical contracts, independent of management/worker |
| Documentation | 44 files | Markdown, relative links and repository hygiene |

These are **128 distinct Python cases**, not the sum of repeated overlapping native runs. The full
PostgreSQL invocation passed 394 cases with four externally observed real container restarts
(worker claim, history, production JSONL result and C2b verified-result barriers). The expanded
production-worker class then passed 20 cases. One of the initial 41 preflight cases exposed an
interrupt flag preventing private failure journaling before launch. The fix temporarily clears the
flag only for bounded known-quiescent closure and restores it; all 41 preflight cases and the targeted
verifier/publisher pre-intent/startup/stop cases passed afterward. The union is 394 plus five new
worker cases, one preflight stop case and one publication stop case = 401; repeated cases are
counted once. No outstanding failed or skipped case remains in this recorded scope.

The production-style cases use the real fenced importer, HTTP source creation/submission and
fixed production lifecycle. Both formats achieved SUCCEEDED with three VERIFIED stages and ten
physical occurrences per original single-batch fixture. New distinct identical-selection runs
reused the same operational result artifact with unchanged output bytes/mtimes; retained batch
and reference bytes/mtimes also stayed unchanged. API restart was exercised for both formats;
the JSONL production result also survived an actual PostgreSQL restart, with unchanged run,
sequence, selection and artifact linkage. Lost final acknowledgment left a second run QUEUED
without relaunch. Known corrupted-input failure continued with an unrelated retained batch;
incomplete output stayed retry ineligible. Failed independent verification, active DB outage and
active stop prevented further claims and retained occupied ownership. An actual worker JVM was
killed while stage INTENT was blocked before commit/child launch; replacement startup refused its
occupied claim. Two production-lifecycle JVMs with different reviewed profiles were excluded by
the same guard. These are observed local process facts, not proof of universal orphan containment.

Exact underlying commands (private DDIP_TEST_DB_* environment supplied by the local harness):

~~~sh
export JAVA_HOME=$(/usr/libexec/java_home -v 21)
export DDIP_TEST_PYTHON="$PWD/outputs/silver-python312/.venv/bin/python"
export PYSPARK_PYTHON="$DDIP_TEST_PYTHON"
export PYSPARK_DRIVER_PYTHON="$DDIP_TEST_PYTHON"
export SPARK_LOCAL_IP=127.0.0.1
export PYTHONPATH=processing:processing/tests

# Full regression: 278 Java and 394 PostgreSQL cases at this point.
# A separate test-container harness restarts PostgreSQL at each READY barrier and writes .done.
mvn -q -f services/content-api/pom.xml -Ppostgres \
  -Dddip.test.worker-restart-signal=/private/tmp/ddip-c2b2-worker-restart \
  -Dddip.test.history-restart-signal=/private/tmp/ddip-c2b2-history-restart \
  -Dddip.test.reliability-restart-signal=/private/tmp/ddip-c2b2-reliability-restart \
  -Dddip.test.production-restart-signal=/private/tmp/ddip-c2b2-production-restart verify

# Expanded gate: production worker 20 passed; the one new preflight stop failure was fixed below.
mvn -q -f services/content-api/pom.xml -Ppostgres \
  -Dit.test=ReliabilityWorkerIT,ReliabilityPreflightIT verify
# Final affected preflight suite: all 41 passed.
mvn -q -f services/content-api/pom.xml -Ppostgres -Dit.test=ReliabilityPreflightIT \
  -Dtest=NoTests -Dsurefire.failIfNoSpecifiedTests=false verify
# All four targeted final pre-intent/shutdown/startup cases passed; 278 Java tests also ran.
mvn -q -f services/content-api/pom.xml -Ppostgres \
  '-Dit.test=ReliabilityPreflightIT#confirmedShutdownBeforeLaunchClosesWithoutInventingAChild+preIntentRuntimeFailureCanCloseOnlyOriginalClaim,ReliabilityExecutionIT#confirmedStopBeforePublisherLaunchClosesAndReturnsReleasedToken+startupFailureIsKnownFailedAndReleased' verify

"$DDIP_TEST_PYTHON" -B -m unittest \
  processing/tests/test_reliability_execution_preflight.py \
  processing/tests/test_reliability_format_inputs.py \
  processing/tests/test_reliability_format_admission.py \
  processing/tests/test_reliability_format_verification.py \
  processing/tests/test_reliability_inputs.py \
  processing/tests/test_reliability_verification.py -v
# Separate interpreter invocations preserve isolated Spark configuration.
"$DDIP_TEST_PYTHON" -B -m unittest processing/tests/test_reliability_execution.py -v
"$DDIP_TEST_PYTHON" -B -m unittest processing/tests/test_reliability_processing.py -v
"$DDIP_TEST_PYTHON" -B -m unittest processing/tests/test_reliability_publication.py -v
"$DDIP_TEST_PYTHON" -B -m unittest processing/tests/test_reliability_format_publication.py -v
mvn -q -f services/content-api/pom.xml -Pintegration \
  -Dtest=NoTests -Dsurefire.failIfNoSpecifiedTests=false \
  -Danalytics.python="$DDIP_TEST_PYTHON" verify
npm run check
git diff --check
git diff --cached --check
~~~

Restart flags are test barriers, not worker configuration. Use them only with an external controller
for a disposable database; without that controller omit them. Local execution used ignored logs
under outputs/e5-c2b2-* and private temporary logs. The original stopped state of the dedicated
container is restored after testing; volumes are retained. The production JAR was inspected and
contains the fixed capability/lifecycle but no WorkerTestSupport, ProductionWorkerChild, fixture
classes or test executors. No migration, accepted ADR, native publication module or CI job changed.
Hosted CI remains unverified until owner publication. No live feed, retained personal data,
100K/1M benchmark or AWS operation was used.

**Implemented:** fixed profile-pinned Reliability dispatch and safe definitive continuation.
**Planned:** owner publication and hosted CI observation.
**Deferred:** C3 execution-owner recovery/fault matrix, Content/RSS execution, dynamic profile
switching/cross-profile automation, new controls, Operations UI, activation, authorization and full
E5 completion. STOP after C2b-2.

### C2b-2 changed files

Production Java (existing execution package):
[DatabaseConfiguration.java](../../services/content-api/src/main/java/platform/contentapi/execution/DatabaseConfiguration.java),
[ReadOnlyVerifierProcess.java](../../services/content-api/src/main/java/platform/contentapi/execution/ReadOnlyVerifierProcess.java),
[ReliabilityExecution.java](../../services/content-api/src/main/java/platform/contentapi/execution/ReliabilityExecution.java),
[ReliabilityPreflight.java](../../services/content-api/src/main/java/platform/contentapi/execution/ReliabilityPreflight.java),
[ReliabilityRuntime.java](../../services/content-api/src/main/java/platform/contentapi/execution/ReliabilityRuntime.java),
[WorkerExecutionCapability.java](../../services/content-api/src/main/java/platform/contentapi/execution/WorkerExecutionCapability.java),
[WorkerIdleLifecycle.java](../../services/content-api/src/main/java/platform/contentapi/execution/WorkerIdleLifecycle.java),
[WorkerRepository.java](../../services/content-api/src/main/java/platform/contentapi/execution/WorkerRepository.java).

Operational Python, configuration and test inclusion:
[reliability_execution_preflight.py](../../processing/reliability_execution_preflight.py),
[application.properties](../../services/content-api/src/main/resources/application.properties),
[pom.xml](../../services/content-api/pom.xml).

Tests and isolated harnesses:
[test_reliability_execution_preflight.py](../../processing/tests/test_reliability_execution_preflight.py),
[ReliabilityExecutionFixtures.java](../../services/content-api/src/test/java/platform/contentapi/execution/ReliabilityExecutionFixtures.java),
[ReliabilityExecutionIT.java](../../services/content-api/src/test/java/platform/contentapi/execution/ReliabilityExecutionIT.java),
[ReliabilityPreflightIT.java](../../services/content-api/src/test/java/platform/contentapi/execution/ReliabilityPreflightIT.java),
[VerifierProcessTest.java](../../services/content-api/src/test/java/platform/contentapi/execution/VerifierProcessTest.java),
[WorkerTestChild.java](../../services/content-api/src/test/java/platform/contentapi/execution/WorkerTestChild.java),
[WorkerCoordinationIT.java](../../services/content-api/src/test/java/platform/contentapi/management/WorkerCoordinationIT.java),
[ProductionWorkerChild.java](../../services/content-api/src/test/java/platform/contentapi/execution/ProductionWorkerChild.java),
[ReliabilityWorkerIT.java](../../services/content-api/src/test/java/platform/contentapi/execution/ReliabilityWorkerIT.java),
[WorkerCapabilityTest.java](../../services/content-api/src/test/java/platform/contentapi/execution/WorkerCapabilityTest.java).

Documentation: this implementation contract and the
[Content API/operator guide](../../services/content-api/README.md). Total: 23 files; no migration,
accepted ADR, native publication contract or CI configuration was changed. Git index/branch are
preserved; publication remains owner-managed.

## C3a reconciliation control admission

**Implemented:** durable RECONCILE intent admission and safe control inspection only, on the
owner-prepared E5 branch at baseline `9176b91ee0c869777108ea24dc44f5919f4569f2`.
C3a creates REQUESTED control/audit records. It does not acquire the installation guard, transfer
ownership, create a reconciliation attempt, mutate run/attempt/stage/worker/coordinator records,
verify publications, launch children or apply/reject controls. The production Reliability worker
and native contracts are unchanged. A free guard, absent/reused PID, stale heartbeat or operator
boolean does not authorize recovery. C3b requires separate owner authorization.

### HTTP command and inspection

`POST /api/management/v1/runs/{runId}/controls` accepts exactly:

~~~json
{"action":"RECONCILE","attemptId":"70000000-0000-4000-8000-000000000001","quiescenceConfirmed":true}
~~~

Run/attempt IDs must be canonical lower-case UUIDs. Require Idempotency-Key (1–128 ASCII token
characters), a single strong If-Match containing the expected decimal run version (for example
`"5"`), application/json UTF-8, the approved Origin and X-DDIP-Local-Request: 1. Existing loopback
Host/Origin policy, strict duplicate/type/field/UTF-8 decoding and 64 KiB body bounds apply. Unknown
query parameters and duplicate headers are rejected. CANCEL, RETRY, PAUSE and RESUME return
INVALID_ARGUMENT with no records. Null/false confirmation and free-text reasons are rejected.
No mutation header is required for GET.

On committed admission return 202, a relative Location identifying
`/api/management/v1/runs/{runId}/controls/{controlId}`, control ETag `"0"` and the established
`{apiVersion:1,data,meta:{correlationId}}` envelope. Data fields are controlId, runId, type, state,
reason, requestedAt, appliedAt, rowVersion and runVersion. Versions are decimal strings; timestamps
are UTC with six fractional digits. New state is REQUESTED with null reason/appliedAt. runVersion
is current inspection context, not a replacement for the command's stored expected version.
`GET` at that Location returns the same safe projection and the control's current row ETag;
unknown or wrong-run identities return NOT_FOUND. Neither DTO exposes the target process,
confirmation, canonical request, idempotency key, private artifact evidence or paths. Existing run
detail latestControl and run-linked event history expose the committed request. GET has no
coordinator-lock or native-verification dependency. Existing 1 MiB response cap applies.

Errors remain fixed management envelopes: 400 INVALID_ARGUMENT, 403 LOCAL_BOUNDARY_REJECTED,
404 NOT_FOUND, 409 IDEMPOTENCY_CONFLICT/STATE_CONFLICT, 412 PRECONDITION_FAILED,
428 PRECONDITION_REQUIRED, 429 MANAGEMENT_CAPACITY_EXCEEDED and 503 MANAGEMENT_UNAVAILABLE.
Positive live-writer evidence and an existing pending request are STATE_CONFLICT. No raw SQL,
process diagnostics or private runtime evidence appears in responses.

### Canonical identity, lock order and admission

Scope is `(run_id,idempotency_key)` under fixed local-operator requester semantics, not authenticated
identity. Exact normalized UTF-8 bytes and SHA-256 are stored in controls; JSONB is not the identity
authority. Object keys use the existing SourceJson canonical encoder. Expected If-Match version
is part of normalized content as a decimal string, consistent with source commands. The body has
no optional/defaulted fields. This literal vector was independently computed with Python hashlib:

~~~text
{"action":"RECONCILE","attemptId":"70000000-0000-4000-8000-000000000001","expectedVersion":"5","quiescenceConfirmed":true,"runId":"60000000-0000-4000-8000-000000000001"}
SHA-256: 46b24ff4d50645dc0961d129b3e61dbaa0b8a7db911e6e648664713c3bf0f528
~~~

Equivalent JSON field order/whitespace has the same identity. Changing the run, attempt or expected
version changes command identity. Keep the original If-Match and body when resolving a lost reply;
a refreshed If-Match under the same key is different content. Identical replay resolves before
current ETag, current process/ownership eligibility, pending-control or capacity checks, returning
the original control ID/state with current run-version context. It never performs handoff.

ControlRepository uses two short transactions, with bounded process observation between them:

1. Lock coordinator first and validate installation. Resolve scoped replay by exact bytes/hash.
   For new work, lock run then exact attempt; validate ETag, occupied binding, worker/installation,
   execution-profile and CONTROL binding membership. Read only bounded process/stage projections.
2. Outside SQL transactions inspect recorded worker PID/start and recorded stage child PID/start.
   A still-live matching start, or an observable live PID with unavailable start, vetoes admission.
   Denied/unsupported observation also fails closed. An absent or reused PID is not positive proof
   of quiescence. No process is killed or launched; native files are not inspected.
3. Re-lock coordinator, resolve replay again, lock run/attempt and recheck the exact original
   coordinator/run/attempt/worker/stage projection, including versions and process identities.
   Any change rejects rather than adopting a new token. Reject another REQUESTED RECONCILE targeting
   this attempt. Enforce 128 controls/run and existing audit reserve. Atomically insert immutable
   REQUESTED control plus CONTROL_REQUESTED/ACCEPTED audit linked to run/attempt/control. Commit.

An ambient caller transaction is rejected so external observation cannot accidentally occur inside
one. The coordinator serializes competing admissions; different keys cannot insert two pending
requests through this protocol, even when their observations overlap. Existing unique scoped key,
same-run target FK, immutable request/evidence guards and append-only audit protect the result.
There is no migration or new table/index. Privileged manual SQL is not a supported admission path.
A rollback leaves no control/audit pair. Lost final acknowledgment is resolved by exact-key replay
or GET of the known control; an unavailable/inconclusive database never permits recovery effects.

Eligible run states are RUNNING, CANCEL_REQUESTED or RECOVERY_REQUIRED with the exact occupied
coordinator/current attempt. EXECUTE attempts may be CLAIMED/RUNNING; RECOVERY_REQUIRED may retain
INTERRUPTED or a terminal FAILED RECONCILE attempt, as specified by the accepted handoff contract.
An active RECONCILE attempt must belong to RECOVERY_REQUIRED. Other terminal/released/inconsistent
bindings fail closed. Admission does not require a stale heartbeat or change coordinator mode/epoch.
Original immutable attempt/worker links and retained stage process evidence remain for C3b.

### Quiescence and next checkpoint

quiescenceConfirmed=true is a trusted operator assertion only. C3a's observable checks are a
negative veto over recorded local process identities, not universal descendant containment or an
attestation verifier. Missing parents can leave unrecorded Python/JVM/Py4J descendants. PID reuse,
released FileChannel locks, STOPPED status and expired heartbeats do not prove those writers stopped.
The operator checklist is in the [service guide](../../services/content-api/README.md#c3a-reconciliation-intent-only).
REQUESTED blocks neither by a new lock nor by a fabricated attempt: the original occupied slot
continues to block ordinary dispatch/import ownership as before. C3a has no way to apply or clear it.

**Planned, not implemented:** C3b must independently establish fresh quiescence for all observable
writers and possible orphans/unmanaged writers; hold the identity-checked installation guard;
validate installation/profile, original process/evidence references, REQUESTED control/target and
captured expected run version; then perform the accepted atomic occupied-to-occupied, epoch-fenced
handoff with attempt ID equal to control ID. Revalidate controls/history bounds and resolve ambiguous
handoff commits by exact identities. No intermediate idle slot is permitted. Native read-only
reconciliation, final control disposition and safe release require their own implemented validation.
**Deferred:** all handoff/recovery execution, automatic takeover/retry, CANCEL/RETRY, process killing,
artifact repair/cleanup, Content processing, UI, AWS and E5 completion.

### C3a local validation

**Tested locally:** 308 distinct Java unit/component cases, **453 distinct PostgreSQL cases** and
20 analytics HTTP/Python compatibility cases passed across the full and final affected runs.
No skipped cases or remaining failures/errors. Do not add repeated focused cases to these totals.
The initial full run was 308 Java passing and 450/451 PostgreSQL passing: one pre-C3a submission
test still expected the controls route to be absent (404). Its body is now correctly rejected as
INVALID_ARGUMENT (400). The assertion was updated while preserving no-control/no-attempt checks.
The final affected run passed all 308 Java cases and **96 PostgreSQL cases** (52 ControlAdmissionIT
plus 44 RunSubmissionIT), including that corrected case and two additional C3a cases. The final
453-case PostgreSQL total is the current suite union, not a claim of a second full-suite execution.

**Gate A:** 30 ControlCommandsTest cases cover the literal canonical vector, equivalent order,
expected-version sensitivity, exact UUIDs, Long.MAX_VALUE, strict fields/types/duplicate keys,
UTF-8/body limits, unsupported controls and strong If-Match/idempotency validation. Seventeen
ControlAdmissionIT command/HTTP cases test real loopback POST/GET plus servlet/filter validation,
202/Location/control ETag/envelope, null fields, safe timestamps/versions, Host/Origin/mutation
protections, required/duplicate headers, lookup and existing detail/event integration. The successful
real HTTP response contains no private process/attestation/evidence fields.

**Gate B:** 35 additional ControlAdmissionIT PostgreSQL cases exercise all accepted occupied
run/attempt combinations; same-key replay after a version change and newly live recorded child;
changed command conflict; matching live worker and child vetoes with fresh/stale heartbeats;
reused PID semantics; wrong run/attempt/installation, released/terminal bindings; control/audit
insertion rollback; observation outside SQL; mutation of coordinator/run/attempt/worker/stage
evidence before finalization; deterministic concurrent same-key/different-key admission; 128-control
capacity and replay; immutable control identity; actual packaged-capability refusal of an occupied
REQUESTED slot; outages before admission, GET and final persistence; and lost final commit acknowledgment.
The latter injects a connection failure after the real PostgreSQL commit, then resolves the original
control by exact-key replay without duplicate control/audit or ownership mutation. Admission/reads
leave original run/attempt/coordinator/worker/stage and artifact metadata unchanged. Fixtures do not
fabricate successful publications or RECONCILE attempts through application code.

Five actual dedicated-container PostgreSQL restarts were observed at the worker, history, control,
Reliability-result and production-result barriers. The C3a control retained its identity, exact
REQUESTED summary, original occupied binding/epoch and replay identity across API/database restart.
The full regression included B1/source/import/submission/history/worker suites, **23 Spark-backed
Silver import cases**, 41 Reliability preflight, 27 execution and 20 production-worker cases, all
passing. No standalone native Python suite was rerun for this control-only change. Existing native
processing occurred only inside the bounded synthetic regression fixtures.

Exact underlying commands (private DDIP_TEST_DB_* supplied by local harness; Java 21 and pinned
outputs/silver-python312/.venv Python; process/scratch configuration from reviewed test profiles):

~~~sh
export JAVA_HOME=$(/usr/libexec/java_home -v 21)
export DDIP_TEST_PYTHON="$PWD/outputs/silver-python312/.venv/bin/python"

# Final focused gates: 30 Java + 50 PostgreSQL at this point, all passed.
mvn -q -f services/content-api/pom.xml -Ppostgres \
  -Dtest=ControlCommandsTest -Dit.test=ControlAdmissionIT verify

# Full regression: 308 Java; 450/451 PostgreSQL, obsolete route assertion corrected below.
# External controller restarts only the disposable server at each READY barrier and writes .done.
mvn -q -f services/content-api/pom.xml -Ppostgres \
  -Dddip.test.worker-restart-signal=/private/tmp/ddip-c3a-worker-restart \
  -Dddip.test.history-restart-signal=/private/tmp/ddip-c3a-history-restart \
  -Dddip.test.reliability-restart-signal=/private/tmp/ddip-c3a-reliability-restart \
  -Dddip.test.production-restart-signal=/private/tmp/ddip-c3a-production-restart \
  -Dddip.test.control-restart-signal=/private/tmp/ddip-c3a-control-restart verify

# Final affected run, including two expanded cases: 308 Java + 96 PostgreSQL, all passed.
mvn -q -f services/content-api/pom.xml -Ppostgres \
  -Dit.test=ControlAdmissionIT,RunSubmissionIT verify

# Twenty actual HTTP/Python analytics cases passed; no Java cases repeated by this command.
mvn -q -f services/content-api/pom.xml -Pintegration \
  -Dtest=NoTests -Dsurefire.failIfNoSpecifiedTests=false \
  -Danalytics.python="$DDIP_TEST_PYTHON" verify
npm run check
git diff --check
git diff --cached --check
~~~

Markdown/local links for 44 files and repository hygiene passed. The production JAR contains no
ControlTestProcess, control test classes or WorkerTestSupport. V1–V7, accepted ADRs, native modules,
worker dispatch and supervision are unchanged; no CI configuration change. The dedicated test
container is restored to its original stopped state with volumes retained. Git index is unchanged;
no branch/stage/commit/push/GitHub mutation was performed. Hosted CI remains unverified until owner
publication. Local observations do not prove universal descendant quiescence or safe handoff.

Changed files: ControlCommands.java, ControlProcesses.java, ControlRepository.java and
ControlController.java; API wiring in ManagementConfiguration.java/SourceAdvice.java; test selection
in services/content-api/pom.xml; ControlCommandsTest.java, ControlAdmissionIT.java,
ControlTestProcess.java, WorkerTestSupport.java and the obsolete-route assertion in RunSubmissionIT.java;
this contract and services/content-api/README.md. No new dependency, table, migration or native contract.

## C3b fenced execution recovery handoff

**Implemented:** internal fresh quiescence qualification, atomic occupied-to-occupied execution
handoff, acknowledgment lookup and fenced recovery-only heartbeat. Baseline is clean owner commit
`cac10709353bed7b2a970c951870adba5d91ae16` on the prepared E5 branch. This implements the accepted
transactional handoff above, without changing ADR 0010/0011, V1–V7 or native contracts.
**No production recovery resolver or invocation is active.** A successful internal handoff is not
native publication recovery. No C3b method verifies an output, records a final run outcome, applies
a control or releases the slot.

### Internal authority and qualification

RecoveryHandoff, RecoveryQuiescence, RecoveryFiles and RecoveryFailure are package-private and are
not Spring components, runtime roles, HTTP routes, CLI commands or dispatcher options. There is no
operator property enabling handoff. Tests call the fixed seam directly in disposable databases.
Production WorkerIdleLifecycle and WorkerRepository dispatch behavior remain unchanged. Ordinary
workers still refuse occupied/RECOVERY_ONLY ownership; no new automatic takeover/retry exists.

The caller acquires the existing InstallationHostGuard with its approved CONTROL binding. Existing
private POSIX ownership/permissions, root/device/inode, guard-file/link and installation.id byte
checks apply. Never unlink, recreate or bypass the guard. WorkerRepository.register creates a
separate immutable process-instance identity under that guard and records RECOVERY_ONLY because
the original slot remains occupied. Registration changes no coordinator pointer, epoch or run.
There must be two distinct instance identities; a matching live original JVM vetoes qualification,
even if someone asserts that its old execution handlers stopped.

The internal sequence is inspect -> explicit Review -> qualify -> transfer. inspect locks the
coordinator, then original run/attempt/control and dependent evidence; it captures an exact target
SHA over the private metadata projection, including versions/process identities and frozen byte
digests. Review must bind that SHA, an inspection UTC instant, explicit assertions that old handlers and unmanaged writers stopped and orphan concerns were reviewed, the exact set of unresolved checkpoint UUIDs, and up to 32 reviewed
descendant PID/start identities. INTENT/RUNNING/UNKNOWN checkpoints all require explicit review;
a missing child PID is an uncertain launch gap, not evidence that no child started. Review is an
operator assertion, not automated universal orphan proof. There is no default-true production review.

Qualification requires:

- Exact REQUESTED/RECONCILE C3a control, original target and expected run version, local-operator
  assertion and canonical normalized request bytes/hash. Run/current attempt must match the occupied
  A/W/E binding. Modes READY/RECOVERY_ONLY are eligible; STOPPING/import ownership are not.
- Matching installation, immutable CONTROL binding and registered RECOVERY_ONLY process identity.
  Original and recovery profiles/digests must match the exact captured run profile/deployment.
  Reuse FrozenRunSelection validation against immutable source revisions, run inputs, registered
  qualification descriptors and storage/profile associations. Current source enablement is not a
  recovery re-selection gate. Captured configurations/inputs remain unchanged.
- The fixed existing ReliabilityRuntime loader/checker must accept the reviewed execution-only
  descriptor, Java 21, original deployment revision/module digests and approved disjoint root
  identities. Both frozen JSONL-v2 and Parquet-v1 profiles are supported. C3b supports the current
  Reliability verifier deployment only; Content/alternate-runtime compatibility is not invented.
  These checks read metadata/modules/roots. C3b deliberately does not call runtime.readiness or
  launch Python to probe installed packages. Fresh actual interpreter/package qualification and
  native consumption are C3c launch responsibilities; manifest version claims alone are not that proof.
- Every recorded old worker/child PID and microsecond start identity, plus explicitly reported
  descendants, is observed again. Matching live or unavailable/denied inspection vetoes. Reused
  recorded PIDs are distinguished by start identity, but are never affirmative proof of quiescence.
  Private gateway PID-only hints require an additional reviewed original start identity; any live
  PID remains ambiguous when native start evidence is absent, including apparent reuse. Missing
  original descendant identity fails closed. Neither historical cleanup assertions nor missing PIDs
  replace the manual orphan/unmanaged-writer inspection.
- Read only exact private operational sidecars for persisted stages under the held CONTROL root.
  Validate fixed command run/attempt/checkpoint/stage/epoch/config/runtime binding, recorded hashes,
  approved relative addresses and symlink/private-file policy. Capture explicit command/selection/
  retained/result/evidence/verified sidecar presence, length, SHA-256, device/inode and mtime, including
  incomplete private bytes where allowed. No native-root scan, artifact inventory rehash, Bronze GET
  or publication outcome interpretation occurs. A PID hint is inspected only as process metadata.

A canonical E5_EXECUTION_QUIESCENCE_V1 packet (at most 1 MiB) is retained with mode 0600 under private
CONTROL/recovery/{controlId}/{qualificationId}/qualification.json, with private parent directories.
It records the unique qualification ID, exact original metadata projection, control/old owner/epoch/
versions, recovery instance, host/profile/runtime/selection digests, explicit review and private
sidecar inventory. Raw source data/full native manifests are not copied into this packet or audit.
Canonical byte columns in the projection are represented by length/SHA-256, not their payloads.
Audit retains only fixed fields, opaque IDs/versions and this approved relative packet reference/hash.
Private qualification files are retained on failure as well; there is no automatic deletion/repair.

A fixed **60-second qualification freshness window** bounds the review and qualified receipt.
It is a conservative internal admission policy, not a lease or heartbeat death threshold. Expiry
requires fresh qualification and never frees ownership. Immediately before transfer, revalidate the
held guard, fresh review/process observations, runtime, packet digest and all captured private file
evidence. SQL never spans filesystem/process observation. An ambient caller transaction is rejected.
The final short SQL transaction compares the original snapshot and freshness again; changed evidence
rejects instead of adopting newer versions. Local filesystem checks remain cooperative, not hard
I/O deadlines, hostile-administrator protection or universal process containment.

### Atomic transfer and fences

One transaction takes the existing coordinator lock first, then R, A, K, worker instances in UUID
order and original stage rows in stage/item order. Targets are immutable; reading K's IDs after the
coordinator lock only resolves which R/A to lock. All SQL is parameterized and projections are
explicit. Bounds remain 20 attempts/run, 128 controls/run, 512 total stage invocations/run and the
existing million-event audit ceiling/reserved lifecycle capacity. No new table/index/migration.

For a new transfer, compare exact installation/A/W/E/coordinator version, current run/expected K
version, control type/state/assertion, worker/profile/host binding, frozen selection and original
process/stage/history evidence. The successor UUID must be unused and equals K.control_id; next
attempt_no is allocated under these locks. The transaction then:

1. Conditionally closes CLAIMED/RUNNING A as INTERRUPTED, recording clock_timestamp when interruption
   is established, outcome INTERRUPTED and row version +1. An already INTERRUPTED predecessor, or
   eligible failed RECONCILE under RECOVERY_REQUIRED, retains its exact terminal outcome/version.
   Other terminal combinations fail closed.
2. Changes only A's INTENT/RUNNING stages to UNKNOWN/RECOVERY_HANDOFF with version +1. VERIFIED,
   FAILED and already UNKNOWN histories, IDs, private bytes and native fingerprints are preserved.
3. Inserts K.control_id as one RECONCILE/CLAIMED attempt, instance N, epoch E+1, version 0 and next
   attempt number. Closing an active A first satisfies the existing single-active-attempt index.
4. Conditionally replaces coordinator A/W/E/version with K/N/E+1/version+1 and RECOVERY_ONLY. The
   pointer is never set null, even within the transaction. The deferred composite FK validates the
   new attempt/instance/epoch at commit.
5. Conditionally sets R to RECOVERY_REQUIRED/current_attempt=K/version+1, fixed recovery outcome and
   retry-ineligible. Frozen sources/inputs/submission identity and original current-stage context
   remain. A prior CANCEL_REQUESTED state is retained in the handoff audit; pending controls are
   untouched. There is no automatic retry, resume or next-stage permission.
6. Appends exact ATTEMPT_INTERRUPTED where applicable, STAGE_UNKNOWN for changed stages,
   ATTEMPT_CLAIMED and RECOVERY_HANDOFF records linked to R/A-or-K/K. The audit references the retained
   qualification packet. K remains REQUESTED with its original expected version and row version.

All related writes roll back on any zero-row conditional update, constraint or audit failure. There
is no committed idle interval. Confirmed commit returns a recovery Owner token bound to N/K/E+1 and
run/attempt/control/worker/coordinator versions. No native result or success is implied. The only
new owner mutation is a short recovery-only heartbeat that rechecks all those bindings plus the
immutable handoff audit anchor, updates worker/coordinator heartbeat versions and returns the exact
new token. There is no heartbeat scheduler here, and no control/stage/run transition or slot release.
Stale tokens are rejected without refreshing or adopting another owner. Existing READY-only worker
mutations cannot operate on this RECOVERY_ONLY binding.

This fixed helper deliberately consumes existing worker/guard/coordinator records instead of
loosening WorkerRepository's READY checks or generalizing the import/execution owners into another
locking framework. No production dispatcher or native supervisor refactor is needed.

### Rollback, acknowledgment and interruption

Each qualification can enter the transfer transaction at most once. A definitive rollback leaves
all original SQL records unchanged. After a failed/unknown call, never blindly call another transfer
with a refreshed token. resolve reads K and attempt ID K.control_id under the coordinator lock:

- CONFIRMED_HANDOFF requires the exact successor/run/instance/epoch/versions, preserved original
  attempt/stage/history, REQUESTED K and exact ordered interruption/stage/claim/handoff audit evidence.
- CONFIRMED_ROLLBACK requires no successor plus the unchanged original full metadata snapshot and
  REQUESTED control. Its receipt identifies the original A/W/E. Only fresh qualify of the same K
  can permit another attempt at that transaction.
- UNRESOLVED covers timeout/outage, changed/new owner, inconsistent records/audit or inconclusive
  evidence. Its receipt contains no claimed owner binding. Nothing is cleared or rejected as a
  purported safe resolution.

Receipts are historical facts and never contain an executable owner/session. A repeated successful
transfer returns only CONFIRMED_HANDOFF and no Owner; no duplicate attempt/epoch/audit is created.
Only the initial confirmed commit returns a token. Lost commit acknowledgment therefore produces
no token; resolve may establish committed facts after PostgreSQL returns, but does not silently
renew execution authority. A heartbeat/version change makes that old exact lookup inconclusive,
not permission to adopt new versions. The canonical private packet and audit retain recovery
context across process interruption; C3c must implement validated packet bootstrap and explicit live
owner authorization before any read-only child. This checkpoint's resolve accepts the internal
Qualified handle; it is not a restart/takeover CLI. A crashed successor still occupies the slot;
recovering that newer owner requires a new explicit C3a control targeting it and fresh qualification.

Control GET/latestControl/events expose REQUESTED, RECOVERY_REQUIRED and actual persisted attempts/
stage history without private packet, process identity, paths or attestation fields. Historical
successful stage evidence is not current artifact readability. A failed handoff says nothing about
whether native publication completed. No handoff outcome activates ANALYTICS_PUBLICATION.

### C3b validation and C3c prerequisites

**Tested locally in C3b:** **323 Java unit/component tests, 517 PostgreSQL integration cases and
20 actual analytics HTTP/Python compatibility cases passed**, with zero failures/errors/skips in
the final integrated runs. The 15 new unit cases and 64 new PostgreSQL cases are subsets of those
totals; repeated focused runs are not added. The historical C3a totals were not used as rerun evidence.

**Gate A:** all 15 RecoveryQuiescenceTest cases and 22 tagged RecoveryHandoffIT cases passed.
They cover exact review/target/checkpoint binding, freshness (including future/stale times), missing
handler/unmanaged/orphan confirmation, actual live/exited/reused JVM identities, live recorded worker,
JVM and controlled Python children, denied/unavailable inspection, changed process/installation/
private-file evidence, missing/changed deployment modules, approved Parquet profile, guard competition
with a separate JVM/importer, PID-only descendant ambiguity and no production activation surface.
The controlled processes are test fixtures, not native verification or process-killing functionality.
A recorded JVM exit is only observed local evidence, not universal descendant proof.

**Gate B:** all 42 other RecoveryHandoffIT cases passed with actual PostgreSQL transactions/constraints.
CLAIMED/RUNNING, already INTERRUPTED and failed RECONCILE predecessors produce successor UUID=control
UUID, next attempt number, E+1 and exact version changes. Unfinished stages become UNKNOWN; terminal
history, controls and frozen input/source bytes remain unchanged. Native input/output byte/hash/size/
mtime snapshots remain identical for both format profiles. Existing history APIs report actual new
attempt/audit state with null publication result, without private packet/process data. Current source
disablement does not substitute frozen selection.

Deterministic faults roll back after old-attempt closure, stage changes, successor insertion,
coordinator replacement, run update and first/partial audit insertion. A real audit constraint failure
also rolls back all state. A connection proxy throws only after the real PostgreSQL commit to simulate
lost acknowledgment: locked lookup confirms exactly one successor/audit/epoch with no duplicate writer
or token renewal. Same-control competing transactions have one winner; different qualification receipts
cannot adopt the winner. Stale run/control/coordinator/attempt/worker/process versions reject. Active
import replacement and STOPPING are never cleared. Attempt/control/stage history exhaustion is checked
against real small SQL fixtures. Audit-reserve exhaustion uses an explicitly injected count response
with real transaction/unchanged-record assertions plus the existing pure budget vectors; no million-
event workload is materialized.

A transaction barrier after the uncommitted coordinator replacement lets another connection observe
the still-occupied old binding, then the occupied new one after commit. A test-only trigger also vetoes
any intermediate null pointer. Deferred FK and active-attempt unique-index violations are asserted
against PostgreSQL. Stale A/W/E/version heartbeat/release and old stage authorization/process/verification/
run-completion predicates update zero rows; the actual old-owner stage callback/publication-persistence
entry is rejected before side effects. The normal packaged Reliability capability cannot claim through
the recovery binding. Fenced recovery heartbeats increment only worker/coordinator versions, generate
no audit stream and reject the stale token. Outages and changed records keep lookup UNRESOLVED.

The full regression reran B1, source management, import ownership/workflow, submission/history, C3a,
C1/C2 lifecycle/execution suites and all **23 Spark-backed Silver import cases**, with pinned Java 21
and matching Python 3.12 driver/worker paths. The PostgreSQL total includes 52 C3a, 56 C1 coordination,
41 preflight, 27 Reliability execution and 20 production-worker cases. Six actual dedicated-container
restarts were observed at worker, history, C3a control, Reliability-result, production-result and C3b
handoff barriers. The C3b case preserves the exact transferred coordinator/epoch, original and successor
attempts, REQUESTED control, audit and frozen evidence across restart, and confirms the same receipt.
Existing native publication occurs only in those existing bounded regression fixtures; no C3b operation
publishes or independently verifies native output. Standalone native Python suites were not rerun.

Exact underlying commands (private DDIP_TEST_DB_* supplied by the disposable-container harness):

~~~sh
export JAVA_HOME=$(/usr/libexec/java_home -v 21)
export DDIP_TEST_PYTHON="$PWD/outputs/silver-python312/.venv/bin/python"
export PYSPARK_PYTHON="$DDIP_TEST_PYTHON"
export PYSPARK_DRIVER_PYTHON="$DDIP_TEST_PYTHON"
export SPARK_LOCAL_IP=127.0.0.1

# Final focused gates: 15 Java + 64 PostgreSQL cases passed.
mvn -q -f services/content-api/pom.xml -Ppostgres \
  -Dtest=RecoveryQuiescenceTest -Dit.test=RecoveryHandoffIT verify

# Full regression: 323 Java + 517 PostgreSQL cases passed.
# External controller restarts only the disposable server at READY and writes each .done file.
mvn -q -f services/content-api/pom.xml -Ppostgres \
  -Dddip.test.worker-restart-signal=/private/tmp/ddip-c3b-worker-restart \
  -Dddip.test.history-restart-signal=/private/tmp/ddip-c3b-history-restart \
  -Dddip.test.reliability-restart-signal=/private/tmp/ddip-c3b-reliability-restart \
  -Dddip.test.production-restart-signal=/private/tmp/ddip-c3b-production-restart \
  -Dddip.test.control-restart-signal=/private/tmp/ddip-c3b-control-restart \
  -Dddip.test.recovery-restart-signal=/private/tmp/ddip-c3b-recovery-restart verify

# Twenty actual HTTP/Python compatibility cases passed; Java cases are not repeated here.
mvn -q -f services/content-api/pom.xml -Pintegration \
  -Dtest=NoTests -Dsurefire.failIfNoSpecifiedTests=false \
  -Danalytics.python="$DDIP_TEST_PYTHON" verify
npm run check
git diff --check
git diff --cached --check
~~~

Restart properties are test barriers, never runtime recovery flags. Omit them without a controller.
Initial focused iterations exposed the need for a distinct immutable qualification ID and conservative
PID-only descendant treatment; both were corrected and the final focused/full runs pass. Markdown/
local links (44 files) and hygiene passed. The production JAR excludes all recovery test classes and
test capability/process helpers. No CI configuration change; hosted CI remains unverified. The test
container is returned to its initial stopped state with its volume retained. The owner branch/index
are preserved; no Git/GitHub mutation was performed.

Changed files: RecoveryFailure.java, RecoveryQuiescence.java, RecoveryFiles.java and
RecoveryHandoff.java in the existing execution package; RecoveryQuiescenceTest.java and
RecoveryHandoffIT.java; the PostgreSQL test include in services/content-api/pom.xml; this contract
and services/content-api/README.md. No migration/dependency/native-module/accepted-ADR change.

**Known limitations:** metadata fencing does not stop external writers; local cooperative guard and
trusted manual orphan/unmanaged-writer inspection remain necessary. Some existing gateway evidence
retains PID without original start instant: if that identity cannot be established, or any such PID
is live/ambiguous, C3b deliberately refuses transfer. It never invents missing process evidence or
repairs it. Missing/corrupt/changed required private command/runtime evidence also remains blocked,
even if native output might independently exist. There is no universal process containment, hard
filesystem I/O deadline, distributed failover or database/filesystem atomicity/power-loss guarantee.
Qualified-handle lookup tests prove in-process ambiguous-commit resolution and actual database restart;
C3c.1 below adds validated packet bootstrap after recovery-JVM restart; operational closure remains deferred.

**Planned, not implemented:** C3c must integrate an explicit operator workflow for fresh qualification,
validated retained-packet bootstrap/commit reconciliation, live RECOVERY_ONLY owner authorization
and coordinated heartbeats, stage INTENT/single-use launch fencing, actual runtime readiness, bounded
independent native read-only verification of exact frozen dependencies/output, safe result matrix,
atomic final control/attempt/run/artifact disposition and conditional slot release. A valid completed
result may win an interruption race; incomplete/unavailable/changed evidence must retain the accepted
classifications. A changed/dead recovery owner needs a new control and guarded handoff, never token
adoption. C3b internal tests do not authorize this production workflow.

**Deferred:** native reconciliation or final SUCCEEDED/FAILED/CANCELLED/APPLIED processing, owner
release, new CANCEL/RETRY behavior, automatic takeover/retry, process killing, native repair/cleanup/
republishing, Content/RSS, UI, AWS and full E5 completion. The C3c.1 slice below extends this checkpoint.

## C3c.1 guarded recovery bootstrap

**Implemented:** bounded internal retained-evidence bootstrap and separate live-owner authorization.
Execution Recovery remains **partially implemented**. No production recovery workflow, endpoint,
operator command or dispatcher integration is activated. ADR 0010/0011, migrations V1–V7 and native
product contracts are unchanged. This section extends the C3b evidence above; its prior test counts
remain historical.

### Acceptance boundary and implementation choices

Acceptance for this slice is exact reconstruction of committed C3b metadata from retained evidence;
read-only, repeatable bootstrap/lookup; fail-closed evidence and version checks; explicit fresh
quiescence plus the exact live guarded owner before authorization; and an occupied RECOVERY_ONLY
slot throughout. No bootstrap/authorization call may create an attempt, increment an epoch, append
an audit, mutate artifacts/private evidence, invoke a native child, finish a control/run or release
ownership. Existing C3b failure injection and ordinary-worker fences must continue to pass.

RecoveryPacket strictly decodes the existing canonical qualificationVersion=1 packet, rather than
introducing a second journal or changing the retained format. It rejects unknown/missing fields,
duplicate JSON keys, noncanonical bytes, invalid types/UUIDs/decimal versions and unsupported policy
or version. The existing 1 MiB packet, 512 stage and 20 attempt bounds apply. The eight original
metadata projections reconstruct typed identities, numbers, booleans and UTC timestamps; canonical
byte columns remain length/SHA-256 descriptors. No payload is reconstructed from a digest. The
reconstructed snapshot must hash identically to the packet and compare to committed PostgreSQL
records, including immutable run/control identity, profile/installation bindings and recorded
versions. Mirrored packet fields must agree with that projection.

RecoveryHandoff.bootstrap requires the held installation guard, an exact private relative packet
address/hash and the approved runtime profile. It checks private/no-link file policy and packet
identity across the read, original operator-review consistency, frozen run/source/input contracts,
runtime/deployment/module/root bindings and the complete C3b private sidecar inventory. Explicit
absence, bytes, length, device/inode and mtime must match, including sidecars that appear later.
It never invokes runtime.readiness or a native verifier. The packet file identity/mtime/hash are
sealed in the bootstrap handle and checked again before authorization.

The same coordinator-locked C3b resolver serves both in-memory qualifications and decoded evidence;
there is no second handoff interpretation. CONFIRMED_HANDOFF requires exact successor binding,
versions, original history and ordered audit anchors, including the packet address/hash.
CONFIRMED_ROLLBACK requires the unchanged original snapshot, no successor and no contradictory
handoff audit. A successor with any recovery-stage invocation is outside this bootstrap slice.
Changed/advanced ownership, version drift, contradictory audit or unavailable SQL is UNRESOLVED.
Inconclusive bootstrap rejects with HANDOFF_UNRESOLVED and yields no handle. Lookup of an existing
handle returns only historical facts and never authorizes, renews or adopts ownership.

RecoveryHandoff.authorize is a separate internal operation. It requires a new explicit review of
the original snapshot and every unresolved checkpoint within the existing 60-second window,
including all retained descendant identities. It observes the exact recovery PID/start as LIVE,
rechecks all old worker/child/descendant identities and PID-only hints, packet/sidecar/runtime
integrity and the held guard. Only then does a final short coordinator-locked transaction confirm
the exact handoff again, compare registered PID/start/instance, profile/host bindings and all
run/attempt/control/worker/coordinator versions, and return the original committed Owner binding.
The guard is checked once more after the final SQL transaction before returning authority.
External observations never run inside SQL transactions; ambient caller transactions are rejected.
Neither bootstrap nor authorization writes SQL or files.

A retained review can establish historical qualification after it expires; it cannot satisfy the
fresh authorization review. A different guard instance or an exited/restarted recovery JVM can
inspect historical facts but cannot adopt that owner, even on the same host/PID. A heartbeat changes
versions and makes the old packet inconclusive; bootstrap never discovers and adopts newer versions.
A dead/changed owner requires another explicit C3a control targeting the occupied recovery attempt,
fresh qualification and a C3b occupied-to-occupied handoff. Ordinary managed dispatch remains blocked.

Reusing the packet and resolver avoids a migration, new persistence authority or dependency. The
tradeoff is deliberate strictness: missing retained files/runtime bindings and version drift can
block historical reconstruction even when a native result might exist. A tolerant best-effort
reader or implicit token refresh would weaken the existing fail-closed contract. No new architecture
decision is proposed or marked accepted by this implementation.

### Validation and remaining boundary

**Tested locally:** 323 Java unit/component cases and 574 distinct PostgreSQL cases passed across
this slice's regression runs. These are suite-union counts, not a claim of one full 574-case run.
The full invocation passed all 323 Java cases and 572/573 PostgreSQL cases; its one error was the
new native-child sentinel fixture omitting required execution-profile/storage associations. That
fixture was corrected without weakening runtime validation. After the final evidence/guard review,
the final affected invocation passed all 323 Java cases and all 118 RecoveryHandoffIT cases with
zero failures, errors or skips. Those 118 include all 64 existing C3b cases and 54 new bootstrap cases;
repeated cases are not added to the totals. The other 456 PostgreSQL cases passed in the full run.

The initial focused run also caught an incorrect assumption about the retained array encoding;
the strict reader was corrected to the existing plain-array format. Forty affected cases passed
before the broader run. No known failing case remains after the final affected verification.

New coverage includes JSONL/Parquet and terminal-predecessor reconstruction; exact stage/audit
preservation; lost commit acknowledgment; rollback without authority; unavailable SQL; truncated,
altered, oversized, noncanonical, duplicate-key and contradictory packets; invalid bindings,
hashes, types and versions; symlink/hardlink/private-permission and parent-symlink rejection;
sidecar absence/appearance/replacement; packet replacement with unchanged bytes/mtime; stale
heartbeat versions; lost guard before and during final SQL comparison; unavailable/reused/dead
recovery identity; fresh review after expiry; and repeated bootstrap/authorization without writes.
A separate actual recovery JVM committed a handoff and exited: a replacement guard could inspect
its facts but required another explicit control and guarded occupied-to-occupied handoff. The
old packet then remained inconclusive against the competing successor. A deliberately failing
native-child sentinel proved qualification/bootstrap/authorization launched no native child.
Native/private snapshots and complete SQL snapshots stayed unchanged on read-only paths; ordinary
managed dispatch remained blocked. Existing transactional fault-injection and stale-owner cases
were retained and rerun.

Two actual dedicated-container PostgreSQL restarts were observed, one in each integrated run.
Each barrier required a later postmaster start time and identical committed state followed by
fresh packet bootstrap and separate same-live-owner authorization. No operational database was used.
The local runtime was GraalVM Java 21.0.7, Python 3.12.15, PySpark 4.0.2, DuckDB 1.4.5 and PostgreSQL
17.11-bookworm at the existing pinned image digest
`sha256:3645570cccdfa447589da9f57dd740faa29b30938e861289a5574b6ca6b03826`.
The full run included existing Spark-backed Silver import and Reliability publication regressions;
only those existing fixtures performed native publication. Recovery operations did not.

With the existing disposable DDIP_TEST_DB_URL/USER/PASSWORD environment and pinned
DDIP_TEST_PYTHON/PYSPARK_PYTHON/PYSPARK_DRIVER_PYTHON plus JAVA_HOME and SPARK_LOCAL_IP:

~~~sh
# Full regression, including the corrected-later sentinel fixture error described above.
# An external test controller restarts only the dedicated DB at READY and writes .done.
mvn -o -q -Daether.syncContext.named.factory=noop -f services/content-api/pom.xml -Ppostgres \
  -Dddip.test.bootstrap-restart-signal=/private/tmp/ddip-c3c1-bootstrap-restart verify

# Final affected run: 323 Java + 118 PostgreSQL, all passed, second actual DB restart.
mvn -o -q -Daether.syncContext.named.factory=noop -f services/content-api/pom.xml -Ppostgres \
  -Dit.test=RecoveryHandoffIT \
  -Dddip.test.bootstrap-restart-signal=/private/tmp/ddip-c3c1-bootstrap-final-restart verify
~~~

Restart properties are test-only barriers. Omit them without a dedicated-container controller.
The offline Maven option uses the existing dependency cache; it adds no dependency. Logs and a
per-suite full-run summary are retained under ignored outputs/e5-c3c1-* paths. Markdown/local links
(45 files), repository hygiene and diff checks passed. The final production JAR includes the
internal bootstrap classes and excludes recovery test helpers. The disposable container was removed.
No branch switch, staging, commit, push or GitHub mutation occurred.

Changed files: RecoveryPacket.java (strict existing-format reader); RecoveryHandoff.java (shared
historical evidence and separate guarded authorization); RecoveryFiles.java (read-only packet
identity seal); RecoveryHandoffIT.java and RecoveryBootstrapTestProcess.java (focused coverage and
exited-owner fixture); this contract, services/content-api/README.md and root README.md. No ADR,
migration, runtime role, public API or native implementation changed.

Diagnostics use the existing fixed RecoveryFailure codes with no causes, stack traces, raw packet,
SQL, path, PID or native exception text. Bootstrap, packet/file handles and Owner stringify to fixed
redacted labels. No rejection appends a durable audit or exposes a new public diagnostic surface.

The C3b packet does not retain its own historical inode in PostgreSQL. Bootstrap validates its
private file policy and hash/audit anchor, then detects replacement between bootstrap and
execution authorization; a byte-identical replacement before the first bootstrap cannot be
distinguished. Original sidecar identities are retained and compared across restarts. These remain
cooperative local POSIX checks, not hostile-administrator protection, universal orphan containment,
a hard filesystem deadline or a database/filesystem power-loss guarantee. Manual orphan/unmanaged
writer review remains essential. Hosted CI and other host/platform behavior remain unverified.

**Ready as a prerequisite for C3c.2, not production recovery:** C3c.2 must wire guarded stage
INTENT/single-use launch fencing and coordinated heartbeats, actual interpreter/package readiness
and bounded independent native read-only verification before consuming artifacts. Every future
operation must validate the live token and accepted quiescence at its boundary; this bootstrap
handle is not stage permission. Final result classification, SUCCEEDED/FAILED/CANCELLED disposition,
control completion and atomic conditional release remain deferred. No automatic takeover, retry,
cancellation, repair, Content/RSS recovery, AWS or native contract change is included.

## C3c.2 fenced native recovery verification

**Implemented:** internal single-use recovery verification intent/launch, actual pinned runtime
readiness, full independent Reliability inspection, bounded private evidence and historical stage
inspection. **Execution Recovery remains partially implemented.** Production recovery is inactive;
there is no new runtime role, dispatcher caller, REST endpoint or operator command. Final run,
attempt/control disposition, result-artifact registration and coordinator release remain C3c.3.
Publication activation remains outside this recovery checkpoint.

### Design review and acceptance boundary

The [implementation record](../adr/0012-recovery-verification-provenance.md) documents the choices
under accepted ADR 0010 without claiming another architecture acceptance. ADRs 0010/0011 and
migrations V1–V7 are unchanged. No native module, published format or run-profile contract changes.

| Required review question | Bounded implementation |
| --- | --- |
| One authorized RECONCILE stage | Reuse VERIFY_RESULT/item 0 in stage_invocations under the recovery attempt. Atomically require no existing stage, insert INTENT, advance only that attempt CLAIMED to RUNNING/version+1 and append bounded audit. Existing PK, version guards and occupied-slot constraints apply |
| Original provenance versus current ownership | A new private E5_RECOVERY_VERIFICATION_V1 envelope binds recovery control/attempt, instance, epoch, qualification, fresh review, checkpoint and adapter digest. An inner unchanged VERIFY_RESULT command retains original EXECUTE run/attempt/epoch/runtime and the exact retained preflight pack |
| Meaning of VERIFY_RESULT | RECONCILE kind, recovery policy and RECOVERY-prefixed audit distinguish inspection from EXECUTE publication. VERIFIED_PUBLICATION is stage evidence only. The run retains its interrupted currentStage context and RECOVERY_REQUIRED state; no stage_artifacts/result-artifact or activation is created |
| Launch, identity, heartbeat, result fences | Coordinator-first short transactions compare exact installation/run/control/attempt/epoch/worker/coordinator versions, registered PID/start/instance and frozen selection. Stage identity/hash/version/child bindings are checked independently. Guard checks bracket transactions; native I/O runs outside SQL |
| Restart and ambiguous child | Read-only inspect validates committed stage, handoff/observation audits and retained file hashes without an owner token. An INTENT/RUNNING stage or uncommitted private completion is INCONCLUSIVE. No existing stage is relaunched and no newer owner/version is adopted |

Acceptance requires real independent JSONL/Parquet reads, runtime readiness before native artifact
consumption, explicit unavailable/changed/incomplete distinctions, one consumed launch, bounded
cleanup, durable inspection and unchanged native bytes. Every outcome must preserve occupied
RECOVERY_ONLY, run RECOVERY_REQUIRED, control REQUESTED and blocked ordinary dispatch. Fault injection
must preserve prior ownership and never mint a final run result from partial/uncommitted evidence.

### Guarded intent, supervision and evidence

RecoveryVerification is package-private and instantiated only by isolated tests. It first obtains
C3c.1's separately authorized exact live owner, validates frozen relational evidence and loads the
original VERIFIED CHECK_INPUTS pack. The pack's canonical command/bundle/result/evidence/anchor
hashes and exact original PUBLISH_RELIABILITY command, intended fingerprint/namespace and runtime
must agree. No discovery or replacement of missing provenance is allowed. The current launch seam
requires an immediate EXECUTE predecessor with that verified preflight and publication intent;
recovery-chain stage history is inspectable, but is not automatically reused to launch again.

Before intent it writes only exclusive private command.json/retained.json under the new recovery
attempt/checkpoint. Intent commits before any child. The final beforeStart callback checks the
held guard, unchanged private bytes/identities, original evidence/runtime and a fresh quiescence
review, then conditionally consumes INTENT -> RUNNING and commits launch audit. The review is
retained in the command and bound by its persisted hash; freshness is checked again under SQL locks.
An existing/ambiguous intent, stale version, expired review or lost guard rejects without refresh.

ReadOnlyVerifierProcess launches exactly one default read-only child, with cleared/whitelisted
environment, closed stdin, independently bounded streams, recorded PID/start/instance and five-second
fenced heartbeats. Readiness and inspection share this child and one timeout (approved profile
verificationSeconds, maximum 900 seconds); cleanup retains the existing bounded graceful/forced/
drain waits. An unexpected descendant or uncertain cleanup cannot produce VERIFIED evidence.
The OS-start/PID-record gap remains explicit: a consumed RUNNING stage without child identity is
ambiguous. Losing the identity/heartbeat transaction stops supervision and retains that stage;
private native output alone cannot establish a committed observation.

The adapter is packaged separately in the API JAR and passed with Python -I -B -X utf8 -c. Its SHA-256
is recorded in command, audit and result anchor. It checks compatibility with the known original
reliability_execution.py and reliability_execution_preflight.py hashes, then uses the original
verify_runtime and inspect functions. It never calls execute, publish, SparkSession or a gateway.
Original command-to-attempt/epoch checks remain byte-for-byte unchanged. The original frozen
manifest, all module/root/profile bindings and actual Python 3.12.15/PySpark 4.0.2/DuckDB 1.4.5
readiness are checked before artifact consumption; Java 21 and JAVA_HOME are checked by the host.
Unsupported original deployments fail closed, never substitute a new native runtime.

The unchanged independent reader rechecks exact frozen dependency captures, native provenance,
publication completion/inventories/schemas and analytical results, including its ordered replay and
rank validation. The adapter inspects only the intended fingerprint. Explicit absence/incompleteness
requires safe no-symlink namespace ancestors and repeated negative observation; generic I/O is never
proof of absence. Dependency/runtime rechecks follow inspection. If original independently verified
publication evidence exists, a different freshly valid publication is classified as changed.
Native output/staging/reference/batch namespaces are never created, repaired or written. Only the
new explicitly authorized private sidecars and approved private runtime scratch may be written.

After proven clean child exit, result.json and evidence.json are strictly decoded, bound to both
recovery ownership and original provenance, re-read and hashed into exclusive verified.json. The
final exact-owner transaction seals only the verification stage and appends RECOVERY_STAGE_OBSERVED.
It changes no run/control state or slot. Lost final acknowledgment is resolved through historical
inspection, not a repeated child or best-effort finalization.

| Observation | Persisted stage evidence; never final run disposition |
| --- | --- |
| Full independent verification succeeds | VERIFIED / VERIFIED_PUBLICATION, complete private native qualification and confirmed clean exit |
| Safe exact namespace or completion absence | FAILED / OUTPUT_ABSENT or OUTPUT_INCOMPLETE; negative evidence rechecked |
| Demonstrated changed or invalid output | FAILED / OUTPUT_CHANGED or OUTPUT_INVALID, fixed native code; no repair |
| Dependency integrity/provenance failure | FAILED / DEPENDENCY_CHANGED or DEPENDENCY_INVALID. A recorded inventory entry proven missing is changed evidence |
| Unavailable dependency/output I/O | FAILED / DEPENDENCY_UNAVAILABLE or OUTPUT_UNAVAILABLE. This means the inspection failed; it is not a known data/run failure |
| Readiness/compatibility or resource rejection | FAILED / RUNTIME_INCOMPATIBLE or RESOURCE_LIMIT; readiness phase, native code and child/cleanup evidence remain distinct |
| Confirmed start/deadline/output/private-evidence failure | FAILED with fixed supervision code and NO_CHILD_LAUNCHED or CONFIRMED_TERMINATED evidence; never inferred publication success |
| Interrupted verifier or unconfirmed cleanup | UNKNOWN with fixed code and UNCONFIRMED evidence, retaining any recorded child identity |
| Ownership/DB loss, unknown intent/launch/result commit | Preserve the last committed stage and occupied owner; inspect returns INCONCLUSIVE unless a fully matching final stage/anchor/audit committed |

Historical inspection takes the coordinator lock for a bounded committed projection, then reads
exact private paths outside SQL. It checks installation, RECONCILE/handoff/qualification identity,
launch review against the original snapshot and recorded launch time, command/retained/adapter
hashes, result anchor, native result schema and child/cleanup bindings. It performs no native launch,
artifact inspection, state transition or token adoption. A replacement guard/JVM can inspect facts;
C3c.1 continues to reject execution adoption and old bootstrap after a stage/heartbeat has advanced.

### Local validation and limits

Local validation used Darwin/aarch64, GraalVM JDK 21.0.7, Python 3.12.15, PySpark 4.0.2,
DuckDB 1.4.5 and disposable PostgreSQL 17.11. The PostgreSQL container used the CI-pinned image
postgres:17.11-bookworm@sha256:3645570cccdfa447589da9f57dd740faa29b30938e861289a5574b6ca6b03826.
JAVA_HOME, DDIP_TEST_PYTHON, PYSPARK_PYTHON, PYSPARK_DRIVER_PYTHON, SPARK_LOCAL_IP=127.0.0.1
and DDIP_TEST_DB_URL/USER/PASSWORD selected these local runtimes and the disposable database;
no cloud credentials or services were used. The existing Java process-supervision and quiescence
unit tests ran alongside the new PostgreSQL cases. Documentation checks used Node.js 26.12.0 with
the lockfile-pinned Markdown tooling; the hosted documentation job's Node.js 24 was not tested locally.

Commands below ran from the repository root, timed with /usr/bin/time -p. Offline mode and the
Maven named-lock override accommodate this local Maven cache; neither changes application behavior.

~~~bash
mvn -o -q -Daether.syncContext.named.factory=noop -f services/content-api/pom.xml \
  -Ppostgres -Dddip.test.verification-restart-signal=/private/tmp/ddip-c3c2-verification-restart verify
mvn -o -q -Daether.syncContext.named.factory=noop -f services/content-api/pom.xml \
  -Ppostgres -Dit.test=RecoveryHandoffIT,RecoveryVerificationIT verify
npm run check
~~~

| Executed check | Result and measured elapsed time |
| --- | --- |
| Full content-api PostgreSQL profile, including real native publication/verification regressions | 323 unit/component + 613 PostgreSQL integration cases; no failures, errors or skips; 2,397.36 seconds (39m57s) |
| Final affected regression after the last command-provenance and adapter review changes | 323 unit/component + 158 PostgreSQL integration cases (118 handoff + 40 verification); no failures, errors or skips; 174.78 seconds (2m55s) |
| New verification class in the final affected run | 40 cases, 75.568 seconds including two cached native JSONL/Parquet fixture publications; slowest individual case 7.171 seconds |
| Earlier focused verification and process/quiescence regression | 38 verification cases plus selected unit tests; passed in 79.69 seconds before the final two coverage additions |
| Markdown/local links and repository hygiene | Passed across 46 Markdown files; 1.60 seconds |
| Whitespace and JAR boundaries | Passed; production JAR includes the private adapter and excludes test classes/fixture publisher |

The full profile preceded the last narrow review changes; the final affected run covers those
changes. Their union covers 614 distinct PostgreSQL cases, not 614 cases in one full-profile run.
The restart-barrier test in the full run performed an actual docker restart of the disposable
PostgreSQL container between verification and historical inspection. That optional test property
requires an external test controller to wait for the signal file, restart only the disposable
container, await pg_isready and write the .done file; without it the case exercises replacement-guard
inspection without claiming a database restart. Other fault tests cover SQL
rollback/commit acknowledgment loss at each intent/launch/child/result boundary, active-child
heartbeat/database loss, actual start failure/deadline, interrupted/unconfirmed cleanup, competing
calls, stale owners/guards, incompatible readiness, changed private evidence and inaccessible I/O.
Successful and unsuccessful observations retain RECOVERY_REQUIRED/REQUESTED/occupied RECOVERY_ONLY,
no registered publication/result artifact, blocked unrelated dispatch and byte-identical native
batch/reference/output/staging snapshots. The disposable container was removed after validation.

CI timing is not proven by these local measurements. The new class costs about 76 seconds locally,
including approximately 25 seconds creating its two native fixtures; no new individual case exceeded
8 seconds. The existing ReliabilityExecutionIT class took 1,769.607 seconds in the full run, with
staleFinalOwnershipCannotInsertArtifact at 1,030.420 seconds and
lostFinalAcknowledgmentResolvesExactlyPersistedResultWithoutSecondWriter at 575.010 seconds.
The cause of these existing-case outliers is unproven. The unchanged processing/API CI job has a
60-minute budget and also runs other checks; its total budget margin and hosted result remain
unverified. No CI restructuring or unrelated timing changes are included.

C3c.3 still needs fresh independent qualification at the disposition boundary, complete result
matrix policy (including unavailable I/O and pending cancellation), atomic fenced attempt/control/
run/artifact disposition and conditional release. A historical verification receipt is not proof
of current artifact readability. Interrupted launch/cleanup and missing/corrupt retained provenance
stay blocked. No automatic retry/takeover, cancellation, repair or broader product recovery exists.
The adapter compatibility allowlist and stored adapter digest intentionally fail closed on binary/
runtime changes; future versions must explicitly preserve compatible historical readers. Cooperative
POSIX guard checks, trusted manual orphan/unmanaged-writer review and existing native contracts
remain essential. No universal orphan containment, hostile-user fencing, hard filesystem deadline,
power-loss atomicity, hosted-CI result or production operational recovery is claimed.

## C3c.3 fenced recovery finalization

**Implemented:** internal fresh qualification and atomic Reliability reconciliation disposition.
**Execution Recovery remains Partially implemented.** There is no production caller, recovery
command, public endpoint, automatic invocation/takeover, cancellation, guarded retry or publication
activation. Earlier C3c.1/C3c.2 limitations describe those checkpoints; this section supersedes only
their statements that internal final disposition is absent. ADRs 0010/0011 remain accepted and
unchanged. ADR 0012 remains an implementation record; [ADR 0013](../adr/0013-recovery-finalization-evidence.md)
records these scoped choices without claiming new architecture acceptance.

### Exact outcome and transition matrix

All release rows require the same exact live RECONCILE owner, fresh explicit original-writer review,
known recovery-child cleanup, fresh independent qualification, immutable private evidence and frozen
selection/root identity validation. Native data mutation is distinct from changed authority,
private provenance, root identity or verification evidence: the latter always blocks disposition.

| Independently qualified observation | Run outcome / safe reason | Attempt / control | Retry flag / coordinator |
| --- | --- | --- | --- |
| Exact completed publication, full native analytical/dependency verification, unchanged freshly readable evidence | SUCCEEDED / RECOVERED_PUBLICATION; exact result artifact/dependency link and counts | SUCCEEDED / RECONCILED_SUCCEEDED; APPLIED / RECOVERED_PUBLICATION | false; release to READY |
| Exact intended namespace positively absent; complete frozen inputs retained | FAILED / PUBLICATION_ABSENT | SUCCEEDED / RECONCILED_FAILED; APPLIED / PUBLICATION_ABSENT | true only below the 20-attempt bound; release to READY |
| Existing intended namespace without complete.json, positively and repeatedly observed | FAILED / INCOMPLETE_OUTPUT; retain all files | SUCCEEDED / RECONCILED_FAILED; APPLIED / INCOMPLETE_OUTPUT | false; release to READY |
| PUBLICATION_INVALID plus separate readable-byte proof of malformed completion metadata, wrong manifest fingerprint or declared inventory/hash mismatch | FAILED / PUBLICATION_INVALID; no repair | SUCCEEDED / RECONCILED_FAILED; APPLIED / PUBLICATION_INVALID | false; release to READY |
| PUBLICATION_CHANGED corroborated by stable fresh qualification | FAILED / PUBLICATION_CHANGED | SUCCEEDED / RECONCILED_FAILED; APPLIED / PUBLICATION_CHANGED | false; release to READY |
| Observed dependency mutation with EVIDENCE_CHANGED, INPUT_CHANGED, SNAPSHOT_CHANGED or DEPENDENCY_CHANGED | FAILED / DEPENDENCY_CHANGED | SUCCEEDED / RECONCILED_FAILED; APPLIED / DEPENDENCY_CHANGED | false; release to READY |
| BATCH_INVALID, SNAPSHOT_INVALID or DEPENDENCY_INVALID from full native dependency verification | FAILED / DEPENDENCY_INVALID | SUCCEEDED / RECONCILED_FAILED; APPLIED / DEPENDENCY_INVALID | false; release to READY |
| Unavailable I/O/dependencies, unsupported runtime, uncertain launch/cleanup, possible writer, path/resource/query ambiguity, contradictory classifications or changed authority/private evidence | No terminal disposition: RECOVERY_REQUIRED; no result artifact | Recovery attempt unchanged; control REQUESTED | false; occupied RECOVERY_ONLY, unrelated dispatch blocked |
| Pending cancellation | Deferred, including an otherwise verified publication; no CANCELLED or TOO_LATE inference | No control finalization | Occupied RECOVERY_ONLY |

A successful reconciliation operation may establish a FAILED run. The attempt's outcome code and
completion audit explicitly record that distinction; a negative verification-stage result is not
itself a failed reconciliation or known failed run. Unresolved evidence does not close/reject the
control in this slice. There are no new run/attempt/control/stage states or schema transitions.
The absent-output retry flag is eligibility evidence only: no RETRY command/execution is implemented,
and any future retry must requalify the same selection and namespace under the accepted policy.

### Fresh qualification and authority

RecoveryFinalization is package-private and has no application bean, role, endpoint or dispatcher
caller. qualify requires the live owner carried from C3c.1 through the actual C3c.2 Observation;
it does not reconstruct or refresh an owner from a historical receipt. The item-0 stage/version,
private result/anchor/command, original qualification/provenance and native identity must agree.
Unconfirmed C3c.2 cleanup or an inconclusive observation cannot start qualification.

Exactly one VERIFY_RESULT/item 1 records finalization qualification. It uses the existing C3c.2
intent-before-launch, conditional one-time authorization, PID/start/instance persistence, bounded
read-only process, exact-owner heartbeat and immutable observation mechanism. Native command item 0
and original EXECUTE attempt/epoch are unchanged: SQL item 1 identifies this second recovery check,
not a new native publication/input item. Its audit adds recoveryItem=1; item-0 protocol/audit bytes
and the packaged Python adapter remain compatible. No existing intent is reopened or relaunched.
An ambiguous item-1 launch, commit or cleanup remains occupied and requires explicit recovery work;
qualification cannot adopt another token or reconstruct a live capability after JVM replacement.

Outside SQL, bounded no-symlink reads seal every selected dependency and intended publication file's
bytes, names, inode/device, mode, size and modification/change times, with safe repeated negative
namespace observations. The seal checks approved root identities and is identical before and after
the fresh full native verifier. Before final disposition it rereads the exact native seal and private
proof, revalidates original writer/descendant quiescence with a fresh operator review, checks the exact
live recovery process and both verifier children, and revalidates frozen relational selection and
original private publication provenance. Both recovery items' private bytes, inode/device and modification evidence are also sealed and reread.
Qualification expires after 60 seconds; freshness is checked
again in the final transaction. Native seal reads are capped at 8 GiB, 8,192 entries, depth 8 and a
cooperative 60-second deadline per pass. Unsupported/oversized trees fail closed, never truncate.
These bounds are safety limits, not measured performance or hard filesystem-I/O deadlines.

A prior VERIFIED stage is historical evidence. A changed/unreadable publication after that stage
cannot be finalized through its receipt. Qualification classifications/native code/publication must
agree with the retained C3c.2 observation; later changes or contradictory observations block release.
The native reader sometimes maps OSError to PUBLICATION_INVALID. Therefore that code alone is never
sufficient: the separate readable-byte fault proof above is mandatory. Other invalid diagnoses stay
blocked rather than silently treating unavailable I/O as corruption. No native module is modified.

### Atomic disposition and replay

The existing handoff fence locks the coordinator first, then run, predecessor/control, registered
workers and current recovery attempt in its established order. Finalization locks both exact recovery
stage items, then result artifact identities. Native/process/large filesystem work finishes before
this short transaction. Only the bounded installation-guard identity check also brackets commit.
The SQL transaction compares installation, run/control/attempt identities and states, owner instance,
epoch, worker/coordinator/run/control/attempt/stage versions, command/result anchors, frozen selection,
absence of cancellation/stop conditions, qualification freshness and audit capacity.

In one transaction it inserts or exactly reuses the immutable Reliability artifact and frozen input
edges, appends the item-1 RESULT link, closes the recovery attempt, closes the original run with exact
counts/result or safe failed reason/retry flag, applies the RECONCILE control, marks the recovery
worker STOPPED, and conditionally clears only this occupied binding while setting coordinator READY.
The epoch does not increment. Five bounded completion audits link the same run/attempt/control,
qualification, both checkpoints, hashes, native classification/code, final operator-review digest
summary, before/after versions, result and disposition. Original submission/selection, interrupted
attempts and existing stage evidence are retained. Any row-count/constraint/artifact/audit/guard
failure rolls back all disposition effects. No intermediate idle state is committed.

Existing read-only run/attempt/control/event projections allow the new fixed safe outcome codes,
recovery audit actions and coordinator audit states; response shapes and private-detail exclusion
are unchanged. Unknown/free-form diagnostics still fail closed.

The coordinator being READY does not advertise a READY recovery worker. Its session is STOPPED and
cannot dispatch. The normal fresh host-guard/worker-registration lifecycle must establish a new
READY process instance before claiming queued work. No automatic transition or runtime caller is added.

finish first resolves exact committed historical facts in a coordinator-locked transaction. A matching
completion requires the exact immutable attempt/control/stage/result/link/dependency/audit projection;
a completed receipt carries no session or execution authority. It can be inspected after restart
under a replacement guard, even after an unrelated newer owner claims the slot. It never clears that
newer owner. Pending confirmation requires the exact original binding and versions; uncertain SQL,
lock/connection failure or inconsistent facts returns UNRESOLVED. No unprotected read proves rollback.

Only a still-held, still-fresh in-memory qualification plus fresh external rechecks may proceed after
a confirmed non-finalized projection. SQL rollback can therefore be retried without another verifier
launch. Lost final acknowledgment is resolved by the same control/checkpoint/qualification identity;
no duplicate artifact, completion audit, epoch increment or release is permitted. A lost qualification,
expired review or changed/dead owner cannot be adopted. Cancellation, automatic repair/retry and
recovery-chain operational orchestration remain deferred.

### C3c.3 local validation and remaining limits

Local validation used Darwin/arm64, GraalVM JDK 21.0.7, Python 3.12.15, PySpark 4.0.2,
DuckDB 1.4.5 and disposable PostgreSQL 17.11. The database used
postgres:17.11-bookworm@sha256:3645570cccdfa447589da9f57dd740faa29b30938e861289a5574b6ca6b03826,
bound only to loopback. JAVA_HOME, DDIP_TEST_PYTHON, PYSPARK_PYTHON,
PYSPARK_DRIVER_PYTHON, SPARK_LOCAL_IP=127.0.0.1 and DDIP_TEST_DB_URL/USER/PASSWORD
selected the local runtimes and disposable database. Native tests required native process-inspection
permissions; sandbox denial did not relax identity or cleanup checks. No AWS/cloud service was used.
Documentation checks used Node.js 26.12.0 and lockfile-pinned Markdown tooling; CI Node.js 24 was not
validated locally.

The stabilized focused run preceded one full content-api PostgreSQL regression. Commands from the
repository root, timed with /usr/bin/time -p, were:

~~~bash
mvn -o -q -Daether.syncContext.named.factory=noop -f services/content-api/pom.xml \
  -Ppostgres -Dtest=VerifierProcessTest,RecoveryQuiescenceTest,RunInspectionTest \
  -Dit.test=RecoveryFinalizationIT verify
mvn -o -q -Daether.syncContext.named.factory=noop -f services/content-api/pom.xml \
  -Ppostgres -Dddip.test.bootstrap-restart-signal=/private/tmp/ddip-c3c3-bootstrap \
  -Dddip.test.verification-restart-signal=/private/tmp/ddip-c3c3-verification \
  -Dddip.test.finalization-restart-signal=/private/tmp/ddip-c3c3-finalization verify
npm run check
~~~

The offline/named-lock options accommodate this local Maven cache, not application behavior.
The optional restart properties require an external test controller to wait for each signal,
restart only the disposable container, await pg_isready and write its .done file. That controller
completed all three actual restarts in this full run. Without those properties the cases exercise
historical/replacement-guard behavior without claiming a database restart.

| Executed check | Result and measured elapsed time |
| --- | --- |
| Stabilized focused finalization plus process/quiescence/history units | 53 PostgreSQL + 36 unit cases; zero failures/errors/skips; 118.48 seconds |
| One full content-api PostgreSQL profile after stabilization | 323 unit/component + 667 PostgreSQL integration cases; zero failures/errors/skips; 975.08 seconds (16m15s) |
| New RecoveryFinalizationIT in the full run | 53 cases, 118.090 seconds, including native JSONL/Parquet fixture creation and actual PostgreSQL restart; slowest individual case 2.343 seconds |
| Retained RecoveryHandoffIT and RecoveryVerificationIT in that full run | 118 cases in 87.306 seconds; 40 cases in 77.672 seconds; all passed |
| Markdown/local links and hygiene | Passed across 47 Markdown files; 1.67 seconds |
| Whitespace and production JAR boundaries | Passed; unchanged C3c.2 adapter included, test fixture classes/publisher excluded |

New cases cover real independent JSONL/Parquet recovery and exact result/dependency linkage; absent,
incomplete, corrupt/changed output and missing/changed dependencies; unavailable I/O versus positive
corruption proof; changed/missing original provenance, root/private-file identity drift, stale owner/
review and lost guard (including inside the final transaction); uncertain child cleanup/launch;
rollback after every disposition mutation; real SQL artifact/audit insertion failures; lost commit
acknowledgment, duplicate/concurrent finalizers, outage before/during/after commit, database restart
and replacement-guard historical lookup. Matching artifact evidence is reused exactly; conflicting
evidence blocks the entire disposition. An unrelated queued run remains blocked through recovery
and can be claimed only after safe release and fresh worker registration. Replaying old completion
after that new claim preserves the newer occupied binding.

Native batch/reference/output/staging byte hashes, sizes and modification times are unchanged
across recovery in both successful and known-failure cases. Fault fixtures introduce bad/missing
files before their preservation snapshot; recovery never repairs them. Snapshot assertions also
cover failure/rollback/acknowledgment paths. The only Spark publications are test-fixture preparation,
not the recovery path. SQL assertions confirm old INTERRUPTED attempt outcomes and exact result/ownership bindings; existing
immutability guards protect submission/frozen inputs and sealed history. Finalization updates no
original stage or audit row. Only fully qualified success registers/links a result. All
unresolved cases retain occupied RECOVERY_ONLY with no completion audit/result registration.

Development runs exposed sandbox process-identity denial and a queued-run fixture that needed a
healthy pre-handoff coordinator heartbeat; those issues were corrected without weakening guards.
No failing case remains in the stabilized focused or full run. Generated harnesses/logs/reports stay
ignored under outputs/ and target/. The disposable PostgreSQL container was removed after validation.
No branch/index/history/GitHub operation was performed.

The new class adds about two minutes locally, including approximately 30 seconds constructing its
two native fixtures; no new individual case exceeded three seconds in the full run. The unchanged
processing/API CI job has a 60-minute budget and additional Python checks. Hosted results, total
job margin and Linux runtime behavior remain unverified; these local timings are not benchmarks.
Large evidence trees, the cooperative 60-second seal deadline/review window and the 8-GiB/8,192-entry
bounds have not been scale-qualified. Conservative invalid-output diagnosis can intentionally keep
otherwise diagnosable native schema/analytical failures blocked when a separate byte fault cannot
be established. Immediate original EXECUTE provenance and the existing native compatibility allowlist
remain prerequisites; full recovery-chain/expired-qualification operation is still future work.

The last native/process/guard observation and PostgreSQL commit are not atomic. Byte/identity seals,
short SQL fences and explicit fresh operator review narrow but do not eliminate the filesystem/
database time-of-check/time-of-use gap. Cooperative locks cannot fence hostile/unmanaged writers or
prove universal descendant containment. Filesystem power loss, hostile mutation, hard blocked-I/O
deadlines, production recovery activation and hosted CI are not claimed. Operational activation still
needs an owner-reviewed invocation/authorization/diagnostics workflow, retained-evidence handling,
recovery-chain and expired/ambiguous qualification procedures, cancellation/retry policy integration,
installation-level orphan review and hosted regression evidence. These are not enabled by this slice.

## C3c.4 controlled operational recovery

C3c.4 adds a trusted `operator-recovery` non-web role to the existing packaged application. Its
[operator procedure](../operations/reliability-recovery.md) defines prerequisites, exact control
selection, bounded read-only inspection, two fresh challenge-bound confirmations, guarded execution,
interruption/restart procedures and supported outcomes. [ADR 0014](../adr/0014-controlled-operational-recovery.md)
is an implementation record, not a new accepted ADR. ADRs 0010/0011 remain accepted and unchanged;
0012/0013 retain their implementation-record statuses. Earlier checkpoint sections are historical.

The role does not admit controls or dispatch work. It obtains the original approved installation
guard before registering the exact recovery process/profile, then retains it through qualification,
fenced handoff, exact committed-packet bootstrap, separate live-owner authorization, independent
verification, fresh final qualification and conditional atomic disposition/release. Inspection
never registers a process or grants authority. Confirmation binds a fresh nonce, phase, exact target,
private inventory and process/status/checkpoint evidence; manual handler/orphan/unmanaged-writer
review and explicit known-descendant start identities remain mandatory. Output is bounded to safe
IDs, hashes and fixed states/codes. No native paths/payloads, SQL, secrets or exception diagnostics
are printed. Input is one bounded 128-KiB JSON line, expiring after 60 seconds; metadata is reread and
external process/filesystem evidence is checked outside SQL transactions.

Repeated completed controls resolve exact immutable historical finalization facts without a new
worker/attempt/artifact/completion audit or changing a newer occupied owner. An uncompleted successor
cannot be adopted by another JVM. A fresh C3a control targeting that successor must establish a new
fenced owner. The new owner can follow at most twenty immutable handoff packets to the original
EXECUTE provenance: exact run, selection, historical process/attempt/epoch relationships, packet
hashes/runtime and private inventories must agree. The operator reviews all ancestor recorded
processes/unresolved checkpoints and retained descendants again. Native verification keeps the
original EXECUTE command identity; historical evidence never supplies live owner authority. Missing,
changed or unsupported ancestry stays blocked. No old verifier intent is reopened.

C3c.3's outcome matrix and atomic release remain authoritative. Unsupported/unavailable observations
report that final qualification cannot proceed and stop occupied. Pending cancellation never becomes
CANCELLED. A safe completion stops the recovery worker; ordinary dispatch requires a separate fresh
worker/guard registration. There are no migrations, native module/data-contract changes, new services,
public recovery routes, arbitrary commands, automatic takeover/retry/publication, AWS or UI changes.

### C3c.4 validation and technical review

The stabilized checkpoint passed focused packaged acceptance and a subsequent complete content-api
PostgreSQL regression. Production Java/resources/POM SHA-256 snapshots remained unchanged across
those final passes. All final cases below passed with zero failures/errors/skips. C3c.3's recorded
323/667/three-restart evidence remains historical; the following results were executed for C3c.4.

Local environment: macOS 27.0.1 / Darwin 27.0.0 arm64, GraalVM JDK 21.0.7, Python 3.12.15,
PySpark 4.0.2, DuckDB 1.4.5 and disposable PostgreSQL 17.11 using the pinned image digest in the
tracked harness. Native children use UTC; the broad Python regression requires TZ=UTC. Maven used
the populated local cache with -o and -Daether.syncContext.named.factory=noop. Markdown checks used
Node.js 26.12.0 and lockfile-pinned tooling; hosted Node.js 24/Linux behavior remains unverified.
These are local elapsed observations with small synthetic fixtures, not performance benchmarks.

| Final executed validation phase | Cases / result | Measured elapsed seconds |
| --- | --- | --- |
| Packaged operator acceptance plus role units | 35 PostgreSQL + 17 unit cases; one actual restart | 226.45 |
| Subsequent full content-api PostgreSQL profile | 702 PostgreSQL + 324 unit/component cases; four actual restarts | 1214.82 |
| Complete Python processing regression with UTC | 373 cases | 422.60 |
| Java ingestion | 29 cases | 4.65 |
| Batch-launch argument/behavior script | Passed | 0.36 |
| Fresh synthetic Gold/JDBC fixture generation | Passed | 17.73 |
| Java JDBC API | 48 cases | 3.34 |
| Spring analytics HTTP/native compatibility | 20 cases | 13.87 |
| Sum of these final successful phase executions | 31m43.82s, excluding development iterations and documentation checks | 1903.82 |

The complete backend phase sum excluding the preceding focused operator pass is 1677.37 seconds
(27m57.37s). Phases were measured individually across local resumed invocations, not as one uninterrupted
benchmark. PostgreSQL phase times include disposable-container setup/teardown; suite times below are
Maven-reported class times. Test reports/logs stay ignored under outputs/ and target/. Every disposable
container and its test volume was removed; the harness now removes anonymous volumes on teardown.

The final focused pass preceded the full profile with these repository-root commands; configure
JAVA_HOME and DDIP_TEST_PYTHON as described in the [operator guide](../operations/reliability-recovery.md):

~~~sh
python3 scripts/test-recovery-postgres.py --operator-only --offline
python3 scripts/test-recovery-postgres.py --full --offline
TZ=UTC PYTHONPATH=processing "$DDIP_TEST_PYTHON" -B -m unittest discover -s processing/tests -v
mvn -o -q -Daether.syncContext.named.factory=noop -f ingestion/pom.xml verify
bash scripts/test-run-batch.sh
PYTHONPATH=processing:processing/tests "$DDIP_TEST_PYTHON" -B \
  services/api/tests/generate_fixtures.py --output services/api/target/fixtures
mvn -o -q -Daether.syncContext.named.factory=noop -f services/api/pom.xml verify
mvn -o -q -Daether.syncContext.named.factory=noop -f services/content-api/pom.xml \
  -Pintegration -Dtest=NoSuchTest -Dsurefire.failIfNoSpecifiedTests=false \
  -Danalytics.python="$DDIP_TEST_PYTHON" verify
npm run check
~~~

Fixture generation requires a fresh target directory. The previous generated fixture directory was
preserved under target before generation. The integration command omits units already covered by the
full PostgreSQL profile. No application scaffolding, dependency changes or CI restructuring were added.
The five actual restarts in the final focused/full passes were verified by postmaster-start changes;
operator-only mode coordinates one, and full mode coordinates bootstrap, verification, finalization
and operator barriers. Both restart outcomes are safe: the same JVM may continue after fresh checks,
or a database error exits occupied and requires a new explicit operator invocation. Tests never
require automatic application retry or reconstruct authority from an old receipt.

| Acceptance scenario | Executed evidence |
| --- | --- |
| Interrupted JSONL and equivalent Parquet | Actual packaged ordinary workers publish native output, then are forcibly terminated at a test-controlled verification boundary; the packaged operator independently verifies and finalizes SUCCEEDED |
| Known failed publication | Absent, incomplete and byte-proven invalid output resolve FAILED through the existing atomic finalizer; stable changed dependency resolves DEPENDENCY_CHANGED |
| Changed or unavailable evidence | Post-observation mutation, unreadable native dependencies, missing/changed retained origin packets, stale ownership and pending cancellation retain occupied recovery with no result/completion audit |
| Ambiguous acknowledgments | A test-only PostgreSQL wire proxy drops actual COMMIT acknowledgments at handoff and finalization; exact same-process/historical resolution yields one attempt/artifact/completion |
| Live/unknown writers and operator approval | Actual active old-writer and live descendant vetoes; missing, expired, inconsistent, wrong-challenge/process/checkpoint/lineage review and changed ownership after inspection all block |
| Recovery JVM interruption | Termination before handoff permits fresh inspection of the same control; after handoff the old control cannot be adopted and an explicitly admitted new control recovers through reviewed immutable ancestry |
| Outage, restart and contention | Startup outage has a fixed safe diagnostic; outage during final confirmation retains ownership; actual restarts and competing guarded JVMs preserve exact ownership |
| Return to ordinary operation | An unrelated queued run ID remains blocked, then completes under a fresh packaged ordinary worker after atomic release; this run reuses the same synthetic input selection through the normal native replay contract |
| Repeated invocation | JSONL/Parquet completed controls add no artifact, completion audit, worker registration or attempt; replay after subsequent ordinary work preserves newer facts |

Operator acceptance compares native batch/reference/output/staging SHA-256, size and modification-time
snapshots across successful and blocked recovery. For native mutation faults, preservation snapshots
are taken after the deliberate mutation. Actual-worker cases retain observed Python/Spark descendant PID/start
identities and explicitly review them; no synthetic timestamp is invented for an unknown PID. The
only native publishers run during fixture preparation or ordinary worker execution. Recovery invokes
the existing fixed read-only adapter and two independent verifiers. The production JAR contains the
operator role and existing adapter, and excludes test fixtures, fault proxy and test process classes.

Technical review covered role isolation, bounded/sanitized summaries, fresh challenge binding,
coordinator-first fences, host-guard lifetime, same-live-owner bootstrap, bounded original-provenance
chains, ambiguous acknowledgments and exact historical replay. Original C3b metadata-only handoff/
bootstrap acceptance remains independent of native provenance; the operator wrapper and native
verification enforce the additional ancestry checks. Accepted ADRs, migrations V1–V7, native modules,
WorkerRepository, WorkerIdleLifecycle and InstallationHostGuard are unchanged.

Final delivery review rechecked all nine changed production-file hashes against the saved final-test
snapshot, the passing suite totals and actual-restart markers, and the packaged role/bridge/test-class
boundary. It corrected stale current-status introductions in the root/module guides, contract and
ADR index while preserving historical checkpoint text. Fresh Markdown/link and repository-hygiene
checks passed across 49 Markdown files in 1.74 seconds; git diff --check passed. These final edits are
documentation-only; the application suites above were not repeated after this consistency review.

Development checks exposed a fixture lookup typo, overly broad ancestry checks at the C3b metadata
boundary, Docker's reassignment of automatically allocated ports on restart, missing UTC/test-fixture
path settings in the broad local driver, and an overly strong expectation that a pooled connection
must survive restart. These were corrected in the fixture/harness/integration boundary. A final review
also refreshed the guarded summary immediately before confirmation. The subsequent focused and full
passes above are clean; no production file changed during them. Python's successful processing run
still emitted existing PySpark socket ResourceWarnings; no unrelated processing change was made.

The unchanged hosted processing/API job has a 60-minute timeout and additional dependency/setup work.
OperatorRecoveryIT costs 225.881 seconds locally in the full run, including native fixture preparation,
actual worker publications, subprocess startup and a deliberate 61-second stale-confirmation wait.
Cached local totals cannot establish hosted CI margin; cold setup, Linux/process-identity behavior and
native timing variance remain risks. No new timeout, optimization or hosted CI result is claimed.

The [operator procedure](../operations/reliability-recovery.md) documents trusted-OS-user approval,
manual orphan/unmanaged-writer review, cooperative POSIX guard limitations and the non-atomic final
filesystem/SQL observation gap. The existing native bounds and 60-second review/seal windows remain
conservative safety limits, not scale qualification. CANCEL/RETRY, managed RSS/Content execution,
Operations UI, multiworker/automatic recovery, hosted release evidence and broader E5 closeout remain
unfinished. No automatic recovery, distributed failover, exactly-once effects or production readiness
is claimed. The root capability row records the new scoped workflow while retaining partial status
for broader Execution Recovery.

| PostgreSQL integration suite in the final full run | Cases | Measured seconds |
| --- | --- | --- |
| ImportOwnershipIT | 42 | 9.875 |
| OperatorRecoveryIT | 35 | 225.881 |
| PersistenceIT | 28 | 13.010 |
| RecoveryFinalizationIT | 53 | 112.047 |
| RecoveryHandoffIT | 118 | 84.404 |
| RecoveryVerificationIT | 40 | 79.427 |
| ReliabilityExecutionIT | 27 | 207.725 |
| ReliabilityPreflightIT | 41 | 28.965 |
| ReliabilityWorkerIT | 23 | 146.557 |
| ImportWorkflowIT | 38 | 22.392 |
| SilverImportIT | 23 | 139.630 |
| ControlAdmissionIT | 52 | 25.005 |
| RunHistoryIT | 46 | 35.850 |
| RunSubmissionIT | 44 | 20.049 |
| SourceManagementIT | 36 | 14.692 |
| WorkerCoordinationIT | 56 | 30.192 |

### C3c.4 exact changed files

- [README.md](../../README.md)
- [docs/adr/0014-controlled-operational-recovery.md](../../docs/adr/0014-controlled-operational-recovery.md)
- [docs/adr/README.md](../../docs/adr/README.md)
- [docs/architecture/durable-execution.md](../../docs/architecture/durable-execution.md)
- [docs/operations/reliability-recovery.md](../../docs/operations/reliability-recovery.md)
- [scripts/test-recovery-postgres.py](../../scripts/test-recovery-postgres.py)
- [services/content-api/README.md](../../services/content-api/README.md)
- [services/content-api/pom.xml](../../services/content-api/pom.xml)
- [services/content-api/src/main/java/platform/contentapi/ContentApiApplication.java](../../services/content-api/src/main/java/platform/contentapi/ContentApiApplication.java)
- [services/content-api/src/main/java/platform/contentapi/execution/OperatorRecoveryConfiguration.java](../../services/content-api/src/main/java/platform/contentapi/execution/OperatorRecoveryConfiguration.java)
- [services/content-api/src/main/java/platform/contentapi/execution/RecoveryOperatorInspection.java](../../services/content-api/src/main/java/platform/contentapi/execution/RecoveryOperatorInspection.java)
- [services/content-api/src/main/java/platform/contentapi/execution/RecoveryHandoff.java](../../services/content-api/src/main/java/platform/contentapi/execution/RecoveryHandoff.java)
- [services/content-api/src/main/java/platform/contentapi/execution/RecoveryFinalization.java](../../services/content-api/src/main/java/platform/contentapi/execution/RecoveryFinalization.java)
- [services/content-api/src/main/java/platform/contentapi/execution/RuntimeRole.java](../../services/content-api/src/main/java/platform/contentapi/execution/RuntimeRole.java)
- [services/content-api/src/main/java/platform/contentapi/execution/RuntimeEnvironment.java](../../services/content-api/src/main/java/platform/contentapi/execution/RuntimeEnvironment.java)
- [services/content-api/src/main/resources/application.properties](../../services/content-api/src/main/resources/application.properties)
- [services/content-api/src/test/java/platform/contentapi/execution/OperatorRecoveryIT.java](../../services/content-api/src/test/java/platform/contentapi/execution/OperatorRecoveryIT.java)
- [services/content-api/src/test/java/platform/contentapi/execution/RecoveryAcknowledgmentProxy.java](../../services/content-api/src/test/java/platform/contentapi/execution/RecoveryAcknowledgmentProxy.java)
- [services/content-api/src/test/java/platform/contentapi/execution/RuntimeRoleTest.java](../../services/content-api/src/test/java/platform/contentapi/execution/RuntimeRoleTest.java)

## E5 D2 — internal managed Bronze storage and receipt checkpoint

D2 extends the merged D1 journal under accepted ADRs 0001/0002/0010. Managed Content remains
**Partially implemented** with production dispatch disabled. The [D1/D2 operations guide](../operations/managed-rss-fetch.md#d2-storage-and-receipt-lifecycle)
records the exact lifecycle, private evidence/receipt schemas, replay matrix, local measurements and
changed files. This implementation does not alter the accepted storage contract or add a migration.

The committed D1 journal, captured revision/endpoint, original RawFeed UUID/Instant/bytes and frozen
CONTROL/BRONZE/runtime bindings must revalidate before a STORE_BRONZE intent. Existing PostgreSQL
coordination CASes the ledger to BRONZE_WRITING and consumes one exact worker/attempt/epoch launch.
A fixed prebuilt Java child uses the existing LocalS3/S3BronzeStorage implementation. Actual bounded
GETs independently compare both original keys, all original payload/metadata bytes, lengths/hashes and
applicable headers. Transactions remain separate from S3 I/O. Only independently verified evidence and
current ownership permit one atomic receipt + VERIFIED stage + audit commit. The run is not successful,
no analytical artifact is registered and no Silver/Gold/Content processing or activation follows.

Read-only inspection can confirm a committed receipt after lost acknowledgment from actual object
reads and the immutable stage/private anchor. It does not reconstruct an Owner token or rewrite a
receipt. Exact NoSuchKey for both keys classifies absence; matching payload-only state identifies the
ADR 0002 retry case. Neither classification activates storage retry after a consumed intent. Metadata-only,
mismatching or unavailable evidence cannot become success. A complete pair without a committed receipt
remains unconfirmed pending explicit process/evidence-qualified Content reconciliation. Uncertain stages
retain occupied ownership, including DB/guard loss and crashes before/among/after PUTs. No automatic
refetch, orphan adoption, repair, delete, takeover or slot release is introduced.

The original D1 journal remains unchanged. New bounded private STORE_BRONZE/INSPECT_BRONZE sidecars anchor
child identity, command/result/evidence hashes and observed clean exit. Receipts retain exact original
identity/configuration/keys/hash linkage and actual read Content-Type. The existing writer still emits
binary payload MIME; pinned SeaweedFS may infer application/xml for payload.xml on GET. Verification
accepts only this documented mapping or the original binary MIME, requires exact JSON metadata MIME and
fails other mismatches. Existing receipt columns are write-once through the fenced application CAS;
immutable terminal-stage hashes plus fresh evidence detect direct administrative receipt/digest changes.
No directory-fsync, end-to-end power-loss or distributed object fencing claim is added.

D3 still requires exact verified Bronze receipt selection and managed Silver qualification. Production
Content wiring, explicit storage-only retry authority, orphan receipt finalization and Content recovery
qualification remain deferred. Reliability production dispatch/recovery and standalone Content/native
contracts remain unchanged. Hosted D2 validation and full native Spark execution are separate from the
scoped local D1/D2, metadata, supervision and control regression recorded in the guide.

## D3 managed Bronze-to-Silver checkpoint

Managed Content Processing remains **Partially implemented**. D3 is an internal extension
of D1/D2; production `WorkerExecutionCapability` and `WorkerIdleLifecycle` still dispatch
only Reliability. There is no managed Gold/enrichment or operational Content recovery.
See [the D3 operator contract](../operations/managed-rss-fetch.md#d3-managed-silver-lifecycle)
and [implementation record ADR 0015](../adr/0015-managed-silver-receipt-adapter.md).

Every selected delivery must be committed `BRONZE_VERIFIED`. For each frozen source ordinal,
D3 reopens the original D1 journal and committed D2 stage anchor and independently checks
both actual Bronze objects. The command freezes the exact original receipt, source revision,
ingestion UUID/time, metadata/payload keys, run/attempt, storage binding and approved runtime.
No listing, latest selection, implicit expansion or publisher request enters this boundary.
The adapter calls native `bronze_to_silver.process_one` with verified frozen bytes. Native
observation semantics, physical Parquet schemas, partition layout and completion identities
remain unchanged; management IDs never replace ingestion IDs or enter native fingerprints.

The existing fenced `SILVER` stage transitions `INTENT` → single-use launch authorization →
`RUNNING` → independent qualification → final read-only evidence recheck → `VERIFIED`.
The intent commits before output-capable launch. Short authority transactions check exact
owner/epoch/instance/versions, frozen selection, installation guard, controls and deadline.
The existing supervisor records process identity, drains bounded output, heartbeats every
five seconds, imposes the reviewed deadline, observes descendants and terminates/reaps on
failure. There is no PostgreSQL transaction during Spark work. Known supervised failures
become `UNKNOWN`/`RECOVERY_REQUIRED` when current authority can commit; stale authority or
lost acknowledgments leave conservative occupied evidence. A duplicate invocation never
relaunches an existing Silver intent, including one without a recorded child PID.

Process success does not verify Silver. A separate supervised process uses the existing
Silver import qualifier and `silver_inputs.load_inputs` against only the intended marker.
It checks physical schemas/files, accounting and provenance, then binds the original marker
bytes, complete file/directory inventory and Bronze hashes. A final GET/hash/inventory
recheck precedes the atomic artifact, `stage_artifacts`, verified stage and audit transaction.
The registered artifact uses the existing Silver qualification format and inventory identity;
its linkage to the exact D2 receipt is retained in the stage command and verified anchor.
No schema migration or new data-product version is necessary.

Full read-only inspection may confirm a committed stage after a lost verification
acknowledgment. It checks the stage anchor, original identity and freshly qualified artifact
against registered evidence; it neither adopts ownership nor registers another result.
An incomplete, missing, corrupt or changed namespace cannot be overwritten, repaired, resumed
or deleted. Occupied ownership remains until a future supported Content recovery procedure.
Reliability operator reconciliation is explicitly incompatible with these Content stages.

## D4 managed Gold checkpoint

Managed Content Processing remains **Partially implemented**. Internal D4 extends the merged
D3 checkpoint using the existing `PREPARE_GOLD` and `PUBLISH_GOLD` stage names. Production
Content dispatch remains disabled. D4 stops with a verified Gold artifact and an occupied
`RUNNING` Content run; no enrichment, final run disposition, coordinator release or analytics
activation occurs. See [the D4 operator contract](../operations/managed-rss-fetch.md#d4-managed-gold-lifecycle)
and [implementation record ADR 0016](../adr/0016-managed-gold-frozen-dependencies.md).

Preparation requires the complete frozen fresh-source set, one committed independently verified
D3 artifact per ordinal, plus exactly submitted retained D3 artifact IDs. It rejects missing,
duplicate, incompatible or additional selected items. Original D1/D2/D3 anchors and receipts
are reopened; actual Bronze/Silver provenance and full qualified inventories are revalidated.
Imported-only history is explicitly unsupported in D4 and fails closed rather than disappearing
from the union. All selected markers share the approved Silver root/runtime; no source discovery,
latest lookup, refetch or unrecorded artifact expansion is allowed.

The preparation intent and one-use authorization precede its read-only Spark child. Full native
`silver_inputs.load_inputs` fixes the combined canonical manifest, fingerprint and accounting.
Its verified private evidence and exact `stage_artifacts` input links commit before publication.
Publication reopens this committed evidence and compares the entire native selection, then calls
existing `gold_publication.publish` under its own fenced intent/child identity/heartbeat lifecycle.
Only the original native manifest determines `v1/processor_revision=1/input_set=<fingerprint>`.
Management IDs, runtime paths and operational settings do not enter the product fingerprint.

A separate Spark child performs read-only full native Gold verification against those inputs;
the DuckDB opener independently checks schemas and article/daily accounting. Complete output
membership, original completion/manifest evidence, file hashes and filesystem identities are
captured and rechecked along with Bronze/Silver immediately before the short PostgreSQL commit.
The commit registers/reuses the exact Gold artifact, links its Silver dependencies and publication
stage, marks that stage verified and appends audit evidence atomically. No transaction spans Spark.
Existing stage/artifact/dependency tables suffice; native schemas and existing migration files
are unchanged. Metadata consumers accept the two existing qualified Silver registry identities
without changing rows or treating metadata as native qualification.

Both stages revalidate current owner/epoch/instance/versions, installation guard, controls,
frozen selection and reviewed runtime at authorization/heartbeat/checkpoint boundaries. The existing
Spark-capable supervisor enforces deadlines, bounded streams/descendant observation and cleanup;
the wrapper explicitly reaps its gateway. Known clean preparation/start failures may mark a stage
`FAILED`; uncertain launch, termination or publication is `UNKNOWN` or remains durably unresolved
when authority/DB acknowledgment is lost. All retain occupied recovery ownership. No automatic
Gold retry, repair, overwrite, namespace deletion, owner adoption or Content reconciliation exists.
A full read-only inspection can confirm a committed publication after a lost verification
acknowledgment without refreshing ownership or registering another artifact. `VERIFY_RESULT`
sidecar directories hold independent evidence; no final Content `VERIFY_RESULT` stage runs in D4.

## D5a managed Content publication and successful finalization

Implementation record: [ADR 0017](../adr/0017-managed-content-successful-finalization.md)
(**owner acceptance pending**). ADRs 0015/0016 retain their existing status. Managed Content
Processing remains **Partially implemented**; production Content dispatch is disabled.

The internal D5a seam consumes exactly the current attempt's committed, independently
verified D4 Gold artifact and the publisher-reference artifact captured by the original
submission. It does not select current references, latest Gold or a broader Silver set.
Original D4 command/anchor/registry/dependency proof is reopened, including all selected
D3 receipts and original D2 object evidence. Reference qualification must be valid E1a
metadata with the exact immutable capture and `publisher_reference.open_snapshot` result.
Actual Gold/reference bytes, completion, schemas, native identities and bound root identities
are rechecked before native publication and after verification. Changed/missing/incompatible
or substituted evidence fails closed.

D5a adds the existing documented `CHECK_INPUTS` as a durable **post-D4 Content dependency
qualification** stage in this internal checkpoint sequence. It does not introduce an initial
production Content worker preflight. `CHECK_INPUTS → PUBLISH_CONTENT → VERIFY_RESULT`
each uses the existing intent/one-use authorization/child identity/heartbeat/version fence.
The dependency digest binds ordered `GOLD/0` and `PUBLISHER_REFERENCE/0` artifact IDs and
qualification digests. The native prepared manifest/fingerprint is privately anchored to the
verified dependency-check result before the publisher intent. Publication identity remains
`v1/processor_revision=1/publication=<native Content fingerprint>`.

A reviewed additive runtime profile binds the D4 runtime digest, reference/Content bindings,
six fixed Content modules and a positive deadline no larger than 900 seconds. D4/D3 runtime
checks continue to pin CPython 3.12.15, Spark 4.0.2, Java 21, boto3/botocore 1.43.114 and
DuckDB 1.4.5, fixed scripts, approved environment/scratch and installation root identity.
Only `PUBLISH_CONTENT` may launch Spark descendants. The existing supervisor bounds
streams, execution and descendant cleanup; native Spark shutdown must also be confirmed.
Each authorization boundary rechecks guard, current authority, runtime and input metadata.
No Spark/native process holds a PostgreSQL transaction open.

The verifier calls the supported `content_analytics.open_publication`, which checks full
physical inventory/hashes/schemas, exact Gold preservation, reference left join, discovery
aggregation and quality accounting. It executes bounded `catalog`, `reference_coverage`
and `source_discovery_daily` reports. Store report hashes/row counts and scalar coverage,
not report rows. A separate final read-only recheck repeats the full applicable reader and
original dependency checks. Private command/result/child/evidence anchors are revalidated.
A marker, process exit or SQL registry record alone cannot establish successful publication.

Normal successful finalization uses existing coordinator → run → attempt/stage lock
ordering and exact worker/attempt/epoch/row versions. Require exactly three verified source
stages per fresh source plus verified `PREPARE_GOLD`, `PUBLISH_GOLD`, `CHECK_INPUTS`,
`PUBLISH_CONTENT`, and the current qualified `VERIFY_RESULT`; unknown/additional stages
block finalization. Require consistent Gold/reference dependency edges, all owned processes
confirmed stopped, a held host guard and no requested control or applied cancellation.
In one transaction register the immutable Content artifact and its two dependency edges,
verify `VERIFY_RESULT`, link its result, finish attempt/run `SUCCEEDED`, bind the run result,
record safe string counts (`articles`, `rows`) and audit evidence, and clear the exact occupied
coordinator. No failed terminal Content path is added.

A stale worker cannot finalize or release. Exact publication replay uses native read-only
verification and does not rewrite bytes. Duplicate invocation cannot obtain another launch.
Clean launch failure still retains occupied recovery ownership; processing/verification failures
retain partial output without overwrite, repair, deletion or automatic relaunch. Unknown cleanup,
DB/guard loss and ambiguous acknowledgments remain blocked. If the final commit actually
succeeded but acknowledgment was lost, the internal inspector confirms exact committed
terminal/result/stage/dependency facts and fully reopens native output. It has no ownership,
heartbeat, registration or release operation and remains safe after another run owns the slot.
Content operator reconciliation remains unsupported by the Reliability recovery command.

See [D5a operator evidence and validation](../operations/managed-rss-fetch.md#d5a-managed-content-successful-completion)
for measured results and D5b prerequisites. No analytics/API activation or
`ANALYTICS_PUBLICATION` selection changes occur.

## D5b-1 explicit managed Content reconciliation

D5b-1 extends C3c's guarded, occupied ownership handoff to
CONTENT_RSS_V1 through a pipeline-specific runtime boundary. Reliability retains its
original verification/finalization path. See the [Content recovery decision table and
operator procedure](../operations/managed-rss-fetch.md#d5b-1-explicit-content-recovery) and
[pending ADR 0018](../adr/0018-managed-content-recovery-boundaries.md).

| Capability | D5b-1 boundary |
| --- | --- |
| Explicit Content RECONCILE | Original attempt/epoch/process/configuration; same control schema/idempotency/version/audit contract |
| Independent read-only qualification | Original journal/GET-only Bronze; selected Silver; frozen Gold; exact reference/native Content |
| Completed Content recovery | Own reconciliation attempt; fenced atomic registration/disposition/release; immutable native bytes |
| Earlier verified checkpoint | Content-only PAUSED; no result and no retry flag; writers must be quiescent before release |
| Uncertain evidence or writer | RECOVERY_REQUIRED with occupied RECOVERY_ONLY; no dispatch |
| Content RESUME / automatic continuation | Unsupported; no control or transition to RUNNING |
| Production managed Content dispatch / activation | Disabled / deferred |

Migration V8 adds PAUSED solely for this explicitly requested Content recovery outcome.
This is an additive implementation departure from ADR 0010's prior pause/resume exclusion,
with owner acceptance of ADR 0018 pending. Accepted ADR text/status is not rewritten.

## D5b-2a explicit resume of recovered Content

The [operator procedure and boundary matrix](../operations/d5b2a-resume-validation.md)
now define guarded explicit RESUME for supported PAUSED Content runs. This supersedes
the D5b-1 table's historical "unsupported" RESUME entry and earlier control exclusions.

| Capability | Current boundary |
| --- | --- |
| RESUME admission | Strict existing conditional/idempotent control envelope; REQUESTED only |
| Apply RESUME | Guarded independent qualification, PAUSED → QUEUED atomically with control/audit |
| Explicit execution | New EXECUTE attempt/epoch/instance; original run and captured selection retained |
| Supported prefixes | Pre-fetch with no prior intent and no retained Silver; complete committed Gold; verified Content CHECK_INPUTS |
| Journal / Bronze / Silver continuation | Fixed RESUME_CHECKPOINT_UNSUPPORTED; no fetch/PUT/Spark replay |
| Success | Existing D5a independent verification and fenced registration/finalization/release |
| Ambiguous/missing proof or existing Content namespace | Fail closed; no next-stage launch or repair |
| Interrupted resumed attempt | Occupied recovery retained; inherited-prefix reconciliation unsupported |
| Production Content dispatch / automatic resume / activation | Disabled |

V9 extends only controls_type_check with RESUME. Historical V1–V8 SQL and historical
upgrade tests remain intact. Current-schema fixtures advance to nine migrations. The
64 MiB/4096-entry retained-evidence cap still limits recovery and resume. [ADR 0019](../adr/0019-explicit-content-resume.md)
is an additive implementation record pending owner acceptance.

## D5b-2b inherited-attempt reconciliation

The [current recovery matrix and validation record](../operations/d5b2b-inherited-recovery-validation.md)
extend the historical D5b-2a interrupted-resume entry above. Explicit C3c reconciliation
can combine actual resumed stages with their exact recorded original Gold/Content-input
prefix, without copying historical stage rows. Original recovery packets, applied RESUME,
claimed plans, child identities, retained seals and independent native verification remain
mandatory. Complete final Content qualifies for the existing fenced SUCCEEDED transaction;
an intact earlier boundary with no intended Content namespace qualifies for PAUSED.

Missing/changed inherited evidence, possible writers and unknown outcomes retain occupied
ownership. Another RESUME after this inherited recovery is explicitly unsupported; an empty
resumed stage list cannot be reinterpreted as original pre-fetch. Ordinary Content dispatch
remains disabled because the complete private-evidence envelope has not been proved to fit recovery.
The [Gate B0 audit](../operations/content-recovery-envelope.md) corrects the erroneous 32-feed claim:
the implemented Content maximum is eight feeds/40 MiB of payloads, before sidecars.
[ADR 0020](../adr/0020-inherited-content-reconciliation.md)
records this implementation with owner acceptance pending. No publication selection changes.
