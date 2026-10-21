# ADR 0011: Artifact import maintenance ownership

Status: **Proposed — owner review required**

Date: October 21, 2026.

Scope: E5 Stage B2b-1 only. This proposal supplements the accepted import exclusion requirement in [ADR 0010](0010-durable-execution-coordination-and-recovery.md). It does not supersede its execution/reconciliation ownership protocol or change accepted native contracts. No implementation, migration or architecture acceptance is claimed.

## Problem and repository evidence

The B2b-1 request authorizes verified input imports and allows a small compatible forward migration. It also requires stopping for owner review if the accepted coordinator model cannot represent import maintenance ownership safely. Inspection at `b0ffd78eb8e9597d74241c679c2b473facec12ac` found that the documented maintenance slot has no corresponding owner representation or abandoned-import closure protocol in B1/B2a:

| Boundary | Current contract and consequence |
| --- | --- |
| [V1 coordinator](../../services/content-api/src/main/resources/db/migration/V1__profiles_and_coordinator.sql) | Modes are READY, STOPPING and RECOVERY_ONLY. `active_attempt_id` and `owner_instance_id` must be null together; an owner references `worker_instances` in the same installation. There is no maintenance/import owner identity |
| [V3 ownership FK](../../services/content-api/src/main/resources/db/migration/V3__runs_attempts_and_stages.sql) | The deferred `(active_attempt_id, owner_instance_id, epoch)` FK must identify an actual attempt. Every attempt requires a run and has kind EXECUTE or RECONCILE. An importer cannot occupy this binding without fabricating execution history |
| [V4 recovery controls](../../services/content-api/src/main/resources/db/migration/V4__controls_rss_and_audit.sql) | RECONCILE requires a run, target attempt and quiescence confirmation. The accepted occupied-slot transfer also requires these records; it cannot close an abandoned import that has no run |
| [V5 coordinator guard](../../services/content-api/src/main/resources/db/migration/V5__immutable_identity_and_version_guards.sql) | Mutable fields are enumerated and versioned. Adding another ownership family requires explicit FK, exclusivity and transition rules, not just setting an otherwise unused UUID |
| [B1 repository](../../services/content-api/src/main/java/platform/contentapi/execution/JdbcCoordinatorRepository.java) | `compareAndSetIdleMode` regards `active_attempt_id IS NULL` as idle, including STOPPING/RECOVERY_ONLY. A second caller with the current version can change that mode; there is no import token to compare or durable import result to resolve |
| [Runtime roles](../../services/content-api/src/main/java/platform/contentapi/execution/RuntimeRole.java) | Only analytics, api, worker and migrate exist. `operator-import` is deliberately rejected; the worker is still idle and has no host guard |

A STOPPING flag could prevent a future well-behaved claim, but does not establish the required durable import identity, conditional finalization, lost-acknowledgment lookup or post-crash closure. A host lock alone can be released while orphan Spark descendants remain. Holding a SQL transaction over verification would violate the accepted short-transaction boundary. Neither approach fulfills B2b-1.

The review point is the addition of an independent owner family and its recovery semantics to the normative attempt-only coordinator contract. An additive column is possible technically; its meaning and release authority must be agreed before implementing it. Existing V1–V6 must remain unchanged.

## Proposed decision for owner approval

Use one forward V7 migration with an import-specific ownership record and a mutually exclusive reference from the existing singleton coordinator. Keep the same host guard/global slot, existing three coordinator modes and all run/attempt/control enums. Continue to use only `artifacts` and `artifact_dependencies` for artifact metadata.

1. Add `artifact_import_operations` solely for import intent, ownership and terminal acknowledgment. Required immutable fields: opaque `import_id`, installation UUID, process-instance UUID/PID/start instant, execution-profile ID/digest, host-binding digest, positive owner epoch, exact canonical catalog descriptor/hash (at most 64 KiB, at most 128 explicit selections) and database start time. Mutable fields: ACTIVE/SUCCEEDED/FAILED/INTERRUPTED state, expected row version, terminal time, fixed safe outcome code and exact result bytes/hash (at most 64 KiB). Result bytes identify registered opaque artifact IDs/evidence digests; they are not a second artifact registry. Permit only ACTIVE-to-terminal transitions with version increment; terminal outcomes and immutable identity are sealed against UPDATE/DELETE/TRUNCATE.
2. Add nullable `coordinator.active_import_id`, with a deferred composite FK to `(import_id, installation_id, owner_epoch)`. Enforce that an active import has STOPPING mode and null execution-attempt/worker-owner fields. Enforce mutual exclusion with execution/recovery ownership and one ACTIVE import. Retain existing deferred attempt-owner FK unchanged. Extend the V5 coordinator mutable-field guard explicitly for the new pointer. Enforce coordinator/operation state consistency at transaction completion; cross-table checks must serialize on the coordinator, including active attempt/run admission.
3. Extend every idle/claim/mode mutation predicate to require both owner pointers null and no unresolved active run, attempt or import. Extend coordinator snapshots to include the import pointer. Source management can continue its existing short metadata transactions and must never release either owner. Test existing B1 mode primitives against occupied import ownership.
4. Acquire the persistent, identity-checked installation host guard before acquiring import ownership. Under a short coordinator-first transaction, require the correct installation, READY mode, no occupied owner and no unresolved active run/attempt/import; increase epoch, insert ACTIVE intent, bind the pointer and append a safe intent audit. Confirm this commit before any verifier/Spark launch. Do not register a worker attempt or submit a run.
5. Qualify the exact catalog selection outside every SQL transaction. After verification, recheck root, marker, directory, inventory and file evidence. Under a final short coordinator-first transaction, compare import ID, process identity, epoch, coordinator version and operation version/state. Atomically insert/reuse immutable artifacts, insert required native dependency edges, store the terminal result, append safe result audit and clear only this import owner. Restore READY only when child cleanup is proven. A conditional mismatch rolls back all registry/result/audit changes and stops the operation.
6. A definite, safely cleaned-up verification failure can atomically record FAILED plus a fixed classification/audit and release this owner. Unknown process cleanup, database failure or uncertain commit acknowledgment never triggers best-effort slot clearing. No native file repair or overwrite occurs.
7. Resolve lost intent/result acknowledgment by reading the exact import ID and canonical request bytes. A committed result is historical evidence; it does not authorize a new verified success without revalidation. A fresh import invocation uses a new operation ID, independently requalifies all inputs, and can reuse only byte-identical registered identities. Never relaunch an ACTIVE operation after a lost acknowledgment.
8. An abandoned ACTIVE import keeps STOPPING ownership even after the host lock becomes free. Provide a separate explicit local import-closure operation, under the same host guard, comparing the exact old import ID/epoch/versions. Require actual installation-level quiescence confirmation and veto any observable surviving owned process. Atomically close it INTERRUPTED, append a fixed safe audit and clear its pointer; this operation performs no qualification or processing. Unknown descendants or uncertain closure acknowledgment keep the installation blocked. This is import maintenance closure, not a new run control, automatic takeover or a RECONCILE attempt. Subsequent qualification requires a fresh import operation.

The explicit abandoned-import closure in item 8 and independent ownership family in items 1–3 are the decisions requested for review. Database metadata cannot prove termination of filesystem writers; no lease/PID/connection-only release is permitted. Migration/application code must be delivered and tested together after approval so an old idle-mode update cannot release an import owner.

## Artifact identity correction in the same checkpoint

V2 already makes `(storage_binding_id, relative_address)` unique and V5 seals artifact/dependency rows. However, `artifacts_native_identity_idx` is nonunique. B2b-1 also needs a database-enforced native identity key, proposed as UNIQUE `(product_id, kind, native_identity)`. The fixed importer must define native identity using the native namespace keys, with fingerprint/evidence compared separately; changing bytes must not create another native identity. Existing duplicates must abort migration for owner inspection, with no deduplication or rewriting. This constraint is separate from the maintenance-owner decision and does not permit Gold/Content/Reliability publication imports.

## Alternatives and tradeoffs

| Alternative | Assessment |
| --- | --- |
| STOPPING plus host lock, without an import owner | Avoids a table but lacks durable request/owner/result binding and guarded abandoned-import closure; insufficient |
| Fake run/EXECUTE or RECONCILE attempt | Reuses current FKs but invents processing history and violates the B2b-1 boundary; rejected |
| Session/advisory lock or verification-long SQL transaction | Loses exclusion on connection loss or holds a transaction during I/O/Spark; rejected |
| General lease/lock framework or owner polymorphism across all execution records | Broader architectural change than this single maintenance operation; unnecessary |
| Independent import operation referenced by the existing coordinator | Adds one operational table and pointer, explicit crash recovery, migration/readiness work and tests; preserves the single slot and native artifact authority |

This is a correctness/complexity tradeoff, not a performance claim. No dependency or service is proposed beyond B1's PostgreSQL and the existing Python/Spark runtime.

## Acceptance criteria before implementation

- Real PostgreSQL proves mutually exclusive attempt/import ownership, conditional version/epoch checks, atomic registry/dependency/result/audit commit and rollback, and duplicate native/location constraints independently of application checks.
- Two cooperative local processes cannot qualify concurrently. Occupied execution/recovery/import ownership blocks admission despite expired heartbeat, disconnected DB session or a free host guard.
- Fault injection before qualification, after qualification and around commit leaves either one committed terminal result or the exact unresolved owner; lost acknowledgment never causes blind verifier relaunch or slot clearing.
- Explicit import closure cannot release a changed owner, bypass actual quiescence, use run controls or modify native artifacts.
- Actual native readers qualify JSONL v2 and Parquet-v1/v2-profile batches, publisher/service snapshots and completed Silver. Full Silver capture supplements the existing Spark schema/provenance boundary without changing its contract.
- All requested corruption, missing dependency, changed inventory, unsafe path, idempotency and regression cases in B2b-1 pass before calling that checkpoint implemented. Hosted CI remains unverified until observed after owner publication.

## Current disposition

Stopped before importer, verifier helper, host guard or migration implementation. The [supporting checkpoint](../architecture/durable-execution.md#stage-b2b-1-inspection-and-owner-review-checkpoint) records inspection and actual local validation. The [operational guide](../../services/content-api/README.md#e5-b2b-1-import-review-checkpoint) states that no runnable import invocation exists yet. Approval of this proposal authorizes implementing/testing this ownership correction as part of B2b-1; B2b-2 and worker execution remain outside that authorization.
