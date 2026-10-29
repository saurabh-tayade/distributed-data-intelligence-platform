# 0015: Managed Silver receipt adapter

Date: October 29, 2026. Status: **Implementation record; owner acceptance pending**.

D3 extends accepted ADR 0010 and the existing D1/D2 internal seams. It does not change
accepted ADRs 0001–0004, 0010 or 0011, or authorize production Content dispatch.

The native Bronze-to-Silver CLI already supports exact `--metadata-key` selection. A new
internal adapter calls its existing `process_one` function with the original receipt's
single metadata key and verified in-memory object bytes. It never calls `discover`.
The small wrapper is needed to compare the committed D2 receipt, retain a machine result,
check the pinned runtime, and explicitly reap Spark's gateway. The native CLI, schemas,
normalizer, publication layout, completion identity and replay keys remain unchanged.
Using a bucket scan or a management run UUID as ingestion identity would violate frozen
selection and native observation semantics. Reimplementing normalization or adding a job
framework would create a second processing contract without a D3 requirement.

One `SILVER` invocation per frozen source ordinal uses the existing intent, authorization,
child identity, heartbeat and occupied-owner model. All selected deliveries must already
be `BRONZE_VERIFIED`. The invocation records the exact receipt and reviewed D3 runtime
through hashed private commands and evidence. The additive runtime profile binds the D2
runtime digest, original run profile/deployment, Python/Spark/package versions, seven
module hashes, frozen Silver/TEMP bindings and a deadline of at most 900 seconds. This
internal profile is reviewed server metadata, not a request-supplied command or a native
fingerprint. It requires no new migration, dependency, worker capability or dispatch bean.

An independent supervised process reuses `silver_import_qualification.execute`, including
`silver_inputs.load_inputs`, original completion bytes, physical schemas, counts, row
provenance and complete file/namespace inventories. A final bounded GET/hash/inventory
recheck follows qualification and precedes the PostgreSQL commit. Registration reuses
`RegisteredImportEvidence`, `artifacts` and `stage_artifacts`; the artifact, linkage,
verified stage and audit commit atomically. The stage's private anchor binds the D2
receipt and qualification evidence. PostgreSQL never holds a transaction across Spark.

Reusing the import qualifier avoids weakening the established managed Silver boundary.
It costs an additional Spark qualification process for a new publication. These are
correctness checks, not throughput measurements. Qualification validates native contracts;
it does not reconstruct every analytical field from the raw feed or authenticate publishers.

A duplicate or ambiguous invocation cannot acquire another intent. Failures retain the
occupied slot; known supervised failures mark `UNKNOWN` and interrupt into recovery-only
state when current authority can still commit. Lost database acknowledgments stop that
repository's authorization channel. Full read-only inspection may confirm a committed
result without refreshing an owner or registering it again. Neither incomplete namespaces
nor uncertain writers are resumed, repaired or removed. Reliability's operator-recovery
command continues to reject Content; operational Content reconciliation is a D4 prerequisite.
