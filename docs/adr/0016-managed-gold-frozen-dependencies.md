# 0016: Managed Gold frozen dependencies and verification

Date: October 29, 2026. Status: **Implementation record; owner acceptance pending**.

D4 extends accepted ADRs 0004/0005 and 0010 with internal `PREPARE_GOLD` and `PUBLISH_GOLD`
checkpoints. ADR 0015 remains an implementation record with owner acceptance pending; this
record does not change that status or mark an accepted ADR superseded.

Gold identity is the existing native canonical Silver input-manifest fingerprint. A completed
single-source D3 stage is not the whole run's Gold input set. Before preparation, require one
verified D3 artifact for every frozen fresh source ordinal, and exactly the explicitly submitted
retained Silver artifact IDs. Retained inputs must also have committed D3 stage/anchor proof,
as required by this checkpoint's owner request. The broader execution contract permits imported
history in principle; D4 rejects unsupported imported-only history instead of omitting it.
All inputs must share the approved Silver binding and compatible reviewed D3 runtime. Older
incompatible deployments remain unavailable until explicitly supported, not silently upgraded.

The existing D3 reader now exposes its committed anchor/receipt proof separately from full Spark
qualification. Gold preparation consumes that proof and independently requalifies every selected
Silver/Bronze baseline with existing native readers, in one supervised Spark process. It then
loads only the exact markers as one native set. The private command binds artifact IDs, original
receipt evidence and qualified inventories. `PREPARE_GOLD` commits the canonical manifest,
fingerprint, counts, child evidence and exact input artifact links before any Gold publisher.

The fixed adapter calls the existing `gold_publication.publish` with that unchanged validated
set. An independent supervised Spark process invokes the additive read-only
`gold_publication.verify_publication` entry point over the existing `_verify` implementation,
then the existing DuckDB Gold opener. This checks complete row multisets against native Gold
built from selected Silver, physical schemas, provenance, hashes, accounting and SQL invariants.
It creates no Gold namespace. A final GET/hash/inventory recheck precedes atomic artifact,
Silver dependency edges, stage result/link and audit registration in the existing tables.

Using only the DuckDB opener would check output consistency without establishing full equality
with selected Silver. Calling the publisher from the independent verifier could create an absent
namespace. The small read-only entry point reuses the existing full verification logic while
avoiding both problems; native publication/CLI behavior and analytical semantics are unchanged.
Repeated qualification costs local CPU/I/O but preserves the established immutable evidence boundary.
There is no measured throughput improvement, multi-node execution or exactly-once claim.

D3 registration uses a qualified inventory digest as registry `native_identity`, whereas older
imports use the canonical address identity. Metadata admission/frozen-selection validation now
recognizes either existing identity only when consistent with the same fully shape-validated
qualification record. No existing rows are rewritten. D4 additionally requires the originating
committed D3 anchor; accepting a registry row alone still grants no processing authority.

No migration, native dataset schema/format change, new dependency, production Content capability,
activation pointer or final Content disposition is introduced. Runs remain occupied at verified
Gold. Known clean preparation/launch failures and uncertain publication outcomes are distinguished
in stage state but both retain recovery ownership. Reliability's recovery operator remains
incompatible with Content. D5 must qualify publisher references, freeze the exact verified Gold
for enrichment, implement final verification/disposition and separately address Content recovery.
