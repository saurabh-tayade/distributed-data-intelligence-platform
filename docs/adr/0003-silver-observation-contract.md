# 0003: Source-scoped Silver article observations

- Date: 2026-10-14
- Status: Accepted
- Acceptance date: 2026-10-14
- Implementation: Fixture-only local PySpark prototype; no operational Silver lake.

## Context

Bronze preserves retrieval identity and immutable bytes. Silver needs article identity without confusing new retrievals, repeated articles and replay. These semantics will affect future persisted consumers, so they need a durable review record beyond a prototype implementation guide.

## Decision

Adopt the version-1 [Silver observation contract](../architecture/silver-processing.md): one row per entry occurrence, deterministic source-scoped article hashes using publisher identifiers (RSS GUID or Atom ID, with suitable article-link fallback), original ingestion UUIDs, document ordinals and explicit provenance. Retain repeated observations and reject missing identifiers (including fallback links) or titles. Invalid optional publication/update dates remain null with deterministic codes in `quality_warnings: array<string>`; these warnings are separate from rejection reasons and do not reject otherwise-valid observations. Do not deduplicate across publishers.

The replay key includes source ID, ingestion UUID, entry ordinal and contract version under immutable-input assumptions. It is not an implemented transactional sink. Keep schema/version evolution and replay publication as separate future design work.

Atom fallback tolerates incomplete/nonconformant entries. Nonblank Atom summary takes precedence over supported inline text or escaped-HTML content; all content remains untrusted and linked resources are never fetched. Fixture Bronze references remain nullable; `bronze_payload_key` corresponds to Issue #9's proposed `payload_key`. The warning field refines the accepted version-1 prototype schema; no production migration is implied.

## Alternatives and consequences

Using ingestion UUIDs as article IDs prevents linking observations of the same article. Title/content hashes conflate identity with mutable content and can merge unrelated articles. A global URL identity requires canonicalization and cross-publisher policy outside this milestone. Source-scoped publisher IDs preserve an understandable boundary, but publisher ID changes and RSS fallback-to-GUID transitions create new article IDs.

Keeping only the latest row loses observation history and requires ordering/upsert semantics. Retaining all occurrences preserves evidence but requires consumers to choose their aggregation/latest-selection rules. The fixture prototype writes plain local Parquet; it provides no transactional append, deduplication, schema migration or exactly-once guarantee.

## Evidence and approval

Offline tests demonstrate deterministic IDs across retrieval UUIDs, source separation, repeated occurrences, explicit quality handling and Parquet read-back. See the architecture guide for runtime evidence and limits. The owner approved these architectural decisions on October 14, 2026. Accepted ADRs 0001 and 0002 remain unchanged.
