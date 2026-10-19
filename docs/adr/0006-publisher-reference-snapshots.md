# ADR 0006: Publisher-reference validation and immutable local snapshots

Status: **Accepted**

Original proposal date: October 19, 2026. Owner acceptance date: October 19, 2026. Approval covers E1a validation, immutable local snapshots, independent verification, local CLI, and focused synthetic tests/documentation only. It does not approve E1b or the broader platform expansion. Acceptance alone does not establish implementation or test completion; executed evidence is recorded separately in the [implementation and validation notes](../architecture/publisher-reference-snapshots.md#implementation-and-validation).

## Context and requirements

[Issue #38](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/issues/38) separates E1a contract review (Stage A) from implementation (Stage B). The merged [product specification](../product-specification.md) and [Content Intelligence specification](../data-products/content-intelligence.md) are proposed expansion requirements, not blanket architectural approval. This ADR originated in Stage A; the owner subsequently approved its scoped Stage B implementation.

The existing Python publication code implements canonical JSON fingerprints, explicit publication selection, verification, completion-last publication and read-only replay for Gold. Reference data needs a separate, small local boundary: strict CSV validation, retained versioned vocabularies, immutable reference/revision bindings, independent verification and bounded parsing. It must work without Spark, Gold, SeaweedFS, a database or network access.

Accepted ADRs [0001](0001-rss-raw-ingestion-contract.md), [0002](0002-bronze-object-storage.md), [0003](0003-silver-observation-contract.md), [0004](0004-gold-analytics-publication.md) and [0005](0005-gold-consumer-integration.md) remain unchanged. Source-scoped article identities, Gold v1 schemas/fingerprints, publication semantics, operator-configured consumer fingerprints, APIs and frontend behavior are unaffected. M6 acceptance and closeout remain separate.

## Decision

Adopt the exact [publisher-reference snapshot contract](../architecture/publisher-reference-snapshots.md). That document is normative for schemas, literal vocabularies, identity payload, layout, interfaces, examples, failure matrix and Stage B acceptance cases; those details are not duplicated here.

- Validate the entire bounded UTF-8 CSV before reserving output. Reject invalid records and duplicate source IDs; accept a header-only reference. Never trim, normalize, partially publish or infer attributes.
- Retain a canonical, reviewed vocabulary file with each snapshot. Unknown tokens and unsupported or altered vocabulary versions fail offline verification.
- Bind exact CSV bytes, vocabulary bytes/version, validation semantics, logical reference ID, immutable revision and curated authoring metadata into one canonical manifest fingerprint.
- Reserve exactly one local directory for each reference ID/revision within the configured root. A different fingerprint cannot reuse that binding. No database, mutable pointer or fingerprint-only directory index is needed.
- Write artifacts without overwriting, read back and verify, then write completion evidence last. Incomplete or corrupt destinations require explicit owner investigation; publication never repairs or deletes them automatically.
- Require explicit reference ID, revision and expected fingerprint for independent open/verify. Return detached validated rows and identity, not unchecked file paths for later reads.
- Use the existing Python runtime, standard CSV/JSON/hash facilities and the unchanged canonical serializer in `processing/publication_metadata.py`. Add bounded intake and contract checks around those facilities in Stage B; parser strict mode alone is insufficient evidence of this contract.

## Responsibilities and guarantees

The reference author chooses identifiers, revision, curated attributes and non-sensitive authoring metadata. A maintainer reviews vocabulary changes and supported versions. The future validator proves structural and semantic validity; the writer establishes the local immutable binding; the independent opener verifies retained evidence and returns validated rows. E1b may later consume that boundary, but this decision does not authorize enrichment, new Gold products, APIs or UI changes.

The configured root has one writer across all references and revisions, and a trusted owner controls its directories. Exclusive creation detects existing destinations; it is not a distributed lock. Verification checks integrity and consistency, not publisher authenticity or the truth of curated attributes. Checksums do not authenticate an author. Completion-last does not claim transactions, power-loss durability or safety against hostile concurrent filesystem mutation.

The contract fixes input, row, field and metadata limits. These bound intake and retained structures; they are not measured peak-memory guarantees. Operational roots, logging and invocation time are excluded from identity. Creation time is recorded once in completion evidence and preserved on identical replay.

## Alternatives and tradeoffs

| Alternative | Reason not selected |
| --- | --- |
| Loose CSV loaded directly at enrichment time | Cannot independently reproduce vocabulary/validation semantics or detect later input changes |
| Fingerprint directories alone | Permit two fingerprints for one reference/revision unless another binding mechanism is introduced |
| Separate mutable revision index/database | Adds consistency, recovery and service responsibilities unnecessarily at E1a scale |
| Normalize CSV rows before hashing | Hides changes in supplied bytes and requires a second equivalence contract; this proposal deliberately treats row order, quoting and line endings as identity-bearing |
| Fetch language/country registries at runtime | Makes verification network-dependent and silently expands accepted values |
| Permissive parsing, skip-invalid or automatic repair | Risks silent data loss, ambiguous joins and unintended revision replacement |
| Lock service, transactional store or automatic latest selection | Broader platform decisions with no E1a requirement; explicitly deferred |

The chosen design duplicates a small vocabulary per snapshot and makes harmless byte-format changes require a new revision. These costs buy self-contained verification and an understandable conflict rule. Separate roots do not enforce global revision uniqueness. Failed reservations can remain on disk and need manual investigation rather than transparent retry repair.

## Stage B acceptance and approved scope

The owner approved Stage B on October 19, 2026. Its acceptance matrix is in the [detailed contract](../architecture/publisher-reference-snapshots.md#stage-b-acceptance-cases). It requires bounded validation, safe diagnostics, fixed identity vectors, standalone verification after original inputs are removed, byte/mtime-preserving replay, conflict detection, failure injection and path protection.

The approved decisions are:

1. The strict CSV dialect, whitespace/control policy, empty-reference behavior and resource limits.
2. The literal vocabulary v1 tokens, their meanings and review/version rules.
3. Identifier syntax, exact-byte identity, identity-bearing authoring metadata and fixed validation policy versions.
4. The reference/revision directory binding, completion schema, explicit fingerprint selection and detached-reader boundary.
5. The single-writer/trusted-filesystem assumptions and fail-without-repair recovery policy.

Approval authorizes the scoped implementation pass, not a claim that E1a is merged or issue #38 is closed. Stage B comprises the validator, immutable snapshot writer, independent opener, local CLI and focused synthetic tests/documentation. E1b joins, enrichment publications, source registration, orchestration, authorization, exports, metadata services, multiwriter support and publication-selection changes need separate work and, where consequential, future ADRs.
