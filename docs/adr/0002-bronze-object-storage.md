# 0002: Bronze object storage

- Date: 2026-10-12
- Status: Accepted
- Acceptance date: 2026-10-12
- Implementation: M1 Day 3 adapter, tests, and local Compose configuration supplied for review; SeaweedFS 4.48 local integration and restart persistence validated on 2026-10-12; Amazon S3 compatibility not tested.

## Context

Raw feeds need durable persistence and provenance before downstream Silver processing. The local MVP must run without AWS credentials, cloud access, or recurring infrastructure costs. The accepted [ADR 0001](0001-rss-raw-ingestion-contract.md) deliberately left a future storage boundary and deferred build tooling until external dependencies were justified. Its retrieval contracts remain unchanged.

## Proposed decision

Use a minimal `BronzeStorage` boundary and `BronzeReceipt`, implemented by `S3BronzeStorage` with the AWS SDK for Java v2. S3-compatible operations are the architectural dependency. SeaweedFS Community Edition is the selected local implementation; Amazon S3 is a future cloud target subject to explicit approval and compatibility validation.

Use the pinned SeaweedFS 4.48 image/digest with `weed mini`, a persistent named volume, loopback-only S3 publication, and explicit local credentials. Disable extra Iceberg/Lance endpoints, WebDAV, and the admin UI. Bucket initialization belongs to explicit development deployment, not application startup. Single-node operation limits cost and setup complexity but provides no high availability or off-host durability guarantee.

Store exact payload bytes separately from versioned JSON provenance. Keys encode source IDs with unpadded Base64URL and partition by UTC ingestion date and existing UUID. Compute SHA-256 over the exact payload. Publish metadata last; consumers require valid metadata and the referenced payload and should verify its length and checksum. Metadata-last is not transactional atomicity.

Require one writer and immutable source/date/provenance per ingestion identity. GET existing objects and compare bytes before PUT: identical retries are no-ops; matching orphan payloads can be completed; conflicts are explicit failures. This costs read I/O but avoids relying on unverified conditional writes. The key namespace is the collision boundary, not a global UUID index. Orphans and uncertain outcomes remain possible; there is no exactly-once guarantee, distributed lock, or automatic cleanup. See the [full contract](../architecture/bronze-storage.md).

Introduce Maven for the ingestion module, AWS SDK BOM 2.55.15 with S3 and URLConnection transport, Jackson 2.22.3 for mature JSON escaping/serialization, and JUnit Jupiter 6.1.3 (Java 17+ baseline, compatible with Java 21). Pin build plugins. The SDK replaces hand-written request signing/retry/protocol machinery; Jackson avoids a custom JSON writer. Exclude unused Apache/Netty transports. Preserve the Day 2 tests through a JUnit wrapper. Default tests are offline; opt-in Failsafe tests target only a loopback server.

`LocalS3` requires all configuration, uses no ambient AWS credential chain or proxy, and cannot target a cloud endpoint. Path-style addressing, fixed-length uploads, required-only SDK checksums, and one SDK attempt simplify local compatibility and error semantics. The storage adapter accepts a caller-configured S3 client, so AWS-specific credential/addressing choices remain outside contracts.

## Alternatives considered

| Alternative | Evaluation for this milestone |
| --- | --- |
| Amazon S3 directly | Strong relevance to the eventual cloud target, but requires cloud authorization, credentials, and cost controls; fails local-first acceptance |
| MinIO | Relevant S3-compatible alternative already mentioned as a candidate in the overview; would still require release/support/distribution review and compatibility tests; no technical benchmark against it was performed |
| RustFS | Another local S3-compatible candidate; adds a different operational/support surface without a demonstrated requirement here; no maturity or compatibility equivalence is assumed |
| Ceph | Broad distributed-storage capabilities, with an operational scope beyond a single-node portfolio milestone; cluster management is unnecessary here |
| SeaweedFS | Official single-process mini setup matches the requested local workflow; local upload/read-back, integrity, identical retries, and restart persistence verified with 4.48; broader S3 compatibility remains unverified |

These are scope/operational tradeoffs, not measured performance or production-maturity rankings. Single-node success would not establish production readiness of any candidate. The S3 protocol and AWS SDK offer industry-relevant experience without tying domain code to a vendor API.

A conditional-create policy could reduce check/write races, but compatibility is unverified and the MVP explicitly permits a single-writer precondition. A database, lock service, or transactional manifest system is disproportionate to today's scope. Inline payloads in JSON would obscure original source fidelity and add encoding overhead; separate binary payload plus metadata is easier to replay and inspect.

## Consequences and compatibility limits

No new retrieval IDs, parsing, normalization, scheduling, or cloud provisioning are introduced. Payload snapshots, metadata serialization, and concurrent writes consume memory; the Day 2 fetch bound does not constrain manually constructed RawFeed values. Read-back comparisons use bounded buffers. Consumer validation, retention, backups, recovery, and production IAM remain future work.

GET/PUT portability does not establish equal behavior across SeaweedFS and AWS. Validate signing, error codes (including NoSuchKey versus NoSuchBucket), checksum/chunking support, object visibility, credentials/IAM, durability, and retry outcomes before changing deployment targets. Local HTTP credentials and single-node storage are not production security or durability settings. No concurrency guarantee is claimed beyond the caller's single-writer discipline.

## Validation and follow-up

Unit tests exercise deterministic keys and metadata, byte preservation, known SHA-256 vectors, identity validation, retries, conflicts, partial writes, and uncertain outcomes. A loopback HTTP stub exercises the real SDK without a storage service. The opt-in integration profile uploads and reads synthetic objects and supports a read-only phase after restart. [Local setup](../local-seaweedfs.md) records exact commands, image provenance, persistence, and safe shutdown behavior.

SeaweedFS 4.48 integration and read-only restart verification passed on 2026-10-12, along with all 23 unit tests on each run. Evidence and environment details are recorded in [local setup](../local-seaweedfs.md#execution-status-and-portability). Amazon S3 compatibility has not been tested. Owner review must confirm the collision namespace, immutable-input/single-writer assumptions, and local deployment choice before this ADR becomes Accepted. Accepted ADR 0001 and the project charter are unchanged.
