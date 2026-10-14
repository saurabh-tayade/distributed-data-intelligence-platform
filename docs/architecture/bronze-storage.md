# Bronze object storage

## Implemented boundary

```text
RssSource -> HttpFeedFetcher -> RawFeed -> BronzeStorage -> S3BronzeStorage
                                                           |
                                                      AWS SDK v2
                                                           |
                                             local SeaweedFS S3 endpoint
                                                           |
                                               persistent named volume
                                                           |
                                        future Silver parsing/normalization
```

`BronzeStorage.write(RssSource, RawFeed)` validates matching identities, preserves bytes, and returns a `BronzeReceipt` containing bucket, payload/metadata keys, ingestion UUID, actual length, and SHA-256. The adapter takes an already configured `S3Client`; the caller owns its lifecycle. No bucket creation, polling, fetching, parsing, or batch orchestration occurs inside storage. Already fetched data may be archived even if its source is now disabled or its HTTP status is non-2xx. The storage caller decides whether to archive fetch-error responses.

Day 2 production contracts are unchanged. [ADR 0001](../adr/0001-rss-raw-ingestion-contract.md) remains the historical accepted retrieval decision. [ADR 0002](../adr/0002-bronze-object-storage.md) records the accepted storage contract and Maven migration.

## Object names and metadata

Default local bucket: `content-lake`. S3 keys form a flat namespace; slashes and partition labels are naming conventions, not directories.

```text
bronze/rss/source_id=<base64url-utf8-without-padding>/
  ingestion_date=<UTC-YYYY-MM-DD>/ingestion_id=<RawFeed UUID>/
    payload.xml
    metadata.json
```

`BronzeKeys` is independent of the storage vendor. Encoding is case-sensitive and injective over valid Unicode source IDs; unpaired surrogates and resulting keys over S3's 1024-byte limit are rejected. Source IDs cannot insert slashes, dot segments, or URL query syntax into the prefix. The date is derived from `RawFeed.ingestedAt` in UTC. Persistence never creates a new ingestion UUID.

The payload preserves exact bytes, including empty bodies, non-UTF-8 bytes, whitespace, and line endings. No compression, decompression, or XML validation occurs. `.xml` describes the expected format, not a validity guarantee. The object's content type is `application/octet-stream`; the upstream content type is preserved in metadata rather than trusted as an upload instruction.

`metadata.json` is UTF-8 JSON with stable field order produced by Jackson. Schema version 1 has these fields:

| Field | Representation |
| --- | --- |
| `schemaVersion` | Integer `1` |
| `ingestionId` | Original UUID string |
| `sourceId` | Original stable source ID |
| `sourceUri` | Configured URI string |
| `ingestedAt` | Original instant, UTC ISO-8601 |
| `httpStatus` | Original integer HTTP status |
| `contentType` | Original string or JSON null |
| `declaredContentLength` | Original header value or JSON null |
| `payloadByteLength` | Actual byte count, independent of the HTTP header |
| `payloadSha256` | Lowercase hex SHA-256 over exact payload bytes |
| `payloadKey` | Key of the payload in this bucket |

Supply only public, non-secret source URIs: the URI is persisted verbatim, including any query string. Storage credentials are never placed in metadata or receipts. SDK exceptions are preserved as causes; callers should sanitize diagnostics before external logging. No payloads, credentials, or endpoint configuration are logged by the adapter.

## Publication, retries, and conflicts

The caller must guarantee **one writer per ingestion identity**, with immutable source ID, ingestion instant, source URI, HTTP metadata, and payload for that identity. This is an application precondition, not a distributed lock. Identity lookup is scoped to the deterministic source/date/UUID key; reusing a UUID with a changed source/date escapes that lookup and violates the precondition. There is no global UUID index.

The adapter validates inputs, generates keys, takes one defensive snapshot of the payload, computes SHA-256, and serializes metadata before storage I/O. It checks existing metadata and payload with streaming byte comparisons, aborting the read on mismatch rather than draining a large conflicting object.

| Existing state | Behavior |
| --- | --- |
| Neither object exists | PUT payload, then PUT metadata, then return receipt |
| Both match expected bytes | Return the same receipt without another PUT |
| Matching payload, no metadata | Publish metadata to finish the interrupted write |
| Any existing object differs | Report `CONFLICT`; do not overwrite it |
| Matching metadata, missing payload | Report `CONFLICT`; do not silently repair a published record |
| Read fails or bucket is absent | Report persistence failure; do not treat it as a missing key |

Only S3 `NoSuchKey` with HTTP 404 is treated as absence. Byte-identical metadata is required: even semantically equivalent JSON with different formatting conflicts. This deliberately simple policy avoids permissive schema reconciliation. A payload-only orphan has no previously published provenance to compare; a retry must therefore retain the original input, not reconstruct different metadata around matching bytes.

Metadata-last publication is a logical completion convention, **not a multi-object transaction**. Consumers must require valid metadata and the referenced payload and verify length/hash. Payload PUT failure prevents metadata publication. Metadata PUT failure reports failure even though the payload may exist. Network failures can occur after the server stored an object: the outcome can be uncertain, including both objects existing after a reported failure. Retry the same immutable input to reconcile. Orphan cleanup is deferred.

`BronzeStorageException` reports `VALIDATION`, `CONFLICT`, or `PERSISTENCE`, plus the stage (`VALIDATE`, `SERIALIZE`, `CHECK_EXISTING`, `PAYLOAD`, `METADATA`). SDK/I/O causes are retained. No automatic storage-layer retry exists. `LocalS3` configures one SDK attempt per operation; caller-supplied clients may have different retry policies. Identical retries do not create additional keys, but this is not exactly-once delivery. New retrievals get new UUIDs and intentionally produce distinct records.

Concurrent writers can race between checks and PUTs. Conditional PUT, distributed locks, and transactions are not assumed or implemented. Out-of-band mutation/deletion can also invalidate a receipt. Restrict writes operationally for this MVP.

## Configuration, portability, and limits

`LocalS3` reads explicit `BRONZE_S3_*` environment values, requires a loopback HTTP endpoint with port, uses static local credentials, path-style addressing, fixed-length uploads, required-only SDK checksums, bounded request timeouts, and no ambient proxy configuration. It never falls back to the AWS credential provider chain or a default AWS endpoint. It does not create a bucket. See [local setup](../local-seaweedfs.md).

`S3BronzeStorage` uses standard GET and PUT operations, not SeaweedFS APIs. For eventual AWS validation, supply a separately configured SDK client with the approved region, credential provider, TLS endpoint/addressing, and checksum settings. The local factory intentionally cannot target AWS. AWS access is neither needed nor authorized by this change. Validate signing, checksum behavior, error codes, visibility, IAM, durability, and retry outcomes against the eventual service; SDK API portability is not proof of behavioral equivalence.

A RawFeed snapshot and metadata are buffered; the SDK receives a repeatable stream over that snapshot to avoid another full `RequestBody.fromBytes` copy. Existing-object comparisons use an 8 KiB buffer and stop at mismatch/excess length. Storage does not impose a new fetch-size policy on manually constructed RawFeed values. Copies, SDK buffers, metadata size, and concurrent writes still consume heap; the fetch limit is not a global memory budget. Read-before-write costs two GETs on retries. Synchronous writes, single-node durability, local credentials without production IAM, and absent cleanup/concurrency coordination are MVP limitations.

## Validation

`mvn -B -ntp -f ingestion/pom.xml verify` compiles Java 21, runs unit tests, and packages the module. `bash scripts/test-ingestion.sh` invokes the same Maven unit-test lifecycle. Tests need no storage server, cloud credentials, or public RSS feed; two suites bind loopback HTTP servers. Dependency bootstrap requires Maven Central access; subsequent cached runs can use `-o`.

JUnit discovers the existing Day 2 regression suite through one wrapper test; every existing assertion remains. Bronze tests cover schema, optional values, checksum, byte preservation, UTC keys, Unicode encoding, input validation, receipts, retries, conflicts, partial writes, uncertain outcomes, and error causes. A real-SDK loopback stub checks S3 HTTP requests but is **not** a SeaweedFS compatibility test.

The opt-in `seaweedfs-it` Failsafe profile performs real object upload/read-back and identical retries, with a unique source/UUID prefix and no deletions. A read-only mode verifies the same objects after manual container restart. SeaweedFS 4.48 integration and read-only verification after container restart **PASSED** on 2026-10-12, including payload byte equality, metadata, SHA-256, and identical retries. All 23 unit tests also passed on both runs. See [local validation evidence](../local-seaweedfs.md#execution-status-and-portability). Amazon S3 compatibility has not been tested.

The [Day 4 batch workflow](batch-ingestion.md) now connects retrieval to this boundary and records verified live publisher observations. Each new fetch keeps its own UUID; the runner writes only successful retrievals.
