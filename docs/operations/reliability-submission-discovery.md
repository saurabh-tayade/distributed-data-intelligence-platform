# Reliability submission discovery — E5-F3.2a

Two API-role GET endpoints project existing immutable registrations for a future Reliability
submission form. They support only `application-reliability` / `RELIABILITY_V2`. They do not
register, import, verify native data, submit runs, claim ownership or establish worker readiness.
The source-management `/capabilities` contract is unchanged.

Implementation: [controller][controller], [repository][repository], [cursor][cursor].
Existing [run admission][admission] remains authoritative and unchanged.

## HTTP and paging

| Method/path | Fixed query parameters |
| --- | --- |
| `GET /api/management/v1/execution-profiles` | Optional `limit`, `cursor`. |
| `GET /api/management/v1/artifacts` | Required `executionProfileId` (canonical UUID), `role` (`RELIABILITY_BATCH` or `SERVICE_REFERENCE`); optional exact `sourceId` for batches only, `limit`, `cursor`. |

Success is HTTP 200, JSON, `Cache-Control: no-store`:

```json
{
  "apiVersion": 1,
  "data": [],
  "meta": {
    "correlationId": "10000000-0000-4000-8000-000000000001",
    "limit": 20,
    "hasMore": false,
    "nextCursor": null
  }
}
```

The default limit is 20; allowed limits are integers 1–100. Unknown or duplicate parameters
are rejected. An empty collection is a successful empty page, not a readiness signal.
An unknown or metadata-incompatible profile ID yields an empty artifact page; a malformed ID
is a request error. There is no artifact-detail endpoint or inferred total.

Both collections use UUID ascending order, one-row lookahead and the existing HMAC signer.
Only follow returned opaque cursors. A cursor binds the endpoint, ordering and all selection
filters, expires after 24 hours and retains its original expiry across pages. Page size may
change within 1–100. Tampering, another endpoint/filter scope or expiry returns 400.
Restart without a cursor to refresh.

These are live keyset traversals, not a transactionally frozen or high-water snapshot. Newly
registered UUIDs before the current position are visible on refresh; later UUIDs may appear
in following pages. Source admission/revision or catalog changes can remove choices between
requests. Independent source, profile and artifact GETs do not constitute a submission snapshot.

## Execution-profile projection

Each row has exactly:

| Field | Meaning |
| --- | --- |
| `executionProfileId` | Immutable canonical UUID accepted by the run submission DTO. |
| `productId`, `pipeline` | `application-reliability`, `RELIABILITY_V2`. |
| `runProfileVersion`, `pipelineVersion` | Supported registered versions, both 1. |
| `sourceProfile` | `request-jsonl-v2` or `request-parquet-v1`. |
| `inputFormat` | Corresponding `JSONL` or `PARQUET`. |
| `configurationSha256` | Digest of the persisted canonical RunProfile configuration. |
| `createdAt` | UTC registration timestamp. |

The shared `execution_profiles` table also contains import/runtime descriptors. Those, Content
profiles, non-UUID IDs, unsupported versions and incompatible storage associations are omitted.
A selectable profile must have 1–128 associations in this installation, permitted product scope
(CONTROL may belong to another product), and all existing Reliability admission roles: BATCH,
REFERENCE, RELIABILITY, CONTROL, STAGING and TEMP.

Returned RunProfiles pass the existing strict `RunProfile` validation and canonical-byte/digest
checks. Runtime configuration, deployment commands and storage descriptors are never returned.
A purported supported registration with malformed configuration fails closed; it is not rendered
as usable or silently converted into an empty catalog. No approval/readiness state is invented.

## Artifact projection

Each row has exactly `artifactId`, `productId`, `kind`, `role`, `contractVersion`,
`processorRevision`, `nativeFormat`, `storageBindingId`, `nativeFingerprint`, `verifiedAt`,
and `identity`:

| Selection | Values and identity |
| --- | --- |
| Batch | Kind `RELIABILITY_BATCH_V2`, role `RELIABILITY_BATCH`, contract/processor 2/2; format matches the selected profile. Identity contains `sourceId`, integer `sourceRevision`, `batchId`, `inputFormat`, `registeredProfileSha256`. |
| Reference | Kind/role `SERVICE_REFERENCE`, contract/processor 1/1, `nativeFormat:null`. Identity contains `referenceId` and string `revision`. |

`artifactId` is the immutable registry UUID. `nativeFingerprint` is the fingerprint captured
at registration. `verifiedAt` is the historical registry verification timestamp. Neither is
fresh verification, a claim of currently readable files, execution readiness or Analytics activation.
Storage binding IDs are opaque references; resolved roots, addresses and credentials are absent.

Artifact choices require:

- Correct product, kind, versions, native format, leaf dependency shape and storage role.
- Binding membership in the selected profile and this installation.
- A matching successful operator-import catalog and completion receipt. The receipt must link the
  artifact UUID and registered evidence digest to the exact catalog selection; projection checks
  that selection against the immutable artifact row.
- For a batch, its captured source revision/profile digest/storage ID must match a current enabled
  source, the selected source profile and the current reviewed source catalog.

The bounded catalog/receipt metadata comes from `artifact_import_operations.request_bytes` and
`result_bytes` (each at most 64 KiB, at most 16 selections per import). **GET never selects
`artifacts.evidence_bytes`**, reads private proof packets, opens native files, contacts storage or
requalifies imports. Registry rows without a matching successful import receipt are omitted.
This includes manually provisioned or historical rows lacking that linkage even if a direct
submission could accept them. There is no fallback to private evidence or automatic backfill;
any operator re-import is a separate, deliberate use of the existing import procedure.

The API emits at most 100 rows and fetches one lookahead. Fixed DTO keys, UUID/digest syntax and
existing identity bounds (source 128, batch/reference 64, reference revision 32, storage token 128
ASCII characters) bound a profile row below 1 KiB and an artifact row below 2 KiB. Including the
512-character cursor and envelope, successful pages stay below 256 KiB; the existing 1 MiB
management response ceiling remains enforced. Existing database/transaction timeouts apply.
No global registry-size limit, indexing/performance claim or new migration is introduced.

## Errors and boundary

The existing fixed envelope is `{apiVersion,code,message,correlationId,retryable}`. Invalid
parameters/cursors return 400 `INVALID_ARGUMENT`; the unchanged Host/Origin boundary returns
403 `LOCAL_BOUNDARY_REJECTED`; database unavailability/timeouts return 503
`MANAGEMENT_UNAVAILABLE`; malformed purported-compatible registration metadata returns 500
`INTERNAL_FAILURE`. Failures are not successful empty pages. Private SQL/exception details and
raw metadata bodies are not exposed.

Only `ddip.role=api` instantiates these routes. Use the existing approved loopback Host/Origin
allowlist and same-origin Vite proxy. These GETs require no mutation headers, ETags or
idempotency keys. They do not lock the coordinator or create audit events, files or run records.

## F3.2b consuming workflow

1. Page execution profiles and retain the selected immutable UUID/digest and source profile.
2. Independently page both artifact roles using that profile UUID. Optionally narrow batches by
   exact source ID. Keep UUID selections separate from display labels and native fingerprints.
3. Use existing Sources/detail/revision GETs to review source admission and the exact revisions
   represented by selected batches. Select at least one batch per submitted source, within
   existing 1–32 source and 1–32 batch bounds; choose exactly one service reference.
4. Review the existing `RELIABILITY_V2` submission DTO. Any 202 from the existing POST means
   accepted/queued, not completed execution. Admission can reject an old discovery choice after
   a source/catalog/coordinator/capacity change. Do not silently replace revisions or retry with
   a new intent.

Implement loading, empty, unavailable and cursor-restart states, cancellation and stale-response
protection in that separate frontend milestone. There is no submission UI in F3.2a. The historical
[F1 audit][audit] records the earlier discovery gap; this document describes its implemented
Reliability follow-up without rewriting that audit.

[controller]: ../../services/content-api/src/main/java/platform/contentapi/management/DiscoveryController.java
[repository]: ../../services/content-api/src/main/java/platform/contentapi/management/DiscoveryRepository.java
[cursor]: ../../services/content-api/src/main/java/platform/contentapi/management/DiscoveryCursor.java
[admission]: ../../services/content-api/src/main/java/platform/contentapi/management/RunRepository.java
[audit]: e5-f1-operations-ui-audit.md
