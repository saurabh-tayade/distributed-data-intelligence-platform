# Gated Content submission and discovery

This is a metadata-only prerequisite of [ADR 0043](../adr/0043-integrated-original-content-pipeline.md).
Normal Content dispatch remains disabled. Discovery and HTTP 202 admission do not qualify installed
packages, files, source reachability, current artifact bytes, worker readiness or publication activation.
The original-run native executor must independently qualify the complete runtime before FETCH.

## Submission boundary

The existing `CONTENT_RSS_V1` request shape and canonical normalization are unchanged. If the selected
execution profile has a registration in `content_pipeline_profiles`, a new request must contain exactly
one `fetchSources` selection, an empty `retainedSilverArtifactIds` array and one qualified
`publisherReferenceArtifactId`. Unsupported selections fail before run/source/input rows or submission
audit events are created. Multiple strict registrations linked to one execution profile fail closed with
`STATE_CONFLICT`; no registry is chosen arbitrarily.

The restriction is not applied to historical profiles without that separate registration. In particular,
RunProfile v2 and historical runtime v2 retain their old meaning; runtime v2 is not redefined as ledger v2.
Idempotent replays still resolve before profile, source, capacity and coordinator admission checks and
retain the original frozen selection. Source locking, queue limits and global ownership are unchanged.

## Opt-in API candidates

The API role provides these read-only selections under the existing loopback Host/Origin boundary:

- `GET /api/management/v1/execution-profiles?pipeline=CONTENT_RSS_V1` returns registered narrow
  Content profile candidates. Omitting `pipeline` still selects only `RELIABILITY_V2`, preserving existing
  Reliability consumers. The explicit Reliability filter is also supported.
- `GET /api/management/v1/execution-profile-sources?executionProfileId=<uuid>` returns current enabled
  RSS source revisions whose reviewed endpoint identity/digest and Bronze binding match the catalog,
  selected profile and installation. Only strict Content candidates are supported by this endpoint.
- `GET /api/management/v1/artifacts?executionProfileId=<uuid>&role=PUBLISHER_REFERENCE` returns
  publisher-reference candidates. The existing Reliability roles are unchanged. `sourceId` is not
  accepted for publisher references; retained Silver discovery is not added.

Each endpoint accepts optional `limit` (1–100, default 20) and `cursor`. Unknown or duplicate query
parameters are rejected. Profile/artifact order is ascending UUID; source order is ASCII source ID.
One-row lookahead and signed 24-hour cursors bind endpoint, pipeline/profile and filters. These are live
pages, not a frozen submission snapshot. Empty pages mean no matching metadata candidates, not readiness.

Every Content profile, source and reference row explicitly includes:

- `dispatchDisabled: true`
- `qualificationScope: "REGISTRATION_METADATA_ONLY"`
- `runtimeQualification: "NOT_CHECKED"`

Content profile rows retain the existing safe profile fields, with `inputFormat: "RSS_ATOM"`, and add
`supportedSelection: {freshSourceCount: 1, retainedSilverCount: 0, publisherReferenceCount: 1}`. Only
canonical RunProfile v2 registrations linked unambiguously to strict-profile metadata and compatible
installation/storage membership are listed. The strict registry ID and raw configuration are not exposed.

Source rows contain only `executionProfileId`, `productId`, `pipeline`, `sourceId`, integer `revision`,
`sourceProfile`, `configurationSha256` and the three metadata-only markers. Endpoint URIs, resolved
storage, runtime paths, commands and credentials are absent.

Publisher references retain the existing safe artifact DTO, with Content product/kind/role, plus the
three metadata-only markers. They require a matching successful operator-import receipt and bounded
catalog selection, exact registered UUID/digest linkage, leaf dependency shape and the profile's
Reference binding. Historical verification time and native fingerprint do not establish current file
readability. Registrations without a matching receipt are omitted, with no private-proof fallback.

GETs never select `artifacts.evidence_bytes`, resolve storage configurations, open native files, launch
children, fetch feeds, lock the coordinator or add audit events. Rows are bounded by existing identifier
and DTO limits and pages by the existing response ceiling. The usual fixed error envelope, no-store
headers and sanitized failure codes apply. POST remains authoritative after any source, catalog,
profile, artifact, capacity or ownership change.

## Verification scope

`NarrowContentSubmissionDiscoveryIT` exercises the real PostgreSQL schema, strict registration and
HTTP controller/boundary/advice with no pipeline launch. It covers shape rejection, legacy v2 selection,
frozen replay, ambiguity, opt-in metadata, missing installed JAR, source gates/catalog, reference receipt
linkage, role isolation, query/cursor boundaries and SQL projection safety. Existing
`RunSubmissionIT` retains the broader idempotency, concurrency, queue, global-owner and Reliability
regressions. Native runtime execution and restart qualification remain separate required gates.

Populated upgrade fixtures explicitly opt into a test-only pre-V21 submission snapshot for their exact
old Flyway version. That helper retains genuine frozen selections, qualification evidence, links and
audit records while returning only the seed run identity/state. It creates no V21 tables and fabricates
no native evidence; the original Flyway upgrade and history-preservation assertions still run. Current
API/repository paths retain the required V21 schema and never fall back when tables are absent.
