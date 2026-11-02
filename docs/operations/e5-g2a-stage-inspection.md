# E5-G2a — Bounded named stage inspection

Backend contract for G2b. This read projects persisted operational evidence; it does
not perform fresh native verification. The earlier [E5-F1 audit](e5-f1-operations-ui-audit.md)
describes the historical gap this endpoint fills. No frontend, execution, recovery,
publication-format or schema changes are included.

## Endpoint and envelope

`GET /api/management/v1/runs/{runId}/attempts/{attemptId}/stages`

Both IDs must be canonical lowercase UUIDs. The attempt must belong to exactly that
run. Unknown runs, unknown attempts and attempt/run mismatches return `404 NOT_FOUND`;
malformed IDs return `400 INVALID_ARGUMENT`. An existing attempt with no stages
returns `200` with an empty array.

Optional query parameters are `limit` (default 20, range 1–100) and `cursor`.
Unknown or duplicate parameters, empty cursors and invalid limits return
`400 INVALID_ARGUMENT`. A subsequent page may use a different valid limit.

The existing management API-role requirement, loopback Host/Origin checks and
versioned success/error envelopes apply. GET requires no mutation headers. All
responses use `Cache-Control: no-store`. Successful pages have no ETag or total:

```json
{
  "apiVersion": 1,
  "data": [
    {
      "attemptId": "10000000-0000-4000-8000-000000000001",
      "stage": "CHECK_INPUTS",
      "itemOrdinal": 0,
      "checkpointId": "20000000-0000-4000-8000-000000000001",
      "state": "VERIFIED",
      "startedAt": "2026-11-01T10:00:00.123456Z",
      "finishedAt": "2026-11-01T10:00:01.654321Z",
      "outcomeCode": "VERIFIED",
      "counts": {"files": "2", "bytes": "100", "rows": "10"}
    }
  ],
  "meta": {
    "correlationId": "opaque",
    "limit": 20,
    "hasMore": false,
    "nextCursor": null
  }
}
```

Errors retain `{apiVersion,code,message,correlationId,retryable}` with fixed public
diagnostics. Database outages return `503 MANAGEMENT_UNAVAILABLE`, rather than an
empty stage collection. Unknown outcome classifications or malformed allowlisted
counts fail closed with `500 INTERNAL_FAILURE`. The shared 1 MiB response ceiling
also applies.

## Persisted identity and fields

The authoritative rows are `ddip_management.stage_invocations`, owned through
`attempts.run_id`. The primary key is `(attempt_id, stage, item_ordinal)`;
`checkpoint_id` is a separate immutable unique UUID. These are metadata identities,
not filesystem or object-store addresses. See the unchanged
[V3 schema](../../services/content-api/src/main/resources/db/migration/V3__runs_attempts_and_stages.sql)
and [V5 identity guards](../../services/content-api/src/main/resources/db/migration/V5__immutable_identity_and_version_guards.sql).

| DTO field | Meaning |
| --- | --- |
| `attemptId` | Actual owning attempt UUID, equal to the requested attempt. |
| `stage` | Persisted name: `CHECK_INPUTS`, `FETCH_SOURCE`, `STORE_BRONZE`, `SILVER`, `PREPARE_GOLD`, `PUBLISH_GOLD`, `PUBLISH_CONTENT`, `PUBLISH_RELIABILITY`, `VERIFY_RESULT`. |
| `itemOrdinal` | Persisted integer, 0–511; distinguishes items/recovery observations of the same named stage. |
| `checkpointId` | Persisted immutable UUID. It grants no native access or control capability. |
| `state` | Persisted `INTENT`, `RUNNING`, `VERIFIED`, `FAILED` or `UNKNOWN`. |
| `startedAt`, `finishedAt` | Persisted UTC timestamps with six fractional digits, or null. No inferred start, duration or finish. |
| `outcomeCode` | Nullable persisted code validated against the existing public Run History allowlist. This is independent of stage state; a recovery observation can be `VERIFIED` with `PUBLICATION_INVALID`. |
| `counts` | Null when absent; otherwise an object containing only reviewed counters, serialized as nonnegative signed-64-bit decimal strings. Empty objects are valid. |

Safe counter keys are `accepted`, `rejected`, `articles`, `occurrences`, `events`,
`windows`, `rows`, `files`, `bytes`, `gold_articles` and `gold_source_daily`.
Stage persistence uses both JSON integers (e.g. Content Silver/Gold) and decimal
strings (e.g. Reliability); the projection normalizes either without precision loss.
Other keys are omitted, never renamed or aggregated. A present safe key with an
invalid value rejects the response. Null means unavailable, not zero.

## Ordering, bounded reads and cursors

Rows are ordered by `(stage COLLATE "C" ASC, item_ordinal ASC)`, the persisted
identity within the requested attempt. This is binary stage-name ordering, **not
execution chronology** or a synthesized pipeline sequence. It remains stable when
timestamps are absent, equal or updated. Consumers should retain server order
during pagination; they can display the recorded timestamps separately.

The first page captures the maximum persisted `(stage, item_ordinal)` key.
Each page reads at most `limit + 1` rows for lookahead; no OFFSET, total-count query,
unbounded evidence fetch or per-stage query is used. The schema itself bounds one
attempt to nine names times 512 ordinals. Reads use ordinary metadata transactions,
without coordinator, attempt or stage ownership locks.

Cursors use the existing HMAC-SHA256 signer and canonical opaque token rules
(maximum 512 characters). They bind the endpoint, ordering, run ID, attempt ID,
high-water key, last returned key and original expiry. They expire after 24 hours;
pagination does not renew that lifetime. Invalid, tampered, expired, wrong-scope or
wrong-key cursors return `400 INVALID_ARGUMENT`. A cursor cannot be moved to another
attempt, even in the same run, or to an existing Run History endpoint. Retaining the
configured signing key preserves cursors across API restarts; an ephemeral key does
not. Treat tokens as opaque and URL-encode them.

As with other management history pages, this is not snapshot isolation. Keys above
the captured high-water are excluded. New keys below it can appear on later pages
if they sort after the cursor, or require a fresh traversal if already passed.
States, timestamps and counts can change while an attempt is active. Refresh from
the first page to inspect current persisted history.

## Historical ownership and safety limits

Original EXECUTE, resumed EXECUTE and RECONCILE attempts remain separate. Reused
stages stay with their original attempt; reconciliation verification rows stay with
the reconciliation attempt, including multiple `VERIFY_RESULT` item ordinals. This
endpoint neither merges inherited stages nor reads audit details to reconstruct them.
Existing run detail, attempt and event DTOs are unchanged.

There is no persisted stage creation sequence, display label, duration, progress
percentage or per-stage reuse pointer. `attempts.reused_checkpoint_id` exists at
attempt scope; this stage DTO does not relabel it as a per-stage reuse relationship
or expand private Content resume plans. No such relationship is synthesized.
Some stages persist no counts or timestamps, and native table-specific counters
outside the allowlist remain unavailable through this DTO.

The SQL projection excludes commands/arguments, paths/addresses, process/host guard
identity, raw proofs, private evidence, credentials and exception diagnostics. Counts
are parsed only from the bounded stage counter object, with an explicit key allowlist.
Reading performs no qualification, native I/O, verification, reprocessing, recovery,
artifact discovery or state transition. `VERIFIED` and outcome codes describe past
recorded observations, not present filesystem health or a new verification result.

## Implementation and verification scope

Implementation: [controller](../../services/content-api/src/main/java/platform/contentapi/management/RunController.java),
[repository](../../services/content-api/src/main/java/platform/contentapi/management/RunHistoryRepository.java),
[cursor](../../services/content-api/src/main/java/platform/contentapi/management/StagePageCursor.java),
[safe values](../../services/content-api/src/main/java/platform/contentapi/management/RunInspectionValues.java).
This uses existing persistence and signing facilities without dependencies or migrations.
Identity keysets avoid mutable timestamp ordering and audit-event reconstruction;
the tradeoff is that alphabetical order is not a native execution timeline.

The existing [RunHistoryIT](../../services/content-api/src/test/java/platform/contentapi/management/RunHistoryIT.java)
contains real PostgreSQL and loopback HTTP tests using its established SQL fixture
mechanisms. Completed Reliability rows are persisted metadata fixtures, not a new
worker shortcut or a fresh end-to-end native acceptance claim. Coverage includes
state/time fidelity, multiple attempts, resumed/reconciliation ownership, bounded
paging, cursor scope/expiry/tampering/restart, safe errors and field exclusion,
unchanged persisted records and reads while ownership rows are locked.
[RunInspectionTest](../../services/content-api/src/test/java/platform/contentapi/management/RunInspectionTest.java)
checks cursor validation and exact safe counter normalization.

`RunHistoryIT` remains in the existing PostgreSQL `core` partition. No integration
class or CI inventory change is required. Full Spark, Content recovery and Reliability
restart matrices are outside validation scope for this read projection. G2b can use
this contract for named stage names, states, times and available safe outcomes/counts;
it must tolerate nulls and preserve attempt ownership and pagination.

### Local validation — 2026-11-02

Final execution used Java 21.0.7 on macOS arm64 and an isolated loopback PostgreSQL
17.11 container (`postgres:17.11-alpine`). This is local validation, not hosted CI
or a performance benchmark. The test container and its volumes were removed afterward.

| Check | Final result |
| --- | --- |
| Focused units: RunInspection, DiscoveryCursor, RunCommands, SourceValidation, ControlCommands, RuntimeRole | 138 passed |
| RunHistoryIT | 69 passed |
| RunSubmissionIT | 77 passed |
| SourceManagementIT | 36 passed |
| ControlAdmissionIT | 52 passed |
| ContentResumeControlIT | 18 passed |
| PersistenceIT | 30 passed |
| CI policy and partition tests | 40 passed; exact 26-class inventory unchanged |
| Markdown lint and relative links; repository hygiene; `git diff --check` | Passed |
| V1–V9 migration byte comparison with HEAD | 9/9 identical |

All final Java suites had zero failures, errors or skips (138 unit and 282 integration
tests). An initial new key-rotation test fixture mistakenly reused the original key;
it was corrected. Earlier persistence runs used Maven's Java 27 selection and failed
the existing worker runtime guard. The final complete run explicitly selected Java 21
and the supported `DDIP_TEST_PYTHON` environment setting. No production runtime guard
or existing persistence test was changed.

With `JAVA_HOME` pointing to Java 21, `DDIP_TEST_PYTHON` to an existing executable,
and the three `DDIP_TEST_DB_*` variables set for a dedicated local test database,
the final Maven command was:

```sh
mvn -o -B -ntp -Daether.syncContext.named.factory=noop \
  -f services/content-api/pom.xml verify -Ppostgres \
  -Dtest=RunInspectionTest,DiscoveryCursorTest,RunCommandsTest,SourceValidationTest,ControlCommandsTest,RuntimeRoleTest \
  -Dit.test=RunHistoryIT,RunSubmissionIT,SourceManagementIT,ControlAdmissionIT,ContentResumeControlIT,PersistenceIT
python3 -m unittest discover -s .github/ci/tests -v
python3 .github/ci/postgres_partitions.py
npm run check
git diff --check
```

Offline Maven used cached dependencies; the no-op named-lock setting accommodates
the local read-only dependency cache. Neither setting changes repository configuration.
