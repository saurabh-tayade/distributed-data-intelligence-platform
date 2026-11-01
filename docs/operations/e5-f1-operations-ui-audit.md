# E5-F1 — Operations UI workflow and API contract audit

Documentation and planning only. Baseline: `d6cd8c0` (HEAD, local `main` and cached
`origin/main` matched; no remote refresh). Findings below come from implementation reads;
existing tests were inspected, not executed. Proposed UI work and API additions are not
accepted architectural decisions or implemented capabilities.

## 1. Verified current state

| Capability | Status and implementation evidence |
| --- | --- |
| Analytics Workspace | **Implemented:** [App][app] offers Publisher Overview, Daily Activity, Data Quality and Article Explorer. Views are React state/buttons, not URL routes; three views load lazily. [Client][analytics-api] consumes only `/api/v1/{publishers,articles,daily,quality}`. |
| Existing UX boundaries | **Implemented:** [useAnalytics][analytics-hook] pins one Gold publication, aborts obsolete requests and ignores late responses. Article pages use offsets; other reports expose bounded rows. [AppShell][shell], [states][states], [timestamps][timestamp] and [styles][styles] supply accessible tables, focus, UTC display, responsive layout and reduced motion. State-panel copy and errors are analytics-specific. |
| Sources and revisions | **Implemented:** [SourceController][sc], [SourceRepository][sr], [SourceCommands][scommands] expose catalog reads and conditional source mutations. Revisions are immutable; `enabled` is the live admission gate. There is no per-revision approval-status field or approval action. |
| Imports and artifact registry | **Partially Implemented for UI:** [operator import][import-role] and [ImportCatalog][import-catalog] perform trusted non-web import/qualification; artifacts and dependencies are persisted. No artifact list/detail/import HTTP controller exists. Native import catalog files are not an HTTP catalog API. |
| Submission and execution | **Partially Implemented:** [RunController][rc] admits both pipeline DTOs; [RunRepository][rr] freezes selections transactionally. [Packaged worker capability][worker] supports only `RELIABILITY_V2`. Content native adapters and explicit operator continuation exist, but ordinary Content dispatch remains blocked. `202` does not activate it. |
| Execution inspection | **Implemented:** [RunHistoryRepository][history] projects runs, detail, attempts and audit events without native I/O or ownership locks. It exposes `currentStage` and attempt `stageCount`, not stage rows. Full frozen configuration/evidence is deliberately absent. |
| Recovery requests versus execution | **Partially Implemented for UI:** [controls][cc] admit only RECONCILE/RESUME and read individual controls. [Operator recovery][recovery-role] separately performs guarded inspection, fresh confirmation, verification and disposition; it is non-web. |
| Operations navigation/pages/client | **Planned:** no management fetches, Operations pages, action forms or router in the React application. This audit supplies the first implementation slices. |
| Activation, cancel/retry execution, automatic recovery | **Deferred/blocked:** no public CANCEL/RETRY admission in the parser; no automatic recovery/resume, artifact activation or Content enable control. Persisted legacy states/actions do not prove executable HTTP controls. |

Evidence priorities: [durable execution][durable] records current and historical checkpoints;
[ADR 0010][adr10] and [0011][adr11] are accepted boundaries. [0014][adr14], [0019][adr19]
and [0020][adr20] describe implemented operator protocols with acceptance limitations.
[ADR 0021][adr21] remains Proposed; [current producer evidence][producers] leaves the native
proof/packet envelope unresolved. Earlier README/ADR status paragraphs are historical, not API discovery.
Preserve the accepted [analytics boundary][adr5] and existing [dashboard conventions][frontend-guide].

Regression anchors inspected: [Analytics UI races/navigation][ui-tests], [analytics wire validation][wire-tests],
[source management][source-tests], [run history/projection/cursors][history-tests],
[reconciliation admission][control-tests] and [RESUME admission][resume-tests]. These source tests
are evidence of intended assertions, not a new passing runtime result from this audit.

## 2. API-to-UI contract matrix

All paths below start with **`/api/management/v1`**. “Usable” means an implementation exists
in a configured local API installation; this audit made no live HTTP requests.
DTOs are manually constructed JSON objects, not generated OpenAPI types.

### Shared wire, deployment and concurrency rules

- [SourceResponses][responses]: success `{apiVersion:1,data,meta:{correlationId}}`; pages add
  `meta:{limit,hasMore,nextCursor}`. There is no page total or `returned` field. Responses are
  `application/json`, `Cache-Control: no-store`, at most 1 MiB. Error responses instead contain
  `{apiVersion:1,code,message,correlationId,retryable}`.
- [ManagementConfiguration][config] enables controllers only in `ddip.role=api`; default
  `analytics` mode has no management routes. [RuntimeRole][role] permits analytics in `api`
  mode too: one existing Spring application can serve both areas, with PostgreSQL configured.
- [ManagementBoundary][boundary] validates exact Host and optional Origin against loopback
  configuration; OPTIONS is rejected. Mutations additionally require an allowed Origin,
  `X-DDIP-Local-Request: 1`, JSON/UTF-8, no Content-Encoding, and a body at most 64 KiB.
  Headers must not be duplicated. This is a trusted local-operator boundary, not login/authentication.
- The existing [Vite proxy][vite] forwards `/api/` with `changeOrigin:false`. For development,
  allow the actual UI authority/origin, e.g. `http://127.0.0.1:5173`, as well as the API origin
  in `ddip.management.origins`; preview uses 4173. Default API-only origin configuration
  otherwise rejects proxied management GETs too. Use same-origin relative requests, not new CORS rules.
- All paginated lists default to 20, accept `limit=1..100`, and reject unknown/duplicate query parameters.
  IDs are canonical UUIDs where specified; source IDs match `[a-z0-9][a-z0-9_-]{0,127}`;
  product IDs are `content-intelligence` or `application-reliability`.
- Row versions, 64-bit counts and event sequences are decimal strings; preserve them without Number coercion.
  Revisions, attempt numbers, limits and contract/processor versions remain JSON numbers.
  Times are UTC strings with six fractional digits; retain nulls and original precision.
  ETags are quoted decimal row versions, e.g. `"12"`; no conditional-GET/304 behavior is implemented.
- Every mutation requires an `Idempotency-Key` matching `[A-Za-z0-9_-]{1,128}`. Keep one key,
  exact body and original precondition per user intent when resolving an ambiguous response.
  A changed body/version needs a new intent/key. Exact replays precede current-state eligibility
  checks; source commands return their saved response, whereas run/control replays return current summaries.

### Read-only HTTP contracts

| Method/path and source | Parameters and returned `data` | Identity, limits and consuming view |
| --- | --- | --- |
| GET `/capabilities` — [SC][sc], [ReviewedSourceCatalog.capabilities][catalog] | No query. `scope:"SOURCE_MANAGEMENT_ONLY"`, `profiles`, `endpoints[{endpointId,sourceId}]`, `reliabilityProfiles[{sourceId,profile,dataOrigin,registeredProfileSha256}]`, `storageProfiles[{profileId,productId,role}]`, `maxSources:128`, `maxRevisionsPerSource:1000`. | Usable for source forms/reviewed choices. Arrays each bounded at 128 where catalog-backed; endpoint URIs/secrets are omitted. This is neither worker capability nor an execution-profile/artifact selector. |
| GET `/sources` — [SC][sc], [SR.sources/map][sr] | Optional `productId,enabled=true\|false,limit,cursor`. Rows: `productId,sourceId,displayName,enabled,currentRevision,rowVersion,configSha256,createdAt,updatedAt`; list omits `config`. | Usable Sources list. Composite `(productId,sourceId)` key. Binary product/source ascending; signed high-water cursor binds filters and excludes new sources. Live fields/filter membership can change during traversal. |
| GET `/sources/{product}/{source}` — [SC][sc], [SR.get/map][sr] | No query. Source list fields plus `config` described below; response ETag. | Usable Source detail. `enabled` and `currentRevision` are current, not historical run selections. No separate approval/health flag. |
| GET `/sources/{product}/{source}/revisions` — [SC][sc], [SR.revisions][sr] | `limit,cursor`. Rows: `productId,sourceId,revision,displayName,configSha256,createdAt,config`. | Usable revision panel, ascending revision with first-page high-water. No single-revision HTTP route; find an older revision through explicit pages. Parent source may have advanced. |
| GET `/runs` — [RC][rc], [History.runs/summary][history] | Optional `productId,sourceId,state,limit,cursor`. Summary: `runId,productId,pipeline,state,currentStage,rowVersion,requestedAt,finishedAt,outcomeCode,retryEligibility,result`; Reliability additionally has `dataOrigin:"synthetic"`. | Usable Executions list, newest submission first. `sourceId` matches selected `run_sources`, not all retained-artifact provenance. No pipeline/date/text/order filter. `result` is null or `{artifactId,nativeFingerprint,productContractVersion,processorRevision,verificationStatus:"VERIFIED_AT_EXECUTION",verifiedAt}`. |
| GET `/runs/{id}` — [RC][rc], [History.detail][history] | No query. Summary plus nullable `counts`, `attempts:{count,currentAttemptId}`, nullable `latestControl:{controlId,type,state,reason,requestedAt,appliedAt,rowVersion}`, `sourceRevisions:[{sourceId,revision}]`, `inputArtifactIds:string[]`; ETag. | Usable Execution detail. Sources bounded at 8 Content/32 Reliability; input IDs at 129. These are captured references, not full frozen configuration, artifact roles or proof bodies. `counts` allows only accepted/rejected/articles/occurrences/events/windows/rows/files/bytes, all strings. |
| GET `/runs/{id}/attempts` — [RC][rc], [History.attempts][history] | `limit,cursor`. Rows: `attemptId,attemptNo,kind,state,startedAt,finishedAt,outcomeCode,stageCount`. | Usable Attempts panel; attempt number ascending. Kind EXECUTE/RECONCILE; no per-attempt ETag, stage names or individual-stage status. Do not merge inherited attempts. |
| GET `/runs/{id}/events` — [RC][rc], [History.events][history] | `limit,cursor`. Rows: `eventSeq,timestamp,action,beforeState,afterState,code,beforeVersion,afterVersion,context:{attemptId,controlId}`. | Usable audit timeline; preserve server sequence order and decimal strings (BigInt if comparing, never lexicographic sorting). No `stage`, checkpoint/item ID or private `details_bytes`. STAGE_* events therefore cannot be assigned to a named stage reliably. |
| GET `/runs/{run}/controls/{control}` — [CC][cc], [ControlRepository.summary][controls] | No query. `controlId,runId,type,state,reason,requestedAt,appliedAt,rowVersion,runVersion`; ETag represents the control row. | Usable once an ID is known from latestControl/events/admission. No control-list endpoint. Control type may historically be CANCEL/RETRY despite those actions not being admitted now. A subsequent mutation requires the run ETag, not this control ETag. |

[SourceCursor][source-cursor] signs opaque tokens at most 512 characters. Source/revision tokens
have no implemented expiry. [RunPageCursor][run-cursor] adds a fixed 24-hour lifetime and binds
endpoint, filters/run and order, preserving expiry across pages. All traversals use high-water
membership, not snapshot isolation of mutable values; signing-key changes invalidate tokens.
The default random signing key is process-local, so restart continuity needs a configured stable key.
Page size is not bound into the token, but the UI should reset traversal when it changes.

### Mutating HTTP contracts

Source `config` has exactly `configVersion:1,profile,dataOrigin,feedEndpointId,
registeredProfileSha256,inputStorageProfileId`. Content requires `rss-atom-v1`, `publisher`,
endpoint UUID and null profile hash. Reliability requires `request-jsonl-v2` or
`request-parquet-v1`, `synthetic`, null endpoint and a registered SHA-256. Input storage is
a UUID reviewed for that product; these are references, not arbitrary URLs/paths/uploads.

| Method/path and controller/DTO | Exact request, result and safeguards | Usability and proposed action |
| --- | --- | --- |
| POST `/sources` — [SC][sc], [SourceCommands][scommands], [SR.execute][sr] | `{productId,sourceId,displayName,enabled,config}`; forbid If-Match. Name nonblank, at most 128 UTF-8 bytes, no controls. Returns 201 source detail + ETag. | Usable F3 source registration, subject to reviewed catalog/storage membership and 128-source capacity. Coordinator-first transaction serializes mutation; it does not launch ingestion or claim worker ownership. |
| POST `/sources/{product}/{source}/revisions` — same sources | `{displayName,config}`; require current source If-Match. Returns 200 detail + ETag. Same profile family only; unchanged configuration/name is a no-op, otherwise immutable revision increments, max 1000. | Usable F3 source edit. Cannot rewrite a revision or existing run selection; no approval/publish/delete action. |
| PUT `/sources/{product}/{source}/enabled` — same sources | `{enabled:boolean}`; require source If-Match; 200 detail + ETag, no-op supported. | Usable F3 enable/disable admission gate. Disabling is not cancellation or termination of active processing. |
| POST `/runs` — [RC][rc], [RunCommands][rcommands], [RR.submit][rr] | Content: `{pipeline:"CONTENT_RSS_V1",executionProfileId,fetchSources:[{sourceId,revision}],retainedSilverArtifactIds:[],publisherReferenceArtifactId}`. Reliability: `{pipeline:"RELIABILITY_V2",executionProfileId,sources:[{sourceId,revision}],batchArtifactIds:[],serviceReferenceArtifactId}`. All fields mandatory; forbid If-Match. Returns 202 current run detail, ETag and Location. | Metadata admission exists for both; F3 executable UI scope is Reliability only. Content accepts 1–8 fresh sources/0–128 retained IDs; Reliability 1–32 sources/1–32 batch IDs; one reference UUID, unique selections. Requires current enabled revisions, matching registered immutable execution/storage/artifact evidence. Coordinator READY/readiness and queue capacity (20 unfinished) checked under lock; a healthy occupied execution may coexist with queued admission. No native processing in HTTP. Missing artifact/execution-profile discovery blocks a friendly selection form. |
| POST `/runs/{run}/controls` — [CC][cc], [ControlCommands][ccommands], [ControlRepository][controls], [ResumeControlRepository][resume-controls] | Require run If-Match. RECONCILE: `{action:"RECONCILE",attemptId:<current UUID>,quiescenceConfirmed:true}`. RESUME: `{action:"RESUME",attemptId:null,quiescenceConfirmed:null}`. Returns 202 control summary, control ETag and Location. | Intent only. RECONCILE requires exact run/current occupied attempt/owner epoch/install bindings, eligible states, no active import or duplicate pending reconciliation; live/unknown recorded processes veto admission. RESUME requires a Content PAUSED run, supported recovered checkpoint, idle READY coordinator, enabled sources, valid bindings/evidence and capacity. Separate operator qualification applies either request. UI cannot perform handoff, prove quiescence or execute continuation. |

RECONCILE does not clear the occupied coordinator. RESUME only requests continuation; the
non-web operator must qualify and requeue, then explicitly execute that exact queued run.
Supported Content boundaries are original pre-fetch without retained Silver, committed Gold,
and verified Content CHECK_INPUTS; not journal/Bronze/Silver/partial-source or recursive RESUME
([ContentResumePlan][resume-plan]). Server rejection remains authoritative; no HTTP eligibility
or coordinator-inspection endpoint lets the UI pre-authorize these actions.

### Safe errors and absent HTTP contracts

[SourceFailure][errors] and [SourceAdvice][advice] define request failures separately from
persisted run outcomes:

| HTTP | Public codes and UI response |
| --- | --- |
| 400 / 403 / 404 | `INVALID_ARGUMENT` / `LOCAL_BOUNDARY_REJECTED` / `NOT_FOUND`: correct input or local configuration; do not show empty success. Cursor failures also use INVALID_ARGUMENT. |
| 409 | `IDEMPOTENCY_CONFLICT`, `SOURCE_DISABLED`, `STATE_CONFLICT`, `RESUME_CHECKPOINT_UNSUPPORTED`, `RESUME_EVIDENCE_INVALID`, `RESUME_NAMESPACE_UNSAFE`: retain entered intent, refresh relevant reads, show safe reason; never substitute a new request automatically. |
| 412 / 428 | `PRECONDITION_FAILED` / `PRECONDITION_REQUIRED`: refresh source/run ETag and ask for a new deliberate submission; no blind rebase/retry. |
| 429 / 500 | `MANAGEMENT_CAPACITY_EXCEEDED` / `INTERNAL_FAILURE`: explicit unavailable/error state, not invented zero rows. |
| 503 | `COORDINATOR_RECOVERY_REQUIRED`, `RESUME_EVIDENCE_UNAVAILABLE`, `MANAGEMENT_UNAVAILABLE`: retryable request classification, not permission to retry a run or perform recovery. |

Render only allowlisted classifications and correlation IDs, never proxy HTML, raw response bodies,
private audit details, SQL, paths or exception text. [RunInspectionValues][values] is the source of
safe outcome/action/count vocabularies; unknown persisted classifications fail closed server-side.

**No HTTP endpoint/DTO was found** for artifact catalog/detail/import/dependencies, execution-profile
discovery, stage list/detail, full frozen selection, owner/process inspection, recovery qualification,
handoff/finalization, artifact activation, CANCEL or RETRY admission. Do not invent routes from
architecture tables. The [operator import role][import-role], [recovery role][recovery-role] and
private [FrozenRunSelection][frozen] are not browser APIs.

## 3. Integrated navigation and minimal component structure

One React application, existing styling/dependencies. Proposed hash navigation avoids a router
dependency and server rewrite requirement; support direct entry, browser Back/Forward and unknown-route fallback.

```text
Analytics (#/analytics): preserve all four existing views, filters and publication pin
Operations
  Sources (#/operations/sources): product/enabled filters; bounded source table
    Source detail (#/operations/sources/:product/:source): current config + revision pages
      Related executions: runs filtered by productId/sourceId
  Executions (#/operations/executions): product/source/state filters; newest-first pages
    Run detail (#/operations/executions/:runId)
      Identity/outcome, captured source revisions/input IDs, attempts, audit events, latest control
      Named stage timeline and resolved artifact records: gated on new read contracts
```

Likely structure: extend `App.tsx`/`AppShell.tsx` with area navigation, area-appropriate skip-link/footer
copy and a small `navigation.ts`;
extract existing analytics composition into `AnalyticsWorkspace.tsx` without changing its contract.
Keep Analytics state/pin across area switches; suspend hidden-area requests and ensure an analytics
publication-change latch cannot block Operations. Add `managementApi.ts` (typed parsers/transport,
including response status/ETag/Location), `operations/useManagementQuery.ts`,
`SourceList.tsx`, `SourceDetail.tsx`, `RunList.tsx`, `RunDetail.tsx` and small
`CursorPager`, `OperationalStatus`, `AttemptList`, `EventTimeline` components as needed.
Add management-specific state panels; existing loading copy promises Gold verification.
Reuse Timestamp/table/focus/layout conventions, not analytical counters/charts as operational metrics.

Source detail shows current revision and enabled state, plus immutable revision configuration.
Capabilities provide reviewed choices, not a new “Approved” status. Run links include captured
revision numbers; never replace them with the latest revision. Until artifact reads exist, render
artifact UUIDs/result fingerprints as selectable text with “record details unavailable”, not broken links.

## 4. UX state, pagination and concurrency rules

- **Read lifecycle:** loading, successful empty list, ready page, and explicit error are distinct.
  A detail 404 is “record not found”. Independent attempts/events panels may fail while run detail
  remains visible; mark partial results and offer panel-specific read refresh, not overall success.
- **Stale work:** use AbortController plus request-key/generation checks as in useAnalytics.
  Clear incompatible old results synchronously when route/filter changes; late responses must not
  overwrite the current selection. Preserve last successful same-key data during a failed poll only
  with a visible stale timestamp/error. Abort on unmount and pause when the document/area is hidden.
- **Bounded pages:** default 20, maximum 100; follow only returned nextCursor. Keep a small cursor
  stack for Back/Next, not numeric page counts. Reset on filter/run/limit changes. Show “more
  results available”; do not imply a complete inventory from one page or fetch every page silently.
  Bad/expired cursors offer an explicit restart from page one. Refresh starts a new high-water
  traversal; old event cursors cannot discover newly appended events.
- **Polling proposal:** active detail states QUEUED/RUNNING/CANCEL_REQUESTED may refresh every
  five seconds after the previous request settles, with one in-flight request per panel. Do not
  poll historical list pages. Stop automatic run polling at SUCCEEDED/FAILED/CANCELLED; PAUSED and
  RECOVERY_REQUIRED show “operator action required” with manual refresh. A known REQUESTED control
  may be polled while visible; refresh paged attempts/events explicitly rather than mixing snapshots.
  GET failures may back off to 30 seconds with a visible error; never auto-submit mutations.
- **State domains:** run states are PAUSED/QUEUED/RUNNING/CANCEL_REQUESTED/SUCCEEDED/FAILED/CANCELLED/
  RECOVERY_REQUIRED. Attempt states are CLAIMED/RUNNING/SUCCEEDED/FAILED/CANCELLED/INTERRUPTED;
  stage states are INTENT/RUNNING/VERIFIED/FAILED/UNKNOWN, but rows are not exposed. Controls use
  REQUESTED/APPLIED/TOO_LATE/REJECTED. “Accepted” describes HTTP admission, not another run state.
  Audit transitions have their own domain; display action, before/after and attempt context, not
  synthetic stage names, percentages or inferred stage durations.
- **202 and writes:** show the returned persisted state and follow Location; an idempotent replay
  may already be terminal. Show REQUESTED separately from APPLIED and completed processing. Disable
  duplicate submit while pending; ambiguous outcomes retain the exact key/body/precondition for
  explicit resolution. A mutation retry is not run RETRY. Never show a Content “Run now” or activate
  a publication because submission succeeded or `retryEligibility` is true.
- **History and accessibility:** `VERIFIED_AT_EXECUTION` is historical verification, not a fresh
  integrity check or Analytics activation. Preserve distinct attempts/inherited records. Use semantic
  tables, captions and labeled controls; aria-current navigation, aria-busy/live status, focus on
  route changes, keyboard paging, text status labels and reduced motion. At narrow widths stack
  summaries, wrap IDs and provide focusable horizontal tables. Disabled actions explain the specific
  missing capability; no unlabeled gray icons or automatic process/ownership assertions.

## 5. Backend gaps, separately scoped

These are proposed future backend tasks, not changes authorized by this audit. None blocks Sources
or basic run/attempt/event browsing.

| Gap | Minimum follow-up and acceptance boundary |
| --- | --- |
| G1 — Artifact and execution-profile discovery | Bounded read-only catalog/detail projections for compatible registered artifacts, known source/dependency relationships and approved execution-profile IDs. Fixed safe fields: IDs, kind/format/versions, fingerprints, verification time and role/relationship IDs. No raw files, private evidence or credentials; no implicit requalification/import. Required for artifact pages and a selectable Reliability submission form. |
| G2 — Named stage inspection | Bounded run/attempt-scoped stage projection: stage, itemOrdinal, checkpointId, state, startedAt/finishedAt, safe outcome/counts and historical reuse reference where present. Verify run/attempt ownership, paging and scope; retain each stage's actual attempt. Exclude command/result addresses, PIDs and proof payloads. Required before claiming a complete named stage timeline; audit events alone cannot supply it. |
| G3 — Frozen-selection summary | Project captured source/configuration hashes, input artifact roles/ordinals, execution-profile ID/digest and selection digest from existing evidence with bounded safe DTOs. Do not expose the full private captured_selection. Required for richer provenance comparison; existing revision/UUID references suffice for the first detail page. |

Operator-only import, process inspection and challenge-driven recovery should remain in their
existing non-web roles. No browser recovery executor or new ownership abstraction is proposed.
Structured eligibility/readiness is absent; future action forms must treat server admission as
authoritative, not infer readiness from `/capabilities`, run status or an absent PID.

## 6. Independently testable implementation tasks

Estimates are Codex active execution ranges including focused mocked tests/checks, assuming cached
dependencies and prepared local fixtures; they exclude owner review, hosted CI and environment setup.
They are planning estimates, not measured delivery times. New filenames below are proposals.

| Task / scope and likely files | Dependencies | Acceptance and test strategy | Estimate; risks/exclusions |
| --- | --- | --- | --- |
| **F2a.1 — Foundation + Sources list:** App/AppShell/navigation, managementApi/query hook, SourceList, management fixtures/tests and styles. | Existing source GET; API-role/proxy configuration above. | Analytics/Operations navigation with Back/Forward, preserved analytics pin, source product/enabled filters and bounded paging. Loading/empty/403/503/invalid-envelope states. Vitest/RTL mock transport, ignored abort/late response, strict decimal strings, cursor reset; existing Analytics tests plus typecheck/lint/build pass. | **60–100 min.** First demonstrable slice. No backend additions, action forms or framework dependency. |
| **F2a.2 — Source detail/revisions:** SourceDetail, CursorPager, Timestamp reuse; related-run link. | F2a.1. | Current source + separately paged immutable revisions; 404/partial panel errors; enabled distinct from approval; captured revision not silently replaced. Tests for old revision/current changes, exact query bounds and stale selection. | **35–60 min.** No artifact relationship claim without G1; no arbitrary feed URL editor. |
| **F2b.1 — Execution list:** RunList, operational status/filter components, managementApi parsers/tests. | F2a.1; existing runs GET. | Exact product/source/state filters, timestamps, synthetic Reliability label, next/back pages, null result/stage support. Tests for all run states, high-water refresh/reset, 64-bit values and safe errors. | **45–75 min.** No date/pipeline/free-text filters or cancel/retry buttons. |
| **F2b.2 — Run inspection:** RunDetail, AttemptList, EventTimeline, captured references and latest-control panel. | F2b.1; existing detail/attempt/event/control GETs. | Independent bounded panels, ordered attemptNo/eventSeq, safe classifications, run ETag retained, five-second visible active polling and cancellation cleanup. Tests: accepted versus completed, failed/paused/recovery-required, REQUESTED control, partial failures, >JS-safe sequences, hidden tab and late poll. | **60–100 min.** Delivers an audit timeline; named stages depend on G2, resolved artifacts on G1, richer frozen selection on G3. Never fabricate missing stage/proof data. |
| **F2b.3 — Complete stage/related-record views:** StageTimeline and ArtifactDetail after backend follow-ups. | G1/G2 separately implemented and tested; G3 for full frozen-input summary. | Contract fixtures from new safe DTOs; tests for attempt scoping, inherited links, null times, pagination and immutable historical identity. Browser links resolve real records. Backend contract tests must prove no private evidence disclosure. | **45–90 min frontend**, backend gaps separately estimated. Excluded from the first read-only demo; no guessed endpoints. |
| **F3.1 — Source actions:** SourceForm/SourceGateAction and mutation client/tests. | F2a.2; capabilities and reviewed local catalog. | Exact CREATE/REVISE/ENABLE bodies and required headers; human confirmation, idempotent ambiguous-response resolution, 412 refresh/new-intent flow and saved-response refetch. RTL fetch assertions, no-op/conflict/capacity/disabled-source cases; existing API contract tests remain applicable. | **60–100 min.** No approval endpoint, revision rewrite/delete, active-run cancellation or secret/path inputs. |
| **F3.2 — Reliability submission:** RunSubmission form, bounded selectors and receipt navigation. | G1 artifact/profile discovery, F2b detail; pre-registered synthetic inputs and worker deployment. | Only RELIABILITY_V2 in the executable form; exact source revisions, batch/reference/profile IDs and bounds. Tests for 202 Location/ETag, terminal replay, idempotency conflict, coordinator 503 and no automatic retry. Demonstrate against prepared local Reliability fixtures. | **60–100 min frontend**, after discovery DTOs. Manual operator-supplied UUID entry is a smaller optional demo, explicitly not a catalog picker. No Content submission/activation or import/upload implementation. |
| **F3.3 — Advanced recovery-request admission:** RecoveryRequest form/control receipt, linked operator procedure. | F2b.2, human local review and reviewed operational availability. | RECONCILE uses current attempt/run ETag and explicit reviewed assertion; requests no takeover. Design RESUME's exact null envelope for supported historical Content checkpoints; PAUSED alone never enables it. Tests for stale owner/version, live-process veto, unsupported checkpoint, 202 REQUESTED and manual control refresh. | **60–100 min.** Offer requests only; actual qualification/requeue/execute remain operator CLI. Keep Content RESUME execution outside this UI milestone and Gate B0 activation blocked. No prechecked quiescence, process kill, automatic retry/cancel/resume or browser-generated challenge approval. |

Backend planning allowances, only if separately authorized: G1 **90–150 min**, G2 **60–100 min**,
G3 **45–90 min**, each with DTO/privacy/pagination and focused PostgreSQL contract tests. Do not
bundle these gaps into F2a. Start with F2a.1, then F2a.2 and F2b.1/2; action work follows read-only acceptance.

## 7. Suggested local browser acceptance and audit validation

Future browser acceptance, not performed in this documentation task:

1. Start an already configured local `api` role with compatible PostgreSQL, reviewed catalog and
   explicit UI/API origins; use existing Vite proxy. Keep the selected Analytics Gold publication.
   Switch areas and Back/Forward; Analytics filters/pin remain stable and Operations works even if
   analytics is unavailable. In analytics-only mode, explain management unavailability without empty data.
2. Browse pre-registered sources across two pages; filter, open detail/revisions, and show an old
   captured revision separately from current. Exercise empty catalog, unknown source, 403 and database
   outage states. No ingestion request occurs during browsing.
3. Browse persisted Reliability and historical Content runs; inspect null/verified results, exact
   large counters and UTC precision. Open attempts/events, page explicitly and refresh to see new
   events. A fresh high-water traversal must not duplicate or merge old pages.
4. Delay a response, switch source/run/filter, then let it finish; it must not replace the active
   view. Hide the tab, confirm polling stops, return and refresh. Keyboard-only navigation and narrow
   layouts retain labels, focus, readable statuses and ID access.
5. In F3 only, demonstrate source ETag conflict and idempotent response recovery on disposable
   approved records. A Reliability POST returns acceptance before work completes. A recovery request
   stays REQUESTED until the separate operator procedure runs. No Content activation/cancel/retry control appears.

Audit validation is documentation-only: run repository Markdown/link/hygiene checks and
`git diff --check`; verify linked source files and the endpoint inventory against controllers.
No application code, ADR status, runtime profile, database, processing or browser change is part of E5-F1.
No live API, Spark, PostgreSQL recovery, Docker or browser execution evidence is claimed.
Executed repository validation: `npm run check` passed Markdown/link checks across 67 files and
hygiene; `git diff --check` passed. A separate static consistency check verifies this new document's
source references and all 14 implemented management HTTP method/path pairs.

[app]: ../../frontend/src/App.tsx
[analytics-api]: ../../frontend/src/api.ts
[analytics-hook]: ../../frontend/src/useAnalytics.ts
[shell]: ../../frontend/src/components/AppShell.tsx
[states]: ../../frontend/src/components/States.tsx
[timestamp]: ../../frontend/src/components/Timestamp.tsx
[styles]: ../../frontend/src/styles.css
[vite]: ../../frontend/vite.config.ts
[frontend-guide]: ../../frontend/README.md
[ui-tests]: ../../frontend/src/AnalyticsApp.test.tsx
[wire-tests]: ../../frontend/src/analyticsApi.test.ts
[source-tests]: ../../services/content-api/src/test/java/platform/contentapi/management/SourceManagementIT.java
[history-tests]: ../../services/content-api/src/test/java/platform/contentapi/management/RunHistoryIT.java
[control-tests]: ../../services/content-api/src/test/java/platform/contentapi/management/ControlAdmissionIT.java
[resume-tests]: ../../services/content-api/src/test/java/platform/contentapi/management/ContentResumeControlIT.java
[sc]: ../../services/content-api/src/main/java/platform/contentapi/management/SourceController.java
[sr]: ../../services/content-api/src/main/java/platform/contentapi/management/SourceRepository.java
[scommands]: ../../services/content-api/src/main/java/platform/contentapi/management/SourceCommands.java
[catalog]: ../../services/content-api/src/main/java/platform/contentapi/management/ReviewedSourceCatalog.java
[responses]: ../../services/content-api/src/main/java/platform/contentapi/management/SourceResponses.java
[boundary]: ../../services/content-api/src/main/java/platform/contentapi/management/ManagementBoundary.java
[config]: ../../services/content-api/src/main/java/platform/contentapi/management/ManagementConfiguration.java
[role]: ../../services/content-api/src/main/java/platform/contentapi/execution/RuntimeRole.java
[rc]: ../../services/content-api/src/main/java/platform/contentapi/management/RunController.java
[rr]: ../../services/content-api/src/main/java/platform/contentapi/management/RunRepository.java
[rcommands]: ../../services/content-api/src/main/java/platform/contentapi/management/RunCommands.java
[history]: ../../services/content-api/src/main/java/platform/contentapi/management/RunHistoryRepository.java
[values]: ../../services/content-api/src/main/java/platform/contentapi/management/RunInspectionValues.java
[source-cursor]: ../../services/content-api/src/main/java/platform/contentapi/management/SourceCursor.java
[run-cursor]: ../../services/content-api/src/main/java/platform/contentapi/management/RunPageCursor.java
[cc]: ../../services/content-api/src/main/java/platform/contentapi/management/ControlController.java
[ccommands]: ../../services/content-api/src/main/java/platform/contentapi/management/ControlCommands.java
[controls]: ../../services/content-api/src/main/java/platform/contentapi/management/ControlRepository.java
[resume-controls]: ../../services/content-api/src/main/java/platform/contentapi/management/ResumeControlRepository.java
[resume-plan]: ../../services/content-api/src/main/java/platform/contentapi/execution/ContentResumePlan.java
[errors]: ../../services/content-api/src/main/java/platform/contentapi/management/SourceFailure.java
[advice]: ../../services/content-api/src/main/java/platform/contentapi/management/SourceAdvice.java
[frozen]: ../../services/content-api/src/main/java/platform/contentapi/management/FrozenRunSelection.java
[worker]: ../../services/content-api/src/main/java/platform/contentapi/execution/WorkerExecutionCapability.java
[import-role]: ../../services/content-api/src/main/java/platform/contentapi/importing/OperatorImportConfiguration.java
[import-catalog]: ../../services/content-api/src/main/java/platform/contentapi/importing/ImportCatalog.java
[recovery-role]: ../../services/content-api/src/main/java/platform/contentapi/execution/OperatorRecoveryConfiguration.java
[durable]: ../architecture/durable-execution.md
[producers]: content-evidence-producers.md
[adr5]: ../adr/0005-gold-consumer-integration.md
[adr10]: ../adr/0010-durable-execution-coordination-and-recovery.md
[adr11]: ../adr/0011-artifact-import-maintenance-ownership.md
[adr14]: ../adr/0014-controlled-operational-recovery.md
[adr19]: ../adr/0019-explicit-content-resume.md
[adr20]: ../adr/0020-inherited-content-reconciliation.md
[adr21]: ../adr/0021-content-evidence-producer-reservations.md
