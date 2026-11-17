# E5.3 — Explicit queued Content submission UI

The November 16, 2026
[owner approval](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/issues/49#issuecomment-6923837089)
authorizes bounded Content queue admission. Public operational qualification remains deferred under
[#47](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/issues/47).
[ADR 0051](../adr/0051-explicit-queued-content-submission-ui.md) records the UI decision and alternatives.
Baseline inspection found local and GitHub main at `560c2886a7ddf449029b14b74a740396712ddb96`.
Untracked owner material in `docs/reviews/` is outside the change.

## Acceptance criteria established before implementation

- Validate exact metadata-only Content profile/source/reference DTOs and bounded cursor pages.
- Select one enabled compatible current RSS/Atom source revision, one profile and one imported
  publisher reference. Recheck the current source before freezing; retain zero Silver artifacts.
- Review exact immutable identifiers and body without a server mutation. Require explicit Content
  queue acknowledgment before a new submission; issue one POST only after confirmation.
- Validate HTTP 202 pipeline, product, source revision, artifact identities, ETag and Location.
  Show the actual persisted state, canonical run link and independent execution history.
- Preserve legacy Reliability intent bytes and one retained intent across both products per tab.
  Keep the original key/body through navigation, refresh, unmount and ambiguous acknowledgment.
- Fail closed on malformed discovery/receipts or unavailable storage. Resolve uncertainty only by
  deliberate same-key/body replay; never silently retry or start a replacement intent.
- Preserve Reliability behavior, worker ownership/capacity and Analytics publication selection.
  Run relevant automated checks and exact-head hosted CI; identify browser acceptance separately.

## Operator journey

1. Open **Operations → Submit Queued Content Run**, or `#/operations/submit/content`.
   An enabled RSS/Atom source detail also offers **Choose queued Content inputs**. Inputs are
   deliberately selected in the form; the contextual link does not preselect a source or submit.
2. Choose a registered Content execution profile, then one advertised compatible current source
   and one receipt-linked publisher reference. Profile changes clear dependent choices. Selections
   survive page changes; **Refresh Content selections** clears source/reference choices and starts
   fresh catalog traversals. Each visible catalog reads one 20-row page at a time, with at most
   20 cursor positions retained. There is no background traversal or readiness polling.
3. Choose **Review Content submission**. The current source must still be enabled and have the
   selected RSS profile, revision and configuration digest. A changed/disabled source requires
   refreshed deliberate selection. No request is admitted during discovery or review.
4. Inspect the frozen profile, source/revision, publisher reference, zero retained Silver, exact
   body and idempotency key. Acknowledge all displayed Content queue limitations, then choose
   **Confirm Content submission**. A refreshed unsubmitted review requires acknowledgment again.
5. Inspect the receipt's actual persisted state and choose **Open admitted run**. Existing run
   detail, attempts, audit events and execution history remain the inspection surface. A replay
   may return an advanced or terminal persisted state; HTTP 202 does not imply it is still queued.

Public Content execution has not completed operational qualification. Admission starts no worker
and does not guarantee processing. A run may remain queued indefinitely, consuming one of the
20 shared unfinished-run places. A separately approved compatible worker could claim it in the
future. Successful processing does not automatically change the publication selected for Analytics.
Discovery's disabled-dispatch and unchecked-runtime markers are preserved as metadata assertions,
not installed health or execution-readiness evidence. Final backend validation, queue capacity,
source admission and execution controls remain authoritative after all UI checks.

## Retained intent and failure handling

Both submission routes use the existing `ddip.reliability-run-intent.v1` session-storage entry and
version-1 envelope. The request's pipeline discriminates the strict Content and unchanged Reliability
DTOs. Existing Reliability bytes are read without migration or mount-time rewriting. Any retained
review, pending/uncertain request, rejection or confirmed receipt blocks another intent until its
appropriate explicit resolution, cancellation or dismissal. Preserve invalid/unavailable tab data for
operator inspection instead of clearing storage to bypass the guard.

One `RunSubmissionPanel` remains mounted above routes. Navigating to the other product or Analytics
does not abort an active submission or redirect the operator after its result. App unmount aborts the
transport and retains uncertainty. A lost response, malformed/inconsistent HTTP 202, unknown error,
HTTP 500 or HTTP 503 may have committed; the original exact command remains. **Resolve same run
request** explicitly resends that key and body without another acknowledgment or rediscovery.
There is no automatic replay and no cancel/dismiss action for an uncertain request. Recognized
HTTP 400/403/404/409/429 errors require deliberate return to selection and review. Public error
codes, validated correlation IDs and retryability guidance are shown without server diagnostics.

Storage must accept the pending command before POST. If receipt storage fails afterward, the
pending command remains resolvable and no confirmed receipt is displayed. Reload retains the exact
body and key. These guarantees apply to the same browser tab; closing/clearing the tab's storage
is outside the retained-intent mechanism and must not be used to forget an unresolved request.

## Contract compatibility and verification

The [existing discovery contract](narrow-content-submission-discovery.md) is unchanged, as are
`POST /api/management/v1/runs`, source/artifact capture, normalization, idempotency, twenty unfinished
runs, the single global worker and all migrations. Reliability's existing route remains
`#/operations/submit`. No new runtime dependency, worker startup, public feed fetch, automatic
Analytics selection or broader Content control is added. Accepted historical ADRs are unchanged.

Frontend tests cover exact/malformed/unknown metadata, qualification markers, incompatible publisher
references, source rechecks, empty catalogs, pagination, frozen review, acknowledgment, genuine
receipt identity correspondence, legacy intent retention, cross-product exclusion, ambiguous errors,
storage failures, navigation, focus and no automatic POST/retry. Existing Reliability suites remain
part of the complete frontend run. Local commands are:

```bash
npm --prefix frontend run typecheck
npm --prefix frontend run lint
npm --prefix frontend test
npm --prefix frontend run build
npm run check
git diff --check
```

Use the repository-constrained Node 24 runtime. A default frontend-only run skips the existing
Content/Reliability analytical fixture suites and the separate submission HTTP parity suite when
their generated fixture environment variables are absent. Those skips are not backend acceptance.
The production build retains Vite's existing large-chunk advisory; broad bundle/UI work is deferred.

`NarrowContentSubmissionDiscoveryIT.queuedContentUiFixtureUsesDiscoveredSelectionAndGenuineAdmissionWithoutWorker`
uses the isolated PostgreSQL fixture and real management HTTP controller/boundary/advice. It reads
live scoped metadata, rechecks the current source, submits the discovered narrow selection, checks
a genuine HTTP 202/QUEUED, repeats the same key, and checks one persisted run plus execution history.
No worker session, attempt, feed/object request or native pipeline operation is created; discovery
leaves the inspected metadata unchanged and queue admission leaves the coordinator unchanged.
A publisher-reference fixture is prepared/imported locally. This is offline API admission evidence,
not public-feed or packaged worker qualification.

The test writes ignored `services/content-api/target/content-submission-http.json`. PostgreSQL core CI
then runs the mandatory frontend parser and UI parity tests against those actual HTTP DTOs:

```bash
# In an isolated existing PostgreSQL test environment with the pinned native fixture prerequisites:
mvn -B -ntp -f services/content-api/pom.xml verify -Ppostgres \
  -Dit.test=NarrowContentSubmissionDiscoveryIT -DskipUnitTests=true
DDIP_CONTENT_SUBMISSION_HTTP_FIXTURES=../services/content-api/target/content-submission-http.json \
  npm --prefix frontend test -- contentSubmissionNativeParity.test.tsx
```

The parity UI consumes captured real responses; it does not itself connect a browser to PostgreSQL
or create a second backend run. A configured fixture path that is absent or invalid fails the test.
The hosted core job creates a fresh file from its real isolated test before running parity.
The existing exhaustive PostgreSQL class inventory, required job names, timeouts and classifier
are preserved. The CI workflow change selects full regression under the trusted base policy.
Hosted outcomes and counts must be tied to the exact PR head and linked in the PR/final report;
this document does not assert that a historical run passed for a later head.

## Separate acceptance and next checkpoint

Automated DOM/keyboard tests do not constitute manual browser acceptance. Clean-checkout browser,
visual/responsive, assistive-technology, API-mode live-browser and restart/demo acceptance remain
separate work under #53. This implementation does not perform a real public feed fetch, start a
Content worker, qualify public DNS/TLS/IPv4, enable scheduling, alter recovery/cancellation or select
an Analytics publication. #47 remains deferred and #49 remains open.

Review this bounded UI and its exact-head CI before any owner merge decision. Consolidate broader
presentation/workflow refinement under #145, then perform separately scoped browser/release
acceptance. Public operational qualification and any activation require their own owner checkpoint.
