# ADR 0048: Cancellation of explicitly retried Reliability runs

Status: **Implementation record under delegated in-scope design authority; local verification complete; PR CI pending**

## Decision and scope

Complete the existing Reliability cancellation workflow for an explicitly requeued
or running retry. This extends the original-attempt limitation of ADR 0046 and the
cancellation exclusion in ADR 0047; their historical verification records remain
unchanged. It does not change Content dispatch, deferred public-feed acceptance,
general retry eligibility, output repair or automatic recovery.

Keeping the limitation would remain fail-closed, but would leave an operator unable
to stop the newly supported retry through its existing controls. Merely removing the
attempt-number check is insufficient: explicit retry execution does not run through
the ordinary dispatcher's cancellation settlement path. Admission, supervision,
durable disposition and truthful operator reporting must be delivered together.

## Acceptance contract established before implementation

1. A queued retry cancellation requires the exact current applied RETRY queue receipt
   and reviewed run version, with no active attempt. Cancellation and explicit claim
   serialize. Cancellation changes only control-plane state, never native artifacts.
2. A running retry cancellation requires validated current retry-claim lineage and
   the exact active EXECUTE owner/version. Original execution behavior remains intact.
   Earlier applied cancellation is history, not a stop instruction for this attempt.
   Unknown, malformed and outstanding controls remain fail-closed.
3. Explicit retry execution handles cancellation settlement after supervised native
   cleanup. A proven pre-writer stop may finish CANCELLED and release the slot.
   Authorized or uncertain writers retain the existing independent recovery boundary.
   A verified completed publication wins as success with cancellation TOO_LATE.
4. Successful cancellation does not grant fresh retry eligibility, clear incomplete
   output, renew an owner or select new source revisions. Historical requeue receipts
   cannot revive a cancelled queue entry or authorize a second launch.
5. Operator output distinguishes recorded execution, cancellation and recovery-needed
   outcomes. A historical receipt is not a current-state guarantee or fresh artifact
   revalidation. An ambiguous response never establishes rollback or completion.
6. UI review preserves the exact conditional request and idempotency identity across
   navigation and interrupted requests. Resolving the same request is explicit;
   no automatic replay or replacement identity is introduced.
7. Verification covers claim/cancel serialization, stale requests, rollback and lost
   acknowledgment, original-run regressions, earlier versus current cancellation,
   pre-writer settlement and native post-authorization independent recovery. UI checks,
   independent review and exact-head CI are required before merge.

## Remaining boundaries

Content cancellation and ordinary Content dispatch remain unsupported. Public-feed
acceptance is still deferred in issue 47; draft PR 137's gate is unchanged. Full managed
startup/restart/teardown demonstration and the user's browser walkthrough are separate
acceptance work. This increment does not close issue 49 or claim production readiness.

## Verification

Full backend unit verification passed **912 of 915 cases**, with three existing
optional-corpus skips and no failures/errors. **47 distinct database/native cases**
passed across the regression run and targeted fixture-correction reruns: queued
cancellation 17, original running cancellation 11, guarded retry 10 and new retry
cancellation 9. The final three native boundary cases passed together.

Those cases use a real owner JVM that exits before independent recovery. The live
child case terminates a supervised sleep surrogate after writer authorization;
the completed-publication case runs the actual pinned Spark publisher and then
cancels before owner completion. Recovery independently verifies absent output or
the completed publication. This is not evidence of arbitrary mid-Spark partial-output
cancellation. Test-only descendant identities are observed from actual processes;
production quiescence checks and bounded review are unchanged.

Frontend verification passed **1,498 tests**, with two existing skips, plus typecheck,
lint and production build. The **67 CI policy/inventory tests** and repository checks
passed. Independent static review covered fencing, lifecycle identity, supervision
and truthful cancellation output. Hosted exact-head CI remains required before merge;
these local results are not public-network or browser-walkthrough acceptance.
