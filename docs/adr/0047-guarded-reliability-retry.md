# ADR 0047: Guarded Reliability retry

Status: **Implementation record under delegated in-scope design authority; local verification complete; PR CI pending**

## Decision and boundaries

Implement ADR 0010's explicit guarded retry for a Reliability run whose independent
recovery finalized a proven absent output. Admission records intent only. A separate
local operator command holds the installation guard, freshly verifies dependencies
and namespace absence, and atomically requeues the same frozen run. A second explicit
command claims that exact requeued run and executes the existing native pipeline.

Historical `retry_eligible` is a hint, never current authority. UI-only requeue or using
an old recovery receipt as an execution lease would miss changed inputs, incomplete
output and newly registered artifacts. Reuse the existing full native dependency
verifier through a bounded read-only bridge instead of changing its pinned protocol.

The feature does not enable Content dispatch, change draft PR 137's public acceptance
gate, repair outputs, refresh source revisions, or extend cancellation to later
attempts. Direct preflight failures and never-claimed cancellation are not qualified
recovery outcomes and remain unsupported by this control. No automatic retry or
operator takeover is introduced. Real network acceptance remains deferred in issue 47.

## Acceptance contract

- HTTP RETRY uses the existing conditional/idempotent control envelope. It creates no
  execution attempt and cannot perform native I/O. Stale/conflicting requests have no
  partial effects; identical retries resolve their original receipt.
- Eligibility requires the exact completed independent recovery receipt, absent output,
  no result artifact, intact frozen selection and enabled sources. Keep enough of the
  existing 20-attempt budget for one execution and one recovery successor.
- Native qualification runs under the host guard without holding SQL transactions.
  Fresh full dependency verification, output absence and before/after byte/identity
  seals must agree. Existing, partial, symlinked, unavailable or changed output rejects
  retry. Registered output identity also rejects it, even if its files disappeared.
- Requeue compares exact run/control/coordinator snapshots, source gates and capacity
  again, then atomically records QUEUED/APPLIED and a bounded durable receipt. Lost
  responses resolve that receipt rather than repeat state mutation.
- Ordinary dispatch skips requeued retry runs. Explicit execution requalifies native
  evidence, binds the exact retry lineage at claim and uses existing execution and
  independent completion verification. Repeated completed commands are facts-only
  and must match the successful attempt's retry control.
- Recovery of a subsequent attempt must not mistake an earlier applied cancellation
  for a new stop request. Only validated older cancellation history can be ignored;
  unknown or current outstanding intents remain fail-closed.
- Operations UI retains the exact reviewed request across navigation and uncertain
  responses, without automatic replay. REQUESTED, APPLIED/requeued and successful
  execution are distinct facts.

## Relationship to prior decisions

This extends ADR 0046's absence-reconciled cancellation outcome with eligibility for
an explicit new retry. It does not reinterpret cancellation as rollback or permit
ordinary later-attempt cancellation. Existing cooperative-lock, trusted-local-user,
non-atomic-filesystem/database and conservative recovery limits remain unchanged.

## Verification

The 23 selected native/database cases passed, including targeted reruns after test
fixture corrections. They cover fresh absence and dependency proofs,
atomic admission/requeue, actual second execution, retained receipts, source and
registered-artifact vetoes, and current-versus-prior cancellation scope. A genuine
separate owner JVM halts after durable publisher authorization but before child launch;
fresh recovery closes that retry with OUTPUT_ABSENT, followed by another explicit
retry and real Spark success. This is a launch-gap crash test, not a claim that arbitrary
partially written output can be retried.

Frontend verification passed 1,474 tests with two existing skips, plus typecheck,
lint and production build. Backend unit verification discovered 734 cases: 732 passed
and two existing cases skipped, without failures/errors. The 67 CI policy/inventory
tests and repository Markdown/hygiene checks passed. Independent static reviews
identified and corrected historical receipt wording and successful-invocation lineage
ambiguity. Exact-head hosted CI remains required before merge; these local checks are
not public-network acceptance or browser-walkthrough evidence.
