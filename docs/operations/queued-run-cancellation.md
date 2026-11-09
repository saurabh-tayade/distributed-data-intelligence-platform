# Queued run cancellation

## Scope

This increment implements the existing ADR 0010 `QUEUED -> CANCELLED` boundary
for original, never-claimed runs, independently of ordinary Content dispatch.
It adds no worker activation, public fetch, retry, recovery, process termination,
publication verification or output cleanup authority. Running cancellation and
safe retry remain unfinished E5 work. PR #137 and its public acceptance gates
are unchanged; this feature does not depend on that branch.

Only a queued run with no current or historical attempt, stage or result can
be cancelled. Resumed or retried histories are excluded. A pending control
also blocks a new cancellation. Disabled sources do not prevent cancellation.
An unrelated occupied or recovery-only coordinator is retained unchanged.

## Local API

`POST /api/management/v1/runs/{runId}/controls` accepts this exact body:

```json
{"action":"CANCEL","attemptId":null,"quiescenceConfirmed":null}
```

Use a fresh `Idempotency-Key`, the reviewed run's quoted `If-Match` version,
the approved local `Origin`, JSON content type and `X-DDIP-Local-Request: 1`.
Existing loopback management boundaries and bounded request parsing apply.
No arbitrary reason, attempt selection or quiescence attestation is accepted.

The transaction locks the installation coordinator before the run, matching
worker claim order. It binds the original normalized request, appends the
request audit event, closes the run as `CANCELLED`, acknowledges the control as
`APPLIED` with reason `CANCELLED`, and appends run/control outcome audit events.
Failure at any step rolls back all effects. No attempt is created. Frozen
selection and registered artifacts are unchanged, and retry eligibility is
false because this increment provides no guarded retry path.

HTTP 202 returns the durable control receipt, `Location` and control `ETag`.
The receipt state, not 202 alone, confirms the applied cancellation. Run and
control versions are distinct. Read the run again to inspect its current state.
Exact key/body/version replay returns the original control even though the
run version advanced. Do not mint a new key to resolve a lost acknowledgment.

A stale run version fails with 412; a nonqueued or previously attempted run
fails with 409. Neither creates a control. If worker claim wins the race,
cancellation does not stop or change that worker. If cancellation wins, claim
cannot select the cancelled run. Control and audit capacity limits still apply;
terminal cancellation releases its own future audit reservation atomically.

## Operations workflow

Open a queued run in Operations, review its identity and version, then explicitly
confirm cancellation. The tab retains one cancellation intent across navigation
and reload. It never automatically submits or replays a mutation. An uncertain
outcome retains the original key/body/precondition for deliberate same-request
resolution. A confirmed receipt is followed by a fresh run read; a failed read
must not turn confirmation into a new mutation. Navigation or browser abort is
not proof that the server rolled back. Existing submission/source intents are
separate and ordinary Content dispatch remains disabled.

## Verification

`QueuedCancelIT` exercises real PostgreSQL and the local HTTP boundary, including
atomic rollback, exact replay, duplicate requests, genuine worker-claim races,
unchanged ownership and immutable selections. `ControlCommandsTest` covers the
strict envelope. The new suite belongs to the exhaustive `core` CI partition;
existing control/resume/worker suites remain required. Frontend tests cover
review, retained uncertainty, receipt refresh and request validation. These
checks use local fixtures, not real public-feed acceptance or performance tests.
