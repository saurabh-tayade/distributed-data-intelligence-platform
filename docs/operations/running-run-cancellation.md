# Running Reliability cancellation

## Scope

The running cancellation implementation targets the active original Reliability
EXECUTE attempt. It does not activate ordinary Content dispatch, cancel an inactive
Content journal, retry a run, delete outputs, or roll back a publication.
[Never-claimed queued cancellation](queued-run-cancellation.md) remains separate.
The real public-feed acceptance test remains deferred until after UI polish.

## Request and outcome

Use the existing local controls endpoint and exact cancellation body:

```json
{"action":"CANCEL","attemptId":null,"quiescenceConfirmed":null}
```

Review the current run and use its quoted `If-Match`, a fresh `Idempotency-Key`,
approved local Origin, JSON content type and `X-DDIP-Local-Request: 1`.
The backend determines the eligible active attempt; the caller cannot select an
arbitrary PID, attempt or termination target.

HTTP 202 confirms admission only. Inspect both control and run afterward:

| Control | Run | Meaning |
| --- | --- | --- |
| `REQUESTED` | `CANCEL_REQUESTED` | Intent is durable; stop/disposition is not yet confirmed |
| `APPLIED`, reason `CANCELLED` | `CANCELLED` | Cancellation has been proven and committed |
| `REQUESTED` | `RECOVERY_REQUIRED` | Slot remains occupied; independent recovery is necessary |
| `TOO_LATE`, reason `SUCCEEDED` | `SUCCEEDED` | Verification proved a completed publication; cancellation did not undo it |

Stale preconditions or unsupported histories fail without admitting a new request.
After a lost response, resolve the original key/body/version. Do not generate another
key or interpret browser cancellation as server rollback. The UI retains the intent
across navigation and offers explicit same-request resolution, not automatic replay.

## Stop and recovery boundary

The owning worker supervises bounded termination and prevents a following writer
stage. A clean stop before any publisher authorization can finish cancellation.
After writer authorization, process exit alone cannot establish the output state.
The run remains occupied for the existing explicit guarded operator recovery flow.
A restarted worker must not adopt or clear this slot. Stop the old worker-role
process before requesting reconciliation; the control endpoint rejects a recorded
worker or child that is still alive. The operator must still review descendant and
unmanaged-writer containment, not infer it from the dispatcher status.

Recovery retains fresh operator containment review and independent native verification.
Qualified absence permits cancellation. A verified publication wins as success with a
`TOO_LATE` control. Incomplete, invalid, changed or unavailable output remains blocked;
this command does not delete, repair or reuse that output. Unconfirmed termination or
unknown descendants also remain blocked. Never assert quiescence based only on a
stale heartbeat, missing HTTP response or a UI message.

The implementation follows [ADR 0046](../adr/0046-reliability-running-cancellation.md).
Its tests and exact-head CI results must be checked before deployment; a source change
or request receipt alone is not proof that cancellation completed.
