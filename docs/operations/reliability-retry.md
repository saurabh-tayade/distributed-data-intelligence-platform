# Guarded Reliability retry

Retry is currently available only after independent recovery finalized a Reliability
run with verified absent output and retained retry eligibility. It reuses that run's
frozen inputs. It never repairs an incomplete output, selects newer source revisions,
or enables Content dispatch. Direct failures and never-claimed queued cancellations
are not eligible simply because their state is FAILED or CANCELLED.

## Request versus execution

Review the run's current ETag and use a new idempotency key with the existing local
Origin, JSON and `X-DDIP-Local-Request: 1` protections:

```json
{"action":"RETRY","attemptId":null,"quiescenceConfirmed":null}
```

HTTP 202/REQUESTED means intent only. Preserve the exact key, body and ETag if its
response is lost. Resolve the same request; do not invent another key. The Operations
UI retains the review and ambiguous request in this tab, including across navigation.

Stop other execution handlers before the existing local operator-recovery role takes
the installation guard. Using the same registered execution profile, control binding,
and runtime profile, select `ddip.recovery.action=retry` and the returned
`ddip.recovery.control-id`. Fresh read-only checks must succeed before this changes the
control to APPLIED with reason RETRY_QUEUED. The CLI returns REQUEUE_RECORDED,
a historical receipt, not a promise that the run is still queued. This does not start
the writer.

Then use `ddip.recovery.action=execute-retry` with the same control and profile
configuration to requalify, claim the exact run, and execute it. Ordinary dispatch
skips this explicitly controlled queue entry. Repeating a successful execute command
only resolves that control's recorded successful execution; it does not rewrite output.
EXECUTION_RECORDED reports that durable completion, not a fresh revalidation of files
after the completed attempt. Read current run inspection separately.

## Failure and safety limits

Changed dependencies, partial output, an existing registered publication, disabled
sources, occupied coordinator, exhausted capacity and stale versions fail closed.
A failed qualification may leave the admitted request REQUESTED; inspect and address
the reported problem before resolving it. Never delete native output to force retry.
Private probes are bounded; exhausted evidence capacity requires operator investigation,
not silent deletion or unlimited replay.

If execution is interrupted, retain the slot and use the existing explicit recovery
procedure with fresh containment review. A lost response is not proof of cancellation,
rollback, success or permission to launch again. Running/queued cancellation of later
retry attempts is not enabled by this feature. UI polish and real public-feed testing
remain separate work.

See [ADR 0047](../adr/0047-guarded-reliability-retry.md) for the contract.
