# 0018: Explicit managed Content recovery boundaries

Date: October 30, 2026. Status: **Implementation record; owner acceptance pending**.

D5b-1 extends the accepted ADR 0010 ownership protocol to explicit Content reconciliation.
It does not activate Content dispatch. ADRs 0015–0017 retain their existing approval status.

Reuse C3c admission, the installation host guard, fresh operator process review, bounded
lineage, occupied coordinator handoff and increasing epoch. A pipeline-specific runtime
boundary preserves Reliability's original runtime JSON and verifier/finalizer. Content
captures the exact original private attempt, including child sidecars in the launch/PID
recording gap. New recovery evidence belongs to the reconciliation attempt. No original
command, journal, verified stage, publication or captured run selection is rewritten.

Use existing GET-only Bronze inspection, Silver qualification, Gold preparation/verification
and the independent Content reader. A fixed read-only dispatcher chooses PREPARE or VERIFY;
it cannot invoke a publisher. Its bytes are included in the recovery qualification digest,
without changing retained D3–D5a module hashes. All external verification finishes outside
PostgreSQL transactions. Repeated observations must agree before disposition.

The requested earlier-boundary outcome is PAUSED. This is an explicit departure from
ADR 0010's earlier exclusion of pause/resume states, authorized for implementation by the
D5b-1 task, not an assertion of owner acceptance of this ADR. Migration V8 permits PAUSED
only for Content, with an original attempt, no final artifact and no retry eligibility.
On owner acceptance, this record supersedes ADR 0010's earlier-checkpoint FAILED disposition
only for the Content PAUSED boundary; its ownership and other state decisions remain in force.
No RESUME control, paused-run dispatcher or RECOVERY_REQUIRED-to-RUNNING path is added.
An operator cannot continue a paused run using this implementation.

An alternative FAILED disposition would retain ADR 0010's state vocabulary but would not
meet this task's requested checkpoint outcome. Automatic continuation would hide an
execution authorization inside a read-only reconciliation. Reusing Reliability's finalizer
unchanged would incorrectly apply its publication identity, dependencies and cancellation
restrictions to Content. Content therefore has a small separate disposition adapter using
the shared C3c fence and existing tables. No new service, dependency or native format is needed.

Unavailable or ambiguous evidence retains occupied recovery ownership. A native error code
alone is not sufficient evidence for destructive action or publication repair. No recovery
path deletes, overwrites, retries ingestion, selects replacement inputs or activates analytics.
