# ADR 0014: Controlled local operational recovery

Status: **Implementation record — C3c.4 requested; not owner-accepted ADR status**

## Boundary and alternatives

Use a dedicated `operator-recovery` non-web role in the existing packaged Spring Boot application.
It selects one canonical existing control UUID; it neither admits controls nor dispatches work.
`inspect` reads bounded relational/process summaries without registering a worker or launching native
code. `recover` acquires the existing approved installation guard, registers one compatible recovery
process, and retains the guard through handoff, bootstrap, two independent native checks and atomic
finalization. Ordinary worker startup remains separate after release.

A public endpoint would expand authorization and process-lifetime concerns. A separate executable
would duplicate reviewed profile/database/runtime loading. A scheduler or worker-startup hook would
turn explicit operator recovery into automatic takeover. A persisted approval token would permit
replay after JVM replacement. Interactive confirmation in the guarded JVM keeps authority local,
fresh and tied to its exact evidence. Piped input is supported for trusted local controllers; it is
not an authentication mechanism and must never be generated without actual operator review.

## Workflow and acceptance criteria established before implementation

1. Select the exact C3a REQUESTED RECONCILE control using existing management reads/admission.
2. Read-only inspection exposes IDs, fixed states, occupancy, process identities/status and unresolved
   checkpoints. It grants no authority. Unknown states and oversized summaries fail closed.
3. Recovery uses the reviewed worker profile/control binding/runtime IDs. It obtains the guard before
   registration. A fresh random challenge binds the exact target digest, private evidence digest,
   process observations and phase. Confirmation must repeat those values, every recorded process and
   checkpoint, plus an explicit manual containment review and known descendant PID/start identities.
   A true flag alone is insufficient. No target or process list is inferred from a historical receipt.
4. Recheck freshness, target, processes and retained files after confirmation. Use the existing fenced
   transfer; bootstrap its exact packet and separately authorize the same live process. On lost transfer
   acknowledgment, bootstrap/authorization may proceed only in that same guarded JVM and only after
   exact committed evidence resolves. Rollback or uncertainty exits occupied.
5. Independently verify, then request another fresh operator review before final qualification and
   disposition. No verifier item is reopened. Resolve lost final acknowledgment by exact historical
   completion. A subsequent invocation checks completed facts without registering or releasing anything.
6. Termination/outage/unknown descendants/changed evidence/guard contention cannot clear ownership.
   An interrupted recovery needs a new C3a control targeting the occupied recovery attempt; old controls
   and receipts cannot authorize replacement processes. Unsupported provenance stays blocked.
7. Known qualified success/failure uses C3c.3's matrix and exact-owner atomic release. Pending cancellation
   and unavailable/contradictory evidence stay unresolved. Recovery never publishes, repairs, fetches,
   cancels or retries. A fresh ordinary worker registration is necessary after release.

Bounded recovery chains follow immutable audited packets back to the original EXECUTE selection,
without creating historical owner/session tokens. Each link must preserve the exact run, selection,
attempt/instance/epoch relationship, runtime hash and retained private inventory. The operator reviews
all ancestor process identities, unresolved checkpoints and retained descendants again. These native
provenance checks belong to the operational transfer wrapper and native verification; the existing
C3b metadata-only handoff/bootstrap seams keep their independent acceptance contract. A metadata
handoff by itself never proves native provenance or authorizes disposition.

Acceptance uses the actual packaged JAR, disposable PostgreSQL 17, pinned Java/Python/native runtimes,
JSONL/Parquet synthetic publications, process boundaries, missing/stale/inconsistent confirmation,
active/unknown processes, crash/new-control handling, outage/restart, duplicate invocation and fresh
ordinary-worker execution after release. Hashes and modification times must remain unchanged.
Run focused checks, then one full backend regression; record suite and total durations and CI risk.

## Limits

The local OS user and reviewed installation configuration are trusted. Cooperative POSIX locks and
negative PID/start observations cannot prove universal orphan containment or fence hostile writers.
Manual descendant/unmanaged-writer review remains mandatory, including launch gaps and PID-only
Py4J hints. Native filesystem observations and PostgreSQL commit are not atomic. Existing freshness,
read bounds, compatibility allowlist and conservative classification limits remain in force.
No AWS, distributed failover, exactly-once effect or production-readiness claim is introduced.
Accepted ADRs 0010/0011 are unchanged; ADRs 0012/0013 retain implementation-record status.
