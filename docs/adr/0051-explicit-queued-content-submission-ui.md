# ADR 0051: Explicit queued Content submission UI

Date: November 17, 2026

Status: Bounded functional scope approved by the owner on November 16 in
[#49](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/issues/49#issuecomment-6923837089).
Implementation and acceptance evidence remain subject to review; no merge or public activation approval.

## Context and approved boundary

[ADR 0043](0043-integrated-original-content-pipeline.md) defines the one-source original Content
contract. [ADR 0050](0050-original-only-content-worker-lifecycle.md) integrates a separately configured
worker, while real public-feed operational qualification remains deferred in
[#47](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/issues/47).
The owner explicitly approved allowing a deliberately acknowledged request to queue despite that
qualification gap. Queue admission is distinct from processing, public readiness and Analytics selection.

## Implementation decision within this scope

Use the existing opt-in Content profile, profile-scoped source and imported publisher-reference
GETs. Validate their complete declared DTOs, including `dispatchDisabled: true`,
`qualificationScope: "REGISTRATION_METADATA_ONLY"` and `runtimeQualification: "NOT_CHECKED"`.
These are discovery markers; they cannot attest that installed runtime files are ready or that a
worker is operating. Add no readiness probe. Recheck the selected current source record's product,
ID, enabled gate, RSS profile, revision and configuration digest before preparing the frozen request.

The UI accepts exactly one fresh source, zero retained Silver and one publisher reference. Preserve
the existing POST, exact request body, idempotency headers, queue limits and authoritative server
validation. A Content review requires explicit acknowledgment of incomplete public qualification,
possibly indefinite queuing, shared unfinished capacity, possible future separately approved worker
claim and independent Analytics selection. Existing uncertain requests remain resolvable without
repeating acknowledgment or rediscovering their original dependencies.

Keep the original `ddip.reliability-run-intent.v1` tab slot and version-1 envelope. The exact request
body's pipeline discriminates Content and Reliability. No migration or mount-time rewrite is needed;
existing Reliability bytes stay valid. Both products share the one mounted submission panel and
one retained intent. Unknown stored shapes fail closed. Navigation and refresh preserve the command;
HTTP 202 must match its pipeline, product, source revision and complete artifact identities before
admission is confirmed. Invalid or missing acknowledgments retain uncertainty, never trigger an
automatic retry or replace the key. Tab-storage failure prevents a new POST or retains the existing
pending command for explicit resolution.

## Alternatives and tradeoffs

- Read-only preparation would avoid queue occupancy but would not deliver the owner-approved
  deliberate admission journey. Requiring a readiness API would invent a contract outside this scope.
- Separate product storage slots could admit two unresolved mutations in one tab and require a new
  arbitration mechanism. Keeping the original slot preserves the existing exclusion and legacy bytes.
- Renaming or versioning storage would require a migration with acknowledgment-loss risk. The
  pipeline union is additive and validates the same Reliability DTO without rewriting saved commands.
- Catalog pages are live metadata. Selections survive pagination, and source changes before review
  require deliberate rediscovery. Reference/profile compatibility beyond the safe DTO and scoped GET
  remains the server's responsibility; the UI cannot verify native bytes or freeze database state.

No backend schema, worker dispatch, source family, scheduling, publication-selection, Content
cancellation/recovery, dependency or service change is introduced. See the
[operator and verification record](../operations/e53-content-queued-submission-ui.md) for acceptance
criteria, test boundaries and separate browser/public qualification gates.
