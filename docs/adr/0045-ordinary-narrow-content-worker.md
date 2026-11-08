# ADR 0045: Ordinary original-only narrow Content worker

Date: November 8, 2026

Status: **Implementation authorized. Operational qualification, merge and deployment remain pending.**

## Discovered production reachability gate

The qualified original chain currently has no source endpoint in common with the production API.
`ReviewedSourceCatalog` version 1 admits reviewed public DNS HTTP(S) endpoints and excludes literal
loopback. `StrictFetchLaunchRuntime` requires `testEndpoint=true` and literal `127.0.0.1` or `[::1]`.
Existing integration fixtures substitute a loopback catalog outside the production loader.
Those fixtures prove bounded internal execution, not an ordinarily reachable API-to-worker path.

The accepted public-source requirement and isolated-test exception in the durable-execution
contract must not be silently rewritten. A separately reviewed, versioned strict public-source
lane and its network/launch qualification are required before ordinary public Content activation.
[ADR 0046](0046-bounded-public-fetch-lane.md) now defines the independently reviewed implementation
contract for that lane. Acceptance of its implementation contract does not close this operational gate.
Promoting the test flag or substituting a fixture catalog is not production acceptance. Initial
read-only startup, live-session and stopping preparation changed no dispatch capability. The
unmerged implementation now selects the real public composition-v2 adapter solely for disposable
qualification installations. It adds no test-only capability or hidden activation flag. Packaged
end-to-end acceptance below remains pending qualification; no existing installation is activated.

## Scope

Connect the existing narrow original-only Content chain to the packaged worker lifecycle for
disposable qualification. Merge, deployment and operational readiness claims require the production
source gate above and all acceptance gates below to be completed.
The existing RunProfile v2, policy, native runtime versions, and the separately versioned
strict pipeline/launch composition in ADR 0046,
one fresh source, qualified publisher reference, isolated literal-loopback qualification endpoints,
64 MiB / 221-entry plan and all semantic reader bounds remain unchanged. This adapter adds no further profile,
dependency, continuation, repair, automatic retry, adoption, cleanup or cloud authority.
Issues #123 and #124 remain separate. Historical runtime-v2 registrations retain ledger-v1 meaning.

## Startup and selection

One worker selects one existing execution profile. Its pipeline determines either Reliability
or the narrow Content adapter, never both. Ordinary Content requires the complete public
composition-v2 profile; historical loopback composition-v1 remains internal qualification only. The existing runtime-profile setting identifies the
Reliability runtime or the exact separately registered strict Content pipeline. Content startup
requires exactly one strict pipeline registration for the selected RunProfile, its exact linked
runtime and storage chain, installed manifests/modules/interpreters, versions and existing host
guard. It performs no source request, object write, attempt preparation or native publication.

Run-independent startup qualification must not manufacture a run, attempt, selected source,
publisher reference or READY receipt. Shared registration checks may be factored into a separate
read-only description, but it is never launch authority and cannot enter run-bound launch methods.
After claim, the existing complete frozen-input/READY/runtime qualification still precedes FETCH.
Configuration and installed-byte mismatch fail closed; metadata discovery is not live readiness.

The global coordinator and installation guard remain shared with Reliability and imports.
An occupied or stopping coordinator is never adopted. The worker claims only its selected
profile and pipeline in submission order. A Content claim atomically enrolls the existing narrow
admission marker and rejects resumed/history-bearing or incompatible selection shapes. Ordinary
dispatch cannot reach legacy Content adapters or a test capability.

## Admission, supervision and release

Only an acknowledged claim permits PREPARING, fresh bounded filesystem preparation, READY and
the existing original chain. The attempt remains CLAIMED throughout native execution. No legacy
stage history or synthetic delivery is created. The same dispatcher thread owns all mutable
owner/session versions and heartbeats; stopping reaches admission fences, probes, bootstrap and
all child supervisors. A stop before another effect prevents that effect, and a stop with a live
child uses the existing bounded cleanup before the dispatcher releases its cooperative guard.

Content failures after claim retain coordinator occupancy and evidence, including a failure
before READY. They do not inherit Reliability's clean prelaunch failure/release shortcut.
Recovery remains an explicit separately qualified operation, and some incomplete admission
states remain blocked. A stopped or uncertain supervisor reports RECOVERY_REQUIRED honestly.

Successful final SQL commit returns the actual refreshed idle Session computed from the live
owner and exact release transaction. That acknowledged token alone permits a later claim.
Neither a durable completion receipt nor a historical lookup may reconstruct or transfer an
owner/session. Claim, admission, operation or success acknowledgment loss stops dispatch; even
if SQL success committed, this process does not infer release and continue.

## Acceptance

Qualify the packaged API and production worker with actual PostgreSQL and native children:
complete Content success, subsequent queue use of the acknowledged session, immutable selection,
profile/legacy exclusion, invalid or missing startup closure, global Reliability/import exclusion,
occupied startup, stopping during live work, and ambiguous claim/admission/final acknowledgment.
Reuse the accepted internal-chain fault corpus instead of adding arbitrary stress workloads.
Retain exact exhaustive CI inventory, strict result coverage and isolated capacity budgets.
Independent review and final exact-head CI are required before representing this worker as
operationally qualified. Issue #49 and UI acceptance are not declared complete by this decision.

## Independent preparation

Run-independent startup checking, an explicitly acknowledged final-release Session, persistent
stopping checks and post-claim probe heartbeat refresh can be implemented and tested without enabling
the dispatcher or broadening endpoint policy. These are preparation only. The remaining production activation and API-to-worker acceptance
work must not be inferred from their completion.
