# ADR 0050: Original-only managed Content worker lifecycle

Date: November 13, 2026

Status: Implementation scope authorized by the owner in
[#49](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/issues/49#issuecomment-6856923409).
Implementation and acceptance evidence remain subject to review. No merge or operational activation authority.

## Context

E5.2-B found no production caller capable of obtaining original Content ownership without
fabricating authorization. ADR 0049 delivered public composition v2 and its bounded child;
ADR 0043 delivered the original 17-operation pipeline. Both remain authoritative. Closed,
unmerged PR #137 supplies candidate worker integration, but predates the Reliability
cancellation and retry contracts in ADRs 0046 and 0047.

## Decision within this checkpoint

Select exactly one registered execution profile for the packaged worker's lifetime:
`RELIABILITY_V2` or original-only `CONTENT_RSS_V1`. Content requires a RunProfile v2 and
exactly one complete public composition-v2 pipeline. Check immutable profile correspondence,
installed executables, JAR/provider isolation, modules, versions, storage identities and private
credentials without fetching a source, contacting object storage or preparing an attempt.
Historical loopback composition v1 cannot enter ordinary Content dispatch.

The existing shared installation guard and coordinator remain the only ownership boundary.
One dispatcher owns all live session versions. Only a genuine acknowledged original claim
may enroll admission, commit PREPARING, prepare the private evidence tree and commit READY.
Invoke the existing original pipeline; preserve its operations, commands, publication
fingerprints, independent verification and atomic completion. Export the next idle session
only from the acknowledged successful completion/release transaction.

Stopping vetoes admission boundaries and native authorization, reaches every Content
supervisor and retains the guard until bounded cleanup returns. Content errors never enter
Reliability's definitive-failure release or cancellation settlement. Uncertain registration,
claim, admission, stage or completion acknowledgments stop dispatch; history cannot recreate
a live token. Partial or uncertain execution retains occupancy and evidence for existing
manual review. V1–V23 migrations, receipts and v1-only new FETCH recovery remain unchanged.

## Alternatives and tradeoffs

A second worker implementation would duplicate global ownership and shutdown behavior.
A capability flag alone supplies neither installed readiness nor acknowledged admission.
Importing PR #137 wholesale would regress newer Reliability cancellation/retry code.
Reuse its reviewed Content portions selectively in the existing lifecycle instead. Runtime
startup checks intentionally incur bounded local reads and a read-only interpreter probe;
metadata discovery still cannot advertise installed or network qualification.
Shared path readiness checks only common filesystem requirements; each adapter checks its
own complete registered module closure, so a Content installation needs no unrelated
Reliability execution wrappers.

No recovery continuation, reauthorization, automatic retry, Content running-cancellation
contract, publication promotion or operational activation is added. Public DNS/TLS/IPv4,
packaged public API/worker acceptance, actual public live-child stopping with fresh quiescence
review and browser acceptance remain separate open gates.

## Acceptance criteria established before implementation

- Exact immutable public composition and installed-runtime closure; reject missing, changed,
  incompatible or duplicate profiles/dependencies and historical loopback-v1 dispatch.
- Read-only startup and one fixed pipeline; shared Content/Reliability/import exclusion,
  queue ordering and frozen source/publisher selection.
- Genuine claim, PREPARING/READY and existing complete native original-chain execution;
  a second eligible claim uses only the acknowledged successful session.
- Persistent stopping and bounded native cleanup; retained evidence/occupancy after
  interruption, cleanup uncertainty and lost claim/admission/stage/final acknowledgments.
- PostgreSQL restart containment and existing Reliability execution, cancellation, retry,
  history and schema compatibility regressions.
- Exact-head complete hosted CI, explicit failures/skips and capacity evidence. Offline
  loopback/native integration is never labeled successful public-feed qualification.

See the [implementation and qualification record](../operations/e5-2c-original-content-worker.md).
