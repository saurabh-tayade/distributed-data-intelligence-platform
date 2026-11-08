# Ordinary original-only Content worker qualification

This is the candidate qualification procedure for [ADR 0045](../adr/0045-ordinary-narrow-content-worker.md)
and [ADR 0046](../adr/0046-bounded-public-fetch-lane.md). It is not deployment approval.
The implementation is unmerged and must only target disposable installations and queues until
independent review, exact-head complete CI and actual public packaged acceptance pass.

## Installation and selection

Use PostgreSQL 17, a complete qualified Java 21 JDK, the pinned Python 3.12.15 environment and
repository processing dependencies. Build the ingestion artifact and its separate
`public-fetch-provider` classifier, then freeze their bytes throughout native qualification.
Do not rebuild an installed JAR while a worker or test is using its digest/identity pin.
The provider service must not be present in the shared ingestion JAR or historical classpaths.
No dependency or network-setting change is part of this procedure.

Supply the existing `DDIP_DB_URL`, `DDIP_DB_USER`, nonblank `DDIP_DB_PASSWORD` and
`DDIP_INSTALLATION_ID` through the local environment, never command-line credentials.
For a disposable database, apply and verify migrations through V23 using the packaged role:

```bash
DDIP_ROLE=migrate java -jar services/content-api/target/content-api-0.1.0-SNAPSHOT.jar
```

V23 is additive. It preserves v1 registration/reservation/history semantics and adds exact public
composition-v2 validation. It does not rewrite existing profiles, queued inputs or receipts.
The API and worker verify schema compatibility; they do not silently migrate it.

The trusted operator must register the immutable storage/runtime chain and RunProfile v2 under the
existing registration controls, then register exactly one complete public pipeline for that
RunProfile. There is no new profile-registration HTTP route or installer in this milestone.
The exact canonical contracts are in ADR 0046 and
[V23](../../services/content-api/src/main/resources/db/migration/V23__bounded_public_fetch_composition.sql):

- The existing FETCH runtime descriptor retains `testEndpoint=false` and its historical identity.
- The distinct strict public FETCH launch profile is version 2, names
  `StrictPublicManagedRssFetch`, binds the fixed network-policy bytes/digest and includes the separate
  hashed provider JAR. It is not a modified strict-loopback v1 launch.
- Bronze launch and verification profiles are composition version 2 and bind that exact FETCH
  launch ID/digest. Their native storage/command formats remain unchanged.
- The strict Content pipeline is composition version 2 and binds all three exact launch/verification
  profile IDs and digests plus the existing runtime, modules and versions.
- Storage paths, identities, private control/temp modes, private credential file, free-space limits,
  and installed runtime closure must satisfy the existing checks. Startup pins the exact validated
  credential bytes/identity without storing or logging their contents.

Set the following existing worker variables to those registered values:

```text
DDIP_WORKER_EXECUTION_PROFILE_ID=<RunProfile v2 ID>
DDIP_WORKER_CONTROL_BINDING_ID=<shared installation control binding ID>
DDIP_WORKER_DEPLOYMENT_SHA256=<RunProfile deployment digest>
DDIP_WORKER_RUNTIME_PROFILE_ID=<strict Content pipeline profile ID>
DDIP_WORKER_PYTHON=<absolute pinned interpreter path>
DDIP_WORKER_JAVA_HOME=<absolute qualified JDK path>
DDIP_WORKER_PROCESSING_ROOT=<absolute pinned processing directory>
DDIP_WORKER_POLL_MILLIS=1000
```

The Java process must itself use that JDK. Start a disposable worker with:

```bash
DDIP_ROLE=worker "$DDIP_WORKER_JAVA_HOME/bin/java" \
  -jar services/content-api/target/content-api-0.1.0-SNAPSHOT.jar
```

A worker selects one fixed profile for its entire lifetime: Reliability or original-only Content,
never both. Content startup requires exactly one matching complete pipeline, public composition v2,
installed byte/version closure and the shared host guard. Historical loopback composition v1 cannot
be selected by ordinary Content dispatch. Startup probes do not resolve a feed, write an object,
prepare an attempt or create synthetic READY authority. Metadata discovery remains registration
metadata, not an installed-runtime readiness claim.

The separate API uses the existing reviewed public-source catalog and management configuration.
Ordinary Content admits only one fresh source, no retained Silver and the qualified publisher
reference, under the unchanged evidence bounds. Source revisions and selected endpoint bytes are
frozen by submission. A later catalog or source change cannot redirect a queued attempt.

## Failure and stopping behavior

Only an acknowledged claim enters PREPARING and the existing original chain. The narrow attempt
remains CLAIMED during execution. The single dispatcher owns heartbeat/session updates and the
actual acknowledged success transaction returns the only session usable for another claim.

Uncertain registration, claim, admission or final acknowledgments stop dispatch. A process cannot
reset uncertain startup by constructing another repository and calling start again. Content errors
never use Reliability's definitive prelaunch release shortcut. Stopping reaches admission/probe
fences and native supervisors; the dispatcher retains its guard until bounded child cleanup returns.
A cleanup timeout or unobservable child is an occupied failure, not evidence of safe release.

Preserve the database and private evidence on failure. Do not retry, adopt, repair, delete, refetch or
reconstruct a live owner from history. Pre-READY partial admission can remain explicitly blocked.
Eligible interrupted public originals use only the existing full-Content
[review and failed safe-end procedure](native-content-restart-qualification.md), with a fresh review
of actual quiescence and unchanged evidence. Stopping a worker alone does not prove quiescence.
Public attempts cannot create a new legacy FETCH-only recovery handoff; old v1 receipts retain their
historical read/finalization semantics. Issues #123 and #124 remain deferred.

## Offline evidence and public acceptance

Normal CI remains offline/loopback. The exhaustive PostgreSQL inventory assigns all new suites
explicitly; [CI operations](ci.md) records the current partition counts. The tests cover:

- Startup/profile closure, credential replacement/deletion/mode loss, queue/profile exclusion,
  frozen selection, global guards/import exclusion and uncertain lifecycle acknowledgments.
- Public policy classification, provider packaging/selection, immutable address pinning, Host/SNI,
  default TLS rejection and negative production-main packets without public network traffic.
- Exact versioned reservation/launch/Bronze history, later unrelated registration stability,
  v1-only new FETCH recovery handoff in Java and SQL, old receipt upgrades and failed safe-end.
- Existing real loopback full-chain success, acknowledged next-claim session and shared native
  supervisor/stop/uncertain-success behavior.

Synthetic native-format history fixtures are component evidence. They do not show a public feed
was fetched, a native public full chain completed or a packaged public child stopped safely.

The separate opt-in entrypoint is:

```bash
scripts/qualify-ordinary-public-content.sh
```

Read its required environment inputs first. It requires an explicitly reviewed, fixed-commit
`raw.githubusercontent.com` HTTPS URI for a tiny immutable single-entry Atom fixture, its exact
payload SHA-256 and expected title, both installed ingestion/provider JAR digests, the qualified
runtime, and an exclusively reserved disposable PostgreSQL instance. It never publishes or
prefetches a fixture. No public run is part of default tests, Maven profiles or hosted workflows.
The fixture dependency must be disclosed; it is not proof that every real catalog feed is available
or semantically supported.

A permitted execution environment must have working native IPv4 name resolution and direct
DNS/TLS/HTTP egress. Proxy-backed metadata reads do not establish this. Do not change network
settings, substitute fixed IPs, add a proxy or use another automation route to evade a restriction.
A blocked public qualification remains blocked; run it only after an authorized usable environment
and reviewed fixture are available.

The harness uses the packaged production API and worker, actual reviewed-catalog loading and the
real public adapter. Retain its private output directory and disposable database on failure.
Review its final evidence rather than interpreting process exit alone as qualification.

## Remaining release gates

Until recorded against the final source/artifacts, all of the following remain required:

1. Independent final source review and exact-head complete CI, with strict inventory/report coverage
   and actual partition capacity results.
2. Actual production-child public DNS/TLS/IPv4 success with the reviewed fixture and unchanged
   provider selection/address policy. Native egress is not yet qualified in this workspace.
3. Packaged API submission through all 17 native operations, verified result/read API, a subsequent
   run on the same worker, immutable selection and duplicate submission with no rewritten outputs.
4. Ordinary packaged stopping during a live native child, retained occupancy/evidence, and fresh
   failed safe-end review where the existing contract permits it. Happy-path success and separate
   supervisor tests alone do not close this composed stopping gate.
5. Explicit activation decision after these results. Operations/UI acceptance and final issue #49
   closure remain separate deliverables.
