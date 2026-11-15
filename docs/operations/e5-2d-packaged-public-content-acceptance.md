# E5.2-D: Opt-in packaged public Content acceptance

Tracking: [#49](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/issues/49),
[#47](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/issues/47).
Scope approval: [owner checkpoint](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/issues/49#issuecomment-6882282491).
Contracts: [ADR 0010](../adr/0010-durable-execution-coordination-and-recovery.md),
[0011](../adr/0011-artifact-import-maintenance-ownership.md),
[0043](../adr/0043-integrated-original-content-pipeline.md),
[0044](../adr/0044-interrupted-content-safe-end-contract.md),
[0049](../adr/0049-inactive-bounded-public-content-composition.md) and
[0050](../adr/0050-original-only-content-worker-lifecycle.md).

This is a separate, explicit qualification procedure for a disposable installation.
It does not enable ordinary operational Content dispatch or select a new Analytics publication.
Do not invoke it from a service, ordinary startup, a scheduler or default CI.

## Acceptance criteria and boundaries

Before any API mutation or worker start, independently establish the exact fixture, installed
artifacts, approved Linux target, immutable public-v2 profile closure, genuine completed publisher
reference import, idle durable coordinator, fresh installation and real storage identities.
The runner may then submit only the original narrow `CONTENT_RSS_V1` request through the packaged API.
It may launch only the packaged worker, which establishes its own acknowledged claim and admission.
There is no direct pipeline, native child, test capability, SQL owner or bootstrap entry point here.

| Boundary | Evidence required | Current qualification outcome |
| --- | --- | --- |
| Offline validation | Deterministic refusal/identity/isolation/evidence tests; compilation | Reported separately from network acceptance |
| Native public DNS diagnostic | Actual JDK IPv4 answers in the approved Linux target | BLOCKED: no target-specific authorization supplied |
| Production-child public DNS/IPv4/HTTPS/TLS | Retained production FETCH launch/checkpoint, original fixture URI, exact payload, qualified installed public provider | NOT RUN; offline tests cannot establish this |
| Packaged API-to-worker completion | Genuine source/revision/submission/claim/PREPARING/READY, all 17 checkpoints, independent native reads, verified registry/read API | NOT RUN |
| Second run and replay | Same worker instance, next epoch after acknowledged idle, immutable file/SQL/object comparisons | NOT RUN |
| Live-child stopping | Separate fresh installation, recorded released native child, intentional worker SIGTERM, retained occupancy/evidence | NOT RUN |
| Reviewed quiescence and any eligible existing safe-end | Fresh operator review of the actual process/storage/SQL evidence under ADR 0044 | OPEN; never automated by this runner |
| Browser acceptance | Separate owner-reviewed browser evidence | OPEN |
| Operational activation | Separate explicit owner decision | NOT AUTHORIZED |

The preparation baseline is main `63afe5c69d0c3f0c0982f3c0a27edcaf0c85d303`.
Its post-merge [CI #43624475522](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/actions/runs/43624475522)
completed successfully on attempt 1: all 20 jobs passed, from 18:03:48 to 18:58:45 UTC on
November 14, 2026. No rerun was requested. Protect Main remains unchanged: PR required,
four required contexts, no bypass, deletion or non-fast-forward updates. This checkpoint
requires complete exact-head PR CI and separate owner review; it grants no merge authority.

## Reuse and compatibility review

Closed/unmerged [PR #137](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/pull/137),
head `9b741b0869a9297acb69900b2d24358a882656c6`, supplied useful observations: real HTTP
submission, packaged role processes, same-instance/next-epoch checks, 17-operation history,
frozen selections, payload digest and completed replay. Those observations are adapted here.

Its qualification class inherited database fixtures, inserted runtime/artifact fixtures, used
an in-process S3 substitute, delayed an object request for stopping, repinned Analytics to the
new publication and cleaned up processes unconditionally. None of those mechanisms is reused.
The new helper inherits no fixture and its JAR contains only the standalone helper classes.

Production code is reused unchanged: `SchemaLifecycle.verify(false)`, `ImportBindings`,
`NarrowContentRuntimeQualification.registration`, `NarrowContentWorkerStartup.startup`,
the strict public FETCH/Bronze/pipeline startup checks, `NarrowContentFetchCompletion.inspect`,
the management API and the packaged worker. The independent publisher-reference and Content
readers are the existing installed `publisher_reference.open_snapshot` and
`content_analytics.open_publication`. These reads do not promote a publication or change API
Analytics selection. Existing Reliability cancellation/retry/history behavior, migrations
V1–V23, native evidence formats and product fingerprints are unchanged.

Requiring a prepared operator installation avoids inventing a second profile installer or
execution authority. The tradeoff is deliberate: missing immutable registrations, an absent
genuine publisher import, unavailable service identities or an unapproved target are blockers,
not work that the acceptance runner silently performs.

## Required operator preparation

Use a persistent, explicitly reserved disposable Linux target with JDK 21, Python 3.12.15,
PySpark 4.0.2, DuckDB 1.4.5 and the registered Boto3/runtime versions. The target and its service
volumes must remain available after the runner exits, including failure or operator interruption.
Do not run the runner as the lifetime-defining command of an automatically removed container.
Retained installations from earlier qualifications are excluded.

The target needs actual native IPv4 DNS and direct HTTPS/TLS/HTTP egress, an unchanged resolver,
hosts file, route and TLS trust configuration, and no proxy. A GitHub connector, web-tool fetch,
synthetic resolver, local feed server or offline fixture does not establish that capability.
Do not supply an IP address, change `/etc/hosts`, alter DNS, disable certificate checks, change
proxy settings or modify routing to obtain success. If existing restrictions prevent execution,
report BLOCKED. The stock JDK DNS diagnostic never supplies its answers to the production child;
the production child independently resolves, filters and pins its own immutable destination.

Prepare PostgreSQL 17 and real SeaweedFS using the already reviewed images:

```text
postgres@sha256:3645570cccdfa447589da9f57dd740faa29b30938e861289a5574b6ca6b03826
chrislusf/seaweedfs:4.48@sha256:4e61d15fd35994cb1e43e1e553dff106794841fd9a99ade2fc8c8bfce4d7872d
```

Both containers must already run, have exact approved container/image IDs, publish their selected
5432/8333 ports only on `127.0.0.1`, and carry `ddip.qualification=e5-2d` and
`ddip.qualification.installation=<installation UUID>` labels. The runner only inspects them.
It never starts, restarts, relabels, reconfigures or deletes a service or volume. The safe Docker
projection retains mount identities, image reference, image ID, container ID, start time and ports;
it does not read service environment variables. PostgreSQL system identifier, database OID,
postmaster start time and version must match the independently approved installation.

Under existing trusted provisioning controls, prepare the private CONTROL guard/installation
files, schema V1–V23 and coordinator, canonical immutable RunProfile v2 and complete strict public
composition v2, public FETCH provider/classpath, Bronze launch and independent verification
profiles, storage bindings and installed module/runtime manifests. Freeze the complete approved
public launch classpath: ingestion JAR, every runtime dependency JAR in registered order, and the
public provider last. Ingestion is a thin JAR; its libraries cannot be omitted. Local mutable storage roots must be
under the dedicated installation root; immutable runtime/code/JAR files and qualification evidence
must be outside it. SILVER/GOLD/CONTENT/STAGING/TEMP roots and the real Bronze bucket must be empty.

Import exactly one publisher reference using the existing packaged `operator-import` role and
its real import ownership. Preserve the resulting successful `IMPORT`/`SAFE_COMPLETION` operation,
artifact ID and evidence SHA-256. Do not insert a reference artifact directly or use test fixtures
to fake an import. There must be no source, run, attempt, worker instance, control or legacy/native
execution history yet. This runner supplies neither provisioning SQL nor an alternative import path.
If these prerequisites cannot be established legitimately, stop at that dependency.

## Immutable fixture

The only eligible URI is:

```text
https://raw.githubusercontent.com/saurabh-tayade/distributed-data-intelligence-platform/6584d52153e599ce8f4a5c314e13535f2e7e7e1b/ingestion/src/test/resources/feeds/atom-public-qualification.xml
```

The payload is exactly **726 bytes**, including its final LF. SHA-256:
`1f99a72b80837b4239189a6b02d4f143b104aae845d2f5ece77776705a66e81c`.
The sole Atom entry title is `A paper star in an imaginary observatory`.
PR #138's merge commit is `6584d52153e599ce8f4a5c314e13535f2e7e7e1b`;
fixture blob `848fd565b99b8ff0c46cd5ff4f6304fac8c11452`.

The preparation independently compared connector-returned fixed-commit bytes, `git show` at
that commit and the current local fixture. All matched. This is fixture identity evidence,
not proof of native egress. Retain that reviewed local copy and hash it before any native network
operation. The runner does not prefetch the source. Each explicitly submitted execution gets
its own single production FETCH application invocation; an ambiguous invocation is never repeated.

The unchanged child retains public-address filtering, IPv4-only bounded resolution, immutable
address pinning, original Host/SNI, default TLS validation, proxy rejection, redirect refusal,
5 MiB body limit, 10-second connect limit, bounded request time and 45-second FETCH supervision.
JDK reconnects, if any, remain bounded to the same approved pin under ADR 0049.

## Build before freezing installed artifacts

Build in an isolated workspace before making the plan or obtaining approval. Do not rebuild or
replace installed artifacts during qualification. Existing Maven dependencies are sufficient.

```bash
mvn -B -ntp -f ingestion/pom.xml verify
mvn -B -ntp -f services/content-api/pom.xml verify
```

Create a separate runner JAR containing only the compiled standalone helper; no test fixtures,
JUnit, `TestSupport`, application classes or dependencies belong in it:

```bash
python3 -I -B - <<'PY'
from pathlib import Path
import zipfile
classes = Path('services/content-api/target/test-classes').resolve()
prefix = 'platform/contentapi/execution/PackagedPublicContentAcceptance'
selected = sorted(classes.glob(prefix + '*.class'))
selected = [p for p in selected if p.name == 'PackagedPublicContentAcceptance.class'
            or p.name.startswith('PackagedPublicContentAcceptance$')]
assert selected
destination = Path('outputs/e5-2d-install/acceptance-runner.jar')
destination.parent.mkdir(parents=True, exist_ok=True)
with zipfile.ZipFile(destination, 'x') as jar:
    for path in selected:
        jar.write(path, path.relative_to(classes).as_posix())
PY
```

Install the frozen JARs, Java/Python binaries and processing modules under canonical immutable
paths. Independently record each SHA-256. The runner uses the API JAR's existing Boot
`PropertiesLauncher` only to load the observer with the packaged production classes/dependencies.
API and worker children use **`java -jar <exact API JAR>`**, without the runner/test classpath.
They inherit only the documented environment and load the packaged application configuration.

## Exact plan and target approval

The launcher is:

```bash
bash scripts/qualify-ordinary-public-content.sh validate /absolute/private/plan.json
bash scripts/qualify-ordinary-public-content.sh preflight /absolute/private/plan.json /absolute/private/approval.json
bash scripts/qualify-ordinary-public-content.sh run /absolute/private/plan.json /absolute/private/approval.json
```

`validate` is offline and creates no output or process. It reports `OFFLINE_INPUTS_VALID`, never
acceptance. `preflight` requires an approved target, creates a new private evidence directory,
checks real read-only PostgreSQL/storage/runtime prerequisites and performs a bounded native DNS
diagnostic. It does not fetch HTTPS, start the API/worker or submit a source/run. `run` repeats
the prerequisites before effects. Use a new evidence directory, newly frozen plan digest and
corresponding approval record after a preflight; a previous directory cannot be resumed or
overwritten. An existing owner decision can cover both plans and invocations; do not request
another approval when its authorized target and scope already cover the action.

The JSON plan is a closed object. Unknown/duplicate fields, historical profiles, extra catalog
entries, remote databases, symlinks, hard-linked artifacts, mismatched/missing digests, shared
writable inputs, runtime/output overlap and existing evidence directories are refused.
Use actual values, not the placeholders below:

```json
{
  "planVersion": 1,
  "scenario": "complete",
  "installationId": "<canonical UUID>",
  "installationRoot": "/srv/ddip-qualification/state",
  "evidenceDirectory": "/srv/ddip-qualification-evidence/new-complete",
  "database": {
    "url": "jdbc:postgresql://127.0.0.1:15432/qualification",
    "user": "qualification",
    "systemIdentifier": "<actual decimal system identifier>",
    "databaseOid": "<actual decimal database OID>"
  },
  "artifacts": {
    "api": {"path": "<absolute API JAR>", "sha256": "<64 lowercase hex>"},
    "ingestion": {"path": "<absolute ingestion JAR>", "sha256": "<64 lowercase hex>"},
    "provider": {"path": "<absolute provider JAR>", "sha256": "<64 lowercase hex>"},
    "runner": {"path": "<absolute observer JAR>", "sha256": "<64 lowercase hex>"},
    "java": {"path": "<canonical JAVA_HOME/bin/java>", "sha256": "<64 lowercase hex>"},
    "python": {"path": "<canonical Python executable>", "sha256": "<64 lowercase hex>"},
    "catalog": {"path": "<absolute exact reviewed catalog>", "sha256": "<64 lowercase hex>"},
    "fixture": {"path": "<absolute verified local Atom copy>", "sha256": "1f99a72b80837b4239189a6b02d4f143b104aae845d2f5ece77776705a66e81c"},
    "docker": {"path": "<canonical Docker executable>", "sha256": "<64 lowercase hex>"}
  },
  "nativeClasspath": [
    {"path": "<same absolute ingestion JAR>", "sha256": "<same digest>"},
    {"path": "<each runtime dependency JAR, in registered order>", "sha256": "<its digest>"},
    {"path": "<same absolute provider JAR>", "sha256": "<same digest>"}
  ],
  "runtime": {
    "javaHome": "<canonical installed JDK home>",
    "javaVersion": "<exact Runtime.version() string beginning 21.>",
    "processingRoot": "<canonical frozen processing directory>"
  },
  "profiles": {
    "pipeline": "CONTENT_RSS_V1", "runProfileVersion": 2, "compositionVersion": 2,
    "executionProfileId": "<canonical registered UUID>",
    "executionProfileSha256": "<64 lowercase hex>", "deploymentSha256": "<64 lowercase hex>",
    "runtimeProfileId": "<registered token>", "controlBindingId": "<registered token>",
    "publisherReferenceArtifactId": "<actual imported UUID>",
    "publisherReferenceEvidenceSha256": "<64 lowercase hex>",
    "sourceId": "qualification-feed", "endpointId": "<reviewed catalog UUID>",
    "bronzeBindingId": "<registered catalog/storage UUID>"
  },
  "services": {
    "postgres": {"containerId": "<64 lowercase hex>", "imageId": "sha256:<64 lowercase hex>"},
    "seaweedfs": {"containerId": "<64 lowercase hex>", "imageId": "sha256:<64 lowercase hex>"}
  },
  "apiPort": 18080,
  "deadlineSeconds": 900,
  "fixture": {
    "uri": "https://raw.githubusercontent.com/saurabh-tayade/distributed-data-intelligence-platform/6584d52153e599ce8f4a5c314e13535f2e7e7e1b/ingestion/src/test/resources/feeds/atom-public-qualification.xml",
    "sha256": "1f99a72b80837b4239189a6b02d4f143b104aae845d2f5ece77776705a66e81c",
    "bytes": 726,
    "title": "A paper star in an imaginary observatory"
  }
}
```

The catalog must be catalogVersion 1 with exactly one endpoint for this source/endpoint UUID
and the exact fixed fixture URI, no Reliability profiles, and exactly one storage profile:
the selected Bronze UUID, product `content-intelligence`, role `BRONZE`.
The installation and evidence parent directories must be owned private mode 0700. Artifacts
must be bounded regular, canonical, singly linked and not group/other writable. Executables
must be executable. Runtime and native output roots must be disjoint.

An approval file is a record of an **actual owner decision**, not a CLI authorization shortcut.
The owner decision must explicitly name the disposable target, unchanged direct native egress,
service/storage identities, exact plan SHA-256 and scenario. General milestone approval alone
does not identify a target. Do not generate a purported approval to make a gate pass.
The closed approval record contains:

```json
{
  "approvalVersion": 1,
  "ownerDecision": "https://github.com/saurabh-tayade/distributed-data-intelligence-platform/issues/49#issuecomment-<actual target approval>",
  "planSha256": "<SHA-256 of exact plan file bytes>",
  "installationId": "<same UUID>",
  "hostname": "<actual Linux hostname>",
  "bootId": "<actual /proc/sys/kernel/random/boot_id>",
  "expiresEpochSeconds": 0,
  "disposable": true,
  "directNativeEgress": true,
  "scenario": "complete"
}
```

Replace the expiry with an approved future time no more than 24 hours away. The Java observer
independently rechecks this binding before connecting to the database. The record is a local
audit input, not a cryptographic substitute for reviewing the linked owner decision.
Supply the database password only as `DDIP_DB_PASSWORD`, using the target's existing secret
handling. Do not store it in the plan, command line, report or Git. Inherited proxy variables,
JVM injection options and application overrides are rejected; the runner does not remove a
configured proxy to force access. Do not edit network settings in response to that refusal.

## Scenario execution

For `complete`, the runner starts the real API on an explicitly selected private port, verifies
that its actual PID owns the listener, creates the reviewed source through HTTP, reads revision
history and submits revision 1 with the exact imported reference and no retained Silver.
A same-key duplicate must return the same run before a worker exists. The packaged worker then
claims/admit/executes the original request. Before the next submission, the runner observes durable
success/release and a fresh `WORKER_QUEUE_IDLE` acknowledgment from that same live worker.

It verifies the frozen source/reference selection, actual original attempt, PREPARING/READY,
all 17 ordered native checkpoints, raw HTTP payload, native command/proof hashes, final completion
and dependency references, management read API and an independent native Content catalog read.
The second execution must use the same worker instance and next owner epoch. A completed replay
must preserve run/attempt/native/registry evidence, local file identities/bytes/mtime and the four
real Bronze objects' bytes/identities. Coordination heartbeat fields are reported separately.
Only after verified successful idle release does it stop its own idle worker/API, verify STOPPED,
and retain the entire installation. It never starts a newly selected Analytics publication.

For `live-stop`, use a **different fresh installation and plan/approval**. The runner submits one
original request, observes an actual released `PUBLISH_SILVER` native child with its PID/start
identity and ancestry and a real Spark JVM below it (release authorization alone precedes stdin
delivery), records its descendants and deliberately sends SIGTERM only to its own
packaged worker. There is no artificial object-store delay or source refetch. Missing the live
window is a failed qualification, not permission to submit another run. Worker exit must be
bounded and the recorded child must no longer be that live process; SQL occupancy and incomplete
native evidence must remain retained. The API, services and data remain available for review.

A worker exit, a dead PID or released host guard does not prove all writers quiescent.
Review retained native launch phases, child/descendant PID/start identities, Linux boot identity,
SQL occupancy and storage evidence afresh using ADR 0044 and the existing read-only operator
assessment. The runner never invokes an assessment to produce a terminal authorization, safe-end,
retry, recovery, cleanup, reauthorization or publication promotion. Any eligible failed safe-end
remains a later separately reviewed operator action.

## Failure and evidence handling

Each structured observer record is new, private and forced to disk. Process logs are retained.
Existing directories or observations are
never resumed or overwritten. Native evidence stays owned by the production worker and in its
existing formats; observer records live outside CONTROL and output roots. Raw SQL bytea exports
are preserved by SHA-256, with snapshots referencing exact bytes. Local inventories include file
identity, size, mtime and digest and refuse symlinks, hard links, excessive depth/count/bytes.

Retain `plan.json`, `approval.json`, `launch.json`, `database-identity.json`, service/mount/storage
identities, registered chain/composition/launch configuration, runtime/fixture hashes, native DNS
observations, API/worker process identities/logs, source/revision/run/history API responses, SQL
snapshots/raw bytea, local/S3 inventories and independent native read results. The database,
service volumes and native installation paths remain the authoritative inspectable originals.
Do not upload private plans, credentials, raw evidence or service volumes to public CI artifacts.

On a rejected prerequisite, classify BLOCKED. On a mutation acknowledgment loss, worker death,
uncertain execution, timeout, missing checkpoint, changed output or failed success/release check,
stop progression and report `FAILED_OR_AMBIGUOUS_RETAINED`; never report acceptance. There is no
unconditional `finally` cleanup. Do not restart or stop retained processes merely because the
observer failed. Do not reissue the ambiguous POST or FETCH. Preserve the target and review it.

## Preparation validation and remaining checkpoint

The deterministic launcher suite runs without DNS, HTTP, Docker, a database or a worker:

```bash
python3 -I -B -m unittest discover -s scripts/tests -p test_packaged_public_content.py -v
python3 -I -B -m unittest discover -s .github/ci/tests -v
```

`PackagedPublicContentAcceptanceTest` exercises append-only private evidence, bounded native
inventories, symlink/hard-link/depth refusal, SQL byte exports and authority exclusion. These
ordinary unit tests do not execute the acceptance helper. Default CI runs only offline refusal
tests and the existing native/PostgreSQL/ingestion/Content/Reliability/migration coverage.
There is no public-network CI job or automatic activation switch.

Local validation used the existing `ddip-pr71-linux` image, ID
`sha256:3c8a4598ffd777da0c786f45a2a5249f97632877eb04f5e1f830d70157caf2d7`, Linux aarch64,
Temurin `21.0.12.1+1-LTS`, Python 3.12.15, PySpark 4.0.2, DuckDB 1.4.5 and Boto3 1.43.114.
Builds used an ignored copied workspace and the existing read-only Maven cache. The separate
`ddip-e52d-offline-pg` PostgreSQL 17 fixture has no published port and an internal Docker network
with no external egress. These are offline test resources, not an approved public installation.

| Actual command/selection | Result |
| --- | --- |
| Launcher unittest above, final complete-classpath version | 40 passed; no failures/skips |
| `PackagedPublicContentAcceptanceTest`, final focused JUnit invocation | 19 passed; no failures/skips |
| `mvn -B -ntp -o -Daether.syncContext.named.factory=noop -f ingestion/pom.xml verify` | 338 passed; no failures/skips; 1m23s Maven |
| `bash ingestion/scripts/public-fetch-packaged-smoke.sh` | 3 production-main refusals plus JAR isolation passed; no DNS/HTTP reached |
| Content API `verify` with the same Maven flags | 997 discovered, 994 passed, 3 existing optional corpus skips; no failures/errors; 1m31s Maven |
| Content API `verify -Ppostgres`, focused selection below | 164 integration cases passed; no failures/skips; 8m33s Maven including the then-current 988-unit suite |
| CI policy unittest above | 70 passed |
| Installed Node 24 `npm run check` | Markdown/links, hygiene and whitespace passed |

The focused PostgreSQL selection is:

```text
NarrowContentWorkerStartupIT,OrdinaryContentWorkerLifecycleIT,OrdinaryContentLegacyLifecycleIT,
PersistenceIT,WorkerCoordinationIT,
NarrowContentPipelineIT#originalFetchThroughContentUsesAllSeventeenOperationsAndRealFinalProof
  +emptyFeedAndLostFinalCommitAcknowledgmentLeaveDurableVerifiedSuccess
  +persistentStopAfterPreflightPreventsFetchAndRetainsOriginalOccupancy
```

Join this into one comma-separated `-Dit.test` value without the visual whitespace.
The local integration reuses existing deterministic fixtures and actual PostgreSQL/native
processing. It is not a test-only public activation path and is not public-feed qualification.
Counts overlap and must not be added as distinct coverage. The final unit skips are
`ContentEnvelopeMeasurementTest`, `NarrowPublicationEnvelopesTest` and
`PublicationProjectionParityTest`, whose separately supplied optional corpora were absent.

An initial host Python 3.9 run found an unsupported `Path.stat(follow_symlinks=...)` call in the
new launcher. It now uses bounded `O_NOFOLLOW` reads and `lstat`; host and Linux offline tests
pass. An initial network-disabled backend unit run omitted the existing CI
`SPARK_LOCAL_IP=127.0.0.1` test setting; one native parity case failed on container-hostname
resolution. The focused rerun and final full run with that established offline setting passed.
No public DNS or network setting was changed. Initial logs/reports remain retained.

Independent installed-manifest review also caught a thin-JAR compatibility assumption before
publication: the approved public manifest includes 41 JARs in this build. The runner now pins
the complete classpath and requires exact registered order/digests; focused dependency mutation,
omission, reordering and observer-exclusion tests pass. Production startup checks are unchanged.

Local preparation evidence is retained under `outputs/e5-2d-20261114T183920Z/`, including fixture
comparison, GitHub main CI/rules/issues and the reviewed PR #137 candidate copies. Exact commands,
runtime identities, counts, failures/skips and final PR CI are recorded in the review evidence.
The pre-existing untracked `docs/reviews/` and prior qualification evidence are excluded from this
change. Generated builds, plans and evidence remain ignored.

No named disposable Linux target was supplied for this preparation. Public execution is therefore
**BLOCKED**, not passed. No public child, source submission, worker, database import or service
installation was activated by this procedure during preparation. The next safe checkpoint is
owner review of this draft and its exact-head CI, followed by a target-specific approval and review
of the prepared installed manifests/registrations/import before the first opt-in execution.
Real public execution, complete packaged acceptance, live-child stopping with reviewed quiescence,
browser acceptance and operational activation remain separate open gates.
