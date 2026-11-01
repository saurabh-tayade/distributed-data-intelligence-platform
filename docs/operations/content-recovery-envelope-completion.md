# Gate B0 completion investigation: retained evidence and interruption safety

Historical investigation at the baseline below. The subsequent
[bounded-producer implementation report](content-evidence-producers.md) records the code,
new command protocol, validation candidate and remaining Gate B0 blocker. Statements about
missing producer safeguards in this document describe that earlier baseline.

October 31, 2026. **Gate B0 remains incomplete. Content dispatch is not safe to consider
for Gate B1.** No supported operational Content profile satisfying the requested guarantee
has been established. Production dispatch remains disabled; no enable switch is added.

This investigation starts at merged baseline `cda404a`, containing inherited recovery and
capture depth hardening. The prerequisite's hosted-CI result is owner-supplied; no remote
Git/GitHub operation was performed. The earlier [B0 audit](content-recovery-envelope.md)
is historical evidence, not proof of an execution envelope. This report adds native fixture
measurement and executable interruption counterexamples. No production, SQL migration or
native processor bytes change.

## Actual limits: enforcement versus assumptions

| Boundary | Enforced now | What it does not establish |
| --- | --- | --- |
| Content submission | 1–8 fresh feeds, 0–128 retained Silver IDs, one publisher reference; 64 KiB request | Recovery capacity or native compatibility of every accepted combination |
| Reliability submission | 1–32 sources/batches | A Content limit; unchanged |
| Native Gold inputs | At most 128 total selected Silver markers | API acceptance of eight fresh plus 128 retained is not native admission |
| RSS bytes | 5,242,880 bytes/feed, enforced during fetch | Sidecars, metadata or qualification budget |
| Private capture | 67,108,864 regular-file bytes, 4096 entries including root, depth-four files; directories at depth four rejected | Pre-write reservation, ancestry quota, installation quota or safe truncation |
| Per-file recovery read | Payload 5 MiB; ordinary sidecar 16 MiB | Producer quota; some stage readers are smaller |
| Canonical inventory / recovery JSON | Independently at most 1,048,576 bytes each | An inventory fitting does not prove the packet containing it plus snapshots/review fits |
| Java private writes | Precheck 64 KiB, 512 KiB or 16 MiB according to file; CREATE_NEW, force and readback | Atomic creation across interruption or aggregate capacity |
| Content publisher INTENT | Input digest, intended fingerprint and namespace in SQL; command address/hash recorded at authorization | A recoverable command link at the pre-file interruption boundary |
| Gold/Content Python evidence | 16 MiB serialized evidence prechecked before file creation | Command + all other evidence fits 64 MiB; a write cannot be partial |
| Silver Python VERIFY output | Java checks evidence and selection at 512 KiB after child writes them | A pre-write 512 KiB producer bound; neither write has that check |
| Run/profile | RunProfile V1 pins reviewed runtime/deployment; 20 attempts, recovery at most 512 stage rows | Byte/entry/metadata budget fields or a limit on every inspection/qualification directory |
| Storage/resources | Exact canonical roots, device/inode, current-user private paths, nonoverlap, point-in-time free-space checks | Reserved capacity, hard filesystem quotas or a hard Spark-memory quota |

Content has not admitted 32 feeds: the first submission implementation `b36d2ae` already
used `content ? 8 : 32`. The erroneous 32-feed statement originated in documentation at
`c5c2a11`. The corrected contract and all migration checksums remain unchanged.

The 64 MiB / 4096-entry limits apply to **one captured UUID tree**, not all ancestry.
Original EXECUTE, earlier RECONCILE, RESUME-control qualification, resumed EXECUTE and current
RECONCILE trees are separately sealed/requalified. Qualification JSON under
`CONTROL/recovery/<control>/<qualification>/qualification.json` is outside those trees.
SQL bytea fields are represented by SHA-256/length in snapshots, not copied payload bodies.
Native Silver/Gold/Content files and Bronze objects are referenced and independently verified.
Their body bytes are **excluded** from the private budget; embedded metadata/inventories count.

Native limits are not private limits: Silver qualification allows 128 analytical files per
accepted/rejected dataset, 512 namespace entries, depth six and 1 GiB total native bytes;
it retains complete.json as base64 plus directory names and file stamps/hashes in its proof.
Content metadata allows 8192 analytical files total, 4096 per dataset, 8 GiB native bytes,
512-byte relative analytical paths, Gold manifest up to 16 MiB and completion up to 8 MiB.
Publisher reference input is independently bounded (10 MiB CSV, 10,000 rows, 32 KiB vocabulary,
16 KiB manifest, 4 KiB completion). Those are native reader limits, not a jointly proved
maximum serialized managed proof or guarantees about Spark's generated file shape.

Runtime preconditions remain Java 21, Python 3.12.15, Spark 4.0.2, DuckDB 1.4.5 and pinned
module/deployment hashes, exact local storage identities, loopback Bronze and separate TEMP.
D1 allows HTTP timeout at most 30s with a 45s supervised fetch. Spark stage profiles allow
at most 900s. The fixed supervisor drains at most 16 MiB per stdout/stderr stream, observes
at most 32 descendants in Spark mode, rejects descendants in ordinary read-only mode and
uses bounded cleanup waits (5s graceful, 5s forced, 5s drain). Streams are drained, not saved
as public/private process logs. A 1 GiB free-space observation at applicable CONTROL,
SILVER/TEMP, GOLD and CONTENT checks is not an aggregate disk reservation. Recovery still
requires a fresh 60s review and independent child/descendant identity checks. These bounds
are useful safeguards; none supplies the missing evidence-production budget.

## Complete fixed-path accounting and missing lifetime bound

S = 65,536, Q = 524,288, L = 16,777,216 bytes. Paths below are relative to
`CONTROL/runs/<run>/attempts/<attempt-or-control>`. Each work directory is new and retained.
These are checked-file ceilings for a successful original pass, no retained Silver and
no extra inspection/qualification calls. They are deliberately conservative, **not jointly
attainable worst-case claims**. A dash means there is no additional private file for that
conceptual stage. `child.json` is a process identity sidecar, not stdout/stderr.

| Stage / private directory | Every normal file and its checked ceiling | Files | Bytes per work directory | Origin / enforcement |
| --- | --- | ---: | ---: | --- |
| Initial CHECK_INPUTS | — | 0 | 0 | Captured selection/validation in SQL; later dependency CHECK_INPUTS listed below |
| FETCH_SOURCE / fetch-source/source/checkpoint | payload.xml 5 MiB; command.json, metadata.json, descriptor.json, result.json, complete.json S each | 6/feed | 5.3125 MiB/feed | Original fetched bytes plus generated journal/descriptor/seals; D1 fetch/journal bounds |
| STORE_BRONZE / store-bronze/checkpoint | command.json, evidence.json, result.json, verified.json S each | 4/feed | 0.25 MiB/feed | Receipt/PUT proof; Java/managed Bronze consumers; native objects retained separately |
| Initial Bronze requalification / inspect-bronze/checkpoint | command.json, evidence.json, result.json, verified.json S each | 4/feed | 0.25 MiB/feed | GET-only exact original receipt proof |
| SILVER / silver/checkpoint | command.json, child.json, result.json, verified.json S each | 4/feed | 0.25 MiB/feed | Generated invocation/process/publication anchor |
| Silver independent VERIFY / verify-silver/checkpoint | command.json, child.json, result.json, verified.json S each; evidence.json, selection.json Q each | 6/feed | 1.25 MiB/feed | Full native qualifier/capture and input selection; Q is a post-write Java read limit |
| Silver recheck / recheck-silver/checkpoint | command.json, child.json, result.json, verified.json S each; evidence.json Q | 5/feed | 0.75 MiB/feed | Copied verification evidence and generated read-only recheck |
| Further Bronze requalification | Three inspect-bronze directories/feed, same four S files | 12/feed | 0.75 MiB/feed | Gold prepare, Gold publish, Content Gold opener; references exact retained objects |
| PREPARE_GOLD / prepare-gold/checkpoint | command.json, evidence.json L each; child.json, result.json, verified.json S each | 5 | 32.1875 MiB | Generated preparation + embedded Silver evidence/manifest |
| PUBLISH_GOLD / publish-gold/checkpoint | command.json, evidence.json, selection.json L each; child.json, result.json, verified.json S each | 6 | 48.1875 MiB | Selection includes registered native artifact proof |
| Gold independent VERIFY / verify-result/checkpoint | command.json, evidence.json L each; child.json, result.json, verified.json S each | 5 | 32.1875 MiB | Full native publication proof/inventory |
| Gold recheck / check-inputs/checkpoint | command.json, evidence.json L each; child.json, result.json S each | 4 | 32.125 MiB | Copied native proof plus requalification |
| Content dependency CHECK_INPUTS / check-inputs/checkpoint | command.json, evidence.json L each; child.json, result.json, verified.json S each | 5 | 32.1875 MiB | Embedded original Gold/reference proofs and prepared Content manifest |
| PUBLISH_CONTENT / publish-content/checkpoint | command.json, evidence.json L each; child.json, result.json, verified.json S each | 5 | 32.1875 MiB | Native output remains in CONTENT binding |
| Content VERIFY_RESULT / verify-result/checkpoint | command.json, evidence.json, selection.json L each; child.json, result.json, verified.json S each | 6 | 48.1875 MiB | Independently verified result and registered-artifact copy |
| Final Content recheck / check-inputs/checkpoint | command.json, evidence.json L each; child.json, result.json S each | 4 | 32.125 MiB | Exact read-only dependency/result requalification |
| Fenced finalization | — | 0 | 0 | Atomic SQL artifact/control/attempt/audit/coordinator mutation; no publisher |

The clean original pass totals **41N+40 files**, **10N+20 directories** (root/shared
stage directories included), **51N+60 entries** and
**T(N)=8.8125N+289.375 MiB** under those loose checked-size ceilings. For one feed:
111 entries / 298.1875 MiB; eight feeds: 468 entries / 359.875 MiB. This does not prove
a valid native workload reaches those byte totals. It proves the present independent
ceilings are insufficient to derive the requested ≤64 MiB guarantee. A tighter joint
bound has not been established. Inventory/packet metadata must still fit separately.

| Additional path | Retained private output / conservative checked-size ceiling | Lifetime/partial-write gap |
| --- | --- | --- |
| Native recovery verifier | command/evidence L, result/verified S: 4 files, 32.125 MiB per observation | Child/evidence/result writes may be interrupted; work directories remain |
| Two final recovery observations, eight feeds | Each has eight Bronze GET proofs plus one native verifier: 68.25 MiB for both observations | Current per-file ceilings do not prove a RECONCILE tree fits |
| RESUME qualification | command/evidence L, child/result/verified S: 5 files, 32.1875 MiB per qualification | Requeue + preclaim allow 64.375 MiB by loose caps; unsuccessful calls also retain directories |
| Resumed committed-Gold attempt | Four Content directories: 20 files, 144.6875 MiB | Original, RECONCILE and RESUME trees remain required separately; no copied stages |
| Read-only inspection/requalification | New UUID work directories, often repeating immutable metadata | No pre-write lifetime byte/entry reservation or aggregate serialized-name bound |
| Qualification / observation packet | Up to 1 MiB each in separate recovery directories | Existing post-serialization limit; orphan/partial packet files are retained |
| Failed/interrupted write | Zero-length or prefix of each intended file; incomplete fixed directory/file sets | Normal CREATE_NEW has no separate temporary filename, but interruption can occur before creation or during any write |
| Spark/native temporary or incomplete publication | Native output/scratch roots, excluded from private byte count | Retain unchanged; absence of completion cannot be interpreted as success or permission to retry |

There is **no proved enforced worst-case budget for the complete permitted lifecycle**.
Post-write Q checks do not bound failed Silver output; per-file caps do not reserve the sum;
repeated inspection/qualification is not charged to a lifetime ledger. Names, directories and
JSON framing also consume inventory/packet capacity even when files are empty. A small native
fixture is a measurement, not a supported-profile proof.

## Interruption evidence and recovery decisions

| Boundary | Current exact behavior | B0 implication |
| --- | --- | --- |
| No intent and no private directory | Empty inventory allowed; no evidence invented | Supported capture case |
| Durable Content publisher intent before command creation/linking | Initial capture/review succeeds; native qualification requires the missing link and refuses disposition with EVIDENCE_UNAVAILABLE; ownership stays occupied after guarded handoff | Tested before publisher authorization; source count alone cannot close this gap |
| Partial command with recorded full hash | EVIDENCE_CHANGED; retain prefix bytes | No missing-proof substitution |
| Partial child.json or result.json | EVIDENCE_CHANGED, including uncommitted files | Process hints may be unavailable; manual quiescence alone cannot bypass capture |
| Partial uncommitted evidence.json | Capture hashes exact opaque bytes; it does not promote a result | Subsequent stage/native qualification still mandatory |
| Exact intended Content namespace exists without complete.json, dependencies valid, writers proven quiescent | FAILED / INCOMPLETE_OUTPUT, retryEligible=false; no result artifact | New injected-namespace test checks no repair/republication and immutable file stamps |
| Complete final result + original/inherited dependencies fully qualify | Existing fenced SUCCEEDED reconciliation | Existing original/inherited real tests; no publisher retry |
| Earlier supported verified boundary + full quiescence proof | Existing PAUSED; separate explicit RESUME where supported | Prefetch without retained Silver, committed Gold, verified Content inputs only |
| Missing/corrupt/unavailable ancestor, changed dependency, stale fence, ambiguous/live writer | Refuse qualification/disposition; occupied RECOVERY_REQUIRED | Preserve exact ancestry and current recovery authorization |

The intent test injects process exit at the adapter's INTENT fault hook, before private
command creation and before command_address/command_sha256 are recorded by authorization.
It does not claim the SQL intent already stores that address/hash. Evidence review and
occupied-to-occupied handoff can succeed; this never frees the slot. The test then checks
the native qualification refusal. A synthetic capture unit case separately exercises a
recorded command address whose file is absent. The incomplete-publication test creates one synthetic partial file in
the **exact intended namespace before child launch**; it is not represented as a Spark
crash artifact. Both use real preceding D1–D4 artifacts and production recovery adapters.
The latter requires live-guard rejection, explicit original process exit, independent
descendant review, current RECONCILE authorization, host/epoch fencing and two read-only
native observations before release. No inference from expired heartbeat/PID absence is added.

## Alternatives and disposition

**A, conservative profile:** remains preferred, but source count alone does not bound output
metadata, partial sidecars, lifetime inspections or packets. RunProfile V1 has no such
budget. No invented source or payload limit is called approved.

**B, sealed references:** native bodies and SQL bytea already use references/digests. Repeated
immutable proof metadata could be factored only through a versioned sealed reference with
full root/identity/inventory/hash/lineage requalification. Removing journals or skipping
sidecars would weaken current recovery and is not implemented.

**C, streaming:** deferred. Larger/streamed capture cannot resolve a missing recorded command
or partial process identity and does not make packets or producers bounded.

Selected disposition: **stop at the safety blocker**, preserve all current limits and
production capability gating, and propose the narrow producer/reservation protocol in
[ADR 0021](../adr/0021-content-evidence-producer-reservations.md), with acceptance pending.
It combines a proved profile with bounded parent-owned evidence persistence, pre-launch
reservations including orphaned writes, command-before-intent ordering and exact immutable
metadata references where useful. Old readers/modules must remain available for old attempts.
This protocol is a recommendation, not implemented functionality or an approval prerequisite
invented for this task.

## Native measurements, tests and CI

The two-feed/no-retained-Silver fixture passes through production D1–D5a adapters with
synthetic loopback feeds, real SeaweedFS objects, Spark Silver/Gold/Content and the independent
Content analytics verifier. Immediately after successful finalization, production capture
measured **502,591 bytes, 122 files, 40 directories, 162 entries**, with **53,758 bytes of
canonical inventory**. Measurements include commands and all private sidecars; native
publication bodies are excluded. UUIDs, filesystem stamps and temporary-root lengths affect
metadata size, so this is an observation, not a newly approved input envelope.

| Measured private directory group | Files | Bytes |
| --- | ---: | ---: |
| check-inputs | 13 | 150,817 |
| fetch-source | 12 | 8,130 |
| inspect-bronze | 32 | 31,256 |
| prepare-gold | 5 | 32,934 |
| publish-content | 5 | 49,481 |
| publish-gold | 6 | 48,338 |
| recheck-silver | 10 | 28,053 |
| silver | 8 | 8,057 |
| store-bronze | 8 | 7,954 |
| verify-result | 11 | 103,983 |
| verify-silver | 12 | 33,588 |
| Total | 122 | 502,591 |

The fixture checks two catalog rows and two daily discovery rows, one matched and one missing
article, exact dependency edges, two feed requests, 11 VERIFIED stage rows, run/attempt
SUCCEEDED, released coordinator and subsequent compatible claim. Content and Gold snapshots
are compared by hash/mtime/inode, Bronze receipt records remain exact, and standalone Content
publication is analytically equivalent. Its Content fingerprint is
`59fac58b67a53ea7a482196cd5ae9dc470575033c250bef64c63a8233a4f8f72`, completion SHA-256
`ffc3983ba10aa32119ae45ad755d45e338c1ff43b7819251f03f16c1e7930faf`.
The whole test, including standalone equivalence and later inspection, took **132.839s**.
This is not normal API/worker Content dispatch acceptance: test-only capability wiring
drives the existing internal adapters; production gating remains unchanged.

Local environment: macOS 27.2/arm64, GraalVM Java 21.0.7, Python 3.12.15, Spark 4.0.2,
DuckDB 1.4.5, boto3/botocore 1.43.114, disposable pinned PostgreSQL 17.11 and SeaweedFS 4.48.
Only isolated synthetic feeds and local storage are used. These are correctness-test timings,
not benchmarks.

| Current-run validation | Cases | Measured duration | Outcome |
| --- | ---: | ---: | --- |
| Capture boundaries and interrupted sidecars | 24 unit, included in full units | Final focus 3.035s class; initial focus 8.660s Maven | Passed |
| All content-api unit tests | 359 | 15.054s sum of class times | Passed |
| Native Content publication, independent verification and lost terminal acknowledgment | 2 integration | 212.918s class | Passed; actual PostgreSQL restart |
| Inherited-attempt recovery | 5 integration | 562.346s class | Passed; complete results, safe checkpoints, corrupt/missing ancestry, fencing and writer vetoes |
| Existing original-attempt recovery cases | 7 integration | 597.504s sum of passing case times | Passed; final-result restart, terminal rollback and lost acknowledgment included |
| New incomplete-publication case | 1 integration | 84.374s case | Passed in scoped rerun; FAILED, no retry/result/repair |
| New interrupted publisher INTENT case | 1 integration | 78.026s class; 88.71s isolated driver including capture units | Passed in final scoped rerun; occupied UNKNOWN stage and unavailable command link |
| Explicit RESUME | 7 integration | 648.445s class | Passed; real PostgreSQL restart and unsupported-boundary rejection |
| Persistence/historical migrations | 30 integration | 14.260s class | Passed |
| Control/admission/worker/finalization | 152 integration | 82.906s sum of class times | Passed |
| Native managed Content adapter/recovery/RESUME contracts | 32 Python | 20.345s unittest | Passed under pinned Python + UTC |
| Reliability C3c | 247 integration + 53 overlapping unit | 616.66s disposable driver | Passed; actual finalization, bootstrap, operator and verification PostgreSQL restarts |
| CI classifier, partition inventory and aggregate gates | 37 Python | 1.797s unittest | Passed; all 26 integration classes assigned exactly once |
| Markdown/relative links, hygiene, whitespace, migration byte comparison | Consistency checks | Not a benchmark | Passed; V1–V9 all unchanged |

The capture tests include 64 MiB accepted / first extra byte refused, 4096 entries accepted /
first extra entry refused, individually oversized sidecars, long-name inventory growth,
depth-four directories and hidden subtrees, corrupt/missing/symlink evidence, two separately
capturable 40 MiB trees and interrupted command/process/evidence writes. The 1 MiB + first
extra byte JSON test establishes RecoveryFiles storage bounds; it is not qualification of
a semantically valid maximum-size C3c packet or a proof of profile overhead.

The supplemental Python command initially omitted PYSPARK_PYTHON and failed fixture setup
(worker 3.9 versus driver 3.12). A second run pinned Python but omitted CI's TZ=UTC and
failed the literal Gold datetime fixture's dependency validation. The final command matching
both CI bindings passed all 32. No native module, verifier or data contract was changed to
make those commands pass. Failed logs remain under ignored outputs and are not counted as
passing native executions. Managed Java adapters already pin their child Python and UTC.

The expanded native driver ran 205 integration + 359 unit cases in **2459.07s** (Maven
40m54s), with **two test assertion failures**: the new intent test wrongly expected initial
review to reject an unlinked command, and the incomplete-publication test omitted the existing
sentinel from its file count. A first two-case rerun passed the corrected partial-publication
case and retained the intent expectation failure. Moving that assertion to qualification then
exposed another incorrect expectation: handoff changes the unresolved stage to UNKNOWN.
The final intent run verifies the actual pre-handoff INTENT / post-handoff UNKNOWN sequence,
qualification refusal, occupied coordinator, null command link/child identity, absent command
file and unchanged original private/native files. No recovery implementation was weakened.
The expanded driver is **not described as a successful single run**. Its seven existing
original cases plus the final two negative cases supply nine distinct passing cases across
scoped runs; repeated cases add no coverage count.

IntelliJ reported no sessions, run configurations or runnable locations for ContentRecoveryIT;
creating a debugger session failed. No logpoints/breakpoints were installed. Maven fixture
assertions and recorded SQL state supplied the fallback runtime evidence. The unavailable
debugger is not the B0 architecture blocker.

Final distinct passing coverage across scoped runs: **452 PostgreSQL integration + 359 Java
unit + 32 native Python + 37 CI-policy = 880 cases**. The 53 C3c units, focused 24 capture
cases and native reruns overlap those counts. The renamed capture case replaces its former
name rather than adding a case. No selected final case is skipped. Historical runs in the
earlier audit are not counted as new validation. No production execution guarantee can be
inferred from passing fixtures. No new test class, CI timeout, partition, migration or native
schema/module is introduced.

Local evidence is retained under ignored outputs: b0-completion-native.log (including its
two failures), b0-completion-recovery.log (passing partial case and remaining intent failure),
b0-completion-intent.log (wrong post-handoff state expectation), b0-completion-intent-final.log
(passing corrected intent/capture cases), b0-completion-python-utc.log and
b0-completion-c3c.log. Corresponding XML snapshots and b0-completion-final-inventory.json
preserve per-case outcomes, source class selection and distinct counts. The final collector
asserts all retained final cases pass; it does not relabel a failed Maven invocation as green.
Disposable test containers and managed fixture worker/publisher processes were absent after
the drivers finished. Ordinary operator data volumes are unaffected.

CI keeps core=10, reliability=7, ingestion=3 and gold=6 classes and the existing aggregate gate.
Both added integration cases remain in ContentRecoveryIT; their final passing local case time
is approximately **162s combined**. The local run excludes the full RssGoldIT class and is
not a hosted Gold-duration measurement. Owner-run hosted validation must establish the full
Gold partition within its current **90-minute** budget, plus the required selected processing,
ingestion, analytics JDBC/HTTP and frontend gates. No new hosted result is claimed. The latter
full suites were not rerun locally under this tests/documentation-only change.

If hosted duration leaves insufficient headroom, a concrete split to measure is the three
Content recovery/RESUME classes versus native Gold/Content publication classes, retaining
every class and requiring both jobs in the aggregate gate. Current local recovery/RESUME
timings cannot be added to an older hosted baseline to prove a timeout. No split, classifier
change, test omission or timeout increase is made without that measurement.

## Changed files and remaining restrictions

Reproduce the native scopes from the repository root with JAVA_HOME set to the pinned JDK 21
and DDIP_TEST_PYTHON to the absolute pinned Python executable. The tracked disposable drivers
provide synthetic endpoints, PostgreSQL/SeaweedFS and restart control; no live feeds are used.

```sh
python3 scripts/test-rss-fetch-postgres.py --offline --content
python3 scripts/test-rss-fetch-postgres.py --offline --content-recovery
python3 scripts/test-rss-fetch-postgres.py --offline --content-inherited-recovery
python3 scripts/test-rss-fetch-postgres.py --offline --content-resume
python3 scripts/test-recovery-postgres.py --offline
TZ=UTC PYSPARK_PYTHON="$DDIP_TEST_PYTHON" PYSPARK_DRIVER_PYTHON="$DDIP_TEST_PYTHON" \
  SPARK_LOCAL_IP=127.0.0.1 PYTHONPATH=processing:processing/tests \
  "$DDIP_TEST_PYTHON" -B -m unittest test_managed_content
mvn -B -ntp -o -Daether.syncContext.named.factory=noop -f services/content-api/pom.xml test
python3 -I -B -m unittest discover -s .github/ci/tests -v
python3 .github/ci/postgres_partitions.py
npm run check
git diff --check
```

Offline mode assumes populated dependency/image caches. The expanded local driver reused
test-rss-fetch-postgres with the exact integration selection
`RssContentIT,ContentInheritedRecoveryIT,ContentRecoveryIT,ContentResumeIT,ContentFinalizationIT,PersistenceIT,WorkerCoordinationIT,ContentResumeControlIT,RunSubmissionIT`
and all units. Scoped reruns selected the two new methods and then the intent method plus
capture units. Those generated drivers/logs/XML snapshots are ignored local evidence, not
additional production commands or replacements for hosted partition execution.

Exact tracked-source inventory for this investigation (generated logs/drivers/reports stay
under ignored outputs):

- `docs/adr/0021-content-evidence-producer-reservations.md` — proposed narrow correction;
  owner acceptance pending, no implementation claim.
- `docs/adr/README.md` — ADR index/status.
- `docs/architecture/durable-execution.md` — current B0 boundary and interruption gap.
- `docs/operations/content-recovery-envelope.md` — historical audit links to this follow-up.
- `docs/operations/content-recovery-envelope-completion.md` — budget, decisions and new evidence.
- `docs/operations/d5b2b-inherited-recovery-validation.md` — follow-up status link.
- `docs/operations/managed-rss-fetch.md` — operator restriction and retention guidance.
- `services/content-api/src/test/java/platform/contentapi/execution/ContentRecoveryFilesTest.java`
  — five interrupted-write/intent capture cases.
- `services/content-api/src/test/java/platform/contentapi/execution/ContentRecoveryFixture.java`
  — missing-command refusal and quiescent incomplete-namespace assertions.
- `services/content-api/src/test/java/platform/contentapi/execution/ContentRecoveryIT.java`
  — two new native-backed interruption cases in the existing Gold partition class.
- `services/content-api/src/test/java/platform/contentapi/execution/RssContentTestSupport.java`
  — test-only production-capture measurement by private directory group.
- `services/content-api/src/test/java/platform/contentapi/management/ContentRecoveryTestProcess.java`
  — exact INTENT/AUTHORIZED boundary fault injection in disposable original worker JVM.
- `services/content-api/src/test/java/platform/contentapi/management/RssContentIT.java`
  — measure/assert the complete two-feed private tree after native finalization.

Ordinary Content worker dispatch remains unavailable. Existing explicit operator recovery/RESUME
procedures keep their documented checkpoint and evidence restrictions; they do not establish
an approved dispatch envelope. There is no supported operator startup/configuration for managed
Content dispatch, no automatic retry/recovery, no publication selection, no cleanup of retained
evidence, and no claim of production readiness or public deployment. PostgreSQL/SeaweedFS
data volumes must be retained by default under the existing operator procedure.

No branch change, staging, commit, push, PR, issue update or GitHub write was performed.
**Gate B0 is incomplete; Gate B1 activation remains blocked by the missing proved producer
and interruption envelope.** The proposed ADR is the concrete next implementation boundary.
