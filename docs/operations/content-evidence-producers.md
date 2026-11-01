# Gate B0: bounded Content evidence producers

**Later CI follow-up:** the [current CI inventory](ci.md) has five partitions, with gold=3 and
content_recovery=3 after repeated Gold timeouts in runs 41317309717 and 41282407982.
The four-partition inventory and validation results below describe this implementation's historical runs.

October 31 / November 1, 2026 (local / UTC). **Gate B0 is incomplete. Production managed
Content dispatch remains disabled and is not safe to consider for Gate B1.** ADR 0021 stays
Proposed, with owner acceptance pending. This implementation supplies enforceable private
writer ceilings and prepared commands; it does not establish a complete operational profile.

The answer to “Can every execution permitted by the proposed Content profile produce all
evidence needed for safe inspection/reconciliation after any supported interruption?” is
**not yet demonstrated**. Native publication may precede serialization of its full proof.
The fixed sinks reject an oversized proof before writing it, retain existing evidence and
leave ownership occupied, but repeated recovery could encounter the same oversized proof.
Passing the bounded native fixture is compatibility evidence, not a worst-case proof.

The selected partial implementation is option A with fixed versioned producer sinks.
Existing native body references already provide option B's principal saving; no new reference
format is introduced. Streaming verification (C) is deferred. Charging both final/prefix forms
and never reclaiming reservations trades capacity for simple fail-closed interruption behavior.

The earlier [completion investigation](content-recovery-envelope-completion.md) is the
producer inventory and baseline evidence. This report records the implementation delta.

## Scope and candidate

RunProfile version 1 and the Content **1–8 fresh feeds**, **0–128 retained Silver IDs** and
one-reference submission contract remain unchanged. Reliability remains 1–32 inputs.
Production worker capability/dispatch still accepts only Reliability. There is no Content
enable switch, dynamic profile switching, arbitrary command hook, new HTTP field, migration,
native schema change, automatic retry, repair, publication selection or control activation.

The additive **validation candidate**, not an approved operational profile, uses:

* RunProfile version 2, pipeline `CONTENT_RSS_V1`, existing pinned deployment fields and an
  exact `contentRuntimeId`. Each Content → Gold → Silver → Bronze → Fetch runtime must be
  version 2, with matching immutable profile IDs/digests and the existing storage identities.
* Exactly one fresh feed, no retained Silver and one publisher reference. Captured reference
  selection is at most 64 KiB; the complete frozen selection is at most 256 KiB; each runtime
  configuration is at most 64 KiB. These checks run before the first fetch/intent, not as a
  silent narrowing of version-1 metadata submission. A rejected candidate retains ownership
  and reports fixed `RESOURCE_LIMIT`/`RUNTIME_MISMATCH`; it is never automatically retried.
* Java 21, Python 3.12.15, Spark 4.0.2, DuckDB 1.4.5, existing pinned libraries, module hashes,
  canonical CONTROL/native/TEMP roots and loopback native services. Version 2 pins the fixed
  `BoundedManagedRssFetch`, `BoundedManagedBronze`, `bounded_content_producer.py` and
  `bounded_evidence.py`. It cannot authorize a legacy writer.

One fresh feed is derived from the conservative file-slot plan below: two fresh feeds
would reserve **76.75 MiB**, exceeding 64 MiB. This derivation is not proof that every
one-feed native proof fits its individual slot. No operational approval is implied.

## Enforced writer protocol

Each initialized `CONTROL/runs/<run>/attempts/<attempt-or-control>` tree has a private
`.content-evidence.json` ledger and zero-byte `.content-evidence.lock`. The ledger binds
run/attempt, tree kind and root device/inode to exact relative file names, per-file byte
allowances and directory names. Its own final and `.preparing` forms each reserve 256 KiB.
The manifest is canonical, forced, atomically installed and read back. Its update prefix
is retained if interrupted; another writer refuses it. A read-only capture can hash that
prefix against the previous complete ledger without adopting it as a new reservation.
A lost ledger cannot be regenerated over retained evidence or downgraded to legacy writes.
Recovery derives mandatory version-2 accounting from the captured runtime/profile.

Before creating a work directory, a producer reserves every fixed final file **and its
same-directory `.preparing` name at full maximum length**, all directories, and conservative
canonical inventory serialization. Reservations are monotonic, including rejected, partial,
orphan and repeated verification work. They are not reclaimed on failure. Exact actual bytes
and entries are rechecked under the same cross-process lock before parent writes and further
reservations. Java FileChannel locks and Python `fcntl.lockf` serialize cooperating writers;
children cannot issue reservations. Fixed, hash-pinned child entry points require their
existing permits, fail before an unreserved write, and never fall back to legacy persistence.
Spark's working directory is the separately bound TEMP root, not the private evidence tree.

Before opening a private output, each sink checks both names and the full byte length. It
creates only a new prefix, writes/forces/readbacks it, atomically moves it into place, forces
the containing directory and verifies the final bytes. Existing final/prefix files are never
overwritten. All prefixes remain counted. Arbitrary same-user programs modifying these
trusted private roots are outside cooperative reservations; mutation is still rejected by
seals/root validation. This is not a filesystem quota or a disk-space allocation guarantee.

The unchanged capture ceilings remain **67,108,864 regular-file bytes**, **4,096 entries
including root**, files at depth at most four and no directories at depth four. Canonical
inventory and each qualification/observation packet independently remain at most
**1,048,576 bytes**. The reservation ledger is at most **262,144 bytes**. Ordinary version-2
proof slots are 1 MiB, Silver proof/selection slots 512 KiB, small sidecars 64 KiB, and the
RSS payload remains 5 MiB. Existing version-1 reader maxima are unchanged.

Canonical inventory accounting uses the exact recorded relative names, escaped JSON,
20-character device/inode/length strings, 64-character timestamps, 64-character hashes and
process-incomplete flag overhead. Actual timestamps longer than that are refused. Directory
entries count even without files. The independent byte, entry, ledger, depth and inventory
limits can each reject a reservation; none is truncated to make another limit fit.

## Fixed original-attempt reservation calculation

S = 65,536 bytes; Q = 524,288 bytes; L = **1,048,576 bytes** for version 2. Each row lists
all reserved final names; **every name also reserves a full-size `.preparing` counterpart**.
Native Bronze objects and Silver/Gold/Content bodies remain exact read-only references;
their bytes are excluded here. Embedded native metadata and inventories are included.

| Private work directory / count | Final names and maximum bytes per directory | Final names | Final-byte allowance |
| --- | --- | ---: | ---: |
| Initial input checks | SQL selection only | 0 | 0 |
| FETCH_SOURCE ×1 | payload.xml 5 MiB; command.json, metadata.json, descriptor.json, result.json, complete.json S each | 6 | 5.3125 MiB |
| STORE_BRONZE ×1 | command.json, evidence.json, result.json, verified.json S each | 4 | 0.25 MiB |
| INSPECT_BRONZE ×4 | same four S names; initial, Gold prepare/publish and Content Gold opener | 16 | 1 MiB |
| SILVER ×1 | command.json, child.json, result.json, verified.json S each | 4 | 0.25 MiB |
| VERIFY_SILVER ×1 | command.json, child.json, result.json, verified.json S; evidence.json, selection.json Q | 6 | 1.25 MiB |
| RECHECK_SILVER ×1 | command.json, child.json, result.json, verified.json S; evidence.json Q | 5 | 0.75 MiB |
| PREPARE_GOLD ×1 | command.json, evidence.json L; child.json, result.json, verified.json S | 5 | 2.1875 MiB |
| PUBLISH_GOLD ×1 | command.json, evidence.json, selection.json L; child.json, result.json, verified.json S | 6 | 3.1875 MiB |
| VERIFY_RESULT ×2 | command.json, evidence.json, selection.json L; child.json, result.json, verified.json S; Gold then Content | 12 | 6.375 MiB |
| CHECK_INPUTS ×3 | command.json, evidence.json L; child.json, result.json, verified.json S; Gold recheck, Content inputs, final Content recheck | 15 | 6.5625 MiB |
| PUBLISH_CONTENT ×1 | command.json, evidence.json L; child.json, result.json, verified.json S | 5 | 2.1875 MiB |
| Terminal transaction | SQL artifact/control/attempt/audit/coordinator updates | 0 | 0 |
| Total final-name allowance | Includes some intentionally unused fixed-stage slots | **84** | **29.3125 MiB** |

Charge both final/prefix names: `2 × 29.3125 MiB`, plus `2 × 256 KiB` ledger capacity =
**59.125 MiB = 61,997,056 bytes**. There are 30 directories including root/shared stage
parents, 168 final/prefix file entries and three ledger/lock entries: **201 reserved entries**.
A complete extra inspection reserves another Bronze inspection and CHECK_INPUTS directory:
`2 × (2L + 7S) = 4.875 MiB`, adding 20 entries. The resulting ceiling is **exactly 64 MiB
and 221 entries**. Subsequent capacity growth is rejected before a directory or child exists.
A deterministic test constructs this full plan with a 128-character source ID and proves
both inventory upper bounds remain below **100 KiB**. It does not use observed fixture sizes
as substitutes for these allowances.

For N fresh feeds the clean plan is `2 × (20.5 + 8.8125N) + 0.5 MiB`; N=2 is 76.75 MiB.
Per-file growth must also fit its individual slot even when aggregate capacity is available.

## Recovery, qualifications and ancestry

Each original EXECUTE, RECONCILE, RESUME-control and resumed EXECUTE has its own bounded
UUID tree. A new candidate cannot retrofit, skip or make an oversized/unavailable old ancestor
recoverable. Original stage rows stay on their actual attempts. All existing ancestry,
dependency, epoch, host-guard, storage-root and process-quiescence checks remain required.
There is no combined installation or ancestry byte quota.

Read-only recovery directories reserve the existing generic VERIFY_RESULT slots, at most
`2 × (3L + 3S) = 6.375 MiB` each, plus the tree ledger. Nine such directories reserve
57.875 MiB; a tenth would require 64.25 MiB and is refused. A one-feed Gold/Content recovery
normally uses two directories per independent observation (Bronze plus native verification).
Both observation sets are reserved before sealing the first observation, with exact UUIDs
held by the qualifier. The second observation consumes those identities and cannot extend
the sealed ledger. A differing observation count or new reservation is refused. The strict
file comparison still includes the ledger, all original commands, sidecars and anchors.

`CONTROL/recovery/<control>` is a separate bounded packet tree. Each 1 MiB packet reserves
its final/prefix pair and directory; the ledger charges another 0.5 MiB. At most **31 packet
pairs = 62.5 MiB plus zero-byte lock** fit; the 32nd is refused before directory creation.
Each packet is independently checked at 1 MiB. This does not prove all possible inventory,
SQL snapshot, review and inherited-proof combinations serialize into a 1 MiB packet.

Partial child/result prefixes are hashed as opaque retained bytes and explicitly mark process
evidence incomplete. Handoff and RESUME refuse them as proof of quiescence. Missing/corrupt
commands, unavailable native evidence, stale epochs, root changes, live/unobservable writers
and contradictory ancestry retain occupied ownership. An absent PID or failed sink never
implies no writer exists. No code retries a fetch, PUT, Spark or Content publisher.

## Command preparation and interruption boundaries

All production D1–D5a Content adapters now prepare canonical command bytes, force/read back
the new prefix, atomically publish it, force its directory, and validate file identity before
SQL stage intent. The intent transaction records the exact command address and SHA-256.
This reuses existing columns and immutable stage/checkpoint identities; V1–V9 are untouched.

| Boundary | Durable evidence / authority | Safe behavior |
| --- | --- | --- |
| Reservation fails | Existing evidence and allowances retained; no new work directory | Fixed failure; no child or external effect |
| Interrupted ledger update | Old complete ledger plus retained bounded prefix | Capture can hash both; producers refuse further growth |
| Interrupted command preparation | Bounded prefix, no committed intent/authorization | Never launch or treat the prefix as a command |
| Prepared command, intent rolled back/not committed | Exact orphan command; no SQL launch authority | Retain it; do not infer execution from its existence |
| Intent committed, before authorization/launch | Exact immutable command link; no authorization yet | After full quiescence review, independently inspect the intended namespace |
| Authorization committed, child outcome unknown | Existing fenced intent/audit; possibly process prefixes | Explicit independent descendant review; unresolved writer retains ownership |
| Child completed, sidecar/checkpoint acknowledgment lost | Original immutable command, native bytes and any prefixes retained | Existing read-only reconciliation; never replay publisher |
| Qualified completed publication | Independently qualifying exact dependencies/result | Existing fenced terminal transaction releases ownership; no native rewrite |
| Missing/mutated/unavailable required evidence | No inferred replacement | Fail closed; no automatic retry, release or substitute artifact |

Preparing a command does not change its row version or authorize a child. Authorization still
requires an exact owner/epoch, source/runtime/storage bindings, deadline, current controls
and row version zero; it can be consumed only once. The internal legacy synthetic test seams
remain for version 1, but cannot create a version-2 intent without a prepared command and
valid ledger. A lost acknowledgment never refreshes authorization to retry native work.

## Unclosed guarantee and recommendation

The new sinks enforce private byte/entry ceilings even when evidence is partial. They do
**not** yet establish that every otherwise valid one-feed execution can retain the complete
proof needed to qualify/recover a completed native publication:

* Native Content verification permits up to 8192 analytical files, an 8 MiB completion and
  other embedded metadata; Gold can retain a 16 MiB input manifest. Silver has its own file,
  namespace and byte limits. These independent bounds do not prove the complete serialized
  commands, nested proofs and copied selections fit the candidate's 512 KiB / 1 MiB slots.
* The native writer can finish a publication before the bridge serializes/checks its proof.
  The bridge then refuses oversized private evidence and writes a bounded `RESOURCE_LIMIT`
  result. It preserves native output, but the same limit can prevent recovery verification.
  There is no proven producer constraint linking native output shape to private proof size.
* A 1 MiB inventory does not itself prove the qualification packet containing it plus rows,
  process review and inherited references fits 1 MiB. The normal fixed plan fits comfortably;
  the complete lifetime combination remains unproved.

The smallest next design work is a narrow, reviewed output-metadata envelope enforced before
native publication can reach an unrecoverable proof size, or an exact sealed-reference form
for recurring immutable proofs with full bounded read-only requalification. It must include
nested commands, inventories, packets and interrupted native output, preserve old readers
and native identity, and supply a numerical proof. Do not add speculative feed caps, raise
scanner limits, discard sidecars or weaken immutable comparisons. General streaming is not
implemented or selected by this work.

Storage loss, disk exhaustion and arbitrary filesystem I/O unavailability remain fail-closed
conditions. Current 1 GiB free-space checks are observations, not reserved disk or hard quotas.
Existing process-count/stream/stage-time limits and recovery review freshness remain unchanged.
Process supervision is not an operating-system PID quota.
No allocation, permission or fsync failure can authorize a substitute writer or unsafe release.

## Validation and CI

The local fixture uses isolated synthetic loopback feeds, exact publisher-reference snapshots,
disposable PostgreSQL 17.11 and SeaweedFS 4.48, Java 21 and pinned Python/Spark/DuckDB. It
exercises production adapters through internal test capability; it is not production worker
dispatch or proof that an HTTP 202 response completed a run.

Local environment: macOS 27.2 arm64, 16 GiB memory, GraalVM Java 21.0.7, Python 3.12.15,
Spark 4.0.2, DuckDB 1.4.5, boto3/botocore 1.43.114 and the repository's pinned Maven libraries.
These are local measurements, not hosted timings or performance claims.

| Validation | Result / measured duration |
| --- | --- |
| Final content-api units | **382 passed**, 17.603 s summed suite time; includes 18 budget and five prepared-command cases |
| Expanded native/control regression | 262 integration cases: **261 passed, one OS process-enumeration allocation failure**; driver 2,275.08 s; its 380 unit cases are superseded by the final 382, not added again |
| Original Content recovery | **10 passed**, 578.230 s within the expanded batch; includes bounded/legacy prepared intent, partial output, actual database restart and complete publication |
| RESUME | **7 passed**, 501.565 s within that batch; strict unsupported boundaries retained |
| Legacy inherited recovery | **5 passed** in that batch; its new bounded case failed in the harness's `Process.descendants()` with native “Cannot allocate memory,” before qualification |
| Isolated bounded inherited recovery rerun | **1 passed**, 145.900 s case / 154.21 s driver; original recovery → applied RESUME → real resumed JVM → interruption → real PostgreSQL restart → SUCCEEDED reconciliation |
| Reliability C3c | **247 integration cases passed**, 597.45 s driver; all four actual PostgreSQL restart probes passed; its focused units are already included in the final 382 |
| Final focused run | **89 integration cases passed**: D1 53, finalization 34, bounded Content two; 188.65 s driver including the final 382 units |
| Final bounded native pipeline | **Passed**, 97.266 s including fixture phases; actual analytical values and full independent verification asserted |
| Java ingestion units | **80 passed**, 4.836 s Maven; includes seven fixed Java sink capacity/interruption/locking cases |
| Fixed Python sink/bridge faults | **11 passed**, 0.343 s; includes post-effect oversized-proof refusal, partial rename, repeated writes, aliases, root identity and cross-process exclusion |
| Mixed Java/Python lock experiment | Passed in 0.373 s: Python `fcntl.lockf` excluded the actual JVM sink's `FileChannel.lock` writer |
| CI policy and partition tests | **37 passed**, 1.667 s; exact 26-class inventory retained (core 10, Reliability 7, ingestion three, Gold six) |
| Repository / contract checks | Markdown and relative links, hygiene, Python syntax and diff whitespace passed; V1–V9 match baseline byte for byte |

The 262-case invocation was **not green**. The process-enumeration failure was not bypassed
or converted to quiescence: its fixture failed and was later rerun in a new disposable
installation. The same runtime and all inspection/fencing assertions were retained. Earlier
focused development runs also exposed ledger mutation between independent observations and
an unreserved missing-file test rename; both were corrected before the recorded final passes.
No timeout was increased, failed case skipped or native publisher retried within its run.

Final native measurement (including one complete extra inspection): **363,354 private bytes,
91 regular files, 123 entries and 40,424 inventory bytes**. Reservations were exactly
67,108,864 bytes / 221 entries; that fixture's conservative inventory maximum was 92,661 bytes.
The next inspection failed before directory creation or another child/GET. There was **one
feed fetch, two Bronze PUTs, zero DELETEs**; read-only qualification accounted for 150 Bronze
GETs, with no increase after exhausted-capacity refusal. The actual publication contained
12 immutable files; inherited recovery's 13-file root snapshot additionally includes its
pre-existing sentinel. Silver, Gold, Content, delivery receipts and historical stage rows
were checked unchanged across requalification/recovery.

The bounded analytical assertions were title `One again`, observation count `2`, distinct ingestion count `1`, publisher `Demo Publisher`, reference status `MATCHED`, exact reference
ID/revision/fingerprint, one daily discovery and zero missing-reference articles. The two-feed
legacy path retained its 11 verified stages, expected counts/coverage, standalone equivalence,
single successful finalization and subsequent compatible claim. Native processor Python bytes
and publication schemas are unchanged; the Java change routes private writes through the
versioned sink without changing fetch/PUT bytes or receipt contracts.

Boundary coverage includes exactly 64 MiB of reservations and capture, first rejected growth,
4,096 reserved/actual entries and the next entry, long names/inventory growth, depth-four
subtree refusal, full 1 MiB packets, 31 packet pairs and the rejected 32nd, repeated validators,
partial ledger/command/process/result creation, both missing reservation files, unavailable or
corrupt commands/evidence, intent rollback/orphans, prepared-link versus launch authority,
child/checkpoint/finalization acknowledgment loss, live/unobservable writers, stale epoch and
host guard, missing/mutated inherited evidence, incomplete native namespaces, and rollback of
artifact/dependency/stage/attempt/control/audit/coordinator mutations. No sidecar is truncated.

Across the retained XML case identities, **510 distinct integration cases have passing
results**: 263 Content/control/persistence cases and 247 Reliability C3c cases. The final unit,
ingestion, Python and CI suites add 382 + 80 + 11 + 37, for **1,020 distinct automated cases
with passing results across the recorded runs**. Repeated focused runs and the C3c unit subset
are not added again. The mixed-language locking experiment and documentation checks are
separate checks. This union does not turn the earlier 262-case failed invocation into a green
batch or replace hosted validation.

The working-tree classifier selects **all five required jobs**: ingestion, Python processing,
analytics JDBC, content PostgreSQL and analytics HTTP. Their existing aggregate gate,
change-aware classification and four disjoint PostgreSQL partitions are unchanged. All Gold
Content/recovery/RESUME classes ran locally; only the native equivalence method of RssGoldIT
was selected, so this is **not** a complete local Gold-partition timing. The 90-minute hosted
budget is unchanged. Measure the complete hosted Gold partition, including all RssGoldIT
methods and the new bounded cases, before closeout; evaluate a measured split if necessary.
Full hosted Python/JDBC/HTTP and required partition/aggregate validation remain owner work.

Reproduce the candidate pipeline with:

```sh
export JAVA_HOME="$(/usr/libexec/java_home -v 21)" # use the installation's Java 21 path on Linux
export DDIP_TEST_PYTHON="$PWD/outputs/silver-python312/.venv/bin/python"
python3 scripts/test-rss-fetch-postgres.py --offline --content \
  --case boundedProfilePublishesAndRetainsACompleteRecoveryTree
python3 scripts/test-rss-fetch-postgres.py --offline --content-inherited-recovery \
  --case boundedInheritedGoldFinalResult
```

The tracked scripts own disposable test containers only. Operator installations retain data
volumes by default: stop API/worker and local service processes using the existing runbook;
do not remove volumes, private attempts, prefixes or incomplete publications. Retain old
version-1 JAR/module deployment copies at their pinned immutable paths for historical runs;
install new bounded entry points in a separately pinned deployment. No deployment or teardown
of an existing installation was performed here.

Gate B1 still requires closure/owner review of the full native proof/packet envelope, ADR
acceptance, green hosted required partitions/aggregate gate, reviewed activation configuration,
normal API/worker dispatch acceptance and its restart/recovery-only checks. This work does not
claim production readiness or public deployability.

## Changed files

* [docs/adr/0021-content-evidence-producer-reservations.md](../../docs/adr/0021-content-evidence-producer-reservations.md)
* [docs/architecture/durable-execution.md](../../docs/architecture/durable-execution.md)
* [docs/operations/content-evidence-producers.md](../../docs/operations/content-evidence-producers.md)
* [docs/operations/content-recovery-envelope-completion.md](../../docs/operations/content-recovery-envelope-completion.md)
* [docs/operations/d5b2b-inherited-recovery-validation.md](../../docs/operations/d5b2b-inherited-recovery-validation.md)
* [docs/operations/managed-rss-fetch.md](../../docs/operations/managed-rss-fetch.md)
* [ingestion/src/main/java/platform/ingestion/managed/BoundedManagedBronze.java](../../ingestion/src/main/java/platform/ingestion/managed/BoundedManagedBronze.java)
* [ingestion/src/main/java/platform/ingestion/managed/BoundedManagedRssFetch.java](../../ingestion/src/main/java/platform/ingestion/managed/BoundedManagedRssFetch.java)
* [ingestion/src/main/java/platform/ingestion/managed/ManagedRssFetch.java](../../ingestion/src/main/java/platform/ingestion/managed/ManagedRssFetch.java)
* [ingestion/src/main/java/platform/ingestion/managed/ReservedEvidence.java](../../ingestion/src/main/java/platform/ingestion/managed/ReservedEvidence.java)
* [ingestion/src/test/java/platform/ingestion/managed/ReservedEvidenceTest.java](../../ingestion/src/test/java/platform/ingestion/managed/ReservedEvidenceTest.java)
* [processing/bounded_content_producer.py](../../processing/bounded_content_producer.py)
* [processing/bounded_evidence.py](../../processing/bounded_evidence.py)
* [processing/tests/managed_content_fixture.py](../../processing/tests/managed_content_fixture.py)
* [processing/tests/test_bounded_evidence.py](../../processing/tests/test_bounded_evidence.py)
* [services/content-api/src/main/java/platform/contentapi/execution/ContentEvidenceBudget.java](../../services/content-api/src/main/java/platform/contentapi/execution/ContentEvidenceBudget.java)
* [services/content-api/src/main/java/platform/contentapi/execution/ContentEvidenceProfile.java](../../services/content-api/src/main/java/platform/contentapi/execution/ContentEvidenceProfile.java)
* [services/content-api/src/main/java/platform/contentapi/execution/ContentRecovery.java](../../services/content-api/src/main/java/platform/contentapi/execution/ContentRecovery.java)
* [services/content-api/src/main/java/platform/contentapi/execution/ContentRecoveryAncestry.java](../../services/content-api/src/main/java/platform/contentapi/execution/ContentRecoveryAncestry.java)
* [services/content-api/src/main/java/platform/contentapi/execution/ContentRecoveryFiles.java](../../services/content-api/src/main/java/platform/contentapi/execution/ContentRecoveryFiles.java)
* [services/content-api/src/main/java/platform/contentapi/execution/ContentResumeEvidence.java](../../services/content-api/src/main/java/platform/contentapi/execution/ContentResumeEvidence.java)
* [services/content-api/src/main/java/platform/contentapi/execution/PreflightFiles.java](../../services/content-api/src/main/java/platform/contentapi/execution/PreflightFiles.java)
* [services/content-api/src/main/java/platform/contentapi/execution/PreparedCommand.java](../../services/content-api/src/main/java/platform/contentapi/execution/PreparedCommand.java)
* [services/content-api/src/main/java/platform/contentapi/execution/RecoveryFiles.java](../../services/content-api/src/main/java/platform/contentapi/execution/RecoveryFiles.java)
* [services/content-api/src/main/java/platform/contentapi/execution/RecoveryHandoff.java](../../services/content-api/src/main/java/platform/contentapi/execution/RecoveryHandoff.java)
* [services/content-api/src/main/java/platform/contentapi/execution/RecoveryRuntime.java](../../services/content-api/src/main/java/platform/contentapi/execution/RecoveryRuntime.java)
* [services/content-api/src/main/java/platform/contentapi/execution/RssBronzeContext.java](../../services/content-api/src/main/java/platform/contentapi/execution/RssBronzeContext.java)
* [services/content-api/src/main/java/platform/contentapi/execution/RssBronzeExecution.java](../../services/content-api/src/main/java/platform/contentapi/execution/RssBronzeExecution.java)
* [services/content-api/src/main/java/platform/contentapi/execution/RssContentExecution.java](../../services/content-api/src/main/java/platform/contentapi/execution/RssContentExecution.java)
* [services/content-api/src/main/java/platform/contentapi/execution/RssContentRuntime.java](../../services/content-api/src/main/java/platform/contentapi/execution/RssContentRuntime.java)
* [services/content-api/src/main/java/platform/contentapi/execution/RssFetchExecution.java](../../services/content-api/src/main/java/platform/contentapi/execution/RssFetchExecution.java)
* [services/content-api/src/main/java/platform/contentapi/execution/RssFetchRuntime.java](../../services/content-api/src/main/java/platform/contentapi/execution/RssFetchRuntime.java)
* [services/content-api/src/main/java/platform/contentapi/execution/RssGoldExecution.java](../../services/content-api/src/main/java/platform/contentapi/execution/RssGoldExecution.java)
* [services/content-api/src/main/java/platform/contentapi/execution/RssGoldRuntime.java](../../services/content-api/src/main/java/platform/contentapi/execution/RssGoldRuntime.java)
* [services/content-api/src/main/java/platform/contentapi/execution/RssJournalFiles.java](../../services/content-api/src/main/java/platform/contentapi/execution/RssJournalFiles.java)
* [services/content-api/src/main/java/platform/contentapi/execution/RssSilverExecution.java](../../services/content-api/src/main/java/platform/contentapi/execution/RssSilverExecution.java)
* [services/content-api/src/main/java/platform/contentapi/execution/RssSilverRuntime.java](../../services/content-api/src/main/java/platform/contentapi/execution/RssSilverRuntime.java)
* [services/content-api/src/main/java/platform/contentapi/execution/WorkerRepository.java](../../services/content-api/src/main/java/platform/contentapi/execution/WorkerRepository.java)
* [services/content-api/src/main/java/platform/contentapi/management/RunProfile.java](../../services/content-api/src/main/java/platform/contentapi/management/RunProfile.java)
* [services/content-api/src/test/java/platform/contentapi/execution/ContentEvidenceBudgetTest.java](../../services/content-api/src/test/java/platform/contentapi/execution/ContentEvidenceBudgetTest.java)
* [services/content-api/src/test/java/platform/contentapi/execution/ContentInheritedRecoveryIT.java](../../services/content-api/src/test/java/platform/contentapi/execution/ContentInheritedRecoveryIT.java)
* [services/content-api/src/test/java/platform/contentapi/execution/ContentRecoveryFixture.java](../../services/content-api/src/test/java/platform/contentapi/execution/ContentRecoveryFixture.java)
* [services/content-api/src/test/java/platform/contentapi/execution/ContentRecoveryIT.java](../../services/content-api/src/test/java/platform/contentapi/execution/ContentRecoveryIT.java)
* [services/content-api/src/test/java/platform/contentapi/execution/PreparedCommandTest.java](../../services/content-api/src/test/java/platform/contentapi/execution/PreparedCommandTest.java)
* [services/content-api/src/test/java/platform/contentapi/execution/RssContentTestSupport.java](../../services/content-api/src/test/java/platform/contentapi/execution/RssContentTestSupport.java)
* [services/content-api/src/test/java/platform/contentapi/execution/RssFetchTestSupport.java](../../services/content-api/src/test/java/platform/contentapi/execution/RssFetchTestSupport.java)
* [services/content-api/src/test/java/platform/contentapi/execution/RssGoldTestSupport.java](../../services/content-api/src/test/java/platform/contentapi/execution/RssGoldTestSupport.java)
* [services/content-api/src/test/java/platform/contentapi/execution/RssSilverTestSupport.java](../../services/content-api/src/test/java/platform/contentapi/execution/RssSilverTestSupport.java)
* [services/content-api/src/test/java/platform/contentapi/management/ContentRecoveryTestProcess.java](../../services/content-api/src/test/java/platform/contentapi/management/ContentRecoveryTestProcess.java)
* [services/content-api/src/test/java/platform/contentapi/management/RssContentIT.java](../../services/content-api/src/test/java/platform/contentapi/management/RssContentIT.java)
* [services/content-api/src/test/java/platform/contentapi/management/RssFetchFixtures.java](../../services/content-api/src/test/java/platform/contentapi/management/RssFetchFixtures.java)
* [services/content-api/src/test/java/platform/contentapi/management/RssFetchIT.java](../../services/content-api/src/test/java/platform/contentapi/management/RssFetchIT.java)
* [services/content-api/src/test/java/platform/contentapi/management/RssGoldFixtures.java](../../services/content-api/src/test/java/platform/contentapi/management/RssGoldFixtures.java)
