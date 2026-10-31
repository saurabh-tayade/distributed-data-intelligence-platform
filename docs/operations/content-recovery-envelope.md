# Gate B0: Content admission and the recovery evidence envelope

October 31, 2026. **Blocked: no approved managed Content execution envelope has been proved.**
Ordinary Content dispatch remains disabled. This increment corrects the earlier feed-count
claim, audits the actual retained evidence, rejects an unsupported depth boundary without
skipping descendants, and tests the existing limits. It does not introduce a budget profile,
raise recovery limits or claim admission guarantees that the producers do not enforce.

## Baseline and prerequisite

Inspected working-tree baseline `c5c2a114e1a45013b5f40194323f85b2b57d580e`, including API
admission, frozen-selection validation, runtime/profile parsers, V1–V9 SQL, all D1–D5a
evidence producers, C3c packets, resume qualification and inherited recovery. Its commit
message mentions dispatch; the actual packaged worker still supports only RELIABILITY_V2.
Commit messages and profile labels are not execution capability evidence.

The user states inherited recovery is merged with green hosted CI. Cached `origin/main`
still points to D5b-2a merge `1f2a16c` and does not contain `c5c2a11`; no fetch, branch change
or GitHub operation was performed. The merge/hosted prerequisite is not independently
confirmed from these local refs. Local work uses the actual inherited-recovery code present.

## Actual admission contract and history

| Boundary | Implemented/enforced behavior |
| --- | --- |
| HTTP submission | RunController reads at most 65,537 bytes; RunCommands/SourceJson reject bodies above 64 KiB and invalid DTOs before repository admission |
| Content | 1–8 distinct fetchSources, revisions 1–1000; 0–128 distinct retained Silver artifact IDs; one publisher reference |
| Reliability | 1–32 distinct sources; 1–32 distinct batch IDs; one service reference |
| Frozen worker/RESUME gate | FrozenRunSelection reparses the captured normalized request with RunCommands.parse and matches exact sources/inputs/profile; a wider forged selection cannot pass that gate |
| SQL | No aggregate source-count constraint on run_sources; SQL alone is not the API validator. It enforces captured-selection bytes ≤16 MiB, request ≤64 KiB, 20 attempts/run, ordinal/domain/identity/immutability rules |
| Reviewed profiles | RunProfile V1 pins pipeline/source family and runtime/deployment, with no feed/evidence-budget fields; D1–D5a runtime profiles pin adapters, modules, storage and stage deadlines |
| Management capabilities | SOURCE_MANAGEMENT_ONLY advertises maxSources=128 for the source registry, not feeds per run; it exposes no 32-feed Content execution capability |
| Native Gold input set | managed_gold.selected_markers accepts at most 128 total Silver inputs. API acceptance of up to 128 retained inputs plus fresh feeds does not prove that entire combination is executable |

The accepted durable-execution contract already says 1–8 Content feeds and 1–32 Reliability
sources. RunCommands introduced exactly `content ? 8 : 32` in `b36d2ae`, the first durable
submission implementation. Its file history contains no widening. The wider `ordinal < 32`
internal worker guard does not override the normalized-request validator. The erroneous
32-feed/160 MiB statements first appeared in the inherited-recovery documentation at
`c5c2a11`; the correction is documentation, not an API/SQL/native-contract change.

The correct payload-only maximum is **8 × 5,242,880 = 41,943,040 bytes (40 MiB)**.
There is 24 MiB left under the private-tree byte cap before sidecars. Forty MiB fitting is
not proof that the complete execution and recovery evidence fits.

## What capture actually includes

ContentRecoveryFiles walks only `CONTROL/runs/<run>/attempts/<UUID>` for the selected
EXECUTE/RECONCILE UUID. Resume qualification also uses such a tree keyed by its RESUME
control UUID, even though that UUID is not an attempts row. Capture reads/hashes each regular
file and records relative address, byte length, SHA-256, device, inode and mtime. Directories
have address/identity/mtime entries too. Child/gateway hints are extracted from child.json,
result.json and evidence.json. It is an inventory operation, not a copy of all those bytes.

| Evidence | Included in private-tree capture? |
| --- | --- |
| RSS journal | Original payload.xml; metadata.json; descriptor.json; command/result/complete.json. Original single-fetch UUID, instant, hashes, keys and seals remain required |
| Bronze | Private STORE_BRONZE and GET-only INSPECT_BRONZE commands/evidence/results/anchors; receipts are embedded there and in later commands. Bronze object bodies stay in SeaweedFS and are independently GET/hash requalified |
| Silver | PUBLISH/VERIFY/RECHECK commands, result/child/anchor sidecars, qualification JSON and selected native input manifest. Native Parquet/marker/bookkeeping files remain in SILVER and are reopened through full native capture/qualification |
| Gold | PREPARE/PUBLISH/VERIFY/RECHECK commands and evidence, registered-artifact selection JSON, child/result/anchors; manifests/completion JSON, inventories and identities are embedded in these proofs. Gold Parquet bodies remain in GOLD |
| Content | Dependency CHECK_INPUTS, PUBLISH_CONTENT, VERIFY_RESULT and final recheck sidecars, original Gold/reference proof, preparation/completion/inventory/report hashes and scalar accounting. Native publication/reference bodies remain in their bindings |
| Process evidence | Private child/cleanup sidecars plus DB stage child identities in the C3c snapshot; operator-reviewed descendants remain mandatory. Parent/PID absence alone grants no release |
| Native scratch / logs | Spark scratch and native publication roots are not traversed by this private capture. Supervised stdout/stderr are bounded and drained, not retained as private log files |
| SQL packet | originalMetadata includes relational snapshots. RecoveryHandoff.json converts bytea values to SHA-256/length objects, not inline payload/selection copies |
| Inherited prefix | Exact applied RESUME/claimed plan/checkpoint links refer to original stages. Original, original RECONCILE, RESUME qualifier, interrupted resumed EXECUTE and current RECONCILE trees are separately recaptured/requalified. No copied VERIFIED rows or latest-artifact inference |

RecoveryFiles.write retains a bounded qualification/observation JSON under
`CONTROL/recovery/<control>/<qualification>/qualification.json`. Those files are outside
the attempt-tree byte/entry count. Native publication inventories can describe GiB of files
without copying those bodies into the private tree. Some immutable *metadata* is nevertheless
duplicated in several private commands, selections and recheck evidence files.

## Independent limits and failure behavior

| Limit | Exact scope and enforcement |
| --- | --- |
| 64 MiB = 67,108,864 bytes | Sum of regular-file sizes in **one** capture call/tree, inclusive at the limit; directories consume entries, not bytes. Checked while reading an already produced tree, not reserved before execution |
| 4096 entries | One tree, including its root and all visited directories/files. The first 4097 entries suffice to reject; never return a partial capture |
| Depth 4 | Relative to the attempt root; normal D1 payload files are at depth four. This increment rejects even an empty directory at that boundary, because its descendants cannot be fully inventoried within the supported shape |
| 5 MiB per payload | Recovery and journal readers; D1 retrieval also enforces this original RSS byte limit |
| 16 MiB ordinary private file | Generic recovery reader ceiling; individual stage consumers can be much smaller (64 KiB or 512 KiB). This is not a filesystem quota |
| 1 MiB inventory JSON | Canonical private inventory must fit independently of regular-file bytes and entry count. Long names can make a zero-byte tree fail this bound |
| 1 MiB recovery JSON file | Entire qualification packet or final observation. Packet must satisfy inventory I + snapshot/operator/runtime overhead H + JSON framing ≤1 MiB; I fitting alone is insufficient |
| Gold/Content command/evidence | Managed wrappers check evidence ≤16 MiB before writing; commands use the Java 16 MiB envelope. Registered selection/anchor envelopes are checked separately |
| Silver qualifier evidence/selection | Java consumes at most 512 KiB, but managed_silver writes these before that Java read. A reader rejection is not a pre-write producer budget |
| Descendant/time bounds | Existing 32-descendant review, 60-second review freshness, 20 attempts/run, 512 aggregate run stages in recovery, stage deadlines ≤900s for Spark adapters; no aggregate filesystem reservation |

The 64 MiB bound is **not** an ancestry-wide or installation-wide cap. Two 40 MiB private
trees can each be captured, although their combined bytes exceed 64 MiB. Every inherited
tree/seal must still qualify separately; a missing, changed or over-limit ancestor fails
the entire operation. No good prefix is converted into partial successful recovery.
No hard filesystem quota exists. Point-in-time usable-space checks are not reserved bytes.

At baseline, Files.walk(root,4) could include a directory at depth four and silently omit
its deeper contents. Such a directory now returns EVIDENCE_UNAVAILABLE before returning a
seal. Valid depth-four journal files keep their existing hashes and identities. No old
evidence is rewritten, repaired or deleted. Aggregate/entry/manifest excess is unavailable;
the existing generic per-file RESOURCE_LIMIT maps to EVIDENCE_CHANGED in this reader.
This diagnostic distinction is preserved rather than introducing a new public error contract.

## Conservative calculations, not fixture extrapolation

Let S=65,536, Q=524,288 and L=16,777,216 bytes. For a successful first original execution,
no retained Silver and no extra inspection/replay calls, applying the actual consumer caps
to each written file gives the following conservative upper envelope. This intentionally
does not assume compression, typical JSON sizes or that all maxima are jointly attainable.

| Private producer group | Files / capped bytes |
| --- | --- |
| D1 per fresh feed | 6 files: 5 MiB payload + 5S = 5.3125 MiB |
| D2 STORE_BRONZE per feed | 4S = 0.25 MiB |
| D3 per feed, including its initial Bronze inspection | INSPECT 4S + SILVER 4S + VERIFY (4S+2Q) + RECHECK (4S+Q) = 2.5 MiB |
| Further original Bronze proof inspections | Three contexts (Gold prepare, Gold publish, Content Gold opener), 4S per fresh feed/context = 0.75 MiB/feed |
| D4 four work directories | PREPARE (2L+3S), PUBLISH (3L+3S), VERIFY (2L+3S), RECHECK (2L+2S): 20 files, 9L+11S = 144.6875 MiB |
| D5a four work directories | CHECK_INPUTS (2L+3S), PUBLISH (2L+3S), VERIFY (3L+3S), RECHECK (2L+2S): 20 files, 9L+11S = 144.6875 MiB |

Thus this clean path has a loose checked-size ceiling of
**T(N)=N×8.8125 + 289.375 MiB**, or **298.1875 MiB for N=1** and
**359.875 MiB for N=8**. These are upper bounds derived from accepted file envelopes,
not a demonstration of a jointly attainable 360 MiB native workload or a proposed cap.
They do **not** prove T≤64. A smaller feed count alone supplies no such proof.
This audit does not claim a demonstrated real valid Content run exceeding 64 MiB; no
tighter joint bound sufficient for an approved execution profile has been established.
File/entry accounting on this same fixed path is 41N+40 regular files plus 10N+20
directories (including root/shared stage directories): **51N+60 entries**, or 468 for
eight feeds. The inventory and C3c packet JSON still require their independent byte checks.
Native publication and scratch bytes are excluded from T, exactly as in actual capture.

Recovery/resume add separate budgets:

- One generic native recovery verifier can retain command/evidence at L each and
  result/anchor at S each: 2L+2S = 32.125 MiB. A final two-observation Content recovery
  with eight Bronze GET verifiers per observation has the tighter loose envelope
  2×(8×4S + 2L+2S) = **68.25 MiB**, already above 64.
- One resume qualifier retains command/evidence at L each and child/result/completion
  seal at S each: **32.1875 MiB**. The requeue and pre-claim execution qualifications
  together permit **64.375 MiB** under those per-file ceilings. No profile charges this
  before either child starts. Not all these maxima need be attainable to invalidate a
  guarantee based solely on the present ceilings.
- A resumed Gold attempt's four Content work directories alone retain the same D5a
  envelope, **144.6875 MiB**, while inherited trees remain separately required.
- Read-only inspection and unsuccessful/repeated qualification create UUID work directories
  without an admission-time lifetime byte/entry reservation. The controls/attempts count is
  not a count of qualification directories. Existing-tree checks cannot reserve the next
  child's output; post-write refusal can strand otherwise useful retained evidence.

There is no demonstrated finite **pre-write aggregate** producer bound for all supported
success, failure, interrupted-child and repeated-qualification paths. The clean-path numbers
above bound files that passed their consumers; they do not bound every file a failed child
may have left before Java reads it. This is why neither a tiny passing fixture nor simply
raising 64 MiB constitutes admission safety.

## Alternatives and selected disposition

**A — Existing format, narrower approved profile.** Preferred if a useful envelope can be
proved. Source count 1, zero retained Silver, bounded reference/selection metadata would
reduce real sizes, but the current profile schema contains no producer/lifetime budgets.
Even that profile has no proved ≤64 MiB bound using existing per-file ceilings. A correct
implementation needs enforceable output bounds for each child, reservations for every
partial file/sidecar/directory, packet/observation overhead, and charged repeated qualifiers
before launch. Post-hoc measurement or rejecting the next stage after publishing is insufficient.
No such profile is introduced or called approved here. The accepted 1–8 API contract stays intact.

**B — Reference existing immutable evidence.** Capture already does this for native data
and bytea snapshots. Removing the D1 spool after Bronze would break original journal
membership, stamps, seal, identity/replay and pre-Bronze recovery; it cannot be replaced by
an unchecked key/path. Even eliminating all eight payload copies saves only 40 MiB from
the loose envelope; it leaves **319.875 MiB** of sidecars. Deduplicating inline Gold/Silver
proofs needs a versioned sealed metadata-reference contract and full original read-only
requalification, retaining old runtime/module bindings. It is not a byte-copy toggle.

**C — Versioned bounded streaming design, proposed only.** Streaming a hash alone reduces
transient file-buffer memory but solves neither producer growth nor the 1 MiB JSON bounds.
A concrete candidate to evaluate, not an implemented/accepted decision, is:

- A separate reviewed one-feed/no-retained-Silver execution profile. Capture limits of
  384 MiB per EXECUTE tree, 128 MiB per RECONCILE tree, 128 MiB per RESUME-control tree;
  512 entries/tree, depth-four files only. These ceilings cover the loose first-pass
  arithmetic, but still require producer/retry proof before approval. Current limits stay unchanged.
- At most three resume qualification directories (two required and one bounded failed
  qualification) with capacity reserved before creation/child authorization. No automatic
  retry, deletion, relocation of old sidecars or bypass when that budget is exhausted.
  Also cap qualification packet creation per control at three, including orphaned/failed
  writes, and charge their root/shards to the same control lifetime ledger; the present
  recovery directory must not remain an uncharged output path.
- Exact enumerable producer file sets, strict pre-write lengths, stage/control/run lifetime
  reservations, all copied metadata and partially written files charged. Original ancestors
  remain immutable; read-only replay writes new bounded verifier evidence separately.
- A versioned root manifest ≤64 KiB, at most four sealed shards ≤256 KiB each, total ≤512
  entries, exact storage/root identity, run/attempt/epoch/checkpoint/stage roles, length,
  SHA-256 and inode/mtime seals. No unchecked directory references or unbounded traversal.
  Existing packet/observation readers cannot silently accept this new representation.
- One fixed supervised read-only capture child, streaming buffers ≤1 MiB, explicit manifest
  parsing limits and JVM heap ceiling 128 MiB (not an RSS or Spark-memory quota). Native
  dependency requalification remains mandatory with its existing independent adapters.
- A 45-second scanner deadline plus existing bounded cleanup within the 60-second review
  window. Check deadline/bytes/counts between chunks; supervise blocked I/O out of process.
  Timeout, inaccessible evidence, unknown child outcome, changed membership/seal or failed
  cleanup retains occupied ownership. No partial result or assumed successful termination.
- Account for all 20 permitted attempts and preserved control trees before advertising a
  run envelope; enforce/profile-bound qualification counts. Verify maximum accepted inputs,
  each first rejected limit, corrupt/missing/unavailable ancestors and rollback/restarts.
  Retain old format/runtime readers for historical attempts. Disk reservations are cooperative
  unless actual filesystem quotas are separately introduced and reviewed.

This candidate is materially larger than a scanner optimization: it changes retained-proof
protocols and writer authorization. The candidate numbers are not a new advertised support
envelope. Native qualifier-output/manifest bounds and compatibility still need proof; the
existing 1 GiB free-space observation cannot reserve the sum of these lifetime budgets.
It is not implemented to satisfy this task by assertion.

**Selected disposition:** preserve the existing format/limits, close the silent-depth hole,
and retain the documented B0 activation blocker. No bounded useful approved profile has been
established, so unsupported oversized *executions* are not newly advertised as safely admitted.
General metadata admission still accepts the established contract; it is not a recovery
guarantee. Production Content dispatch stays disabled. No quota, publisher retry, missing-proof
substitution, analytics activation, native schema change or general storage engine is added.

## Validation and delivery

Acceptance for the implemented hardening: all normal D1–D5a private paths remain capturable;
an unvisited directory subtree cannot produce a complete recovery seal; maximum accepted
and first rejected byte/entry/packet limits are tested without truncation; Content admission
and inherited fencing/native identities remain unchanged. The broader B0 admission guarantee
is **not achieved** and remains the precise blocker described above.

Boundary tests use real private filesystem entries under a held installation guard:
64 MiB accepted/64 MiB+1 rejected; ordinary sidecar 16 MiB+1 rejected; eight 5 MiB payloads;
4096 entries accepted/4097 rejected (root included); long-name zero-byte inventory rejection;
1 MiB recovery JSON file accepted/+1 rejected before creating its directory (storage envelope,
not a claim of semantic packet qualification); two independently
capturable 40 MiB trees; bytea snapshot hash/length projection; hidden/empty depth-four
directories rejected. Existing corrupt/missing/symlink/child-identity cases remain.
The exact 8/9 Content and 32/33 Reliability boundaries are already covered by RunCommandsTest.
Synthetic file tests establish capture behavior, not a worst-case native execution guarantee.

Local environment: macOS 27.2/arm64, GraalVM Java 21.0.7, Python 3.12.15, Spark 4.0.2,
DuckDB 1.4.5, pinned disposable PostgreSQL 17.11 and SeaweedFS 4.48. These are correctness
durations, not benchmarks. No public feeds or cloud credentials are used.

| Validation actually run | Cases | Measured elapsed | Result |
| --- | ---: | ---: | --- |
| Resource/parser/resume-seal/default-off focus | 55 unit, overlapping full unit suite | 6.582s Maven | Passed |
| Combined Content/admission regression | 201 integration + 354 unit | 19m00s Maven; 1144.76s driver | Passed, including wrapper restart and cleanup checks |
| Inherited recovery, included above | 5 integration | 423.417s class | Passed; original/resumed evidence and publications unchanged |
| D5b-1 recovery, included above | 7 integration | 258.243s class | Passed |
| D5b-2a resume, included above | 7 integration | 342.518s class | Passed |
| Persistence, admission, worker, terminal/control, included above | 182 integration | 99.559s combined class time | Passed, including all 30 historical persistence cases |
| Reliability C3c | 247 integration + 53 overlapping unit | 535.71s driver | Passed; finalization/bootstrap/operator/verification restart receipts all present |
| Final unit suite on final source | 354 unit, overlapping combined run | 15.281s Maven | Passed, zero failures/errors/skips |
| CI policy, partitions and aggregate gates | 37 | 2.095s | Passed; 26 integration classes assigned once |
| Markdown/relative links, repository hygiene, whitespace | Consistency checks | Not a benchmark | Passed |
| V1–V9 migration byte/SHA-256 comparison with HEAD | 9 files | Not a benchmark | All unchanged |

Distinct final coverage: **448 PostgreSQL integration + 354 Java unit + 37 CI-policy =
839 cases**, all passing in the final scoped runs. Focused/repeated cases and the 53 C3c
units add no distinct cases. Actual selected XML reports are snapshotted separately under
ignored `outputs/b0-*-reports`; counts are `outputs/b0-distinct-counts.json`. Native and
C3c logs are `outputs/b0-native.log` and `outputs/b0-c3c.log`.

An additional sandboxed unit rerun failed the existing BoundedProcessRunnerTest descendant
cleanup case with “Child still running.” It had passed in the combined native run. An isolated
rerun outside that sandbox passed all nine cases; the final full unit rerun in the same context
passed all 354. This does not establish the failed run's root cause. The ij-debugger workflow
could not launch because IntelliJ exposed no configurations or runnable test locations and no
active debug session. No debugger probes were installed, and no analytics supervisor code or
test timeout was changed. The failed log remains `outputs/b0-units.log`; final output is
`outputs/b0-units-final.log`. The failed rerun is not represented as a passing execution.

Reproduce individual native suites with the tracked disposable drivers (JAVA_HOME must be
JDK 21; DDIP_TEST_PYTHON must identify the pinned native environment):

```sh
python3 scripts/test-rss-fetch-postgres.py --offline --content-inherited-recovery
python3 scripts/test-rss-fetch-postgres.py --offline --content-recovery
python3 scripts/test-rss-fetch-postgres.py --offline --content-resume
python3 scripts/test-recovery-postgres.py --offline
mvn -B -ntp -o -Daether.syncContext.named.factory=noop -f services/content-api/pom.xml test
python3 -I -B -m unittest discover -s .github/ci/tests -v
python3 .github/ci/postgres_partitions.py
npm run check
git diff --check
```

The combined local driver reused test-rss-fetch-postgres with the exact Failsafe selection
`ContentInheritedRecoveryIT,ContentRecoveryIT,ContentResumeIT,ContentFinalizationIT,PersistenceIT,WorkerCoordinationIT,ContentResumeControlIT,RunSubmissionIT`
and enabled all Surefire units. It correctly expected the original-recovery controller restart;
the inherited and resumed cases also directly restart the exact disposable PostgreSQL container.
No test partition, native module, SQL migration or timeout was changed. The full expanded Gold,
ingestion, JDBC/HTTP and frontend suites were not rerun locally; existing CI classification and
required aggregate gates remain intact. Owner-run hosted validation must confirm selected suites,
historical upgrade checks and Gold within its existing 90-minute budget. This task provides no
new hosted result and no Content dispatch/API-worker activation acceptance.

The earlier per-tree limits remain binding. Old evidence with an unsupported deeper directory
is now safely refused; retain it and occupied ownership rather than deleting it to fit. No native
publication is repaired/overwritten, no missing acknowledgment is assumed successful, no prior
module hash is replaced, and no automatic retry/continuation is introduced. The pending B0
blocker is a proved, enforceable aggregate writer/retention envelope including interrupted and
repeated qualification paths—not a fictitious 32-feed Content admission limit.

No branches, staging, commits, pushes, PRs or GitHub writes were performed.

## Exact changed files

- `README.md`
- `docs/adr/0020-inherited-content-reconciliation.md`
- `docs/architecture/durable-execution.md`
- `docs/operations/content-recovery-envelope.md`
- `docs/operations/d5b2b-inherited-recovery-validation.md`
- `docs/operations/managed-rss-fetch.md`
- `services/content-api/src/main/java/platform/contentapi/execution/ContentRecoveryFiles.java`
- `services/content-api/src/test/java/platform/contentapi/execution/ContentRecoveryFilesTest.java`
