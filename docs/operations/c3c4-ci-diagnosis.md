# C3c.4 acknowledgment-loss fixture correction — PR 66

## Observed failure and diagnosis

The [failed job 121260469835](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/actions/runs/40223680654/job/121260469835)
finished Maven Failsafe with 702 PostgreSQL cases, two failures and zero errors. Both failures were
the parameterized packaged acknowledgment-loss method in OperatorRecoveryIT. The job did not time
out: its PostgreSQL step took 35m53s (Maven reported 35m51s), and the job lasted 49m18s. The current
[workflow](../../.github/workflows/ci.yml) has a 90-minute processing-job budget.

This was a **test-only TCP relay defect**, not missing native publication evidence or an incorrect
recovery disposition. The relay's sockets used Nagle buffering while forwarding small PostgreSQL
protocol writes. On Linux, this introduced repeated delayed-ACK stalls. A read-only transaction
that should take milliseconds repeatedly took about 83 ms; larger fenced transactions took seconds.
Buffering individual frames alone still failed. Explicit TCP_NODELAY on both relay legs removed
the stalls. No application or native contract, configuration, dependency or deadline changes.

The failure was reproduced twice for both parameters in a disposable Linux environment, including
the actual packaged application, pinned native readers and PostgreSQL 17.11:

1. Handoff committed; only its selected acknowledgment was lost in the HANDOFF parameter.
2. VERIFY_RESULT item 0 independently verified the publication. Finalization confirmation was fresh.
3. VERIFY_RESULT item 1 also completed VERIFIED_PUBLICATION, phase OUTPUT, readiness READY, no native
   error. Both children were recorded and the native verification stages took about 4.4 seconds.
4. The final atomic disposition transaction rolled back after 3,033 / 3,027 ms, with its last forwarded
   mutation updating the run. This exhausted the existing three-second management transaction budget.
   The buffering-only experiment likewise rolled back after about 3.1 seconds, at the control update.
5. The command caught MANAGEMENT_UNAVAILABLE from finish and called the historical finalization
   resolver. There was no committed RECOVERY_FINALIZED audit/result to confirm. The resolver returned
   UNRESOLVED; complete rejected that receipt with EVIDENCE_UNAVAILABLE. This was unavailable **committed
   finalization evidence**, not unavailable native verification evidence.
6. The coordinator remained occupied and there was no completion audit or result artifact. In the
   FINALIZED parameter, the proxy had not dropped any acknowledgment yet: disposition failed before
   that injection point. Neither the proxy nor the application retried native execution.

The relevant path is [OperatorRecoveryConfiguration](../../services/content-api/src/main/java/platform/contentapi/execution/OperatorRecoveryConfiguration.java)
→ [RecoveryFinalization.finish/apply/resolve](../../services/content-api/src/main/java/platform/contentapi/execution/RecoveryFinalization.java)
→ [ManagementTransactions](../../services/content-api/src/main/java/platform/contentapi/execution/ManagementTransactions.java).
[RecoveryVerification](../../services/content-api/src/main/java/platform/contentapi/execution/RecoveryVerification.java)
and its retained evidence reader confirmed both native observations before disposition. The fixed
[native adapter](../../services/content-api/src/main/resources/recovery/reliability_verification.py) was unchanged.

The 60-second confirmation freshness limit and fixture's 30-second native verification deadline were
not the exhausted limits. Confirmation receipt ages in the instrumented Linux reproduction were
1–6 ms. The HANDOFF and FINALIZATION reviews are distinct: time between them does not extend either
approval. The final failure occurred about 29.3 seconds after the second confirmation.

| Boundary timing | HANDOFF parameter | FINALIZED parameter |
| --- | ---: | ---: |
| Hosted failing case total | 72.39s | 70.99s |
| Hosted HANDOFF → FINALIZATION prompt | 28.130s | 26.880s |
| Linux reproduction, HANDOFF → FINALIZATION prompt | 29.025s | 27.565s |
| Linux reproduction, operator start → failure | 71.937s | 70.544s |
| Corrected Linux, HANDOFF → FINALIZATION prompt | 0.414s | 0.432s |
| Corrected Linux, operator start → completion | 2.131s | 2.127s |
| Corrected Linux, atomic disposition transaction | 11ms | 11ms |

These are observations on tiny synthetic fixtures, not performance benchmarks or hosted timing claims.

## Correction and diagnostics

[RecoveryAcknowledgmentProxy](../../services/content-api/src/test/java/platform/contentapi/execution/RecoveryAcknowledgmentProxy.java)
now enables TCP_NODELAY on both accepted/upstream sockets and buffers type/length/body into complete
frames before flushing. It still drops exactly one selected backend COMMIT acknowledgment after the
real commit. It does not drop an initial challenge, native result, audit INSERT acknowledgment or an
unrelated transaction. A bounded transaction tail reports only fixed operation kinds and durations.

[OperatorRecoveryIT](../../services/content-api/src/test/java/platform/contentapi/execution/OperatorRecoveryIT.java)
asserts both confirmation phases and whether the chosen acknowledgment has been dropped before final
approval. Unexpected terminal output fails immediately with bounded status/phase timing and diagnostic
fields: checkpoint, verification ordinal/state/outcome, child presence, native classification/readiness,
coordinator occupancy and completion-audit count. Native fields are allowlisted and reads bounded.
It does not dump private commands, retained/native payloads, SQL, exception text, credentials or paths.

[RecoveryAcknowledgmentProxyTest](../../services/content-api/src/test/java/platform/contentapi/execution/RecoveryAcknowledgmentProxyTest.java)
checks both injection actions with real loopback sockets. Startup negotiation and complete frames are
preserved; SELECT payloads, a different table, a different audit action and an INSERT completion alone
cannot trigger the selected COMMIT drop. The packaged PostgreSQL cases separately establish actual
commit and single-effect semantics. No latency threshold substitutes for those assertions.

No production files changed. The existing host guard, same-live-owner authorization, freshness,
independent verification, one-time native launch, immutable evidence and exact-owner atomic release
remain in force. Uncertain/rolled-back disposition remains occupied. No ADR acceptance is changed.

## Reproduction and validation

Use JDK 21, the documented absolute DDIP_TEST_PYTHON interpreter, and dedicated disposable
DDIP_TEST_DB_URL/USER/PASSWORD settings from the [backend test procedure](../../services/content-api/README.md#e5-b1-persistence-and-runtime-roles).
The initial command ran before any correction:

```sh
mvn -B -ntp -f services/content-api/pom.xml -Ppostgres verify \
  -Dit.test='OperatorRecoveryIT#lostAcknowledgmentAtActualProcessBoundaryResolvesOnlyExactFacts'
```

The additional focused negative matrix and recovery regressions use:

```sh
mvn -B -ntp -f services/content-api/pom.xml -Ppostgres verify \
  -Dtest=RecoveryAcknowledgmentProxyTest,RuntimeRoleTest \
  -Dit.test='OperatorRecoveryIT#lostAcknowledgmentAtActualProcessBoundaryResolvesOnlyExactFacts+missingOrInconsistentApprovalCannotTransfer+uncertainOrChangedEvidenceRetainsOccupiedRecovery'
python3 scripts/test-recovery-postgres.py --offline
python3 scripts/test-recovery-postgres.py --full --offline
```

Local execution used the populated Maven cache with -o and -Daether.syncContext.named.factory=noop.
Native environment: Python 3.12.15, PySpark 4.0.2, DuckDB 1.4.5, UTC and digest-pinned PostgreSQL 17.11.
macOS used GraalVM 21.0.7; the disposable Linux aarch64 image used Temurin 21.0.12.1+1, matching the
hosted JDK patch. Hosted Ubuntu is x86_64. The IDE exposed only a GENERAL_MODULE and could not create
a runnable Java debug configuration; runtime evidence came from actual packaged-process output,
fixed PostgreSQL protocol observations and read-only database/private evidence projections.

Executed October 25, 2026 (EDT). Final passes have zero failures, errors and skips. Method-command
elapsed times include Maven/fixture setup; harness times also include disposable-container lifecycle.
The full backend was run once after the focused correction stabilized.

| Validation phase | Cases / result | Elapsed seconds |
| --- | --- | ---: |
| Initial macOS command, before correction | 324 unit + 2 affected cases passed | 43.81 |
| Linux reproduction | Both affected cases failed with the hosted sequence | 169.72 |
| Linux transaction-trace reproduction | Both affected cases failed; native items verified, final SQL rollback | 171.33 |
| Frame buffering alone | Both affected cases still failed | 167.86 |
| Corrected Linux first pass | 2 affected cases passed | 35.29 |
| Corrected Linux repeats | 2 + 2 affected cases passed | 33.79 / 32.31 |
| Corrected Linux all units plus affected method | 326 unit + 2 affected cases passed | 45.13 |
| macOS focused acknowledgment/negative matrix | 19 unit + 14 PostgreSQL cases passed | 58.17 |
| Complete operator and affected recovery suites | 53 unit + 246 PostgreSQL cases passed; four actual restarts | 496.95 |
| Subsequent complete content-api PostgreSQL profile | 326 unit + 702 PostgreSQL cases passed; four actual restarts | 1260.73 |
| Complete Python processing | 373 passed | 507.97 |
| Java ingestion | 29 passed | 9.16 |
| Batch-launch argument/behavior checks | Passed | 0.20 |
| Fresh synthetic analytics fixture generation | Passed | 46.08 |
| JDBC API | 48 passed | 9.50 |
| Analytics HTTP/native compatibility | 20 passed | 24.06 |
| Complete backend phase sum | 1498 test cases, plus launcher/fixture checks; 30m57.70s | 1857.70 |

The complete recovery pass included OperatorRecoveryIT 35 / 218.472s, RecoveryHandoffIT 118 / 83.401s,
RecoveryVerificationIT 40 / 80.113s and RecoveryFinalizationIT 53 / 109.892s. In the full profile,
OperatorRecoveryIT passed 35 cases in 239.167s; its two affected invocations took 3.412s and 3.209s.
The negative matrix retained occupied ownership for missing/wrong approval, live descendants, changed
private/ownership evidence, unavailable dependencies, changed output and pending cancellation. Native
SHA-256/size/modification-time snapshots remained unchanged in the corrected successful and blocked
cases; existing deliberate-mutation cases compare preservation after the injected mutation.

A preliminary Linux-container bootstrap also ran 324 units and hit one unrelated analytics child-liveness
assertion in 20.57s while Maven was PID 1. The diagnostic invocation was corrected to use Docker --init;
all 326 Linux units subsequently passed, including that unchanged test. This was separate from the
acknowledgment reproduction. No required case was skipped or weakened. The sum of recorded validation
phase times, including these failed setup/reproduction experiments, is 3132.63s; it excludes image
building, investigation and documentation checks and is not a performance benchmark.

All 167 tracked production/native/build files in the saved inspection matched HEAD; production Java,
native modules, migrations, POM, CI and ADRs are unchanged. Test sources stabilized before the full
regression, and the packaged JAR excludes the fault proxy, unit/integration fixtures and publishers.
The existing Python run emitted PySpark socket ResourceWarnings without test failures.

Markdown/link checks passed across 50 files. Tracked/task-file whitespace checks passed. The whole
repository hygiene command flags trailing whitespace only in the pre-existing untracked gh-ci-log.tct;
that user file was preserved. Logs, timing snapshots and generated fixtures remain ignored under
outputs/ and target/.

| PostgreSQL suite in the final full profile | Cases | Maven class seconds |
| --- | ---: | ---: |
| ImportOwnershipIT | 42 | 9.943 |
| OperatorRecoveryIT | 35 | 239.167 |
| PersistenceIT | 28 | 12.847 |
| RecoveryFinalizationIT | 53 | 119.462 |
| RecoveryHandoffIT | 118 | 90.056 |
| RecoveryVerificationIT | 40 | 81.116 |
| ReliabilityExecutionIT | 27 | 220.402 |
| ReliabilityPreflightIT | 41 | 29.983 |
| ReliabilityWorkerIT | 23 | 146.946 |
| ImportWorkflowIT | 38 | 23.309 |
| SilverImportIT | 23 | 142.368 |
| ControlAdmissionIT | 52 | 25.719 |
| RunHistoryIT | 46 | 36.487 |
| RunSubmissionIT | 44 | 20.767 |
| SourceManagementIT | 36 | 15.388 |
| WorkerCoordinationIT | 56 | 30.489 |

## Hosted impact and remaining uncertainty

The correction removes artificial relay delay from these two fault-injection cases. It does not
increase CI timeouts, relax runtime deadlines, retry ambiguous native work or alter normal operation.
A new hosted run is still required to establish the result on Ubuntu x86_64 and its total timing
margin. No GitHub rerun or mutation was performed during this investigation.

Changed files are this report and the three linked test files above. The user-supplied gh-ci-log.tct
was left untouched. No staging, commit, push, branch switch, issue update or PR mutation was performed.
