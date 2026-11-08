# Original Content B0 closure review

November 8, 2026. This bounded evidence review covers the original-only narrow Content chain
and failed safe-end under [ADR 0022](../adr/0022-narrow-managed-content-profile.md),
[ADR 0043](../adr/0043-integrated-original-content-pipeline.md) and
[ADR 0044](../adr/0044-interrupted-content-safe-end-contract.md). It changes no accepted contract,
production capability, numerical limit or retained data.

## Verdict and remaining gates

Source review found no concrete uncovered internal original-chain authority transition in the
supported scope: one fresh source, no retained Silver, an exact qualified publisher reference,
original live ownership, and the unchanged 64 MiB / 221-entry plan. Success requires full native
verification and the final SQL transaction. Interruption either permits freshly reviewed failed
safe-end or remains explicitly blocked. Refusal is a supported safety result; the contract does
not promise every admitted workload succeeds.

The [current-envelope](current-content-envelope-qualification.md) and
[producer-pressure](content-pressure-qualification.md) evidence now includes hosted
[CI #219](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/actions/runs/42495925021):
all 19 jobs passed, including 240 native-publication cases and all three pressure cases.
[PR #135](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/pull/135)
merged as `d54fa397`. Native publication took about 75 minutes in Maven and 77 minutes for the job.
The preceding [main CI #218](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/actions/runs/42495790204)
also passed all 19 jobs, with 237 native-publication cases taking about 81 minutes in Maven and
82 minutes for the job. Both fit the 90-minute budget, but the variability and limited headroom
justify the exhaustive partition split described in [CI operations](ci.md).

**This is a bounded closure candidate, not a declaration that Gate B0 is closed or dispatch is
approved.** The remaining evidence-closure conditions are independent acceptance of this matrix
and exact-head CI/capacity verification of the split partitions, without dropped or duplicated
classes or weaker gates. Any review finding must identify a concrete uncovered obligation; there
is no additional unspecified maximum-workload corpus in this verdict. Ordinary-worker integration,
its end-to-end acceptance and the activation decision remain separate work below. Issue #49 is
not complete.

## Envelope and authority matrix

Sizes below are serialized bytes unless stated otherwise. A filesystem slot, a SQL column
ceiling and a stricter semantic reader bound are distinct; the narrowest applicable bound wins.
Source links identify current producers/readers; test links identify evidence, not new authority.

| Boundary | Producer and cap | Reader and refusal | Tests |
| --- | --- | --- | --- |
| Original plan/admission | [ContentOperationPlan](../../services/content-api/src/main/java/platform/contentapi/execution/ContentOperationPlan.java): 17 core operations plus two post-success inspections; 64 MiB / 221-entry original plan, 256 KiB ledger. [ContentEvidenceBudget](../../services/content-api/src/main/java/platform/contentapi/execution/ContentEvidenceBudget.java).reservePlan bounds conservative inventory to 128 KiB and reserves final plus `.preparing` files. | [NarrowContentAdmission](../../services/content-api/src/main/java/platform/contentapi/execution/NarrowContentAdmission.java), [NarrowContentEvidenceContext](../../services/content-api/src/main/java/platform/contentapi/execution/NarrowContentEvidenceContext.java) and [narrow_evidence_sink](../../processing/narrow_evidence_sink.py) require exact sealed paths, caps and identities. Refusal cannot invent READY or adopt partial files. Reservations are not OS quotas. | [ContentOperationPlanTest](../../services/content-api/src/test/java/platform/contentapi/execution/ContentOperationPlanTest.java), [ContentEvidenceBudgetTest](../../services/content-api/src/test/java/platform/contentapi/execution/ContentEvidenceBudgetTest.java), [NarrowContentAdmissionIT](../../services/content-api/src/test/java/platform/contentapi/management/NarrowContentAdmissionIT.java); actual retained-tree measurements. |
| FETCH payload/sidecars | [HttpFeedFetcher](../../ingestion/src/main/java/platform/ingestion/rss/HttpFeedFetcher.java) and [StrictManagedRssFetch](../../ingestion/src/main/java/platform/ingestion/managed/StrictManagedRssFetch.java): payload 5,242,880 bytes; command/metadata/descriptor/result/completion slots 64 KiB each. | [NarrowContentFetchCandidateVerification](../../services/content-api/src/main/java/platform/contentapi/execution/NarrowContentFetchCandidateVerification.java) and completion validation retain native metadata wire bytes. Oversize emits `FETCH_PAYLOAD_TOO_LARGE`; no completed payload/receipt or Bronze effect, occupancy retained. | [NarrowContentPipelineIT](../../services/content-api/src/test/java/platform/contentapi/management/NarrowContentPipelineIT.java): exact payload limit and first rejected byte, with exact failure-code and no-replay assertions. |
| Bronze/Silver commands | [NarrowContentPipelineCommands](../../services/content-api/src/main/java/platform/contentapi/execution/NarrowContentPipelineCommands.java).command and prefix builders: STORE_BRONZE, INSPECT_BRONZE, SILVER, VERIFY_SILVER and RECHECK_SILVER command slots 64 KiB. | [PreflightFiles](../../services/content-api/src/main/java/platform/contentapi/execution/PreflightFiles.java).canonical, actual SourceJson and strict child readers recheck complete bytes/correspondence before effects. The V21 1 MiB SQL ceiling does not widen these slots. | [NarrowContentPipelineCommandsTest](../../services/content-api/src/test/java/platform/contentapi/execution/NarrowContentPipelineCommandsTest.java), [NarrowContentCommandCorrespondenceTest](../../services/content-api/src/test/java/platform/contentapi/execution/NarrowContentCommandCorrespondenceTest.java), [NarrowContentPipelineIT](../../services/content-api/src/test/java/platform/contentapi/management/NarrowContentPipelineIT.java). |
| Gold/Content commands | [NarrowContentPipelineCommands](../../services/content-api/src/main/java/platform/contentapi/execution/NarrowContentPipelineCommands.java).command: complete command <=1 MiB, including Silver qualification, Gold proof, frozen reference and prepared/verified payloads. | [PreflightFiles](../../services/content-api/src/main/java/platform/contentapi/execution/PreflightFiles.java) and [narrow_projection_transport](../../processing/narrow_projection_transport.py).read_command_v2 use actual complete readers and independent expectations. `RESOURCE_LIMIT` stops that operation before reservation/launch; no truncation. | [NarrowContentPipelineCommandsTest](../../services/content-api/src/test/java/platform/contentapi/execution/NarrowContentPipelineCommandsTest.java) exact/plus-one composed-builder corpus; [tests/test_narrow_projection_transport](../../processing/tests/test_narrow_projection_transport.py); actual pipeline/pressure corpus. |
| Release bootstrap | [NarrowContentPipelineCommands](../../services/content-api/src/main/java/platform/contentapi/execution/NarrowContentPipelineCommands.java).bootstrap/qualifyBootstrapCapacity: preliminary packet <65,500; released packet <65,536 including child PID. Fixed-width proof hashes; all downstream capacity checked before FETCH. | [ReadOnlyVerifierProcess](../../services/content-api/src/main/java/platform/contentapi/execution/ReadOnlyVerifierProcess.java) bounded stdin/EOF and [strict_native_content](../../processing/strict_native_content.py) startup parser. The compact packet does not replace complete independently matched command evidence. | [NarrowContentPipelineCommandsTest](../../services/content-api/src/test/java/platform/contentapi/execution/NarrowContentPipelineCommandsTest.java) fixed-header rejection/hash-only invariance; [tests/test_strict_native_pipeline](../../processing/tests/test_strict_native_pipeline.py). |
| Silver whole qualification | [narrow_silver_precompletion](../../processing/narrow_silver_precompletion.py) / [narrow_content_profile](../../processing/narrow_content_profile.py): completion 4 KiB, local capture 32 KiB, inventory identity 16 KiB, verification 52 KiB, qualification 60 KiB, selection 36 KiB. <=4 analytical files/dataset, 24 regular files, four directories. | [NarrowContentProfile](../../services/content-api/src/main/java/platform/contentapi/execution/NarrowContentProfile.java), [RegisteredImportEvidence](../../services/content-api/src/main/java/platform/contentapi/importing/RegisteredImportEvidence.java) and actual small reader. Prospective whole bounds precede marker completion; actual verification follows. VERIFY_SILVER SQL evidence <=60 KiB; its 512 KiB filesystem slots do not widen semantic caps. | [NarrowContentProfileTest](../../services/content-api/src/test/java/platform/contentapi/execution/NarrowContentProfileTest.java), [tests/test_narrow_silver_precompletion](../../processing/tests/test_narrow_silver_precompletion.py); mixed accepted/rejected pressure case. |
| Publication/artifact/recheck | [narrow_gold_publication_v2](../../processing/narrow_gold_publication_v2.py) and [narrow_content_publication_v2](../../processing/narrow_content_publication_v2.py) project complete temporary-marker capture. [narrow_publication_lifecycle_v2](../../processing/narrow_publication_lifecycle_v2.py).qualify checks full command/evidence/artifact/applicable wrappers at 1 MiB; result 64 KiB. [NarrowContentPipeline](../../services/content-api/src/main/java/platform/contentapi/execution/NarrowContentPipeline.java).artifact independently caps actual artifact at 1 MiB. | [narrow_publication_projection_v2](../../processing/narrow_publication_projection_v2.py) and transport readers bind v2 context. Full qualification precedes rename; actual native verification, projection comparison and qualification follow. Failure retains output and cannot grant SQL success. Recovery/RESUME shape checks grant no continuation authority. | [tests/test_narrow_publication_projection_v2](../../processing/tests/test_narrow_publication_projection_v2.py), [NarrowNativePublisherTransportTest](../../services/content-api/src/test/java/platform/contentapi/execution/NarrowNativePublisherTransportTest.java); pipeline/pressure actual artifact measurements. |
| Report rows/reference | [narrow_content_profile](../../processing/narrow_content_profile.py).validate_report_row / [narrow_content_reports_v2](../../processing/narrow_content_reports_v2.py): complete public row <=61,440 canonical UTF-8 bytes. [publisher_reference](../../processing/publisher_reference.py) caps 10,000 reference rows. | Production all-row precompletion scan, not a smoke page. Oversized row fails before Content completion/artifact/success; reference row 10,001 fails before selectable snapshot. | [NarrowContentPressureIT](../../services/content-api/src/test/java/platform/contentapi/management/NarrowContentPressureIT.java): two exact rows, 10,000 reference/coverage rows and late 61,441-byte row beyond first ten; [tests/test_narrow_content_profile](../../processing/tests/test_narrow_content_profile.py) reference-plus-one. |
| Candidate/result/phases | [strict_native_content](../../processing/strict_native_content.py) result serialization <=65,535; result slot/read <=65,536. Evidence/selection use stage slots. [NarrowContentPipeline](../../services/content-api/src/main/java/platform/contentapi/execution/NarrowContentPipeline.java).phase has six phases per suffix operation. | [NarrowContentPipelineScope](../../services/content-api/src/main/java/platform/contentapi/execution/NarrowContentPipelineScope.java).complete pins actual bytes/identity; Pipeline.validateResult/semantic checks runtime, command, cleanup and successors. Unknown cleanup, failed child, mutation, controls or ownership loss stop continuation. | [NarrowContentPipelineIT](../../services/content-api/src/test/java/platform/contentapi/management/NarrowContentPipelineIT.java) mutation/cancellation/lost-release cases; [NarrowContentPipelineCrashIT](../../services/content-api/src/test/java/platform/contentapi/management/NarrowContentPipelineCrashIT.java) live-child/live-Spark containment; strict subprocess tests. |
| Checkpoint/final proof | [NarrowContentPipeline](../../services/content-api/src/main/java/platform/contentapi/execution/NarrowContentPipeline.java).execute: whole checkpoint proof <=3 MiB, 15 suffix checkpoints. succeed binds ordered checkpoint hashes and actual artifact/dependencies; [V21](../../services/content-api/src/main/resources/db/migration/V21__native_content_pipeline.sql) final proof <=64 KiB. | [NarrowContentPipelineRecoveryAssessment](../../services/content-api/src/main/java/platform/contentapi/execution/NarrowContentPipelineRecoveryAssessment.java) reconstructs records; V21 enforces exact scope/correspondence and atomic success closure. [WorkerRepository](../../services/content-api/src/main/java/platform/contentapi/execution/WorkerRepository.java).finishNarrowContent coordinates release. Unknown acknowledgment does not grant continuation. | [NarrowContentPipelineSqlIT](../../services/content-api/src/test/java/platform/contentapi/management/NarrowContentPipelineSqlIT.java), [NarrowContentPipelineRestartIT](../../services/content-api/src/test/java/platform/contentapi/management/NarrowContentPipelineRestartIT.java), [NarrowContentPipelineCrashIT](../../services/content-api/src/test/java/platform/contentapi/management/NarrowContentPipelineCrashIT.java); actual complete-proof measurements. |
| Artifact registration gaps | [NarrowContentPipeline](../../services/content-api/src/main/java/platform/contentapi/execution/NarrowContentPipeline.java).registerSilver/registerGold register bounded actual evidence and dependencies in separate fenced transactions. Content registration is in final success transaction. | Artifact/SQL validators and [NarrowContentPipelineRecoveryAssessment](../../services/content-api/src/main/java/platform/contentapi/execution/NarrowContentPipelineRecoveryAssessment.java) preserve graph correspondence. Intermediate registration is neither next-stage authority nor Content success. | [NarrowContentPipelineSqlIT](../../services/content-api/src/test/java/platform/contentapi/management/NarrowContentPipelineSqlIT.java) exact graph/counts; [NarrowContentPipelineCrashIT](../../services/content-api/src/test/java/platform/contentapi/management/NarrowContentPipelineCrashIT.java) separate Silver/Gold post-registration safe-end cases. |
| Interrupted evidence tree | [NarrowContentTerminalEvidence](../../services/content-api/src/main/java/platform/contentapi/execution/NarrowContentTerminalEvidence.java).tree/read: <=17 ordered operations, <=15 suffix operations with six phases each; depth-four walk rejects >221 entries; exact paths/caps and <=64 MiB total. Two current inventories and separately reconstructed SQL history. | Required missing, altered, unknown, linked or oversized evidence vetoes mutation. External native output remains unverified and requires stopped-writer review. No claim of historical inode continuity. | [NarrowContentTerminalEvidenceTest](../../services/content-api/src/test/java/platform/contentapi/execution/NarrowContentTerminalEvidenceTest.java), [NarrowContentPipelineRecoveryAssessmentIT](../../services/content-api/src/test/java/platform/contentapi/management/NarrowContentPipelineRecoveryAssessmentIT.java); crash and pressure no-new-effects/retained-tree assertions. |
| Safe-end disposition | [NarrowContentTerminal](../../services/content-api/src/main/java/platform/contentapi/execution/NarrowContentTerminal.java): target/review 8 KiB each, snapshot 64 KiB; [V22](../../services/content-api/src/main/resources/db/migration/V22__native_content_terminal_recovery.sql) independently bounds reconstruction. Six audit events, details <=8 KiB; capacity reserve checked. Review freshness 60 seconds; [NarrowContentTerminalOperator](../../services/content-api/src/main/java/platform/contentapi/execution/NarrowContentTerminalOperator.java) wire <=16 KiB, <=32 additional descendants. | Canonical/digest/identity validation, one-use capability consumed before mutation, coordinator-first fence, fresh exact review and single atomic receipt/disposition. Overflow retains occupancy; uncertain commit resolves authoritatively, never by blind retry. | [NarrowContentTerminalIT](../../services/content-api/src/test/java/platform/contentapi/management/NarrowContentTerminalIT.java), [ContentTerminalSqlIT](../../services/content-api/src/test/java/platform/contentapi/management/ContentTerminalSqlIT.java), [NarrowContentTerminalOperatorIT](../../services/content-api/src/test/java/platform/contentapi/management/NarrowContentTerminalOperatorIT.java); pressure worker death, measured target/review/snapshot, real PostgreSQL restart and fresh-JVM lookup. |

The shared [SourceJson reader](../../services/content-api/src/main/java/platform/contentapi/management/SourceJson.java)
has nesting depth 16, number length 20, and string constraints of 65,536 characters for the small
reader or 131,072 characters for the evidence reader, measured in UTF-16 units rather than UTF-8
bytes. Field-name limits remain independent. PreflightFiles requires byte-identical canonical
roundtrips; [Python transport](../../processing/narrow_transport_json.py) and parity tests cover
cross-language grammar. Synthetic exact/plus-one vectors prove enforcement, not native reachability.

## Supported blocked outcomes

- Pre-READY partial admission without a verifiable sealed root; missing, changed, unreadable or
  unclassifiable evidence; unsupported hardlinks or unknown paths. Preserve occupancy and bytes;
  do not repair or adopt them.
- Live or unobservable writers, incomplete process/namespace review, competing controls or review
  changes. Negative PID observations and worker shutdown alone do not establish quiescence.
- Already committed success is historical success, ineligible for failed safe-end. A contradictory
  history or uncertain outcome cannot be forced into failure; inspect authoritative history first.
- A lost opaque FETCH recovery owner from the earlier two-transaction handoff remains occupied.
  [#124](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/issues/124)
  reauthorization and [#123](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/issues/123)
  continuation remain deferred. Direct atomic full-Content safe-end does not create that intermediate
  owner. Neither issue waives #49's original safety and complete-workflow obligations.

No retry, refetch, partial-publication repair, resumed-owner reconstruction, finalized-run revival,
wider profile, automatic cleanup or actual retained-data disposition is authorized here. Existing
live-child/Spark crash tests establish containment; the pressure death case occurs after child exit
and supplements, rather than replaces, those tests. See the
[restart/safe-end procedure](native-content-restart-qualification.md).

## Finite ordinary-worker activation checklist

At the #136 review baseline,
[WorkerExecutionCapability](../../services/content-api/src/main/java/platform/contentapi/execution/WorkerExecutionCapability.java)
and [WorkerIdleLifecycle](../../services/content-api/src/main/java/platform/contentapi/execution/WorkerIdleLifecycle.java)
supported ordinary Reliability only. [ADR 0045](../adr/0045-ordinary-narrow-content-worker.md) and
[ADR 0046](../adr/0046-bounded-public-fetch-lane.md) now govern the unmerged implementation of
ordinary packaged claim/admission → strict original Content execution → confirmed release or
retained occupied failure. Candidate wiring is exercised only in disposable qualification
installations. Public packaged acceptance, independent code review and exact-head CI remain open;
this checklist does not turn the earlier internal-chain evidence into operational readiness.
See the [candidate worker procedure](ordinary-content-worker-qualification.md) for exact startup
selection, retained-failure behavior and the separate public acceptance gates.

1. Accept the precise original-only scope and explicit blocked outcomes above. Preserve historical
   runtime/ledger meanings; metadata discovery must not imply installed readiness.
2. Define installed startup/profile selection and qualify the complete native chain and bootstrap
   before FETCH, without widening source/dependency shapes.
3. Wire ordinary claim to narrow durable admission and the existing pipeline under the single
   coordinator and host guard. Preserve incompatible-profile exclusion, queue ordering, immutable
   inputs and Reliability/import mutual exclusion.
4. Define supervision for heartbeat refresh, shutdown/control boundaries, cleanup uncertainty,
   preflight/admission failure, success and lost acknowledgment. Never map an exception to
   unconditional release, automatic retry or reconstructed authority.
5. Qualify packaged API submission → ordinary worker → verified Content result/history, duplicate
   submission without rewritten publications, incompatible queued work, retained original failure
   and freshly reviewed failed safe-end. Cover restart/uncertain acknowledgment and mutual exclusion
   through this new wiring; reuse deeper lifecycle suites rather than duplicating every fault.
6. Document exact startup, migration, profile and operator procedures, retained-data behavior and
   blocked outcomes. Keep Operations/UI acceptance a separate finite #49 deliverable.
7. Obtain independent authority/source review, exact-head complete CI/capacity evidence and the
   activation decision before ordinary dispatch. This document supplies none of those permissions.

## Closure stopping rule

The existing matrix plus successful pressure/CI #219 evidence addresses the identified original-chain
qualification corpus. Finish the explicit split-capacity and independent-review conditions above;
add tests only for a concrete uncovered contract obligation. Do not manufacture four-plus-four
Silver layouts, special filenames or native-reader maxima without a reachable producer risk.
`local[2]` and two shuffle partitions do not prove a universal output-file maximum, but that fact
alone is not a new qualification milestone.

Retained byte/file observations are not peak Spark memory or scratch measurements. Investigate
actual capacity failures; do not expand this gate into the E4 benchmark expressly excluded by
[#49](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/issues/49).
The issue permits interrupted attempts to recover safely **or block explicitly**. Bounded failure
and the separate operational activation checklist are not reasons to implement deferred #123/#124.
