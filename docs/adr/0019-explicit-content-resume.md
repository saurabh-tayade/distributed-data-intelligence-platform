# 0019: Explicit fenced continuation of recovered Content runs

Date: October 30, 2026. Status: **Implementation record; owner acceptance pending**.

This additive extension to [ADR 0018](0018-managed-content-recovery-boundaries.md)
implements the explicitly requested PAUSED → QUEUED → new EXECUTE lifecycle. It does
not change ADR 0018's acceptance status or the earlier accepted ADRs. On acceptance,
this record supersedes only its statement that Content has no RESUME implementation.

The existing management endpoint accepts a strict RESUME envelope with null attemptId
and quiescenceConfirmed. The run's optimistic version selects its completed recovery
attempt; callers cannot choose different historical evidence. Admission creates a
REQUESTED control only. A separate installation-guarded operator action qualifies the
retained evidence read-only and atomically applies the control and requeues the original
run. A further explicit operator invocation claims that exact queued run and executes.
Neither API admission nor reconciliation dispatches a child. Production Content worker
capability remains disabled. V9 extends only the controls type constraint; V8 and all
historical SQL remain unchanged.

Supported boundaries are no prior fetch intent with no retained Silver inputs, committed complete Gold, and committed
Content CHECK_INPUTS. Every original prefix stage must already be VERIFIED; independently
recovered files with a lost original stage acknowledgment do not establish continuation
authority. Such histories are rejected with RESUME_CHECKPOINT_UNSUPPORTED.
Gold and Content-input continuation refers to immutable original
stages through a claimed audit plan and reused_checkpoint_id; successful stage rows are
never copied into a new attempt. Existing D5a finalization combines that qualified prefix
with the actual new stage invocations in its existing fenced terminal transaction.

Journal, Bronze and Silver PAUSED checkpoints are explicitly rejected. Their current
execution readers bind delivery ownership to one attempt. Generalizing those readers or
replaying stages would require additional provenance and namespace authority proofs.
Pre-fetch runs with retained Silver are rejected until their complete native dependency
chain can qualify before any new retrieval. One completed Silver source is never promoted to a complete multi-source selection.
Supporting only independently proven boundaries is preferable to claiming arbitrary
stage restart. A completed/incomplete intended Content namespace is rejected before a
resumed publisher launch, without repair, overwrite or a second publication attempt.

The original private evidence and recovery packet/observation remain mandatory. The
existing 4096-entry/64 MiB aggregate private-attempt cap remains binding, including
retained D1 payload bytes. This limits supported resumed workloads as well as recovery.
A retained completion seal for each earlier resume verifier records supervisor-confirmed
process-tree quiescence; a launch/PID-recording gap without that seal vetoes another
qualification. This does not infer termination from parent exit.
Verification errors, unavailable dependencies, possible writers and changed evidence
all block continuation. Reconciliation of an interrupted resumed attempt is not yet
supported: occupied ownership must remain fenced for a future explicit recovery design.

No new native formats, transformations, services or infrastructure are introduced.
Activation, generalized cross-attempt D1/D2/D3 provenance and interrupted-resume recovery
remain D5b-2b owner decisions and acceptance prerequisites.
