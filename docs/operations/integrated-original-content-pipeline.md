# Integrated original-run Content pipeline

[ADR 0043](../adr/0043-integrated-original-content-pipeline.md) describes the integrated narrow
pipeline implementation. **Normal Content dispatch remains gated.** This work does not supersede
the existing restart/recovery qualification requirement and is not an operator activation procedure.

## Scope and evidence

The implementation connects the existing original FETCH/Bronze prefix to all remaining mandatory
Bronze inspections, Silver, Gold and Content preparation/publication/verification/recheck operations.
The original plan has 17 core operations plus two unused post-success inspection slots. Native
command checkpoints and directory checkpoints remain distinct where the plan specifies aliases.

Separately qualified strict Python modules and native Java adapters are used rather than historical
ledger-v1 launchers. Each new invocation consumes its owned prefix before effects, is supervised with
actual cleanup evidence, and produces bounded candidates for independent verification. The native
terminal transaction requires the complete proof and actual artifact dependencies before success and
global-slot release. It does not fabricate legacy stage history.

The supported shape remains one fresh source, no retained Silver, an exact publisher reference,
original uninterrupted ownership and the unchanged 64 MiB / 221-entry private ledger. Runtime bytes,
package versions and the whole required chain must qualify before FETCH. Metadata discovery is not
installed-runtime readiness. Existing profiles retain their historical meaning.

The [submission and discovery contract](narrow-content-submission-discovery.md) documents opt-in
metadata routes, strict-profile shape restrictions and explicit dispatch-disabled markers.

## Read-only diagnostics

The bounded native history endpoint is:

```text
GET /api/management/v1/runs/{runId}/attempts/{attemptId}/native-operations
```

It accepts limit/cursor pagination and returns database observations of the fixed core operations,
including separate directory/native checkpoint IDs, safe phase/checkpoint timestamps and digests.
It returns no command, private proof, path, credential, child PID or continuation authority. A cursor
is scoped to the requested run/attempt and a fixed high-water view; it cannot resume execution.

Native attempts add nativeOperationCount while retaining stageCount for legacy stages. A native
successful result is recognized only through its exact current-attempt completion proof and final
checkpoint. Existing legacy response fields remain unchanged when native history is absent.

## Failure boundaries

Do not infer activation readiness, retry permission or recovered-owner authority from candidates,
checkpoints, historical diagnostics or a completed artifact. Unknown cleanup, changed dependencies,
lost ownership, cancellation and uncertain acknowledgments stop the live channel. Retained prefixes
and occupied attempts are not automatically adopted, repaired, deleted or released. Post-success
inspection slots are not an unlimited replay pool.

The default replay behavior of standalone native libraries remains unchanged. The new strict
original-run publisher requires a fresh namespace and rejects raced existing output; this does not
claim a remote transaction or stronger object-store concurrency semantics.

## Qualification record

Development uses an isolated pinned Python 3.12.15 environment, exact repository processing dependency
versions and a complete Java 21 JDK. Standard release-21 compilation and actual Spark/DuckDB fixtures
are available locally; previous missing-PySpark and JRE limitations do not qualify as current passes.

Focused tests, independent review and full integration results will be recorded for the final change.
Final-head hosted CI, actual partition timings and remaining restart/recovery gates must be reported
separately. Do not count earlier foundation PRs or a standalone publisher pass as full integrated
pipeline qualification.

## Local integration evidence

The real Java/PostgreSQL chain passed all 17 operations through exact native Content verification,
artifact/dependency registration, success and coordinator release. Thirty pipeline/SQL cases passed;
one additional full empty/reference-only run passed while losing final commit acknowledgment and
confirming historical success without retry or new effects. Fault cases include cancellation, lost
release acknowledgment, incompatible probe history, same-byte/new-inode candidate substitutions and
retained complete proof before terminal success.

The strict Python subprocess chain passed all 12 native modes with real Spark, native readers,
GET-only Bronze and actual gateway cleanup. The final reference-identity run took 220.992 seconds.
Fifty-two transport, projection, publisher lifecycle and strict contract regressions also passed.
Independent review reran focused strict/runtime/sink tests and checked exact reference identity,
candidate byte/inode binding, terminal proof, safe diagnostics and legacy compatibility.

Final submission/discovery and legacy submission coverage passed 91 tests. Eleven populated historical
migration-upgrade cases passed using explicit version-asserted pre-V21 fixture admission, without any
production missing-table fallback. The full service unit run used the pinned environment and standard
Java release 21: 890 cases, 887 passed, zero failures/errors and three optional generated-corpus
assumptions skipped because those external fixtures were not supplied. No test exclusion or compiler
override was used. Ingestion production is unchanged; its full regression remains in hosted CI.

All 66 CI-policy tests, Markdown/link checks, hygiene and whitespace checks passed. Actual complete
Python discovery reports 496 core and 152 Reliability cases across 47 modules; the PostgreSQL inventory
assigns all 51 classes exactly once. These are discovery counts, not claims that the entire application
regression suite has executed locally.

Full hosted final-head CI and measured partition capacity remain pending. Real restart/recovery
qualification, normal dispatch activation and broader end-to-end/UI qualification remain separate
open gates; local pipeline success does not complete those gates or issue #49.
