# R1.1 Reliability operator journey and UI acceptance

Baseline: `21c0897e1455274904a69b2fe018846c6ba1e853` (November 11, 2026).
Tracking: [#145](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/issues/145),
[#53](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/issues/53),
[#49](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/issues/49).
This is the approved bounded R1.1 UI slice. It does not close these umbrella issues.

## Baseline investigation and findings

GitHub main and local main both resolved to the baseline. The only pre-existing worktree change
was the untracked November 11 capability-gap assessment, which was read as reference and left
untouched and outside this slice's commit. The owner confirmed this agent is the sole editor of
`feature/r1-1-reliability-operator-journey`; no second agent was delegated branch work.

Existing behavior observed in source and component tests:

- Source registration/revisions/admission, compatible input discovery and explicit Reliability
  submission already exist. Imports and profile provisioning are operator-only prerequisites.
  Enabling a source does not ingest or execute it.
- Selection checks current revisions; profile changes clear dependent selections. Frozen intents
  retain exact bodies/keys and require explicit confirmation or same-request resolution.
- Run, attempt, stage, audit and control receipts have separate state and refresh semantics.
  Guarded retry needs trusted operator commands; cancellation can require independent recovery.
- Gold, enriched Content and Reliability have separate startup-selected publications and session
  identity guards. A completed run does not activate any of those selections.

Code-observed usability gaps selected for R1.1: Source Detail had an execution-history link but
no direct route to preparing inputs; frozen review led with expanded JSON; run state/outcome and
next-step guidance competed with technical metadata; the run-to-Analytics handoff lacked an
explicit navigation link. These are bounded usability improvements, not demonstrated browser
defects. No pre-existing processing, API or safety defect was established.

## Changes and preserved contracts

- Sources list and enabled Reliability Source Detail link to **Choose Reliability inputs**.
  Links do not preselect a source, submit or adopt a changed revision. Disabled sources explain
  admission management. Forms/actions clarify registration, immutable revisions and prerequisites.
- Submission explains profile → enabled source → matching batches → service reference → review
  → explicit confirmation. Labels lead with format/batch/reference names and retain exact IDs.
- Frozen review summarizes only the existing validated saved request. Batch IDs and full JSON
  remain expandable. It does not reload current configuration or rewrite the saved command/key.
- Run inspection leads with state-specific guidance and recorded outcome. Failed refreshes label
  the summary **Last recorded state** alongside the existing stale warning and disabled controls.
  Metadata, captured references, counts and independent histories remain inspectable.
- A separate Reliability handoff links to existing Analytics and explains startup selection.
  Result metadata remains historical and has no artifact link. All three selections stay separate.

No API client contract, hook, backend, processing module, migration, dependency, accepted ADR,
execution eligibility, polling schedule or publication identity changed. No upload, automatic
trigger/retry/promotion, registry, progress percentage, live monitoring or recovery capability
was added. Attempt/stage/audit components were reviewed and retained; their order and ownership
are not relabelled as a live chronological pipeline.

## R1 acceptance matrix

Automated evidence uses existing synthetic fixtures and mocked HTTP transport. It does not run
a real worker or constitute browser acceptance. Every actual-browser criterion remains open.

| Criterion | Required behavior | Automated evidence | Browser evidence / outstanding acceptance |
| --- | --- | --- | --- |
| R1-AC01 | Distinguish registration, revision, admission and execution; explain installed/imported prerequisites | [SourceActions](../../frontend/src/SourceActions.test.tsx), [SourceDetailApp](../../frontend/src/SourceDetailApp.test.tsx), [new journey tests](../../frontend/src/ReliabilityOperatorJourney.test.tsx): enabled/disabled source handoff, no automatic intent | Outstanding: actual registration/revision/admission forms and outcomes |
| R1-AC02 | Profile → enabled sources → matching revision/format/storage batches → service reference; compatibility/empty/error states | [ReliabilitySubmission](../../frontend/src/ReliabilitySubmission.test.tsx): changed/incompatible sources, batches, profile reset, absent inputs and discovery failure; new journey reuses matching fixtures | Outstanding: real discovery and incompatible/empty selections |
| R1-AC03 | Readable frozen review retains exact revisions/artifact IDs/body/key and focus | New journey verifies summary, expandable IDs/JSON, original request/key and review-heading focus; [submission safety](../../frontend/src/RunSubmissionSafety.test.tsx) | Outstanding: visual scan, keyboard disclosures and narrow-screen IDs |
| R1-AC04 | Explicit confirmation/resolution only; ambiguous response retains original intent without automatic replay | New journey asserts exactly one POST with original body/key; [RunSubmissionSafety](../../frontend/src/RunSubmissionSafety.test.tsx) covers uncertain/rejected/lost outcomes | Outstanding: real admission and ambiguous response |
| R1-AC05 | Source→selection→admitted-run navigation; admission differs from execution | New complete navigation scenario and eight run-state cases; [RunInspectionApp](../../frontend/src/RunInspectionApp.test.tsx) preserves active/terminal boundaries | Outstanding: actual admitted/active/successful worker run |
| R1-AC06 | Honest outcome/next step, absent result and historical verification; stale controls disabled | New all-state, first-read-error and stale-refresh tests; existing [RetryCancelSafety](../../frontend/src/RetryCancelSafety.test.tsx), run inspection and control suites | Outstanding: actual failure/cancellation/recovery/too-late outcomes |
| R1-AC07 | Separate attempts, selected-attempt stages and audit histories | New journey opens stage/audit panels; [StageInspectionApp](../../frontend/src/StageInspectionApp.test.tsx), run inspection and history hook suites | Outstanding: genuine records, independent refresh and paging |
| R1-AC08 | Result handoff does not activate Analytics; all three publication pins remain independent | New different-run/analytics-fingerprint handoff and cross-workspace pin test; [ContentReports](../../frontend/src/ContentReports.test.tsx), [ReliabilityAnalytics](../../frontend/src/ReliabilityAnalytics.test.tsx) preserve mismatch/stale guards | Outstanding: compare real managed output and separately selected analytic fixture |
| R1-AC09 | Keyboard/focus, navigation, Back/Forward and narrow-screen usability | [OperationsApp keyboard test](../../frontend/src/OperationsApp.test.tsx) checks added source link; new review-focus/Back/Forward tests; existing accessible scrolling tests. jsdom has no layout. | Outstanding: actual keyboard, responsive layout, visual focus and browser history |
| R1-AC10 | Loading/empty/safe errors/stale completions preserve safety and contracts | New pending/error/stale/no-mutation cases; full existing Vitest safety/contract suites, typecheck/lint/build | Outstanding: real API failure/absent/stale scenarios; #53 remains open |

## Executed validation

Local tooling uses existing Node 24.22.2 and locked dependencies. Baseline frontend suite:
**1,498 passed, two existing optional native-fixture tests skipped** (40 passed files, two skipped).

| Command | Result |
| --- | --- |
| `npm --prefix frontend run typecheck` | Passed |
| `npm --prefix frontend run lint` | Passed with zero warnings, including the final test changes |
| `npm --prefix frontend test` | 1,511 passed, two existing optional generated native-parity tests skipped; 41 passed files and two skipped files. Includes 13 new journey cases. |
| `npm --prefix frontend run build` | Passed, including typecheck; existing >500 kB chunk advisory remains (833.97 kB main JS / 233.36 kB gzip). No chunk-limit change. |
| `npm run check` | Passed: 143 local Markdown files, zero formatting/relative-link issues; repository hygiene passed. The count includes the preserved untracked assessment report, which is excluded from the commit. |
| `git diff --check` | Passed; new-file whitespace also covered by repository hygiene |

An initial focused regression found the new Analytics link inside a deliberately link-free
result record. It was moved into a separate handoff panel, preserving the original assertion.
New test fixtures initially needed method/type/error-heading corrections. The full suite caught
the source link's additional tab stop; the existing keyboard regression now checks it and retains
the subsequent filter-focus assertion. No safety test was removed or weakened.

No analytical schema/parser/query/native boundary changed. Native/API and Spark/SQL suites were
not rerun locally for this presentation/navigation slice. Default optional generated native-parity
tests remain identifiable as skips, not passes. Required PR CI scope/results are recorded in the PR.

## Browser and disposable-demo limitations

Computer-use inventory returned no browsers and **“The browser access host is unavailable.”**
No screen, screenshot, keyboard or browser interaction was observed. Component assertions are
not reported as browser results.

Read-only preflight found no Docker daemon socket. The documented example native Python
environment path was absent. An existing `local-data/operations-demo/` directory was present,
so it was neither adopted, mutated nor removed. Even read-only launcher `status` could not
complete because the environment denied `ps`. No `up`, `down`, forced cleanup, system repair
or infrastructure startup was attempted. These are acceptance blockers, not R1.1 runtime fixes.

Use the existing [demo runbook](local-operations-demo.md) in an available supported environment
with independently owned disposable resources. Its 15-occurrence managed Reliability result
and separate 10-occurrence analytical fixture intentionally differ. Confirm their distinct
identities through the handoff, then inspect every criterion with actual keyboard, narrow-layout,
error and history scenarios. Do not mutate a retained installation to manufacture acceptance.

## Remaining ownership

- **#145:** actual joint visual/keyboard/narrow-screen review, selected follow-up polish and owner
  agreement on iteration completion.
- **#53:** real demo/browser walkthrough, screenshots, clean lifecycle and retained-state restart.
  Component tests and PR CI do not close release acceptance.
- **#49:** operator-only imports/profile setup, guarded recovery/retry execution and disabled
  ordinary Content dispatch remain execution boundaries.
- Startup publication selection remains operator-owned; dynamic promotion is deferred in
  [#50](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/issues/50).
