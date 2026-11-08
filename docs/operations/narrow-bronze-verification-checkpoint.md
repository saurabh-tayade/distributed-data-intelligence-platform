# Inactive native Bronze verification checkpoint

[ADR 0041](../adr/0041-inactive-native-bronze-checkpoint.md) adds GET-only verification and an
original-live-owner checkpoint after guarded Bronze launch. No operator route or normal dispatcher
is installed. The private live callback cannot be recreated from historical inspection.

## Evidence boundaries

- Separate installed verifier profile and PID-bound bounded bootstrap.
- Fresh payload/metadata GETs against exact original FETCH facts; no feed refetch or PUT.
- Candidate, credentials and dependency rechecks between object reads.
- Owner/control/deadline checks during supervision and before accepting durable evidence.
- Immutable OBSERVED receipt/anchor intent before the already-reserved verified.json write.
- Fenced SQL checkpoint tied to exact original reservation, candidate and completed FETCH.
- No extra ledger allowance, legacy history, downstream authority or occupied-slot release.

An in-flight GET can finish around a concurrent cancellation, but a failed owner/control fence
cannot authorize the anchor or checkpoint. A checkpoint is a historical remote observation, not a
promise that objects cannot change later. Lost acknowledgments and partial evidence never authorize
repeat publication or a fresh process to adopt the slot. Do not remove retained prefixes or retry the
publishing child as a verifier.

## Verification requirements

Exercise real independent GET-only verification after both new publication and existing exact pairs.
Confirm unchanged feed/PUT counts, invalid or changed payload/metadata refusal, credential/FETCH/file
substitution, installed runtime replacement, wrong bootstrap/process identity and bounded diagnostics.
Cover candidate/FETCH/credential changes between object reads and owner/control/deadline changes
before delivery, during supervision and before durable writes. Cover every
phase and anchor-write interruption, duplicate attempts, lost intent/checkpoint acknowledgments,
forged direct-SQL receipts and historical inspection without launch authority.

Execution results belong in the implementation PR. Authored tests are not a passing result until
executed. Ordinary Content dispatch, Silver/Gold continuation and end-to-end recovery remain absent.

## Local verification

The complete ingestion suite passed 275 tests. Final service units ran 851 tests with zero failures
or errors and three skips, excluding one PySpark-dependent test that is unavailable locally.
This exclusion is an environment limitation, not a full supported-environment unit-suite pass.

The completion suite passed 67 cases; after a clean compile, eight final-source guard/success cases
passed, explicitly including direct null-callback rejection and both ambient-transaction guards.
Predecessor regressions passed 245 cases. SQL and populated-upgrade coverage passed 52 cases, plus
one V5 upgrade regression. The CI-policy suite passed all 50 tests. Independent review reran all
41 new verifier cases and 10 PostgreSQL fault/ownership regressions, all passing without skips.

Local Java compilation used a command-line-only source/target 21 workaround for missing ct.sym;
repository build settings remain unchanged. Standard hosted exact-head CI is required before merge.
Repository Markdown, hygiene and whitespace checks passed. None of this activates normal dispatch.
