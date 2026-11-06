# Inactive native FETCH recovery assessment

The package-private `NarrowContentFetchRecoveryAssessment` implements
[ADR 0034](../adr/0034-inactive-native-fetch-recovery-assessment.md). Its callers are tests only.
There is no operator command, HTTP endpoint, production worker route or automatic recovery.

## Reading the facts

- Durable facts describe one validated immutable SQL prefix, including a checkpoint only when its
  intent, command, descriptor, reference and launch/admission provenance correspond exactly.
- Current integrity is separate: no checkpoint means files are not inspected; no supplied guard
  means integrity was not requested; completed checks mean verified at inspection; missing,
  changed, unsafe or inaccessible files mean unavailable or changed.
- A completion intent stays consumed and incomplete even if a valid-looking marker exists.
- Worker and child PID/start observations and explicit launch gaps are review targets, not proof
  that all writers are gone. Every result retains manual-review requirements and zero authority.
- V11 read-only transport probes and mixed legacy stage history are incompatible with this
  native-only seam and fail closed.

The summary contains fixed enums, UUIDs, digests, bounded phase lists and at most two recorded
processes. It contains no command/bootstrap/receipt bytes, endpoint, arbitrary path, owner or token.
Returned lists are detached and immutable. The original 64 MiB / 221-entry envelope is unchanged.
Historical SQL facts remain available without installed runtime files or the original attempt tree.

## Verification

Coverage includes admission and launch prefixes, every original-owner completion crash boundary,
real owner death, fresh-JVM assessment, an actual disposable PostgreSQL restart, missing/tampered
and identity-replaced files, symlinks/hardlinks, within-call replacement, same-JVM and separate-process occupied budget locks, uncertain processes and
read-only database/filesystem invariants. The operator inspection regression isolates the narrow
history veto even when all other legacy qualification predicates match.

Local production compilation uses the JDK 21 compiler module and pinned dependencies. Maven and
Docker are unavailable in the implementation environment, so PostgreSQL, native child, fresh-JVM
and restart tests require hosted CI. The new assessment suite is part of the fail-closed
`content_recovery` inventory. Required CI must pass on the exact submitted head before merge.

This is an assessment, not recovery execution. No handoff, owner adoption, checkpoint insertion,
repair, successor, release, refetch, dispatch or activation is performed. #49 remains open.
