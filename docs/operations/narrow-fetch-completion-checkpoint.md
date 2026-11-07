# Inactive native FETCH completion checkpoint

The package-private `NarrowContentFetchCompletion` seam implements
[ADR 0033](../adr/0033-inactive-fetch-completion-checkpoint.md). It is invoked only by tests and
an inactive same-invocation launcher seam. No API, operator command, production dispatch or
restart recovery route is installed.

## Recorded boundary

- Original live owner, current versions, controls/deadline, immutable READY and six launch phases.
- Fresh independent semantic verification of the exact original raw HTTP candidate, including
  command, descriptor, payload, metadata, result, candidate digest and original file stamps.
- Immutable SQL intent for exact native marker bytes before its reserved filesystem write.
- Forced prefix, atomic marker publication, independent reopen and additive SQL checkpoint linkage.
- Exact read-only acknowledgment confirmation after uncertain final commit, with no new authority.

A native FETCH checkpoint means a verified raw HTTP journal was durably linked by the original
owner. It does not mean valid XML, a legacy VERIFIED stage, a Bronze delivery, a completed
publication, a successful run or permission for another effect. Attempt/run state, global occupancy,
launch history and existing legacy entry-point fencing remain unchanged.

`complete.json` retains the existing native format. The 64 MiB / 221-entry filesystem envelope is
unchanged. V14 stores at most one intent and one checkpoint per operation, with fixed byte ceilings.

## Failure and inspection

Any persisted intent consumes normal completion, even with no marker or only a partial prefix.
A new invocation never completes that intent. Marker-only output, inconsistent receipt data,
stale ownership, changed controls, expired deadline and uncertain writers are retained without
cleanup, repair, refetch or release. A later recovery implementation will require separate explicit
authority and quiescence evidence.

Historical inspection is SQL-only and detached. It can confirm persisted correspondence after
restart, without installed runtime files or an attempt directory. It cannot certify that the
current filesystem still exists or is unchanged, and it cannot grant recovery or continuation.

## Verification

Local production compilation uses the JDK 21 compiler module with the existing pinned dependency
artifacts. Maven and Docker are unavailable in the implementation environment. Hosted CI is
required for Maven, PostgreSQL, native-child loopback, real launcher death and database restart
execution. Local compilation and static checks are not a substitute for that CI result.

The completion and recovery suites belong to the `native_fetch_recovery` PostgreSQL partition; the
fail-closed test inventory must account for both suites. Authored coverage includes success,
duplicate/concurrent invocations, raw non-XML payloads, candidate and marker mutation, controls,
stale/foreign ownership, lost commit acknowledgments, crashes around each persistence boundary,
fresh-JVM history inspection and a disposable PostgreSQL restart. CI must pass for the exact
submitted commit before merge.

Issue #49 and Gate B0 remain incomplete. Subsequent native stages, recovery, ordinary Content
dispatch and full end-to-end Content execution remain separate work.
