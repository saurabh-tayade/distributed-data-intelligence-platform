# Inactive guarded native STORE_BRONZE launch

[ADR 0040](../adr/0040-inactive-guarded-bronze-launch.md) connects original-live-owner native
reservation to the strict Bronze child, ending at candidate inventory. It is a package-private
integration boundary, not an operator command or normal dispatcher route.

## Retained boundaries

- Separate installed launch profile; historical Bronze runtime-v2 remains unchanged.
- Original uninterrupted owner chain, with no historical reservation adoption.
- Fresh reserved Bronze directories and no-replace command publication.
- Ordered durable phases, child process identity and checked stdin-pipe delivery.
- Exact completed FETCH dependency and credential rechecks at effect boundaries.
- Dedicated combined evidence reader without extra filesystem allowance.
- Structurally validated candidate inventory only, including explicit failure-state output.

A candidate, child exit, receipt-shaped evidence or EXACT claim does not prove remote correctness,
pipeline success, quiescence or permission to repeat. Interrupted or uncertain work stays occupied.
Do not remove retained prefixes, reuse the slot, synthesize legacy history or rerun the child to
verify storage. Independent remote verification and receipt commit remain subsequent work.

Standard output must be empty; standard error is limited to silence or the exact existing 210-byte
SLF4J no-provider warning, checked by length and SHA-256. Raw child diagnostics are discarded.
Other launchers retain their existing output policy.

## Verification requirements

Exercise actual strict-child loopback storage, including existing EXACT pairs with zero PUT and
ABSENT publication, plus failure states. Cover installed-byte replacement, dependency and credential
mutation, owner/control/deadline changes at actual delivery, phase gaps and duplicate writes, SQL
acknowledgment loss, repeated launch, active prefixes, malformed candidates and bounded cleanup.
Direct SQL tests must reject forged phase state and recovery overlap. Test refreshed live-owner
continuation without granting historical continuation.

Local and exact-head hosted results will be recorded with the implementation PR. Authored tests
are not passing evidence until executed. Ordinary Content dispatch, independent Bronze verification,
receipt commit, subsequent stages and end-to-end recovery remain incomplete.

## Local verification

The 52 focused runtime, candidate/output and supervisor unit tests passed. PostgreSQL 17.6 ran
200 new and regression cases: 199 passed initially; the remaining fault-injection test was corrected
to lose acknowledgment on the actual phase INSERT, then it and four related cases passed on rerun.
This covers 94 new launch/SQL cases and 106 existing reservation, ownership and upgrade regressions.
The real strict child exercised new two-object publication, preexisting EXACT with no PUT, partial
and conflicting objects without repair, unavailable storage, and uncertain phase acknowledgments.

All 50 CI-policy tests and repository whitespace checks passed. The full local unit attempt had
835 passing tests, three existing skips and one native-transport failure because local Python lacks
pyspark. This is not a full-suite pass. Local Java compilation used a command-line source/target 21
workaround for missing ct.sym; repository build settings were unchanged. Standard hosted exact-head
CI must provide the full supported-environment validation before merge.
