# Inactive native Silver reservation

[ADR 0042](../adr/0042-inactive-native-silver-reservation.md) adds a one-shot SILVER/0 reservation
through the acknowledged original-live-owner Bronze checkpoint path. It has no operator command,
process launch or normal dispatcher route.

## Retained boundaries

- Only the private current-owner callback after acknowledged Bronze checkpoint can reserve.
- A historical checkpoint or lost-acknowledgment confirmation grants no continuation.
- Exact original FETCH, Bronze receipt/anchor, candidate pins and frozen runtime/storage bindings.
- Explicit non-executable reservation schema, not a historical Silver runtime-v2 packet.
- Separate INSPECT_BRONZE/0 remains required before future SILVER/0 execution.
- No filesystem writes, object-store requests, new allowance or legacy execution history.
- Owner/control/deadline fencing and immutable single-use SQL evidence.

Do not infer that this reservation proves current remote storage integrity, completed inspection or
permission to launch Python/Spark. Do not repeat an uncertain reservation, remove retained evidence
or adopt the attempt through historical facts. The run and occupied slot remain unresolved until
subsequent separately implemented stages and disposition.

## Verification requirements

Cover exact original-owner success, distinct operation/checkpoint identities, duplicate reservations,
callback reuse and lost checkpoint/reservation acknowledgments. Reject changed candidate/anchor/FETCH
or credentials, occupied target directories, wrong frozen runtime/storage, stale owner versions and
changed controls/deadline. Direct SQL tests must reject forged correspondence, owner fences and
mutation/deletion. Assert no additional feed/storage requests, files, legacy stages or budget entries.

Execution evidence belongs in the implementation PR. Authored cases are not passing results until
executed. Ordinary Content dispatch, Silver execution and end-to-end recovery remain incomplete.

## Local verification

The Silver Java suite passed 48 cases. The SQL suite initially passed 45 of 46; its deadline case
failed during fixture setup because the attempt guard correctly rejected clock mutation. The isolated
fixture was corrected to restore all triggers before reservation. A 15-case rerun passed the corrected
deadline, all related owner/control variants and SQL success/immutability, covering all 94 new cases.

Nine Bronze compatibility cases and 122 persistence, source and populated-upgrade regressions passed.
The final unit run passed 851 cases with three existing skips, excluding the separately attempted
native parity test that requires PySpark unavailable locally. All 50 CI-policy tests and the 48-suite
integration inventory passed. Independent review passed 27 final-source integration cases, 22 targeted
SQL cases, three token units and four lifecycle cases; it reviewed the deadline-fixture correction.

Local Java uses a command-line-only source/target 21 workaround for missing ct.sym. No repository
build setting changed. These limitations require standard hosted exact-head full CI before merge.
Markdown, repository hygiene and whitespace checks passed.
