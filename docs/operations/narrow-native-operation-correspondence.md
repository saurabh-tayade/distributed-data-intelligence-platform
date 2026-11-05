# Inactive native operation correspondence

November 5, 2026. Additive work for
[issue #49](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/issues/49),
with the boundary recorded in [ADR 0027](../adr/0027-inactive-native-operation-correspondence.md).
**Content dispatch remains disabled. These checks do not launch or authorize native execution.**

## Complete original slot catalog

`NarrowContentOperationRouting` assigns the existing counts to distinct semantic roles in the
historical call order. This is an inactive correspondence catalog, not an accepted new native
routing/continuation policy. All source indices and historical SQL stage item ordinals below
are zero for the one-source profile. Auxiliary and inspection operations create no SQL row.

| Ledger slot | Native family/mode | Historical SQL relationship | Native checkpoint origin |
| --- | --- | --- | --- |
| FETCH_SOURCE/0 | FETCH, no mode field | FETCH_SOURCE/0 | Own invocation |
| STORE_BRONZE/0 | BRONZE/PUBLISH | STORE_BRONZE/0 | Own invocation |
| INSPECT_BRONZE/0 | BRONZE/INSPECT | Before SILVER/0 | Own invocation |
| SILVER/0 | SILVER/PUBLISH | SILVER/0 | Own invocation |
| VERIFY_SILVER/0 | SILVER/VERIFY | Auxiliary of SILVER/0 | Silver publisher |
| RECHECK_SILVER/0 | SILVER/RECHECK | Auxiliary of SILVER/0 | Silver publisher |
| INSPECT_BRONZE/1 | BRONZE/INSPECT | Before PREPARE_GOLD/0 | Own invocation |
| PREPARE_GOLD/0 | GOLD/PREPARE | PREPARE_GOLD/0 | Own invocation |
| INSPECT_BRONZE/2 | BRONZE/INSPECT | Before PUBLISH_GOLD/0 | Own invocation |
| PUBLISH_GOLD/0 | GOLD/PUBLISH | PUBLISH_GOLD/0 | Own invocation |
| VERIFY_RESULT/0 | GOLD/VERIFY | Auxiliary of PUBLISH_GOLD/0 | Gold publisher |
| CHECK_INPUTS/0 | GOLD/RECHECK | Auxiliary of PUBLISH_GOLD/0 | Gold publisher |
| INSPECT_BRONZE/3 | BRONZE/INSPECT | Before Content CHECK_INPUTS/0 | Own invocation |
| CHECK_INPUTS/1 | CONTENT/PREPARE | CHECK_INPUTS/0 | Own invocation |
| PUBLISH_CONTENT/0 | CONTENT/PUBLISH | PUBLISH_CONTENT/0 | Own invocation |
| VERIFY_RESULT/1 | CONTENT/VERIFY | VERIFY_RESULT/0 | Own invocation |
| CHECK_INPUTS/2 | CONTENT/RECHECK | Auxiliary of VERIFY_RESULT/0 | Content verifier |
| INSPECT_BRONZE/4 | BRONZE/INSPECT | After success, Content inspection | Own invocation |
| CHECK_INPUTS/3 | CONTENT/VERIFY | After success, examines VERIFY_RESULT/0 | Independent inspection invocation |

The original plan aliases only VERIFY_SILVER's directory UUID to SILVER's. RECHECK_SILVER,
Gold VERIFY_RESULT/0 and CHECK_INPUTS/0, and Content CHECK_INPUTS/2 have directory UUIDs that
are different from their native command checkpoint origins. The historical final Content
inspection separately allocates its directory and command UUIDs. The inspector requires
independently supplied exact native checkpoints; it does not derive them from a command,
assume they equal ledger UUIDs, allocate replacements or persist new mappings.

Sources: `RssFetchExecution.execute`, `RssBronzeExecution.execute/inspect`,
`RssSilverExecution.execute/context/committed/recheck`, `RssGoldExecution.prepare/publish/context`,
and `RssContentExecution.execute/context/inspect`. The fixed counts and single extra pair are
retained from the [original producer reservation proof](content-evidence-producers.md).
Silver inspection would need another VERIFY_SILVER slot; Gold inspection would need additional
Bronze inspections and a VERIFY_RESULT slot. Neither is covered by the extra Content pair.

The V11 READ_ONLY_PROBE consumes ledger CHECK_INPUTS/0 on its own attempt permanently. The native
catalog entry for that coordinate does not change the probe meaning or allow that attempt to
continue. No consumer of this catalog calls V11 or changes its schema. Initial frozen-selection
checks remain SQL-only and do not consume another CHECK_INPUTS directory.

## Registered runtime and command correspondence

`NarrowContentRuntimeQualification` reads the frozen run and separately registered CONTENT →
GOLD → SILVER → BRONZE → FETCH records. It compares canonical configuration bytes and hashes,
exact version-2 schemas, all parent-child links, profile/deployment identities, storage roles,
installation/product memberships and the frozen source's Bronze binding. Unknown fields,
missing registrations, mixed versions, changed READY identities and mismatched links fail closed.
The guard overload reconstructs actual READY facts first. The explicit Expected overload is a
correspondence seam whose caller must independently establish those facts.

The returned snapshot owns its canonical bytes and exposes defensive copies. It includes no
Owner, Session, launch argv or writable sink. `ledgerVersion()` remains **1** and
`nativeLaunchCompatible()` remains **false**. This is a registered configuration check, not
installed-artifact or complete frozen-source/input-registration validation. It does not read
credentials, hash installed JARs/modules, execute a version probe, contact storage or establish
that a registered runtime is installed or usable.

The command inspector accepts only the exact historical protocol-v1 top-level shape for the
selected native family. It binds run/attempt, exact epoch and checkpoint, registered runtime
hash, fixed mode, native source ordinal zero, and relevant frozen source/runtime/root binding
fields. Gold/Content preparation and verification fields must have the mode's expected null or
object shape. It checks the existing slot byte ceiling and canonical JSON without writing it.

A matching result says **identity correspondence only**. It does not verify receipts, Bronze
objects, Silver qualification, Gold proofs, prepared manifests, completion records or artifact
linkage. Native payload verification remains mandatory in a future qualified adapter. Explicit
projection protocol-v2 and probe packets fail rather than downgrade. No result authorizes
execution, installation adoption, inspection after success, repeated work or recovery.

## Verification and remaining gates

Focused tests cover all 19 routes, all 19 command-family headers, the eight historical SQL
stages, five checkpoint reuse relationships, five Bronze inspection roles, the extra Content
pair, wrong run/attempt/epoch/checkpoint/runtime, fixed directory/native UUID swaps, canonical
JSON and slot ceilings, mixed/projection versions, registered-chain changes, missing membership,
unknown fields and defensive copies. Synthetic fixtures deliberately use nonexistent deployment
paths and do not claim native evidence. The production additions contain no SQL mutation,
filesystem write, process execution, journal consumption or dispatch call.

```bash
JAVA_HOME=/path/to/jdk-21 mvn -f services/content-api/pom.xml \
  -Dtest=NarrowContentOperationRoutingTest,NarrowContentCommandCorrespondenceTest,NarrowContentRuntimeQualificationTest test
```

Final local validation used Java 21.0.13, Maven 3.9.11 and the existing Python 3.12.14 / PySpark
4.0.2 test environment for unchanged native transport parity, not deployment-pinned Python
3.12.15. The full content-api unit run passed **703 cases: 700 executed, three existing optional
corpus cases skipped**, with zero failures/errors. It includes **112 new cases**: 25 catalog,
44 command correspondence and 43 registered-runtime checks. Independent review found no remaining
blocking findings after the command-byte snapshot, binding-product comparison and retained
selection reader ceiling corrections. The 47 CI policy tests, 28-suite PostgreSQL inventory,
Markdown/local links, repository hygiene and whitespace checks passed.

No PostgreSQL integration or new native execution ran locally. Required hosted CI remains separate
acceptance evidence. These unit checks do not replace real PostgreSQL READY reconstruction,
pinned-runtime verification, native
writer/precompletion rejection, post-rename verification, complete recovery ancestry or a
recoverable Content lifecycle. No operational registration or activation setting is introduced.

Future work still needs a separately qualified native runtime/strict-sink contract, durable
native operation reservation and bootstrap, explicit directory/native checkpoint routing with
compatible readers, independent payload verification, continuation/recovery capacity and full
interruption/restart acceptance. Existing budgets, migrations V1–V11, probe behavior, historical
registrations and readers are unchanged. Gate B0 remains incomplete and #49 remains open.
