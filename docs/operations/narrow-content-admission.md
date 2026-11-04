# Inactive tracked narrow Content admission

November 4, 2026. **Gate B0 is incomplete. Ordinary Content dispatch remains disabled.**
This implements the tracked claimed → preparing → ready direction approved by the owner in
[ADR 0023](../adr/0023-tracked-narrow-content-admission.md). It builds on the
[narrow profile foundation](narrow-content-profile.md) and [strict reader](narrow-content-strict-reader.md).
It is not an operator-ready execution profile, recovery tool or publication path.

## Durable binding and lifecycle

Migration V10 adds `attempts.content_admission_policy`, `content_attempt_admissions` and
`content_admission_milestones`. Existing rows retain a null policy. Existing attempt state,
coordinator ownership, historical profile bytes and native contracts are unchanged.

`WorkerRepository.claimNarrow` is package-private, exact-run and explicit. It requires the
existing execution capability, a frozen version-2 Content selection with one fresh source and
one publisher reference, no retained Silver, no existing narrow history, no resumed admission
and no prior stage/delivery effects. It preserves a recovery slot using the next actual attempt
number, not a count of existing rows. Production capability and ordinary dispatch do not call it.

The claim atomically stores immutable policy enrollment and ordinary owner pointers. Its exact
canonical object contains admission version 1, policy, installation/run/attempt, original worker
and epoch, ORIGINAL plan, source, complete CONTROL binding and digest, selection digest,
execution-profile ID/digest and content-runtime ID/digest. The application independently checks
frozen profile bytes against immutable execution-profile bytes and the CONTROL registration.
The runtime digest is an identity binding only: this does not load or qualify native runtime
modules, a runtime chain or producer hashes.

`NarrowContentAdmission.prepare` performs two short owner-fenced SQL transactions separated by
bounded filesystem work:

* PREPARING contains protocol version 1 and the enrollment digest. It commits before the first
  namespace effect. There must be zero previous milestones and zero stages.
* Preparation rejects any existing attempt root, including an empty directory. It creates a
  private root and zero-length lock, installs the empty legacy ledger and then the complete
  immutable version-2 original plan with forced prefix/rename/directory steps. It forces every
  ancestor back through the CONTROL root, including pre-existing ancestors.
* The expected canonical ledger hash is reconstructed independently from the exact fixed plan
  and fresh root identity. Strict inspection must succeed before READY is written.
* READY contains protocol version 1, enrollment digest, root and lock device/inode strings,
  and the ledger SHA-256. Ownership, cancellation, deadline, enrollment and zero stages are
  rechecked before commit. Root/lock identifiers use canonical signed-long decimal strings.

All three stored JSON objects are at most 64 KiB and use existing canonical encoding. SQL
checks exact top-level field sets and stored-byte SHA-256, while Java additionally requires
canonical bytes. SQL rows are append-only, and the attempt policy marker cannot change.
The deferred enrollment foreign key prevents a marked attempt committing without enrollment.
READY cannot precede PREPARING; no marked attempt may enter RUNNING or mutate a stage before READY.

There is no new attempt-root receipt file. The existing original plan remains exactly
67,108,864 reserved bytes and 221 entries; reservations are accounting, not disk preallocation.
Preparation does not create operation directories or produce report rows.

## Interruption and restart

PREPARING is single-use. Interruption before root creation still forbids another preparation
call. Interruption after prefix creation, partial writing, forcing or rename retains the exact
partial state. No call repairs, adopts, overwrites or cleans an existing preparation.

A lost PREPARING acknowledgment permits no filesystem work. A lost READY acknowledgment may
leave committed READY evidence, but the original repository stops authorizing operations.
`inspectReady` permits a new process to reconstruct facts from immutable SQL independently of
process memory. It requires enrollment, PREPARING and READY before touching the attempt tree,
reconstructs selection/profile/runtime identities from their immutable rows, compares the
entire enrollment and validates the externally supplied host binding. It then strictly checks
root/lock identity, exact version-2 plan, canonical ledger hash and bounded tree.

Facts lookup returns an expectation value, never an Owner or Session. Missing root/markers,
v1 downgrade, coherent alternate source/plan, stale ledger digest and replaced lock/root all
fail without mutation. SQL bindings do not independently authenticate stage sidecars or native
publication semantics. The existing cooperative process/filesystem threat model still applies.

Marked histories cannot enter old stage adapters, generic mark-running, ordinary claim,
legacy recovery or RESUME, even after READY. Therefore the legacy absent-root/no-stage shortcut
cannot reinterpret an incomplete narrow preparation as clean historical evidence. The old
clean-prelaunch release is refused after PREPARING. Heartbeat and explicit interruption remain
available; interruption retains occupied recovery-required ownership. No narrow recovery
handoff, cleanup, continuation or publisher activation is implemented.

## Validation and reproduction

Java 21 and the existing content-api Maven setup are required. Focused filesystem tests run
without PostgreSQL or native publishers:

```bash
mvn -f services/content-api/pom.xml \
  -Dtest=NarrowContentPreparationTest,NarrowContentPreparationRestartTest,NarrowContentEvidenceContextTest,NarrowContentEvidenceRestartTest,ContentOperationPlanTest,ContentEvidenceBudgetTest test
```

The new preparation suites include 38 in-process and 26 actual fresh-JVM cases. They cover
20 injected creation/write/force/rename/inspection boundaries, hard child-JVM termination,
partial prefixes, unsafe ancestors, pre-existing roots of every relevant kind, read-only
preservation and replaced identities. These are process-interruption tests, not simulated
power-loss or proof of storage hardware durability.

`NarrowContentAdmissionIT` uses real PostgreSQL and is assigned to the existing `core` CI
partition. It covers atomic claim rollback, deferred enrollment, exact phases and hashes,
immutable rows, no stage execution, authority changes during preparation, concurrent preparers,
uncertain acknowledgments, durable facts in a fresh JVM, capacity boundaries and a populated
V9-to-V10 upgrade. The supported CI fixture supplies its exact disposable PostgreSQL container
for restart testing; the test verifies `pg_postmaster_start_time` changes. An explicit external
restart-signal hook is also available for a separately managed test server.

```bash
mvn -f services/content-api/pom.xml verify -Ppostgres \
  -Dit.test=platform.contentapi.management.NarrowContentAdmissionIT
```

Use only an isolated disposable test database with `DDIP_TEST_DB_URL`, `DDIP_TEST_DB_USER`
and `DDIP_TEST_DB_PASSWORD`. The existing fixture recreates its management schema. Never point
these tests at user or production data. The restart case additionally requires either
`DDIP_TEST_POSTGRES_CONTAINER_ID` for that disposable service, or the
`ddip.test.narrow-admission-restart-signal` system property with an external restart harness.
It does not silently skip the restart when neither is provided. The CI partition verifier
requires every assigned integration test report to complete without failures, errors or skips.

Local evidence so far: Java 21.0.13/Maven 3.9.11 compiled all current main and test sources,
and the six focused suites passed **118 tests**. The final full backend unit suite passed
**517 tests**, including three connection-free admission wiring checks, with no failures,
errors or skips. The 45 CI policy tests, Markdown/local-link lint, repository hygiene and
whitespace checks also passed. Local
PostgreSQL startup was blocked by the environment's socket restriction; it was not retried
through a different transport. Docker and real PostgreSQL integration/restart execution are
unavailable locally. Those tests must pass in supported CI before this slice can be accepted.
Pinned Python 3.12.15, Spark/native publication and maximum-output suites were not run; no
Python or native producer changed in this slice.

## Still closed

This establishes inactive durable preparation and facts reconstruction, not operational
execution safety. Remaining gates include native precompletion guards, complete nested
Gold/Content/recovery/resume envelope closure, exact original and inherited stage-to-slot
allocation, a versioned child sink, qualified runtime-chain identities, effectful continuation
with reserved recovery capacity, original/inherited reconciliation and end-to-end fault tests.
No automatic repair, orphan cleanup, publication switching or dispatch activation is implied.
