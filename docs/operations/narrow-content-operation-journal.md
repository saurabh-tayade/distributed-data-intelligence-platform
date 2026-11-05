# Inactive narrow operation journal and pipe probe

November 5, 2026. Additive work for
[issue #49](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/issues/49).
The owner-approved design direction is recorded in [ADR 0026](../adr/0026-narrow-operation-journal-and-bootstrap.md).
**Content dispatch remains disabled. This is not an operational publisher or recovery path.**

## What is integrated

`NarrowContentOperationJournal` connects independently reconstructed V10 READY facts, a separate
V11 durable operation reservation, parent command creation, bounded process supervision and the
Python strict sink. Only ORIGINAL CHECK_INPUTS/0 in READ_ONLY_PROBE mode is supported. The attempt
must still be CLAIMED, exact-owner fenced, uncancelled and within its existing two-hour deadline.

Reservation commits before directory/command effects. The operation ID and fixed slot are unique
and permanent. The parent then records FILES_READY and START_AUTHORIZED, launches a child waiting
on stdin, records its UUID/PID/OS start instant, and commits RELEASE_AUTHORIZED with the final
bootstrap SHA-256. Only then may the actual writer thread recheck authority and deliver the packet
and EOF. After a silent zero exit and confirmed bounded cleanup, PROBE_COMPLETED is appended.

The read-only child verifies the fixed packet, reconstructed original plan and exact command
through `StrictSink.read_command()`. It does not execute that command, inspect native dependencies,
write result/evidence sidecars, spawn descendants or establish publication success. The probe
consumes the slot even on success; it cannot be followed by the real adapter on that attempt.
Run/attempt/stage/result history and coordinator occupancy are unchanged.

The internal runtime argument names only the fixed local probe module directory and Python
executable. It is not a public configuration or a qualified Content runtime registration. Existing
runtime digests in bootstrap identify admission facts; they do not certify native producer modules.
No native runtime qualification is claimed by the read-only probe.

## Failure and ownership semantics

Every effect boundary has a current-owner observation, with no SQL transaction held across I/O.
The existing cooperative local model still has an observation-to-effect gap. A control arriving
after the final check may be too late to prevent delivery; no database-atomic cancellation veto or
hostile-writer fencing is claimed. The final authority, persistent-stop and deadline checks occur
on the actual pipe writer, not only on the earlier coordinator thread.

The packet must be nonempty, canonical and strictly smaller than 65,536 bytes. The receiver reads
at most that many bytes in total and rejects on reaching the cap. It waits no more than five
seconds for EOF; the sender also has a bounded startup write and the existing overall supervision
deadline. Empty or incomplete EOF, a valid prefix without EOF, a regular file instead of a pipe,
wrong PID/operation identities or altered private evidence all fail closed with fixed diagnostics.

An uncertain reservation/phase commit stops that repository's authorization. A fresh process can
read immutable facts but cannot adopt an owner or repeat the slot. Unknown commit, child cleanup,
parent death, empty operation directory or absence of a completed sidecar does not permit replay.
Child identity and release hash remain available in the separate journal. No partial state is
removed, repaired, promoted to native success or reinterpreted through legacy recovery.

The implementation adds no automatic attempt disposition. Explicit interruption still retains
RECOVERY_REQUIRED ownership. Until a narrow recovery protocol is implemented and qualified, these
inactive test attempts must not be created in an operational installation.

## Verification

The mandatory CI `core` PostgreSQL partition now includes `NarrowContentOperationJournalIT`.
It exercises the actual journal and fixed Python child, ordered phases, every binding, concurrent
reservation, owner/version and cancellation changes, all injected interruption boundaries,
acknowledgment loss at reservation and every durable phase, actual writer-JVM termination and
facts-only lookup from a fresh JVM, exact recorded-child exit after parent death, and populated
V10 PREPARING/READY preservation across V11 migration. The existing admission suite retains its real PostgreSQL
restart check. Schema tests reject a missing V11 table even with intact Flyway history.

Run against Java 21, Python with the repository's pinned processing requirements, and a dedicated
disposable PostgreSQL 17 service only:

```bash
mvn -f services/content-api/pom.xml \
  -Dtest=NarrowContentBootstrapTest,VerifierProcessTest test
python3 -m unittest discover -s processing/tests -p test_narrow_startup_probe.py -v
mvn -f services/content-api/pom.xml -Ppostgres \
  -Dit.test=platform.contentapi.management.NarrowContentOperationJournalIT,platform.contentapi.management.NarrowContentAdmissionIT,platform.contentapi.execution.PersistenceIT verify
```

Supply `DDIP_TEST_DB_URL`, `DDIP_TEST_DB_USER` and `DDIP_TEST_DB_PASSWORD` for the disposable test
service. The inherited admission restart test additionally requires its documented disposable
container/restart-signal fixture. Never target operational data. Optional `probe.python` selects
an absolute Python path for the probe tests; the default is `/usr/bin/python3`. `transport.python`
selects the existing full native transport test interpreter. No production activation setting is
introduced.

Local Java 21.0.13/Maven 3.9.11 compilation and the current backend unit run passed with
588 executed cases and three existing opt-in measurement cases skipped. That run used the local
Python 3.12.14/PySpark 4.0.2 test environment for existing native parity checks; it is not a claim
of the deployment-pinned Python 3.12.15 runtime. A first attempt with the default interpreter
failed the existing native parity case because PySpark was absent, then passed with the prepared
processing test environment. Ten focused Python startup-pipe tests passed. All 33 affected Java
bootstrap/supervision checks passed, including the small-pipe cleanup,
stop/deadline, sticky interruption and poisoned-bytecode cases. All 47 CI policy tests, the
28-suite PostgreSQL inventory, Markdown/local-link lint, repository hygiene and whitespace checks
passed. The complete narrow Python selection passed **157 tests** in 277 seconds with
`PYTHONPATH=processing`, matching CI. A prior broad invocation without that path setting failed one
existing subprocess import check; the correctly configured rerun passed every case.

The expanded Python regression command is:

```bash
PYTHONPATH=processing python3 -m unittest discover -s processing/tests -p 'test_narrow*' -v
```

PostgreSQL execution is unavailable locally because the environment previously denied service
socket startup; no alternate transport was attempted. The new database cases must pass supported
CI before acceptance. Database integration, hosted CI, native writer interruption and a complete
recoverable Content lifecycle must not be inferred from unit or pipe tests alone. This slice
does not satisfy Gate B0 or authorize Content dispatch.
