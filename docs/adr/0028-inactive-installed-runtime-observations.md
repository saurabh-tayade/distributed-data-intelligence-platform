# ADR 0028: Inactive installed runtime observations

Date: November 5, 2026

Status: **Inactive read-only implementation; native execution and recovery remain proposed.**

## Context

[ADR 0027](0027-inactive-native-operation-correspondence.md) establishes registered configuration
correspondence. It deliberately does not establish that the registered local files still exist
or match those configurations. Calling the historical runtime loaders would mix observation
with delivery journals, credentials and launch-oriented objects.

## Read-only boundary

Add a separate package-private installed observer. Its only production entry reconstructs
original READY from PostgreSQL and the strict ledger, then obtains the existing independent
configuration snapshot. Filesystem inspection happens after those metadata transactions end.
After inspection, repeat the bounded filesystem observations and independently reconstruct
READY and the registered chain again. Reject changes instead of adopting replacements.

Inspect only registered JAR/module bytes, declared executable availability, canonical directories,
registered local root identities, the held installation guard and existing free-space floors.
Retain the historical Java-home/current-JVM identity checks. Never run an interpreter, execute a
version probe, import a module, read Bronze credentials or contact an endpoint. Ordinary Python
executable symlinks remain supported, with resolved regular-file/executable observations and
separation from storage roots. Gold and Content v2 use Silver's interpreter; Gold's distinct
registered interpreter remains a separate availability observation, without requiring equality.

Return detached immutable observations, without filesystem paths or credential configuration
in the installed observation records. The privately held correspondence remains its existing
immutable snapshot. No command, owner, session, sink or continuation is returned. Matching bytes
do not establish Python/PySpark/boto3/DuckDB versions, Java subprocess behavior, native payload
validity, or strict-sink compatibility. `nativeLaunchCompatible`, `runtimeVersionsVerified` and
`strictSinkCompatible` remain false. The existing runtime-v2/ledger-v1 distinction is unchanged.

## Alternatives and consequences

Reusing launch-oriented loaders would broaden the boundary and access credentials unnecessarily.
Treating matching artifact hashes as deployment qualification would overstate what those hashes
prove. Updating old registrations or requiring both Python paths to be identical would change
historical semantics. None of those alternatives is selected.

Two bounded observations plus final registration/READY checks detect changes during inspection,
but are not an atomic filesystem snapshot or permission to act later. The result is historical
observation, not ownership or recovery disposition. No new persistent report is written.

Existing limits remain 128 classpath entries, 64 MiB per JAR, 256 MiB total classpath, 1 MiB per
fixed module, and 1 GiB available on CONTROL/SILVER/TEMP/GOLD/CONTENT roots. REFERENCE and STAGING
receive no new free-space requirement. The original 64 MiB / 221-entry evidence plan, migrations
V1–V11, V11's consumed one-shot probe, historical readers and dispatch gates are unchanged.

The [verification record](../operations/narrow-installed-runtime-observations.md) describes the
coverage and remaining gaps. #49 remains open and Gate B0 remains incomplete.
