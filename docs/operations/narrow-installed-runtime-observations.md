# Inactive installed runtime observations

November 5, 2026. Related to
[issue #49](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/issues/49).
[ADR 0028](../adr/0028-inactive-installed-runtime-observations.md) records this read-only boundary.
**Content dispatch stays disabled. A successful observation is never launch permission.**

## What is observed

`NarrowContentInstalledRuntimeQualification.inspect` reconstructs original READY from the
immutable admission/milestone rows and actual strict ledger. It obtains the separate
[registered runtime chain](narrow-native-operation-correspondence.md), then inspects the installed
files outside metadata transactions. It performs a second bounded filesystem pass and repeats
independent READY and registration reconstruction before returning.

The installed observations include:

- Registered FETCH classpath JAR bytes and all fixed Silver/Gold/Content module hashes. Shared
  module registrations must agree. Canonical regular files with a single hard link are required.
- Canonical, mutually separate frozen local storage directories and registered device/inode
  identities, including STAGING; processing-root identity and separation are also observed.
- Both registered Java homes matching the current Java 21 JVM and the FETCH manifest's exact
  JVM version. Java executables and both declared Python executables must be regular/executable.
  No executable contents are hashed or executed. Interpreter symlinks may resolve outside storage
  roots. Version-2 Gold and Content use Silver's Python; the separate Gold Python path is not
  silently substituted or required to be identical.
- Held installation identity and private CONTROL/TEMP/STAGING directories. Existing 1 GiB
  available-space floors apply only to CONTROL/SILVER/TEMP/GOLD/CONTENT. Space is observed again
  without requiring the numeric readings to be identical.

The existing 128-entry, 64 MiB/JAR, 256 MiB/classpath and 1 MiB/module ceilings are retained on
each bounded pass. JAR reads also respect the remaining aggregate allowance. No larger evidence
budget, new file grammar, reservation or recovery disposition is selected.

The observer does not read or resolve the Bronze credential file, access storage services,
import native modules, run version probes, initialize budgets, consume journals or change any
SQL row. Installed observation records contain immutable identities, hashes and counts rather
than credential data or arbitrary paths. Snapshot diagnostics are redacted.

## Meaning and limits

`installedArtifactsVerified()` means the registered JAR and module bytes matched during the
inspection. It does not prove interpreter/package versions, native input/publication validity,
complete source/input registration, or compatibility with the READY strict sink. The existing
configuration snapshot continues to report ledger version 1 and installed artifacts unverified;
the separate installed result does not alter that snapshot's meaning.

`runtimeVersionsVerified()`, `strictSinkCompatible()` and `nativeLaunchCompatible()` are always
false. There is no operator endpoint, launch argv, ownership token, writable sink or continuation.
Two passes and final registration/READY checks detect observed drift, but cannot prevent a
change after inspection or provide an atomic filesystem view. A new observation is required
for later investigation; historical success is not permission to execute or adopt an installation.

## Verification

The real-filesystem unit fixture uses synthetic JAR/module bytes and executable stubs that must
never run. Cases cover missing/changed/oversized artifacts, symlinks/hard links, non-file paths,
shared-module disagreement, root replacement/canonical aliases, invalid local descriptors,
missing/non-executable interpreters, distinct Gold/Silver paths, the exact existing space floors,
a closed guard, immutability and the retained 256 MiB aggregate ceiling.

`NarrowContentInstalledRuntimeIT` uses actual PostgreSQL migrations, registration rows and READY
preparation with real temporary files. It covers fresh connections, complete before/after SQL
and filesystem snapshots, missing READY, registration and file/root drift, and changes at the
post-observation boundary: membership removal, module changes, READY ledger/lock replacement
and a closed guard. It is included in the comprehensive PostgreSQL profile and the core hosted
CI partition. No test launches a native publisher or imports a native module.

```bash
mvn -B -ntp -f services/content-api/pom.xml \
  -Dtest=NarrowContentInstalledRuntimeQualificationTest test
mvn -B -ntp -f services/content-api/pom.xml -Ppostgres \
  -Dit.test=NarrowContentInstalledRuntimeIT verify
```

Local validation compiled all production Java sources through the available JDK compiler module
using dependencies from an existing packaged application. A separate real-filesystem smoke check
observed the installed fixture and rejected changed module bytes. CI policy tests (47 cases),
the complete 29-suite PostgreSQL inventory, repository hygiene and whitespace checks passed.
The local Maven/dependency cache was unavailable, so this does not claim a local Maven/JUnit run.
Local PostgreSQL startup was not permitted and was not retried; real PostgreSQL execution is
required in hosted CI. Exact-head CI results are recorded with the pull request.

Native strict-sink/runtime qualification, operation reservation/bootstrap, payload verification,
continuation/recovery, complete interruption/restart acceptance and the Content lifecycle remain
separate gates. No registration or migration changes are introduced. The V11 probe and historical
readers remain unchanged. Gate B0 remains incomplete and #49 remains open.
