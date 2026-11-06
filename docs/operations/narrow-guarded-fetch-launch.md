# Inactive guarded native FETCH_SOURCE launch

The package-private `NarrowContentFetchLaunch` seam implements
[ADR 0031](../adr/0031-inactive-guarded-fetch-launch.md) for synthetic literal-loopback endpoints.
It is accessible only through test bridges; no operator command or dispatcher invokes it.

## Implemented boundary

- Separate immutable launch identity with unchanged historical FETCH runtime bytes.
- Permanently consumed native reservation, ordered SQL phases and durable command preparation.
- Exact child PID/start recording and acknowledged packet digest before EOF-gated pipe delivery.
- Final writer-thread owner/control/runtime checks and bounded active-prefix-aware supervision.
- Candidate inventory only, with no completion, stage/artifact linkage or occupied-slot release.
- Facts-only history after uncertainty, process death or restart; never automatic refetch.

A successful candidate is not an independently verified native result. Do not remove its files,
reuse its reservation or infer that a subsequent pipeline stage is authorized. Failure and
uncertainty leave the attempt occupied for future explicit recovery work.

## Verification

Local production compilation uses JDK 21 and the existing pinned dependency artifacts. It is not
a Maven test pass. A standalone smoke check passed for separate launch identity, external
endpoint rejection, PID-bound packet construction, exact FETCH ancestry, active prefixes and
forbidden completion. All 47 CI-policy tests, inventory, Markdown and hygiene checks passed.
Maven and PostgreSQL are unavailable in this environment; hosted CI must
execute the unit and actual PostgreSQL tests for the exact submitted commit before merge.

Authored acceptance covers real strict-child loopback HTTP, single-use/racing reservations,
ordered immutable phases, live owner/control changes at external-effect boundaries, uncertain
phase acknowledgment, active prefixes, cancellation, installed-JAR substitution, foreign evidence,
invalid candidates, historical inspection, parent-JVM death and disposable PostgreSQL restart.
The `content_recovery` CI partition builds the ingestion JAR before these tests and requires
passing integration reports. Existing reservation tests also cover populated V12 upgrade without
backfill and separate immutable registration guards. Authored tests are not execution evidence
until the corresponding hosted run passes.

Content dispatch remains disabled. #49 and Gate B0 remain incomplete; full candidate verification,
completion, subsequent native stages and end-to-end recovery are still required.
