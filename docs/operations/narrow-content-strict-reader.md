# Inactive narrow Content expectation reader

November 4, 2026. **No production admission, runtime, publisher, claim or dispatch path
uses this reader. Gate B0 remains incomplete.** This follows the
[narrow profile foundation](narrow-content-profile.md) without changing its ledger format
or selecting a durable receipt format or admission lifecycle.

## Explicit expectations, not authority learned from disk

`NarrowContentEvidenceContext` is a package-private, read-only input contract. Construction
requires independently supplied expectations for:

* The exact `ddip-content-narrow-v1` policy, run UUID and attempt UUID.
* `ORIGINAL` with a valid source identifier, or `RECOVERY` with no source identifier.
* The complete frozen CONTROL host binding, including installation and root/file identities.
* The attempt-root device/inode and the existing ledger lock's device/inode.
* Opaque selection, profile and runtime SHA-256 identities.
* The exact canonical ledger SHA-256.

The identity strings must be lowercase 64-character hexadecimal values. Every inspection
also receives an independently supplied current selection/profile/runtime identity tuple,
which must equal the expected tuple in every component. The context and diagnostics are
redacted; the result is ordinary byte/entry totals, not an execution or admission token.

**This is an equality contract, not provenance verification.** The current ledger contains
no selection/profile/runtime identities. The reader does not reconstruct those identities,
prove their relationship to the source or host, or establish where either supplied tuple
came from. Supplying matching values copied from untrusted input would not establish
authority. Neither successful construction nor inspection authenticates persisted evidence.
Authoritative source reconstruction, durable binding, receipt encoding and admission ordering
remain separate design and integration work. No production receipt format is introduced.

## Strict read boundary

The reader derives one exact attempt root from the expected CONTROL root, run and attempt.
The held host guard must match the complete expected binding. Every intermediate directory,
the attempt root and existing lock must satisfy the existing canonical/private-path checks;
attempt-root and lock identities must match the independently supplied values.

It never calls the legacy ancestor-marker discovery path, never depends on the process-local
`REQUIRED` set, and never initializes, reserves, creates, repairs or adopts a ledger. Missing
root, lock or manifest is a failure, even when a fresh JVM has no memory of the attempt.

Under the existing cooperative ledger lock, it reads a bounded canonical byte array and
validates and hashes that same array. The ledger must be version 2 with the exact expected
policy, run, attempt, plan kind and source. The complete operation grammar is reconstructed
from the expected plan, rather than trusting an internally coherent alternate plan found
on disk. The existing byte, entry, depth, inventory, file permission and tree checks apply.
Stage sidecar contents are not authenticated by this ledger hash; their independent
content seals, semantics and provenance remain obligations of later qualification.

The host, ancestor, attempt-root and lock checks run before opening the lock, after opening
it, after lock acquisition and after the tree walk. Manifest bytes, device/inode and mtime
are compared across inspection. These checks retain the existing cooperative-writer model;
they are not filesystem isolation against an arbitrary same-user process performing hostile
path replacement. The host guard and ledger lock do not establish durable run ownership.

The new strict reader refuses an existing `.content-evidence.json.preparing` prefix without
changing or deleting it. This is a conservative condition for this read-only operation, not
a decision about a future PREPARING/READY protocol or recovery state transition. The legacy
inspection path still permits bounded retained prefixes as before.

## Compatibility and scope

All legacy signatures, marker discovery, readers and writer behavior remain available and
unchanged. Their known downgrade and missing-marker behavior is not globally redefined.
The new strict path specifically rejects those cases when independently supplied expectations
require the narrow policy. It also rejects a coherently rewritten alternate source or plan,
a substituted CONTROL binding/root, a replaced lock and stale ledger hash.

No native Python module, RunProfile schema, database schema, frozen selection encoding,
durable receipt, publication completion point or dispatch capability is changed. An expected
context cannot authorize a legacy child, allocate capacity or turn the prior version-2 runtime
candidate into the narrow profile. Existing historical tests remain regression requirements.

## Validation

Focused tests can be run with the existing Java 21 content-api setup:

```bash
mvn -f services/content-api/pom.xml \
  -Dtest=NarrowContentEvidenceContextTest,NarrowContentEvidenceRestartTest,ContentOperationPlanTest,ContentEvidenceBudgetTest test
```

In-process tests cover required shapes, independently supplied opaque identities, exact ledger
hash and expected plan, prefix retention, replaced root/lock identities, ancestor permissions,
aliased markers, unplanned entries, actual slot ceilings and a released host guard.

The restart tests launch actual child JVMs with independently retained test-only expectations.
The child acquires the existing host guard and calls the strict reader without initializing
the evidence tree. They exercise clean original/recovery plans, absent root/markers, canonical
v1 downgrade, coherent alternate plans/sources, replaced identities and mismatched opaque
identity values. File bytes, mtimes and directory inventory must remain unchanged by reads
and rejections. Test-only expectation transport does not define a production receipt.

Fresh JVM coverage establishes independence from process-local marker memory. It is **not**
PostgreSQL restart, native publication interruption, durable admission or operational recovery
proof. Docker/PostgreSQL and full Spark/native qualification remain outside this slice.

Local validation used Java 21.0.13 and Maven 3.9.11: **450 backend unit tests passed**, including
nine new in-process and 18 fresh-JVM cases, with no failures or skips. The 45 CI policy tests,
Markdown/local-link lint, repository hygiene and whitespace checks also passed. No Python
module changed, and Python/native publication suites were not rerun for this Java-only slice.

## Still undecided and unwired

The authoritative source of expected policy, identity tuples, ledger hash and root identities
must be designed and approved before production callers exist. The receipt's format and any
PREPARING-to-READY ordering remain undecided. Integration must additionally address admission
transactions, attempt capacity, exact stage allocation, versioned child sinks, precompletion
native limits, complete nested evidence bounds and original/inherited recovery. This reader
alone closes none of those operational gates.
