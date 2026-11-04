# Narrow managed Content profile foundation

November 4, 2026. **Gate B0 remains incomplete. Ordinary Content dispatch stays disabled.**
This is an independently testable implementation slice of the owner-approved direction in
[ADR 0022](../adr/0022-narrow-managed-content-profile.md), not a runnable managed profile.
It builds on the [bounded producer candidate](content-evidence-producers.md) without changing
its pinned Python modules or admitting legacy publications into a new profile automatically.

## Implemented boundaries

`processing/narrow_content_profile.py` adds read-only validators under policy
`ddip-content-narrow-v1`. They measure complete production canonical bytes and reject without
truncation or modification. They do not independently validate Parquet semantics, Bronze
provenance, native publication correctness or authority to execute.

| Envelope | Maximum |
| --- | ---: |
| Silver completion bytes, including original whitespace | 4 KiB |
| Complete local capture, including base64 completion and directory names | 32 KiB |
| Inventory identity value | 16 KiB |
| Complete Silver verification | 52 KiB |
| Complete Silver qualification, including selection and verification | 60 KiB |
| Complete native selection, including manifest and fingerprint | 36 KiB |
| Each complete public Content report row | 60 KiB |

These component allocations are conservative implementation candidates within the approved
narrow direction, not a claim that every otherwise valid native publication fits. Actual
complete bytes govern rejection. Silver capture checks at most four analytical files in
each of `accepted` and `rejected`, 24 regular files, four directories including the root,
and six ancestry guards. Namespace names and file/directory inventories must agree.
Identity is recomputed using the unchanged production qualification encoder.

The Python grammar is integer-only, with at most 16 nested containers, scalar depth 16,
20-character signed integer tokens and valid Unicode scalars. Field names are limited to
50,000 UTF-16 code units independently of values. Values use the existing small/large
reader limits of 65,536/131,072 UTF-16 units. Canonical input rejects duplicate fields,
BOMs, invalid UTF-8, noncanonical bytes and trailing data. These are conservative guards
for the actual readers, not a replacement parser contract for standalone publications.
The signed-token limit is deliberately stricter than Jackson's digit-only number limit;
the Java helper applies the same conservative check as Python.

`NarrowContentProfile` measures Java canonical bytes and reparses them using the unchanged
`SourceJson` reader. This matters because reading up to 512 KiB from disk does not enlarge
the subsequent default 64 KiB parser used by `RssSilverExecution`. The Python Silver
verification parser itself remains 512 KiB. Tests also exercise Jackson's independent
field-name limit, depth, number tokens, escaping and supplementary Unicode characters.

`validate_report_row` uses the complete native `REPORTS` projection and `public` conversion,
including numeric strings, nulls, arrays and unused-reference coverage. It must eventually
run for every projected row before native completion. A 60 KiB row bound means two rows
for a limit-one query fit within the existing 128 KiB raw-text preflight. It does not prove
that default limit-ten or limit-100 pages fit, nor that all nested evidence envelopes fit.

## Fixed reservation grammar

`ContentOperationPlan` defines deterministic checkpoint identities from policy, run,
attempt, stage and ordinal. The source identifier is separately bound in the ledger and
Fetch path. Silver's verification directory retains its publisher checkpoint identity.
The complete map is immutable; unknown stages and ordinals are rejected.

The original plan contains:

* One Fetch, one Bronze store and five Bronze inspections.
* One each of Silver publish, Silver verify and Silver recheck.
* One each of Gold prepare, Gold publish and Content publish.
* Four input checks and two result verifications.

This includes the proposed core plus one additional inspection pair: 19 operation
directories. Using the actual `ContentEvidenceBudget` slot and inventory serializers,
with a maximum-length 128-character source identifier, it reserves **67,108,864 bytes
(64 MiB), 221 entries**, and less than 100 KiB of conservative inventory. The separate
recovery plan has nine result-verification directories, **60,686,336 bytes (57.875 MiB)
and 122 entries**. Both enforce a 128 KiB inventory ceiling in addition to the unchanged
aggregate limits. No operation directory is created by reserving the plan.

The additive version-2 ledger records policy, plan kind and source identifier. Its exact
file/directory grammar is recomputed on every read, not accepted merely because totals
fit. It is atomically installed only over an initialized empty legacy ledger. Existing
exact slots can be consumed; no new file, changed capacity or even empty directory can
extend it. Reopening the same plan is read-only and idempotent. A conflicting plan,
partial or missing ledger, or grammar-invalid modification is retained and refused, not
repaired or relabelled.

This file schema alone does **not** provide downgrade protection. The unchanged legacy
reader still accepts a structurally valid version-1 ledger. If a version-2 file were rewritten
into that shape, the ledger reader by itself could not know the required policy. A
characterization test records this open gate. Future runtime admission and recovery must
require the versioned policy from independently frozen/sealed evidence, rather than trusting
the ledger to declare its own authority. In-memory markers are not a restart-safe substitute.

The pure continuation predicate permits another effectful attempt only with fewer than
19 existing attempts, preserving one slot under the existing 20-attempt maximum. It is
not yet called by SQL admission. Likewise, runtime stages do not yet use the deterministic
plan allocator. These predicates are not substitutes for atomic durable enforcement.

## Validation and reproduction

Focused Java tests run through the ordinary content-api unit-test job:

```bash
mvn -f services/content-api/pom.xml \
  -Dtest=NarrowContentProfileTest,ContentOperationPlanTest,ContentEvidenceBudgetTest test
```

Focused Python tests are discovered by the existing processing job, with its pinned
Python 3.12.15 and dependencies:

```bash
PYTHONPATH=processing python -m unittest discover \
  -s processing/tests -p test_narrow_content_profile.py -v
```

The Silver tests execute production `silver_import_qualification.capture` and `identity`
on synthetic local namespaces. They execute unchanged Spark-independent `silver_inputs`
inventory function bodies extracted by AST, avoiding a Spark import. Those files are
deliberately not semantically valid Parquet, so this is serializer/layout evidence, not
native Spark qualification. Exact/plus-one tests cover each byte cap, file/directory
counts, full projections, arrays, reference-only rows, private diagnostics and read-only
hash/mtime preservation. Tests vary component caps around actual serializer output as
well as testing configured numeric caps directly.

The committed `narrow-content-profile-native-vectors.json` test resource records synthetic
Python capture/identity/selection and public-report serializer outputs. Java roundtrips
their exact canonical bytes through the actual existing reader and new envelope guards.
Its device/inode/time strings are inert captured test values, not filesystem authorization.
This fixture is compatibility evidence, not a maximum-output or restart proof.

Ledger tests cover exact totals, planned writes, unknown files and empty directories,
reopening without ledger rewrite, retained interrupted prefixes, root/source mismatch,
tampered policy/capacity/directory grammar, legacy retrofit refusal and fixed recovery
capacity. Reacquiring a host guard is not a real PostgreSQL or JVM restart test.

Local validation used Java 21.0.13 and Python 3.12.14 with DuckDB 1.4.5. Pinned Python
3.12.15, full Spark publication, Docker/PostgreSQL restart and the complete native integration
suite were not run for this foundation. CI and later slices must supply that evidence.
The local backend suite passed 423 tests, the new Python profile suite passed 15 tests,
and 45 CI classification/gate tests passed. The existing bounded-evidence Python suite
was also attempted: ten tests passed and one could not run its error-classification path
because PySpark was unavailable. That is not counted as a full regression pass.

## Gates still closed

Before any supported profile or ordinary dispatch can be claimed:

1. Add explicit versioned producer hooks before Silver, Gold and Content completion writes,
   including existing-publication read-only validation. Never check only after native effects.
2. Bind policy and complete operation reservations to frozen runtime identities and durable
   admission, including restart-safe downgrade rejection. Supply a versioned child sink for
   the new ledger; the existing sink rejects it.
3. Route every original, inspection, recovery and resume stage to its exact bounded slot.
   Prove the reachable operation grammar and atomic continuation/recovery capacity admission.
4. Measure complete Gold/Content commands and proof wrappers, recovery/resume results,
   reference qualification, inherited trees, inventories and observation packets against all
   byte, depth, string and entry readers. The provisional 296 KiB packet model is not proof.
5. Exercise maximum native outputs and each first rejected boundary, interruption before/after
   effects, real PostgreSQL restart, original/inherited recovery and all historical regressions.

No accepted historical contract, API error vocabulary, automatic publication selection,
ordinary dispatch, operator control, database schema or runtime hash is changed by this slice.

The subsequent [inactive strict expectation reader](narrow-content-strict-reader.md) provides
an explicit read-only API requiring independently supplied policy, plan, identities and ledger
hash. It rejects downgrade and absent-marker shortcuts for that API without changing legacy
paths. Authoritative persistence and production integration remain deferred.
