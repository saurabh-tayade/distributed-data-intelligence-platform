# Inactive whole-publication envelope qualification

November 5, 2026. First pure construction/qualification slice after
[PR #101's measurements](narrow-content-envelope-measurements.md), for
[issue #49](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/issues/49).
The owner approved rejecting otherwise valid **managed** Gold or Content outputs before
completion when the complete projected proof/recovery records cannot fit the existing slots.
Standalone formats remain unchanged. Gold acceptance does not guarantee that later Content
publication will fit. **This slice does not yet implement that precompletion rule.**

`NarrowPublicationEnvelopes` is an additive, package-private Java helper. It builds complete
v1 objects from supplied complete projected values and qualifies their exact canonical bytes
against their existing destination and unchanged `SourceJson` reader. It has no runtime callers,
file I/O, SQL, publisher hooks, reservation writes, registration or dispatch capability.
No native or legacy Java constructor, Python module, reader, schema, limit or migration changes.
**Gate B0 remains incomplete; ordinary Content dispatch remains disabled.**

## Current complete constructors and readers

| Complete object | Existing constructor represented | Current allocation / reader |
| --- | --- | --- |
| Gold artifact | `RssGoldExecution.publish`, including input links and full native verification | `PUBLISH_GOLD/selection.json`: 1 MiB; `SourceJson` LARGE |
| Gold committed proof | `RssGoldExecution.committed`, including full artifact, digest and original PUBLISH command | No independent persisted slot; qualify its actual enclosing Content and RESUME commands |
| Gold/Content commands | Preserve every original command field; replace only mode, prepared and verified | Current Gold/Content command slots: 1 MiB; `SourceJson` LARGE |
| Gold/Content verification | Complete supplied `managed_gold` / `managed_content` value, not component lengths | `VERIFY_RESULT/evidence.json`: 1 MiB; `SourceJson` LARGE |
| Content artifact | `RssContentExecution.execute` / successful `ContentRecovery.observe`, with both dependency links and full verification | `VERIFY_RESULT/selection.json`: 1 MiB; `SourceJson` LARGE |
| Recovery command | `ContentRecovery.nativeRead`: `{kind,original}`; original uses VERIFY or PREPARE, with verified cleared | `VERIFY_RESULT/command.json`: 1 MiB; `SourceJson` LARGE |
| Recovery result | Native verification returned by `content_recovery_verifier.execute` | `VERIFY_RESULT/evidence.json`: 1 MiB; `SourceJson` LARGE |
| Prepared Gold/Content checkpoint | `managed_gold` / `managed_content` PREPARED result, inside `ContentRecovery.observe` with full receipts | `evidence.json` and observation: 1 MiB; corresponding unchanged readers |
| Gold checkpoint observation | `ContentRecovery.observe`: PAUSED state/code, full receipts and checkpoint result | `RecoveryFiles.MAXIMUM`: 1 MiB; observation reader uses the same maximum |
| Content success observation | `ContentRecovery.observe`: SUCCEEDED state/code, full receipts and full artifact | `RecoveryFiles.MAXIMUM`: 1 MiB; no receipt or artifact omission |
| RESUME request | `ContentResumeEvidence.verify`: boundary, repeated reference/bindings and complete Content PREPARE command | `VERIFY_RESULT/command.json`: 1 MiB; `SourceJson` LARGE |
| RESUME result | `content_resume_verifier.execute`: boundary, full Gold verification, new Content preparation and reference verification | `VERIFY_RESULT/evidence.json`: 1 MiB; `SourceJson` LARGE |
| Small result sidecar | Generic complete-value byte/reader guard only; not a semantic result constructor | `VERIFY_RESULT/result.json`: 64 KiB; small `SourceJson` reader |

The helper reads allocations from `ContentEvidenceBudget.slots`, `RecoveryFiles.MAXIMUM` and
`PreflightFiles.LARGE`; it does not invent per-component allowances or file-count caps.
`SourceJson.canonical` and then the actual downstream `SourceJson.parse` run on the **whole**
object. This retains depth/container limits, field-name and UTF-16 value limits, exact escaping,
integer-only JSON and canonical roundtripping. For example, the actual Jackson number limit
counts digits without the minus sign; this helper does not add a different whole-token limit.

`qualifyGoldV1` covers Gold-local command, verification, artifact, complete RECHECK, recovery
command/result and full checkpoint observation, including the PREPARED boundary. `goldProofV1` constructs the committed proof
without assigning a nonexistent independent slot. `qualifyContentV1` charges that proof inside
the complete Content PUBLISH/RECHECK/recovery commands, as well as complete verification,
artifact and successful observation, plus the complete CHECK_INPUTS PREPARED checkpoint. `qualifyResumeV1` separately requires both complete
request and result to fit. GOLD and CONTENT_INPUTS resume requests differ in whether the
Content preparation is already present; BEFORE_FETCH has no Content command or Gold result.

These helpers are **size/grammar checks**, not proof of native validity, approved commands,
recovery authority or a durable admission. Their top-level shape/correlation checks cannot
replace the existing native verifier, artifact/lineage readers, source selection, quiescence,
owner fencing or evidence stability checks. Returned trees own deep copies of supplied trees.
No successful helper call reserves space or writes any output.

## Projection boundary and next design review

The later [inactive temporary-marker projection](narrow-publication-projection.md) uses actual
`complete.tmp` capture rather than guessed future stamp widths. It is pure and does not yet
connect these checks to publication or select a new command transport.

The caller must supply complete values, including actual reference evidence, bindings, original
command, artifact identities, receipts, Gold/Content preparation and native verification.
There are no default roots, zero-filled hashes, assumed UUIDs, sample receipts, file-count
estimates or omitted fields in the production helper.

The first helper is Java-only, where the exact destination reader is available. It is not called
from the Python publisher. Before connecting a precompletion guard, the next slice must establish:

1. How complete, independently bound command/context reaches the precompletion code. Existing
   Python publisher arguments do not contain every enclosing artifact/binding/recovery field.
2. How the full prospective completion capture, including final marker/directory entries and
   post-rename filesystem stamps, is bounded without pretending a pre-rename capture is final.
   Content coverage/smoke and all repeated native metadata must be included as well.
3. How child-side byte/grammar checks agree with the unchanged actual Java reader, and how the
   final native verification is checked again before any success/checkpoint is committed.
4. How full recovery qualification packets, private inventories, inherited ancestry, operation
   lifetime reservations, failure sidecars and interruption windows are qualified. This helper
   does not close those envelopes or prove restart/recovery capacity.

Two transport options remain for review: a versioned extension carrying immutable projection
context in a **new** managed command, or a separately sealed, versioned projection descriptor
referenced by a new managed child path. Either requires exact identity/digest binding and
full requalification. Neither may alter pinned legacy commands/modules or turn sizing projections
into native evidence. No descriptor/command schema is selected or implemented here.

A later integrated rejection must occur before the completion marker becomes visible, leave
incomplete output untouched for inspection, and never repair, replay or relabel it. Current
standalone publication, prior inactive publisher guards and production dispatch are untouched.

## Verification and evidence limits

Always-run synthetic tests exercise exact and first-extra-byte destination limits; parser depth,
empty-container depth, field names, supplementary Unicode, integer grammar and invalid values;
combined wrappers that overflow despite individually fitting bodies; complete receipts in recovery
observations; the full RESUME result; no independent committed-proof cap; and copy isolation.
Synthetic projection fixtures are not native-valid commands or publications and are never executed.

An optional four-case corpus test consumes a freshly generated PR #101 measurement corpus. It
compares complete proof, command, artifact, recovery and RESUME constructor bytes against that
harness, accepts the small native consumer case, and rejects the previously measured 902-file
Gold/Content cases at the existing slots. The harness invokes actual Java command constructors;
its database-backed artifact/proof/recovery wrappers remain explicitly labeled shape mirrors.
It executes native consumers and actual Content verification, not Spark producers or full managed
runtime qualification. Fixture source/provenance restrictions remain those of the measurement doc.

```sh
python scripts/measure-content-envelopes.py --output outputs/envelope-qualification
for case in small_nonempty gold_902_empty_escaped content_902_empty_escaped both_902_empty_escaped; do
  mvn -f services/content-api/pom.xml -Dtest=ContentEnvelopeMeasurementTest \
    -Denvelope.measurement.directory="$PWD/outputs/envelope-qualification/$case" test
done
python scripts/measure-content-envelopes.py --output outputs/envelope-qualification --verify-java
mvn -f services/content-api/pom.xml -Dtest=NarrowPublicationEnvelopesTest \
  -Denvelope.qualification.directory="$PWD/outputs/envelope-qualification" test
```

Without that property, the corpus case is explicitly skipped. Always-run synthetic passes are
not a substitute for the four native corpus cases. No native Spark producer, PostgreSQL restart,
recovery/RESUME integration, dispatch activation or production-readiness result is claimed.

Local validation on JDK 21.0.13 / Python 3.12.14 with DuckDB 1.4.5:

- Focused final corpus run: 13 tests passed, no skips; independent reviewer reran the same 13.
- Fresh native consumer corpus: all four cases generated; four Java measurement runs passed
  all four tests each, with 84 Python/Java canonical byte and structural comparisons matching.
- Full content-api unit run: 534 tests, 532 passed and two optional corpus tests skipped.
  The explicit corpus runs above cover those optional cases separately; overlapping synthetic
  cases are not counted as distinct additional coverage.
- Existing measurement harness: seven Python tests passed.
- Markdown/relative links, repository hygiene and whitespace checks passed.

Initial local test launches selected a JRE without the Java 21 compiler; using the existing
JDK fixed that environment issue. A test's initial nested-array depth was corrected from 13
arrays to 12 so that the Content command fits depth 16 and the added recovery wrapper is the
first rejection. No production parser or limit was weakened to make it pass.
