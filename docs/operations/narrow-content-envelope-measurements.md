# Narrow Content envelope measurements

November 4, 2026. Measurement-only continuation of
[issue #49](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/issues/49),
from the [inactive native guards](narrow-native-precompletion.md) at
`564590ef4f0c66088e17af0694f7858412a18eed` (draft PR #100).
**Gate B0 remains incomplete; ordinary Content dispatch remains disabled.**
No production module, cap, contract, runtime selection, ledger allocation or accepted ADR changes.

## Finding and its limits

The existing native-consumer limits do not establish a complete 1 MiB managed evidence envelope.
A direct, consumer-valid empty Gold fixture with 902 real Parquet files produced a **1,397,310-byte**
verification-shaped envelope. The same file-count stress on Content produced an actual
`managed_content.verify` result of **1,418,286 bytes**. Both exceed the 1,048,576-byte candidate
stage slot while remaining acceptable to the existing Java 16 MiB reader in these cases.

This is a concrete serializer/consumer counterexample, not an operational admission result or
a claim that Spark produces these filenames. It does not prove a maximum, a first rejected native
boundary, or a smaller supported allocation. The fixtures are deliberately below the native
4,096-files-per-dataset and 8,192-files-total limits. Do not derive a production cap from this sample.

## Evidence classes

* **Small native consumer fixture:** the existing DuckDB-authored Gold/Content test helpers,
  with four Gold articles and four Content catalog rows, ordinary filenames, and a real published
  reference snapshot. Existing full native metadata/schema/relational readers verify the files.
* **Empty file-count/escaping stress:** 451 actual empty Parquet files in each of two datasets.
  Each basename has 200 double quotes followed by a four-digit index and `.parquet`, totaling
  212 UTF-8 bytes. Native path validation accepts these names. Their canonical JSON escaping
  and repetition in completion, capture and directory inventories are measured, not estimated.
  Cases stress Gold alone, Content alone, and both together. Empty consumer validity does not
  establish a reachable Bronze/Silver/Spark production history.
* **Gold prepared/verification serialization:** the harness selects and evaluates the exact
  production constructor expressions in `managed_gold.spark_action`, records source SHA-256
  and line range, and supplies measured metadata/capture. It does **not** execute that enclosing
  Spark function, `gold_publication.verify_publication`, native Silver qualification or shutdown.
  The `verifiers` strings retained by the constructor are a shape measurement, not proof those
  verifiers ran. Content preparation similarly uses its exact production constructor expression.
* **Content verification/reference:** actual `managed_content.verify`, native Content opener,
  all three standard smoke reports, and `artifact_import_verifier.capture`/`qualify` execute.
  These calls do not execute the top-level managed runtime gate or qualify a managed command.
* **Java command constructors:** the test invokes actual private Gold/Content command methods
  with inert reflected contexts. No runtime load/check, database access, child launch, stage write
  or publication call occurs. Gold/Content artifact, committed-proof and recovery/RESUME outer
  objects are explicitly labeled inline-constructor shape mirrors, not executed database flows.
* **Nested command inputs:** the existing synthetic Silver serializer vector supplies realistic
  nesting and representative field widths. It is unrelated to the direct Gold fixture's input
  provenance. Generated UUIDs, digests, runtime configuration and command context are synthetic.
  These JSON files must never be submitted as executable commands or registered evidence.
* **Synthetic boundaries:** separate always-run Java tests exercise exact and plus-one aggregate
  byte lengths, nesting depth and evidence-string length. They are reader tests, not native
  publications or claims about accepted maximum workloads.

The existing fixture helper preserves native-accepted legacy `TIMESTAMP_MICROS` physical
annotations when writing DuckDB fixtures. No production schema or timestamp validator is changed.
Every native case records a full before/after hash, file identity, mode and modification-time
comparison; readers leave all generated native bytes and namespace entries unchanged.

## Measured native components

Values below are canonical UTF-8 bytes from one local run. Filesystem device/inode/stamp widths
can vary across machines, so exact capture totals are observations, not golden acceptance constants.

| Component | Small nonempty | Gold 902 | Content 902 | Both 902 |
| --- | ---: | ---: | ---: | ---: |
| Gold completion | 824 | 482,222 | 822 | 482,222 |
| Gold capture | 1,265 | 913,667 | 1,261 | 913,667 |
| Gold prepared | 1,191 | 1,191 | 1,191 | 1,191 |
| Gold verification shape | 3,510 | 1,397,310 | 3,504 | 1,397,310 |
| Content prepared | 1,000 | 1,003 | 1,000 | 1,003 |
| Content completion | 661 | 661 | 491,961 | 491,961 |
| Content capture | 1,738 | 1,744 | 924,494 | 924,500 |
| Content verification | 4,230 | 4,239 | 1,418,286 | 1,418,295 |
| Reference qualification | 2,472 | 2,472 | 2,472 | 2,472 |

The Gold stress verification exceeds a 1 MiB slot by 348,734 bytes; the isolated Content stress
exceeds it by 369,710 bytes. Gold completion and capture each fitting separately does not imply
their combined verification, artifact or downstream command fits. Content verification similarly
contains both its completion and capture, then appears again inside artifact and recheck wrappers.

## Complete nested envelopes

The following complete objects include the original Gold command, synthetic retained Silver input,
real reference qualification and actual binding strings. Paths differ by case and affect a few
bytes. Artifact/proof and recovery/RESUME outer objects remain shape mirrors as labeled above.

| Complete object | Small nonempty | Gold 902 | Content 902 | Both 902 |
| --- | ---: | ---: | ---: | ---: |
| Gold PUBLISH command | 8,854 | 8,878 | 8,887 | 8,878 |
| Gold RECHECK command | 12,360 | 1,406,184 | 12,387 | 1,406,184 |
| Gold artifact | 4,034 | 1,397,834 | 4,028 | 1,397,834 |
| Gold committed proof, including original PUBLISH | 13,048 | 1,406,872 | 13,075 | 1,406,872 |
| Content PUBLISH command | 17,910 | 1,411,761 | 17,970 | 1,411,761 |
| Content RECHECK command | 22,136 | 1,415,996 | 1,436,252 | 2,830,052 |
| Content artifact | 4,946 | 4,955 | 1,419,002 | 1,419,011 |
| Gold recovery command | 8,880 | 8,904 | 8,913 | 8,904 |
| Content recovery command | 17,939 | 1,411,790 | 17,999 | 1,411,790 |
| CONTENT_INPUTS RESUME command | 21,185 | 1,415,052 | 21,267 | 1,415,052 |

Recovery models the existing branch's original PUBLISH command changed to VERIFY with
`verified=null` before the `{kind,original}` wrapper. It does not incorrectly charge a final
Content verification into that actual-path command. A separate artificial wrapped RECHECK stress
is retained in raw output and explicitly labeled; its combined 2,830,082 bytes are not a claim
about a reachable recovery request. Recovery result evidence remains the native verification;
complete observations, private capture/ancestry and resume result qualification remain open work.

Across all four cases, all **84 measured objects** passed the actual large `SourceJson` reader
and roundtripped identically; the production Python serializer independently reproduced every
byte and structural measurement. Observed maximum value depth and container nesting were both
13, and the longest value was 596 UTF-16 units (in the synthetic Silver proof). This is sample
compatibility, not a bound on all possible nested histories, strings, field names or references.

## Readers and allocations are separate

| Boundary | Existing behavior | What this measurement establishes |
| --- | --- | --- |
| `ContentEvidenceBudget.slots` | Gold/Content command, evidence and applicable selection slots are 1 MiB; small result/child/verified slots are 64 KiB | Compare actual envelope size to the candidate allocation; no production write or claim admission occurs |
| `PreflightFiles.LARGE` / `SourceJson.parse(bytes, LARGE)` | 16 MiB aggregate byte ceiling | Each measured emitted Java/Python envelope is actually parsed and canonical-roundtripped |
| `SourceJson` evidence parser | Depth 16, value strings 131,072 UTF-16 units, number length 20; field-name constraints also apply | Record actual depth/container nesting/string/key widths; synthetic exact/plus-one tests are independent of byte limits |
| `SourceJson.parse(bytes)` | 64 KiB aggregate and small string reader | Not substituted for the large command/evidence reader; synthetic aggregate boundary exercised separately |
| Python Gold/Content/recovery/resume bridges | 16 MiB command/evidence checks; Python canonical JSON | Production canonical bytes are measured; bridge execution and runtime compatibility are not claimed |
| Native Content metadata | 16 MiB Gold manifest, 8 MiB completion, bounded files/paths/native bytes | Direct consumers accept this generated subset, not every permitted native output |
| Recovery packet/observation and inherited ancestry | Additional envelopes, capture inventories and independent readers | Not closed by measuring only native commands and proofs |

Code locations: [fixed slots](../../services/content-api/src/main/java/platform/contentapi/execution/ContentEvidenceBudget.java),
[large reads](../../services/content-api/src/main/java/platform/contentapi/execution/PreflightFiles.java),
[Java JSON](../../services/content-api/src/main/java/platform/contentapi/management/SourceJson.java),
[Gold commands/proofs](../../services/content-api/src/main/java/platform/contentapi/execution/RssGoldExecution.java),
[Content commands/proofs](../../services/content-api/src/main/java/platform/contentapi/execution/RssContentExecution.java),
[recovery wrappers](../../services/content-api/src/main/java/platform/contentapi/execution/ContentRecovery.java),
[resume wrappers](../../services/content-api/src/main/java/platform/contentapi/execution/ContentResumeEvidence.java).

## Reproduce without changing production

Use the supported test dependencies, JDK 21 and Maven. All generated fixtures and JSON remain in
a new ignored local output directory. The Python harness refuses an existing run directory; the
Java harness refuses an existing output directory/report. Failed or partial runs remain available
for inspection. Choose a new output path rather than deleting or overwriting earlier evidence.

```bash
python scripts/measure-content-envelopes.py --output outputs/envelope-run
for case in small_nonempty gold_902_empty_escaped content_902_empty_escaped both_902_empty_escaped; do
  mvn -f services/content-api/pom.xml -Dtest=ContentEnvelopeMeasurementTest \
    -Denvelope.measurement.directory="$PWD/outputs/envelope-run/$case" test
done
python scripts/measure-content-envelopes.py --output outputs/envelope-run --verify-java
python -m unittest discover -s scripts/tests -p test_measure_content_envelopes.py -v
```

Each case retains `fixture.json`, canonical input `envelopes/`, `python-measurements.json`,
`java-envelopes/` and `java-measurements.json`. The final cross-language pass recomputes hashes,
lengths and structural measurements and compares actual Java-emitted bytes with production Python
`canonical_bytes`. Only synthetic data is generated; no credentials or connected services are read.

Without the system property, the optional generated-fixture Java test is explicitly skipped;
the independent synthetic boundary tests still execute. Such a default test run is not native
fixture evidence. The explicit four-case runs must be reported separately.

The seven Python harness guard tests passed. Four explicit Java case runs each passed all four
tests with zero failures, errors or skips (three synthetic tests plus the optional generated-fixture
measurement). The separate default Java run passed its three synthetic tests and skipped the
optional native fixture. Markdown/local links, repository hygiene and whitespace checks passed.
No production regression suite was rerun for these additive measurement-only files.

Local Python was 3.12.14 with DuckDB 1.4.5 and JDK 21.0.13. PySpark/Boto3 and the supported
Python 3.12.15 managed runtime were not executed. No version gate was weakened. The measurement
therefore supplements, and does not replace, pinned native CI or operational integration tests.

## Remaining Gate B0 work

1. Determine producer-reachable Gold/Content metadata bounds and all first rejected boundaries,
   including real Spark output names, sidecars, directories, maximum rows and resource cost.
2. Establish complete allocations across commands, proofs, reference snapshots, observation packets,
   inherited evidence, resume results and original/recovery lifetime plans using actual readers.
3. Integrate a separately reviewed versioned child/ledger path with authoritative admission,
   interruption handling and fixed operation routing. This measurement adds no such capability.
4. Exercise real restart, recovery, inherited continuation, one-recovery-attempt headroom and all
   historical regressions before considering dispatch activation.

A native consumer accepting a fixture is not a durable READY receipt, a reservation, a restart
proof, or evidence that ordinary Content dispatch is safe.
