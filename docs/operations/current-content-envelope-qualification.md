# Current Content execution-envelope qualification

November 8, 2026. This test-only milestone measures the integrated original-owner chain from
[ADR 0043](../adr/0043-integrated-original-content-pipeline.md), following the interrupted safe-end
work in [ADR 0044](../adr/0044-interrupted-content-safe-end-contract.md).
**Gate B0 remains incomplete and ordinary Content dispatch remains disabled.**
Issue #49 remains open. Continuation (#123) and recovery reauthorization (#124) remain deferred.
No production limits, admission policy, readers, runtime identities or reservation slots change.

## Acceptance matrix and evidence classes

- **Producer-reachable transport boundary:** a synthetic loopback Atom feed is exactly 5,242,880
  UTF-8 bytes, the existing FETCH ceiling. It contains 64 distinct articles with escaped quotes,
  ampersands and supplementary Unicode, followed by XML-comment padding. The complete real
  Java/PostgreSQL/strict-Python/Spark chain must register 64 articles, verify all 17 operations,
  complete successfully and release the slot. This is the maximum payload byte boundary, not a
  maximum article-count, report-row, reference-size or native-layout workload.
- **First rejected transport byte:** the same corpus at 5,242,881 bytes must fail before any Bronze
  request, Bronze reservation, downstream operation or successful completion. Occupancy must remain
  held; another invocation cannot refetch or change retained files.
- **Synthetic composed command boundaries:** production Gold/Content PUBLISH and RECHECK builders
  accept exactly 1 MiB and reject one additional byte. Every input component fits independently;
  strings stay under their separate limits. Actual SourceJson canonical roundtrips distinguish an
  aggregate-slot failure from a grammar/depth failure. These padded proof bodies are deliberately
  synthetic and are never used as native evidence, submitted, or launched.
- **Bootstrap scaling:** increasing nested predecessor/verification bodies by over 550 KiB changes
  their bound hashes without increasing bootstrap length. It does not prove all fixed runtime or
  binding strings fit; the existing pre-FETCH bootstrap-capacity check remains authoritative.

## Actual retained-byte measurement

`NarrowContentEnvelopeTestSupport` reads the actual fixed-plan files and downstream SQL command,
checkpoint, completion and exact completed dependency-graph artifact bytes. It checks each file
against its actual reserved slot, the retained private tree against 64 MiB / 221 entries, and complete SQL objects against their existing V21
ceilings, including the 60 KiB VERIFY_SILVER checkpoint guard and the narrower registered-artifact
limits. SQL command and candidate fields also respect their fixed stage slots. These headroom figures
are destination-specific, not promises that every enclosing or semantic component will fit. JSON
observations use the real SourceJson reader and must reproduce identical canonical bytes. Native RSS metadata is the explicit exception: it retains native serializer key order, is
parsed without rewriting, and reports whether its wire bytes are canonical. Each observation includes
byte length, cap, headroom, SHA-256, depth and longest UTF-16 string/key widths. The successful test also compares complete filesystem snapshots around inspection.

The test emits one `CONTENT_ENVELOPE_MEASUREMENT` JSON line per corpus into Maven output and its
standard test report. Generated measurements are local/test artifacts, not committed publications or
new recovery authority. Identity, path and timestamp widths vary between executions, so measured
sizes are observations rather than golden maxima. Private-tree totals describe the retained endpoint,
not peak Spark memory, scratch disk, remote Bronze capacity or all native publication files.

## Reproduction

Use a disposable PostgreSQL installation, the pinned Python 3.12.15 native dependencies and a full
Java 21 JDK. Build the ingestion JAR first. Set `DDIP_TEST_PYTHON` and the normal PostgreSQL test
variables; use no real feeds or operator-retained storage.

```sh
mvn -f ingestion/pom.xml -DskipTests package
mvn -f services/content-api/pom.xml -Dtest=NarrowContentPipelineCommandsTest test
mvn -f services/content-api/pom.xml -Ppostgres -DskipUnitTests=true \
  '-Dit.test=NarrowContentPipelineIT#exactFetchPayloadLimitTraversesCurrentChainWithMeasuredCompleteEnvelopes+firstByteBeyondFetchLimitCannotReachBronzeOrSuccess' verify
```

Both integration methods remain in the existing native-publication CI partition. No discovery
exclusions, partition changes, timeouts or required gates are weakened. Run heavy native suites
sequentially in constrained local environments.

## Local qualification result

The final focused integration run passed both cases with zero failures, errors or skips in 229.710
seconds on PostgreSQL 17.11, Java 21.0.13 and the pinned Python 3.12.15 runtime. The successful case
retained 5,529,495 private bytes across 86 entries and measured 118 complete JSON objects. Its largest
measured command was the final Content recheck at 33,391 bytes; completion was 2,835 bytes. Registered
Silver, Gold, Content and reference evidence measured 9,912, 7,001, 6,808 and 2,491 bytes respectively.
Observed maximum JSON depth was 12 and longest string was 588 UTF-16 units. These are corpus-specific
observations, not universal producer bounds.

The rejected case retained 16,856 bytes across eight entries and explicitly recorded
`FAILED / FETCH_PAYLOAD_TOO_LARGE`; no payload publication, FETCH completion or Bronze effects
followed. Development exposed a test-only assumption that native RSS metadata used canonical key
order; the collector now distinguishes its preserved raw wire bytes. The earlier failed run is not
counted as passing evidence.

All 30 focused Java command tests and 66 CI-policy tests passed. Markdown/local links, repository
hygiene, whitespace and complete PostgreSQL partition inventory checks passed. Independent review
found no remaining blockers after correcting artifact coverage, exact rejection assertions and the
VERIFY_SILVER SQL ceiling. Final-head CI #217 passed all selected jobs and aggregate gates; all
eight PostgreSQL partitions ran. Native publication passed 237 cases with no failures/errors/skips
in 53m05s Maven (about 54 minutes including setup), within the unchanged 90-minute budget. The
trusted classifier skipped untouched Python processing, ingestion and JDBC jobs; main CI #216
passed those broader regressions. PR #134 merged as `91ca818`.

## Subsequent qualification and remaining gates

The [producer-pressure qualification](content-pressure-qualification.md) completed the identified
row/reference/layout and interrupted safe-end corpus using the unchanged producer. Final-head
[CI #219](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/actions/runs/42495925021)
passed all 19 jobs and 240 native-publication cases, including all three pressure cases;
[PR #135](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/pull/135)
merged as `d54fa397`. The native-publication job took about 75 minutes in Maven and 77 minutes
including setup. These are hosted results, distinct from the local observations above.

The [bounded B0 closure matrix](content-b0-closure.md) maps current producers, caps, actual readers,
refusal behavior and tests. It found no concrete uncovered internal original-chain authority
transition. Its remaining evidence-closure conditions are independent review and exact-head
verification of the exhaustive CI partition split's capacity. Historical legacy measurements or
synthetic shape mirrors do not substitute for current-chain observations, but no unspecified new
maximum-workload corpus is required by this verdict.

This finite evidence does not promise every admitted workload succeeds: bounded refusal and
retained occupancy remain supported safety outcomes. A change to limits or supported scope needs
its own reviewed decision. Ordinary-worker wiring and activation acceptance, followed by the
separate Operations/UI acceptance, remain outstanding; neither this document nor successful tests
close Gate B0 or authorize dispatch. Issues #123 and #124 remain deferred.
