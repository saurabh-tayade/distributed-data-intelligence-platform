# Content producer-pressure qualification

This test-only continuation of [current envelope qualification](current-content-envelope-qualification.md)
uses the unchanged installed original-run chain and actual native publishers. Gate B0 remains
incomplete, ordinary dispatch stays disabled, and issues #123 and #124 stay deferred. No input cap,
runtime configuration, native writer, reservation, authority or accepted contract is widened.

## Corpus and acceptance boundaries

`NarrowContentPressureIT` uses disposable PostgreSQL, synthetic loopback Atom/Bronze services and the
pinned Java/Python/Spark dependencies. Every case registers a real 10,000-row publisher-reference
snapshot through the native publisher, captures it, independently qualifies it and binds that exact
snapshot into submission. The matched publisher name is 200 characters with escaping and Unicode;
unused source IDs are 128 characters. Vocabulary and remaining per-field limits are unchanged.

- Two distinct native catalog rows must each serialize to exactly 61,440 canonical UTF-8 bytes,
  including keys, timestamps, escaping, reference attributes and counters. A missing-title entry
  populates the rejected Silver dataset; native qualification must report two accepted entries and
  one rejected entry. Full-chain success requires the existing exact native verification and final
  transactional release. Inspection uses the production all-row report scan plus one real public
  page per report, rather than treating a smoke page as all rows. It verifies 10,000 coverage rows including 9,999 unused
  references. Catalog measurements have a separate test-harness bound of 100 rows.
- One 61,441-byte catalog row follows the first ten catalog results among 12 actual rows. The
  production all-row precompletion scan must return `RESOURCE_LIMIT`, with no temporary/final
  Content completion marker, Content artifact registration or run success. Retained Parquet
  inspection is explicitly unverified, read-only and used only to measure the actual rejected row
  and its ordering. Reinvocation must preserve every management row and retained file, perform no
  new feed/Bronze requests and retain global occupancy.
- A separate worker JVM runs the exact-boundary/reference-pressure corpus through final Content
  recheck, then is killed before success. All native child processes have already exited at that
  boundary. Detached assessment remains non-authorizing. Fresh exact-target test review permits only
  the existing terminal failed disposition, with target/review/snapshot bodies measured against
  8 KiB / 8 KiB / 64 KiB. Full management rows survive an actual disposable PostgreSQL restart;
  fresh-process receipt lookup remains read-only. Native/private bytes and request counts remain
  unchanged, and the failed terminal result releases occupancy without continuation.

The Python fixture tests separately verify the 10,001st reference row is rejected by the actual
reference publisher before a selectable snapshot exists. This is an input-publication boundary,
not a dispatched downstream failure. The feed size model uses production public conversion and
canonical serialization only to select padding; actual native row measurements establish the tested
row boundary. The model is never registered as evidence.

## Layout and resource interpretation

Silver reads one feed document and explodes entries. Gold and Content introduce their normal
aggregation/join/shuffle plans. The unchanged runtime uses `local[2]` and two shuffle partitions;
these settings alone do not prove a universal two-file upper bound because adaptive execution,
joins and file-read partitioning matter. Four files per Silver dataset is a profile guard, not a
claim that the unchanged producer reaches a fabricated four-plus-four layout.

The mixed accepted/rejected workload and actual Content layout observations exercise reachable
layouts. No repartition/coalesce override, custom output filename, replacement Parquet, or extra
native sidecar is injected. The existing envelope collector checks actual reserved files and current
SQL records. Final retained size and file counts are not peak Spark memory/scratch measurements or
proof of every permitted workload's maximum.

## Focused evidence

Final-source focused native runs passed all three cases with zero errors or skips: exact rows and
maximum references in 334.368 seconds, late oversized row in 181.846 seconds, and interrupted safe-end
with actual database restart in a 348.745-second rerun. These are separate focused results, not a claim of
one green aggregate run: the first aggregate reached safe-end but its external local restart
controller pointed at the parent of the PostgreSQL data directory. After correcting that ignored
controller, the affected restart case passed. An earlier development run also caught an uncleared
test storage-error fixture; no production behavior was changed for either correction.

The successful native corpus retained 424,415 private bytes across 86 entries. Actual Content output
had 12 files in three directories (27,531 bytes), including one analytical file for each dataset.
Both catalog rows measured exactly 61,440 bytes and the full report scan counted 10,000 reference
coverage rows. These observations describe this corpus, not producer maxima. Safe-end target,
review and snapshot measured 560 / 1,087 / 10,281 bytes against 8 KiB / 8 KiB / 64 KiB limits. The
restart test verified a changed PostgreSQL postmaster timestamp, byte-identical management rows and
read-only receipt lookup in a fresh JVM.

All 19 focused Python profile tests, 66 CI-policy tests, Markdown/local links, repository hygiene and
whitespace checks passed. Independent source review found no remaining blockers after fixing bounded
subprocess output/waits, descendant cleanup and immutable-row assertions. A separate bounded closure
audit found no concrete uncovered internal original-chain authority transition; ordinary packaged
worker activation remains distinct work.

## Reproduction and CI

Build ingestion first and use a full Java 21 JDK, pinned Python 3.12.15 and a disposable PostgreSQL
instance. Set the normal `DDIP_TEST_DB_*` and `DDIP_TEST_PYTHON` variables. The restart test requires
an explicit disposable `DDIP_TEST_POSTGRES_CONTAINER_ID` or the existing
`ddip.test.narrow-pipeline-restart-signal` controller; it verifies a changed postmaster timestamp.

```sh
PYTHONPATH=processing:processing/tests "$DDIP_TEST_PYTHON" -m unittest test_narrow_content_profile
mvn -f ingestion/pom.xml -DskipTests package
mvn -f services/content-api/pom.xml -Ppostgres -DskipUnitTests=true \
  -Dit.test=NarrowContentPressureIT verify
```

The pressure class ran in the exhaustive native-publication partition in final-head
[CI #219](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/actions/runs/42495925021).
All 19 jobs passed, including all 240 native-publication cases and all three pressure cases.
[PR #135](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/pull/135)
merged as `d54fa397`. Native publication took about 75 minutes in Maven and 77 minutes including
job setup, within the unchanged 90-minute budget. No cases or required gates were removed.

The preceding [main CI #218](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/actions/runs/42495790204)
also passed all 19 jobs; its 237 native-publication cases took about 81 minutes in Maven and
82 minutes for the job. These actual measurements supersede the earlier estimate based on focused
local timings. Their variability and limited headroom motivate an exhaustive partition split;
verification of the split's exact-head capacity remains pending. See [CI operations](ci.md).

The [bounded B0 closure matrix](content-b0-closure.md) records the supported original-only envelope,
readers, refusal behavior and evidence. Its remaining conditions are split-capacity verification
and independent acceptance of the matrix, not an unspecified additional pressure corpus.
Ordinary-worker integration, activation acceptance and later Operations/UI acceptance remain
separate work. This pressure result neither closes Gate B0 by itself nor completes issue #49.
