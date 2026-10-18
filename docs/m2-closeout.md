# M2 closeout

**M2 — Gold Analytics & Data Products is complete against its local acceptance criteria.** The six retained M1 snapshots passed validated Silver selection, Spark Gold publication and full read-back, unchanged replay, and standalone DuckDB reconciliation on October 17, 2026 (America/New_York; October 18 UTC). This closeout adds documentation and local evidence only. GitHub publishing and closure of [M2.6 issue #25](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/issues/25) remain with the repository owner; that issue was open when checked.

[M6 — Consumer APIs & React Dashboard is the next execution priority](charter.md#milestones-m0m9), ahead of M5 streaming. Milestone numbers are unchanged. M2 completion does not complete the full platform MVP or establish production scalability.

## Delivered and merged work

GitHub read-only checks confirmed these PRs merged and their issues closed as completed. Local HEAD during validation was `2be0c93ff04c777b1b4b98d7b282ffa26055b48c`, the M2.5 merge; work stayed on `chore/m2-6-end-to-end-closeout`.

| Slice | Delivered capability | Merged PR | Completed issue |
| --- | --- | --- | --- |
| M2.1 | Accepted Gold v1 grains, schemas, metrics and publication design | [#16](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/pull/16) | [#15](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/issues/15) |
| M2.2 | Completed-Silver discovery, schema/provenance validation, replay collapse and canonical input fingerprint | [#18](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/pull/18) | [#17](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/issues/17) |
| M2.3 | Deterministic Spark article catalog and source/UTC-day aggregates | [#20](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/pull/20) | [#19](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/issues/19) |
| M2.4 | Immutable Parquet publication, full read-back, input integrity recheck, completion-last metadata and verified replay | [#22](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/pull/22) | [#21](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/issues/21) |
| M2.5 | Standalone publication verification and four bounded, parameterized DuckDB reports | [#24](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/pull/24) | [#23](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/issues/23) |

[ADR 0004](adr/0004-gold-analytics-publication.md) remains Accepted and unchanged by closeout. Its implementation note reflects the earlier M2.5 checkpoint; current completion evidence is recorded here. No new analytical contract or architectural decision is introduced.

## Retained input selection and actual Silver results

The [reproduction guide](architecture/m2-retained-validation.md) provides the exact opt-in procedure, assertions, CLI replay and troubleshooting. Validation read existing local Silver at `outputs/bronze-to-silver`, with bucket digest `2100f4104505229cc5f0838e401903a6ded3484106f7a286793755886ff7da1b`. Only the following exact markers were selected; all use UTC ingestion date `2026-10-13` and the existing v1 namespace. No live publisher requests, Bronze reads/writes or Silver normalization were performed. Original Bronze integrity remains the [earlier M1 evidence](m1-closeout.md#verified-local-evidence).

| Source | Original ingestion UUID | Ingested at (UTC, 2026-10-13) | Accepted | Rejected | Warnings |
| --- | --- | --- | --- | --- | --- |
| guardian-world | `0a171a1a-6149-4693-b5f2-c15282dd6b21` | `23:54:13.051439` | 45 | 0 | 0 |
| guardian-world | `9574943a-da7c-441f-8e1b-b94dfd348868` | `23:54:12.595541` | 45 | 0 | 0 |
| nasa-technology | `1251f3be-2d17-4147-b09d-dd3c96c795de` | `23:54:12.965161` | 10 | 0 | 0 |
| nasa-technology | `f11cfda7-3047-468e-9255-b0d632b5d5ce` | `23:54:12.294026` | 10 | 0 | 0 |
| xkcd | `78e1a5a5-1734-4f8d-b342-37fbc2ae645c` | `23:54:12.846422` | 4 | 0 | 0 |
| xkcd | `bf801dba-84d7-43bf-8314-1d3d969c5288` | `23:54:13.098211` | 4 | 0 | 0 |

`load_inputs()` passed marker, physical schema, provenance and replay-conflict validation before Gold processing. Actual totals were **118 accepted observations, 59 distinct article identities, six ingestion UUIDs, zero rejected observations and zero warning codes**. Accepted counts before/after replay collapse were both 118. Duplicate selections, duplicate publications and duplicate accepted replay rows were all zero. All 66 files beneath the retained Silver root, including bookkeeping files, retained their lengths, SHA-256 digests and modification times.

## Gold publication and reconciliation

The existing `publish(ValidatedInputs, output_root)` workflow returned **PROCESSED**. The exact completed publication is:

```text
outputs/gold/v1/processor_revision=1/input_set=195999b03c48ca9a5828f5a67c1e00c312efe6f52e0916c180188f8bb9bdf185/
```

Publication version, both Gold contract versions and processor revision are all 1. The canonical `inputs.json` SHA-256 and input-set fingerprint are both `195999b03c48ca9a5828f5a67c1e00c312efe6f52e0916c180188f8bb9bdf185`. The completion marker SHA-256 is `74447fba041b71bff1cb2df3f294abce84256954921681c3859c8a28f23b1958`.

| Source | Silver observations | Silver distinct articles / Gold article rows | Gold article observation total | Gold daily observation total | Daily distinct ingestions | Daily warning codes |
| --- | --- | --- | --- | --- | --- | --- |
| guardian-world | 90 | 45 | 90 | 90 | 2 | 0 |
| nasa-technology | 20 | 10 | 20 | 20 | 2 | 0 |
| xkcd | 8 | 4 | 8 | 8 | 2 | 0 |
| Total for this selection | 118 | 59 | 118 | 118 | 6 across these disjoint source groups | 0 |

`gold_articles` has **59 rows** and `gold_source_daily` has **three rows**, one per source on `2026-10-13`. Every article has two observations across two ingestion IDs. These observations remain distinct; no repeated article observation was lost. Global ingestion cardinality was checked from Silver, not inferred by summing article-level distinct counts.

Both exact Spark schemas matched Gold v1, including field order/types. The publication workflow passed physical-file schemas, required values, primary-key uniqueness, full row-multiset comparison in both directions, selected-observation provenance, first/last timestamps, distinct counts, article/daily observation reconciliation and warning-code reconciliation. `verify_unchanged()` ran after processing actions and before completion. The standalone reader additionally validated completion structure, manifest identity and output hashes.

| Dataset | Parquet files | File bytes | Recorded file SHA-256 |
| --- | --- | --- | --- |
| gold_articles | 1 | 54,397 | `628696de57bce831497f17cde677f3182d64588fd7d1ed37cef8c290aa68e35b` |
| gold_source_daily | 1 | 2,773 | `1997d9481088eb4a281dfa9fbc8415d74e55ac259f39caa37bd7316915165d4f` |

These physical output digests describe this publication only; independently written Spark Parquet may have different filenames/bytes. The local evidence retains the full inventory. No publisher payload, article text or dataset is added to Git.

## Replay and DuckDB evidence

A fresh `run()` call reloaded the identical six markers and returned **SKIPPED** with the same fingerprint, counts and diagnostics. Inventories of all **10 Gold files**, including `inputs.json`, `complete.json`, Parquet and Spark bookkeeping, matched before/after replay by relative path, length, SHA-256 and nanosecond modification time. An additional invocation of the documented Gold CLI also verified replay. A final comparison after CLI replay and all queries confirmed the same 10 Gold files and 66 Silver files remained unchanged. No existing publication was overwritten or deleted.

After the producer process stopped, a separate DuckDB process ran all four existing CLI entry points. Java was set to `/nonexistent`; Spark/producer/storage-SDK imports, Python Silver opens, subprocess creation and network connections were blocked. All checks passed. Direct CLI execution of each report subsequently returned identical JSON. The Python access guard is not an OS sandbox; the consumer's native Parquet reads use the exact verified Gold inventory. Original Silver remained in place and unchanged. This complements the synthetic consumer test that removes its own temporary Silver fixture after Spark shutdown.

| DuckDB report | Actual result |
| --- | --- |
| `publishers` | 3 rows: Guardian 45 articles/90 observations; NASA 10/20; xkcd 4/8 |
| `daily` | 3 rows on `2026-10-13` UTC; observations 90/20/8; distinct articles 45/10/4; distinct ingestions 2 each; zero warnings |
| `articles --limit 100` | 59 unique article rows; 118 summed observations; two observations/two ingestions each; selected timestamp equals last observed timestamp; deterministic timestamp/ID ordering |
| `quality` | 3 rows; observations 90/20/8; zero warning codes and `0.0` warning codes per accepted observation for every source |

All four xkcd catalog rows have null `published_at`; Guardian and NASA have none missing. Null dates remain null and do not create warnings. The quality denominator is accepted observations, not articles or warning-bearing rows. No global ingestion metric, engagement, popularity or category is invented. Both views use the same explicit publication fingerprint and cross-table checks pass before reports are exposed.

## Local regression and commands executed

Environment: macOS ARM64, Python 3.12.15, Java 21.0.7, PySpark 4.0.2 `local[2]`, DuckDB 1.4.5, Node.js 24.19.0; Spark and DuckDB sessions use UTC. The integration/replay evidence completed at `2026-10-18T03:05:39.256641+00:00`. Timing below describes a test run, not a benchmark.

The [guide](architecture/m2-retained-validation.md) includes the exact contents of the two ignored evidence drivers. Executed commands, with the guide's environment exported:

```sh
"$PYSPARK_PYTHON" outputs/m2-closeout/validate.py
JAVA_HOME=/nonexistent "$PYSPARK_PYTHON" outputs/m2-closeout/query-check.py
# Also ran the guide's explicit-marker gold_publication.py CLI replay.
"$PYSPARK_PYTHON" -m unittest discover -s processing/tests -v
"$PYSPARK_PYTHON" processing/gold_analytics.py --help
# PUBLICATION is the fingerprint returned by publication.json, as in the guide.
for report in publishers daily articles quality; do
  JAVA_HOME=/nonexistent "$PYSPARK_PYTHON" processing/gold_analytics.py \
    --gold-root outputs/gold --publication "$PUBLICATION" --report "$report" --limit 100
done
npm run check
git diff --check
git check-ignore outputs/m2-closeout/publication.json outputs/gold
```

Logs and evidence JSON are retained locally under ignored `outputs/m2-closeout/`: exact selection, Silver aggregates, initial publication, replay, before/after inventories, integration summary, standalone assertions, four reports and test logs. The commands were redirected to local log files where appropriate. Those files are operational evidence, not a prerequisite for hosted CI or a tracked dataset.

| Check | Actual result |
| --- | --- |
| Full processing suite | PASSED: **78 tests in 110.989 seconds**, no failures/errors/skips |
| Suite composition | 13 Silver, 8 Bronze integration, 19 Silver-input validation, 5 Gold transformation, 19 Gold publication, 14 DuckDB analytics tests |
| Retained-data publication/read-back/replay | PASSED, separately from synthetic tests |
| Standalone retained-data DuckDB | PASSED: all four reports and aggregate/ordering assertions |
| CLI replay, four direct reports and analytics help | PASSED |
| Guide snippets | PASSED: Bash/Python syntax checked; both embedded evidence drivers match the executed files |
| `npm run check` | PASSED: Markdown formatting, local links and repository hygiene |
| `git diff --check` and output ignore checks | PASSED |
| Local Java/shell regressions | Not rerun: no Java, shell or production integration code changed; hosted baseline jobs verified below |

Existing deliberate-corruption tests emitted expected Spark checksum diagnostics. Native-Hadoop fallback and Python socket ResourceWarnings were observed; the suite passed. No new test or CI dependency on retained local data was introduced.

## Hosted CI verification

The GitHub API confirmed [CI run 39090685129](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/actions/runs/39090685129) succeeded for M2.5 head `806edfb8b6dda88d56c47cfc2d7ddeabbd96829f` (PR #24). All three jobs completed successfully: documentation/repository hygiene, Java 21 ingestion/Bronze with shell argument regressions, and Python 3.12/Spark/DuckDB. The [processing job log](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/actions/runs/39090685129/job/117566585670) independently records **78 tests in 254.638 seconds, OK** at `2026-10-18T02:40:57Z`.

This is verified hosted evidence for the merged implementation baseline, not a hosted run of these uncommitted M2.6 documentation changes. No new workflow run was triggered. The unchanged [CI configuration](../.github/workflows/ci.yml) installs pinned Spark/Boto3/DuckDB dependencies with all requirements files in the cache key; its tests use synthetic data and local doubles. Package bootstrap requires network access, while test execution requires no publishers, retained data, SeaweedFS, cloud credentials or database service. Hosted CI does not reproduce the local six-snapshot integration.

## Limitations and next work

Mandatory retained-data checks passed; no integration blocker remains. M2 can be closed by the owner after reviewing and publishing this record. Issue/PR state and branch publishing were not changed during validation.

Gold and Silver remain local Parquet. Publication assumes immutable inputs and one writer; there is no transactional snapshot, multiwriter isolation, exactly-once guarantee, fsync durability, automatic retention or recovery. Integrity checks cannot detect edits reverted between checks, and hashes verify consistency with recorded metadata rather than authenticity against a trusted external authority. Gold replay still requires Silver; DuckDB queries require only the completed Gold publication and DuckDB runtime.

Spark windows, groupings and full-row verification incur scans/shuffles, while inventory hashing and DuckDB validation scan complete files. Driver inventories and bounded local query results are not evidence of cluster scalability. No distributed, load, capacity, crash-recovery or production benchmark was run; no AWS resources or cloud compatibility claims are introduced.

M6 should next expose the accepted Gold metrics through consumer APIs and a React dashboard, preserving explicit publication selection, bounded queries, source/date grains and safe handling of untrusted publisher metadata. API contracts, sanitization, pagination and UI design remain future work. M5 streaming, scheduling, broker selection, object-storage analytical persistence, Iceberg, production operations and resource benchmarks remain deferred. Existing milestone numbers and accepted ADRs are preserved.
