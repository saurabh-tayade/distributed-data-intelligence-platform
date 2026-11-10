# D02 hosted CI evidence, November 4, 2026

Read-only review for [issue #54](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/issues/54).
The change-aware policy and five PostgreSQL partitions are deployed and have successful hosted
evidence. This record updates the earlier pending observations in the [CI guide](ci.md).
It changes no workflow, timeout, dependency, test selection, required check or application behavior.

## Measurement method and limits

The sample contains eight successful first-attempt runs from November 2–4, 2026: the latest completed
documentation-only, frontend-scoped and API-selective examples, the latest two full PR examples,
and the latest three completed full main runs available at the 18:40 UTC observation.
These are representative observations, not an exhaustive failure-rate sample or controlled benchmark.

- Workflow execution span: `run_started_at` through the latest successful job's `completed_at`.
  This includes dependency scheduling after run start; it does not measure time before run start.
- Job duration: `completed_at - started_at`. Sum every executed job, including classification,
  documentation, frontend and both aggregates; divide by 60 for unrounded runner-minutes.
  These are elapsed job-minutes, **not billed usage or a monetary cost**.
- Skipped jobs have no measured execution duration. Some API timestamps for them are inverted by one
  second; they are excluded rather than treated as negative or zero-duration executed work.
- Do not use mutable run `updated_at` as a completion timestamp. PR #89's jobs ended at
  **17:29:01 UTC**, but its run metadata was updated at **17:42:47 UTC**. Its measured execution span
  is **54m23s**, not 68m09s. Other rows differ from metadata completion by at most one second.
- All runs use `ubuntu-24.04`. Inspected classifier logs and main-after-#88 Gold/recovery logs show
  runner image **20261025.353.1**. Both of those native jobs restored the existing pip cache.
  Cache state for every job was not collected; this is not a cold-cache benchmark.
- Workloads evolved between commits, including Reliability HTTP coverage in #89. The runs share the
  five-partition layout, but are not identical-workload before/after pairs. A passing PR does not
  establish success for its merge commit or for later code.

## Successful hosted snapshots

Head and trusted-policy SHA prefixes below identify the linked immutable run. PRs execute GitHub's
event merge checkout while classifying against the event's base policy. All rows are attempt 1.

| Run | UTC date; head / trusted policy | Classification | Execution span | Sum job-minutes |
| --- | --- | --- | ---: | ---: |
| [PR #91](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/actions/runs/41887648684) | 2026-11-04; `cc5e8e3` / `24a0540` | Documentation only | 44s | 1.07 |
| [PR #86](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/actions/runs/41391698672) | 2026-11-02; `95ae4a7` / `8c6d603` | Documentation + frontend | 1m13s | 1.47 |
| [PR #87](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/actions/runs/41476976918) | 2026-11-02; `58908d5` / `c15485a` | API + frontend + documentation | 55m28s | 176.32 |
| [PR #89](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/actions/runs/41868663753) | 2026-11-04; `eb0fd52` / `cb7596a` | Shared + API + frontend + documentation | 54m23s | 182.63 |
| [PR #88](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/actions/runs/41508028334) | 2026-11-02; `1d9f7d5` / `552c9e4` | Unknown scripts + documentation | 43m56s | 179.70 |
| [Main after #88](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/actions/runs/41520347243) | 2026-11-02; `cb7596a` / `same as head` | Main push: full | 52m25s | 186.82 |
| [Main after #87](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/actions/runs/41499618847) | 2026-11-02; `552c9e4` / `same as head` | Main push: full | 55m29s | 189.13 |
| [Main after #86](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/actions/runs/41392679436) | 2026-11-02; `c15485a` / `same as head` | Main push: full | 55m40s | 176.37 |

The three main rows execute all five logical backend suites. Both full PR rows do too:
PR #88's unknown scripts conservatively select all suites; #89 includes shared infrastructure/dependency
changes. The API-selective #87 runs PostgreSQL and HTTP while skipping ingestion, Python and JDBC.
Documentation and frontend still execute on every run.

[PR #91's classifier](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/actions/runs/41887648684/job/126776321930)
classified all three changed files as documentation. Its five backend decisions are false; all backend
jobs skip and the [required gate](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/actions/runs/41887648684/job/126776400724)
checks each explicit skip. Classification took 5s, documentation 15s, frontend 41s and the gate 3s.
The frontend-scoped #86 example similarly skips all backends: classification 4s, documentation 12s,
frontend 70s and gate 2s. These are real examples of avoiding unrelated work, not estimated savings.

### Full-run job durations

Columns refer to the corresponding linked runs above. Every entry succeeded.

| Executed job | PR #89 | PR #88 | Main #88 | Main #87 | Main #86 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Classification | 5s | 8s | 15s | 29s | 6s |
| Documentation | 21s | 16s | 16s | 17s | 12s |
| Frontend | 57s | 57s | 1m04s | 1m06s | 41s |
| Ingestion | 31s | 29s | 35s | 27s | 33s |
| Python / Spark / DuckDB | 18m55s | 19m44s | 15m00s | 19m16s | 18m42s |
| JDBC | 1m21s | 1m31s | 1m03s | 1m33s | 1m04s |
| PostgreSQL core | 12m13s | 14m56s | 13m54s | 14m10s | 14m12s |
| PostgreSQL Reliability | 24m28s | 36m59s | 36m50s | 37m09s | 25m31s |
| PostgreSQL ingestion | 14m08s | 17m21s | 18m03s | 16m18s | 16m20s |
| PostgreSQL Gold | 53m55s | 41m17s | 51m50s | 54m34s | 41m44s |
| PostgreSQL Content recovery | 52m13s | 43m27s | 45m40s | 41m33s | 55m14s |
| HTTP / native | 3m24s | 2m29s | 2m13s | 2m05s | 1m55s |
| PostgreSQL fan-in | 4s | 5s | 3s | 3s | 4s |
| Required backend gate | 3s | 3s | 3s | 8s | 4s |

For API-selective #87, classification/documentation/frontend took 8s/29s/54s, HTTP 2m43s,
PostgreSQL core/Reliability/ingestion 14m10s/36m28s/16m11s, Gold 50m15s, Content recovery 54m55s,
PostgreSQL fan-in 2s and required gate 4s. Its long recovery critical path remains even though three
logical suites are safely omitted. Selectivity need not improve wall-clock when skipped jobs were
not on the critical path.

## Gold and Content recovery headroom

Across the six completed partition-bearing rows above:

| Partition | Observed job range | Largest observation | Remaining below 90m |
| --- | ---: | --- | ---: |
| Gold | 41m17s–54m34s | [Main #87, job 125463721497](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/actions/runs/41499618847/job/125463721497) | 35m26s |
| Content recovery | 41m33s–55m14s | [Main #86, job 125107325065](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/actions/runs/41392679436/job/125107325065) | 34m46s |

These are observed margins for completed runs, not guaranteed future capacity or a reason to reduce
the timeout. The earlier four-partition [90-minute cancellations](ci.md#acceptance-criteria-and-workflow-graph)
remain censored failure observations; do not assign them a completed duration or claim a controlled
speedup from splitting them.

In main-after-#88, the [Gold job](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/actions/runs/41520347243/job/125534629950)
completed all three classes: RssGoldIT 29 cases, RssContentIT 4 and ContentFinalizationIT 34.
The [Content recovery job](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/actions/runs/41520347243/job/125534629875)
completed ContentInheritedRecoveryIT 6, ContentRecoveryIT 10 and ContentResumeIT 7.
All reported zero failures/errors/skips, and the step that also checks exact XML report membership
succeeded. Recovery logs record actual PostgreSQL restarts for inherited and resumed execution.
This is concrete hosted coverage, not just a green aggregate or local static validation.

## Baseline and tradeoffs

The [October 26 baseline](ci.md#hosted-timing-evidence) is retained. Re-reading its successful jobs with
the method above gives Phase 1 parallel PR **45m13s / 67.28 job-minutes** and main
**47m45s / 70.98 job-minutes**. The previous sequential PR is **49m37s / 50.75 job-minutes** and main
**42m46s / 43.88 job-minutes**. One-second differences from the original record come from using
the final job completion instead of run metadata update time.

The recent completed main examples take **52m25s–55m40s** and **176.37–189.13 job-minutes**.
Full regression has not been shown faster or cheaper by this comparison: native integration
coverage has grown, five isolated PostgreSQL environments do more setup, and host/cache variation
is uncontrolled. The supported benefit is that scoped PRs can avoid unnecessary suites, while
independent partitions finish successfully within their unchanged ceilings. Existing pip/npm caches
remain dependency-only; no fixture/result cache or shared live native state is introduced.

## Required checks and acceptance status

Read-only inspection of active [main ruleset 25349747](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/rules/25349747)
on November 4 reconfirmed the four contexts, each with GitHub Actions integration ID 15368:

- Documentation and repository hygiene
- Java 21 ingestion and Bronze storage
- Python 3.12, local Spark processing and DuckDB analytics
- React publisher dashboard

The [dependency map and trusted-base classifier](ci.md#change-categories-and-dependency-rules) remain
conservative. Mixed paths take their union; shared/schema/dependency/workflow changes and unknown
paths select full validation. Failed collection cannot authorize skips. Both fan-ins reject missing,
failed, cancelled or unexpectedly skipped selected dependencies.

The existing policy/inventory tests passed locally on November 4: **45 tests**, including all
32,768 top-level selection/result combinations, PostgreSQL fan-in combinations, malformed diffs,
hostile PR policy, renames/deletions, mixed-path unions and missing/extra/failed/skipped report cases.
The inventory still assigns **26 PostgreSQL classes** exactly once; GoldApiIT and ReliabilityApiIT
are separately required in the HTTP profile. No new tests or CI code were needed for this evidence update.

Issue #54's baseline, dependency rules, scoped documentation, shared/mixed coverage, negative
fixtures and timing/tradeoff evidence are now documented. **Release-gate wording still needs an
owner decision:** every push to main runs full regression and the required backend context verifies
it, but `ci.yml` triggers only PRs and main pushes. There is no tag/release trigger or automated
release-publication gate. A selective PR green check alone is not full regression for a release SHA.
Do not mark that criterion fully enforced or close the issue on this record alone if it requires
automatic prevention of unvalidated releases.

At the observation cutoff, main runs
[41883771912](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/actions/runs/41883771912)
and [41887971634](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/actions/runs/41887971634)
were still running and are excluded from completed-run claims. This docs-only work does not rerun
application suites, change GitHub settings, publish a release or change the issue state.

## November 10 follow-up: release procedure

The earlier observation above remains a historical record: no automated publication gate exists.
The [required release verification procedure](release-validation-gate.md) now specifies the
exact-commit full-main-CI evidence an operator must record before a tag or release. This uses the
existing workflow and does not add release automation, change required checks, claim an automated
publication block, or establish the separate local/browser acceptance criteria.

## Reproduce the evidence

Use read-only Actions run/job endpoints; request the exact attempt when a run has been rerun:

```sh
gh api repos/saurabh-tayade/distributed-data-intelligence-platform/actions/runs/RUN_ID
gh api 'repos/saurabh-tayade/distributed-data-intelligence-platform/actions/runs/RUN_ID/attempts/1/jobs?per_page=100'
gh api repos/saurabh-tayade/distributed-data-intelligence-platform/actions/jobs/JOB_ID/logs
gh api repos/saurabh-tayade/distributed-data-intelligence-platform/rulesets/25349747
python3 -I -B -m unittest discover -s .github/ci/tests -v
python3 .github/ci/postgres_partitions.py
npm run check
```

All sampled runs contain 14 jobs, so their 100-item pages are complete. Recompute spans and sums
from successful executed jobs, keep skipped/cancelled observations separate, and retain exact
run/attempt/head references. Do not use a later rerun's jobs to explain an earlier attempt.
