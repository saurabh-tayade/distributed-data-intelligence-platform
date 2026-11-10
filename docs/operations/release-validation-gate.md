# Required release verification gate

Before creating a release or its tag, the operator must verify full regression on the exact
candidate commit using the existing main-push CI workflow. A selective PR pass is insufficient.
This bounded procedure supplies a required **operator-enforced** gate for
[issue #54](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/issues/54)
without introducing release automation or new infrastructure.

GitHub does **not** automatically prevent an operator from bypassing this procedure. There is no
release/tag-triggered workflow or automated release publisher. If automatic prevention becomes a
requirement, it needs a separate explicit design and validation; do not describe this procedure as
that control. No release is authorized or published merely by documenting or executing these checks.

## Candidate and evidence

1. Choose and record the full 40-character candidate commit SHA already merged into `main`.
   Resolve an existing annotated tag to its commit, not its tag-object SHA. Do not use a mutable
   branch name or assume the most recent green run covers the candidate.
2. Locate the repository's `.github/workflows/ci.yml` run whose event is `push`, branch is `main`
   and `head_sha` exactly equals the candidate. Record its run URL, run ID and attempt number.
   PR runs, runs from another workflow/repository and runs on a different SHA do not qualify.
3. Require that run/attempt to be completed with conclusion `success`. Read every page of its
   job results, including reusable-workflow jobs, and verify the classifier, each selected test
   suite/partition, Python and PostgreSQL partition fan-ins, final backend gate, documentation
   and frontend completed successfully.
4. Confirm the classifier's full plan and all five logical backend selections are true. Verify
   the expected jobs against the workflow and partition inventories at the candidate SHA, not
   a hard-coded current job count. A missing job, skipped suite, incomplete/failed/cancelled job,
   missing evidence or a classification discrepancy blocks release.
5. If GitHub reports multiple attempts, retain attempt-specific evidence. A partial rerun must
   not be mistaken for a complete successful full regression. If the attempt's complete required
   coverage cannot be established, run the full workflow again on the **same candidate commit**
   using GitHub's rerun-all-jobs control and wait for its result. Do not silently change the SHA.
6. Recheck the candidate SHA immediately before creating the tag/release. Retain the evidence
   below with the release checklist. Any code change creates a different candidate and requires
   matching full regression evidence.

The full-main policy and fail-closed aggregate behavior are documented in the
[CI guide](ci.md#required-checks-and-aggregate-semantics). Main pushes already request full
regression, including documentation-only changes. No new workflow trigger is needed for this
procedure, and no test is skipped to reduce release waiting time.

## Read-only evidence collection example

These commands collect evidence; they **do not validate it automatically** or create a release.
Use the GitHub UI or connected GitHub tooling for the same reads when `gh` is unavailable.
Substitute the verified run ID and exact attempt number. Keep captured evidence outside Git.

```sh
REPO=saurabh-tayade/distributed-data-intelligence-platform
RUN_ID=REPLACE_WITH_RUN_ID
ATTEMPT=REPLACE_WITH_ATTEMPT_NUMBER

gh api "repos/$REPO/actions/runs/$RUN_ID/attempts/$ATTEMPT" > run.json
gh api --paginate --slurp \
  "repos/$REPO/actions/runs/$RUN_ID/attempts/$ATTEMPT/jobs?per_page=100" > jobs-pages.json
```

Inspect the classifier's logs/summary for the full selections as well as the job conclusions.
If logs, attempt data or the candidate's workflow/inventory cannot be retrieved, the gate is
not verified. Waiting, missing evidence and unavailable infrastructure are not passing results.

## Release checklist record

- Candidate SHA and intended tag/version:
- CI workflow path, run URL/ID and attempt:
- Exact `push` / `main` / matching-head verification:
- Classifier full-plan evidence and expected inventory at candidate SHA:
- All required jobs and aggregates passed; complete paginated evidence:
- Evidence collection timestamp (UTC) and verifier:
- Separate demo/browser acceptance evidence and known limitations:
- Candidate/tag target rechecked immediately before publication:

## Boundary with product acceptance

A passing CI gate is necessary but not sufficient for a fully accepted release. It does not prove
clean Docker-managed startup/restart/teardown or the owner's manual browser review under
[issue #53](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/issues/53).
Use the [local demo checklist](local-operations-demo.md#owner-browser-acceptance-checklist) for that
separate evidence. Native integration tests must not be relabeled as the complete Docker demo.

This procedure also does not enable Content dispatch, satisfy the deferred real-public-feed
acceptance in [issue #47](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/issues/47),
or establish completion of [issue #49](https://github.com/saurabh-tayade/distributed-data-intelligence-platform/issues/49).
Keep those limitations explicit in release notes; do not imply that a green CI run removes them.
