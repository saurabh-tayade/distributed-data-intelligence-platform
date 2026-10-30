"""Focused policy, real Git collection, gate truth-table and workflow wiring checks."""

from contextlib import redirect_stdout
import importlib.util
import io
import itertools
import json
import os
from pathlib import Path
import subprocess
import tempfile
import textwrap
import unittest
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[3]
WORKFLOW = (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")
POSTGRES_WORKFLOW = (ROOT / ".github/workflows/postgres-partition.yml").read_text(encoding="utf-8")
POLICY_PATH = ROOT / ".github/ci/classify.py"
SPEC = importlib.util.spec_from_file_location("ci_policy", POLICY_PATH)
POLICY = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(POLICY)
JOBS = POLICY.JOBS


def block(name):
    """Read a top-level job block; YAML/expression validity is checked by actionlint."""
    lines = WORKFLOW.splitlines()
    start = lines.index(f"  {name}:") + 1
    end = next((i for i in range(start, len(lines))
                if lines[i].startswith("  ") and not lines[i].startswith("   ")), len(lines))
    return "\n".join(lines[start:end])


def run_script(job):
    body = block(job).split("        run: |\n", 1)[1]
    return textwrap.dedent(body)


GATE_SCRIPT = run_script("processing")
GATE_CODE = GATE_SCRIPT.split("<<'PY'\n", 1)[1].rsplit("\nPY", 1)[0]
GATE_SCOPE = {"__name__": "test_gate"}
exec(compile(GATE_CODE, "ci.yml:processing", "exec"), GATE_SCOPE)
CHECK_GATE = GATE_SCOPE["check_gate"]


def needs_for(required):
    return {"changes": {"result": "success", "outputs": {
        "classified": "true", "mode": "full" if set(required) == set(JOBS) else "selective",
        **{job: "true" if job in required else "false" for job in JOBS}}},
        **{job: {"result": "success" if job in required else "skipped"} for job in JOBS}}


class ClassificationTests(unittest.TestCase):
    def assert_selection(self, records, required, count=None):
        result = POLICY.classify(records, count if count is not None else 1)
        self.assertEqual({job for job, value in result["jobs"].items() if value == "true"}, set(required))
        return result

    def test_dependency_table(self):
        cases = [
            ("README.md", ()), ("docs/operations/ci.md", ()),
            ("docs/architecture/gold-contracts.md", ()), ("docs/visuals/diagram.svg", ()),
            ("AGENTS.md", ()), (".gitignore", ()), (".markdownlint-cli2.mjs", ()),
            ("tests/.gitkeep", ()), ("frontend/README.md", ()),
            ("frontend/src/App.tsx", ()), ("frontend/src/test/fixtures.ts", ()),
            ("ingestion/src/test/java/ExampleTest.java", ("ingestion",)),
            ("ingestion/src/test/java/platform/ingestion/managed/BronzeFixture.java", ("ingestion", "content_postgres")),
            ("scripts/run-batch.sh", ("ingestion",)),
            ("scripts/test-run-batch.sh", ("ingestion",)),
            ("scripts/test-ingestion.sh", ("ingestion",)),
            ("ingestion/src/main/java/RawFeed.java", JOBS),
            ("ingestion/src/main/java/platform/ingestion/managed/ManagedRssFetch.java", JOBS),
            ("ingestion/src/main/java/platform/ingestion/managed/ManagedBronze.java", JOBS),
            ("ingestion/src/main/java/platform/ingestion/bronze/BronzeObjectVerifier.java", JOBS),
            ("services/content-api/src/test/java/platform/contentapi/management/RssFetchIT.java", ("content_postgres", "analytics_http")),
            ("scripts/test-rss-fetch-postgres.py", JOBS),
            ("ingestion/src/test/resources/feeds/rss-business.xml", JOBS),
            ("processing/silver.py", JOBS[1:]),
            ("processing/tests/test_bronze.py", JOBS[1:]),
            ("processing/tests/gold_fixtures.py", JOBS[1:]),
            ("services/api/src/main/java/JdbcReports.java", ("analytics_jdbc",)),
            ("services/api/src/test/java/JdbcReportsTest.java", ("analytics_jdbc",)),
            ("services/api/tests/generate_fixtures.py", ("analytics_jdbc", "analytics_http")),
            ("services/content-api/src/main/java/Api.java", ("content_postgres", "analytics_http")),
            ("services/content-api/src/test/resources/recovery_fixtures.py", ("content_postgres", "analytics_http")),
            ("services/content-api/src/main/resources/recovery/reliability_verification.py", ("content_postgres", "analytics_http")),
            ("services/content-api/src/main/resources/db/migration/V8__schema.sql", JOBS),
            ("config/rss-sources.json", JOBS), ("config/publisher-reference-vocabulary-v1.json", JOBS),
            ("infrastructure/local/compose.yaml", JOBS), (".env.example", JOBS),
            ("contracts/new.json", JOBS), ("frontend/schemas/api.json", JOBS),
            ("processing/schema/new.json", JOBS), ("frontend/package.json", JOBS),
            ("frontend/package-lock.json", JOBS), ("package.json", JOBS),
            ("services/api/pom.xml", JOBS), ("processing/requirements-analytics.txt", JOBS),
            (".github/workflows/ci.yml", JOBS), (".github/ci/classify.py", JOBS),
            (".github/ci/tests/test_policy.py", JOBS), ("tests/shared_fixture.json", JOBS),
            ("scripts/test-recovery-postgres.py", JOBS), ("docs/executable.py", JOBS),
            ("new-module/source.py", JOBS), ("unknown-root.txt", JOBS),
        ]
        for path, required in cases:
            for status in ("A", "M", "D"):
                with self.subTest(path=path, status=status):
                    self.assert_selection(f"{status}\0{path}\0".encode(), required)

    def test_mixed_changes_take_union(self):
        self.assert_selection(b"M\0README.md\0M\0scripts/run-batch.sh\0M\0services/api/src/main/java/Main.java\0",
                              ("ingestion", "analytics_jdbc"), 3)

    def test_rename_checks_old_and_new_names(self):
        for status in ("R100", "R50"):
            for old, new in (("processing/silver.py", "docs/silver.md"),
                             ("docs/silver.md", "processing/silver.py")):
                self.assert_selection(f"{status}\0{old}\0{new}\0".encode(), JOBS[1:])
        self.assert_selection(b"R100\0README.md\0docs/readme.md\0", ())
        self.assert_selection(b"R100\0unknown.py\0README.md\0", JOBS)

    def test_incomplete_or_unsupported_lists_validate_everything(self):
        for data in (b"", b"M\0README.md", b"M\0", b"R100\0README.md\0", b"R101\0a\0b\0",
                     b"T\0README.md\0", b"U\0README.md\0", b"C100\0a\0b\0",
                     b"M\0../README.md\0", b"M\0docs//a.md\0", b"M\0/docs/a.md\0",
                     b"M\0docs/a\nb.md\0", b"M\0docs/a\\b.md\0", b"M\0docs/\xff.md\0"):
            with self.subTest(data=data):
                self.assert_selection(data, JOBS)
        for count in (None, 0, -1, 2, "1", True):
            with self.subTest(count=count):
                self.assertEqual(POLICY.classify(b"M\0README.md\0", count)["mode"], "full")

    def test_many_files_are_not_truncated(self):
        data = b"".join(f"A\0docs/{i}.md\0".encode() for i in range(3101))
        self.assert_selection(data, (), 3101)
        self.assert_selection(data + b"M\0processing/silver.py\0", JOBS[1:], 3102)

    def test_file_names_are_data(self):
        # No filename is interpolated into a shell, command, output key or value.
        self.assert_selection(b"M\0docs/$(touch nope) `echo nope`.md\0", ())

    def test_push_and_unsupported_events_are_full(self):
        for name, event in (("push", {"ref": "refs/heads/main"}), ("push", {}),
                            ("workflow_dispatch", {}), ("merge_group", {}),
                            ("pull_request_target", {}), ("pull_request", {})):
            with self.subTest(name=name, event=event):
                self.assertEqual(POLICY.plan_event(name, event)["mode"], "full")

    def test_cli_outputs_and_malformed_event(self):
        for event in ('{"ref":"refs/heads/main"}', '{bad', '[]'):
            with tempfile.TemporaryDirectory() as directory:
                directory = Path(directory)
                (directory / "event").write_text(event)
                result = subprocess.run(["python3", "-I", "-B", str(POLICY_PATH)],
                    env={**os.environ, "GITHUB_EVENT_NAME": "push", "GITHUB_EVENT_PATH": str(directory / "event"),
                         "GITHUB_OUTPUT": str(directory / "output"), "GITHUB_STEP_SUMMARY": str(directory / "summary")},
                    capture_output=True, text=True)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(json.loads(result.stdout)["mode"], "full")
                self.assertEqual(dict(line.split("=") for line in (directory / "output").read_text().splitlines()),
                                 {"classified": "true", "mode": "full", **dict.fromkeys(JOBS, "true")})


class GitCollectionTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.repository = Path(self.temporary.name)
        self.git("init", "-q")
        self.base = self.commit({"README.md": "original\n", "processing/old.py": "old\n"})

    def git(self, *args, data=None):
        return subprocess.run(["git", "-C", str(self.repository), *args], input=data,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True,
            env={**os.environ, "GIT_AUTHOR_NAME": "CI fixture", "GIT_AUTHOR_EMAIL": "ci@example.invalid",
                 "GIT_COMMITTER_NAME": "CI fixture", "GIT_COMMITTER_EMAIL": "ci@example.invalid"}).stdout

    def commit(self, files, parents=()):
        # Only temporary Git objects: no index, project commit, branch or remote mutation.
        tree = {}
        for path, content in files.items():
            node = tree
            parts = path.split("/")
            for part in parts[:-1]:
                node = node.setdefault(part, {})
            node[parts[-1]] = content

        def write_tree(node):
            entries = []
            for name, value in node.items():
                if isinstance(value, dict):
                    entries.append(f"040000 tree {write_tree(value)}\t{name}\n")
                else:
                    blob = self.git("hash-object", "-w", "--stdin", data=value.encode()).decode().strip()
                    entries.append(f"100644 blob {blob}\t{name}\n")
            return self.git("mktree", data="".join(entries).encode()).decode().strip()

        args = ["commit-tree", write_tree(tree), "-m", "isolated fixture"]
        for parent in parents:
            args.extend(("-p", parent))
        return self.git(*args).decode().strip()

    def plan(self, base, head, count):
        event = {"pull_request": {"base": {"ref": "main", "sha": base},
                                  "head": {"sha": head}, "changed_files": count}}
        original = POLICY.git
        with patch.object(POLICY, "git", side_effect=lambda *args: self.git(*args)):
            result = POLICY.plan_event("pull_request", event)
        self.assertIs(POLICY.git, original)
        return result

    def test_actual_added_modified_deleted_and_renamed_paths(self):
        files = {"README.md": "original\n", "processing/old.py": "old\n"}
        scenarios = [
            ({**files, "docs/new.md": "added\n"}, ()),
            ({**files, "README.md": "modified\n"}, ()),
            ({"README.md": "original\n"}, JOBS[1:]),
            ({"README.md": "original\n", "docs/moved.md": "old\n"}, JOBS[1:]),
        ]
        for changed, expected in scenarios:
            with self.subTest(files=changed):
                head = self.commit(changed, (self.base,))
                result = self.plan(self.base, head, 1)
                self.assertEqual({job for job, value in result["jobs"].items() if value == "true"}, set(expected))

    def test_diverged_base_uses_pr_merge_base(self):
        base = self.commit({"README.md": "original\n", "processing/old.py": "old\n", "config/new.json": "{}"}, (self.base,))
        head = self.commit({"README.md": "updated\n", "processing/old.py": "old\n"}, (self.base,))
        self.assertTrue(all(value == "false" for value in self.plan(base, head, 1)["jobs"].values()))

    def test_missing_commit_shallow_history_and_count_mismatch_are_full(self):
        head = self.commit({"README.md": "updated\n", "processing/old.py": "old\n"}, (self.base,))
        self.assertEqual(self.plan(self.base, "0" * 40, 1)["mode"], "full")
        self.assertEqual(self.plan(self.base, head, 2)["mode"], "full")
        (self.repository / ".git/shallow").write_text(head + "\n")
        self.assertEqual(self.plan(self.base, head, 1)["mode"], "full")

    def test_collection_errors_and_ambiguous_merge_base_are_full(self):
        event = {"pull_request": {"base": {"ref": "main", "sha": self.base},
                                  "head": {"sha": self.base}, "changed_files": 1}}
        for error in (OSError("unavailable"), subprocess.TimeoutExpired("git", 60),
                      subprocess.CalledProcessError(1, "git")):
            with patch.object(POLICY, "git", side_effect=error):
                self.assertEqual(POLICY.plan_event("pull_request", event)["mode"], "full")
        for bases in (b"", (self.base + "\n" + "a" * 40 + "\n").encode()):
            with patch.object(POLICY, "git", side_effect=[b"false\n", b"", b"", bases]):
                self.assertEqual(POLICY.plan_event("pull_request", event)["mode"], "full")
        event["pull_request"]["head"]["sha"] = "$(echo unsafe)"
        self.assertEqual(POLICY.plan_event("pull_request", event)["mode"], "full")

    def test_trusted_loader_ignores_pr_policy_and_bootstraps_full(self):
        # Run the exact YAML loader with a trusted base policy and a hostile worktree file.
        trusted = self.commit({".github/ci/classify.py": POLICY_PATH.read_text(), "README.md": "old\n"})
        (self.repository / ".github/ci").mkdir(parents=True)
        (self.repository / ".github/ci/classify.py").write_text("raise RuntimeError('PR policy executed')\n")
        (self.repository / "event.json").write_text('{"ref":"refs/heads/main"}')
        for sha in (trusted, self.base):
            output, summary = self.repository / "output", self.repository / "summary"
            output.write_text("")
            result = subprocess.run(["bash", "-e", "-o", "pipefail", "-c", run_script("changes")],
                cwd=self.repository, env={**os.environ, "POLICY_SHA": sha, "RUNNER_TEMP": str(self.repository),
                    "GITHUB_EVENT_NAME": "push", "GITHUB_EVENT_PATH": str(self.repository / "event.json"),
                    "GITHUB_OUTPUT": str(output), "GITHUB_STEP_SUMMARY": str(summary)},
                capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(dict(line.split("=") for line in output.read_text().splitlines()),
                             {"classified": "true", "mode": "full", **dict.fromkeys(JOBS, "true")})

    def test_crashed_trusted_policy_blocks_without_skip_outputs(self):
        trusted = self.commit({".github/ci/classify.py": "raise RuntimeError('classifier failure')\n"})
        output = self.repository / "output"
        output.write_text("")
        result = subprocess.run(["bash", "-e", "-o", "pipefail", "-c", run_script("changes")],
            cwd=self.repository, env={**os.environ, "POLICY_SHA": trusted, "RUNNER_TEMP": str(self.repository),
                "GITHUB_OUTPUT": str(output), "GITHUB_STEP_SUMMARY": str(self.repository / "summary")},
            capture_output=True, text=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(output.read_text(), "")


class GateTests(unittest.TestCase):
    def check(self, needs, event_name="pull_request", event_ref="refs/pull/1/merge"):
        with redirect_stdout(io.StringIO()):
            return CHECK_GATE(needs, event_name, event_ref)

    def test_all_32768_selection_and_terminal_result_combinations(self):
        checked = accepted = 0
        for flags in itertools.product((False, True), repeat=len(JOBS)):
            required = {job for job, flag in zip(JOBS, flags) if flag}
            for results in itertools.product(("success", "failure", "cancelled", "skipped"), repeat=len(JOBS)):
                needs = needs_for(required)
                for job, result in zip(JOBS, results):
                    needs[job]["result"] = result
                expected = all(result == ("success" if flag else "skipped") for flag, result in zip(flags, results))
                actual = self.check(needs)
                self.assertEqual(actual, expected, (flags, results))
                checked += 1
                accepted += actual
        self.assertEqual((checked, accepted), (32768, 32))

    def test_missing_unknown_and_timed_out_results_fail(self):
        for required in (set(), set(JOBS)):
            for job in JOBS:
                for result in ("", "timed_out", "neutral", "pending", None, "unknown"):
                    needs = needs_for(required)
                    needs[job]["result"] = result
                    self.assertFalse(self.check(needs), (required, job, result))
                for value in ({}, None, "success"):
                    needs = needs_for(required)
                    needs[job] = value
                    self.assertFalse(self.check(needs))
                needs = needs_for(required)
                del needs[job]
                self.assertFalse(self.check(needs))

    def test_classifier_failure_and_invalid_outputs_always_fail(self):
        for result in ("failure", "cancelled", "skipped", "", "timed_out", None):
            needs = needs_for(JOBS)
            needs["changes"]["result"] = result
            self.assertFalse(self.check(needs))
        for key in ("classified", "mode", *JOBS):
            for value in (None, "", "unknown", True):
                needs = needs_for(JOBS)
                needs["changes"]["outputs"][key] = value
                self.assertFalse(self.check(needs), (key, value))
            needs = needs_for(JOBS)
            del needs["changes"]["outputs"][key]
            self.assertFalse(self.check(needs))
        for key, value in (("classified", "false"), ("ingestion", "false")):
            needs = needs_for(JOBS)
            needs["changes"]["outputs"][key] = value
            self.assertFalse(self.check(needs))
        for malformed in (None, [], {}, {**needs_for(JOBS), "unknown": {"result": "success"}}):
            self.assertFalse(self.check(malformed))

    def test_exact_gate_shell_and_invalid_json_fail(self):
        for needs, expected in ((needs_for(JOBS), 0), (needs_for(()), 0),
                                ({**needs_for(JOBS), "ingestion": {"result": "skipped"}}, 1)):
            result = subprocess.run(["bash", "-e", "-o", "pipefail", "-c", GATE_SCRIPT],
                                    env={**os.environ, "NEEDS_JSON": json.dumps(needs),
                                         "EVENT_NAME": "pull_request", "EVENT_REF": "refs/pull/1/merge"}, capture_output=True)
            self.assertEqual(result.returncode, expected, result.stderr)
        result = subprocess.run(["bash", "-e", "-o", "pipefail", "-c", GATE_SCRIPT],
                                env={**os.environ, "NEEDS_JSON": "{bad"}, capture_output=True)
        self.assertNotEqual(result.returncode, 0)

    def test_pushes_require_full_ci_and_unsupported_events_block(self):
        self.assertTrue(self.check(needs_for(JOBS), "push", "refs/heads/main"))
        self.assertFalse(self.check(needs_for(()), "push", "refs/heads/main"))
        self.assertFalse(self.check(needs_for(JOBS), "push", "refs/heads/other"))
        for event in ("workflow_dispatch", "pull_request_target", "merge_group", ""):
            self.assertFalse(self.check(needs_for(JOBS), event))


class WorkflowTests(unittest.TestCase):
    def test_real_bronze_storage_is_isolated_and_cleaned_up(self):
        body = POSTGRES_WORKFLOW
        self.assertIn("chrislusf/seaweedfs:4.48@sha256:4e61d15fd35994cb1e43e1e553dff106794841fd9a99ade2fc8c8bfce4d7872d", body)
        self.assertIn("--publish 127.0.0.1:8333:8333", body)
        self.assertIn("BRONZE_S3_ENDPOINT: http://127.0.0.1:8333", body)
        self.assertIn("--env S3_BUCKET=ddip-d2", body)
        self.assertIn("-s3.autoCreateBucket=false", body)
        self.assertIn('docker rm --force --volumes "ddip-ci-bronze-${POSTGRES_PARTITION}-${GITHUB_RUN_ID}-${GITHUB_RUN_ATTEMPT}"', body)
        self.assertIn("always() && (inputs.partition == 'ingestion' || inputs.partition == 'gold')", body)
        pom = (ROOT / "services/content-api/pom.xml").read_text(encoding="utf-8")
        self.assertIn("**/*IT.java", pom)
        self.assertIn("**/GoldApiIT.java", pom)

    def test_graph_filters_and_required_contexts(self):
        for job in (*[j for j in JOBS if j != "content_postgres"], "content_postgres_core", "content_postgres_reliability", "content_postgres_ingestion", "content_postgres_gold"):
            body = block(job)
            key = "content_postgres" if job.startswith("content_postgres_") else job
            self.assertIn("    needs: changes", body)
            self.assertIn("if: ${{ always() && (needs.changes.result != 'success' || "
                          f"needs.changes.outputs.{key} != 'false') }}}}", body)
        for job, name in (("documentation", "Documentation and repository hygiene"),
                          ("ingestion", "Java 21 ingestion and Bronze storage"),
                          ("processing", "Python 3.12, local Spark processing and DuckDB analytics"),
                          ("frontend", "React publisher dashboard")):
            self.assertEqual(WORKFLOW.count(f"    name: {name}\n"), 1)
            self.assertIn(f"    name: {name}", block(job))
        self.assertIn("needs: [changes, ingestion, processing_python, analytics_jdbc, content_postgres, analytics_http]", block("processing"))
        self.assertIn("if: ${{ always() }}", block("processing"))
        self.assertIn("NEEDS_JSON: ${{ toJSON(needs) }}", block("processing"))
        self.assertNotIn("needs:", block("documentation"))
        self.assertNotIn("needs:", block("frontend"))
        for forbidden in ("paths:", "paths-ignore:", "continue-on-error:", "pull_request_target:", "workflow_dispatch:"):
            self.assertNotIn(forbidden, WORKFLOW)
        self.assertIn("  pull_request:\n    branches: [main]\n  push:\n    branches: [main]", WORKFLOW)
        self.assertIn("permissions:\n  contents: read", WORKFLOW)

    def test_classifier_trust_outputs_and_isolation(self):
        body = block("changes")
        self.assertNotIn("    if:", body)
        self.assertIn("fetch-depth: 0", body)
        self.assertIn("ref: ${{ github.sha }}", body)
        self.assertIn("POLICY_SHA: ${{ github.event.pull_request.base.sha || github.sha }}", body)
        self.assertIn('python3 -I -B "$RUNNER_TEMP/ddip-classify.py"', body)
        for key in ("classified", "mode", *JOBS):
            self.assertIn(f"{key}: ${{{{ steps.classify.outputs.{key} }}}}", body)
        self.assertIn("python3 -I -B -m unittest discover -s .github/ci/tests -v", block("documentation"))
        self.assertEqual(POSTGRES_WORKFLOW.count("    services:"), 1)
        self.assertIn("mvn -B -ntp -f services/content-api/pom.xml verify -Ppostgres", POSTGRES_WORKFLOW)
        self.assertIn('mvn -B -ntp -f services/content-api/pom.xml verify -Pintegration -Danalytics.python="$(command -v python)"', block("analytics_http"))
        self.assertIn("python -m unittest discover -s processing/tests -v", block("processing_python"))
        self.assertIn("mvn -B -ntp -f services/api/pom.xml verify", block("analytics_jdbc"))
        for job in JOBS[1:]:
            self.assertIn("python-version: '3.12.15'", POSTGRES_WORKFLOW if job == "content_postgres" else block(job))


if __name__ == "__main__":
    unittest.main()
