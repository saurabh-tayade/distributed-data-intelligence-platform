"""Conservative CI policy. Standard library only; changed files are data, never code."""

import json
import os
from pathlib import Path, PurePosixPath
import re
import subprocess


JOBS = ("ingestion", "processing_python", "analytics_jdbc", "content_postgres", "analytics_http")
NATIVE = frozenset(JOBS[1:])
ALL = frozenset(JOBS)
DEPENDENCIES = {"pom.xml", "package.json", "package-lock.json", "pyproject.toml",
                "uv.lock", "poetry.lock", ".python-version", ".java-version", ".tool-versions"}
READMES = {"README.md", "AGENTS.md", "ingestion/README.md", "processing/README.md",
           "services/api/README.md", "services/content-api/README.md", "frontend/README.md"}
HYGIENE = {".editorconfig", ".gitignore", ".markdownlint-cli2.mjs", "tests/.gitkeep",
           "scripts/.gitkeep", "services/.gitkeep", "processing/.gitkeep"}
DOC_SUFFIXES = {".md", ".png", ".svg", ".jpg", ".jpeg", ".gif", ".webp", ".pdf"}


def full(reason):
    return {"classified": "true", "mode": "full", "categories": ["shared"],
            "reasons": [reason], "jobs": {job: "true" for job in JOBS}}


def area(path):
    """Return a category and its backend consumers. Unknown paths select everything."""
    parts = PurePosixPath(path).parts
    name = parts[-1]
    if (name in DEPENDENCIES or name.startswith("requirements") or
            any(part in {"schema", "schemas", "contracts"} for part in parts) or
            (path.startswith((".github/", "config/", "infrastructure/", "tests/")) and path not in HYGIENE) or
            path == ".env.example"):
        return "shared", ALL
    if (path in READMES or path in HYGIENE or
            path.startswith("docs/") and (PurePosixPath(path).suffix in DOC_SUFFIXES or name == ".gitkeep")):
        return "documentation", frozenset()
    if path.startswith("frontend/"):
        return "frontend", frozenset()
    if path == "ingestion/src/test/java/platform/ingestion/managed/BronzeFixture.java":
        # This test-only SDK helper is also executed by the PostgreSQL/SeaweedFS suite.
        return "java_ingestion", frozenset({"ingestion", "content_postgres"})
    if path.startswith("ingestion/src/test/java/") or path in {
            "scripts/run-batch.sh", "scripts/test-run-batch.sh", "scripts/test-ingestion.sh"}:
        return "java_ingestion", frozenset({"ingestion"})
    if path.startswith("ingestion/"):
        # Raw contracts and feeds are consumed through Bronze/Silver into Gold fixtures.
        return "java_ingestion", ALL
    if path.startswith("processing/"):
        # PostgreSQL native tests and both Gold consumers import processing/tests helpers.
        return "python_processing", NATIVE
    if path.startswith("services/api/tests/"):
        return "jdbc_compatibility", frozenset({"analytics_jdbc", "analytics_http"})
    if path.startswith("services/api/"):
        return "jdbc_compatibility", frozenset({"analytics_jdbc"})
    if path.startswith("services/content-api/src/main/resources/db/"):
        return "shared", ALL
    if path.startswith("services/content-api/"):
        # Both profiles compile the same sources and run the same unit/component tests.
        return "postgres_backend", frozenset({"content_postgres", "analytics_http"})
    return "unknown", ALL


def parse_diff(data):
    """Parse complete NUL-delimited git --name-status output, including both rename paths."""
    if not data or not data.endswith(b"\0"):
        raise ValueError("empty or incomplete change list")
    tokens = data[:-1].decode("utf-8", errors="strict").split("\0")
    changes = []
    index = 0
    while index < len(tokens):
        status = tokens[index]
        index += 1
        count = 2 if re.fullmatch(r"R(?:100|[0-9]{1,2})", status) else 1
        if status not in {"A", "M", "D"} and count != 2:
            raise ValueError("unsupported change status")
        paths = tokens[index:index + count]
        if len(paths) != count:
            raise ValueError("incomplete rename or path")
        for path in paths:
            if (not path or path.startswith("/") or "\\" in path or
                    any(part in {"", ".", ".."} for part in path.split("/")) or
                    any(ord(char) < 32 or ord(char) == 127 for char in path)):
                raise ValueError("ambiguous path")
        changes.append((status, paths))
        index += count
    return changes


def classify(data, expected_count):
    try:
        changes = parse_diff(data)
        if type(expected_count) is not int or expected_count <= 0 or len(changes) != expected_count:
            return full("missing or inconsistent changed_files count")
    except ValueError:
        return full("invalid or incomplete change list")
    categories = set()
    required = set()
    for _, paths in changes:
        for path in paths:
            category, consumers = area(path)
            categories.add(category)
            if "analytics_http" in consumers:
                categories.add("analytics_http")
            required.update(consumers)
    mode = "full" if required == ALL else "selective"
    return {"classified": "true", "mode": mode, "categories": sorted(categories),
            "reasons": ["complete base-to-head Git diff"], "changed_files": len(changes),
            "jobs": {job: "true" if job in required else "false" for job in JOBS}}


def git(*args):
    return subprocess.run(["git", *args], check=True, stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE, timeout=60).stdout


def plan_event(event_name, event):
    if event_name == "push" and event.get("ref") == "refs/heads/main":
        return full("push to main requires full regression")
    if event_name != "pull_request":
        return full("unsupported event requires full regression")
    try:
        pr = event["pull_request"]
        if pr["base"]["ref"] != "main":
            return full("unsupported PR base requires full regression")
        base, head = pr["base"]["sha"], pr["head"]["sha"]
        if any(not isinstance(sha, str) or not re.fullmatch(r"[0-9a-f]{40}", sha) for sha in (base, head)):
            return full("invalid base or head SHA")
        if git("rev-parse", "--is-shallow-repository").strip() != b"false":
            return full("incomplete Git history")
        for sha in (base, head):
            git("cat-file", "-e", sha + "^{commit}")
        bases = git("merge-base", "--all", base, head).decode("ascii").splitlines()
        if len(bases) != 1 or not re.fullmatch(r"[0-9a-f]{40}", bases[0]):
            return full("missing or ambiguous merge base")
        # Three-dot PR semantics, no API pagination/3000-file cap, no external diff drivers.
        data = git("diff", "--no-ext-diff", "--no-textconv", "--name-status", "-z",
                   "--find-renames=50%", bases[0], head, "--")
        return classify(data, pr.get("changed_files"))
    except (KeyError, TypeError, ValueError, OSError, subprocess.SubprocessError):
        return full("event or Git collection failure; inspect classifier logs and reproduce locally")


def main():
    try:
        event = json.loads(Path(os.environ["GITHUB_EVENT_PATH"]).read_text(encoding="utf-8"))
        if not isinstance(event, dict):
            raise ValueError("event must be an object")
        plan = plan_event(os.environ.get("GITHUB_EVENT_NAME", ""), event)
    except (KeyError, OSError, ValueError):
        plan = full("unreadable or malformed event")
    print(json.dumps(plan, sort_keys=True))
    # Emit only fixed keys and enumerated values after the entire plan is ready.
    with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as output:
        for key in ("classified", "mode"):
            output.write(f"{key}={plan[key]}\n")
        for job, required in plan["jobs"].items():
            output.write(f"{job}={required}\n")
    with open(os.environ["GITHUB_STEP_SUMMARY"], "a", encoding="utf-8") as summary:
        summary.write("### CI classification\n\n```json\n" + json.dumps(plan, indent=2, sort_keys=True) + "\n```\n")


if __name__ == "__main__":
    main()
