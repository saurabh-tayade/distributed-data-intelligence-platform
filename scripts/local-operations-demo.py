#!/usr/bin/env python3
"""Disposable, foreground DDIP demo. See docs/operations/local-operations-demo.md."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import secrets
import shutil
import signal
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid

REPO = Path(__file__).resolve().parent.parent
ROOT = REPO / "local-data/operations-demo"
IMAGE = "postgres:17.11-bookworm@sha256:3645570cccdfa447589da9f57dd740faa29b30938e861289a5574b6ca6b03826"
LABEL = "ddip.operations-demo"
INSTALL = "10000000-0000-4000-8000-000000000001"
PROFILE = "10000000-0000-4000-8000-000000000002"
RUNTIME = "reliability-preflight-v1"
IMPORT = "demo-native-import-v1"
EXPECTED = dict(occurrences="15", accepted="10", rejected="5", events="4", windows="3")


def require(value, message):
    if not value:
        raise RuntimeError(message)


def write_json(path, value):
    temporary = path.with_suffix(".new")
    temporary.write_text(json.dumps(value, indent=2) + "\n")
    temporary.chmod(0o600)
    temporary.replace(path)


def load_state():
    require(not ROOT.is_symlink() and ROOT.resolve() == ROOT, "Demo directory must be canonical, without symlinks")
    state = json.loads((ROOT / "state.json").read_text())
    require(state["root"] == str(ROOT) and str(uuid.UUID(state["id"])) == state["id"], "Invalid demo ownership marker")
    require(ROOT.stat().st_uid == os.getuid() and ROOT.stat().st_mode & 0o077 == 0, "Demo directory must be private and owned by this user")
    return state


def process_stamp(pid):
    result = subprocess.run(["ps", "-p", str(pid), "-o", "lstart=", "-o", "command="], capture_output=True, text=True, check=False)
    return result.stdout.strip() if result.returncode == 0 else None


def docker(*args, data=None):
    try:
        return subprocess.run(["docker", *args], input=data, text=True, capture_output=True, check=True, timeout=60).stdout.strip()
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
        raise RuntimeError(f"Docker {args[0]} failed; check the local Docker daemon/image. Resources are preserved unless ownership and shutdown can be verified") from None


def owned_container(state):
    container = state.get("container") or state.get("containerName")
    if not container:
        return None
    result = subprocess.run(["docker", "inspect", container], capture_output=True, text=True, timeout=15)
    if result.returncode:
        missing = result.stderr.lower()
        require("no such object:" in missing or "no such container:" in missing, "Cannot inspect demo container; infrastructure preserved")
        return None
    info = json.loads(result.stdout)[0]
    require((not state.get("container") or info["Id"] == container) and info["Config"]["Labels"].get(LABEL) == state["id"], "Container ownership mismatch; refusing teardown")
    return info["Id"]


def sql(state, statement):
    container = owned_container(state)
    require(container is not None, "Demo PostgreSQL is unavailable")
    return docker("exec", "-i", container, "psql", "-X", "-v", "ON_ERROR_STOP=1", "-U", "ddip_demo", "-d", "ddip_demo", "-At", data=statement)


def http(base, path, body=None, etag=None, expected=200, origin=None):
    headers = {"Accept": "application/json", "Origin": origin or base}
    if body is not None:
        headers.update({"Content-Type": "application/json", "X-DDIP-Local-Request": "1", "Idempotency-Key": str(uuid.uuid4())})
    if etag is not None:
        headers["If-Match"] = etag
    request = urllib.request.Request(base + path, headers=headers, data=None if body is None else json.dumps(body).encode())
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            payload = json.load(response)
            require(response.status == expected, f"Unexpected HTTP status for {path}")
            if path.startswith("/api/management/"):
                require(response.headers.get("Cache-Control") == "no-store" and payload["apiVersion"] == 1, "Management envelope/cache contract mismatch")
            return payload, response.headers
    except urllib.error.HTTPError as error:
        raise RuntimeError(f"HTTP {error.code} for {path}; see private demo logs") from None


def wait_for(description, predicate, seconds=60):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.3)
    raise RuntimeError(description + " timed out")


def quote(value):
    return "'" + value.replace("'", "''") + "'"


def prepare(root):
    """Reuse existing synthetic producers. No management record is fabricated here."""
    scratch = root / "fixture-scratch"
    scratch.mkdir(mode=0o700)
    os.chdir(root)
    os.environ.update(TMPDIR=str(scratch), SPARK_LOCAL_DIRS=str(scratch), SPARK_SUBMIT_OPTS="-Djava.io.tmpdir=" + str(scratch))
    sys.path[:0] = [str(REPO / "processing"), str(REPO / "processing/tests")]
    from publication_metadata import canonical_bytes
    from reliability_preflight_fixtures import create
    from gold_fixtures import create_analytics_publications
    fixture = create(root / "native", "JSONL", multi=True, publication=True)
    (root / "fixture.json").write_bytes(canonical_bytes(fixture))
    frozen = fixture["bundle"]["capturedSelection"]
    storage = frozen["storageBindings"]
    source = frozen["sources"][0]
    catalog = dict(catalogVersion=1, endpoints=[], reliabilityProfiles=[dict(sourceId=source["sourceId"], profile=source["configuration"]["profile"], dataOrigin="synthetic", registeredProfileSha256=source["configuration"]["registeredProfileSha256"])],
                   storageProfiles=[dict(profileId=source["configuration"]["inputStorageProfileId"], productId="application-reliability", role="BATCH")])
    (root / "source-catalog.json").write_bytes(canonical_bytes(catalog))
    from hashlib import sha256
    # ImportBindings.MODULES, unchanged production v1 import contract.
    modules = ("artifact_import_verifier.py", "publication_metadata.py", "reliability_format_inputs.py", "reliability_format_metadata.py", "reliability_format_parquet.py", "reliability_inputs.py", "reliability_metadata.py", "publisher_reference.py", "service_reference.py")
    processing = REPO / "processing"
    import_profile = dict(importProtocolVersion=1, python=sys.executable, processingRoot=str(processing), pythonVersion="3.12.15", duckdbVersion="1.4.5", timeoutSeconds=30,
                          modules={name: sha256((processing / name).read_bytes()).hexdigest() for name in modules})
    profiles = {PROFILE: frozen["executionProfile"]["configuration"], RUNTIME: fixture["command"]["runtime"]["configuration"], IMPORT: import_profile}
    statements = ["BEGIN;"]
    for identity, configuration in profiles.items():
        raw = canonical_bytes(configuration)
        statements.append(f"INSERT INTO ddip_management.execution_profiles(profile_id,configuration_bytes,configuration_sha256) VALUES ({quote(identity)},decode('{raw.hex()}','hex'),'{sha256(raw).hexdigest()}');")
    for binding in storage:
        raw = canonical_bytes(binding["configuration"])
        statements.append(f"INSERT INTO ddip_management.storage_bindings(binding_id,product,role,configuration_bytes,configuration_sha256,installation_id) VALUES ({quote(binding['bindingId'])},'application-reliability',{quote(binding['role'])},decode('{raw.hex()}','hex'),'{sha256(raw).hexdigest()}','{INSTALL}');")
        for identity in profiles:
            statements.append(f"INSERT INTO ddip_management.execution_profile_storage VALUES ({quote(identity)},{quote(binding['bindingId'])});")
    statements.append("COMMIT;")
    (root / "provision.sql").write_text("\n".join(statements))
    selection = dict(catalogVersion=1, operationId=str(uuid.uuid4()), executionProfileId=IMPORT, controlBindingId="test-control",
                     artifacts=[item["qualification"]["selection"] for item in frozen["inputs"]])
    (root / "import-catalog.json").write_bytes(canonical_bytes(selection))
    gold, publications = create_analytics_publications(root / "analytics")
    write_json(root / "prepared.json", dict(source=source, runtime=profiles[RUNTIME], goldRoot=str(gold), publication=publications["full"].rsplit("=", 1)[1]))


def verify_native(root, fingerprint):
    sys.path[:0] = [str(REPO / "processing")]
    from reliability_format_analytics import open_publication
    fixture = json.loads((root / "fixture.json").read_bytes())
    roots = fixture["bundle"]["bindings"]
    output = Path(roots["output"]["root"])
    before = {str(p.relative_to(output)): (hashlib.sha256(p.read_bytes()).hexdigest(), p.stat().st_mtime_ns) for p in output.rglob("*") if p.is_file()}
    with open_publication(output, fingerprint, batch_root=roots["batch"]["root"], reference_root=roots["reference"]["root"]) as opened:
        # Explicit table queries after the full independent native reader reconciles inputs/output.
        counts = dict(occurrences=str(opened._db.execute("SELECT count(*) FROM reliability_occurrences_v2").fetchone()[0]),
                      events=str(opened._db.execute("SELECT count(*) FROM reliability_events_v1").fetchone()[0]),
                      windows=str(opened._db.execute("SELECT count(*) FROM reliability_requests_5m_v1").fetchone()[0]))
        counts.update(accepted=str(opened._marker["occurrence_counts"]["valid_occurrences"]), rejected=str(opened._marker["occurrence_counts"]["rejected_occurrences"]))
    require(counts == EXPECTED, "Independent fixture counts differ from the established 15-occurrence workload")
    after = {str(p.relative_to(output)): (hashlib.sha256(p.read_bytes()).hexdigest(), p.stat().st_mtime_ns) for p in output.rglob("*") if p.is_file()}
    require(before == after, "Independent read changed native output")
    write_json(root / "native-verification.json", dict(counts=counts, fingerprint=fingerprint, unchangedFiles=len(before)))


class Demo:
    def __init__(self, args):
        self.args, self.children, self.created = args, [], False
        self.started = time.monotonic()
        self.state = dict(id=str(uuid.uuid4()), root=str(ROOT), phase="INITIALIZING", supervisor=os.getpid(), supervisorStamp=process_stamp(os.getpid()), timings={})
        self.env = {key: value for key, value in os.environ.items() if not key.startswith(("DDIP_", "ANALYTICS_", "SPRING_", "SERVER_"))}
        for key in ("JAVA_TOOL_OPTIONS", "JDK_JAVA_OPTIONS", "_JAVA_OPTIONS"):
            self.env.pop(key, None)

    def phase(self, name):
        self.state["phase"] = name
        self.state["timings"][name] = round(time.monotonic() - self.started, 2)
        write_json(ROOT / "state.json", self.state)
        print(f"{name}: {self.state['timings'][name]:.2f}s", flush=True)

    def command(self, name, command, env=None, timeout=180):
        child = self.launch(name, command, env)
        child.wait(timeout=timeout)
        require(child.returncode == 0, f"{name} failed; inspect {ROOT / (name + '.log')}")

    def launch(self, name, command, env=None, cwd=REPO):
        with (ROOT / (name + ".log")).open("w") as log:
            child = subprocess.Popen(command, cwd=cwd, env=env or self.env, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        self.children.append((name, child))
        self.state.setdefault("processes", {})[name] = dict(pid=child.pid, stamp=process_stamp(child.pid))
        write_json(ROOT / "state.json", self.state)
        return child

    def java(self, role):
        return [str(Path(self.env["JAVA_HOME"]) / "bin/java"), "-jar", str(REPO / "services/content-api/target/content-api-0.1.0-SNAPSHOT.jar"), "--ddip.role=" + role,
                "--spring.config.location=classpath:/application.properties", "--spring.main.banner-mode=off", "--logging.level.root=ERROR"]

    def up(self):
        require(ROOT.resolve() == ROOT, "Demo directory ancestors must not be symlinks")
        require(not ROOT.exists() and not ROOT.is_symlink(), "Demo directory already exists. Use status/down; existing resources are never overwritten")
        for port in (self.args.api_port, self.args.ui_port):
            with socket.socket() as reservation:
                reservation.bind(("127.0.0.1", port))
        require(self.args.api_port != self.args.ui_port, "API and UI ports must differ")
        python = Path(os.environ.get("DDIP_DEMO_PYTHON", os.environ.get("DDIP_TEST_PYTHON", "")))
        require(python.is_absolute() and python.is_file(), "Set DDIP_DEMO_PYTHON to the pinned Python 3.12.15 environment")
        self.env.update(PYSPARK_PYTHON=str(python), PYSPARK_DRIVER_PYTHON=str(python), SPARK_LOCAL_IP="127.0.0.1", TZ="UTC", PYTHONDONTWRITEBYTECODE="1")
        require(Path(self.env.get("JAVA_HOME", "")).is_absolute(), "Set JAVA_HOME to JDK 21")
        version = subprocess.run([str(Path(self.env["JAVA_HOME"]) / "bin/java"), "-version"], capture_output=True, text=True, check=True)
        require('version "21.' in version.stderr, "Java 21 is required")
        version = subprocess.check_output(["node", "--version"], text=True)
        require(version.startswith("v24."), "Node.js 24 is required")
        subprocess.run([str(python), "-c", "import sys,pyspark,duckdb; assert sys.version_info[:3]==(3,12,15); assert pyspark.__version__=='4.0.2'; assert duckdb.__version__=='1.4.5'"], check=True)
        endpoint = os.environ.get("DOCKER_HOST") or json.loads(docker("context", "inspect"))[0]["Endpoints"]["docker"]["Host"]
        require(endpoint.startswith("unix://"), "A local Unix Docker endpoint is required; remote Docker contexts are unsupported")
        docker("info", "--format", "{{.ServerVersion}}")
        require((REPO / "frontend/node_modules/vite/bin/vite.js").is_file(), "Install the pinned frontend dependencies with npm ci --prefix frontend")
        ROOT.mkdir(mode=0o700, parents=True)
        self.created = True
        self.phase("BUILDING")
        maven = ["mvn", "-B", "-ntp", "-Daether.syncContext.named.factory=noop", "-f", "services/content-api/pom.xml", "-DskipTests", "package"]
        if self.args.offline:
            maven.insert(1, "-o")
        self.command("build-api", maven)
        self.command("build-ui", ["npm", "--prefix", "frontend", "run", "build"])
        self.phase("PREPARING_SYNTHETIC_FIXTURES")
        self.command("fixtures", [str(python), str(Path(__file__).resolve()), "_prepare", str(ROOT)])
        prepared = json.loads((ROOT / "prepared.json").read_bytes())
        self.phase("STARTING_POSTGRES")
        self.state["containerName"] = "ddip-demo-" + self.state["id"]
        write_json(ROOT / "state.json", self.state)
        container = docker("run", "--detach", "--name", self.state["containerName"], "--label", LABEL + "=" + self.state["id"], "--publish", "127.0.0.1::5432",
                           "--env", "POSTGRES_DB=ddip_demo", "--env", "POSTGRES_USER=ddip_demo", "--env", "POSTGRES_PASSWORD=" + secrets.token_urlsafe(32),
                           "--env", "POSTGRES_INITDB_ARGS=--auth-host=scram-sha-256", IMAGE)
        self.state["container"] = container
        write_json(ROOT / "state.json", self.state)
        def postgres_ready():
            result = subprocess.run(["docker", "exec", container, "pg_isready", "-h", "127.0.0.1", "-U", "ddip_demo", "-d", "ddip_demo"], capture_output=True)
            return result.returncode == 0
        wait_for("PostgreSQL readiness", postgres_ready)
        info = json.loads(docker("inspect", container))[0]
        password = next(value.split("=", 1)[1] for value in info["Config"]["Env"] if value.startswith("POSTGRES_PASSWORD="))
        port = info["NetworkSettings"]["Ports"]["5432/tcp"][0]["HostPort"]
        base = f"http://127.0.0.1:{self.args.api_port}"
        ui = f"http://127.0.0.1:{self.args.ui_port}"
        self.state.update(api=base, ui=ui)
        runtime = prepared["runtime"]
        self.env.update(DDIP_DB_URL=f"jdbc:postgresql://127.0.0.1:{port}/ddip_demo", DDIP_DB_USER="ddip_demo", DDIP_DB_PASSWORD=password, DDIP_INSTALLATION_ID=INSTALL,
                        DDIP_MANAGEMENT_CATALOG=str(ROOT / "source-catalog.json"), DDIP_MANAGEMENT_CURSOR_KEY=secrets.token_urlsafe(32),
                        DDIP_IMPORT_CATALOG=str(ROOT / "import-catalog.json"), DDIP_WORKER_EXECUTION_PROFILE_ID=PROFILE, DDIP_WORKER_RUNTIME_PROFILE_ID=RUNTIME,
                        DDIP_WORKER_CONTROL_BINDING_ID="test-control", DDIP_WORKER_DEPLOYMENT_SHA256=runtime["deploymentSha256"], DDIP_WORKER_PYTHON=runtime["python"],
                        DDIP_WORKER_JAVA_HOME=runtime["javaHome"], DDIP_WORKER_PROCESSING_ROOT=runtime["processingRoot"])
        self.phase("MIGRATING_AND_PROVISIONING_REFERENCES")
        self.command("migrate", self.java("migrate"))
        sql(self.state, (ROOT / "provision.sql").read_text())
        self.phase("STARTING_API")
        api = self.launch("api", self.java("api") + [f"--server.port={self.args.api_port}", f"--ddip.management.origins={base},{ui}",
                          "--analytics.python-executable=" + str(python), "--analytics.script=" + str(REPO / "processing/gold_analytics.py"),
                          "--analytics.gold-root=" + prepared["goldRoot"], "--analytics.publication=" + prepared["publication"]])
        def api_ready():
            require(api.poll() is None, "API exited during startup; inspect api.log")
            try:
                http(base, "/api/management/v1/capabilities")
                return True
            except (OSError, urllib.error.URLError):
                return False
        wait_for("API readiness", api_ready)
        source = prepared["source"]
        http(base, "/api/management/v1/sources", dict(productId="application-reliability", sourceId=source["sourceId"], displayName="Synthetic demo requests", enabled=True, config=source["configuration"]), expected=201)
        self.phase("IMPORTING_NATIVE_INPUTS")
        self.command("operator-import", self.java("operator-import"))
        imported = json.loads((ROOT / "operator-import.log").read_text())
        require(imported.get("status") == "VERIFIED", "Operator import did not freshly verify inputs")
        profiles, _ = http(base, "/api/management/v1/execution-profiles?limit=20")
        require(any(row["executionProfileId"] == PROFILE for row in profiles["data"]), "Run profile discovery missing")
        batches, _ = http(base, f"/api/management/v1/artifacts?executionProfileId={PROFILE}&role=RELIABILITY_BATCH&sourceId={source['sourceId']}&limit=20")
        references, _ = http(base, f"/api/management/v1/artifacts?executionProfileId={PROFILE}&role=SERVICE_REFERENCE&limit=20")
        require(len(batches["data"]) == 2 and len(references["data"]) == 1, "Compatible artifact discovery mismatch")
        self.phase("STARTING_WORKER")
        worker = self.launch("worker", self.java("worker"))
        def worker_ready():
            require(worker.poll() is None, "Worker readiness failed; inspect worker.log")
            return sql(self.state, "SELECT EXISTS(SELECT 1 FROM ddip_management.worker_instances WHERE status='READY');") == "t"
        wait_for("Worker readiness", worker_ready)
        request = dict(pipeline="RELIABILITY_V2", executionProfileId=PROFILE, sources=[dict(sourceId=source["sourceId"], revision=1)],
                       batchArtifactIds=[row["artifactId"] for row in batches["data"]], serviceReferenceArtifactId=references["data"][0]["artifactId"])
        admitted, headers = http(base, "/api/management/v1/runs", request, expected=202)
        run = admitted["data"]["runId"]
        require(headers["Location"] == "/api/management/v1/runs/" + run, "Admission Location mismatch")
        self.state["runId"] = run
        self.phase("EXECUTING_RELIABILITY")
        def completed():
            detail, _ = http(base, "/api/management/v1/runs/" + run)
            state = detail["data"]["state"]
            require(state not in ("FAILED", "RECOVERY_REQUIRED", "CANCELLED"), f"Actual worker execution ended in {state}; inspect private logs")
            require(worker.poll() is None, "Worker stopped before completion; inspect worker.log")
            return state == "SUCCEEDED"
        wait_for("Reliability execution", completed, 180)
        self.phase("VERIFYING_HTTP_AND_NATIVE_RESULT")
        detail, _ = http(base, "/api/management/v1/runs/" + run)
        detail = detail["data"]
        require(detail["counts"] == EXPECTED and detail["result"]["verificationStatus"] == "VERIFIED_AT_EXECUTION", "Historical result/count mismatch")
        require(detail["sourceRevisions"] == request["sources"] and set(detail["inputArtifactIds"]) == set(request["batchArtifactIds"] + [request["serviceReferenceArtifactId"]]), "Captured source/input reference mismatch")
        attempts, _ = http(base, f"/api/management/v1/runs/{run}/attempts")
        require(len(attempts["data"]) == 1 and attempts["data"][0]["state"] == "SUCCEEDED", "Attempt did not succeed")
        attempt = attempts["data"][0]["attemptId"]
        stages, _ = http(base, f"/api/management/v1/runs/{run}/attempts/{attempt}/stages")
        require([(row["stage"], row["state"]) for row in stages["data"]] == [(name, "VERIFIED") for name in ("CHECK_INPUTS", "PUBLISH_RELIABILITY", "VERIFY_RESULT")], "Stage history mismatch")
        events, _ = http(base, f"/api/management/v1/runs/{run}/events?limit=100")
        require(any(row["action"] == "RUN_SUCCEEDED" for row in events["data"]), "Success audit missing")
        require(sql(self.state, "SELECT active_attempt_id IS NULL AND active_import_id IS NULL AND mode='READY' FROM ddip_management.coordinator;") == "t", "Coordinator was not released")
        self.command("independent-verification", [str(python), str(Path(__file__).resolve()), "_verify", str(ROOT), detail["result"]["nativeFingerprint"]])
        source_path = "/api/management/v1/sources/application-reliability/" + source["sourceId"]
        current, _ = http(base, source_path)
        revisions, _ = http(base, source_path + "/revisions")
        require(current["data"]["currentRevision"] == 1 and current["data"]["enabled"] and len(revisions["data"]) == 1, "Source revision history mismatch")
        sources, _ = http(base, "/api/management/v1/sources?limit=20")
        require(len(sources["data"]) == 1 and sources["data"][0]["currentRevision"] == 1, "Source list mismatch")
        self.env["CONTENT_API_PROXY_TARGET"] = base
        ui_process = self.launch("frontend", [shutil.which("node"), str(REPO / "frontend/node_modules/vite/bin/vite.js"), "preview", "--host", "127.0.0.1", "--port", str(self.args.ui_port), "--strictPort"], cwd=REPO / "frontend")
        def frontend_ready():
            require(ui_process.poll() is None, "Frontend exited; inspect frontend.log")
            try:
                with urllib.request.urlopen(ui, timeout=2) as response:
                    return response.status == 200
            except (OSError, urllib.error.URLError):
                return False
        wait_for("Frontend readiness", frontend_ready)
        runs, _ = http(ui, "/api/management/v1/runs?limit=20")
        require(any(row["runId"] == run for row in runs["data"]), "Frontend management proxy mismatch")
        compatible, _ = http(ui, f"/api/management/v1/artifacts?executionProfileId={PROFILE}&role=RELIABILITY_BATCH&sourceId={source['sourceId']}&limit=20")
        require(len(compatible["data"]) == 2, "Demo inputs must remain discoverable for the current source revision")
        publishers, _ = http(ui, "/api/v1/publishers")
        require(publishers["publication"] == prepared["publication"] and len(publishers["data"]) == 2, "Separate Analytics fixture mismatch")
        write_json(ROOT / "acceptance.json", dict(run=detail, attempts=attempts["data"], stages=stages["data"], events=events["data"], source=current["data"], revisions=revisions["data"], analyticsPublication=prepared["publication"], coordinatorReleased=True))
        self.phase("READY")
        print(f"Analytics: {ui}/#/analytics\nOperations: {ui}/#/operations/executions/{run}\nPrivate status/logs: {ROOT}\nStop: Ctrl-C, or python3 scripts/local-operations-demo.py down", flush=True)
        while True:
            for name, child in self.children:
                if name in ("api", "worker", "frontend"):
                    require(child.poll() is None, name + " exited; demo no longer ready")
            time.sleep(1)

    def cleanup(self):
        stopped = True
        for name, child in reversed(self.children):
            if child.poll() is None:
                child.terminate()
                try:
                    child.wait(timeout=60)
                except subprocess.TimeoutExpired:
                    stopped = False
                    print(name + " did not stop; infrastructure and artifacts preserved", flush=True)
            try:
                os.killpg(child.pid, 0)
                stopped = False
            except ProcessLookupError:
                pass
        if stopped:
            container = owned_container(self.state)
            if container:
                docker("rm", "--force", "--volumes", container)
        if ROOT.exists():
            self.phase("STOPPED" if stopped else "STOP_INCOMPLETE")
        return stopped


def status():
    if not ROOT.exists() and not ROOT.is_symlink():
        print(json.dumps(dict(phase="ABSENT", supervisorRunning=False, containerPresent=False), indent=2))
        return
    state = load_state()
    summary = {key: state[key] for key in ("phase", "failurePhase", "api", "ui", "runId", "timings") if key in state}
    summary["supervisorRunning"] = process_stamp(state["supervisor"]) == state["supervisorStamp"]
    summary["containerPresent"] = owned_container(state) is not None
    if summary["supervisorRunning"] and state.get("runId"):
        try:
            record, _ = http(state["api"], "/api/management/v1/runs/" + state["runId"])
            summary["runState"] = record["data"]["state"]
        except (RuntimeError, OSError, urllib.error.URLError):
            summary["httpReachable"] = False
    print(json.dumps(summary, indent=2))


def down():
    if not ROOT.exists():
        print("Demo is already absent")
        return
    state = load_state()
    stamp = process_stamp(state["supervisor"])
    if stamp == state["supervisorStamp"]:
        require("local-operations-demo.py" in stamp, "Supervisor command mismatch; refusing signal")
        os.kill(state["supervisor"], signal.SIGTERM)
        wait_for("Demo shutdown", lambda: process_stamp(state["supervisor"]) != stamp, 90)
        state = load_state()
    for item in state.get("processes", {}).values():
        require(process_stamp(item["pid"]) is None, "A recorded process/PID still exists; refusing deletion")
        try:
            os.killpg(item["pid"], 0)
        except ProcessLookupError:
            continue
        raise RuntimeError("A demo process group still exists; refusing deletion")
    container = owned_container(state)
    if container:
        docker("rm", "--force", "--volumes", container)
    shutil.rmtree(ROOT)
    print("Demo processes, PostgreSQL/volumes and private native data removed")


def main():
    os.umask(0o077)
    if len(sys.argv) > 2 and sys.argv[1] in ("_prepare", "_verify"):
        if sys.argv[1] == "_prepare":
            prepare(Path(sys.argv[2]))
        else:
            verify_native(Path(sys.argv[2]), sys.argv[3])
        return
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("up", "status", "down"))
    parser.add_argument("--offline", action="store_true", help="build using the existing Maven dependency cache")
    parser.add_argument("--api-port", type=int, default=18080)
    parser.add_argument("--ui-port", type=int, default=14173)
    args = parser.parse_args()
    if args.action == "status":
        status()
    elif args.action == "down":
        down()
    else:
        demo = Demo(args)
        def stop(_signum, _frame):
            raise KeyboardInterrupt
        signal.signal(signal.SIGTERM, stop)
        try:
            demo.up()
        except KeyboardInterrupt:
            print("Stopping demo", flush=True)
        except Exception:
            if demo.created and ROOT.exists() and (ROOT / "state.json").exists():
                demo.state["failurePhase"] = demo.state["phase"]
                demo.phase("FAILED")
            raise
        finally:
            if demo.created and ROOT.exists() and (ROOT / "state.json").exists() and load_state()["id"] == demo.state["id"]:
                demo.cleanup()


if __name__ == "__main__":
    main()
