#!/usr/bin/env python3
"""D1 acceptance against an owned disposable PostgreSQL container, including actual restart.

Requires Docker, Maven and JAVA_HOME (JDK 21). No publisher, S3, Python/Spark or existing DB is used.
"""
import argparse
import os
import re
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import threading
import time
import uuid

IMAGE = "postgres:17.11-bookworm@sha256:3645570cccdfa447589da9f57dd740faa29b30938e861289a5574b6ca6b03826"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--offline", action="store_true", help="use populated Maven cache")
    parser.add_argument("--regression", action="store_true", help="also run all content-api units and existing persistence/worker/control integration suites")
    parser.add_argument("--case", help="run one RssFetchIT method during development")
    args = parser.parse_args()
    if args.case and (args.regression or not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]*", args.case)):
        parser.error("--case requires a single method name and cannot be combined with --regression")
    root = Path(__file__).resolve().parent.parent
    env = os.environ.copy()
    # Existing negative worker-startup tests need an absolute executable path. They never run
    # Python or claim native readiness; a supplied pinned environment still takes precedence.
    env.setdefault("DDIP_TEST_PYTHON", str(Path(sys.executable).resolve()))
    java = Path(env["JAVA_HOME"]) / "bin/java"
    assert java.is_absolute()
    version = subprocess.run([str(java), "-version"], capture_output=True, text=True, check=True)
    assert 'version "21.' in version.stderr
    name = "ddip-rss-d1-" + uuid.uuid4().hex[:12]
    password = uuid.uuid4().hex
    stop = threading.Event()
    errors = []
    restarted = []

    def docker(*arguments):
        return subprocess.run(["docker", *arguments], check=True, text=True,
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=60)

    def ready():
        deadline = time.monotonic() + 45
        while time.monotonic() < deadline:
            try:
                docker("exec", name, "pg_isready", "-h", "127.0.0.1", "-U", "ddip_test", "-d", "ddip_test")
                return
            except subprocess.CalledProcessError:
                time.sleep(0.2)
        raise RuntimeError("Disposable PostgreSQL readiness timed out")

    started = time.monotonic()
    created = False
    thread = None
    try:
        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0))
            port = reservation.getsockname()[1]
        docker("run", "--detach", "--name", name, "--publish", f"127.0.0.1:{port}:5432",
               "--env", "POSTGRES_DB=ddip_test", "--env", "POSTGRES_USER=ddip_test",
               "--env", "POSTGRES_PASSWORD=" + password, IMAGE)
        created = True
        ready()
        env.update(DDIP_TEST_DB_URL=f"jdbc:postgresql://127.0.0.1:{port}/ddip_test",
                   DDIP_TEST_DB_USER="ddip_test", DDIP_TEST_DB_PASSWORD=password, TZ="UTC")
        with tempfile.TemporaryDirectory(prefix="ddip-rss-d1-") as temporary:
            signal = Path(temporary) / "rss-restart"

            def controller():
                try:
                    while not stop.wait(0.1):
                        if not restarted and signal.exists():
                            docker("restart", name)
                            ready()
                            restarted.append("rss")
                            signal.with_name(signal.name + ".done").write_text("RESTARTED\n")
                            print("Actual PostgreSQL restart: rss", flush=True)
                except Exception as error:
                    errors.append(type(error).__name__)

            thread = threading.Thread(target=controller, daemon=True)
            thread.start()
            maven = ["mvn", "-B", "-ntp", "-Daether.syncContext.named.factory=noop"]
            if args.offline:
                maven.append("-o")
            subprocess.run(maven + ["-f", "ingestion/pom.xml", "-DskipTests", "package"], cwd=root, env=env, check=True)
            command = maven + ["-f", "services/content-api/pom.xml", "-Ppostgres"]
            if args.regression:
                command += ["-Dit.test=RssFetchIT,PersistenceIT,WorkerCoordinationIT,ControlAdmissionIT"]
            else:
                command += ["-Dtest=RuntimeRoleTest,WorkerCapabilityTest,InstallationHostGuardTest,VerifierProcessTest",
                            "-Dit.test=RssFetchIT" + ("#" + args.case if args.case else "")]
            command += [f"-Dddip.test.rss-restart-signal={signal}", "verify"]
            result = subprocess.run(command, cwd=root, env=env)
            assert result.returncode == 0, "D1 acceptance failed"
            expected_restart = ["rss"] if args.case in (None, "checkpointSurvivesActualDatabaseRestart") else []
            assert not errors and restarted == expected_restart, f"Restart controller incomplete: {restarted}, {errors}"
    finally:
        stop.set()
        if thread is not None:
            thread.join(timeout=60)
        if created:
            docker("rm", "--force", "--volumes", name)
        print(f"D1 local elapsed seconds: {time.monotonic() - started:.2f}", flush=True)


if __name__ == "__main__":
    main()
