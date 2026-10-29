#!/usr/bin/env python3
"""Managed RSS/Bronze acceptance with disposable PostgreSQL and optional real SeaweedFS.

Requires Docker, Maven and JAVA_HOME (JDK 21). Uses synthetic HTTP and owned local storage only;
no live publisher, AWS or existing database is used. --silver runs actual pinned Spark.
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
SEAWEED = "chrislusf/seaweedfs:4.48@sha256:4e61d15fd35994cb1e43e1e553dff106794841fd9a99ade2fc8c8bfce4d7872d"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--offline", action="store_true", help="use populated Maven cache")
    parser.add_argument("--regression", action="store_true", help="also run all content-api units and existing persistence/worker/control integration suites")
    parser.add_argument("--case", help="run one RssFetchIT (or --bronze RssBronzeIT) method during development")
    parser.add_argument("--bronze", action="store_true", help="D2 acceptance with actual disposable SeaweedFS")
    parser.add_argument("--silver", action="store_true", help="D3 acceptance with actual disposable SeaweedFS and pinned Spark")
    args = parser.parse_args()
    if args.silver:
        args.bronze = True
        assert os.environ.get("DDIP_TEST_PYTHON"), "--silver requires the absolute pinned Python executable"
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
    seaweed_created = False
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
        if args.bronze:
            with socket.socket() as reservation:
                reservation.bind(("127.0.0.1", 0))
                s3_port = reservation.getsockname()[1]
            access, secret = uuid.uuid4().hex, uuid.uuid4().hex
            docker("run", "--detach", "--name", name + "-seaweed", "--hostname", "seaweedfs",
                   "--publish", f"127.0.0.1:{s3_port}:8333", "--env", "AWS_ACCESS_KEY_ID=" + access,
                   "--env", "AWS_SECRET_ACCESS_KEY=" + secret, "--env", "S3_BUCKET=ddip-d2", SEAWEED,
                   "mini", "-dir=/data", "-ip=seaweedfs", "-s3.port=8333", "-s3.port.iceberg=0",
                   "-s3.port.lance=0", "-webdav=false", "-admin.ui=false", "-s3.autoCreateBucket=false",
                   "-s3.allowDeleteBucketNotEmpty=false")
            seaweed_created = True
            env.update(BRONZE_S3_ENDPOINT=f"http://127.0.0.1:{s3_port}", BRONZE_S3_REGION="us-east-1",
                       BRONZE_S3_BUCKET="ddip-d2", BRONZE_S3_ACCESS_KEY=access, BRONZE_S3_SECRET_KEY=secret)
        with tempfile.TemporaryDirectory(prefix="ddip-rss-d1-") as temporary:
            signal = Path(temporary) / "rss-restart"
            bronze_signal = Path(temporary) / "bronze-restart"

            def controller():
                try:
                    while not stop.wait(0.1):
                        for phase, request in (("rss", signal), ("bronze", bronze_signal)):
                            if phase not in restarted and request.exists():
                                docker("restart", name)
                                ready()
                                restarted.append(phase)
                                request.with_name(request.name + ".done").write_text("RESTARTED\n")
                                print("Actual PostgreSQL restart: " + phase, flush=True)
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
                command += ["-Dit.test=RssFetchIT,PersistenceIT,WorkerCoordinationIT,ControlAdmissionIT" + (",RssBronzeIT" if args.bronze else "") + (",RssSilverIT" if args.silver else "")]
            else:
                command += ["-Dtest=RuntimeRoleTest,WorkerCapabilityTest,InstallationHostGuardTest,VerifierProcessTest",
                            "-Dit.test=" + ("RssSilverIT" if args.silver else "RssBronzeIT" if args.bronze else "RssFetchIT") + ("#" + args.case if args.case else "")]
            command += [f"-Dddip.test.rss-restart-signal={signal}", f"-Dddip.test.bronze-restart-signal={bronze_signal}", "verify"]
            result = subprocess.run(command, cwd=root, env=env)
            assert result.returncode == 0, "Managed RSS/Bronze acceptance failed"
            expected_restart = ({"rss", "bronze"} if args.regression and args.bronze else set() if args.silver else {"bronze" if args.bronze else "rss"}) if args.case in (None, "checkpointSurvivesActualDatabaseRestart") else set()
            assert not errors and set(restarted) == expected_restart, f"Restart controller incomplete: {restarted}, {errors}"
    finally:
        stop.set()
        if thread is not None:
            thread.join(timeout=60)
        if created:
            docker("rm", "--force", "--volumes", name)
        if seaweed_created:
            docker("rm", "--force", "--volumes", name + "-seaweed")
        print(f"Managed RSS/Bronze local elapsed seconds: {time.monotonic() - started:.2f}", flush=True)


if __name__ == "__main__":
    main()
