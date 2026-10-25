#!/usr/bin/env python3
"""Disposable local C3 recovery regression with actual PostgreSQL restarts.

Requires Docker, Maven, JAVA_HOME (JDK 21), DDIP_TEST_PYTHON (pinned native environment).
No existing database/container is used. Generated signals live in a temporary directory.
"""
import argparse
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import threading
import time
import uuid

IMAGE = "postgres:17.11-bookworm@sha256:3645570cccdfa447589da9f57dd740faa29b30938e861289a5574b6ca6b03826"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    scope = parser.add_mutually_exclusive_group()
    scope.add_argument("--full", action="store_true", help="run every content-api unit/PostgreSQL suite")
    scope.add_argument("--operator-only", action="store_true", help="run packaged operator acceptance and its actual database restart")
    parser.add_argument("--offline", action="store_true", help="use the already populated Maven cache")
    args = parser.parse_args()
    root = Path(__file__).resolve().parent.parent
    env = os.environ.copy()
    python = Path(env["DDIP_TEST_PYTHON"])
    java = Path(env["JAVA_HOME"]) / "bin/java"
    assert python.is_absolute() and java.is_absolute()
    subprocess.run([str(python), "-c", "import sys,pyspark,duckdb; assert sys.version_info[:3] == (3,12,15); assert pyspark.__version__ == '4.0.2'; assert duckdb.__version__ == '1.4.5'"], check=True)
    version = subprocess.run([str(java), "-version"], capture_output=True, text=True, check=True)
    assert 'version "21.' in version.stderr
    env.update(PYSPARK_PYTHON=str(python), PYSPARK_DRIVER_PYTHON=str(python), SPARK_LOCAL_IP="127.0.0.1", TZ="UTC")
    name = "ddip-recovery-" + uuid.uuid4().hex[:12]
    password = uuid.uuid4().hex
    stop = threading.Event()
    errors = []
    restarted = []

    def docker(*arguments, capture=False):
        return subprocess.run(["docker", *arguments], check=True, text=True,
                              stdout=subprocess.PIPE if capture else subprocess.DEVNULL,
                              stderr=subprocess.PIPE, timeout=60)

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
    try:
        # Docker can reallocate an automatically assigned host port on restart. Keep an explicit port.
        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0))
            port = reservation.getsockname()[1]
        docker("run", "--detach", "--name", name, "--publish", f"127.0.0.1:{port}:5432",
               "--env", "POSTGRES_DB=ddip_test", "--env", "POSTGRES_USER=ddip_test",
               "--env", "POSTGRES_PASSWORD=" + password, IMAGE)
        ready()
        env.update(DDIP_TEST_DB_URL=f"jdbc:postgresql://127.0.0.1:{port}/ddip_test",
                   DDIP_TEST_DB_USER="ddip_test", DDIP_TEST_DB_PASSWORD=password)
        with tempfile.TemporaryDirectory(prefix="ddip-recovery-") as temporary:
            phases = ("operator",) if args.operator_only else ("bootstrap", "verification", "finalization", "operator")
            signals = {phase: Path(temporary) / phase for phase in phases}

            def controller():
                try:
                    while not stop.wait(0.1):
                        for phase, signal in signals.items():
                            if phase not in restarted and signal.exists():
                                docker("restart", name)
                                ready()
                                restarted.append(phase)
                                signal.with_name(signal.name + ".done").write_text("RESTARTED\n")
                                print("Actual PostgreSQL restart:", phase, flush=True)
                except Exception as error:
                    errors.append(type(error).__name__)

            thread = threading.Thread(target=controller, daemon=True)
            thread.start()
            command = ["mvn", "-q", "-Daether.syncContext.named.factory=noop", "-f", "services/content-api/pom.xml", "-Ppostgres"]
            if args.offline:
                command.append("-o")
            if args.operator_only:
                command += ["-Dtest=RuntimeRoleTest", "-Dit.test=OperatorRecoveryIT"]
            elif not args.full:
                command += ["-Dtest=RuntimeRoleTest,RecoveryQuiescenceTest,VerifierProcessTest,RunInspectionTest",
                            "-Dit.test=OperatorRecoveryIT,RecoveryHandoffIT,RecoveryVerificationIT,RecoveryFinalizationIT"]
            command += [f"-Dddip.test.{phase}-restart-signal={path}" for phase, path in signals.items()]
            command.append("verify")
            result = subprocess.run(command, cwd=root, env=env)
            stop.set()
            thread.join(timeout=60)
            assert result.returncode == 0, "Maven regression failed"
            assert not errors and len(restarted) == len(signals), f"Restart controller incomplete: {restarted}, {errors}"
    finally:
        stop.set()
        docker("rm", "--force", "--volumes", name)
        print(f"Total local regression seconds: {time.monotonic() - started:.2f}", flush=True)


if __name__ == "__main__":
    main()
