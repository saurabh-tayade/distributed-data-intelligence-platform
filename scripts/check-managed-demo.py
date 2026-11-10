#!/usr/bin/env python3
"""Bounded offline-fixture demo lifecycle check; never upload private demo data."""
import importlib.util
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time

REPO = Path(__file__).resolve().parent.parent
SPEC = importlib.util.spec_from_file_location("operations_demo", REPO / "scripts/local-operations-demo.py")
demo = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(demo)
LAUNCHER = str(REPO / "scripts/local-operations-demo.py")


class CheckFailure(RuntimeError):
    """An allowlisted harness diagnostic, never raw process/configuration output."""


def require(condition, message):
    if not condition:
        raise CheckFailure(message)


def status():
    result = subprocess.run([sys.executable, LAUNCHER, "status"], cwd=REPO,
                            capture_output=True, text=True, timeout=30)
    require(result.returncode == 0, "Demo status failed; private output was not published")
    return json.loads(result.stdout)


def ready(record):
    return (record.get("phase") == "READY" and record.get("supervisorRunning") is True
            and record.get("containerPresent") is True and record.get("runState") == "SUCCEEDED")


def wait_ready(process, timeout=900):
    deadline = time.monotonic() + timeout
    last_phase = None
    while time.monotonic() < deadline:
        require(process.poll() is None, "Demo exited before readiness")
        if (demo.ROOT / "state.json").is_file():
            record = status()
            phase = record.get("phase")
            if phase != last_phase:
                # Restrict public diagnostics to launcher-defined phase constants.
                allowed = {"BUILDING", "PREPARING_SYNTHETIC_FIXTURES", "STARTING_POSTGRES",
                           "MIGRATING_AND_PROVISIONING_REFERENCES", "STARTING_API",
                           "IMPORTING_NATIVE_INPUTS", "STARTING_WORKER", "EXECUTING_RELIABILITY",
                           "VERIFYING_HTTP_AND_NATIVE_RESULT", "READY", "FAILED", "STOPPED",
                           "STOP_INCOMPLETE"}
                print("Demo phase: " + (phase if phase in allowed else "UNKNOWN"), flush=True)
                last_phase = phase
            require(record.get("phase") not in ("FAILED", "STOPPED", "STOP_INCOMPLETE"),
                    "Demo reported failed or stopped startup")
            if ready(record):
                return record
        time.sleep(2)
    raise CheckFailure("Demo readiness timed out")


def stop(process):
    # Reap the supervisor concurrently: down waits for its PID to disappear.
    reaper = threading.Thread(target=process.wait, daemon=True)
    reaper.start()
    result = subprocess.run([sys.executable, LAUNCHER, "down"], cwd=REPO,
                            capture_output=True, text=True, timeout=150)
    require(result.returncode == 0, "Ownership-checked demo shutdown failed; data was not force-deleted")
    reaper.join(timeout=5)
    require(not reaper.is_alive(), "Demo supervisor remains alive after shutdown")
    require(process.returncode == 0, "Demo supervisor failed during shutdown")


def verify_removed(state, volumes):
    require(not demo.ROOT.exists() and not demo.ROOT.is_symlink(), "Private demo directory remains")
    require(demo.owned_container(state) is None, "Owned demo container remains")
    for volume in volumes:
        result = subprocess.run(["docker", "volume", "inspect", volume], capture_output=True,
                                text=True, timeout=30)
        require(result.returncode != 0 and "no such volume" in result.stderr.lower(),
                "Cannot prove disposable volume removal")
    for item in state.get("processes", {}).values():
        require(demo.process_stamp(item["pid"]) is None, "Recorded process still exists")
    for port in (18080, 14173):
        with socket.socket() as connection:
            connection.settimeout(2)
            require(connection.connect_ex(("127.0.0.1", port)) != 0, "Demo listener remains")
    require(status() == {"phase": "ABSENT", "supervisorRunning": False, "containerPresent": False},
            "Demo did not report absent")


def cycle():
    # Never adopt or clean up somebody else's installation, including a symlink.
    require(not demo.ROOT.exists() and not demo.ROOT.is_symlink(), "Existing demo installation; refusing lifecycle check")
    with tempfile.TemporaryFile() as private_log:
        process = subprocess.Popen([sys.executable, LAUNCHER, "up"], cwd=REPO,
                                   stdout=private_log, stderr=subprocess.STDOUT)
        try:
            record = wait_ready(process)
            state = demo.load_state()
            require(state["supervisor"] == process.pid, "Supervisor ownership mismatch")
            container = demo.owned_container(state)
            require(container is not None, "Owned demo container missing")
            info = json.loads(demo.docker("inspect", container))[0]
            volumes = [mount["Name"] for mount in info.get("Mounts", []) if mount["Type"] == "volume"]
            require(bool(volumes), "Disposable PostgreSQL volume missing")
        finally:
            # Only the launched supervisor's marker grants cleanup authority.
            if (demo.ROOT / "state.json").is_file():
                saved = demo.load_state()
                require(saved["supervisor"] == process.pid, "Refusing cleanup of another supervisor")
                stop(process)
            else:
                # This is our unreaped direct child, not a PID loaded from a file.
                # Stop it without deleting anything when startup has no marker yet.
                if process.poll() is None:
                    process.terminate()
                process.wait(timeout=90)
        verify_removed(state, volumes)
        return {"installationId": state["id"], "runState": record["runState"], "cleanup": "verified"}


def main():
    os.umask(0o077)
    def interrupted(_signal, _frame):
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, interrupted)
    results = [cycle(), cycle()]
    require(results[0]["installationId"] != results[1]["installationId"], "Second launch reused installation")
    print(json.dumps({"result": "passed", "cycles": results,
                      "scope": "offline-fixture managed demo startup, HTTP/native verification and fresh teardown/relaunch",
                      "excluded": ["SeaweedFS persistence", "database restart persistence", "manual browser acceptance", "public-feed acceptance"]}, indent=2))


if __name__ == "__main__":
    try:
        main()
    except (Exception, KeyboardInterrupt) as error:
        # Never copy captured launcher/configuration output to public Actions logs.
        detail = str(error) if isinstance(error, CheckFailure) else type(error).__name__
        print("Managed demo lifecycle check failed: " + detail, file=sys.stderr)
        sys.exit(1)
