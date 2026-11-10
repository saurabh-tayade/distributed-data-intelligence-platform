"""Lifecycle harness contracts; these mocks do not establish Docker acceptance."""
import contextlib
import io
import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import Mock, patch

SPEC = importlib.util.spec_from_file_location("managed_check", Path(__file__).resolve().parents[1] / "check-managed-demo.py")
check = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(check)


class ManagedDemoCheckTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name) / "demo"
        patcher = patch.object(check.demo, "ROOT", self.root)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_ready_requires_actual_success_and_all_services(self):
        record = dict(phase="READY", supervisorRunning=True, containerPresent=True, runState="SUCCEEDED")
        self.assertTrue(check.ready(record))
        for key in record:
            changed = record.copy()
            del changed[key]
            self.assertFalse(check.ready(changed))
        for value in ("FAILED", "RECOVERY_REQUIRED", "CANCELLED", "RUNNING"):
            self.assertFalse(check.ready(record | {"runState": value}))

    def test_existing_installation_not_adopted_or_stopped(self):
        self.root.mkdir()
        with patch.object(check.subprocess, "Popen") as launch, patch.object(check, "stop") as stop:
            with self.assertRaisesRegex(RuntimeError, "Existing"):
                check.cycle()
        launch.assert_not_called()
        stop.assert_not_called()

    def test_dangling_symlink_not_adopted(self):
        self.root.symlink_to(self.root.parent / "missing")
        with self.assertRaisesRegex(RuntimeError, "Existing"):
            check.cycle()

    def test_startup_process_exit_is_failure(self):
        with self.assertRaisesRegex(RuntimeError, "exited"):
            check.wait_ready(Mock(poll=Mock(return_value=1)))

    def test_startup_timeout_is_bounded(self):
        with patch.object(check.time, "monotonic", side_effect=[0, 901]):
            with self.assertRaisesRegex(RuntimeError, "timed out"):
                check.wait_ready(Mock())

    def test_failed_startup_phase_is_not_ready(self):
        self.root.mkdir()
        (self.root / "state.json").write_text("{}")
        with patch.object(check, "status", return_value={"phase": "STOP_INCOMPLETE"}):
            with self.assertRaisesRegex(RuntimeError, "failed or stopped"):
                check.wait_ready(Mock(poll=Mock(return_value=None)))

    def test_status_failure_does_not_expose_private_output(self):
        result = subprocess.CompletedProcess([], 1, "secret", "password")
        with patch.object(check.subprocess, "run", return_value=result):
            with self.assertRaises(RuntimeError) as error:
                check.status()
        self.assertNotIn("secret", str(error.exception))
        self.assertNotIn("password", str(error.exception))

    def test_shutdown_failure_is_not_force_cleaned(self):
        process = Mock(returncode=0)
        with patch.object(check.subprocess, "run", return_value=Mock(returncode=1)) as run:
            with self.assertRaisesRegex(RuntimeError, "Ownership-checked"):
                check.stop(process)
        self.assertEqual(run.call_count, 1)
        self.assertEqual(run.call_args.args[0][-1], "down")
        process.kill.assert_not_called()

    def test_volume_daemon_error_does_not_count_as_removed(self):
        result = subprocess.CompletedProcess([], 1, "", "Cannot connect to daemon")
        with patch.object(check.demo, "owned_container", return_value=None), patch.object(check.subprocess, "run", return_value=result):
            with self.assertRaisesRegex(RuntimeError, "volume removal"):
                check.verify_removed({}, ["owned-volume"])

    def test_remaining_directory_fails_teardown_verification(self):
        self.root.mkdir()
        with self.assertRaisesRegex(RuntimeError, "directory remains"):
            check.verify_removed({}, [])

    def test_foreign_marker_never_calls_down(self):
        process = Mock(pid=123, poll=Mock(return_value=None))
        def fail_ready(_process):
            self.root.mkdir()
            (self.root / "state.json").write_text("{}")
            raise RuntimeError("startup failed")
        with patch.object(check.subprocess, "Popen", return_value=process), patch.object(check, "wait_ready", side_effect=fail_ready), patch.object(check.demo, "load_state", return_value={"supervisor": 999}), patch.object(check, "stop") as stop:
            with self.assertRaisesRegex(RuntimeError, "another supervisor"):
                check.cycle()
        stop.assert_not_called()

    def test_pre_marker_failure_stops_only_direct_child(self):
        process = Mock(pid=123, poll=Mock(return_value=None))
        with patch.object(check.subprocess, "Popen", return_value=process), patch.object(check, "wait_ready", side_effect=RuntimeError("failed")), patch.object(check, "stop") as stop:
            with self.assertRaisesRegex(RuntimeError, "failed"):
                check.cycle()
        process.terminate.assert_called_once()
        process.wait.assert_called_once_with(timeout=90)
        stop.assert_not_called()

    def test_repeated_installation_identity_is_rejected(self):
        with patch.object(check, "cycle", return_value={"installationId": "same"}), patch.object(check.signal, "signal"):
            with self.assertRaisesRegex(RuntimeError, "reused"):
                check.main()

    def test_unknown_phase_is_not_copied_to_public_output(self):
        self.root.mkdir()
        (self.root / "state.json").write_text("{}")
        output = io.StringIO()
        with patch.object(check, "status", return_value={"phase": "private secret"}), patch.object(check.time, "monotonic", side_effect=[0, 1, 901]), patch.object(check.time, "sleep"), contextlib.redirect_stdout(output):
            with self.assertRaisesRegex(RuntimeError, "timed out"):
                check.wait_ready(Mock(poll=Mock(return_value=None)))
        self.assertIn("UNKNOWN", output.getvalue())
        self.assertNotIn("private secret", output.getvalue())

    def test_successful_cycle_checks_ownership_and_cleanup(self):
        process = Mock(pid=123, poll=Mock(return_value=None))
        state = {"supervisor": 123, "id": "owned-id"}
        def become_ready(_process):
            self.root.mkdir()
            (self.root / "state.json").write_text("{}")
            return {"runState": "SUCCEEDED"}
        info = json.dumps([{"Mounts": [{"Type": "volume", "Name": "owned-volume"}]}])
        with patch.object(check.subprocess, "Popen", return_value=process), patch.object(check, "wait_ready", side_effect=become_ready), patch.object(check.demo, "load_state", return_value=state), patch.object(check.demo, "owned_container", return_value="owned-container"), patch.object(check.demo, "docker", return_value=info), patch.object(check, "stop") as stop, patch.object(check, "verify_removed") as removed:
            result = check.cycle()
        stop.assert_called_once_with(process)
        removed.assert_called_once_with(state, ["owned-volume"])
        self.assertEqual(result, {"installationId": "owned-id", "runState": "SUCCEEDED", "cleanup": "verified"})

    def test_workflow_is_manual_read_only_and_does_not_upload_private_files(self):
        workflow = (check.REPO / ".github/workflows/managed-demo.yml").read_text()
        self.assertIn("workflow_dispatch:", workflow)
        self.assertNotIn("pull_request:", workflow)
        self.assertNotIn("push:", workflow)
        self.assertIn("contents: read", workflow)
        self.assertNotIn("upload-artifact", workflow)
        self.assertIn("persist-credentials: false", workflow)
        self.assertIn("timeout-minutes: 40", workflow)


if __name__ == "__main__":
    unittest.main()
