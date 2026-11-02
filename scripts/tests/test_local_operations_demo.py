"""Focused lifecycle safety checks; actual native/HTTP acceptance is run separately."""
import importlib.util
import io
import json
import os
from pathlib import Path
import signal
import subprocess
import tempfile
import unittest
from unittest.mock import Mock, patch
import uuid

SPEC = importlib.util.spec_from_file_location("operations_demo", Path(__file__).resolve().parents[1] / "local-operations-demo.py")
demo = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(demo)


class DemoSafetyTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve() / "demo"
        self.root_patch = patch.object(demo, "ROOT", self.root)
        self.root_patch.start()
        self.addCleanup(self.root_patch.stop)
        self.process_patch = patch.object(demo, "process_stamp", return_value="test local-operations-demo.py up")
        self.process_patch.start()
        self.addCleanup(self.process_patch.stop)
        self.state = dict(id=str(uuid.uuid4()), root=str(self.root), supervisor=1234, supervisorStamp="original local-operations-demo.py up", processes={}, timings={})

    def save(self):
        self.root.mkdir(mode=0o700, exist_ok=True)
        demo.write_json(self.root / "state.json", self.state)

    def test_existing_directory_is_never_overwritten_even_on_failed_up(self):
        self.save()
        original = (self.root / "state.json").read_bytes()
        with patch.object(demo.sys, "argv", [str(Path(demo.__file__)), "up"]), patch.object(signal, "signal"), patch.object(demo, "docker") as docker:
            with self.assertRaisesRegex(RuntimeError, "already exists"):
                demo.main()
        self.assertEqual((self.root / "state.json").read_bytes(), original)
        docker.assert_not_called()

    def test_symlink_root_and_symlink_ancestor_are_rejected(self):
        outside = self.root.parent / "retained"
        outside.mkdir()
        self.root.symlink_to(outside, target_is_directory=True)
        with self.assertRaisesRegex(RuntimeError, "symlinks"):
            demo.Demo(Mock()).up()
        self.assertEqual(list(outside.iterdir()), [])

    def test_marker_permissions_and_scope_are_validated(self):
        self.save()
        self.assertEqual(demo.load_state(), self.state)
        self.root.chmod(0o755)
        with self.assertRaisesRegex(RuntimeError, "private"):
            demo.load_state()
        self.root.chmod(0o700)
        demo.write_json(self.root / "state.json", self.state | {"root": "/retained"})
        with self.assertRaisesRegex(RuntimeError, "ownership marker"):
            demo.load_state()

    def test_foreign_container_is_not_removed(self):
        self.state["container"] = "a" * 64
        self.save()
        info = [{"Id": self.state["container"], "Config": {"Labels": {demo.LABEL: "someone-else"}}}]
        with patch.object(demo.subprocess, "run", return_value=subprocess.CompletedProcess([], 0, json.dumps(info), "")), patch.object(demo, "docker") as docker:
            with self.assertRaisesRegex(RuntimeError, "ownership mismatch"):
                demo.owned_container(self.state)
            docker.assert_not_called()

    def test_docker_outage_does_not_imply_container_absence(self):
        self.state["container"] = "a" * 64
        for message in ("daemon unavailable", "dial unix docker.sock: connect: no such file or directory"):
            with patch.object(demo.subprocess, "run", return_value=subprocess.CompletedProcess([], 1, "", message)):
                with self.assertRaisesRegex(RuntimeError, "preserved"):
                    demo.owned_container(self.state)

    def test_missing_container_diagnostic_is_case_insensitive(self):
        self.state["container"] = "a" * 64
        for message in ("error: no such object: id", "Error: No such container: id"):
            with patch.object(demo.subprocess, "run", return_value=subprocess.CompletedProcess([], 1, "[]", message)):
                self.assertIsNone(demo.owned_container(self.state))

    def test_labelled_container_can_be_found_after_lost_creation_response(self):
        self.state["containerName"] = "ddip-demo-" + self.state["id"]
        info = [{"Id": "a" * 64, "Config": {"Labels": {demo.LABEL: self.state["id"]}}}]
        with patch.object(demo.subprocess, "run", return_value=subprocess.CompletedProcess([], 0, json.dumps(info), "")):
            self.assertEqual(demo.owned_container(self.state), "a" * 64)

    def test_external_service_and_jvm_configuration_is_not_inherited(self):
        with patch.dict(demo.os.environ, {"DDIP_DB_URL": "retained", "SPRING_CONFIG_LOCATION": "retained", "ANALYTICS_GOLD_ROOT": "retained", "JAVA_TOOL_OPTIONS": "-Dddip.database.url=retained"}):
            environment = demo.Demo(Mock()).env
        for name in ("DDIP_DB_URL", "SPRING_CONFIG_LOCATION", "ANALYTICS_GOLD_ROOT", "JAVA_TOOL_OPTIONS"):
            self.assertNotIn(name, environment)

    def test_reused_supervisor_pid_is_never_signalled(self):
        self.save()
        with patch.object(demo, "process_stamp", return_value="different process"), patch.object(demo.os, "kill") as kill, patch.object(demo, "owned_container", return_value=None):
            demo.down()
            kill.assert_not_called()
        self.assertFalse(self.root.exists())

    def test_live_recorded_child_blocks_deletion(self):
        self.state["processes"] = {"worker": dict(pid=99, stamp="original")}
        self.save()
        with patch.object(demo, "process_stamp", side_effect=[None, "possibly-reused-pid"]), patch.object(demo, "owned_container") as owned:
            with self.assertRaisesRegex(RuntimeError, "refusing deletion"):
                demo.down()
            owned.assert_not_called()
        self.assertTrue(self.root.exists())

    def test_orphaned_process_group_blocks_deletion(self):
        self.state["processes"] = {"worker": dict(pid=99, stamp="original")}
        self.save()
        with patch.object(demo, "process_stamp", return_value=None), patch.object(demo.os, "killpg"), patch.object(demo, "owned_container") as owned:
            with self.assertRaisesRegex(RuntimeError, "process group"):
                demo.down()
            owned.assert_not_called()
        self.assertTrue(self.root.exists())

    def test_successful_down_is_repeatable(self):
        self.save()
        with patch.object(demo, "process_stamp", return_value=None), patch.object(demo, "owned_container", return_value="owned-id"), patch.object(demo, "docker") as docker:
            demo.down()
            demo.down()
            docker.assert_called_once_with("rm", "--force", "--volumes", "owned-id")

    def test_status_reports_absent_without_contacting_services(self):
        with patch.object(demo.sys, "stdout", new_callable=io.StringIO) as output, patch.object(demo, "docker") as docker:
            demo.status()
        self.assertEqual(json.loads(output.getvalue())["phase"], "ABSENT")
        docker.assert_not_called()

    def test_cleanup_preserves_database_when_worker_does_not_stop(self):
        self.save()
        instance = demo.Demo(Mock())
        instance.state = self.state
        worker = Mock(pid=99)
        worker.poll.return_value = None
        worker.wait.side_effect = subprocess.TimeoutExpired("worker", 60)
        instance.children = [("worker", worker)]
        with patch.object(demo.os, "killpg"), patch.object(demo, "owned_container") as owned:
            self.assertFalse(instance.cleanup())
            owned.assert_not_called()
        worker.terminate.assert_called_once()
        self.assertEqual(json.loads((self.root / "state.json").read_text())["phase"], "STOP_INCOMPLETE")

    def test_management_http_uses_real_origin_and_mutation_headers(self):
        response = Mock(status=201, headers={"Cache-Control": "no-store"})
        response.read.return_value = b'{"apiVersion":1,"data":{},"meta":{}}'
        response.__enter__ = Mock(return_value=response)
        response.__exit__ = Mock(return_value=False)
        with patch.object(demo.urllib.request, "urlopen", return_value=response) as opened:
            demo.http("http://127.0.0.1:18080", "/api/management/v1/sources", {"enabled": True}, expected=201)
        request = opened.call_args.args[0]
        self.assertEqual(request.get_header("Origin"), "http://127.0.0.1:18080")
        self.assertEqual(request.get_header("X-ddip-local-request"), "1")
        self.assertEqual(request.get_method(), "POST")
        self.assertEqual(json.loads(request.data), {"enabled": True})

    def test_http_error_never_copies_private_server_body(self):
        error = demo.urllib.error.HTTPError("http://local", 503, "private credential", {}, io.BytesIO(b"PRIVATE"))
        with patch.object(demo.urllib.request, "urlopen", side_effect=error):
            with self.assertRaisesRegex(RuntimeError, "HTTP 503") as failure:
                demo.http("http://127.0.0.1:18080", "/api/management/v1/runs")
        self.assertNotIn("credential", str(failure.exception))
        self.assertNotIn("PRIVATE", str(failure.exception))


if __name__ == "__main__":
    unittest.main()
