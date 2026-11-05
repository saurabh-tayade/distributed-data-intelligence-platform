"""Focused lifecycle safety checks; actual native/HTTP acceptance is run separately."""
import importlib.util
from copy import deepcopy
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
        inherited = {"DDIP_DB_URL": "retained", "SPRING_CONFIG_LOCATION": "retained", "ANALYTICS_GOLD_ROOT": "retained",
                     "JAVA_TOOL_OPTIONS": "-Dddip.database.url=retained", "JDK_JAVA_OPTIONS": "retained", "_JAVA_OPTIONS": "retained",
                     "SERVER_PORT": "9999", "RELIABILITY_ANALYTICS_ROOT": "/retained/output",
                     "RELIABILITY_ANALYTICS_BATCH_ROOT": "/retained/batches", "RELIABILITY_ANALYTICS_REFERENCE_ROOT": "/retained/reference",
                     "RELIABILITY_ANALYTICS_PUBLICATION": "f" * 64, "RELIABILITY_ANALYTICS_PYTHON_EXECUTABLE": "/retained/python",
                     "RELIABILITY_ANALYTICS_PYTHONEXECUTABLE": "/retained/python", "RELIABILITY_ANALYTICS_PROCESSING_ROOT": "/retained/code",
                     "RELIABILITY_ANALYTICS_FUTURE_OPTION": "retained",
                     **{"CONTENT_ANALYTICS_" + suffix: "retained" for suffix in (
                         "ROOT", "GOLD_ROOT", "GOLDROOT", "REFERENCE_ROOT", "REFERENCEROOT", "PUBLICATION",
                         "PYTHON_EXECUTABLE", "PYTHONEXECUTABLE", "PROCESSING_ROOT", "PROCESSINGROOT", "FUTURE_OPTION")}}
        with patch.dict(demo.os.environ, inherited | {"PATH": "/approved/bin", "JAVA_HOME": "/approved/jdk", "UNRELATED": "kept"}):
            environment = demo.Demo(Mock()).env
            self.assertEqual({name: demo.os.environ[name] for name in inherited}, inherited)
        for name in inherited:
            self.assertNotIn(name, environment)
        for name, value in {"PATH": "/approved/bin", "JAVA_HOME": "/approved/jdk", "UNRELATED": "kept"}.items():
            self.assertEqual(environment[name], value)

    def selection(self):
        root = self.root / "reliability-analytics"
        return dict(root=str(root / "output"), batchRoot=str(root / "batches"), referenceRoot=str(root / "references"), publication="a" * 64)

    def test_reliability_arguments_pin_every_operator_setting(self):
        selected = self.selection()
        self.assertEqual(demo.reliability_arguments("/approved/python", selected), [
            "--reliability.analytics.python-executable=/approved/python",
            "--reliability.analytics.processing-root=" + str(demo.REPO / "processing"),
            "--reliability.analytics.root=" + selected["root"],
            "--reliability.analytics.batch-root=" + selected["batchRoot"],
            "--reliability.analytics.reference-root=" + selected["referenceRoot"],
            "--reliability.analytics.publication=" + selected["publication"]])

    def test_missing_invalid_or_external_reliability_selection_is_rejected(self):
        for identity in (None, "", "latest", "A" * 64, "../publication", "a" * 63, 1):
            with self.subTest(identity=identity), self.assertRaisesRegex(RuntimeError, "selected Reliability publication"):
                demo.reliability_arguments("/approved/python", self.selection() | {"publication": identity})
        for key in ("root", "batchRoot", "referenceRoot"):
            with self.subTest(key=key), self.assertRaisesRegex(RuntimeError, "isolated demo fixture"):
                demo.reliability_arguments("/approved/python", self.selection() | {key: "/retained"})

    def analytics_evidence(self):
        self.save()
        native = dict(product_id="application-reliability", product_contract_version=2, processor_revision=2,
                      input_format="JSONL", origin="synthetic", label="Synthetic request events — not production reliability.",
                      product_fingerprint="a" * 64, reference=dict(reference_id="bench-services", revision="r1", snapshot_fingerprint="b" * 64),
                      filters=dict(start=demo.RELIABILITY_START, end=demo.RELIABILITY_END, service_id=None, environment=None),
                      rows=[dict(zip(demo.RELIABILITY_ROWS, [demo.RELIABILITY_START, "svc00", "test", "2", "1", 0.5, "30", 15.0, "20",
                                                          "Service 00", None, None, "MATCHED"]))], limit="1", offset="0", has_more=True)
        response = demo.reliability_response(native)
        scenario = dict(report="requests_5m", endpoint="request-windows", name="first-page", query=dict(start=demo.RELIABILITY_START, end=demo.RELIABILITY_END, limit=1, offset=0), response=response)
        demo.write_json(self.root / "reliability-analytics-verification.json", dict(selection=self.selection(), scenarios=[scenario],
            inventory=demo.selection_inventory(self.selection()), unchangedFiles=0))
        return response

    def test_reliability_http_matches_exact_native_projection_and_query(self):
        response = self.analytics_evidence()
        with patch.object(demo, "http", return_value=(response, {"Cache-Control": "no-store"})) as http:
            result = demo.verify_reliability_http("http://127.0.0.1:14173", self.selection())
        self.assertEqual(result["publication"], "a" * 64)
        self.assertEqual(result["scenarios"][0]["response"]["data"][0]["requestCount"], "2")
        self.assertIsNone(result["scenarios"][0]["response"]["data"][0]["ownerTeam"])
        base, path = http.call_args.args
        self.assertEqual(base, "http://127.0.0.1:14173")
        self.assertEqual(demo.urllib.parse.parse_qs(demo.urllib.parse.urlsplit(path).query),
                         dict(start=[demo.RELIABILITY_START], end=[demo.RELIABILITY_END], limit=["1"], offset=["0"]))

    def test_reliability_http_rejects_switched_publication_types_rows_and_cache(self):
        response = self.analytics_evidence()
        invalid = [response | {"publication": "f" * 64}, response | {"data": []}, response | {"origin": "production"},
                   response | {"meta": response["meta"] | {"hasMore": 1}}, response | {"reference": response["reference"] | {"revision": "r2"}}]
        changed = deepcopy(response)
        changed["data"][0]["requestCount"] = 2
        invalid.append(changed)
        for payload in invalid:
            with self.subTest(payload=payload), patch.object(demo, "http", return_value=(payload, {"Cache-Control": "no-store"})):
                with self.assertRaisesRegex(RuntimeError, "native report mismatch"):
                    demo.verify_reliability_http("http://127.0.0.1:18080", self.selection())
        with patch.object(demo, "http", return_value=(response, {})):
            with self.assertRaisesRegex(RuntimeError, "cache contract"):
                demo.verify_reliability_http("http://127.0.0.1:18080", self.selection())

    def test_changed_selection_or_failed_http_never_falls_back(self):
        self.analytics_evidence()
        with patch.object(demo, "http") as http:
            with self.assertRaisesRegex(RuntimeError, "selection changed"):
                demo.verify_reliability_http("http://127.0.0.1:18080", self.selection() | {"publication": "f" * 64})
            http.assert_not_called()
        with patch.object(demo, "http", side_effect=RuntimeError("HTTP 500")) as http:
            with self.assertRaisesRegex(RuntimeError, "HTTP 500"):
                demo.verify_reliability_http("http://127.0.0.1:18080", self.selection())
            self.assertEqual(http.call_count, 1)

    def content_selection(self):
        root = self.root / "content-analytics"
        return dict(root=str(root / "output"), goldRoot=str(root / "gold"),
                    referenceRoot=str(root / "references"), publication="c" * 64)

    def test_content_arguments_pin_every_operator_setting(self):
        selected = self.content_selection()
        self.assertEqual(demo.content_arguments("/approved/python", selected), [
            "--content.analytics.python-executable=/approved/python",
            "--content.analytics.processing-root=" + str(demo.REPO / "processing"),
            "--content.analytics.root=" + selected["root"],
            "--content.analytics.gold-root=" + selected["goldRoot"],
            "--content.analytics.reference-root=" + selected["referenceRoot"],
            "--content.analytics.publication=" + selected["publication"]])

    def test_content_rejects_invalid_missing_and_external_selections(self):
        for selection in (None, {}, [], *[self.content_selection() | {"publication": value}
                for value in (None, "", "latest", "C" * 64, "../publication", "c" * 63, 1)]):
            with self.subTest(selection=selection), self.assertRaisesRegex(RuntimeError, "selected Content publication"):
                demo.content_arguments("/approved/python", selection)
        for key in ("root", "goldRoot", "referenceRoot"):
            for value in (None, "/retained", str(Path(self.content_selection()[key]) / "..")):
                with self.subTest(key=key, value=value), self.assertRaisesRegex(RuntimeError, "isolated demo fixture"):
                    demo.content_arguments("/approved/python", self.content_selection() | {key: value})

    def test_both_analytics_selections_reject_symlink_roots_and_ancestors(self):
        outside = self.root.parent / "retained"
        outside.mkdir()
        for product, selection, arguments in (
                ("content", self.content_selection(), demo.content_arguments),
                ("reliability", self.selection(), demo.reliability_arguments)):
            directory = self.root / (product + "-analytics")
            directory.mkdir(parents=True)
            for key in ("root", "goldRoot", "batchRoot", "referenceRoot"):
                if key not in selection:
                    continue
                link = Path(selection[key])
                link.symlink_to(outside, target_is_directory=True)
                with self.subTest(product=product, key=key), self.assertRaisesRegex(RuntimeError, "without symlinks"):
                    arguments("/approved/python", selection)
                link.unlink()
            directory.rmdir()
            directory.symlink_to(outside, target_is_directory=True)
            with self.subTest(product=product, key="ancestor"), self.assertRaisesRegex(RuntimeError, "without symlinks"):
                arguments("/approved/python", selection)
            directory.unlink()
        self.assertEqual(list(outside.iterdir()), [])

    def content_native(self):
        return dict(product_id="content-intelligence", product_fingerprint="c" * 64, gold_fingerprint="d" * 64,
                    reference=dict(reference_id="publishers", revision="r1", snapshot_fingerprint="e" * 64),
                    filters=dict(source="alpha", start_date="2026-10-12", end_date="2026-10-13", declared_language=None),
                    rows=[dict(source_id="alpha", article_count="9007199254740993", publisher_category=None,
                               reference_status="MATCHED", reference_usage="USED",
                               missing_optional_attributes=["publisher_category", "country_code"])],
                    totals=dict(article_count="9007199254740993", missing_article_count="0"),
                    limit="1", offset="0", has_more=True)

    def test_content_projection_preserves_identity_nulls_decimal_strings_and_enum_values(self):
        native = self.content_native()
        original = deepcopy(native)
        self.assertEqual(demo.content_response(native), dict(
            publication="c" * 64, productId="content-intelligence", goldPublication="d" * 64,
            reference=dict(referenceId="publishers", revision="r1", snapshotFingerprint="e" * 64),
            filters=dict(sourceId="alpha", **{"from": "2026-10-12", "to": "2026-10-13"}, declaredLanguage=None),
            data=[dict(sourceId="alpha", articleCount="9007199254740993", publisherCategory=None,
                       referenceStatus="MATCHED", referenceUsage="USED",
                       missingOptionalAttributes=["publisher_category", "country_code"])],
            totals=dict(articleCount="9007199254740993", missingArticleCount="0"),
            meta=dict(returned=1, limit=1, offset=0, hasMore=True)))
        self.assertEqual(native, original)
        del native["totals"]
        self.assertNotIn("totals", demo.content_response(native))

    def report_evidence(self, product):
        """Use real local files to exercise the inventory guard, not mocked inventories."""
        self.analytics_evidence()
        selection = self.content_selection() if product == "content" else self.selection()
        for key in ("root", "goldRoot", "batchRoot", "referenceRoot"):
            if key in selection:
                directory = Path(selection[key])
                directory.mkdir(parents=True, exist_ok=True)
                (directory / "fixture.json").write_text('{"original":true}')
        if product == "content":
            response = demo.content_response(self.content_native())
            reports = (("catalog", "catalog"), ("source_discovery_daily", "source-discovery-daily"),
                       ("reference_coverage", "reference-coverage"))
            query = dict(sourceId="alpha", **{"from": "2026-10-12", "to": "2026-10-13"}, limit=1, offset=0)
        else:
            response = json.loads((self.root / "reliability-analytics-verification.json").read_text())["scenarios"][0]["response"]
            response["totals"] = dict(requestCount="2", missingServiceCount="0")
            reports = (("requests_5m", "request-windows"), ("input_coverage", "input-coverage"),
                       ("reference_coverage", "reference-coverage"))
            query = dict(serviceId="svc00", environment="test", limit=1, offset=0)
        scenarios = [dict(report=report, endpoint=endpoint, name="first-page", query=query, response=response)
                     for report, endpoint in reports]
        inventory = demo.selection_inventory(selection)
        evidence = dict(selection=selection, scenarios=scenarios, inventory=inventory,
                        unchangedFiles=sum(map(len, inventory.values())))
        demo.write_json(self.root / (product + "-analytics-verification.json"), evidence)
        return selection, evidence

    def test_http_routes_all_content_and_reliability_reports_with_exact_queries(self):
        for product in ("content", "reliability"):
            selection, evidence = self.report_evidence(product)
            responses = [(case["response"], {"Cache-Control": "no-store"}) for case in evidence["scenarios"]]
            with self.subTest(product=product), patch.object(demo, "http", side_effect=responses) as http:
                result = demo.verify_analytics_http(product, "http://127.0.0.1:14173", selection)
            self.assertEqual(result["publication"], selection["publication"])
            self.assertEqual(result["unchangedFiles"], 3)
            self.assertEqual(len(result["scenarios"]), 3)
            for call, case, observed in zip(http.call_args_list, evidence["scenarios"], result["scenarios"], strict=True):
                base, path = call.args
                self.assertEqual(base, "http://127.0.0.1:14173")
                parsed = demo.urllib.parse.urlsplit(path)
                self.assertEqual(parsed.path, "/api/v1/" + product + "/" + case["endpoint"])
                self.assertEqual(demo.urllib.parse.parse_qs(parsed.query), {key: [str(value)] for key, value in case["query"].items()})
                self.assertEqual(observed, {key: case[key] for key in ("report", "name", "response")})

    def test_all_reports_reject_changed_identity_types_totals_and_cache_without_fallback(self):
        for product in ("content", "reliability"):
            selection, evidence = self.report_evidence(product)
            for index, case in enumerate(evidence["scenarios"]):
                original = case["response"]
                invalid = [original | {"publication": "f" * 64}, original | {"data": []},
                           original | {"totals": {}}, original | {"totals": {key: "99" for key in original["totals"]}},
                           original | {"totals": {key: 0 for key in original["totals"]}},
                           original | {"filters": {}}, original | {"productId": "switched-product"},
                           original | {"meta": original["meta"] | {"hasMore": 1}},
                           original | {"reference": original["reference"] | {"revision": "r2"}}]
                changed = deepcopy(original)
                count = "articleCount" if product == "content" else "requestCount"
                changed["data"][0][count] = int(changed["data"][0][count])
                invalid.append(changed)
                changed_null = deepcopy(original)
                changed_null["data"][0]["publisherCategory" if product == "content" else "ownerTeam"] = ""
                invalid.append(changed_null)
                if product == "content":
                    changed_enums = deepcopy(original)
                    changed_enums["data"][0]["missingOptionalAttributes"] = ["publisherCategory", "countryCode"]
                    invalid.append(changed_enums)
                    invalid.append(original | {"goldPublication": "f" * 64})
                for payload, headers, error in [(value, {"Cache-Control": "no-store"}, "native report mismatch") for value in invalid] + [(original, {}, "cache contract")]:
                    preceding = [(previous["response"], {"Cache-Control": "no-store"}) for previous in evidence["scenarios"][:index]]
                    with self.subTest(product=product, report=case["report"], payload=payload, headers=headers), patch.object(
                            demo, "http", side_effect=preceding + [(payload, headers)]) as http:
                        with self.assertRaisesRegex(RuntimeError, error):
                            demo.verify_analytics_http(product, "http://127.0.0.1:18080", selection)
                        self.assertEqual(http.call_count, index + 1)
            for key in selection:
                with self.subTest(product=product, selection=key), patch.object(demo, "http") as http:
                    with self.assertRaisesRegex(RuntimeError, "selection changed"):
                        demo.verify_analytics_http(product, "http://127.0.0.1:18080", selection | {key: "changed"})
                    http.assert_not_called()
            with patch.object(demo, "http", side_effect=RuntimeError("HTTP 500")) as http:
                with self.assertRaisesRegex(RuntimeError, "HTTP 500"):
                    demo.verify_analytics_http(product, "http://127.0.0.1:18080", selection)
                self.assertEqual(http.call_count, 1)

    def test_http_refuses_hash_or_mtime_mutation_before_and_after_reads(self):
        for product in ("content", "reliability"):
            keys = ("root", "goldRoot", "referenceRoot") if product == "content" else ("root", "batchRoot", "referenceRoot")
            for key in keys:
                for mutation in ("hash", "mtime"):
                    for timing in ("before", "after"):
                        selection, evidence = self.report_evidence(product)
                        path = Path(selection[key]) / "fixture.json"
                        stat = path.stat()
                        def mutate():
                            if mutation == "hash":
                                path.write_text('{"original":false}')
                                os.utime(path, ns=(stat.st_atime_ns, stat.st_mtime_ns))
                            else:
                                os.utime(path, ns=(stat.st_atime_ns, stat.st_mtime_ns + 1_000_000))
                        calls = []
                        def respond(base, route):
                            calls.append(route)
                            if len(calls) == len(evidence["scenarios"]):
                                mutate()
                            return evidence["scenarios"][len(calls)-1]["response"], {"Cache-Control": "no-store"}
                        if timing == "before":
                            mutate()
                        with self.subTest(product=product, key=key, mutation=mutation, timing=timing), patch.object(demo, "http", side_effect=respond) as http:
                            with self.assertRaisesRegex(RuntimeError, "changed after preparation" if timing == "before" else "HTTP reads changed"):
                                demo.verify_analytics_http(product, "http://127.0.0.1:18080", selection)
                            self.assertEqual(http.call_count, 0 if timing == "before" else 3)

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
