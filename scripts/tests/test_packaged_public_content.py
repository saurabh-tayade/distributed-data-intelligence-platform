"""Offline prerequisite/refusal tests. No test calls public DNS, HTTP, Docker or a worker."""
import contextlib
import copy
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import zipfile

REPO = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location("public_qualification", REPO / "scripts/qualify-packaged-public-content.py")
q = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(q)


class PublicContentInputTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name).resolve()
        for name in ("installation", "evidence", "runtime", "processing"):
            (self.base / name).mkdir(mode=0o700)
        (self.base / "runtime/bin").mkdir()
        self.artifacts = {}
        for name in q.ARTIFACT_LIMITS:
            suffix = ".jar" if name in ("api", "ingestion", "provider", "runner") else ""
            path = self.base / ("runtime/bin/java" if name == "java" else "runtime/" + name + suffix)
            if name in ("api", "ingestion", "provider", "runner"):
                with zipfile.ZipFile(path, "w") as jar:
                    if name == "api":
                        jar.writestr("META-INF/MANIFEST.MF", "Manifest-Version: 1.0\nMain-Class: org.springframework.boot.loader.launch.JarLauncher\nStart-Class: platform.contentapi.ContentApiApplication\n")
                        jar.writestr("org/springframework/boot/loader/launch/PropertiesLauncher.class", b"offline-placeholder")
                    elif name == "ingestion":
                        jar.writestr("META-INF/MANIFEST.MF", "Manifest-Version: 1.0\n")
                    elif name == "provider":
                        jar.writestr("META-INF/services/java.net.spi.InetAddressResolverProvider", "platform.ingestion.managed.PublicFetchResolverProvider\n")
                    else:
                        jar.writestr(q.RUNNER_PREFIX + ".class", b"offline-placeholder")
            elif name == "fixture":
                path.write_bytes((REPO / "ingestion/src/test/resources/feeds/atom-public-qualification.xml").read_bytes())
            else:
                path.write_bytes(name.encode())
            if name in ("java", "python", "docker"):
                path.chmod(0o755)
            self.artifacts[name] = dict(path=str(path), sha256=hashlib.sha256(path.read_bytes()).hexdigest())
        ids = [f"10000000-0000-4000-8000-{i:012d}" for i in range(1, 8)]
        self.plan = dict(planVersion=1, scenario="complete", installationId=ids[0],
            installationRoot=str(self.base / "installation"), evidenceDirectory=str(self.base / "evidence/new"),
            database=dict(url="jdbc:postgresql://127.0.0.1:5432/qualification", user="qualification", systemIdentifier="123", databaseOid="456"),
            artifacts=self.artifacts, nativeClasspath=[self.artifacts["ingestion"], self.artifacts["provider"]],
            runtime=dict(javaHome=str(self.base / "runtime"), javaVersion="21.0.12.1+1-LTS", processingRoot=str(self.base / "processing")),
            profiles=dict(pipeline="CONTENT_RSS_V1", runProfileVersion=2, compositionVersion=2,
                executionProfileId=ids[1], executionProfileSha256="a" * 64, deploymentSha256="b" * 64,
                runtimeProfileId="public-pipeline", controlBindingId="private-control", publisherReferenceArtifactId=ids[2],
                publisherReferenceEvidenceSha256="c" * 64, sourceId="qualification-feed", endpointId=ids[3], bronzeBindingId=ids[4]),
            services=dict(postgres=dict(containerId="d" * 64, imageId="sha256:" + "e" * 64),
                seaweedfs=dict(containerId="f" * 64, imageId="sha256:" + "a" * 64)), apiPort=18080, deadlineSeconds=900,
            fixture=dict(uri=q.FIXTURE_URI, sha256=q.FIXTURE_SHA, bytes=726, title=q.FIXTURE_TITLE))
        catalog = dict(catalogVersion=1, endpoints=[dict(endpointId=ids[3], sourceId="qualification-feed", uri=q.FIXTURE_URI)],
                       reliabilityProfiles=[], storageProfiles=[dict(profileId=ids[4], productId="content-intelligence", role="BRONZE")])
        self.catalog = catalog
        self.change_file("catalog", json.dumps(catalog).encode())
        self.plan_path = self.base / "plan.json"

    def change_file(self, name, raw):
        path = Path(self.artifacts[name]["path"])
        path.write_bytes(raw)
        self.artifacts[name]["sha256"] = hashlib.sha256(raw).hexdigest()

    def save(self):
        self.plan_path.write_text(json.dumps(self.plan))

    def validate(self):
        self.save()
        return q.validate(self.plan_path)

    def rejected(self):
        with self.assertRaises((q.Blocked, OSError, ValueError, KeyError)):
            self.validate()
        self.assertFalse(Path(self.plan["evidenceDirectory"]).exists())

    def test_valid_inputs_are_offline_and_do_not_create_evidence(self):
        with patch.object(q.os, "execve") as execute:
            raw, result = self.validate()
        self.assertEqual(result, self.plan)
        self.assertEqual(json.loads(raw), self.plan)
        self.assertFalse(Path(result["evidenceDirectory"]).exists())
        execute.assert_not_called()

    def test_exact_fixture_identity_is_independent_of_plan(self):
        raw = Path(self.artifacts["fixture"]["path"]).read_bytes()
        self.assertEqual(len(raw), 726)
        self.assertEqual(hashlib.sha256(raw).hexdigest(), q.FIXTURE_SHA)
        self.assertIn(q.FIXTURE_TITLE.encode(), raw)
        self.assertIn("/6584d52153e599ce8f4a5c314e13535f2e7e7e1b/", q.FIXTURE_URI)

    def test_unreviewed_fixture_variants_are_refused(self):
        for key, value in dict(uri=q.FIXTURE_URI.replace("6584d52153e599ce8f4a5c314e13535f2e7e7e1b", "main"),
                               sha256="0" * 64, bytes=727, title="Changed").items():
            with self.subTest(key=key):
                original = self.plan["fixture"][key]
                self.plan["fixture"][key] = value
                self.rejected()
                self.plan["fixture"][key] = original

    def test_fixture_mutation_is_rejected_even_with_rehashed_artifact(self):
        self.change_file("fixture", b"x" * 726)
        self.rejected()

    def test_artifact_mutation_is_refused(self):
        Path(self.artifacts["ingestion"]["path"]).write_bytes(b"mutated")
        self.rejected()

    def test_missing_dependency_is_refused(self):
        Path(self.artifacts["provider"]["path"]).unlink()
        self.rejected()

    def test_complete_thin_jar_dependency_classpath_is_validated(self):
        path = self.base / "runtime/library.jar"
        with zipfile.ZipFile(path, "w") as jar:
            jar.writestr("META-INF/MANIFEST.MF", "Manifest-Version: 1.0\n")
        entry = dict(path=str(path), sha256=hashlib.sha256(path.read_bytes()).hexdigest())
        self.plan["nativeClasspath"].insert(1, entry)
        self.validate()
        path.write_bytes(b"changed-installed-dependency")
        self.rejected()

    def test_classpath_omission_reordering_and_duplicates_are_refused(self):
        original = self.plan["nativeClasspath"]
        for value in ([], original[:-1], list(reversed(original)), original[:-1] + original):
            with self.subTest(value=value):
                self.plan["nativeClasspath"] = value
                self.rejected()
        self.plan["nativeClasspath"] = original

    def test_observer_jar_cannot_enter_native_classpath(self):
        self.plan["nativeClasspath"].insert(1, self.artifacts["runner"])
        self.rejected()

    def test_symlink_artifact_is_refused(self):
        file = Path(self.artifacts["python"]["path"])
        moved = file.with_suffix(".real")
        file.rename(moved)
        file.symlink_to(moved)
        self.rejected()

    def test_hard_linked_artifact_is_refused(self):
        os.link(self.artifacts["ingestion"]["path"], self.base / "second-link")
        self.rejected()

    def test_group_writable_artifact_is_refused(self):
        Path(self.artifacts["provider"]["path"]).chmod(0o664)
        self.rejected()

    def test_oversized_artifact_is_refused_before_hashing(self):
        self.change_file("catalog", b" " * 65537)
        self.rejected()

    def test_wrong_provider_is_refused_even_with_matching_jar_digest(self):
        path = Path(self.artifacts["provider"]["path"])
        with zipfile.ZipFile(path, "w") as jar:
            jar.writestr("META-INF/services/java.net.spi.InetAddressResolverProvider", "test.BypassResolver")
        self.artifacts["provider"]["sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
        self.rejected()

    def test_non_boot_api_is_refused(self):
        path = Path(self.artifacts["api"]["path"])
        with zipfile.ZipFile(path, "w") as jar:
            jar.writestr("META-INF/MANIFEST.MF", "Main-Class: test.Other\n")
        self.artifacts["api"]["sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
        self.rejected()

    def test_fixture_classes_cannot_enter_runner_jar(self):
        path = Path(self.artifacts["runner"]["path"])
        with zipfile.ZipFile(path, "a") as jar:
            jar.writestr("platform/contentapi/management/OrdinaryContentWorkerFixtures.class", b"x")
        self.artifacts["runner"]["sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
        self.rejected()

    def test_historical_and_reliability_profiles_are_refused(self):
        for key, value in (("pipeline", "RELIABILITY_V2"), ("compositionVersion", 1), ("runProfileVersion", 1), ("compositionVersion", True)):
            with self.subTest(key=key, value=value):
                original = self.plan["profiles"][key]
                self.plan["profiles"][key] = value
                self.rejected()
                self.plan["profiles"][key] = original

    def test_catalog_substitution_or_extra_selection_is_refused(self):
        for mutate in (lambda c: c["endpoints"][0].update(uri="https://example.org/other"),
                       lambda c: c["endpoints"].append(copy.deepcopy(c["endpoints"][0])),
                       lambda c: c["reliabilityProfiles"].append({}),
                       lambda c: c["storageProfiles"][0].update(role="REFERENCE")):
            with self.subTest(mutate=mutate):
                changed = copy.deepcopy(self.catalog)
                mutate(changed)
                self.change_file("catalog", json.dumps(changed).encode())
                self.rejected()

    def test_unknown_plan_field_is_refused(self):
        self.plan["retry"] = True
        self.rejected()

    def test_duplicate_json_fields_are_refused(self):
        self.plan_path.write_text('{"planVersion":1,"planVersion":1}')
        with self.assertRaisesRegex(q.Blocked, "Duplicate"):
            q.validate(self.plan_path)

    def test_remote_database_and_query_overrides_are_refused(self):
        for value in ("jdbc:postgresql://example.org:5432/test", "jdbc:postgresql://127.0.0.1:5432/test?sslmode=disable",
                      "jdbc:postgresql://127.0.0.1:65536/test"):
            with self.subTest(value=value):
                self.plan["database"]["url"] = value
                self.rejected()

    def test_secrets_are_not_plan_inputs(self):
        self.plan["database"]["password"] = "private-sentinel"
        self.rejected()

    def test_runtime_and_output_overlap_is_refused(self):
        self.plan["runtime"]["processingRoot"] = self.plan["installationRoot"]
        self.rejected()

    def test_evidence_cannot_be_under_native_installation(self):
        self.plan["evidenceDirectory"] = str(self.base / "installation/new")
        self.rejected()

    def test_existing_evidence_is_retained_and_refused(self):
        path = Path(self.plan["evidenceDirectory"])
        path.mkdir()
        marker = path / "retained"
        marker.write_bytes(b"immutable")
        with self.assertRaises(q.Blocked):
            self.validate()
        self.assertEqual(marker.read_bytes(), b"immutable")

    def test_dangling_evidence_symlink_is_refused(self):
        Path(self.plan["evidenceDirectory"]).symlink_to(self.base / "missing")
        with self.assertRaises(q.Blocked):
            self.validate()

    def test_invalid_scenarios_and_boolean_limits_are_refused(self):
        for key, value in (("scenario", "recover"), ("apiPort", True), ("deadlineSeconds", 1801), ("planVersion", 1.0)):
            with self.subTest(key=key):
                original = self.plan[key]
                self.plan[key] = value
                self.rejected()
                self.plan[key] = original

    def test_proxy_and_runtime_overrides_are_rejected_without_removal(self):
        for name in ("HTTPS_PROXY", "http_proxy", "NO_PROXY", "JAVA_TOOL_OPTIONS", "JDK_JAVA_OPTIONS", "_JAVA_OPTIONS",
                     "LD_PRELOAD", "PYTHONPATH", "SPRING_APPLICATION_JSON", "DDIP_WORKER_EXECUTION_PROFILE_ID", "CONTENT_ANALYTICS_PUBLICATION"):
            env = {name: "sentinel"}
            with self.subTest(name=name), self.assertRaises(q.Blocked):
                q.environment_gate(env)
            self.assertEqual(env, {name: "sentinel"})

    def test_only_database_password_is_an_allowed_ddip_environment_input(self):
        q.environment_gate(dict(DDIP_DB_PASSWORD="private", HTTPS_PROXY=""))

    def test_invalid_inputs_prevent_authorization_and_every_launch(self):
        self.plan["profiles"]["compositionVersion"] = 1
        self.save()
        with patch.dict(os.environ, {}, clear=True), patch.object(q, "authorization") as authorize, patch.object(q.os, "execve") as execute:
            with self.assertRaises(q.Blocked):
                q.launch("run", self.plan_path, self.base / "approval")
        authorize.assert_not_called()
        execute.assert_not_called()

    def test_missing_approval_prevents_evidence_creation_and_launch(self):
        self.save()
        with patch.dict(os.environ, {}, clear=True), patch.object(q, "authorization", side_effect=q.Blocked("No approved target")), patch.object(q.os, "execve") as execute:
            with self.assertRaises(q.Blocked):
                q.launch("run", self.plan_path, self.base / "approval")
        execute.assert_not_called()
        self.assertFalse(Path(self.plan["evidenceDirectory"]).exists())

    def approval(self):
        self.save()
        raw = self.plan_path.read_bytes()
        approval = dict(approvalVersion=1, ownerDecision="https://github.com/saurabh-tayade/distributed-data-intelligence-platform/issues/49#issuecomment-1",
            planSha256=hashlib.sha256(raw).hexdigest(), installationId=self.plan["installationId"], hostname=os.uname().nodename,
            bootId="offline-boot", expiresEpochSeconds=2000, disposable=True, directNativeEgress=True, scenario="complete")
        return raw, approval

    def authorize_offline(self, raw, approval):
        path = self.base / "approval.json"
        path.write_text(json.dumps(approval))
        # Only the authorization predicate is exercised; these mocks authorize no real target.
        with patch.object(q.sys, "platform", "linux"), patch.object(q.Path, "exists", return_value=True), patch.object(q.Path, "read_text", return_value="offline-boot\n"), patch.object(q.time, "time", return_value=1000), patch.dict(os.environ, {"DDIP_DB_PASSWORD": "private"}, clear=True):
            return q.authorization(path, raw, self.plan)

    def test_approval_must_name_the_exact_plan_installation_and_scenario(self):
        raw, approval = self.approval()
        for key, value in (("planSha256", "0" * 64), ("installationId", "different"), ("scenario", "live-stop")):
            with self.subTest(key=key), self.assertRaises(q.Blocked):
                self.authorize_offline(raw, approval | {key: value})

    def test_approval_must_name_the_actual_linux_boot_and_hostname(self):
        raw, approval = self.approval()
        for key in ("hostname", "bootId"):
            with self.subTest(key=key), self.assertRaises(q.Blocked):
                self.authorize_offline(raw, approval | {key: "another-target"})

    def test_approval_must_be_current_explicit_and_bounded(self):
        raw, approval = self.approval()
        for key, value in (("expiresEpochSeconds", 999), ("expiresEpochSeconds", 1000000),
                           ("disposable", 1), ("directNativeEgress", False), ("ownerDecision", "https://example.org/approval")):
            with self.subTest(key=key, value=value), self.assertRaises(q.Blocked):
                self.authorize_offline(raw, approval | {key: value})

    def test_non_linux_target_is_refused_before_approval_read(self):
        self.save()
        with patch.object(q.sys, "platform", "darwin"), self.assertRaisesRegex(q.Blocked, "Linux"):
            q.authorization(self.base / "absent", self.plan_path.read_bytes(), self.plan)

    def test_valid_authorization_predicate_is_offline_only(self):
        raw, approval = self.approval()
        self.assertEqual(json.loads(self.authorize_offline(raw, approval)), approval)
        self.assertFalse(Path(self.plan["evidenceDirectory"]).exists())

    def test_failed_launch_preserves_frozen_plan_and_never_cleans_up(self):
        self.save()
        with patch.dict(os.environ, {"DDIP_DB_PASSWORD": "private-sentinel"}, clear=True), patch.object(q, "authorization", return_value=b'{"actualOwnerRecord":"offline-mock"}'), patch.object(q.os, "chdir") as change_directory, patch.object(q.os, "execve", side_effect=OSError("launch unavailable")) as execute:
            with self.assertRaises(OSError):
                q.launch("run", self.plan_path, self.base / "approval")
        evidence = Path(self.plan["evidenceDirectory"])
        self.assertEqual((evidence / "plan.json").read_bytes(), self.plan_path.read_bytes())
        self.assertEqual(json.loads((evidence / "launch.json").read_bytes())["status"], "LAUNCH_REQUESTED")
        self.assertNotIn(b"private-sentinel", b"".join(p.read_bytes() for p in evidence.iterdir()))
        argv, env = execute.call_args.args[1:]
        self.assertEqual(argv[0], self.artifacts["java"]["path"])
        self.assertNotIn("JAVA_TOOL_OPTIONS", env)
        self.assertNotIn("-Djava.net.useSystemProxies=false", argv)
        self.assertEqual(env["DDIP_DB_PASSWORD"], "private-sentinel")
        change_directory.assert_called_once_with(evidence)

    def test_write_once_does_not_replace_retained_bytes(self):
        path = self.base / "retained"
        q.write_once(path, b"first")
        with self.assertRaises(FileExistsError):
            q.write_once(path, b"second")
        self.assertEqual(path.read_bytes(), b"first")

    def test_cli_diagnostic_is_redacted_and_does_not_claim_a_pass(self):
        output = io.StringIO()
        with patch.object(q, "validate", side_effect=OSError("private-sentinel")), contextlib.redirect_stderr(output):
            self.assertEqual(q.main(["validate", str(self.plan_path)]), 2)
        self.assertNotIn("private-sentinel", output.getvalue())
        self.assertEqual(json.loads(output.getvalue())["status"], "BLOCKED")

    def test_public_runner_remains_absent_from_default_ci(self):
        workflow = (REPO / ".github/workflows/ci.yml").read_text()
        self.assertNotIn("qualify-ordinary-public-content.sh", workflow)
        self.assertNotIn("qualify-packaged-public-content.py run", workflow)
        source = (REPO / "services/content-api/src/test/java/platform/contentapi/execution/PackagedPublicContentAcceptance.java").read_text()
        for prohibited in ("WorkerRepository.Owner", "WorkerRepository(", "executeOriginal(", "originalPipeline(",
                           "TestSupport", "extends Ordinary", '"INSERT ', '"UPDATE ', '"DELETE ', "destroyForcibly("):
            self.assertNotIn(prohibited, source)


if __name__ == "__main__":
    unittest.main()
