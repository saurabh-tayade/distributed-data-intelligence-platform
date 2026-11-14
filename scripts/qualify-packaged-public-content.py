#!/usr/bin/env python3
"""Explicit E5.2-D launcher. Validation is offline; execution requires a reviewed target.

This module never provisions an installation, registers profiles, imports an artifact,
constructs execution ownership, changes networking, or cleans up an interrupted run.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import sys
import time
import uuid
import zipfile

FIXTURE_URI = ("https://raw.githubusercontent.com/saurabh-tayade/"
               "distributed-data-intelligence-platform/6584d52153e599ce8f4a5c314e13535f2e7e7e1b/"
               "ingestion/src/test/resources/feeds/atom-public-qualification.xml")
FIXTURE_SHA = "1f99a72b80837b4239189a6b02d4f143b104aae845d2f5ece77776705a66e81c"
FIXTURE_TITLE = "A paper star in an imaginary observatory"
RUNNER = "platform.contentapi.execution.PackagedPublicContentAcceptance"
RUNNER_PREFIX = RUNNER.replace(".", "/")
ARTIFACT_LIMITS = dict(api=128 << 20, ingestion=128 << 20, provider=2 << 20,
                       runner=1 << 20, java=128 << 20, python=128 << 20,
                       catalog=65536, fixture=726, docker=128 << 20)


class Blocked(ValueError):
    """A prerequisite failed. This is never an acceptance pass."""


def need(condition, message):
    if not condition:
        raise Blocked(message)


def fields(value, *names):
    need(isinstance(value, dict) and set(value) == set(names), "Unexpected or missing fields")


def text(value, pattern):
    need(isinstance(value, str) and re.fullmatch(pattern, value) is not None, "Invalid input value")
    return value


def identity(value):
    text(value, r"[0-9a-f-]{36}")
    need(str(uuid.UUID(value)) == value, "Noncanonical UUID")
    return value


def digest(value):
    return text(value, r"[0-9a-f]{64}")


def integer(value, low, high):
    need(type(value) is int and low <= value <= high, "Invalid integer input")


def unique_pairs(pairs):
    result = {}
    for key, value in pairs:
        need(key not in result, "Duplicate JSON member")
        result[key] = value
    return result


def regular(path, limit):
    need(isinstance(path, str), "Expected absolute path")
    p = Path(path)
    need(p.is_absolute() and str(p) == path and p.resolve(strict=True) == p,
         "Path must be absolute, canonical and free of symlinks")
    before = p.lstat()
    need(stat.S_ISREG(before.st_mode) and before.st_nlink == 1 and before.st_size <= limit,
         "Expected bounded regular file with one link")
    need(before.st_uid == os.getuid() or before.st_uid == 0, "Untrusted file owner")
    need(before.st_mode & 0o022 == 0, "Input must not be writable by group or others")
    fd = os.open(p, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(fd, "rb") as stream:
        opened = os.fstat(stream.fileno())
        need((opened.st_dev, opened.st_ino) == (before.st_dev, before.st_ino), "Input changed before opening")
        raw = stream.read(limit + 1)
    after = p.lstat()
    need(len(raw) <= limit and (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) ==
         (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns), "Input changed while reading")
    return raw


def document(path, limit=65536):
    raw = regular(str(path), limit)
    try:
        value = json.loads(raw, object_pairs_hook=unique_pairs,
                           parse_constant=lambda _: (_ for _ in ()).throw(Blocked("Nonfinite JSON")))
    except (UnicodeError, json.JSONDecodeError) as error:
        raise Blocked("Invalid JSON input") from error
    return raw, value


def directory(value, private=False):
    need(isinstance(value, str), "Expected directory path")
    p = Path(value)
    need(p.is_absolute() and str(p) == value and p.resolve(strict=True) == p and p.is_dir(),
         "Expected existing canonical directory")
    mode = p.stat()
    need(mode.st_uid in (0, os.getuid()) and mode.st_mode & 0o022 == 0, "Untrusted directory")
    if private:
        need(mode.st_uid == os.getuid() and stat.S_IMODE(mode.st_mode) == 0o700, "Expected private owned directory")
    return p


def disjoint(a, b):
    return a != b and a not in b.parents and b not in a.parents


def jar_entry(raw_path, name, expected=None):
    with zipfile.ZipFile(raw_path) as jar:
        names = jar.namelist()
        need(len(names) == len(set(names)) and len(names) <= 50000, "Invalid JAR inventory")
        item = jar.getinfo(name)
        need(item.file_size <= 65536, "Oversized JAR metadata")
        result = jar.read(item)
        if expected is not None:
            need(result.strip() == expected, "Incompatible installed JAR")
        return result


def validate(plan_path):
    raw, plan = document(plan_path)
    fields(plan, "planVersion", "scenario", "installationId", "installationRoot", "evidenceDirectory",
           "database", "artifacts", "nativeClasspath", "runtime", "profiles", "services", "apiPort", "deadlineSeconds", "fixture")
    integer(plan["planVersion"], 1, 1)
    need(plan["scenario"] in ("complete", "live-stop"), "Unsupported scenario")
    identity(plan["installationId"])
    root = directory(plan["installationRoot"], private=True)
    need(isinstance(plan["evidenceDirectory"], str), "Expected evidence path")
    evidence = Path(plan["evidenceDirectory"])
    need(evidence.is_absolute() and str(evidence) == plan["evidenceDirectory"] and
         evidence.name not in (".", "..") and not os.path.lexists(evidence), "Evidence must be new; never resume or overwrite")
    parent = directory(str(evidence.parent), private=True)
    need(disjoint(root, evidence) and disjoint(root, parent), "Evidence must be outside native installation")
    db = plan["database"]
    fields(db, "url", "user", "systemIdentifier", "databaseOid")
    text(db["url"], r"jdbc:postgresql://127\.0\.0\.1:[1-9][0-9]{0,4}/[A-Za-z0-9_-]{1,63}")
    integer(int(db["url"].split(":")[-1].split("/")[0]), 1, 65535)
    text(db["user"], r"[A-Za-z_][A-Za-z0-9_]{0,62}")
    text(db["systemIdentifier"], r"[1-9][0-9]{0,19}")
    text(db["databaseOid"], r"[1-9][0-9]{0,9}")
    integer(plan["apiPort"], 1024, 65535)
    integer(plan["deadlineSeconds"], 120, 1800)
    fixture = plan["fixture"]
    fields(fixture, "uri", "sha256", "bytes", "title")
    need(fixture == dict(uri=FIXTURE_URI, sha256=FIXTURE_SHA, bytes=726, title=FIXTURE_TITLE),
         "Only the reviewed immutable Atom fixture is eligible")
    integer(fixture["bytes"], 726, 726)
    runtime = plan["runtime"]
    fields(runtime, "javaHome", "javaVersion", "processingRoot")
    java_home = directory(runtime["javaHome"])
    processing = directory(runtime["processingRoot"])
    text(runtime["javaVersion"], r"21\.[A-Za-z0-9.+_-]{1,100}")
    need(all(disjoint(a, b) for a in (java_home, processing) for b in (root, evidence)), "Runtime/output overlap")
    profiles = plan["profiles"]
    fields(profiles, "pipeline", "runProfileVersion", "compositionVersion", "executionProfileId",
           "executionProfileSha256", "deploymentSha256", "runtimeProfileId", "controlBindingId",
           "publisherReferenceArtifactId", "publisherReferenceEvidenceSha256", "sourceId", "endpointId", "bronzeBindingId")
    need(profiles["pipeline"] == "CONTENT_RSS_V1", "Only original Content is eligible")
    integer(profiles["runProfileVersion"], 2, 2)
    integer(profiles["compositionVersion"], 2, 2)
    for name in ("executionProfileId", "publisherReferenceArtifactId", "endpointId", "bronzeBindingId"):
        identity(profiles[name])
    for name in ("runtimeProfileId", "controlBindingId"):
        text(profiles[name], r"[A-Za-z0-9_-]{1,128}")
    for name in ("executionProfileSha256", "deploymentSha256", "publisherReferenceEvidenceSha256"):
        digest(profiles[name])
    text(profiles["sourceId"], r"[a-z0-9][a-z0-9_-]{0,62}")
    fields(plan["services"], "postgres", "seaweedfs")
    for service in plan["services"].values():
        fields(service, "containerId", "imageId")
        digest(service["containerId"])
        text(service["imageId"], r"sha256:[0-9a-f]{64}")
    artifacts = plan["artifacts"]
    fields(artifacts, *ARTIFACT_LIMITS)
    pins = {}
    for name, limit in ARTIFACT_LIMITS.items():
        entry = artifacts[name]
        fields(entry, "path", "sha256")
        expected = digest(entry["sha256"])
        payload = regular(entry["path"], limit)
        need(hashlib.sha256(payload).hexdigest() == expected, "Installed artifact digest mismatch: " + name)
        path = Path(entry["path"])
        need(disjoint(path, root) and disjoint(path, evidence), "Installed artifact/output overlap")
        if name in ("java", "python", "docker"):
            need(os.access(path, os.X_OK), "Required executable is not executable")
        pins[name] = entry
    need(len({p["path"] for p in pins.values()}) == len(pins), "Artifact paths must be distinct")
    classpath = plan["nativeClasspath"]
    need(isinstance(classpath, list) and 2 <= len(classpath) <= 128, "Complete native classpath is required")
    need(classpath[0] == pins["ingestion"] and classpath[-1] == pins["provider"], "Native classpath must retain ingestion/dependency/provider ordering")
    selected = set()
    for entry in classpath:
        fields(entry, "path", "sha256")
        payload = regular(entry["path"], 128 << 20)
        need(hashlib.sha256(payload).hexdigest() == digest(entry["sha256"]), "Native dependency digest mismatch")
        path = Path(entry["path"])
        need(path.suffix == ".jar" and disjoint(path, root) and disjoint(path, evidence), "Native dependency path is not isolated")
        need(path not in selected and path not in (Path(pins["api"]["path"]), Path(pins["runner"]["path"])), "Duplicate or non-native classpath member")
        selected.add(path)
        with zipfile.ZipFile(path) as jar:
            need(len(jar.namelist()) <= 50000, "Native JAR entry limit")
    need(Path(pins["java"]["path"]) == java_home / "bin/java", "JDK binary must match javaHome")
    need(pins["fixture"]["sha256"] == FIXTURE_SHA and len(regular(pins["fixture"]["path"], 726)) == 726,
         "Fixture bytes differ")
    manifest = jar_entry(pins["api"]["path"], "META-INF/MANIFEST.MF").replace(b"\r\n", b"\n")
    need(b"Main-Class: org.springframework.boot.loader.launch.JarLauncher\n" in manifest and
         b"Start-Class: platform.contentapi.ContentApiApplication\n" in manifest, "Expected packaged Content API JAR")
    jar_entry(pins["api"]["path"], "org/springframework/boot/loader/launch/PropertiesLauncher.class")
    jar_entry(pins["ingestion"]["path"], "META-INF/MANIFEST.MF")
    jar_entry(pins["provider"]["path"], "META-INF/services/java.net.spi.InetAddressResolverProvider",
              b"platform.ingestion.managed.PublicFetchResolverProvider")
    with zipfile.ZipFile(pins["runner"]["path"]) as jar:
        names = jar.namelist()
        need(len(names) == len(set(names)) and 1 <= len(names) <= 32 and RUNNER_PREFIX + ".class" in names,
             "Invalid qualification runner JAR")
        need(all(re.fullmatch(re.escape(RUNNER_PREFIX) + r"(?:\$[A-Za-z0-9_]+)?\.class", name) for name in names),
             "Runner JAR must contain only the reviewed standalone helper")
    _, catalog = document(pins["catalog"]["path"])
    fields(catalog, "catalogVersion", "endpoints", "reliabilityProfiles", "storageProfiles")
    expected_catalog = dict(catalogVersion=1, endpoints=[dict(endpointId=profiles["endpointId"],
        sourceId=profiles["sourceId"], uri=FIXTURE_URI)], reliabilityProfiles=[],
        storageProfiles=[dict(profileId=profiles["bronzeBindingId"], productId="content-intelligence", role="BRONZE")])
    need(catalog == expected_catalog, "Catalog must close exactly over the reviewed fixture and Bronze binding")
    integer(catalog["catalogVersion"], 1, 1)
    return raw, plan


def environment_gate(environ):
    for key, value in environ.items():
        lower = key.lower()
        forbidden = ("proxy" in lower or lower in ("java_tool_options", "jdk_java_options", "_java_options", "classpath",
                     "ld_preload", "ld_library_path", "pythonpath", "pythonhome") or
                     key.startswith(("SPRING_", "SERVER_", "ANALYTICS_", "CONTENT_ANALYTICS_", "RELIABILITY_ANALYTICS_")) or
                     (key.startswith("DDIP_") and key != "DDIP_DB_PASSWORD"))
        need(not (forbidden and value), "Refusing inherited network/runtime/application override: " + key)


def authorization(path, raw, plan):
    need(sys.platform == "linux" and Path("/proc/self/fd/0").exists(), "Public acceptance requires native Linux")
    approval_raw, approval = document(path)
    fields(approval, "approvalVersion", "ownerDecision", "planSha256", "installationId", "hostname", "bootId",
           "expiresEpochSeconds", "disposable", "directNativeEgress", "scenario")
    integer(approval["approvalVersion"], 1, 1)
    text(approval["ownerDecision"], r"https://github\.com/saurabh-tayade/distributed-data-intelligence-platform/issues/49#issuecomment-[0-9]+")
    need(approval["planSha256"] == hashlib.sha256(raw).hexdigest() and approval["installationId"] == plan["installationId"]
         and approval["scenario"] == plan["scenario"], "Approval does not bind this exact plan")
    need(approval["disposable"] is True and approval["directNativeEgress"] is True, "Disposable/direct-egress approval missing")
    need(approval["hostname"] == os.uname().nodename and approval["bootId"] == Path("/proc/sys/kernel/random/boot_id").read_text().strip(),
         "Approval names a different Linux target")
    integer(approval["expiresEpochSeconds"], int(time.time()) + 1, int(time.time()) + 86400)
    need(bool(os.environ.get("DDIP_DB_PASSWORD")), "DDIP_DB_PASSWORD is required; do not put it in the plan")
    return approval_raw


def write_once(path, raw):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "wb") as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())
    parent_fd = os.open(Path(path).parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(parent_fd)
    finally:
        os.close(parent_fd)


def launch(mode, plan_path, approval_path):
    environment_gate(os.environ)
    raw, plan = validate(plan_path)
    approval = authorization(approval_path, raw, plan)
    # No effect above this line: no process, network request, database connection or output creation.
    evidence = Path(plan["evidenceDirectory"])
    evidence.mkdir(mode=0o700)
    write_once(evidence / "plan.json", raw)
    write_once(evidence / "approval.json", approval)
    write_once(evidence / "launch.json", json.dumps(dict(status="LAUNCH_REQUESTED", mode=mode,
               planSha256=hashlib.sha256(raw).hexdigest(), operationalActivation="NOT_AUTHORIZED")).encode())
    java = plan["artifacts"]["java"]["path"]
    command = [java, "-Dloader.path=" + plan["artifacts"]["runner"]["path"], "-Dloader.main=" + RUNNER,
               "-cp", plan["artifacts"]["api"]["path"], "org.springframework.boot.loader.launch.PropertiesLauncher",
               mode, str(evidence / "plan.json"), hashlib.sha256(raw).hexdigest()]
    env = dict(PATH=str(Path(java).parent) + ":/usr/bin:/bin", TZ="UTC", LANG="C.UTF-8",
               JAVA_HOME=plan["runtime"]["javaHome"], DDIP_DB_PASSWORD=os.environ["DDIP_DB_PASSWORD"],
               OPENBLAS_NUM_THREADS="1", OMP_NUM_THREADS="1")
    os.chdir(evidence)  # PropertiesLauncher cannot pick up a repository-local loader.properties.
    # Replace the launcher. No finally/atexit cleanup can kill a worker after an ambiguous result.
    os.execve(java, command, env)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("validate", "preflight", "run"))
    parser.add_argument("plan", type=Path)
    parser.add_argument("approval", type=Path, nargs="?")
    args = parser.parse_args(argv)
    try:
        if args.mode == "validate":
            need(args.approval is None, "Offline validation does not take an approval")
            raw, _ = validate(args.plan)
            print(json.dumps(dict(status="OFFLINE_INPUTS_VALID", planSha256=hashlib.sha256(raw).hexdigest(),
                                 publicExecution="NOT_RUN", packagedAcceptance="NOT_RUN")))
        else:
            need(args.approval is not None, "An actual owner-approved target record is required")
            launch(args.mode, args.plan, args.approval)
    except (Blocked, OSError, KeyError, zipfile.BadZipFile, ValueError) as error:
        # File/database/credential payloads and subprocess output never become diagnostics.
        reason = str(error) if isinstance(error, Blocked) else "Invalid or unavailable prerequisite"
        print(json.dumps(dict(status="BLOCKED", reason=reason, publicExecution="NOT_RUN")), file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
