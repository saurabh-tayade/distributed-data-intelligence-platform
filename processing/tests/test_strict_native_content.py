"""Strict framing, correspondence, dispatch, closure and cleanup contracts."""
import ast
from copy import deepcopy
from dataclasses import replace
from hashlib import sha256
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import MagicMock, patch

sys.path[:0] = [str(Path(__file__).resolve().parent), str(Path(__file__).resolve().parents[1])]
import narrow_evidence_sink as ledger
import narrow_startup_probe as startup
import strict_native_content as child
import strict_native_runtime as runtime
from narrow_transport_json import canonical_envelope
import projection_transport_fixtures as fixtures
import test_narrow_startup_probe as startup_fixtures


class StrictContractTest(unittest.TestCase):
    def setUp(self):
        self.fixture = startup_fixtures.NarrowStartupProbeTest()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.projection, gold, _, _, _ = fixtures.fixture()
        self.command = gold.value()
        value = self.fixture.value
        self.command.update(runId=value['runId'], attemptId=value['attemptId'], ownerEpoch=int(value['ownerEpoch']),
            checkpointId=ledger.checkpoint_id(value['runId'], value['attemptId'], 'PUBLISH_GOLD', 0))
        selected = self.command['inputs'][0]
        selected['originRunId'] = value['runId']
        selected['sourceId'] = value['sourceId']
        selected['receipt'].update(runId=value['runId'], attemptId=value['attemptId'], sourceId=value['sourceId'])
        selected['qualification']['selection']['identity']['sourceId'] = value['sourceId']
        selected['evidenceSha256'] = child._digest(selected['qualification'])
        self.command['inputSha256'] = child._digest(self.command['inputs'])
        compact = {key: self.projection[key] for key in ('contentRuntime', 'goldRuntime', 'silverRuntime', 'bindings')}
        compact.update(silverInputSha256=child._digest(selected), referenceSha256=None, goldProofSha256=None)
        self.bootstrap = value | dict(bootstrapVersion=2, mode='STRICT_CONTENT', family='GOLD', nativeMode='PUBLISH',
            stage='PUBLISH_GOLD', ordinal=0, checkpointId=self.command['checkpointId'],
            runtimeSha256=self.projection['readyContentRuntimeSha256'], nativeRuntimeSha256=self.command['runtimeSha256'],
            strictRuntimeSha256=child.STRICT_RUNTIME_SHA256, expectations=compact, sourceId=selected['sourceId'],
            commandSha256=child._digest(self.command))

    def parse(self, value=None):
        return child.parse_bootstrap(canonical_envelope(self.bootstrap if value is None else value, 65535))

    def validate(self, command=None, bootstrap=None):
        return child.validate_command(canonical_envelope(self.command if command is None else command, 1024**2),
                                      self.bootstrap if bootstrap is None else bootstrap)

    def test_compact_bootstrap_and_full_native_expectations_remain_distinct(self):
        bootstrap, expected, current = self.parse()
        command, bound, full = self.validate()
        self.assertEqual(expected.writer, current.writer)
        self.assertEqual(command, bound.value())
        self.assertEqual(full['original']['silverInput'], command['inputs'][0])
        self.assertEqual(full['readyContentRuntimeSha256'], bootstrap['runtimeSha256'])
        self.assertNotEqual(bootstrap['runtimeSha256'], command['runtimeSha256'])
        self.assertNotIn('silverInput', bootstrap['expectations'])
        self.assertLess(len(canonical_envelope(bootstrap, 65535)), 8192)

    def test_malformed_identity_scalar_unknown_field_and_wrong_slot_fail_before_io(self):
        variants = [self.bootstrap | {key: replacement} for key, replacement in (
            ('bootstrapVersion', True), ('mode', 'READ_ONLY_PROBE'), ('family', 'RECOVERY'),
            ('nativeMode', 'RECHECK'), ('ordinal', False), ('ordinal', 1), ('stage', 'SILVER'),
            ('childPid', str(os.getpid() + 1)), ('strictRuntimeSha256', 'f' * 64),
            ('checkpointId', fixtures.ARTIFACT), ('extra', None), ('ownerEpoch', 1))]
        for key in self.bootstrap:
            changed = deepcopy(self.bootstrap)
            del changed[key]
            variants.append(changed)
        for value in variants:
            with self.subTest(value=value.get('family')), patch.object(Path, 'lstat', side_effect=AssertionError('IO')):
                with self.assertRaises(Exception):
                    self.parse(value)

    def test_no_eof_no_native_work_or_path_inspection(self):
        read, write = os.pipe()
        try:
            os.write(write, canonical_envelope(self.bootstrap, 65535))
            with patch.object(startup, 'BOOTSTRAP_TIMEOUT_SECONDS', 0.05), patch.object(child, 'EffectSink') as sink:
                with self.assertRaises(startup.StartupError):
                    child.run(read)
                sink.assert_not_called()
        finally:
            os.close(read)
            os.close(write)

    def test_argv_never_supplies_a_command_and_error_never_echoes_secrets(self):
        from io import StringIO
        error = StringIO()
        with patch.object(sys, 'argv', ['strict_native_content', '/secret-command']), \
             patch.object(sys, 'stderr', error), patch.object(child, 'run') as run:
            self.assertEqual(child.main(), 1)
        run.assert_not_called()
        self.assertEqual(error.getvalue(), child.ERROR + '\n')

    def test_command_aliases_are_exact_not_arbitrary_valid_slot(self):
        for mode, stage, ordinal, origin, origin_ordinal in (
            ('PREPARE', 'PREPARE_GOLD', 0, 'PREPARE_GOLD', 0),
            ('VERIFY', 'VERIFY_RESULT', 0, 'PUBLISH_GOLD', 0),
            ('RECHECK', 'CHECK_INPUTS', 0, 'PUBLISH_GOLD', 0)):
            command = deepcopy(self.command)
            command.update(mode=mode, checkpointId=ledger.checkpoint_id(command['runId'], command['attemptId'], origin, origin_ordinal))
            if mode == 'RECHECK':
                command['verified'] = fixtures.fixture()[2]
            bootstrap = self.bootstrap | dict(nativeMode=mode, stage=stage, ordinal=ordinal,
                checkpointId=ledger.checkpoint_id(command['runId'], command['attemptId'], stage, ordinal))
            self.validate(command, bootstrap)
            command['checkpointId'] = fixtures.ARTIFACT
            with self.assertRaises(Exception):
                self.validate(command, bootstrap)

    def test_component_substitution_and_legacy_commands_cannot_rebuild_expectations(self):
        for field in ('inputs', 'silver', 'gold', 'runtime', 'runtimeSha256', 'protocolVersion', 'projectionContext'):
            command = deepcopy(self.command)
            if field == 'inputs':
                command[field][0]['receipt']['ingestedAt'] = 'changed'
                command['inputSha256'] = child._digest(command['inputs'])
            elif field in {'silver', 'gold'}:
                command[field]['rootInode'] = '999'
            elif field == 'runtime':
                command[field]['javaHome'] = '/wrong'
            elif field == 'runtimeSha256':
                command[field] = 'f' * 64
            elif field == 'protocolVersion':
                command[field] = 1
            else:
                command[field]['outputBindingId'] = 'wrong'
            with self.subTest(field=field), self.assertRaises(Exception):
                self.validate(command)

    def test_import_closure_includes_lazy_imports_and_excludes_ledger1_writer(self):
        root = Path(child.__file__).parent
        todo, observed = {'strict_native_content'}, set()
        while todo:
            name = todo.pop()
            if name in observed:
                continue
            observed.add(name)
            for node in ast.walk(ast.parse((root / (name + '.py')).read_text())):
                modules = ([alias.name.split('.')[0] for alias in node.names] if isinstance(node, ast.Import)
                           else [node.module.split('.')[0]] if isinstance(node, ast.ImportFrom) and node.module else [])
                todo.update(module for module in modules if (root / (module + '.py')).exists() and module not in observed)
        self.assertEqual({name.removesuffix('.py') for name in child.MODULES}, observed)
        self.assertNotIn('bounded_evidence', observed)
        self.assertNotIn('bounded_content_producer', observed)
        self.assertEqual(len(child.MODULES), len(set(child.MODULES)))

    def test_cleanup_requires_reaped_zero_not_just_shutdown_claim(self):
        cleanup = runtime.Cleanup()
        self.assertEqual(cleanup.value(), dict(status='NOT_STARTED', gatewayPid=None))
        for code in (None, 1, -9):
            gateway = MagicMock()
            gateway.proc.pid = 123
            gateway.proc.poll.return_value = code
            with self.assertRaises(Exception):
                cleanup.observe(gateway)
            self.assertEqual(cleanup.value(), dict(status='UNCONFIRMED', gatewayPid='123'))
        gateway.proc.poll.return_value = 0
        cleanup.observe(gateway)
        self.assertEqual(cleanup.value(), dict(status='CONFIRMED', gatewayPid='123'))

    def test_registered_bronze_binding_version_is_preserved_and_exact(self):
        from types import SimpleNamespace
        import bronze_to_silver
        config = dict(importBronzeVersion=1, endpoint='http://127.0.0.1:9000', region='local',
                      bucket='local-bucket', credentialsFile='/private/credentials.json')
        credentials = SimpleNamespace(value=dict(accessKey='local-test', secretKey='local-test-secret'))
        with patch.object(bronze_to_silver, 'local_client', return_value=('client', 'local-bucket')) as client:
            self.assertEqual(child._client(config, credentials), ('client', 'local-bucket'))
            client.assert_called_once()
            for version in (True, 0, 2, '1'):
                with self.assertRaises(Exception):
                    child._client(config | dict(importBronzeVersion=version), credentials)
            with self.assertRaises(Exception):
                child._client({key: value for key, value in config.items() if key != 'importBronzeVersion'}, credentials)
            with self.assertRaises(Exception):
                child._client(config | dict(unexpected='field'), credentials)
            self.assertEqual(client.call_count, 1)

    def test_launcher_ignores_timestamp_valid_unregistered_bytecode(self):
        import py_compile
        import subprocess
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            module = root / 'registered_source.py'
            module.write_text("VALUE = 'stale'\n")
            stamp = module.stat()
            py_compile.compile(str(module), doraise=True)
            module.write_text("VALUE = 'fresh'\n")
            os.utime(module, ns=(stamp.st_atime_ns, stamp.st_mtime_ns))
            script = 'import sys;sys.path.insert(0,sys.argv[1]);import registered_source;print(registered_source.VALUE)'
            baseline = subprocess.run([sys.executable, '-I', '-B', '-c', script, str(root)],
                                      capture_output=True, check=True)
            self.assertEqual(baseline.stdout, b'stale\n')
            observed = subprocess.run([sys.executable, '-I', '-B', '-X', 'pycache_prefix=/dev/null',
                                       '-c', script, str(root)], capture_output=True, check=True)
            self.assertEqual(observed.stdout, b'fresh\n')

    def test_verify_and_recheck_select_readonly_entry_points(self):
        from contextlib import contextmanager
        import narrow_gold_publication_v2 as native
        @contextmanager
        def spark(*_):
            yield object()
        for mode in ('VERIFY', 'RECHECK'):
            stage = 'VERIFY_RESULT' if mode == 'VERIFY' else 'CHECK_INPUTS'
            bootstrap = self.bootstrap | dict(nativeMode=mode, stage=stage)
            command = deepcopy(self.command) | dict(mode=mode)
            if mode == 'RECHECK':
                command['verified'] = fixtures.fixture()[2]
            _, bound, expectations = self.validate(command, bootstrap)
            with patch.object(child, 'spark_session', spark), patch.object(native, 'publish_v2') as publish, \
                 patch.object(native, 'verify_publication_v2', return_value=fixtures.fixture()[2]) as verify, \
                 patch.object(native, 'recheck_publication_v2') as recheck:
                result = child.execute_native(command, bootstrap, bound, expectations, None, runtime.Cleanup())
                publish.assert_not_called()
                (verify if mode == 'VERIFY' else recheck).assert_called_once()
                self.assertEqual(result[0], 'VERIFIED' if mode == 'VERIFY' else 'UNCHANGED')


if __name__ == '__main__':
    unittest.main()
