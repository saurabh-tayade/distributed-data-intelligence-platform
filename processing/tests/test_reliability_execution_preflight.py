"""Gate A: real native readers and actual pinned packages; no Spark session/publication."""
from copy import deepcopy
from hashlib import sha256
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch
from publication_metadata import canonical_bytes
from reliability_preflight_fixtures import create, load
import artifact_import_verifier as capture


class PreflightTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.base = Path(self.tmp.name).resolve()
        self.f = create(self.base/'fixture')
        self.p = load(self.f['command']['runtime'])

    def run_preflight(self, f=None):
        f = self.f if f is None else f
        return load(f['command']['runtime']).qualify(f['command'], f['bundle'])

    def fail(self, code=None):
        with self.assertRaises(Exception) as caught:
            self.run_preflight()
        actual = getattr(caught.exception, 'code', '')
        if code:
            self.assertEqual(actual, code)
        self.assertNotIn('/private/', str(caught.exception))
        return actual

    def refresh(self):
        c, b = self.f['command'], self.f['bundle']
        c['capturedConfigSha256'] = sha256(canonical_bytes(b['capturedSelection'])).hexdigest()
        c['bundleSha256'] = sha256(canonical_bytes(b)).hexdigest()
        c['runtime']['profileSha256'] = sha256(canonical_bytes(c['runtime']['configuration'])).hexdigest()

    def native_file(self, name='data/events.jsonl', ref=False):
        f = self.f['bundle']['capturedSelection']
        item = next(n for n in f['inputs'] if (n['kind'] == 'SERVICE_REFERENCE') == ref)
        return Path(self.f['bundle']['bindings']['reference' if ref else 'batch']['root'])/item['relativeAddress']/name

    def test_valid_jsonl_and_literal_native_dependency_identity(self):
        r = self.run_preflight()
        self.assertEqual(r['counts'], dict(files='1', bytes='2439', rows='10'))
        self.assertEqual(r['nativeDependencyFingerprint'], '3b9354d578f017cefea8e9ace2556c6ed474afec606722ee01e9fba8e4a39a7a')
        self.assertEqual(r['runtime'], self.p.VERSIONS)
        self.assertEqual(self.p.recheck(self.f['command'],self.f['bundle'],r),r)

    def test_valid_parquet_and_physical_provenance(self):
        f = create(self.base/'parquet', 'PARQUET')
        r = self.run_preflight(f)
        self.assertEqual(r['inputFormat'], 'PARQUET')
        self.assertEqual(r['counts']['rows'], '10')
        self.assertEqual(r['nativeDependencies']['semantic_config']['raw_decoder']['duckdb'], '1.4.5')

    def test_complete_multi_batch_selection_and_order_determinism(self):
        f = create(self.base/'multi', multi=True)
        a = self.run_preflight(f)
        f['bundle']['capturedSelection']['inputs'].reverse()
        f['command']['capturedConfigSha256'] = sha256(canonical_bytes(f['bundle']['capturedSelection'])).hexdigest()
        b = self.run_preflight(f)
        self.assertEqual(a['nativeDependencyFingerprint'], b['nativeDependencyFingerprint'])
        self.assertEqual(a['counts'], dict(files='2', bytes='4878', rows='20'))
        self.assertEqual(len(a['nativeDependencies']['batches']), 2)

    def test_actual_joint_native_interface_is_mandatory(self):
        import reliability_format_inputs as native
        with patch.object(native, 'verify_dependencies', wraps=native.verify_dependencies) as called:
            self.run_preflight()
        self.assertEqual(called.call_count, 1)

    def test_no_native_writes_or_mtime_changes_or_publisher_calls(self):
        roots = [Path(b['root']) for b in self.f['bundle']['bindings'].values()]
        def snapshot():
            return {str(p):(p.read_bytes() if p.is_file() else None,p.stat().st_mtime_ns) for r in roots for p in (r,*r.rglob('*'))}
        before = snapshot()
        with patch('reliability_format_publication.publish', side_effect=AssertionError('NO_PUBLICATION')), patch('reliability_format_inputs.stage',side_effect=AssertionError('NO_STAGING')):
            self.run_preflight()
        self.assertEqual(snapshot(), before)

    def test_changed_module_digest(self):
        root = Path(self.f['command']['runtime']['configuration']['processingRoot'])
        with (root/'reliability_format_inputs.py').open('ab') as stream:
            stream.write(b'\n# changed\n')
        self.fail('RUNTIME_MISMATCH')

    def test_changed_deployment_evidence(self):
        root = Path(self.f['command']['runtime']['configuration']['processingRoot'])
        (root/'e5-reliability-deployment.json').write_bytes(b'{}')
        self.fail('INPUT_INVALID')

    def test_missing_deployment_module(self):
        root = Path(self.f['command']['runtime']['configuration']['processingRoot'])
        (root/'service_reference.py').unlink()
        self.fail('EVIDENCE_UNAVAILABLE')

    def test_missing_executable(self):
        self.f['command']['runtime']['configuration']['python'] = str(self.base/'missing')
        self.refresh()
        self.fail('RUNTIME_MISMATCH')

    def test_wrong_python_version(self):
        with patch.object(sys, 'version_info', (3, 11, 0)):
            self.fail('RUNTIME_MISMATCH')

    def test_wrong_pyspark_and_duckdb_versions(self):
        import pyspark, duckdb
        for module in (pyspark,duckdb):
            with self.subTest(module=module.__name__), patch.object(module,'__version__','0.0.0'):
                self.fail('RUNTIME_MISMATCH')

    def test_wrong_frozen_profile_digest(self):
        self.f['bundle']['capturedSelection']['executionProfile']['configurationSha256'] = '0'*64
        self.refresh()
        self.fail('RUNTIME_MISMATCH')

    def test_wrong_captured_configuration_digest(self):
        self.f['command']['capturedConfigSha256'] = '0'*64
        self.fail('INPUT_INVALID')

    def test_root_identity_mismatch(self):
        self.f['bundle']['bindings']['batch']['rootInode'] = '0'
        self.fail('EVIDENCE_CHANGED')

    def test_unapproved_writable_root(self):
        self.f['bundle']['bindings']['output'] = self.f['bundle']['bindings']['temp']
        self.fail('EVIDENCE_CHANGED')

    def test_symlink_escape(self):
        p = self.native_file()
        target = self.base/'outside'
        p.rename(target)
        p.symlink_to(target)
        self.fail('PATH_UNSAFE')

    def test_missing_raw_is_unavailable(self):
        self.native_file().unlink()
        self.assertIn(self.fail(), ('EVIDENCE_CHANGED','EVIDENCE_UNAVAILABLE'))

    def test_corrupt_or_substituted_raw_after_import_is_changed(self):
        path = self.native_file()
        path.write_bytes(b'!'+path.read_bytes()[1:])
        self.fail('EVIDENCE_CHANGED')

    def test_missing_reference_dependency(self):
        self.native_file('vocabulary.json', True).unlink()
        self.fail('EVIDENCE_CHANGED')

    def test_corrupt_reference_completion(self):
        self.native_file('complete.json', True).write_bytes(b'{}')
        self.fail('EVIDENCE_CHANGED')

    def test_wrong_source_association(self):
        self.f['bundle']['capturedSelection']['sources'][0]['sourceId'] = 'other'
        self.refresh()
        self.fail('INPUT_INVALID')

    def test_wrong_reference_association(self):
        item = next(n for n in self.f['bundle']['capturedSelection']['inputs'] if n['kind']=='SERVICE_REFERENCE')
        item['qualification']['selection']['identity']['revision'] = 'other'
        self.refresh()
        self.fail('INPUT_INVALID')

    def test_mixed_format_rejected(self):
        f = create(self.base/'mixed', 'PARQUET')
        self.f['bundle']['capturedSelection']['inputs'].insert(0,f['bundle']['capturedSelection']['inputs'][0])
        self.refresh()
        self.fail('INPUT_INVALID')

    def test_wrong_contract_version(self):
        item = self.f['bundle']['capturedSelection']['inputs'][0]
        item['qualification']['selection']['contractVersion'] = 1
        item['evidenceSha256'] = sha256(canonical_bytes(item['qualification'])).hexdigest()
        self.refresh()
        self.fail('ADAPTER_INVALID')

    def test_modified_registered_evidence_fails_closed(self):
        item = self.f['bundle']['capturedSelection']['inputs'][0]
        item['qualification']['verification']['native']['inventory'][0]['record_count'] += 1
        self.refresh()
        self.fail('INPUT_INVALID')

    def test_observed_change_during_qualification(self):
        import reliability_format_inputs as native
        old = native.verify_dependencies
        def change(**kwargs):
            result = old(**kwargs)
            path = self.native_file()
            path.write_bytes(path.read_bytes()+b'\n')
            return result
        with patch.object(native,'verify_dependencies',side_effect=change):
            self.fail('DEPENDENCY_CHANGED')

    def test_disk_admission_reserve(self):
        class Full:
            f_bavail = 1
            f_frsize = 1
        with patch.object(os,'statvfs',return_value=Full()):
            self.fail('RESOURCE_LIMIT')

    def test_native_resource_limits_unchanged(self):
        import reliability_format_metadata as m
        self.assertEqual((m.MAX_BATCHES,m.MAX_INPUT_FILES,m.MAX_RAW,m.MAX_ROWS), (32,128,1024**3,2_000_000))
        from reliability_format_parquet import connection
        with connection() as db:
            self.assertEqual(db.execute("SELECT current_setting('threads'),current_setting('temp_directory'),current_setting('autoload_known_extensions')").fetchone(), (2,'',False))

    def test_fixed_subprocess_result_and_no_raw_streams(self):
        stage = self.base/'stage'
        stage.mkdir(mode=0o700)
        for name, n in [('command.json',self.f['command']),('selection.json',self.f['bundle'])]:
            path=stage/name
            path.write_bytes(canonical_bytes(n))
            path.chmod(0o600)
        rc=self.f['command']['runtime']['configuration']
        result=subprocess.run([sys.executable,'-I','-B',str(Path(rc['processingRoot'])/'reliability_execution_preflight.py'),str(stage/'command.json')],
                              env=dict(PATH='/usr/bin:/bin',TZ='UTC',LANG='C.UTF-8'),stdin=subprocess.DEVNULL,capture_output=True,timeout=15)
        self.assertEqual(result.returncode,0,result.stderr)
        self.assertEqual((result.stdout,result.stderr),(b'',b''))
        r=json.loads((stage/'result.json').read_bytes())
        self.assertEqual(r['status'],'OK')
        self.assertEqual(r['evidenceId'],self.f['command']['checkpointId'])
        self.assertNotIn('reliability_format_processing', sys.modules)

    def test_strict_command_and_canonical_protocol(self):
        for raw in (b'{"x":1,"x":2}',b'{"x":1.0}',b'{} {}',b'\xef\xbb\xbf{}'):
            with self.assertRaises(self.p.Failure):
                self.p.parse(raw,65536)
        self.f['command']['arbitraryExecutable']='PRIVATE'
        self.fail('INPUT_INVALID')


if __name__ == '__main__':
    unittest.main()
