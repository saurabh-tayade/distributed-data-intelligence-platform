"""Full Python discovery, explicit module coverage and two-way fail-closed fan-in."""
import importlib.util
import io
import itertools
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import Mock
from test_policy import ROOT, WORKFLOW, block, run_script

SPEC = importlib.util.spec_from_file_location('python_partitions', ROOT / '.github/ci/python_partitions.py')
PARTITIONS = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(PARTITIONS)
CODE = run_script('processing_python').split("<<'PY'\n", 1)[1].rsplit('\nPY', 1)[0]
SCOPE = {'__name__': 'test_python'}
exec(compile(CODE, 'ci.yml:processing_python', 'exec'), SCOPE)
GATE = SCOPE['check_python']
JOBS = tuple('processing_python_' + group for group in PARTITIONS.GROUPS)


def needs():
    return {'changes': {'result': 'success', 'outputs': {'classified': 'true', 'mode': 'full', 'processing_python': 'true'}},
            **{job: {'result': 'success'} for job in JOBS}}


class PythonGateTests(unittest.TestCase):
    def test_every_terminal_combination(self):
        for mode in ('full', 'selective'):
            for results in itertools.product(('success', 'failure', 'cancelled', 'skipped'), repeat=len(JOBS)):
                value = needs(); value['changes']['outputs']['mode'] = mode
                for job, result in zip(JOBS, results):
                    value[job]['result'] = result
                self.assertEqual(GATE(value), all(r == 'success' for r in results), (mode, results))

    def test_missing_unknown_and_malformed_results_fail(self):
        for job in JOBS:
            for value in (None, {}, 'success', [], *({'result': r} for r in ('timed_out', 'unknown', 'neutral', 'pending', '', None, True, [], {}))):
                n = needs(); n[job] = value
                self.assertFalse(GATE(n))
            n = needs(); del n[job]
            self.assertFalse(GATE(n))
        n = needs(); n['extra'] = {'result': 'success'}
        self.assertFalse(GATE(n))
        for malformed in (None, [], {}, 'success'):
            self.assertFalse(GATE(malformed))

    def test_classifier_must_succeed_and_select_partition(self):
        for result in ('failure', 'cancelled', 'skipped', 'timed_out', '', None):
            n = needs(); n['changes']['result'] = result
            self.assertFalse(GATE(n))
        for key in ('classified', 'mode', 'processing_python'):
            for value in ('false', '', 'unexpected', None, True, {}, []):
                n = needs(); n['changes']['outputs'][key] = value
                self.assertFalse(GATE(n))
            n = needs(); del n['changes']['outputs'][key]
            self.assertFalse(GATE(n))
        for value in (None, [], {}, 'success'):
            n = needs(); n['changes'] = value
            self.assertFalse(GATE(n))
            n = needs(); n['changes']['outputs'] = value
            self.assertFalse(GATE(n))

    def test_real_fan_in_shell_exit_status(self):
        bad = needs(); bad[JOBS[1]]['result'] = 'cancelled'
        for value, expected in ((json.dumps(needs()), 0), (json.dumps(bad), 1), ('{bad', 1)):
            result = subprocess.run(['bash', '-e', '-o', 'pipefail', '-c', run_script('processing_python')],
                                    env={**os.environ, 'NEEDS_JSON': value}, capture_output=True)
            self.assertEqual(result.returncode, expected)

    def test_independent_execution_jobs_and_unchanged_logical_gate(self):
        condition = "if: ${{ always() && (needs.changes.result != 'success' || needs.changes.outputs.processing_python != 'false') }}"
        for group, job in zip(PARTITIONS.GROUPS, JOBS):
            body = block(job)
            self.assertIn('needs: changes', body)
            self.assertIn(condition, body)
            self.assertIn('runs-on: ubuntu-24.04', body)
            self.assertIn('timeout-minutes: 30', body)
            self.assertIn("python-version: '3.12.15'", body)
            self.assertIn('python -m pip install -r processing/requirements-bronze.txt -r processing/requirements-analytics.txt', body)
            self.assertIn('export JAVA_HOME="$JAVA_HOME_21_X64"', body)
            self.assertIn('export PYSPARK_PYTHON="$(command -v python)"', body)
            self.assertIn('SPARK_LOCAL_IP: 127.0.0.1\n      TZ: UTC\n      PYTHONPATH: processing', body)
            self.assertIn(f'python .github/ci/python_partitions.py --partition {group} --run', body)
            self.assertNotIn('continue-on-error', body)
        self.assertEqual(tuple(line.strip().removesuffix(':') for line in WORKFLOW.splitlines()
                               if line.startswith('  processing_python_') and line.endswith(':')), JOBS)
        gate = block('processing_python')
        self.assertIn(condition, gate)
        self.assertIn('name: Python 3.12 processing, Spark and DuckDB', gate)
        self.assertIn('needs: [changes, processing_python_core, processing_python_reliability]', gate)
        self.assertIn('run: python3 .github/ci/python_partitions.py', block('documentation'))
        self.assertIn('python -m unittest discover -s processing/tests -v', (ROOT / 'docs/operations/ci.md').read_text())


class PythonInventoryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.directory = self.root / 'processing/tests'; self.directory.mkdir(parents=True)
        self.manifest = self.root / '.github/ci/python-partitions.json'; self.manifest.parent.mkdir(parents=True)
        self.groups = {'core': ['test_example'], 'reliability': ['test_reliability_example']}
        self.manifest.write_text(json.dumps(self.groups))
        self.source = 'import unittest\nclass Example(unittest.TestCase):\n    def test_passes(self): pass\n'
        for module in sum(self.groups.values(), []):
            (self.directory / (module + '.py')).write_text(self.source)
        shutil.copyfile(ROOT / '.github/ci/python_partitions.py', self.manifest.with_name('python_partitions.py'))

    def test_repository_inventory_is_disjoint_exhaustive_and_module_level(self):
        groups = PARTITIONS.inventory()
        self.assertEqual({k: len(v) for k, v in groups.items()}, {'core': 33, 'reliability': 14})
        self.assertFalse(set(groups['core']) & set(groups['reliability']))
        self.assertEqual(set(sum(groups.values(), [])), {p.stem for p in (ROOT / 'processing/tests').glob('test*.py')})
        self.assertEqual(PARTITIONS.inventory(self.root), self.groups)

    def test_missing_extra_duplicate_malformed_and_misassigned_inventory(self):
        for value in (None, [], {}, {**self.groups, 'extra': ['test_extra']},
                      {'core': self.groups['core']}, {**self.groups, 'core': []},
                      {**self.groups, 'core': 'test_example'}, {**self.groups, 'core': [1]},
                      {**self.groups, 'core': ['test_example.test_passes']},
                      {**self.groups, 'core': ['test_example', 'test_example']},
                      {'core': self.groups['reliability'], 'reliability': self.groups['core']},
                      {**self.groups, 'core': ['test_other']},
                      {**self.groups, 'core': ['test_example', 'test_other']}):
            with self.subTest(value=value):
                self.manifest.write_text(json.dumps(value))
                with self.assertRaises(ValueError): PARTITIONS.inventory(self.root)
        self.manifest.write_text('{"core":["test_example"],"core":["test_example"],"reliability":["test_reliability_example"]}')
        with self.assertRaises(ValueError): PARTITIONS.inventory(self.root)

    def test_new_deleted_and_nested_test_sources_cannot_escape(self):
        for name in ('test_new.py', 'testReliability.py', 'package/test_new.py'):
            path = self.directory / name; path.parent.mkdir(exist_ok=True)
            path.write_text(self.source)
            with self.assertRaises(ValueError): PARTITIONS.inventory(self.root)
            path.unlink()
        (self.directory / 'test_example.py').unlink()
        with self.assertRaises(ValueError): PARTITIONS.inventory(self.root)

    def command(self, *args):
        return subprocess.run([sys.executable, '-I', '-B', str(self.manifest.with_name('python_partitions.py')), *args],
                              cwd=self.root, capture_output=True, text=True)

    def test_script_preserves_repository_namespace_imports(self):
        (self.root / 'processing' / 'helper.py').write_text('VALUE = 7\n')
        (self.directory / 'test_example.py').write_text(
            'from processing.helper import VALUE\n' + self.source.replace(
                'def test_passes(self): pass', 'def test_passes(self): self.assertEqual(VALUE, 7)'))
        result = self.command('--partition', 'core', '--run')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('core: 1 passing Python cases', result.stdout)

    def test_real_full_discovery_is_disjoint_exhaustive_and_successful(self):
        for group in PARTITIONS.GROUPS:
            result = self.command('--partition', group, '--run')
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn('Complete Python discovery: core=1, reliability=1', result.stdout)
            self.assertIn(f'{group}: 1 passing Python cases', result.stdout)
            self.assertIn('Ran 1 test', result.stderr)
        result = self.command('--discover')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertNotIn('Ran 1 test', result.stderr)
        self.assertIn('Complete Python discovery: core=1, reliability=1', result.stdout)

    def test_full_discovery_failure_in_other_partition_also_blocks(self):
        for source in ('raise ImportError("unavailable")\n', 'import unittest\n',
                       self.source + '\ndef load_tests(loader, tests, pattern):\n    return unittest.TestSuite([tests, tests])\n'):
            (self.directory / 'test_reliability_example.py').write_text(source)
            result = self.command('--partition', 'core', '--run')
            self.assertNotEqual(result.returncode, 0, result.stderr)
            self.assertNotIn('passing Python cases', result.stdout)

    def test_failures_errors_skips_expected_failures_and_missing_execution_fail(self):
        sources = (
            self.source.replace(': pass', ': self.fail("failed")'),
            self.source.replace(': pass', ': raise RuntimeError("failed")'),
            self.source.replace(': pass', ': self.skipTest("skipped")'),
            self.source.replace('class Example', '@unittest.skip("class skipped")\nclass Example'),
            self.source.replace('    def test_passes', '    @unittest.expectedFailure\n    def test_passes'),
            self.source.replace('    def test_passes', '    @unittest.expectedFailure\n    def test_passes').replace(': pass', ': self.fail("expected failure")'),
            self.source + '    def run(self, result): return result\n',
        )
        for source in sources:
            with self.subTest(source=source):
                (self.directory / 'test_example.py').write_text(source)
                result = self.command('--partition', 'core', '--run')
                self.assertNotEqual(result.returncode, 0)
                self.assertNotIn('passing Python cases', result.stdout)

    def test_execution_arguments_cannot_bypass_validation(self):
        for args in (('--run',), ('--partition', 'core', '--run', '--discover'), ('--partition', 'unknown', '--run')):
            self.assertNotEqual(self.command(*args).returncode, 0)


class PythonExecutionCoverageTests(unittest.TestCase):
    def test_actual_full_discovery_uses_unchanged_pattern_and_counts_every_case_once(self):
        groups = {'core': ['test_example'], 'reliability': ['test_reliability_example']}
        tests = [type('Example', (unittest.TestCase,), {'__module__': name, 'test_passes': lambda self: None})('test_passes')
                 for name in sum(groups.values(), [])]
        loader = Mock(errors=[])
        loader.discover.return_value = unittest.TestSuite([unittest.TestSuite(tests)])
        selected = PARTITIONS.discover(groups, ROOT, loader)
        loader.discover.assert_called_once_with(str(ROOT / 'processing/tests'))
        self.assertEqual(selected, {'core': tests[:1], 'reliability': tests[1:]})
        for bad_tests in (tests + tests[:1], tests[:1], [unittest.FunctionTestCase(lambda: None)]):
            loader.discover.return_value = unittest.TestSuite(bad_tests)
            with self.assertRaises(ValueError): PARTITIONS.discover(groups, ROOT, loader)
        loader.errors = ['import failed']
        with self.assertRaises(ValueError): PARTITIONS.discover(groups, ROOT, loader)

    def test_result_must_match_exact_discovered_case_identities(self):
        test = unittest.FunctionTestCase(lambda: None)
        result = unittest.TextTestRunner(stream=io.StringIO(), resultclass=PARTITIONS.CompleteResult).run(unittest.TestSuite([test]))
        self.assertEqual(PARTITIONS.verify_result([test], result), 1)
        for successful in ([], ['unexpected'], [test.id(), test.id()]):
            result.successful_ids = successful
            with self.assertRaises(ValueError): PARTITIONS.verify_result([test], result)
        result.successful_ids = [test.id()]
        result.testsRun = 0
        with self.assertRaises(ValueError): PARTITIONS.verify_result([test], result)
        with self.assertRaises(ValueError): PARTITIONS.verify_result([], result)
        with self.assertRaises(ValueError): PARTITIONS.verify_result([test, test], result)


if __name__ == '__main__':
    unittest.main()
