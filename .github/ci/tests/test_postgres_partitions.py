"""Complete source inventory, actual Maven reports and four-way fail-closed fan-in."""
import importlib.util
import itertools
import json
from pathlib import Path
import shutil
import os
import subprocess
import tempfile
import unittest
import xml.etree.ElementTree as ET
from test_policy import ROOT, WORKFLOW, POSTGRES_WORKFLOW, block, run_script

SPEC = importlib.util.spec_from_file_location('partitions', ROOT / '.github/ci/postgres_partitions.py')
PARTITIONS = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(PARTITIONS)
CODE = run_script('content_postgres').split("<<'PY'\n", 1)[1].rsplit('\nPY', 1)[0]
SCOPE = {'__name__': 'test_postgres'}
exec(compile(CODE, 'ci.yml:content_postgres', 'exec'), SCOPE)
GATE = SCOPE['check_postgres']
JOBS = tuple('content_postgres_' + g for g in PARTITIONS.GROUPS)


def needs():
    return {'changes': {'result': 'success', 'outputs': {'classified': 'true', 'mode': 'full', 'content_postgres': 'true'}},
            **{j: {'result': 'success'} for j in JOBS}}


class PostgresGateTests(unittest.TestCase):
    def test_all_partition_terminal_combinations(self):
        for results in itertools.product(('success', 'failure', 'cancelled', 'skipped'), repeat=4):
            value = needs()
            for job, result in zip(JOBS, results):
                value[job]['result'] = result
            self.assertEqual(GATE(value), all(r == 'success' for r in results), results)

    def test_missing_malformed_or_unknown_required_partition_fails(self):
        for job in JOBS:
            for value in (None, {}, 'success', {'result': 'timed_out'}):
                n = needs(); n[job] = value
                self.assertFalse(GATE(n))
            n = needs(); del n[job]
            self.assertFalse(GATE(n))
        n = needs(); n['extra'] = {'result': 'success'}
        self.assertFalse(GATE(n))

    def test_bad_or_unselected_classifier_cannot_report_success(self):
        for key, value in (('classified', 'false'), ('mode', 'bad'), ('content_postgres', 'false')):
            n = needs(); n['changes']['outputs'][key] = value
            self.assertFalse(GATE(n))
        n = needs(); n['changes']['result'] = 'failure'
        self.assertFalse(GATE(n))

    def test_real_fan_in_shell_exit_status(self):
        bad = needs(); bad[JOBS[0]]['result'] = 'cancelled'
        for value, expected in ((json.dumps(needs()), 0), (json.dumps(bad), 1), ('{bad', 1)):
            result = subprocess.run(['bash', '-e', '-o', 'pipefail', '-c', run_script('content_postgres')],
                                    env={**os.environ, 'NEEDS_JSON': value}, capture_output=True)
            self.assertEqual(result.returncode, expected)

    def test_independent_jobs_preserve_logical_check_and_safe_skip(self):
        for group, job in zip(PARTITIONS.GROUPS, JOBS):
            body = block(job)
            self.assertIn('needs: changes', body)
            self.assertIn('uses: ./.github/workflows/postgres-partition.yml', body)
            self.assertIn('partition: ' + group, body)
            self.assertIn("needs.changes.outputs.content_postgres != 'false'", body)
        summary = block('content_postgres')
        for job in JOBS:
            self.assertIn(job, summary.split('    steps:')[0])
        self.assertIn("if: ${{ always() && (needs.changes.result != 'success' || needs.changes.outputs.content_postgres != 'false') }}", summary)
        self.assertIn('timeout-minutes: 90', POSTGRES_WORKFLOW)
        self.assertIn('ubuntu-24.04', POSTGRES_WORKFLOW)
        self.assertIn('"-DskipUnitTests=$skip_units"', POSTGRES_WORKFLOW)
        self.assertIn('if [ "$POSTGRES_PARTITION" = core ]; then skip_units=false; fi', POSTGRES_WORKFLOW)
        self.assertIn('--reports services/content-api/target/failsafe-reports', POSTGRES_WORKFLOW)
        self.assertNotIn('continue-on-error', POSTGRES_WORKFLOW)
        self.assertIn('run: python3 .github/ci/postgres_partitions.py', block('documentation'))


class PostgresInventoryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.groups = PARTITIONS.inventory()
        for relative in ('.github/ci/postgres-partitions.json', 'services/content-api/pom.xml'):
            target = self.root / relative; target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(ROOT / relative, target)
        for name in (*sum(self.groups.values(), []), 'platform.contentapi.GoldApiIT'):
            target = self.root / 'services/content-api/src/test/java' / (name.replace('.', '/') + '.java')
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text('package ' + name.rsplit('.', 1)[0] + ';\n')

    def manifest(self, change):
        path = self.root / '.github/ci/postgres-partitions.json'
        n = json.loads(path.read_text()); change(n); path.write_text(json.dumps(n))

    def test_real_inventory_and_default_maven_discovery_are_complete(self):
        self.assertEqual(PARTITIONS.inventory(self.root), self.groups)
        self.assertEqual(len(sum(self.groups.values(), [])), len(set(sum(self.groups.values(), []))))

    def test_new_integration_class_cannot_escape(self):
        for name in ('NewIT', 'ITNew', 'NewITCase'):
            path = self.root / ('services/content-api/src/test/java/platform/contentapi/' + name + '.java')
            path.write_text('package platform.contentapi;\n')
            with self.assertRaises(ValueError): PARTITIONS.inventory(self.root)
            path.unlink()

    def test_test_methods_outside_maven_names_cannot_escape(self):
        path = self.root / 'services/content-api/src/test/java/platform/contentapi/Unselected.java'
        path.write_text('package platform.contentapi;\n@Test void check() {}\n')
        with self.assertRaises(ValueError): PARTITIONS.inventory(self.root)

    def test_duplicate_or_missing_membership_fails(self):
        self.manifest(lambda n: n['core'].append(n['gold'][0]))
        with self.assertRaises(ValueError): PARTITIONS.inventory(self.root)

    def test_deleted_class_cannot_be_silently_selected(self):
        path = self.root / 'services/content-api/src/test/java' / (self.groups['gold'][0].replace('.', '/') + '.java')
        path.unlink()
        with self.assertRaises(ValueError): PARTITIONS.inventory(self.root)

    def test_omitted_manifest_class_fails(self):
        self.manifest(lambda n: n['core'].pop())
        with self.assertRaises(ValueError): PARTITIONS.inventory(self.root)

    def test_narrowed_default_postgres_discovery_fails(self):
        path = self.root / 'services/content-api/pom.xml'
        path.write_text(path.read_text().replace('**/*IT.java', '**/PersistenceIT.java'))
        with self.assertRaises(ValueError): PARTITIONS.inventory(self.root)

    def report(self, name, **attributes):
        directory = self.root / 'reports'; directory.mkdir(exist_ok=True)
        suite = ET.Element('testsuite', name=name, tests='1', **attributes)
        ET.SubElement(suite, 'testcase', classname=name, name='actualTest')
        ET.ElementTree(suite).write(directory / ('TEST-' + name + '.xml'))
        return directory

    def test_actual_success_reports_cover_exact_selection(self):
        for name in self.groups['gold']:
            directory = self.report(name)
        self.assertEqual(PARTITIONS.verify_reports(self.groups, 'gold', directory), len(self.groups['gold']))

    def test_duplicate_and_empty_reports_are_not_completion(self):
        directory = self.report(self.groups['gold'][0])
        original = next(directory.iterdir())
        shutil.copyfile(original, directory / 'TEST-duplicate.xml')
        with self.assertRaises(ValueError): PARTITIONS.verify_reports(self.groups, 'gold', directory)
        (directory / 'TEST-duplicate.xml').unlink()
        original.write_text('<testsuite name="' + self.groups['gold'][0] + '" tests="1"/>')
        with self.assertRaises(ValueError): PARTITIONS.verify_reports(self.groups, 'gold', directory)

    def test_missing_failed_cancelled_skipped_and_unexpected_reports_reject(self):
        directory = self.root / 'reports'; directory.mkdir()
        with self.assertRaises(ValueError): PARTITIONS.verify_reports(self.groups, 'gold', directory)
        for key in ('failures', 'errors', 'skipped'):
            self.report(self.groups['gold'][0], **{key: '1'})
            with self.assertRaises(ValueError): PARTITIONS.verify_reports(self.groups, 'gold', directory)
        self.report(self.groups['gold'][0])
        self.report(self.groups['core'][0])
        with self.assertRaises(ValueError): PARTITIONS.verify_reports(self.groups, 'gold', directory)


if __name__ == '__main__':
    unittest.main()
