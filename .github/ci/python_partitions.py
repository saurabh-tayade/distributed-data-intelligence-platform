"""Disjoint, exhaustive Python module inventory and fail-closed unittest execution.

Local ``python -m unittest discover -s processing/tests -v`` remains unchanged.
Each CI partition uses that same full discovery before selecting whole modules.
"""
import argparse
from collections import Counter
import json
from pathlib import Path
import re
import sys
import unittest

ROOT = Path(__file__).resolve().parents[2]
# Match python -m unittest's repository-root import path when this is run as a file.
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
GROUPS = ('core', 'reliability')


def inventory(root=ROOT):
    def unique(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError('duplicate partition key')
            result[key] = value
        return result

    groups = json.loads((root / '.github/ci/python-partitions.json').read_text(), object_pairs_hook=unique)
    if not isinstance(groups, dict) or set(groups) != set(GROUPS):
        raise ValueError('unexpected Python partition inventory')
    selected = []
    for group in GROUPS:
        modules = groups[group]
        if not isinstance(modules, list) or not modules or any(
            not isinstance(m, str) or not re.fullmatch(r'test[A-Za-z0-9_]*', m) for m in modules
        ):
            raise ValueError('invalid Python partition modules')
        if any(m.startswith('test_reliability_') != (group == 'reliability') for m in modules):
            raise ValueError('module assigned outside its explicit Reliability/core boundary')
        selected.extend(modules)
    if len(selected) != len(set(selected)):
        raise ValueError('Python module assigned more than once')
    directory = root / 'processing/tests'
    found = set()
    for path in directory.rglob('test*.py'):
        # Nested packages need an intentional discovery/assignment update, never silent omission.
        if path.parent != directory:
            raise ValueError('nested Python test source requires explicit discovery support: ' + str(path))
        found.add(path.stem)
    if found != set(selected):
        raise ValueError('unassigned, removed or unexpected Python test module: ' + ','.join(sorted(found ^ set(selected))))
    return groups


def flatten(suite):
    for test in suite:
        if isinstance(test, unittest.TestSuite):
            yield from flatten(test)
        else:
            yield test


def discover(groups, root=ROOT, loader=None):
    loader = loader or unittest.TestLoader()
    # This is the unchanged local discovery pattern and start directory, not a narrower glob.
    tests = list(flatten(loader.discover(str(root / 'processing/tests'))))
    if loader.errors:
        raise ValueError('complete Python discovery failed:\n' + '\n'.join(loader.errors))
    modules = {module: group for group in GROUPS for module in groups[group]}
    selected = {group: [] for group in GROUPS}
    seen = set()
    seen_modules = set()
    for test in tests:
        module = type(test).__module__
        identifier = test.id()
        if module not in modules or identifier in seen:
            raise ValueError('unexpected or duplicate discovered Python test: ' + identifier)
        seen.add(identifier)
        seen_modules.add(module)
        selected[modules[module]].append(test)
    if seen_modules != set(modules):
        raise ValueError('Python inventory modules without discovered tests: ' + ','.join(sorted(set(modules) - seen_modules)))
    return selected


class CompleteResult(unittest.TextTestResult):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.successful_ids = []

    def addSuccess(self, test):
        super().addSuccess(test)
        self.successful_ids.append(test.id())


def verify_result(tests, result):
    expected = Counter(test.id() for test in tests)
    if (not expected or any(count != 1 for count in expected.values())
            or not result.wasSuccessful() or result.skipped or result.expectedFailures
            or result.testsRun != len(tests) or Counter(result.successful_ids) != expected):
        raise ValueError('incomplete or unsuccessful Python partition: every discovered test must pass without skips')
    return len(tests)


def run(tests):
    result = unittest.TextTestRunner(verbosity=2, resultclass=CompleteResult).run(unittest.TestSuite(tests))
    return verify_result(tests, result)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--partition', choices=GROUPS)
    parser.add_argument('--run', action='store_true')
    parser.add_argument('--discover', action='store_true', help='verify actual full discovery without running tests')
    args = parser.parse_args()
    try:
        groups = inventory()
        if args.run and (not args.partition or args.discover):
            raise ValueError('execution requires one partition and cannot be combined with discovery only')
        print('Python module inventory: ' + ', '.join(f'{g}={len(groups[g])}' for g in GROUPS), flush=True)
        if args.run or args.discover:
            tests = discover(groups)
            print('Complete Python discovery: ' + ', '.join(f'{g}={len(tests[g])}' for g in GROUPS), flush=True)
            if args.run:
                print(f'{args.partition}: {run(tests[args.partition])} passing Python cases')
    except (ValueError, OSError, ImportError, AttributeError) as error:
        parser.exit(1, f'Python coverage rejected: {error}\n')


if __name__ == '__main__':
    main()
