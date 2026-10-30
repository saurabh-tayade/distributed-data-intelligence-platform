"""Fail-closed PostgreSQL inventory, fixed Maven selection and actual report coverage.

Default -Ppostgres still discovers every *IT except the separate analytics HTTP profile.
CI may narrow that selection only after proving the complete, disjoint partition inventory.
"""
import argparse
import fnmatch
import json
from pathlib import Path
import re
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[2]
GROUPS = ('core', 'reliability', 'ingestion', 'gold')
INCLUDES = ['**/IT*.java', '**/*IT.java', '**/*ITCase.java']
UNIT_NAMES = ('Test*.java', '*Test.java', '*Tests.java', '*TestCase.java')
NS = {'m': 'http://maven.apache.org/POM/4.0.0'}


def inventory(root=ROOT):
    def unique(items):
        result = {}
        for k, v in items:
            if k in result:
                raise ValueError('duplicate partition key')
            result[k] = v
        return result
    groups = json.loads((root / '.github/ci/postgres-partitions.json').read_text(), object_pairs_hook=unique)
    if set(groups) != set(GROUPS):
        raise ValueError('unexpected partition inventory')
    selected = []
    for group in GROUPS:
        values = groups[group]
        if not isinstance(values, list) or not values or any(not isinstance(v, str) or not re.fullmatch(r'platform\.[A-Za-z0-9_.]+', v) for v in values):
            raise ValueError('invalid partition classes')
        selected.extend(values)
    if len(selected) != len(set(selected)):
        raise ValueError('integration class assigned more than once')
    pom = ET.parse(root / 'services/content-api/pom.xml')
    profiles = {p.findtext('m:id', namespaces=NS): p for p in pom.findall('m:profiles/m:profile', NS)}
    config = profiles['postgres'].find('.//m:configuration', NS)
    includes = [n.text for n in config.findall('m:includes/m:include', NS)]
    excludes = [n.text for n in config.findall('m:excludes/m:exclude', NS)]
    if includes != INCLUDES or excludes != ['**/GoldApiIT.java']:
        raise ValueError('PostgreSQL default discovery must remain comprehensive')
    other = [n.text for n in profiles['integration'].findall('.//m:includes/m:include', NS)]
    if other != excludes:
        raise ValueError('excluded integration classes must execute in the HTTP profile')
    found = set()
    http = set()
    for path in (root / 'services/content-api/src/test/java').rglob('*.java'):
        source = path.read_text()
        integration = any(fnmatch.fnmatch(path.as_posix(), pattern) for pattern in includes)
        if not integration:
            if re.search(r'^\s*@(?:(?:org\.junit\.jupiter\.(?:api|params)\.)?)(?:Test|ParameterizedTest|RepeatedTest|TestFactory|TestTemplate)\b', source, re.MULTILINE) and not any(fnmatch.fnmatch(path.name, p) for p in UNIT_NAMES):
                raise ValueError('test-bearing source outside Maven discovery: ' + path.name)
            continue
        match = re.search(r'^package\s+([\w.]+);', source, re.MULTILINE)
        if not match:
            raise ValueError('integration class has no package')
        name = match[1] + '.' + path.stem
        if fnmatch.fnmatch(path.as_posix(), excludes[0]):
            http.add(name)
        else:
            if name in found:
                raise ValueError('duplicate integration source class')
            found.add(name)
    if http != {'platform.contentapi.GoldApiIT'} or found != set(selected):
        raise ValueError('unassigned, removed or unexpected integration class: ' + ','.join(sorted(found ^ set(selected))))
    return groups


def verify_reports(groups, partition, directory):
    expected = set(groups[partition])
    seen = set()
    suites = set()
    cases = 0
    for path in directory.glob('TEST-*.xml'):
        suite = ET.parse(path).getroot()
        full_name = suite.attrib['name']
        name = full_name.split('$', 1)[0]
        if name not in expected:
            raise ValueError('unexpected integration report: ' + name)
        if full_name in suites:
            raise ValueError('duplicate integration report: ' + full_name)
        suites.add(full_name)
        tests = int(suite.attrib['tests'])
        if tests <= 0 or len(suite.findall('testcase')) != tests or any(int(suite.attrib.get(k, '0')) for k in ('failures', 'errors', 'skipped')):
            raise ValueError('incomplete or unsuccessful integration report: ' + name)
        seen.add(name)
        cases += tests
    if seen != expected:
        raise ValueError('missing integration report: ' + ','.join(sorted(expected - seen)))
    return cases


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--partition', choices=GROUPS)
    parser.add_argument('--reports', type=Path)
    args = parser.parse_args()
    try:
        groups = inventory()
        if args.reports:
            if not args.partition:
                raise ValueError('report verification requires a partition')
            print(f'{args.partition}: {verify_reports(groups, args.partition, args.reports)} passing integration cases')
        elif args.partition:
            print(','.join(groups[args.partition]))
        else:
            print('PostgreSQL inventory: ' + ', '.join(f'{k}={len(groups[k])}' for k in GROUPS))
    except (ValueError, OSError, KeyError, ET.ParseError) as error:
        parser.exit(1, f'PostgreSQL coverage rejected: {error}\n')


if __name__ == '__main__':
    main()
