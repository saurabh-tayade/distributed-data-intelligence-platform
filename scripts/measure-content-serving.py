"""Bounded local #58 evidence, not a load test or production benchmark.

Use the existing four-article synthetic Content fixture and unchanged native/HTTP
consumers. All generated fixtures, logs and raw samples stay in a new output root.
"""
import argparse
from functools import wraps
from hashlib import sha256
import http.client
import json
import os
from pathlib import Path
import platform
import shutil
import socket
import statistics
import subprocess
import sys
import time
from urllib.parse import urlencode

REPO = Path(__file__).resolve().parents[1]
PAGES = (('first_page', 0), ('later_page', 1))
PHASES = ('verify_dependencies', 'connection', 'register', 'validate_tables')


def elapsed(start):
    return (time.perf_counter_ns() - start) / 1_000_000


def options(offset):
    return dict(limit=1, offset=offset)


def native_sample(root, publication):
    # Start before native imports, after Python and this standard-library harness load.
    start = time.perf_counter_ns()
    sys.path.insert(0, str(REPO / 'processing'))
    import content_analytics as analytics
    result = dict(module_import_ms=elapsed(start), phases_ms={})
    for name in PHASES:
        original = getattr(analytics, name)

        def measured(function, key):
            @wraps(function)
            def call(*args, **kwargs):
                begin = time.perf_counter_ns()
                try:
                    return function(*args, **kwargs)
                finally:
                    result['phases_ms'][key] = result['phases_ms'].get(key, 0) + elapsed(begin)
            return call

        setattr(analytics, name, measured(original, name))
    start = time.perf_counter_ns()
    session = analytics.open_publication(root / 'output', publication,
                                        gold_root=root / 'gold', reference_root=root / 'references')
    result['open_verified_session_ms'] = elapsed(start)
    result['pages'] = {}
    with session:
        for name, offset in PAGES:
            start = time.perf_counter_ns()
            envelope = session.report('catalog', **options(offset))
            report_ms = elapsed(start)
            # Diagnostic query only, on an already verified session; never a serving bypass.
            values, where, limit, position = analytics.filters('catalog', **options(offset))
            table, columns, order, _ = analytics.REPORTS['catalog']
            begin = time.perf_counter_ns()
            session._conn.execute(f'SELECT {",".join(columns)} FROM {table} WHERE {where} '
                f'ORDER BY {order} LIMIT $limit OFFSET $offset', values | dict(limit=limit+1, offset=position)).fetchall()
            query_ms = elapsed(begin)
            result['pages'][name] = dict(report_ms=report_ms, query_only_ms=query_ms, envelope=envelope)
    return result


def rest_envelope(native):
    def camel(name):
        first, *rest = name.split('_')
        return first + ''.join(piece.title() for piece in rest)

    def convert(value):
        return {camel(key): field for key, field in value.items()}

    filters = convert(native['filters'])
    for old, new in (('source', 'sourceId'), ('startDate', 'from'), ('endDate', 'to')):
        filters[new] = filters.pop(old)
    result = dict(productId=native['product_id'], publication=native['product_fingerprint'],
                  goldPublication=native['gold_fingerprint'], reference=convert(native['reference']),
                  filters=filters, data=[convert(row) for row in native['rows']],
                  meta=dict(returned=len(native['rows']), limit=int(native['limit']),
                            offset=int(native['offset']), hasMore=native['has_more']))
    return result


def require_equal(actual, expected):
    # JSON comparison preserves primitive types (unlike Python's True == 1).
    if json.dumps(actual, sort_keys=True) != json.dumps(expected, sort_keys=True):
        raise RuntimeError('Consumer output or primitive types differ from the native reference')


def check_workload(pages):
    for name, offset in PAGES:
        page = pages[name]
        require_equal([page['limit'], page['offset'], page['has_more'], len(page['rows'])],
                      ['1', str(offset), True, 1])
    require_equal([pages[name]['rows'][0]['source_id'] for name, _ in PAGES], ['gamma', 'alpha'])


def inventory(root):
    return {str(path.relative_to(root)): dict(bytes=path.stat().st_size,
            sha256=sha256(path.read_bytes()).hexdigest(), mtime_ns=path.stat().st_mtime_ns)
            for path in sorted(root.rglob('*')) if path.is_file()}


def fixture_description(root, publication):
    import content_metadata as metadata
    directory = metadata.publication_directory(root / 'output', publication)
    completion = json.loads((directory / 'complete.json').read_text())
    sizes = {}
    for name in ('output', 'gold', 'references'):
        files = inventory(root / name)
        sizes[name] = dict(files=len(files), bytes=sum(value['bytes'] for value in files.values()))
    return dict(publication=publication, output_counts=completion['output_counts'], retained_sizes=sizes)


def child_env():
    # Do not accidentally use an operator's real deployment or JVM tuning.
    excluded = ('DDIP_', 'SPRING_', 'SERVER_', 'ANALYTICS_', 'RELIABILITY_ANALYTICS_', 'CONTENT_ANALYTICS_')
    env = {key: value for key, value in os.environ.items()
           if not key.startswith(excluded) and key not in (
               'JAVA_TOOL_OPTIONS', '_JAVA_OPTIONS', 'JDK_JAVA_OPTIONS', 'CLASSPATH', 'PYTHONPATH', 'PYTHONHOME')}
    env.update(PYTHONDONTWRITEBYTECODE='1', PYTHONHASHSEED='0')
    return env


def cli_command(root, publication, offset):
    command = [sys.executable, str(REPO / 'processing/content_analytics.py'),
               '--root', str(root / 'output'), '--publication', publication,
               '--gold-root', str(root / 'gold'), '--reference-root', str(root / 'references'),
               '--report', 'catalog']
    for key, value in options(offset).items():
        command.extend(['--' + key, str(value)])
    return command


def http_get(connection, path):
    start = time.perf_counter_ns()
    connection.request('GET', path)
    response = connection.getresponse()
    body = response.read()
    milliseconds = elapsed(start)
    if response.status != 200:
        raise RuntimeError(f'Expected HTTP 200, received {response.status}')
    return dict(elapsed_ms=milliseconds, response_bytes=len(body)), json.loads(body)


def http_sample(java, jar, root, publication, expected, log, env):
    with socket.socket() as reserved:
        reserved.bind(('127.0.0.1', 0))
        port = reserved.getsockname()[1]
    command = [str(java), '-jar', str(jar), '--server.address=127.0.0.1', f'--server.port={port}',
               '--spring.main.banner-mode=off', '--logging.level.root=WARN', '--ddip.role=analytics',
               '--analytics.publication=', '--content.analytics.python-executable=' + sys.executable,
               '--content.analytics.processing-root=' + str(REPO / 'processing'),
               '--content.analytics.root=' + str(root / 'output'),
               '--content.analytics.gold-root=' + str(root / 'gold'),
               '--content.analytics.reference-root=' + str(root / 'references'),
               '--content.analytics.publication=' + publication]
    start = time.perf_counter_ns()
    with log.open('wb') as stream:
        process = subprocess.Popen(command, cwd=REPO, env=env, stdout=stream, stderr=subprocess.STDOUT)
        try:
            deadline = time.monotonic() + 60
            while True:
                if process.poll() is not None:
                    raise RuntimeError('Analytics JVM exited before health readiness; inspect local log')
                probe = http.client.HTTPConnection('127.0.0.1', port, timeout=1)
                try:
                    probe.request('GET', '/actuator/health')
                    response = probe.getresponse()
                    body = response.read()
                    if response.status == 200 and json.loads(body).get('status') == 'UP':
                        break
                except (OSError, http.client.HTTPException):
                    pass
                finally:
                    probe.close()
                if time.monotonic() >= deadline:
                    raise RuntimeError('Analytics JVM did not become healthy within 60 seconds')
                time.sleep(0.05)
            result = dict(jvm_health_ready_ms=elapsed(start), pages={})
            # One fresh HTTP/1.1 connection per JVM; both pages use it. Health uses another connection.
            connection = http.client.HTTPConnection('127.0.0.1', port, timeout=40)
            try:
                for name, offset in PAGES:
                    path = '/api/v1/content/catalog?' + urlencode(options(offset))
                    timing, actual = http_get(connection, path)
                    require_equal(actual, rest_envelope(expected[name]))
                    result['pages'][name] = timing
            finally:
                connection.close()
            return result
        finally:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=10)


def summary(samples):
    return dict(n=len(samples), minimum_ms=min(samples), median_ms=statistics.median(samples),
                maximum_ms=max(samples))


def read_optional(path):
    try:
        return Path(path).read_text().strip()
    except OSError:
        return None


def environment(java, jar, env):
    import duckdb
    cpu = read_optional('/proc/cpuinfo') or ''
    memory = read_optional('/proc/meminfo') or ''
    return dict(python=sys.version, duckdb=duckdb.__version__, platform=platform.platform(),
                machine=platform.machine(), cpu_model=next((line.split(':', 1)[1].strip()
                for line in cpu.splitlines() if line.startswith('model name')), None),
                logical_cpus=os.cpu_count(), affinity_cpus=len(os.sched_getaffinity(0))
                if hasattr(os, 'sched_getaffinity') else None,
                mem_total=next((line for line in memory.splitlines() if line.startswith('MemTotal:')), None),
                cgroup_cpu_max=read_optional('/sys/fs/cgroup/cpu.max'),
                cgroup_memory_max=read_optional('/sys/fs/cgroup/memory.max'),
                java=subprocess.run([str(java), '-version'], capture_output=True, text=True,
                                    check=True, timeout=10, env=env).stderr.strip(),
                source_commit=subprocess.run(['git', 'rev-parse', 'HEAD'], cwd=REPO,
                                             capture_output=True, text=True, check=True, timeout=10).stdout.strip(),
                harness_sha256=sha256(Path(__file__).read_bytes()).hexdigest(),
                jar_sha256=sha256(jar.read_bytes()).hexdigest())


def measure(args):
    if not 1 <= args.samples <= 20:
        raise ValueError('Choose 1..20 samples; this is a bounded local experiment')
    jar, java = args.jar.resolve(), Path(args.java).resolve()
    if not jar.is_file() or not java.is_file():
        raise ValueError('Build the content-api JAR and select an existing Java 21 executable first')
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    env = child_env()
    sys.path[:0] = [str(REPO / 'processing'), str(REPO / 'processing/tests')]
    reference = json.loads((args.fixture / 'reference.json').read_text())['publications']['standard']
    result = dict(schema_version=1, measured_at_utc=time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
                  environment=environment(java, jar, env), samples=args.samples, formats={})
    for fmt in ('CONTENT',):
        root = (args.fixture / reference['root']).resolve()
        publication = reference['publication']
        before = inventory(root)
        data = dict(fixture=fixture_description(root, publication), native_samples=[], cli_samples=[], http_samples=[])
        expected = None
        for _ in range(args.samples):
            command = [sys.executable, str(Path(__file__).resolve()), '--native-root', str(root),
                       '--publication', publication]
            child = subprocess.run(command, capture_output=True, text=True, check=True, timeout=60, env=env)
            sample = json.loads(child.stdout)
            envelopes = {name: sample['pages'][name].pop('envelope') for name, _ in PAGES}
            if expected is None:
                expected = envelopes
                check_workload(expected)
            require_equal(envelopes, expected)
            data['native_samples'].append(sample)
        for _ in range(args.samples):
            sample = {}
            for name, offset in PAGES:
                start = time.perf_counter_ns()
                child = subprocess.run(cli_command(root, publication, offset), capture_output=True,
                                       check=True, timeout=60, env=env)
                sample[name] = dict(elapsed_ms=elapsed(start), response_bytes=len(child.stdout))
                require_equal(json.loads(child.stdout), expected[name])
            data['cli_samples'].append(sample)
        for index in range(args.samples):
            data['http_samples'].append(http_sample(java, jar, root, publication, expected,
                output / f'{fmt.lower()}-jvm-{index}.log', env))
        if before != inventory(root):
            raise RuntimeError('Measurement changed a fixture file or timestamp')
        data['retained_inventory'] = before
        data['expected_native_pages'] = expected
        data['summary'] = {
            'native_import': summary([s['module_import_ms'] for s in data['native_samples']]),
            'native_open_verified_session': summary([s['open_verified_session_ms'] for s in data['native_samples']]),
            'native_open_phases': {phase: summary([s['phases_ms'][phase] for s in data['native_samples']]) for phase in PHASES},
            'native_report': {name: summary([s['pages'][name]['report_ms'] for s in data['native_samples']]) for name, _ in PAGES},
            'query_only': {name: summary([s['pages'][name]['query_only_ms'] for s in data['native_samples']]) for name, _ in PAGES},
            'fresh_cli': {name: summary([s[name]['elapsed_ms'] for s in data['cli_samples']]) for name, _ in PAGES},
            'http': {name: summary([s['pages'][name]['elapsed_ms'] for s in data['http_samples']]) for name, _ in PAGES},
            'jvm_health_ready': summary([s['jvm_health_ready_ms'] for s in data['http_samples']])}
        result['formats'][fmt] = data
    (output / 'results.json').write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(dict(output=str(output / 'results.json'),
                          summary={fmt: value['summary'] for fmt, value in result['formats'].items()}), indent=2))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path)
    parser.add_argument('--fixture', type=Path, help='Generated ContentApiIT fixture root')
    parser.add_argument('--samples', type=int, default=5)
    parser.add_argument('--jar', type=Path, default=REPO / 'services/content-api/target/content-api-0.1.0-SNAPSHOT.jar')
    parser.add_argument('--java', default=shutil.which('java'))
    parser.add_argument('--native-root', type=Path, help=argparse.SUPPRESS)
    parser.add_argument('--publication', help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.native_root is not None:
        print(json.dumps(native_sample(args.native_root, args.publication)))
    elif args.output is None or args.java is None or args.fixture is None:
        parser.error('--output, --fixture and an available Java 21 executable are required')
    else:
        measure(args)


if __name__ == '__main__':
    main()
