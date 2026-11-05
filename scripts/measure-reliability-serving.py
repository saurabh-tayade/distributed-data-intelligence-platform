"""Bounded local #58 evidence, not a load test or production benchmark.

Use the independent ten-occurrence G1 request-window fixture or its fourteen-occurrence
coverage extension with unchanged native reports and fixed HTTP consumers. All generated fixtures, logs and raw samples stay in a new output root.
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
START = '2026-10-01T00:00:00.000000Z'
END = '2026-10-01T00:10:00.000000Z'
PAGES = (('first_page', 0), ('later_page', 1))
PHASES = ('verify_dependencies', 'connection', 'register_table', 'validate_tables',
          'verify_raw', 'prepare_reports')


def elapsed(start):
    return (time.perf_counter_ns() - start) / 1_000_000


def options(offset, report='requests_5m'):
    return (dict(start=START, end=END) if report == 'requests_5m' else {}) | dict(limit=1, offset=offset)


def native_sample(root, publication, report='requests_5m'):
    # Start before native imports, after Python and this standard-library harness load.
    start = time.perf_counter_ns()
    sys.path.insert(0, str(REPO / 'processing'))
    import reliability_format_analytics as analytics
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
                                        batch_root=root / 'batches', reference_root=root / 'references')
    result['open_verified_session_ms'] = elapsed(start)
    result['pages'] = {}
    with session:
        for name, offset in PAGES:
            start = time.perf_counter_ns()
            envelope = session.report(report, **options(offset, report))
            result['pages'][name] = dict(report_ms=elapsed(start), envelope=envelope)
    return result


def rest_envelope(native):
    def camel(name):
        first, *rest = name.split('_')
        return first + ''.join(piece.title() for piece in rest)

    def convert(value):
        return {camel(key): field for key, field in value.items()}

    result = convert({key: native[key] for key in (
        'product_id', 'product_contract_version', 'processor_revision', 'input_format', 'origin', 'label')})
    result.update(publication=native['product_fingerprint'], reference=convert(native['reference']),
                  filters=convert(native['filters']), data=[convert(row) for row in native['rows']],
                  meta=dict(returned=len(native['rows']), limit=int(native['limit']),
                            offset=int(native['offset']), hasMore=native['has_more']))
    if 'totals' in native:
        result['totals'] = convert(native['totals'])
    return result


def require_equal(actual, expected):
    # JSON comparison preserves primitive types (unlike Python's True == 1).
    if json.dumps(actual, sort_keys=True) != json.dumps(expected, sort_keys=True):
        raise RuntimeError('Consumer output or primitive types differ from the native reference')


def check_workload(pages, report='requests_5m'):
    if report != 'requests_5m':
        for name, offset in PAGES:
            page = pages[name]
            require_equal([page['limit'], page['offset'], page['has_more'], len(page['rows'])], ['1', str(offset), True, 1])
            row = page['rows'][0]
            if report == 'input_coverage':
                require_equal([row['batch_id'], row['physical_occurrence_count']], ['b00', '10'] if offset == 0 else ['b01', '2'])
                require_equal(page['totals']['physical_occurrence_count'], '14')
            else:
                require_equal([row['service_id'], row['reference_usage']], ['svc-unused', 'UNUSED'] if offset == 0 else ['svc00', 'USED'])
                require_equal(page['totals']['event_count'], '4')
        require_equal(pages['first_page']['totals'], pages['later_page']['totals'])
        return
    # Fail if the shared fixture no longer represents this documented two-page workload.
    for name, offset in PAGES:
        page = pages[name]
        require_equal([page['limit'], page['offset'], page['has_more'], len(page['rows'])],
                      ['1', str(offset), offset == 0, 1])
        row = page['rows'][0]
        require_equal([row['service_id'], row['request_count'], row['duration_sum_ms']],
                      ['svc00', '2', '30'] if offset == 0 else ['svc01', '1', '7'])


def inventory(root):
    return {str(path.relative_to(root)): dict(bytes=path.stat().st_size,
            sha256=sha256(path.read_bytes()).hexdigest(), mtime_ns=path.stat().st_mtime_ns)
            for path in sorted(root.rglob('*')) if path.is_file()}


def fixture_description(root, publication):
    import reliability_format_metadata as metadata
    directory = metadata.publication_directory(root / 'output', publication)
    completion = json.loads((directory / 'complete.json').read_text())
    sizes = {}
    for name in ('output', 'batches', 'references'):
        files = inventory(root / name)
        sizes[name] = dict(files=len(files), bytes=sum(value['bytes'] for value in files.values()))
    return dict(publication=publication, occurrence_counts=completion['occurrence_counts'],
                output_counts=completion['output_counts'], retained_sizes=sizes)


def child_env():
    # Do not accidentally use an operator's real deployment or JVM tuning.
    excluded = ('DDIP_', 'SPRING_', 'SERVER_', 'ANALYTICS_', 'RELIABILITY_ANALYTICS_')
    env = {key: value for key, value in os.environ.items()
           if not key.startswith(excluded) and key not in (
               'JAVA_TOOL_OPTIONS', '_JAVA_OPTIONS', 'JDK_JAVA_OPTIONS', 'CLASSPATH', 'PYTHONPATH', 'PYTHONHOME')}
    env.update(PYTHONDONTWRITEBYTECODE='1', PYTHONHASHSEED='0')
    return env


def cli_command(root, publication, offset, report='requests_5m'):
    command = [sys.executable, str(REPO / 'processing/reliability_format_analytics.py'),
               '--root', str(root / 'output'), '--publication', publication,
               '--batch-root', str(root / 'batches'), '--reference-root', str(root / 'references'),
               '--report', report]
    for key, value in options(offset, report).items():
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


def http_sample(java, jar, root, publication, expected, log, env, report='requests_5m'):
    with socket.socket() as reserved:
        reserved.bind(('127.0.0.1', 0))
        port = reserved.getsockname()[1]
    command = [str(java), '-jar', str(jar), '--server.address=127.0.0.1', f'--server.port={port}',
               '--spring.main.banner-mode=off', '--logging.level.root=WARN', '--ddip.role=analytics',
               '--analytics.publication=', '--reliability.analytics.python-executable=' + sys.executable,
               '--reliability.analytics.processing-root=' + str(REPO / 'processing'),
               '--reliability.analytics.root=' + str(root / 'output'),
               '--reliability.analytics.batch-root=' + str(root / 'batches'),
               '--reliability.analytics.reference-root=' + str(root / 'references'),
               '--reliability.analytics.publication=' + publication]
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
                    endpoint = {'requests_5m': 'request-windows', 'input_coverage': 'input-coverage', 'reference_coverage': 'reference-coverage'}[report]
                    path = '/api/v1/reliability/' + endpoint + '?' + urlencode(options(offset, report))
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
    sys.path[:0] = [str(REPO / 'processing'), str(REPO / 'processing/tests'), str(REPO / 'services/content-api/tests')]
    from reliability_format_fixtures import completed
    from reliability_coverage_fixtures import completed_coverage
    report = getattr(args, 'report', 'requests_5m')
    result = dict(schema_version=1, measured_at_utc=time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
                  environment=environment(java, jar, env), samples_per_format=args.samples, report=report, formats={})
    for fmt in ('JSONL', 'PARQUET'):
        root = output / fmt.lower()
        root.mkdir()
        _, publication, _, _ = (completed if report == 'requests_5m' else completed_coverage)(root, fmt)
        before = inventory(root)
        data = dict(fixture=fixture_description(root, publication), native_samples=[], cli_samples=[], http_samples=[])
        expected = None
        for _ in range(args.samples):
            command = [sys.executable, str(Path(__file__).resolve()), '--native-root', str(root),
                       '--publication', publication, '--report', report]
            child = subprocess.run(command, capture_output=True, text=True, check=True, timeout=60, env=env)
            sample = json.loads(child.stdout)
            envelopes = {name: sample['pages'][name].pop('envelope') for name, _ in PAGES}
            if expected is None:
                expected = envelopes
                check_workload(expected, report)
            require_equal(envelopes, expected)
            data['native_samples'].append(sample)
        for _ in range(args.samples):
            sample = {}
            for name, offset in PAGES:
                start = time.perf_counter_ns()
                child = subprocess.run(cli_command(root, publication, offset, report), capture_output=True,
                                       check=True, timeout=60, env=env)
                sample[name] = dict(elapsed_ms=elapsed(start), response_bytes=len(child.stdout))
                require_equal(json.loads(child.stdout), expected[name])
            data['cli_samples'].append(sample)
        for index in range(args.samples):
            data['http_samples'].append(http_sample(java, jar, root, publication, expected,
                output / f'{fmt.lower()}-jvm-{index}.log', env, report))
        if before != inventory(root):
            raise RuntimeError('Measurement changed a fixture file or timestamp')
        data['retained_inventory'] = before
        data['expected_native_pages'] = expected
        data['summary'] = {
            'native_import': summary([s['module_import_ms'] for s in data['native_samples']]),
            'native_open_verified_session': summary([s['open_verified_session_ms'] for s in data['native_samples']]),
            'native_open_phases': {phase: summary([s['phases_ms'][phase] for s in data['native_samples']]) for phase in PHASES},
            'native_report': {name: summary([s['pages'][name]['report_ms'] for s in data['native_samples']]) for name, _ in PAGES},
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
    parser.add_argument('--samples', type=int, default=5)
    parser.add_argument('--report', choices=('requests_5m', 'input_coverage', 'reference_coverage'), default='requests_5m')
    parser.add_argument('--jar', type=Path, default=REPO / 'services/content-api/target/content-api-0.1.0-SNAPSHOT.jar')
    parser.add_argument('--java', default=shutil.which('java'))
    parser.add_argument('--native-root', type=Path, help=argparse.SUPPRESS)
    parser.add_argument('--publication', help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.native_root is not None:
        print(json.dumps(native_sample(args.native_root, args.publication, args.report)))
    elif args.output is None or args.java is None:
        parser.error('--output and an available Java 21 executable are required')
    else:
        measure(args)


if __name__ == '__main__':
    main()
