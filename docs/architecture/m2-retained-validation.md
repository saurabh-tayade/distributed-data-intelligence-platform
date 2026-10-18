# Retained-data M2 integration guide

This opt-in procedure validates the six [documented M1 snapshots](bronze-to-silver.md#actual-local-integration-results) through the existing M2.2–M2.5 interfaces. It does not fetch publishers or run Bronze-to-Silver ingestion. The [M2 closeout](../m2-closeout.md) records the actual run separately from synthetic tests and hosted CI.

## Prerequisites and acceptance

Run from the repository root with Python **3.12.15**, Java **21** (local evidence used 21.0.7), PySpark **4.0.2**, DuckDB **1.4.5**, and sufficient local disk/memory. Use the [isolated Python setup](silver-processing.md#contract-correction-acceptance-and-python-setup). Existing installations can be reused. Dependency bootstrap needs package access; the integration itself needs no network, credentials, SeaweedFS or database service.

```sh
export PYSPARK_PYTHON="$PWD/outputs/silver-python312/.venv/bin/python"
export PYTHONPATH=processing SPARK_LOCAL_IP=127.0.0.1 TZ=UTC
# Select your installed Java 21; on macOS:
export JAVA_HOME=$(/usr/libexec/java_home -v 21)
"$PYSPARK_PYTHON" -m pip install -r processing/requirements-bronze.txt -r processing/requirements-analytics.txt
mkdir -p outputs/m2-closeout
```

The retained Silver root is `outputs/bronze-to-silver`, bucket identity `content-lake`, UTC ingestion date `2026-10-13`. The exact six ingestion UUIDs are embedded below. Source-only discovery is deliberately avoided because later snapshots could change the baseline. If any marker is absent, stop retained-data validation and report missing evidence; do not fetch replacements or substitute synthetic data. Synthetic tests remain runnable separately.

Acceptance requires 118 accepted observations before and after replay collapse, 59 source-scoped article identities, six ingestions, zero rejected observations, zero warnings and zero replay duplicates. Expected source totals are Guardian 90/45, NASA 20/10 and xkcd 8/4 (observations/articles). Gold must have 59 article rows and three source/day rows, with 118 observations in each product. These are dataset-specific assertions, not universal Gold invariants.

## Validate selection, publish and replay

Save this local evidence driver under ignored `outputs/`. It reuses `load_inputs`, `publish`, `run` and `verify_metadata`; it does not implement transformations or publication logic. Run with normal Python, not `python -O`, because assertions are acceptance checks. Keep one writer and immutable inputs throughout.

The preflight validates Silver before calling the publisher. The publisher verifies every physical schema, required values, keys, selected provenance, complete row multisets, distinct counts, timestamps and aggregate totals, and calls `verify_unchanged()` after Spark actions. The driver additionally compares exact Gold schemas and retains file lengths, SHA-256 and modification times for replay and Silver preservation evidence. The filesystem inventories include housekeeping files; the analytical inventory remains the publisher's exact Parquet list.

```sh
cat > outputs/m2-closeout/validate.py <<'PYTHON'
"""Opt-in retained-data evidence; run from repository root, never python -O."""
from hashlib import sha256
import json
from pathlib import Path
from datetime import datetime, timezone
from pyspark.sql import functions as F
from silver import session
from silver_inputs import load_inputs
from gold_publication import publish, run, SCHEMAS
from gold_reader import verify_metadata

ROOT = Path('outputs/bronze-to-silver')
GOLD = Path('outputs/gold')
EVIDENCE = Path('outputs/m2-closeout')
EXPECTED = {'guardian-world': (90, 45), 'nasa-technology': (20, 10), 'xkcd': (8, 4)}
SNAPSHOTS = {
    'guardian-world': ['0a171a1a-6149-4693-b5f2-c15282dd6b21', '9574943a-da7c-441f-8e1b-b94dfd348868'],
    'nasa-technology': ['1251f3be-2d17-4147-b09d-dd3c96c795de', 'f11cfda7-3047-468e-9255-b0d632b5d5ce'],
    'xkcd': ['78e1a5a5-1734-4f8d-b342-37fbc2ae645c', 'bf801dba-84d7-43bf-8314-1d3d969c5288'],
}
# Construct exact documented paths; no discovery of additional snapshots.
import base64
bucket = sha256(b'content-lake').hexdigest()
markers = [f'v1/{bucket}/source_id={base64.urlsafe_b64encode(s.encode()).decode().rstrip("=")}'
           f'/ingestion_date=2026-10-13/ingestion_id={i}/complete.json'
           for s, ids in SNAPSHOTS.items() for i in ids]
assert len(markers) == 6 and all((ROOT / p).is_file() for p in markers)


def inventory(root):
    return {p.relative_to(root).as_posix(): {
        'bytes': p.stat().st_size, 'sha256': sha256(p.read_bytes()).hexdigest(),
        'mtime_ns': p.stat().st_mtime_ns}
        for p in sorted(root.rglob('*')) if p.is_file()}


def save(name, value):
    (EVIDENCE / name).write_text(json.dumps(value, default=str, indent=2, sort_keys=True) + '\n')


silver_before = inventory(ROOT)
save('silver-before.json', silver_before)
save('selection.json', markers)
spark = session()
spark.sparkContext.setLogLevel('ERROR')
try:
    inputs = load_inputs(spark, ROOT, markers=markers)
    assert inputs.counts == {'accepted': 118, 'accepted_before_replay_collapse': 118, 'rejected': 0}
    assert inputs.diagnostics == {'duplicate_selections': 0, 'duplicate_publications': 0,
                                  'duplicate_accepted_rows': 0, 'incomplete': []}
    by_source = [r.asDict() for r in inputs.accepted.groupBy('source_id').agg(
        F.count('*').alias('observations'), F.count_distinct('article_id').alias('articles'),
        F.count_distinct('ingestion_id').alias('ingestions'),
        F.sum(F.size('quality_warnings')).alias('warnings')).orderBy('source_id').collect()]
    assert {r['source_id']: (r['observations'], r['articles']) for r in by_source} == EXPECTED
    assert all(r['ingestions'] == 2 and r['warnings'] == 0 for r in by_source)
    snapshots = [r.asDict() for r in inputs.accepted.groupBy('source_id', 'ingestion_id', 'ingested_at')
                 .count().orderBy('source_id', 'ingestion_id').collect()]
    assert {(r['source_id'], r['ingestion_id']) for r in snapshots} == {
        (s, i) for s, ids in SNAPSHOTS.items() for i in ids}
    assert len(snapshots) == 6
    assert all(r['count'] == EXPECTED[r['source_id']][1] for r in snapshots)
    inputs.verify_unchanged()
    save('silver-metrics.json', dict(counts=inputs.counts, diagnostics=inputs.diagnostics,
                                    sources=by_source, snapshots=snapshots))
    print('Silver preflight passed: 118 observations, 59 articles, 6 ingestions, 0 rejected/warnings', flush=True)
    first = publish(inputs, GOLD)
    save('publication.json', first)
    print(json.dumps(first), flush=True)
    directory = GOLD / first['publication']
    metadata = verify_metadata(GOLD, first['publication'])
    gold_metrics = {}
    for name, schema in SCHEMAS.items():
        frame = spark.read.parquet(*metadata.paths(name))
        assert frame.schema == schema
        metrics = frame.agg(F.count('*').alias('rows'), F.sum('observation_count').alias('observations')).first().asDict()
        assert metrics == {'rows': 59 if name == 'gold_articles' else 3, 'observations': 118}
        gold_metrics[name] = metrics
    inputs.verify_unchanged()
    before = inventory(directory)
    save('gold-before-replay.json', before)
    replay = run(spark, ROOT, GOLD, markers=markers)
    save('replay.json', replay)
    assert replay['status'] == 'SKIPPED'
    assert {k: v for k, v in first.items() if k != 'status'} == {k: v for k, v in replay.items() if k != 'status'}
    after = inventory(directory)
    save('gold-after-replay.json', after)
    assert before == after
    metadata.verify_unchanged()
    assert silver_before == inventory(ROOT)
    save('integration.json', dict(validated_at_utc=datetime.now(timezone.utc).isoformat(),
         fingerprint=inputs.input_set_fingerprint, first_status=first['status'], replay_status=replay['status'],
         gold=gold_metrics, unchanged_gold_files=len(before), unchanged_silver_files=len(silver_before),
         marker_sha256=sha256((directory / 'complete.json').read_bytes()).hexdigest(),
         output_inventory=metadata.marker['files']))
    print('Gold schema/reconciliation, replay file invariance and Silver preservation passed', flush=True)
finally:
    spark.stop()
PYTHON
"$PYSPARK_PYTHON" outputs/m2-closeout/validate.py > outputs/m2-closeout/integration.log 2>&1
cat outputs/m2-closeout/integration.json
```

On a fresh Gold root, expect `first_status: PROCESSED`, `replay_status: SKIPPED`, article rows 59 and daily rows 3. If that exact publication already exists and validates, both calls return `SKIPPED`; this is successful replay, not a new publication claim. Re-running this driver refreshes only the local evidence files. Existing Gold output is never overwritten. Select a fresh ignored evidence directory if preserving previous logs is necessary.

The actual retained physical files produced fingerprint `195999b03c48ca9a5828f5a67c1e00c312efe6f52e0916c180188f8bb9bdf185`. Rewritten Parquet bytes or changed input paths/inventories can produce a different fingerprint even with identical logical rows. Always use the identity returned by your run.

For subsequent CLI-only replay using the validated `selection.json`, this argument-list invocation is equivalent to six explicit `--marker` options:

```sh
"$PYSPARK_PYTHON" - <<'PYTHON'
import json
import subprocess
import sys
from pathlib import Path
markers = json.loads(Path('outputs/m2-closeout/selection.json').read_text())
command = [sys.executable, 'processing/gold_publication.py',
           '--silver-root', 'outputs/bronze-to-silver', '--gold-root', 'outputs/gold']
for marker in markers:
    command.extend(['--marker', marker])
subprocess.run(command, check=True)
PYTHON
```

## Query the pinned publication without Spark

After the publication process exits, run the consumer in a separate Python process. No Spark session is running in this procedure. This verification invokes the existing CLI entry point for all four reports with Java deliberately unavailable. It rejects producer imports, Python opens beneath the retained Silver root, subprocess launches and network connections. This is an execution check, not an OS security sandbox; DuckDB's native Parquet reads use only the reader-verified Gold file inventory. It never renames, deletes or changes the retained Silver directory.

```sh
cat > outputs/m2-closeout/query-check.py <<'PYTHON'
"""Run all four existing CLI reports with producer imports and Silver opens blocked."""
import builtins
from contextlib import redirect_stdout
from hashlib import sha256
import io
import json
import os
from pathlib import Path
import sys

base = Path('outputs/m2-closeout')
publication = json.loads((base / 'publication.json').read_text())['publication']
silver = Path('outputs/bronze-to-silver').resolve()
original_import = builtins.__import__
blocked = {'pyspark', 'py4j', 'silver', 'silver_inputs', 'gold', 'gold_publication', 'boto3', 'botocore'}


def guarded_import(name, *args, **kwargs):
    if name.split('.')[0] in blocked:
        raise AssertionError('Producer dependency imported')
    return original_import(name, *args, **kwargs)


def audit(event, args):
    if event == 'open' and isinstance(args[0], (str, bytes, os.PathLike)):
        path = Path(os.fsdecode(args[0])).resolve()
        if path == silver or silver in path.parents:
            raise AssertionError('Silver access attempted')
    if event in {'subprocess.Popen', 'os.system', 'socket.connect'}:
        raise AssertionError('External process or network attempted')


builtins.__import__ = guarded_import
sys.addaudithook(audit)
from gold_analytics import main
reports = {}
for report in ('publishers', 'daily', 'articles', 'quality'):
    output = io.StringIO()
    with redirect_stdout(output):
        assert main(['--gold-root', 'outputs/gold', '--publication', publication,
                     '--report', report, '--limit', '100']) == 0
    (base / (report + '.json')).write_text(output.getvalue())
    reports[report] = json.loads(output.getvalue())['rows']
assert not blocked.intersection(sys.modules)
expected = [('guardian-world', 45, 90), ('nasa-technology', 10, 20), ('xkcd', 4, 8)]
assert [(r['source_id'], r['article_count'], r['observation_count'])
        for r in reports['publishers']] == expected
assert [(r['source_id'], r['distinct_article_count'], r['observation_count'])
        for r in reports['daily']] == expected
assert all(r['ingestion_date'] == '2026-10-13' and r['distinct_ingestion_count'] == 2
           and r['quality_warning_count'] == 0 for r in reports['daily'])
assert [(r['source_id'], r['observation_count']) for r in reports['quality']] == [(s, o) for s, a, o in expected]
assert all(r['quality_warning_count'] == 0 and r['warning_codes_per_observation'] == 0.0
           for r in reports['quality'])
articles = reports['articles']
assert len(articles) == len({r['article_id'] for r in articles}) == 59
assert sum(r['observation_count'] for r in articles) == 118
assert all(r['observation_count'] == r['distinct_ingestion_count'] == 2 for r in articles)
assert all(not r['quality_warnings'] and r['selected_ingested_at'] == r['last_observed_at'] for r in articles)
# Check the two-key order without relying on retained titles.
assert [r['last_observed_at'] for r in articles] == sorted((r['last_observed_at'] for r in articles), reverse=True)
for instant in {r['last_observed_at'] for r in articles}:
    ids = [r['article_id'] for r in articles if r['last_observed_at'] == instant]
    assert ids == sorted(ids)
summary = dict(status='PASSED', report_rows={k: len(v) for k, v in reports.items()},
               producer_imports_blocked=True, silver_python_opens_blocked=True,
               subprocess_and_network_blocked=True,
               articles_without_published_at=sum(r['published_at'] is None for r in articles))
(base / 'standalone.json').write_text(json.dumps(summary, indent=2) + '\n')
print(json.dumps(summary))
PYTHON
JAVA_HOME=/nonexistent "$PYSPARK_PYTHON" outputs/m2-closeout/query-check.py
```

Expect `status: PASSED` with report row counts `publishers: 3`, `daily: 3`, `articles: 59`, `quality: 3`. All daily rows are dated `2026-10-13` UTC and have two distinct ingestions. Each article has two observations and two distinct ingestions. All warning totals and codes-per-observation rates are zero. Four xkcd articles have null publication timestamps, correctly preserved without warnings. The driver verifies report metrics and deterministic article ordering; full report JSON stays in ignored `outputs/m2-closeout/`, including publisher metadata in the article report.

For normal interactive use, the same reports are available directly through the documented CLI. An [analytics-only environment](duckdb-analytics.md#installation-and-version-choice) needs Python and DuckDB alone:

```sh
PUBLICATION=$("$PYSPARK_PYTHON" -c 'import json; print(json.load(open("outputs/m2-closeout/publication.json"))["input_set_fingerprint"])')
for report in publishers daily articles quality; do
  "$PYSPARK_PYTHON" processing/gold_analytics.py \
    --gold-root outputs/gold --publication "$PUBLICATION" \
    --report "$report" --limit 100
done
```

Both tables are pinned to that exact identity. Limit 100 covers this small baseline; limits are not pagination or a bound on validation work. The publisher report does not expose a global distinct-ingestion metric. Do not sum per-article ingestion counts to infer one. The six-ingestion baseline comes from validated Silver; daily distinct counts apply only to each source/date group. See the [report definitions](duckdb-analytics.md#python-interface-and-metric-definitions).

## Regression and troubleshooting

```sh
"$PYSPARK_PYTHON" -m unittest discover -s processing/tests -v
npm run check
git diff --check
```

Hosted CI runs synthetic fixtures only. Do not add the retained-data driver or local evidence directory as a CI dependency. Keep all publisher datasets and report JSON out of Git.

| Failure | Action |
| --- | --- |
| Missing retained marker or preflight assertion | Check the exact local root, bucket digest, date and six UUIDs; report unavailable or differing evidence. Do not retrieve live replacements. |
| Spark gateway cannot start | Check Python/Java versions, `JAVA_HOME`, `PYSPARK_PYTHON` and local loopback permission. |
| `INPUT_CHANGED`, hash/count/schema/provenance failure | Stop concurrent writers and investigate originals; never edit Silver to force the expected baseline. |
| `INCOMPLETE_OUTPUT` | Inspect the incomplete Gold directory and logs. Use a new ignored Gold root for a fresh attempt; do not overwrite or delete retained data automatically. |
| Reader rejects metadata, inventory, version or schema | Confirm the exact publication and intact files; do not repair completion metadata to bypass checks. |
| Consumer assertion or cross-table mismatch | Preserve logs and inspect the source/report metrics; the milestone remains pending until understood. |

No transactional snapshot, multiwriter isolation, fsync durability, automatic recovery or exactly-once guarantee follows from this procedure. Hash validation establishes consistency with recorded publication metadata, not external authenticity. Full hashing, Spark shuffles/windows and DuckDB scans are exercised only on a small local dataset; no distributed or production scalability is measured. Original Bronze integrity is the earlier M1 evidence, not revalidated by this Silver-to-Gold run.
