"""Mandatory Java parity exporter using real native producer verification bodies."""
import base64
import os
from pathlib import Path
import sys
import tempfile
from unittest.mock import patch

sys.path[:0] = [str(Path(__file__).resolve().parent), str(Path(__file__).resolve().parents[1])]


def main():
    from silver import session
    from silver_import_qualification import quiet_stdout
    from pyspark import SparkContext
    import narrow_gold_publication_v2 as gold
    import narrow_content_publication_v2 as content
    import v2_publisher_native_fixtures as fixtures
    os.environ['PYSPARK_PYTHON'] = sys.executable
    import narrow_publication_lifecycle_v2 as lifecycle
    original_qualify = lifecycle.qualify
    rows = []
    with quiet_stdout(), tempfile.TemporaryDirectory() as directory:
        spark = session()
        spark.sparkContext.setLogLevel('ERROR')
        try:
            for label, empty in [('nonempty', False), ('reference_only', True)]:
                with fixtures.source(spark, Path(directory) / label, empty=empty) as case:
                    captured = {'GOLD': [], 'CONTENT': []}
                    def collect(target):
                        def observe(bound, verification):
                            result = original_qualify(bound, verification)
                            if bound.kind == target:
                                marker = (Path(bound.value()[target.lower()]['root']) / 'v1' /
                                    'processor_revision=1' / (('input_set=' + verification['prepared']['inputSetFingerprint'])
                                    if target == 'GOLD' else ('publication=' + verification['prepared']['fingerprint'])) / 'complete.json')
                                captured[target].append((marker.exists(), result))
                            return result
                        return observe
                    with patch.object(lifecycle, 'qualify', side_effect=collect('GOLD')):
                        gv = gold.publish_v2(spark, case['gold'], expectations=case['expected'])
                    cc, _ = fixtures.content_command(case, gv)
                    with patch.object(lifecycle, 'qualify', side_effect=collect('CONTENT')):
                        cv = content.publish_v2(spark, cc, expectations=case['expected'])
                    descriptors = fixtures.envelopes(case['gold'], gv, cc, cv)
                    for kind in ('GOLD', 'CONTENT'):
                        assert [visible for visible, _ in captured[kind]] == [False, True]
                        metadata = descriptors[:12] if kind == 'GOLD' else descriptors[12:]
                        for phase, (_, raw_bodies) in zip(('projected', 'actual'), captured[kind], strict=True):
                            rows.extend((label + '_' + phase + '_' + name, destination, raw)
                                for (name, destination, _), raw in zip(metadata, raw_bodies, strict=True))
                    assert case['store'].writes == []
        finally:
            spark.stop()
            gateway = SparkContext._gateway
            gateway.shutdown()
            gateway.proc.stdin.close()
            gateway.proc.wait(timeout=10)
    for name, destination, raw in rows:
        print(name + '\t' + destination + '\t' + base64.b64encode(raw).decode('ascii'))


if __name__ == '__main__':
    main()
