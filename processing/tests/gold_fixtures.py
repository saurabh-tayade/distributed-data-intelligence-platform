"""Shared synthetic M2.4 publications for Python and JDBC compatibility tests."""
from datetime import datetime, timezone
from hashlib import sha256
import json
from pathlib import Path
import shutil


def create_analytics_publications(root):
    """Build full/first/empty snapshots; stop local Spark and remove synthetic Silver."""
    from bronze_to_silver import encode, process_one
    from gold_publication import run
    from silver import session
    from test_bronze import Store, snapshot
    root = Path(root)
    silver = root / 'silver'
    gold_base = root / 'gold'
    spark = session()
    spark.sparkContext.setLogLevel('ERROR')
    store = Store()
    markers = []
    try:
        for number, source, instant, entries in [
            (1, 'alpha', '2026-10-12T23:59:59.123456+00:00',
             '<entry><id>a</id><title>Older A</title><published>2026-10-10T00:00:00Z</published></entry>'
             '<entry><id>b</id><title>B</title></entry>'),
            (2, 'alpha', '2026-10-12T20:00:00.000001-04:00',
             '<entry><id>a</id><title>New A</title><published>bad</published><updated>bad</updated></entry>'
             '<entry><id>a</id><title>Selected A</title><published>bad</published><updated>bad</updated></entry>'),
            (3, 'beta', '2026-10-13T01:00:00+00:00', '<entry><id>a</id><title>Beta A</title></entry>'),
            (4, 'empty', '2026-10-13T02:00:00+00:00', '<entry><id>invalid</id></entry>'),
        ]:
            _, metadata = snapshot(store, number=number)
            instant = datetime.fromisoformat(instant).astimezone(timezone.utc)
            prefix = (f'bronze/rss/source_id={encode(source)}/ingestion_date={instant.date()}/'
                      f'ingestion_id={metadata["ingestionId"]}/')
            payload = ('<feed xmlns="http://www.w3.org/2005/Atom">' + entries + '</feed>').encode()
            metadata.update(sourceId=source, ingestedAt=instant.isoformat().replace('+00:00', 'Z'),
                            payloadKey=prefix + 'payload.xml', payloadByteLength=len(payload),
                            payloadSha256=sha256(payload).hexdigest())
            key = prefix + 'metadata.json'
            store.objects[key] = json.dumps(metadata).encode()
            store.objects[metadata['payloadKey']] = payload
            process_one(spark, store, 'local', key, silver)
            markers.append(next(p for p in silver.rglob('complete.json') if metadata['ingestionId'] in str(p)))
        full = run(spark, silver, gold_base, markers=markers[:3])['publication']
        first = run(spark, silver, gold_base, markers=markers[:1])['publication']
        empty = run(spark, silver, gold_base, markers=markers[3:])['publication']
    finally:
        spark.stop()
    shutil.rmtree(silver)
    return gold_base, dict(full=full, first=first, empty=empty)
