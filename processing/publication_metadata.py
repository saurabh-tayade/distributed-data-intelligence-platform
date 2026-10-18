"""Canonical manifest serialization shared by Spark producers and local readers."""
from hashlib import sha256
import json


def canonical_bytes(manifest):
    return json.dumps(manifest, sort_keys=True, separators=(',', ':'),
                      ensure_ascii=False, allow_nan=False).encode('utf-8')


def fingerprint(manifest):
    return sha256(canonical_bytes(manifest)).hexdigest()
