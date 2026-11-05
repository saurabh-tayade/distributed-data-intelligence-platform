"""Batch subprocess bridge used unconditionally by NarrowTransportJsonTest."""
import base64
import sys
from processing.narrow_transport_json import TransportJsonError, canonical_envelope, parse_envelope

for line in sys.stdin:
    maximum, encoded = line.rstrip('\n').split('\t')
    maximum = int(maximum)
    try:
        raw = base64.b64decode(encoded)
        value = parse_envelope(raw, maximum, large=maximum > 65536)
        print(base64.b64encode(canonical_envelope(value, maximum, large=maximum > 65536)).decode())
    except TransportJsonError:
        print('INVALID')
