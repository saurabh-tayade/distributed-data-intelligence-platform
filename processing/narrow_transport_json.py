"""Inactive canonical JSON codec for bounded publication envelopes.

The byte budget is enforced as bytes are emitted, before extending the output.
This module is deliberately independent of the legacy serializers and dispatch.
"""
import json


class TransportJsonError(ValueError):
    code = "CONTENT_TRANSPORT_INVALID"

    def __init__(self):
        super().__init__(self.code)


def _invalid(*_args):
    raise TransportJsonError()


def _budget(limit):
    if type(limit) is not int or not 0 < limit <= 16 * 1024 * 1024:
        _invalid()


def _scalar(value, maximum):
    units = 0
    for char in value:
        point = ord(char)
        if 0xD800 <= point <= 0xDFFF:
            _invalid()
        units += 1 if point <= 0xFFFF else 2
        if units > maximum:
            _invalid()


def canonical_envelope(value, limit, *, large=True):
    """Encode plain JSON values without constructing an oversized output blob."""
    _budget(limit)
    if type(large) is not bool:
        _invalid()
    output = bytearray()
    string_limit = 131072 if large else 65536

    def emit(fragment):
        if len(fragment) > limit - len(output):
            _invalid()
        output.extend(fragment)

    def quote(text, maximum):
        _scalar(text, maximum)
        emit(b'"')
        escapes = {'"': b'\\"', '\\': b'\\\\', '\b': b'\\b', '\f': b'\\f',
                   '\n': b'\\n', '\r': b'\\r', '\t': b'\\t'}
        for char in text:
            if char in escapes:
                emit(escapes[char])
            elif ord(char) < 32:
                emit(('\\u%04x' % ord(char)).encode('ascii'))
            else:
                emit(char.encode('utf-8'))
        emit(b'"')

    def append(node, containers):
        kind = type(node)
        if kind in (dict, list):
            if containers >= 16:
                _invalid()
            if kind is dict:
                # Even the shortest possible member needs a quoted key, colon,
                # value and separator. Reject before allocating a sorted key list.
                if node and 5 * len(node) + 1 > limit - len(output):
                    _invalid()
                for key in node:
                    if type(key) is not str:
                        _invalid()
                    _scalar(key, 50000)
                emit(b'{')
                # Unicode scalar order equals unsigned UTF-8 order. Validating
                # before sorting avoids allocating encoded copies of all keys.
                for index, key in enumerate(sorted(node)):
                    if index:
                        emit(b',')
                    quote(key, 50000)
                    emit(b':')
                    append(node[key], containers + 1)
                emit(b'}')
            else:
                emit(b'[')
                for index, item in enumerate(node):
                    if index:
                        emit(b',')
                    append(item, containers + 1)
                emit(b']')
        elif kind is str:
            quote(node, string_limit)
        elif node is None:
            emit(b'null')
        elif kind is bool:
            emit(b'true' if node else b'false')
        elif kind is int:
            if not -(10 ** 20) < node < 10 ** 20:
                _invalid()
            emit(str(node).encode('ascii'))
        else:
            _invalid()

    append(value, 0)
    return bytes(output)


def parse_envelope(raw, limit, *, large=True):
    """Read strictly canonical UTF-8 bytes, rejecting duplicates and trailing data."""
    _budget(limit)
    if type(large) is not bool:
        _invalid()
    if type(raw) is not bytes or not raw or len(raw) > limit:
        _invalid()
    try:
        text = raw.decode('utf-8', errors='strict')
        if text.startswith('\ufeff'):
            _invalid()
        # Guard the standard decoder before it can recurse into hostile input.
        depth = 0
        quoted = escaped = False
        for char in text:
            if quoted:
                if escaped:
                    escaped = False
                elif char == '\\':
                    escaped = True
                elif char == '"':
                    quoted = False
            elif char == '"':
                quoted = True
            elif char in '[{':
                depth += 1
                if depth > 16:
                    _invalid()
            elif char in ']}':
                depth -= 1

        def pairs(items):
            result = {}
            for key, value in items:
                if key in result:
                    _invalid()
                result[key] = value
            return result

        def integer(token):
            if len(token.lstrip('-')) > 20:
                _invalid()
            return int(token)

        value = json.loads(text, object_pairs_hook=pairs, parse_int=integer,
                           parse_float=_invalid, parse_constant=_invalid)
        if canonical_envelope(value, limit, large=large) != raw:
            _invalid()
        return value
    except (ValueError, UnicodeError, RecursionError) as error:
        raise TransportJsonError() from error
