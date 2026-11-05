"""Inactive pipe-gated read-only strict-sink probe; never dispatches native work.

Bootstrap facts are parent-bound correspondence, not DB authority or child
self-authentication. The parent must fence and record the actual child PID plus
OS process-start identity before delivering bytes. This module only checks PID
correspondence. Current is reconstructed separately from those same parent facts,
not discovered from the command or ledger. Nothing here grants launch authority.
"""
import os
from pathlib import Path
import re
import select
import stat
import sys
import time
from uuid import UUID

from narrow_evidence_sink import (Current, Expected, Failure, Frozen, Identity,
                                  StrictSink, Writer, validate_expectations)
from narrow_transport_json import parse_envelope

BOOTSTRAP_LIMIT = 64 * 1024
BOOTSTRAP_TIMEOUT_SECONDS = 5.0
ERROR = 'CONTENT_STARTUP_REJECTED'
_FIELDS = frozenset('''bootstrapVersion mode operationId installationId policy planKind
runId attemptId sourceId controlRoot controlDevice controlInode rootDevice rootInode
lockDevice lockInode ledgerSha256 selectionSha256 profileSha256 runtimeSha256
workerInstanceId ownerEpoch stage ordinal checkpointId commandSha256 childPid'''.split())


class StartupError(Exception):
    """A fixed diagnostic that cannot reveal bootstrap values."""
    def __init__(self):
        super().__init__(ERROR)


def _need(condition):
    if not condition:
        raise StartupError()


def receive_bootstrap(fd):
    """Own an input pipe and wait for EOF, with a fixed total byte/time budget.

    Accepted frames are strictly smaller than 64 KiB. Reaching the cap rejects
    immediately, without consuming an extra byte to distinguish oversized input.
    No caller-selected paths are inspected and no sink is constructed here.
    """
    deadline = time.monotonic() + BOOTSTRAP_TIMEOUT_SECONDS
    raw = bytearray()
    try:
        _need(stat.S_ISFIFO(os.fstat(fd).st_mode))
        os.set_blocking(fd, False)
        while True:
            remaining = deadline - time.monotonic()
            _need(remaining > 0)
            ready, _, _ = select.select([fd], [], [], remaining)
            _need(ready and time.monotonic() < deadline)
            try:
                chunk = os.read(fd, min(4096, BOOTSTRAP_LIMIT - len(raw)))
            except (BlockingIOError, InterruptedError):
                continue
            if not chunk:
                _need(raw)
                return bytes(raw)
            raw.extend(chunk)
            _need(len(raw) < BOOTSTRAP_LIMIT)
    except (OSError, ValueError) as error:
        raise StartupError() from error


def _decimal(value, *, positive=False, maximum=10**20):
    _need(type(value) is str and len(value) <= 20
          and re.fullmatch('0|[1-9][0-9]*', value) is not None)
    number = int(value)
    _need((number > 0 if positive else number >= 0) and number < maximum)
    return number


def parse_bootstrap(raw):
    """Validate pure data only; paths are lexical until the sink is constructed."""
    try:
        value = parse_envelope(raw, BOOTSTRAP_LIMIT - 1, large=False)
        _need(type(value) is dict and set(value) == _FIELDS)
        _need(type(value['bootstrapVersion']) is int and value['bootstrapVersion'] == 1)
        _need(value['mode'] == 'READ_ONLY_PROBE' and value['stage'] == 'CHECK_INPUTS')
        _need(type(value['ordinal']) is int and value['ordinal'] == 0)
        _need(type(value['operationId']) is str
              and str(UUID(value['operationId'])) == value['operationId'])
        _need(_decimal(value['childPid'], positive=True, maximum=2**63) == os.getpid())
        root = value['controlRoot']
        _need(type(root) is str and '\x00' not in root and root.startswith('/')
              and not root.startswith('//') and str(Path(root)) == root
              and '..' not in Path(root).parts)

        def frozen():
            return Frozen(value['selectionSha256'], value['profileSha256'], value['runtimeSha256'])

        def writer():
            return Writer(value['installationId'], value['workerInstanceId'],
                          _decimal(value['ownerEpoch'], maximum=2**63), value['runId'],
                          value['attemptId'], value['stage'], value['ordinal'],
                          value['checkpointId'], value['commandSha256'])

        def identity(prefix):
            return Identity(_decimal(value[prefix + 'Device']),
                            _decimal(value[prefix + 'Inode'], positive=True))

        expected = Expected(value['installationId'], value['policy'], value['planKind'],
                            value['runId'], value['attemptId'], value['sourceId'], Path(root),
                            identity('control'), identity('root'), identity('lock'),
                            value['ledgerSha256'], frozen(), writer())
        current = Current(value['policy'], frozen(), writer())
        validate_expectations(expected, current)
        return expected, current
    except (ValueError, TypeError, KeyError, Failure) as error:
        raise StartupError() from error


def run_probe(fd):
    """EOF is the gate: a valid prefix alone cannot start filesystem inspection."""
    expected, current = parse_bootstrap(receive_bootstrap(fd))
    StrictSink(expected, current).read_command()


def main():
    try:
        _need(len(sys.argv) == 1)
        run_probe(sys.stdin.fileno())
        return 0
    except (StartupError, Failure, OSError, ValueError):
        sys.stderr.write(ERROR + '\n')
        return 1


if __name__ == '__main__':
    sys.exit(main())
