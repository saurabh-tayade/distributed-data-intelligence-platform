"""Inactive ORIGINAL-only evidence probe and fixed-slot sink.

Inputs are independently supplied in-memory expectations, not an authority learned
from a command/ledger. No launcher, initializer, receipt transport or recovery API.
Legacy bounded_evidence.Sink is deliberately not imported or changed.
"""
from contextlib import contextmanager
from dataclasses import dataclass
import fcntl
from hashlib import md5, sha256
import os
from pathlib import Path
import re
import stat
import threading
from uuid import UUID

from narrow_transport_json import canonical_envelope

POLICY = 'ddip-content-narrow-v1'
MANIFEST = '.content-evidence.json'
LOCK = '.content-evidence.lock'
LEDGER_LIMIT = 256 * 1024
BYTE_LIMIT = 64 * 1024 * 1024
ENTRY_LIMIT = 4096
INVENTORY_LIMIT = 128 * 1024
_COUNTS = {'FETCH_SOURCE': 1, 'STORE_BRONZE': 1, 'INSPECT_BRONZE': 5,
           'SILVER': 1, 'VERIFY_SILVER': 1, 'RECHECK_SILVER': 1,
           'PREPARE_GOLD': 1, 'PUBLISH_GOLD': 1, 'VERIFY_RESULT': 2,
           'CHECK_INPUTS': 4, 'PUBLISH_CONTENT': 1}
_CHILD_NAMES = frozenset({'evidence.json', 'selection.json', 'result.json'})
_LOCAL_LOCK = threading.RLock()


class Failure(Exception):
    """Fixed private diagnostic; never includes supplied identities or paths."""
    def __init__(self, code='EVIDENCE_UNAVAILABLE'):
        self.code = code
        super().__init__(code)


def _need(value, code='EVIDENCE_UNAVAILABLE'):
    if not value:
        raise Failure(code)


def _hash(value):
    _need(type(value) is str and re.fullmatch('[0-9a-f]{64}', value), 'INPUT_INVALID')


def _uuid(value):
    _need(type(value) is str, 'INPUT_INVALID')
    try:
        _need(str(UUID(value)) == value, 'INPUT_INVALID')
    except (ValueError, AttributeError) as error:
        raise Failure('INPUT_INVALID') from error


@dataclass(frozen=True, repr=False)
class Identity:
    device: int
    inode: int


@dataclass(frozen=True, repr=False)
class Frozen:
    selection_sha256: str
    profile_sha256: str
    runtime_sha256: str


@dataclass(frozen=True, repr=False)
class Writer:
    installation_id: str
    worker_instance_id: str
    epoch: int
    run_id: str
    attempt_id: str
    stage: str
    ordinal: int
    checkpoint_id: str
    command_sha256: str


@dataclass(frozen=True, repr=False)
class Expected:
    installation_id: str
    policy: str
    plan_kind: str
    run_id: str
    original_attempt_id: str
    source_id: str
    control_root: Path
    control_identity: Identity
    root_identity: Identity
    lock_identity: Identity
    ledger_sha256: str
    frozen: Frozen
    writer: Writer


@dataclass(frozen=True, repr=False)
class Current:
    policy: str
    frozen: Frozen
    writer: Writer


@dataclass(frozen=True)
class Totals:
    actual_bytes: int
    actual_entries: int
    reserved_bytes: int
    reserved_entries: int
    inventory_maximum: int


def checkpoint_id(run_id, attempt_id, stage, ordinal):
    """Java UUID.nameUUIDFromBytes, NOT Python uuid3(namespace, name)."""
    _uuid(run_id)
    _uuid(attempt_id)
    _need(type(stage) is str and stage in _COUNTS and type(ordinal) is int
          and 0 <= ordinal < _COUNTS[stage], 'INPUT_INVALID')
    identity = ('SILVER' if stage == 'VERIFY_SILVER' else stage) + ':' + str(ordinal)
    raw = (POLICY + '\n' + run_id + '\n' + attempt_id + '\n' + identity).encode('utf-8')
    return str(UUID(bytes=md5(raw, usedforsecurity=False).digest(), version=3))


def validate_expectations(expected, current):
    """Pure correspondence only; successful validation is not launch authority."""
    _need(type(expected) is Expected and type(current) is Current, 'INPUT_INVALID')
    _need(expected.policy == POLICY and expected.plan_kind == 'ORIGINAL', 'INPUT_INVALID')
    _uuid(expected.installation_id)
    _uuid(expected.run_id)
    _uuid(expected.original_attempt_id)
    _need(type(expected.source_id) is str
          and re.fullmatch('[a-z0-9][a-z0-9_-]{0,127}', expected.source_id), 'INPUT_INVALID')
    _need(isinstance(expected.control_root, Path) and expected.control_root.is_absolute()
          and '..' not in expected.control_root.parts, 'INPUT_INVALID')
    for identity in (expected.control_identity, expected.root_identity, expected.lock_identity):
        _need(type(identity) is Identity and type(identity.device) is int
              and type(identity.inode) is int and 0 <= identity.device < 10**20
              and 0 < identity.inode < 10**20, 'INPUT_INVALID')
    _hash(expected.ledger_sha256)
    for frozen in (expected.frozen, current.frozen):
        _need(type(frozen) is Frozen, 'INPUT_INVALID')
        for value in (frozen.selection_sha256, frozen.profile_sha256, frozen.runtime_sha256):
            _hash(value)
    for writer in (expected.writer, current.writer):
        _need(type(writer) is Writer, 'INPUT_INVALID')
        _uuid(writer.installation_id)
        _uuid(writer.worker_instance_id)
        _need(writer.installation_id == expected.installation_id and type(writer.epoch) is int
              and 0 <= writer.epoch < 2**63, 'INPUT_INVALID')
        _uuid(writer.run_id)
        _uuid(writer.attempt_id)
        _uuid(writer.checkpoint_id)
        _hash(writer.command_sha256)
        _need(writer.run_id == expected.run_id and writer.attempt_id == expected.original_attempt_id
              and writer.checkpoint_id == checkpoint_id(writer.run_id, writer.attempt_id,
                                                        writer.stage, writer.ordinal), 'INPUT_INVALID')
    _need(current.policy == expected.policy and current.frozen == expected.frozen
          and current.writer == expected.writer, 'INPUT_CHANGED')


def _slots(stage):
    small, quarter, large = 65536, 512 * 1024, 1024 * 1024
    if stage == 'FETCH_SOURCE':
        return {'command.json': small, 'payload.xml': 5 * large, 'metadata.json': small,
                'descriptor.json': small, 'result.json': small, 'complete.json': small}
    bronze = stage in {'STORE_BRONZE', 'INSPECT_BRONZE'}
    slots = {'command.json': small if bronze or stage in {'SILVER', 'VERIFY_SILVER', 'RECHECK_SILVER'}
             else large, 'result.json': small, 'verified.json': small}
    if not bronze:
        slots['child.json'] = small
    if stage != 'SILVER':
        slots['evidence.json'] = small if bronze else quarter if stage in {'VERIFY_SILVER', 'RECHECK_SILVER'} else large
    if stage in {'PUBLISH_GOLD', 'VERIFY_RESULT', 'VERIFY_SILVER'}:
        slots['selection.json'] = quarter if stage == 'VERIFY_SILVER' else large
    return slots


def _address(expected, stage, ordinal):
    parts = [stage.lower().replace('_', '-')]
    if stage == 'FETCH_SOURCE':
        parts.append(expected.source_id)
    parts.append(checkpoint_id(expected.run_id, expected.original_attempt_id, stage, ordinal))
    return '/'.join(parts)


def _plan(expected):
    directories, files = {''}, {}
    for stage, count in _COUNTS.items():
        for ordinal in range(count):
            address = _address(expected, stage, ordinal)
            parts = address.split('/')
            directories.update('/'.join(parts[:index]) for index in range(1, len(parts) + 1))
            for name, cap in _slots(stage).items():
                for suffix in ('', '.preparing'):
                    files[address + '/' + name + suffix] = cap
    return dict(version=2, treeKind='ATTEMPT', runId=expected.run_id,
                attemptId=expected.original_attempt_id, rootDevice=str(expected.root_identity.device),
                rootInode=str(expected.root_identity.inode), reservationPolicy=expected.policy,
                planKind=expected.plan_kind, sourceId=expected.source_id,
                directories=sorted(directories), files=files)


def _limits(plan):
    reserved_bytes = 2 * LEDGER_LIMIT + sum(plan['files'].values())
    reserved_entries = len(plan['directories']) + len(plan['files']) + 3
    prefix = 'runs/' + plan['runId'] + '/attempts/' + plan['attemptId']
    inventory = []
    for directory, names in ((True, plan['directories']), (False, list(plan['files']) + [MANIFEST, MANIFEST + '.preparing', LOCK])):
        for name in names:
            entry = dict(address=prefix + ('/' + name if name else ''), device='9' * 20,
                         inode='9' * 20, modifiedAt='x' * 64, directory=directory)
            if not directory:
                entry.update(length='9' * 20, sha256='f' * 64)
            inventory.append(entry)
    maximum = len(canonical_envelope(dict(processEvidenceIncomplete=True, files=inventory), INVENTORY_LIMIT))
    _need(reserved_bytes == BYTE_LIMIT and reserved_entries == 221 and maximum <= INVENTORY_LIMIT,
          'RESOURCE_LIMIT')
    return reserved_bytes, reserved_entries, maximum


def _identity(info):
    return Identity(info.st_dev, info.st_ino)


def _stamp(info):
    return (info.st_dev, info.st_ino, info.st_mode, info.st_uid, info.st_gid,
            info.st_nlink, info.st_size, info.st_mtime_ns, info.st_ctime_ns)


def _private(path, directory=False):
    info = path.lstat()
    _need(path.is_absolute()
          and (stat.S_ISDIR(info.st_mode) if directory else stat.S_ISREG(info.st_mode))
          and info.st_uid == os.getuid() and info.st_mode & 0o077 == 0
          and (directory or info.st_nlink == 1))
    try:
        _need(path == path.resolve(strict=True))
    except RuntimeError as error:  # Python 3.12 reports cyclic ancestors this way.
        raise Failure() from error
    return info


def _read(path, cap):
    before = _private(path)
    _need(before.st_size <= cap, 'RESOURCE_LIMIT')
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        _need(_stamp(os.fstat(fd)) == _stamp(before))
        with os.fdopen(fd, 'rb', closefd=False) as stream:
            raw = stream.read(cap + 1)
        _need(len(raw) == before.st_size and len(raw) <= cap
              and _stamp(before) == _stamp(os.fstat(fd)) == _stamp(_private(path)))
        return raw, _stamp(before)
    finally:
        os.close(fd)


def _absent(path):
    # lexists() can turn permission/I/O failures into apparent absence.
    try:
        path.lstat()
        return False
    except FileNotFoundError:
        return True


class StrictSink:
    """No discovery or initialization. Only the exact operation's child slots are writable."""
    def __init__(self, expected, current):
        validate_expectations(expected, current)
        self._expected, self._current = expected, current
        self._root = expected.control_root / 'runs' / expected.run_id / 'attempts' / expected.original_attempt_id
        self._plan = _plan(expected)
        self._ledger = canonical_envelope(self._plan, LEDGER_LIMIT)
        _need(sha256(self._ledger).hexdigest() == expected.ledger_sha256, 'INPUT_CHANGED')
        self._reserved = _limits(self._plan)
        writer = expected.writer
        self._directory = self._root / _address(expected, writer.stage, writer.ordinal)
        self.inspect()

    def _check_paths(self):
        expected = self._expected
        validate_expectations(expected, self._current)
        _need(_identity(_private(expected.control_root, True)) == expected.control_identity)
        path = expected.control_root
        for segment in self._root.relative_to(path).parts:
            path = path / segment
            _private(path, True)
        _need(_identity(_private(self._root, True)) == expected.root_identity)
        info = _private(self._root / LOCK)
        _need(_identity(info) == expected.lock_identity and info.st_size == 0)
        return info

    @contextmanager
    def _locked(self):
        # POSIX record locks serialize processes, not threads in the same process.
        with _LOCAL_LOCK:
            fd = None
            try:
                before = self._check_paths()
                fd = os.open(self._root / LOCK, os.O_WRONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
                _need(_stamp(os.fstat(fd)) == _stamp(before) == _stamp(self._check_paths()))
                fcntl.lockf(fd, fcntl.LOCK_EX)
                _need(_stamp(os.fstat(fd)) == _stamp(before) == _stamp(self._check_paths()))
                yield
                _need(_stamp(os.fstat(fd)) == _stamp(before) == _stamp(self._check_paths()))
            except Failure:
                raise
            except (OSError, ValueError) as error:
                raise Failure() from error
            finally:
                if fd is not None:
                    os.close(fd)

    def _inspect(self):
        raw, ledger_stamp = _read(self._root / MANIFEST, LEDGER_LIMIT)
        _need(raw == self._ledger and sha256(raw).hexdigest() == self._expected.ledger_sha256,
              'INPUT_CHANGED')
        actual_bytes, entries = 0, 0
        pending = [self._root]
        while pending:
            path = pending.pop()
            relative = path.relative_to(self._root).as_posix() if path != self._root else ''
            entries += 1
            _need(entries <= ENTRY_LIMIT, 'RESOURCE_LIMIT')
            info = path.lstat()
            directory = stat.S_ISDIR(info.st_mode)
            _private(path, directory)
            if directory:
                _need(relative in self._plan['directories'] and len(path.relative_to(self._root).parts) < 4)
                with os.scandir(path) as children:
                    # Bound traversal allocation even for an unknown huge directory.
                    for child in children:
                        _need(entries + len(pending) < ENTRY_LIMIT, 'RESOURCE_LIMIT')
                        pending.append(Path(child.path))
            else:
                _need(not relative.endswith('.preparing'))
                cap = 0 if relative == LOCK else LEDGER_LIMIT if relative == MANIFEST else self._plan['files'].get(relative)
                _need(cap is not None and info.st_size <= cap, 'RESOURCE_LIMIT' if cap is not None else 'INPUT_CHANGED')
                actual_bytes += info.st_size
                _need(actual_bytes <= BYTE_LIMIT, 'RESOURCE_LIMIT')
        command, _ = _read(self._directory / 'command.json', _slots(self._expected.writer.stage)['command.json'])
        _need(sha256(command).hexdigest() == self._expected.writer.command_sha256, 'INPUT_CHANGED')
        after, after_stamp = _read(self._root / MANIFEST, LEDGER_LIMIT)
        _need(raw == after and ledger_stamp == after_stamp)
        self._check_paths()
        return Totals(actual_bytes, entries, *self._reserved), command

    def inspect(self):
        with self._locked():
            return self._inspect()[0]

    def read_command(self):
        with self._locked():
            return self._inspect()[1]

    def write(self, name, data):
        _need(type(name) is str and name in _CHILD_NAMES and name in _slots(self._expected.writer.stage),
              'INPUT_INVALID')
        _need(type(data) is bytes, 'INPUT_INVALID')
        _need(len(data) <= _slots(self._expected.writer.stage)[name], 'RESOURCE_LIMIT')
        target, temporary = self._directory / name, self._directory / (name + '.preparing')
        with self._locked():
            self._inspect()
            _need(_absent(target) and _absent(temporary))
            fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
            with os.fdopen(fd, 'wb') as stream:
                stream.write(data)
                stream.flush()
                os.fsync(stream.fileno())
            _need(_read(temporary, len(data))[0] == data)
            self._force_directory()
            self._check_paths()
            # Atomic no-replace publication. A crash between link/unlink retains
            # both names (and nlink=2), which this strict lane refuses, never repairs.
            os.link(temporary, target, follow_symlinks=False)
            self._force_directory()
            os.unlink(temporary)
            self._force_directory()
            _need(_read(target, len(data))[0] == data)
            self._inspect()

    def _force_directory(self):
        fd = os.open(self._directory, os.O_RDONLY | os.O_NOFOLLOW | os.O_DIRECTORY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
