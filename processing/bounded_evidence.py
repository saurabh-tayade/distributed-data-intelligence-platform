"""Fixed Content private-output sink. Reservations are issued by the supervising worker."""
import fcntl
import json
import os
from pathlib import Path
import stat
from publication_metadata import canonical_bytes

MANIFEST = '.content-evidence.json'
LOCK = '.content-evidence.lock'
LEDGER_LIMIT = 256 * 1024


class Failure(Exception):
    code = 'EVIDENCE_UNAVAILABLE'


def need(value):
    if not value:
        raise Failure()


def private(path, directory=False):
    info = path.lstat()
    need(path.is_absolute() and path == path.resolve(strict=True))
    need(stat.S_ISDIR(info.st_mode) if directory else stat.S_ISREG(info.st_mode))
    need(info.st_uid == os.getuid() and info.st_mode & 0o077 == 0)
    if not directory:
        need(info.st_nlink == 1)
    return info


class Sink:
    def __init__(self, command):
        self.directory = command.parent
        self.root = None
        for candidate in (self.directory, *list(self.directory.parents)[:4]):
            if (candidate / LOCK).exists():
                self.root = candidate
                break
        need(self.root is not None)
        private(self.directory, True)
        self.identity = private(self.root, True)
        with self._lock():
            self._manifest()

    def _lock(self):
        import contextlib
        @contextlib.contextmanager
        def locked():
            private(self.root / LOCK)
            fd = os.open(self.root / LOCK, os.O_WRONLY | os.O_NOFOLLOW)
            try:
                fcntl.lockf(fd, fcntl.LOCK_EX)
                yield
            finally:
                os.close(fd)
        return locked()

    def _manifest(self):
        current = private(self.root, True)
        need((current.st_dev, current.st_ino) == (self.identity.st_dev, self.identity.st_ino))
        need(not (self.root / (MANIFEST + '.preparing')).exists())
        info = private(self.root / MANIFEST)
        need(info.st_size <= LEDGER_LIMIT)
        with (self.root / MANIFEST).open('rb') as stream:
            raw = stream.read(LEDGER_LIMIT + 1)
        need(len(raw) <= LEDGER_LIMIT)
        value = json.loads(raw)
        need(canonical_bytes(value) == raw and value['version'] == 1)
        need((value['rootDevice'], value['rootInode']) == (str(current.st_dev), str(current.st_ino)))
        need(self.root.parts[-4:] == ('runs', value['runId'], 'attempts', value['attemptId']))
        need(all(type(cap) is int and 0 < cap <= 16*1024**2 for cap in value['files'].values()))
        need(2*LEDGER_LIMIT + sum(value['files'].values()) <= 64*1024**2)
        need(len(value['directories']) + len(value['files']) + 3 <= 4096)
        return value

    def read_command(self, command):
        with self._lock():
            manifest = self._manifest()
            relative = command.relative_to(self.root).as_posix()
            need(relative in manifest['files'])
            before = private(command)
            maximum = min(manifest['files'][relative], 1024**2)
            need(before.st_size <= maximum)
            with command.open('rb') as stream:
                raw = stream.read(maximum + 1)
            after = private(command)
            need(len(raw) <= maximum and len(raw) == before.st_size)
            need((before.st_dev, before.st_ino, before.st_mtime_ns, before.st_size) ==
                 (after.st_dev, after.st_ino, after.st_mtime_ns, after.st_size))
            return raw

    def write(self, name, data):
        need(type(data) is bytes and '/' not in name and name in {
            'evidence.json', 'selection.json', 'result.json'})
        target = self.directory / name
        temporary = self.directory / (name + '.preparing')
        with self._lock():
            manifest = self._manifest()
            for path in (target, temporary):
                relative = path.relative_to(self.root).as_posix()
                need(relative in manifest['files'])
                if len(data) > manifest['files'][relative]:
                    error = Failure()
                    error.code = 'RESOURCE_LIMIT'
                    raise error
                need(not os.path.lexists(path))
            fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
            with os.fdopen(fd, 'wb') as stream:
                stream.write(data)
                stream.flush()
                os.fsync(stream.fileno())
            need(temporary.read_bytes() == data and not os.path.lexists(target))
            os.rename(temporary, target)
            fd = os.open(self.directory, os.O_RDONLY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
            need(private(target).st_size == len(data) and target.read_bytes() == data)
