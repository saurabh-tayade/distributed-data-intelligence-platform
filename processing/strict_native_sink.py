"""One-invocation effect claim on the unchanged ORIGINAL ledger-v2 plan.

The claim is the already reserved result prefix, not an extra sidecar or lock.
Only its originating process and open descriptor may finish it. A new sink never
adopts prefixes, including an empty prefix left before any external effect.
"""
import os
import stat
from pathlib import Path
from hashlib import sha256

import narrow_evidence_sink as base


class EffectSink(base.StrictSink):
    def __init__(self, expected, current):
        self._claim_fd = None
        self._claim_stamp = None
        self._pid = os.getpid()
        self._finished = False
        self._command_stamp = None
        self._operation_identity = None
        super().__init__(expected, current)
        with self._locked():
            self._command_stamp = base._read(self._directory / 'command.json',
                base._slots(expected.writer.stage)['command.json'])[1]
            self._operation_identity = base._identity(base._private(self._directory, True))
            self._inspect()

    def _owned(self, path):
        base._need(os.getpid() == self._pid and self._claim_fd is not None)
        base._need(path == self._directory / 'result.json.preparing')
        current = base._stamp(base._private(path))
        base._need(current == self._claim_stamp == base._stamp(os.fstat(self._claim_fd)))

    def _inspect(self):
        raw, ledger_stamp = base._read(self._root / base.MANIFEST, base.LEDGER_LIMIT)
        base._need(raw == self._ledger and sha256(raw).hexdigest() == self._expected.ledger_sha256,
                   'INPUT_CHANGED')
        actual_bytes, entries = 0, 0
        pending = [self._root]
        while pending:
            path = pending.pop()
            relative = path.relative_to(self._root).as_posix() if path != self._root else ''
            entries += 1
            base._need(entries <= base.ENTRY_LIMIT, 'RESOURCE_LIMIT')
            info = path.lstat()
            directory = stat.S_ISDIR(info.st_mode)
            base._private(path, directory)
            if directory:
                base._need(relative in self._plan['directories'] and len(path.relative_to(self._root).parts) < 4)
                with os.scandir(path) as children:
                    for child in children:
                        base._need(entries + len(pending) < base.ENTRY_LIMIT, 'RESOURCE_LIMIT')
                        pending.append(Path(child.path))
            else:
                if relative.endswith('.preparing'):
                    self._owned(path)
                cap = (0 if relative == base.LOCK else base.LEDGER_LIMIT if relative == base.MANIFEST
                       else self._plan['files'].get(relative))
                base._need(cap is not None and info.st_size <= cap,
                           'RESOURCE_LIMIT' if cap is not None else 'INPUT_CHANGED')
                actual_bytes += info.st_size
                base._need(actual_bytes <= base.BYTE_LIMIT, 'RESOURCE_LIMIT')
        command, stamp = base._read(self._directory / 'command.json',
                                  base._slots(self._expected.writer.stage)['command.json'])
        base._need(sha256(command).hexdigest() == self._expected.writer.command_sha256, 'INPUT_CHANGED')
        if self._command_stamp is not None:
            base._need(stamp == self._command_stamp, 'INPUT_CHANGED')
        if self._operation_identity is not None:
            base._need(base._identity(base._private(self._directory, True)) == self._operation_identity)
        after, after_stamp = base._read(self._root / base.MANIFEST, base.LEDGER_LIMIT)
        base._need(raw == after and ledger_stamp == after_stamp)
        self._check_paths()
        return base.Totals(actual_bytes, entries, *self._reserved), command

    def claim(self):
        """Force the one-shot prefix before Spark, object GETs, or publication."""
        with self._locked():
            base._need(self._claim_fd is None and not self._finished and os.getpid() == self._pid)
            self._inspect()
            base._need(base._absent(self._directory / 'result.json'))
            # Reject already populated child slots even if result is missing.
            for name in base._CHILD_NAMES.intersection(base._slots(self._expected.writer.stage)):
                base._need(base._absent(self._directory / name))
            path = self._directory / 'result.json.preparing'
            self._claim_fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
            self._claim_stamp = base._stamp(os.fstat(self._claim_fd))
            os.fsync(self._claim_fd)
            self._force_directory()
            self._inspect()

    def write(self, name, data):
        base._need(name != 'result.json' and self._claim_fd is not None and not self._finished)
        super().write(name, data)

    def finish(self, data):
        base._need(type(data) is bytes and len(data) < 65536, 'RESOURCE_LIMIT')
        path, target = self._directory / 'result.json.preparing', self._directory / 'result.json'
        with self._locked():
            base._need(not self._finished)
            self._inspect()
            self._owned(path)
            base._need(base._absent(target))
            # Never truncate or retry an uncertain partial result write.
            base._need(os.fstat(self._claim_fd).st_size == 0)
            view = memoryview(data)
            while view:
                written = os.write(self._claim_fd, view)
                base._need(written > 0)
                view = view[written:]
            os.fsync(self._claim_fd)
            self._claim_stamp = base._stamp(os.fstat(self._claim_fd))
            self._owned(path)
            base._need(base._read(path, 65535)[0] == data)
            self._force_directory()
            self._check_paths()
            os.link(path, target, follow_symlinks=False)
            self._force_directory()
            os.unlink(path)
            self._force_directory()
            os.close(self._claim_fd)
            self._claim_fd = None
            self._finished = True
            base._need(base._read(target, 65535)[0] == data)
            self._inspect()

    def close(self):
        """Release this invocation's descriptor, never remove retained evidence."""
        if self._claim_fd is not None:
            os.close(self._claim_fd)
            self._claim_fd = None
