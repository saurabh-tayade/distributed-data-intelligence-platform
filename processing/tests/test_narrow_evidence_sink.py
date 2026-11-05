"""Disposable ORIGINAL-only strict sink tests; no native effects or production launcher."""
from dataclasses import replace
import fcntl
from hashlib import sha256
import multiprocessing
import os
from pathlib import Path
import shutil
import sys
import tempfile
import unittest
from unittest.mock import patch
from uuid import uuid4

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import narrow_evidence_sink as sink
from narrow_transport_json import canonical_envelope


def identity(path):
    info = path.stat()
    return sink.Identity(info.st_dev, info.st_ino)


def snapshot(root):
    paths = [root, *root.rglob('*')] if root.exists() else []
    return {str(path.relative_to(root)): (path.lstat().st_ino, path.lstat().st_mtime_ns,
            path.lstat().st_mode, path.read_bytes() if path.is_file() and not path.is_symlink() else None,
            os.readlink(path) if path.is_symlink() else None) for path in paths}


def child_probe(expected, current, connection, name=None, data=None):
    # multiprocessing spawn transports test-only objects, not a production bootstrap contract.
    try:
        connection.send('STARTED')
        value = sink.StrictSink(expected, current)
        if name is not None:
            value.write(name, data)
        connection.send(('OK', value.inspect()))
    except sink.Failure as error:
        connection.send(('REJECTED', error.code))
    finally:
        connection.close()


class NarrowEvidenceSinkTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.control = Path(self.temp.name).resolve()
        self.run, self.attempt = str(uuid4()), str(uuid4())
        self.root = self.control / 'runs' / self.run / 'attempts' / self.attempt
        self.root.mkdir(parents=True)
        for path in (self.root, *self.root.parents):
            if path == self.control.parent:
                break
            path.chmod(0o700)
        self.file(self.root / sink.LOCK, b'')
        frozen = sink.Frozen('a' * 64, 'b' * 64, 'c' * 64)
        self.installation = str(uuid4())
        writer = sink.Writer(self.installation, str(uuid4()), 1, self.run, self.attempt, 'PUBLISH_GOLD', 0,
                             sink.checkpoint_id(self.run, self.attempt, 'PUBLISH_GOLD', 0),
                             sha256(b'{}').hexdigest())
        self.expected = sink.Expected(self.installation, sink.POLICY, 'ORIGINAL', self.run, self.attempt, 'source-one',
                                      self.control, identity(self.control), identity(self.root),
                                      identity(self.root / sink.LOCK), '0' * 64, frozen, writer)
        self.current = sink.Current(sink.POLICY, frozen, writer)
        self.ledger = sink._plan(self.expected)
        self.write_ledger()
        self.directory = self.root / sink._address(self.expected, writer.stage, writer.ordinal)
        self.directory.mkdir(parents=True, mode=0o700)
        self.directory.parent.chmod(0o700)
        self.file(self.directory / 'command.json', b'{}')

    def tearDown(self):
        self.temp.cleanup()

    def file(self, path, data):
        path.write_bytes(data)
        path.chmod(0o600)

    def write_ledger(self, update_expected=True):
        raw = canonical_envelope(self.ledger, sink.LEDGER_LIMIT)
        self.file(self.root / sink.MANIFEST, raw)
        if update_expected:
            self.expected = replace(self.expected, ledger_sha256=sha256(raw).hexdigest())

    def strict(self):
        return sink.StrictSink(self.expected, self.current)

    def rejected_unchanged(self, call):
        before = snapshot(self.control)
        with self.assertRaises(sink.Failure):
            call()
        self.assertEqual(before, snapshot(self.control))

    def child(self, expected=None, current=None, name=None, data=None):
        context = multiprocessing.get_context('spawn')
        parent, child = context.Pipe()
        process = context.Process(target=child_probe,
                                  args=(expected or self.expected, current or self.current, child, name, data))
        before = snapshot(self.control)
        process.start()
        child.close()
        try:
            self.assertTrue(parent.poll(10))
            self.assertEqual('STARTED', parent.recv())
            self.assertTrue(parent.poll(10))
            result = parent.recv()
            process.join(10)
            self.assertEqual(0, process.exitcode)
            if name is None or result[0] == 'REJECTED':
                self.assertEqual(before, snapshot(self.control))
            return result
        finally:
            if process.is_alive():
                process.terminate()
                process.join(10)
            parent.close()

    def test_clean_inspection_and_fresh_process_do_not_change_bytes_or_mtimes(self):
        before = snapshot(self.control)
        value = self.strict()
        self.assertEqual(b'{}', value.read_command())
        totals = value.inspect()
        self.assertEqual((64 * 1024 * 1024, 221), (totals.reserved_bytes, totals.reserved_entries))
        self.assertLess(totals.inventory_maximum, 100 * 1024)
        self.assertEqual(before, snapshot(self.control))
        self.assertEqual(('OK', totals), self.child())

    def test_exact_and_plus_one_for_every_permitted_child_slot(self):
        original_expected, original_current = self.expected, self.current
        tested = 0
        for stage, count in sink._COUNTS.items():
            for ordinal in range(count):
                writer = replace(original_expected.writer, stage=stage, ordinal=ordinal,
                                 checkpoint_id=sink.checkpoint_id(self.run, self.attempt, stage, ordinal))
                expected = replace(original_expected, writer=writer)
                current = replace(original_current, writer=writer)
                directory = self.root / sink._address(expected, stage, ordinal)
                directory.mkdir(parents=True, exist_ok=True, mode=0o700)
                for path in directory.parents:
                    if path == self.root:
                        break
                    path.chmod(0o700)
                command = directory / 'command.json'
                if not command.exists():
                    self.file(command, b'{}')
                value = sink.StrictSink(expected, current)
                for name in sink._CHILD_NAMES.intersection(sink._slots(stage)):
                    cap = sink._slots(stage)[name]
                    self.rejected_unchanged(lambda: value.write(name, b'x' * (cap + 1)))
                    before = snapshot(self.control)
                    value.write(name, b'x' * cap)
                    for address, stamp in before.items():
                        path = self.control / address
                        if path.is_file():
                            self.assertEqual(stamp, snapshot(self.control)[address])
                    self.assertEqual(b'x' * cap, (directory / name).read_bytes())
                    self.rejected_unchanged(lambda: value.write(name, b'x' * cap))
                    tested += 1
        self.assertEqual(40, tested)

    def test_invalid_names_and_parent_owned_slots_fail_before_effect(self):
        value = self.strict()
        for name in ('command.json', 'child.json', 'verified.json', 'complete.json', 'payload.xml',
                     '../result.json', '/result.json', 'result.json.preparing', 'other.json', None):
            with self.subTest(name=name):
                self.rejected_unchanged(lambda: value.write(name, b'{}'))
        self.rejected_unchanged(lambda: value.write('result.json', bytearray(b'{}')))

    def test_wrong_operation_slot_is_not_borrowed_from_another_operation(self):
        writer = replace(self.expected.writer, stage='SILVER',
                         checkpoint_id=sink.checkpoint_id(self.run, self.attempt, 'SILVER', 0))
        expected, current = replace(self.expected, writer=writer), replace(self.current, writer=writer)
        directory = self.root / sink._address(expected, 'SILVER', 0)
        directory.mkdir(parents=True, mode=0o700)
        directory.parent.chmod(0o700)
        self.file(directory / 'command.json', b'{}')
        value = sink.StrictSink(expected, current)
        for name in ('evidence.json', 'selection.json'):
            self.rejected_unchanged(lambda: value.write(name, b'{}'))

    def test_missing_markers_and_root_are_never_discovered_or_initialized(self):
        for name in (sink.MANIFEST, sink.LOCK):
            path = self.root / name
            raw = path.read_bytes()
            path.unlink()
            self.assertEqual('REJECTED', self.child()[0])
            self.file(path, raw)
            if name == sink.LOCK:
                self.expected = replace(self.expected, lock_identity=identity(path))
        shutil.rmtree(self.root)
        self.assertEqual('REJECTED', self.child()[0])

    def test_alternate_or_downgraded_ledger_even_with_new_digest_is_refused(self):
        original = self.ledger
        for change in ('version', 'source', 'run', 'attempt', 'plan', 'policy', 'cap', 'extra-file', 'extra-directory'):
            import copy
            self.ledger = copy.deepcopy(original)
            if change == 'version':
                self.ledger['version'] = 1
                for key in ('reservationPolicy', 'planKind', 'sourceId'):
                    del self.ledger[key]
            elif change == 'source':
                self.ledger = sink._plan(replace(self.expected, source_id='different'))
            elif change in ('run', 'attempt'):
                self.ledger['runId' if change == 'run' else 'attemptId'] = str(uuid4())
            elif change == 'plan':
                self.ledger['planKind'] = 'RECOVERY'
            elif change == 'policy':
                self.ledger['reservationPolicy'] = 'other'
            elif change == 'cap':
                self.ledger['files'][next(iter(self.ledger['files']))] += 1
            elif change == 'extra-file':
                self.ledger['files']['extra.json'] = 1
            else:
                self.ledger['directories'].append('extra')
            self.write_ledger()
            with self.subTest(change=change):
                self.assertEqual('REJECTED', self.child()[0])
        self.ledger = original
        self.write_ledger()

    def test_stale_ledger_digest_and_noncanonical_bytes_are_refused(self):
        self.file(self.root / sink.MANIFEST, (self.root / sink.MANIFEST).read_bytes() + b'\n')
        self.rejected_unchanged(self.strict)
        self.write_ledger()
        self.expected = replace(self.expected, ledger_sha256='d' * 64)
        self.rejected_unchanged(self.strict)

    def test_every_independent_current_identity_is_required(self):
        for field in ('selection_sha256', 'profile_sha256', 'runtime_sha256'):
            current = replace(self.current, frozen=replace(self.current.frozen, **{field: 'd' * 64}))
            self.assertEqual('REJECTED', self.child(current=current)[0])
        for current in (replace(self.current, policy=None), replace(self.current, policy='legacy'),
                        replace(self.current, writer=None), replace(self.current, frozen=None)):
            self.assertEqual('REJECTED', self.child(current=current)[0])

    def test_recovery_and_inherited_current_attempts_are_not_supported(self):
        for kind in ('RECOVERY', 'RESUME', '', None):
            expected = replace(self.expected, plan_kind=kind)
            self.rejected_unchanged(lambda: sink.StrictSink(expected, self.current))
        writer = replace(self.current.writer, attempt_id=str(uuid4()))
        self.assertEqual('REJECTED', self.child(current=replace(self.current, writer=writer))[0])

    def test_exact_operation_checkpoint_ordinal_and_command_are_required(self):
        variants = [replace(self.current.writer, installation_id=str(uuid4())),
                    replace(self.current.writer, worker_instance_id=str(uuid4())),
                    replace(self.current.writer, epoch=2), replace(self.current.writer, epoch=True),
                    replace(self.current.writer, epoch=-1), replace(self.current.writer, epoch=2**63),
                    replace(self.current.writer, stage='VERIFY_RESULT'),
                    replace(self.current.writer, ordinal=1), replace(self.current.writer, ordinal=True),
                    replace(self.current.writer, checkpoint_id=str(uuid4())),
                    replace(self.current.writer, command_sha256='d' * 64)]
        for writer in variants:
            with self.subTest(writer=writer):
                self.assertEqual('REJECTED', self.child(current=replace(self.current, writer=writer))[0])
        self.file(self.directory / 'command.json', b'{"different":true}')
        self.assertEqual('REJECTED', self.child()[0])

    def test_unknown_file_directory_and_any_retained_prefix_fail_closed(self):
        for path in (self.root / 'unknown', self.directory / 'unreserved.json',
                     self.root / (sink.MANIFEST + '.preparing'), self.directory / 'result.json.preparing'):
            self.file(path, b'{')
            self.rejected_unchanged(self.strict)
            path.unlink()
        unknown = self.root / 'unknown'
        unknown.mkdir(mode=0o700)
        self.rejected_unchanged(self.strict)
        unknown.rmdir()
        sibling = self.root / sink._address(self.expected, 'CHECK_INPUTS', 3)
        sibling.mkdir(parents=True, mode=0o700)
        sibling.parent.chmod(0o700)
        self.file(sibling / 'evidence.json.preparing', b'{')
        self.assertEqual('REJECTED', self.child()[0])

    def test_file_cap_plus_one_in_other_reserved_operation_is_refused(self):
        path = self.directory / 'child.json'
        self.file(path, b'x' * 65536)
        self.strict()
        self.file(path, b'x' * 65537)
        self.rejected_unchanged(self.strict)

    def test_exact_and_plus_one_command_limits_before_any_output(self):
        for stage in ('VERIFY_SILVER', 'PUBLISH_GOLD'):
            writer = replace(self.expected.writer, stage=stage, ordinal=0,
                             checkpoint_id=sink.checkpoint_id(self.run, self.attempt, stage, 0))
            directory = self.root / sink._address(self.expected, stage, 0)
            directory.mkdir(parents=True, exist_ok=True, mode=0o700)
            directory.parent.chmod(0o700)
            cap = sink._slots(stage)['command.json']
            data = b'x' * cap
            self.file(directory / 'command.json', data)
            writer = replace(writer, command_sha256=sha256(data).hexdigest())
            expected, current = replace(self.expected, writer=writer), replace(self.current, writer=writer)
            value = sink.StrictSink(expected, current)
            self.assertEqual(data, value.read_command())
            self.file(directory / 'command.json', data + b'x')
            self.rejected_unchanged(lambda: value.write('result.json', b'{}'))
            self.file(directory / 'command.json', b'{}')

    def test_lock_mutation_during_acquisition_is_not_an_identity_seal(self):
        value = self.strict()
        real_lock = fcntl.lockf
        def changed_lock(fd, operation):
            real_lock(fd, operation)
            info = (self.root / sink.LOCK).stat()
            os.utime(self.root / sink.LOCK, ns=(info.st_atime_ns, info.st_mtime_ns + 1000000))
        with patch('narrow_evidence_sink.fcntl.lockf', side_effect=changed_lock):
            with self.assertRaises(sink.Failure):
                value.write('result.json', b'{}')
        self.assertFalse((self.directory / 'result.json').exists())
        self.assertFalse((self.directory / 'result.json.preparing').exists())

    def test_missing_command_never_becomes_an_empty_command(self):
        (self.directory / 'command.json').unlink()
        self.assertEqual('REJECTED', self.child()[0])

    def test_symlinks_hardlinks_and_unsafe_permissions_are_refused(self):
        path = self.directory / 'command.json'
        path.chmod(0o644)
        self.rejected_unchanged(self.strict)
        path.chmod(0o600)
        held = self.control / 'held-command'
        path.rename(held)
        path.symlink_to(held)
        self.rejected_unchanged(self.strict)
        path.unlink()
        os.link(held, path)
        self.rejected_unchanged(self.strict)
        path.unlink()
        held.rename(path)
        self.directory.parent.chmod(0o755)
        self.rejected_unchanged(self.strict)

    def test_cyclic_symlink_returns_fixed_failure_without_path_or_mutation(self):
        command = self.directory / 'command.json'
        command.unlink()
        command.symlink_to('command.json')
        self.rejected_unchanged(self.strict)
        self.assertEqual(('REJECTED', 'EVIDENCE_UNAVAILABLE'), self.child())
        with self.assertRaises(sink.Failure) as failure:
            self.strict()
        self.assertEqual('EVIDENCE_UNAVAILABLE', str(failure.exception))

    def test_replaced_root_and_control_identities_are_refused(self):
        self.expected = replace(self.expected, root_identity=replace(self.expected.root_identity, inode=self.expected.root_identity.inode + 1))
        self.assertEqual('REJECTED', self.child()[0])
        self.expected = replace(self.expected, root_identity=identity(self.root),
                                control_identity=replace(self.expected.control_identity, inode=self.expected.control_identity.inode + 1))
        self.assertEqual('REJECTED', self.child()[0])

    def test_replaced_root_with_same_ledger_bytes_is_refused_in_fresh_process(self):
        retained = self.control / 'retained-root'
        self.root.rename(retained)
        shutil.copytree(retained, self.root, copy_function=shutil.copy2)
        self.assertNotEqual(identity(self.root), self.expected.root_identity)
        self.assertEqual('REJECTED', self.child()[0])

    def test_lock_replacement_with_identical_bytes_is_refused(self):
        lock = self.root / sink.LOCK
        lock.rename(self.control / 'held-lock')
        self.file(lock, b'')
        self.assertNotEqual(identity(lock), self.expected.lock_identity)
        self.assertEqual('REJECTED', self.child()[0])

    def test_lock_replacement_while_fresh_child_waits_cannot_authorize_output(self):
        context = multiprocessing.get_context('spawn')
        parent, child = context.Pipe()
        lock = self.root / sink.LOCK
        with lock.open('r+b') as stream:
            fcntl.lockf(stream, fcntl.LOCK_EX)
            process = context.Process(target=child_probe, args=(self.expected, self.current, child, 'result.json', b'{}'))
            process.start()
            child.close()
            try:
                self.assertTrue(parent.poll(10))
                self.assertEqual('STARTED', parent.recv())
                self.assertFalse(parent.poll(.2))
                lock.rename(self.control / 'held-lock')
                self.file(lock, b'')
                before = snapshot(self.control)
                fcntl.lockf(stream, fcntl.LOCK_UN)
                self.assertTrue(parent.poll(10))
                self.assertEqual('REJECTED', parent.recv()[0])
                process.join(10)
                self.assertEqual(0, process.exitcode)
                self.assertEqual(before, snapshot(self.control))
            finally:
                if process.is_alive():
                    process.terminate()
                    process.join(10)
                parent.close()

    def test_fresh_writer_obeys_lock_then_publishes_exact_reserved_slot(self):
        self.assertEqual('OK', self.child(name='result.json', data=b'{}')[0])
        self.assertEqual(b'{}', (self.directory / 'result.json').read_bytes())
        self.assertEqual('REJECTED', self.child(name='result.json', data=b'changed')[0])

    def test_interrupted_prefix_is_retained_and_never_repaired(self):
        value = self.strict()
        with patch('narrow_evidence_sink.os.link', side_effect=OSError('interrupted')):
            with self.assertRaises(sink.Failure):
                value.write('result.json', b'{}')
        self.assertEqual(b'{}', (self.directory / 'result.json.preparing').read_bytes())
        self.assertFalse((self.directory / 'result.json').exists())
        self.rejected_unchanged(lambda: value.write('result.json', b'{}'))
        self.assertEqual('REJECTED', self.child()[0])

    def test_interrupted_link_unlink_keeps_both_names_and_fails_closed(self):
        value = self.strict()
        with patch('narrow_evidence_sink.os.unlink', side_effect=OSError('interrupted')):
            with self.assertRaises(sink.Failure):
                value.write('result.json', b'{}')
        final, prefix = self.directory / 'result.json', self.directory / 'result.json.preparing'
        self.assertEqual(b'{}', final.read_bytes())
        self.assertEqual(b'{}', prefix.read_bytes())
        self.assertEqual(final.stat().st_ino, prefix.stat().st_ino)
        self.assertEqual(2, final.stat().st_nlink)
        self.assertEqual('REJECTED', self.child()[0])

    def test_absence_requires_enoent_not_permission_or_io_error(self):
        value = self.strict()
        real_lstat = Path.lstat
        target = self.directory / 'result.json'
        for error in (PermissionError('unavailable'), OSError('io failure')):
            def unavailable(path, *args, **kwargs):
                if path == target:
                    raise error
                return real_lstat(path, *args, **kwargs)
            before = snapshot(self.control)
            with patch.object(Path, 'lstat', unavailable):
                with self.assertRaises(sink.Failure):
                    value.write('result.json', b'{}')
            self.assertEqual(before, snapshot(self.control))

    def test_atomic_publication_never_overwrites_a_racing_final(self):
        value = self.strict()
        real_link = os.link
        final = self.directory / 'result.json'
        def raced_link(*args, **kwargs):
            self.file(final, b'other writer')
            return real_link(*args, **kwargs)
        with patch('narrow_evidence_sink.os.link', side_effect=raced_link):
            with self.assertRaises(sink.Failure):
                value.write('result.json', b'{}')
        self.assertEqual(b'other writer', final.read_bytes())
        self.assertEqual(b'{}', (self.directory / 'result.json.preparing').read_bytes())

    def test_empty_slot_output_is_valid_without_rewriting_existing_files(self):
        value = self.strict()
        value.write('result.json', b'')
        self.assertEqual(b'', (self.directory / 'result.json').read_bytes())
        value.inspect()

    def test_malformed_expectations_are_rejected_without_filesystem_access(self):
        malformed = [replace(self.expected, source_id='UPPER'), replace(self.expected, source_id='x' * 129),
                     replace(self.expected, control_root=Path('relative')), replace(self.expected, policy=None),
                     replace(self.expected, run_id='00000000-0000-4000-8000-00000000000A'), replace(self.expected, root_identity=None),
                     replace(self.expected, ledger_sha256='g' * 64), replace(self.expected, frozen=None)]
        with patch('narrow_evidence_sink.os.open', side_effect=AssertionError('filesystem must not be opened')):
            for expected in malformed:
                with self.subTest(expected=expected), self.assertRaises(sink.Failure):
                    sink.StrictSink(expected, self.current)


if __name__ == '__main__':
    unittest.main()
