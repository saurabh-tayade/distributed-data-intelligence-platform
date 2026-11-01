"""Producer bounds and interrupted writes, without native publication or Spark."""
import multiprocessing
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch
import uuid
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from bounded_evidence import Sink, Failure, MANIFEST, LOCK
from publication_metadata import canonical_bytes


def locked_writer(command, started, done):
    started.set()
    Sink(Path(command)).write('result.json', b'1234')
    done.set()


class BoundedEvidenceTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        run, attempt = str(uuid.uuid4()), str(uuid.uuid4())
        self.root = Path(self.temp.name).resolve() / 'runs' / run / 'attempts' / attempt
        self.directory = self.root / 'verify-result' / str(uuid.uuid4())
        self.directory.mkdir(parents=True)
        for p in (self.directory, *self.directory.parents):
            if p == Path(self.temp.name).resolve().parent:
                break
            p.chmod(0o700)
        (self.root / LOCK).touch(mode=0o600)
        self.command = self.directory / 'command.json'
        self.command.write_bytes(b'{}'); self.command.chmod(0o600)
        info = self.root.stat()
        self.value = dict(version=1, treeKind='ATTEMPT', runId=run, attemptId=attempt,
                          rootDevice=str(info.st_dev), rootInode=str(info.st_ino),
                          directories=['', 'verify-result', str(self.directory.relative_to(self.root))], files={})
        for name in ('command.json', 'evidence.json', 'result.json', 'selection.json'):
            for suffix in ('', '.preparing'):
                self.value['files'][str(self.directory.relative_to(self.root) / (name + suffix))] = 4
        self.ledger()

    def tearDown(self):
        self.temp.cleanup()

    def ledger(self):
        (self.root / MANIFEST).write_bytes(canonical_bytes(self.value))
        (self.root / MANIFEST).chmod(0o600)

    def test_exact_per_file_capacity_and_first_rejected_byte(self):
        sink = Sink(self.command)
        sink.write('evidence.json', b'1234')
        with self.assertRaises(Failure): sink.write('result.json', b'12345')
        self.assertFalse((self.directory / 'result.json.preparing').exists())
        self.assertEqual((self.directory / 'evidence.json').read_bytes(), b'1234')

    def test_unreserved_file_and_repeated_verification_do_not_grow(self):
        sink = Sink(self.command); sink.write('evidence.json', b'1234')
        with self.assertRaises(Failure): sink.write('evidence.json', b'1')
        with self.assertRaises(Failure): sink.write('other.json', b'1')
        self.assertEqual((self.directory / 'evidence.json').read_bytes(), b'1234')

    def test_interrupted_atomic_publication_retains_prefix(self):
        sink = Sink(self.command)
        with patch('bounded_evidence.os.rename', side_effect=OSError('interruption')):
            with self.assertRaises(OSError): sink.write('result.json', b'1234')
        self.assertFalse((self.directory / 'result.json').exists())
        self.assertEqual((self.directory / 'result.json.preparing').read_bytes(), b'1234')
        with self.assertRaises(Failure): sink.write('result.json', b'1234')

    def test_missing_corrupt_and_unavailable_ledger_fail_before_output(self):
        for raw in (b'{', b'{}'):
            (self.root / MANIFEST).write_bytes(raw)
            with self.assertRaises((ValueError, KeyError, Failure)): Sink(self.command)
        self.ledger(); (self.root / MANIFEST).unlink()
        with self.assertRaises(FileNotFoundError): Sink(self.command)
        self.assertFalse((self.directory / 'result.json').exists())

    def test_unsafe_or_aliased_ledger_is_unavailable(self):
        (self.root / MANIFEST).chmod(0o644)
        with self.assertRaises(Failure): Sink(self.command)
        (self.root / MANIFEST).chmod(0o600)
        retained = Path(self.temp.name).resolve() / 'held-ledger'
        (self.root / MANIFEST).rename(retained); (self.root / MANIFEST).symlink_to(retained)
        with self.assertRaises(Failure): Sink(self.command)
        self.assertFalse((self.directory / 'result.json').exists())

    def test_pending_ledger_never_authorizes_writes(self):
        (self.root / (MANIFEST + '.preparing')).touch(mode=0o600)
        with self.assertRaises(Failure): Sink(self.command)

    def test_changed_root_identity_is_refused(self):
        self.value['rootInode'] = '0'; self.ledger()
        with self.assertRaises(Failure): Sink(self.command)

    def test_aggregate_bytes_and_entry_limits_are_independent(self):
        self.value['files'] = {str(i): 16*1024**2 for i in range(4)}; self.ledger()
        with self.assertRaises(Failure): Sink(self.command)
        self.value['files'] = {str(i): 1 for i in range(4096)}; self.ledger()
        with self.assertRaises(Failure): Sink(self.command)

    def test_missing_command_and_first_byte_beyond_reservation_are_refused(self):
        sink = Sink(self.command); self.assertEqual(sink.read_command(self.command), b'{}')
        self.command.write_bytes(b'12345')
        with self.assertRaises(Failure): sink.read_command(self.command)
        self.command.unlink()
        with self.assertRaises(FileNotFoundError): sink.read_command(self.command)

    def test_oversized_proof_after_a_native_effect_is_retained_as_a_safe_failure(self):
        # This fault models the unclosed native-output envelope. It is not native compatibility evidence.
        from bounded_content_producer import main
        evidence = str(self.directory.relative_to(self.root) / 'evidence.json')
        result = str(self.directory.relative_to(self.root) / 'result.json')
        for name in (evidence, evidence + '.preparing'): self.value['files'][name] = 1024**2
        for name in (result, result + '.preparing'): self.value['files'][name] = 65536
        self.ledger()
        native = Path(self.temp.name).resolve() / 'retained.native'
        def publish(command):
            native.write_bytes(b'immutable publication')
            return dict(status='VERIFIED', proof='x'*(1024**2))
        with patch('managed_content.execute', side_effect=publish) as execute, patch.object(sys, 'argv', ['bounded', 'CONTENT', str(self.command)]):
            self.assertEqual(main(), 1); self.assertEqual(execute.call_count, 1)
        import json
        failure = json.loads((self.directory / 'result.json').read_bytes())
        self.assertEqual(failure['code'], 'RESOURCE_LIMIT'); self.assertEqual(failure['status'], 'FAILED')
        self.assertEqual(native.read_bytes(), b'immutable publication')
        self.assertFalse((self.directory / 'evidence.json').exists())
        self.assertFalse((self.directory / 'evidence.json.preparing').exists())

    def test_another_process_cannot_bypass_the_shared_lock(self):
        import fcntl
        context = multiprocessing.get_context('spawn')
        started, done = context.Event(), context.Event()
        with (self.root / LOCK).open('wb') as lock:
            fcntl.lockf(lock, fcntl.LOCK_EX)
            child = context.Process(target=locked_writer, args=(str(self.command), started, done))
            child.start()
            try:
                self.assertTrue(started.wait(10)); self.assertFalse(done.wait(.2))
                self.assertFalse((self.directory / 'result.json').exists())
                fcntl.lockf(lock, fcntl.LOCK_UN)
                self.assertTrue(done.wait(10)); child.join(10); self.assertEqual(child.exitcode, 0)
            finally:
                if child.is_alive(): child.terminate(); child.join(10)
        self.assertEqual((self.directory / 'result.json').read_bytes(), b'1234')


if __name__ == '__main__':
    unittest.main()
