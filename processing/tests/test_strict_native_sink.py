"""Owned result-prefix failures on the unchanged 64 MiB / 221-entry plan."""
from dataclasses import replace
import os
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

sys.path[:0] = [str(Path(__file__).resolve().parent), str(Path(__file__).resolve().parents[1])]
import narrow_evidence_sink as base
from strict_native_sink import EffectSink
from test_narrow_evidence_sink import NarrowEvidenceSinkTest, snapshot


class EffectSinkTest(unittest.TestCase):
    setUp = NarrowEvidenceSinkTest.setUp
    tearDown = NarrowEvidenceSinkTest.tearDown
    file = NarrowEvidenceSinkTest.file
    write_ledger = NarrowEvidenceSinkTest.write_ledger

    def sink(self):
        value = EffectSink(self.expected, self.current)
        self.addCleanup(value.close)
        return value

    def test_claim_is_forced_one_shot_and_does_not_hold_filesystem_lock(self):
        sink = self.sink()
        with patch('strict_native_sink.os.fsync', wraps=os.fsync) as sync:
            sink.claim()
        self.assertGreaterEqual(sync.call_count, 2)
        self.assertEqual(b'', (self.directory / 'result.json.preparing').read_bytes())
        # A new process can acquire the lock while effect work is underway.
        import subprocess
        script = "import fcntl,sys;f=open(sys.argv[1],'wb');fcntl.lockf(f,fcntl.LOCK_EX|fcntl.LOCK_NB)"
        result = subprocess.run([sys.executable, '-c', script, str(self.root / base.LOCK)], capture_output=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        sink.write('evidence.json', b'{}')
        sink.write('selection.json', b'{}')
        sink.finish(b'{"status":"VERIFIED"}')
        self.assertFalse((self.directory / 'result.json.preparing').exists())
        self.assertEqual(221, sink.inspect().reserved_entries)
        for action in (sink.claim, lambda: sink.finish(b'{}'), lambda: sink.write('evidence.json', b'{}')):
            with self.assertRaises(base.Failure):
                action()

    def test_fresh_sink_never_adopts_empty_or_written_prefix(self):
        sink = self.sink()
        sink.claim()
        for constructor in (base.StrictSink, EffectSink):
            with self.assertRaises(base.Failure):
                constructor(self.expected, self.current)
        sink.close()
        before = snapshot(self.control)
        with self.assertRaises(base.Failure):
            EffectSink(self.expected, self.current)
        self.assertEqual(before, snapshot(self.control))

    def test_replaced_claim_and_forked_identity_are_refused(self):
        sink = self.sink()
        sink.claim()
        with patch('strict_native_sink.os.getpid', return_value=os.getpid() + 1):
            with self.assertRaises(base.Failure):
                sink.inspect()
        path = self.directory / 'result.json.preparing'
        path.unlink()
        self.file(path, b'')
        with self.assertRaises(base.Failure):
            sink.finish(b'{}')
        self.assertFalse((self.directory / 'result.json').exists())

    def test_other_operation_or_other_preparation_is_never_tolerated(self):
        sink = self.sink()
        sink.claim()
        self.file(self.directory / 'evidence.json.preparing', b'')
        before = snapshot(self.control)
        with self.assertRaises(base.Failure):
            sink.finish(b'{}')
        self.assertEqual(before, snapshot(self.control))

    def test_replaced_command_with_same_bytes_is_refused(self):
        sink = self.sink()
        command = self.directory / 'command.json'
        command.unlink()
        self.file(command, b'{}')
        with self.assertRaises(base.Failure):
            sink.claim()
        self.assertFalse((self.directory / 'result.json.preparing').exists())

    def test_claim_fsync_failure_is_retained_without_effect_authority(self):
        sink = self.sink()
        with patch('strict_native_sink.os.fsync', side_effect=OSError('fault')):
            with self.assertRaises(base.Failure):
                sink.claim()
        sink.close()
        self.assertTrue((self.directory / 'result.json.preparing').exists())
        with self.assertRaises(base.Failure):
            EffectSink(self.expected, self.current)

    def test_finish_link_uncertainty_retains_both_names_and_refuses_fresh_process(self):
        sink = self.sink()
        sink.claim()
        real_link = os.link
        def uncertain(*args, **kwargs):
            real_link(*args, **kwargs)
            raise OSError('acknowledgment lost')
        with patch('strict_native_sink.os.link', side_effect=uncertain):
            with self.assertRaises(base.Failure):
                sink.finish(b'{}')
        self.assertEqual((self.directory / 'result.json').stat().st_nlink, 2)
        self.assertTrue((self.directory / 'result.json.preparing').exists())
        with self.assertRaises(base.Failure):
            EffectSink(self.expected, self.current)

    def test_result_limit_and_no_legacy_direct_write(self):
        sink = self.sink()
        with self.assertRaises(base.Failure):
            sink.write('evidence.json', b'{}')
        sink.claim()
        with self.assertRaises(base.Failure):
            sink.write('result.json', b'{}')
        with self.assertRaises(base.Failure):
            sink.finish(b'x' * 65536)
        sink.finish(b'x' * 65535)


del NarrowEvidenceSinkTest

if __name__ == '__main__':
    unittest.main()
