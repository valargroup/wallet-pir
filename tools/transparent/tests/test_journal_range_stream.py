"""The SSH capture preserves byte extents; native decoding is a separate gate."""
import importlib.util
import io
import json
from pathlib import Path
import struct
import tarfile
import tempfile
import unittest

SCRIPT = Path(__file__).resolve().parents[1] / 'scripts/journal-range-stream.py'
SPEC = importlib.util.spec_from_file_location('journal_range_stream', SCRIPT)
STREAM = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(STREAM)


class JournalRangeStreamTest(unittest.TestCase):
    def fixture(self, root):
        (root / 'meta.json').write_text(json.dumps(dict(version=3, start_height=10, genesis_hash='01' * 32)))
        (root / 'events.bin').write_bytes(b'aaa' + b'bbbb' + b'ccccc' + b'uncommitted')
        records = b''.join(bytes([i]) * 32 + struct.pack('<QQ', offset, 1)
                           for i, offset in enumerate((0, 3, 7), 10))
        (root / 'blocks.bin').write_bytes(records)
        (root / 'checkpoint.bin').write_bytes(struct.pack('<QQ', 12, len(records)))

    def test_inclusive_range_rebases_offsets_and_excludes_uncommitted_tail(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.fixture(root)
            before = {p.name: p.read_bytes() for p in root.iterdir()}
            output = io.BytesIO()
            report = STREAM.capture(root, 11, 12, output)
            with tarfile.open(fileobj=io.BytesIO(output.getvalue())) as archive:
                self.assertEqual(archive.extractfile('events.bin').read(), b'bbbbccccc')
                records = archive.extractfile('blocks.bin').read()
                self.assertEqual(struct.unpack_from('<Q', records, 32)[0], 0)
                self.assertEqual(struct.unpack_from('<Q', records, 80)[0], 4)
                self.assertEqual(json.load(archive.extractfile('meta.json'))['start_height'], 11)
                self.assertEqual(json.load(archive.extractfile('capture.json')), report)
            self.assertEqual(report['blocks_without_sidecars'], 2)
            self.assertEqual(report['parent_hash'], '0a' * 32)
            self.assertEqual(before, {p.name: p.read_bytes() for p in root.iterdir()})

    def test_refuses_uncommitted_range_and_symlinked_inputs(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.fixture(root)
            with self.assertRaisesRegex(ValueError, 'not committed'):
                STREAM.capture(root, 11, 13, io.BytesIO())
            (root / 'events.bin').rename(root / 'real-events')
            (root / 'events.bin').symlink_to('real-events')
            with self.assertRaisesRegex(ValueError, 'regular journal file'):
                STREAM.capture(root, 11, 12, io.BytesIO())

    def test_preserves_both_sidecar_families(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.fixture(root)
            for family in ('display-v1', 'display-v2x'):
                (root / family).mkdir()
                (root / family / ('0b' * 32 + '.bin')).write_bytes(family.encode())
            output = io.BytesIO()
            report = STREAM.capture(root, 11, 12, output)
            self.assertEqual(report['blocks_without_sidecars'], 1)
            with tarfile.open(fileobj=io.BytesIO(output.getvalue())) as archive:
                for family in ('display-v1', 'display-v2x'):
                    self.assertEqual(archive.extractfile(f'{family}/' + '0b' * 32 + '.bin').read(), family.encode())


if __name__ == '__main__':
    unittest.main()
