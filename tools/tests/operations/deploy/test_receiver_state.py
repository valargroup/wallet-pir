"""Small stopped-state transfers preserve contents and refuse corruption."""
import gzip
import json
import os
from pathlib import Path
import pwd
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[4]
HELPER = ROOT / 'tools/lib/wallet_pir_ops/deploy/receiver_state.py'


class ReceiverState(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        source = self.root / 'index'
        (source / 'distribution').mkdir(parents=True)
        (source / 'publisher.lock').touch()
        self.data = b'receiver payment rows' + bytes(512 * 1024)
        (source / 'directory-v3.sqlite').write_bytes(self.data)
        (source / 'distribution/authority.json').write_text('{"sequence":17}')
        self.packed = subprocess.run([sys.executable, str(HELPER), 'pack', str(source), 'test'],
                                     capture_output=True, check=True, timeout=10).stdout
        self.destination = self.root / '.receiver-adoption-test'

    def unpack(self, data):
        return subprocess.run([sys.executable, str(HELPER), 'unpack', str(self.destination), 'test',
                               pwd.getpwuid(os.getuid()).pw_name], input=data, capture_output=True, timeout=10)

    def test_sparse_state_is_compressed_and_restored_with_its_authority(self):
        self.assertLess(len(self.packed), len(self.data) // 10)
        result = self.unpack(self.packed)
        self.assertEqual(result.returncode, 0, result.stderr.decode())
        self.assertEqual((self.destination / 'directory-v3.sqlite').read_bytes(), self.data)
        self.assertEqual(json.loads((self.destination / 'distribution/authority.json').read_text())['sequence'], 17)

    def test_changed_contents_are_refused_even_with_a_valid_gzip_container(self):
        data = bytearray(gzip.decompress(self.packed))
        data[data.index(b'receiver payment rows')] ^= 1
        result = self.unpack(gzip.compress(bytes(data)))
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('differs from its manifest', result.stderr.decode())
        self.assertFalse(self.destination.exists())
