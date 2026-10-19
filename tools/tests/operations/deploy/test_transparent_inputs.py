"""The offline shard deploy gate refuses incomplete sets and malformed units."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[4]
SCRIPT = ROOT / 'tools/transparent/scripts/deploy-transparent-shard.sh'


class ShardInputs(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        self.shards = root / 'shard-set'
        self.shards.mkdir()
        for digest in ('aaa', 'bbb'):
            (self.shards / digest).mkdir()
        (self.shards / 'shards.json').write_text(json.dumps({'shards': [
            {'shard_id': index, 'manifest_digest': digest}
            for index, digest in enumerate(('aaa', 'bbb'))]}))
        artifact = root / 'artifact'
        artifact.mkdir()
        self.unit = artifact / 'transparent-shard-server.service'
        self.env = dict(os.environ, TRANSPARENT_WORKER_HOST='worker.example.net',
                        WALLET_PIR_DEPLOY_USER='deploy',
                        TRANSPARENT_RELEASE_SHA='da39a3ee5e6b4b0d3255bfef95601890afd80709',
                        TRANSPARENT_ARTIFACT_DIR=str(artifact),
                        TRANSPARENT_SHARD_DIR='/srv/transparent-pir/sets',
                        TRANSPARENT_PUBLIC_URL='https://transparent-pir.example.net',
                        TRANSPARENT_SHARD_SOURCE=str(self.shards))

    def validate(self):
        return subprocess.run(['bash', str(SCRIPT), 'validate'], cwd=ROOT,
                              env=self.env, capture_output=True, text=True, timeout=10)

    def test_complete_set_and_current_unit_are_accepted(self):
        result = self.validate()
        self.assertEqual(result.returncode, 0, result.stderr)
        shutil.copyfile(ROOT / 'tools/transparent/deploy/transparent-shard-server.service', self.unit)
        result = self.validate()
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_missing_shard_directory_is_refused(self):
        (self.shards / 'bbb').rmdir()
        result = self.validate()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('missing 1 of the 2 revisions', result.stderr)

    def test_malformed_units_are_refused(self):
        units = {
            'line continuation': 'ExecStart=/usr/local/bin/x \\' + '\n  --shard-dir TRANSPARENT_SHARD_SET_PATH\n',
            'unsubstituted token': 'ExecStart=/usr/local/bin/x --shard-dir /hardcoded\n',
            'second ExecStart': 'ExecStart=/a --shard-dir TRANSPARENT_SHARD_SET_PATH\nExecStart=/b\n',
        }
        for name, text in units.items():
            with self.subTest(unit=name):
                self.unit.write_text(text)
                result = self.validate()
                self.assertNotEqual(result.returncode, 0, result.stdout)


if __name__ == '__main__':
    unittest.main()
