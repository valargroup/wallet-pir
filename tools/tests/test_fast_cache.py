"""A missing fast cache is an explicit preparation failure, not skipped validation."""
import json
from pathlib import Path
import sys
import tempfile
import unittest
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'tools/ci'))
import cargo_cache


class FastCacheTests(unittest.TestCase):
    def test_empty_cache_fails_and_populated_cache_still_requires_the_test_command(self):
        with tempfile.TemporaryDirectory() as directory:
            env = {'RUNNER_TEMP': directory}
            state = cargo_cache.state_dir(env)
            state.mkdir()
            for before in [{}, {'dependency': 'fingerprint'}]:
                (state / 'fast.json').write_text(json.dumps({'before': before}))
                if before:
                    self.assertIsNone(cargo_cache.require_warm('fast', env))
                else:
                    with self.assertRaisesRegex(ValueError, 'Prepare fast validation'):
                        cargo_cache.require_warm('fast', env)
