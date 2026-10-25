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

    def test_primer_reuses_only_a_recent_main_snapshot_for_the_lockfile(self):
        prefix = 'v2-wallet-pir-workspace-compat-lock-'
        caches = [
            {'ref': 'refs/heads/main', 'key': prefix + 'old', 'created_at': '2026-10-23T00:00:00Z'},
            {'ref': 'refs/heads/main', 'key': prefix + 'new', 'created_at': '2026-10-24T10:00:00Z'},
            {'ref': 'refs/pull/1/merge', 'key': prefix + 'pr', 'created_at': '2026-10-24T11:00:00Z'},
            {'ref': 'refs/heads/main', 'key': 'v2-wallet-pir-workspace-compat-other-x', 'created_at': '2026-10-24T11:00:00Z'},
        ]
        now = cargo_cache.timestamp('2026-10-24T12:00:00Z')
        env = {'GITHUB_REPOSITORY': 'owner/repo'}
        original = cargo_cache.api
        cargo_cache.api = lambda path, env: {'actions_caches': caches}
        try:
            self.assertEqual(cargo_cache.fresh_main_snapshot(prefix, env, now=now), prefix + 'new')
            # Evicted or older than the window: the primer compiles and saves again.
            self.assertIsNone(cargo_cache.fresh_main_snapshot(prefix, env, max_age=3600, now=now))
            caches[:] = []
            self.assertIsNone(cargo_cache.fresh_main_snapshot(prefix, env, now=now))
        finally:
            cargo_cache.api = original
