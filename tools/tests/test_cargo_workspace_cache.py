"""Real Cargo checkout/cache oracle: actual fresh flags and executable behavior.

Keep this long oracle early in alphabetical discovery so the bounded helper
pool overlaps it with the short tooling suites.
"""
from contextlib import redirect_stdout
import argparse
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'tools/ci'))
import workspace_cache
import cargo_cache

from workspace_cache_fixture import WorkspaceCacheFixture


class WorkspaceCacheOracle(WorkspaceCacheFixture):
    def test_verified_all_workspace_snapshot_does_not_prepare_affected_feature_graph(self):
        # A second workspace root enables an extra feature on a shared leaf.
        # A valid archive/source-mtime hit cannot make its wider Cargo unit
        # interchangeable with the affected root's narrower unit.
        manifest = self.root / 'Cargo.toml'
        manifest.write_text(manifest.read_text().replace(
            '[workspace]\n', '[dependencies]\nfeature-leaf = { path="leaf" }\n'
            '[workspace]\nmembers=["owner"]\n'))
        for name, extra in [('leaf', '[features]\nwide=[]\n'),
                            ('owner', '[dependencies]\nfeature-leaf={path="../leaf",features=["wide"]}\n')]:
            directory = self.root / name
            (directory / 'src').mkdir(parents=True)
            (directory / 'Cargo.toml').write_text(
                f'[package]\nname="feature-{name}"\nversion="0.1.0"\nedition="2021"\n{extra}')
        (self.root / 'leaf/src/lib.rs').write_text(
            'pub fn value() -> bool { cfg!(feature="wide") }\n')
        (self.root / 'owner/src/lib.rs').write_text(
            'pub fn value() -> bool { feature_leaf::value() }\n')
        source = self.root / 'src/main.rs'
        source.write_text(source.read_text().replace('"rust-A|{}|{}",', '"rust-A|{}|{}|{}", feature_leaf::value(),'))
        self.commit('feature-unification fixture')
        self.restore()
        _, behavior = self.build(selection=['--workspace'])
        self.assertEqual(behavior, 'rust-A|true|include-A|build-A')
        self.seal()
        self.archive_roundtrip()
        self.checkout()
        with redirect_stdout(io.StringIO()) as output:
            self.restore()
        self.assertIn('"metadata": "verified"', output.getvalue())
        self.assertIn('"new_source_mtimes": 0', output.getvalue())
        affected = ['-p', 'cache-oracle']
        artifacts, behavior = self.build(selection=affected)
        self.assertEqual(behavior, 'rust-A|false|include-A|build-A')
        self.assertTrue(any(entry['target']['name'] == 'feature_leaf' and not entry['fresh']
                            for entry in artifacts))
        # Seal only after the actual affected graph has also been prepared.
        self.seal()
        self.archive_roundtrip()
        self.checkout()
        self.restore()
        artifacts, behavior = self.build(selection=affected)
        self.assertEqual(behavior, 'rust-A|false|include-A|build-A')
        self.assertTrue(artifacts and all(entry['fresh'] for entry in artifacts))

    def test_action_lifecycle_seals_metadata_and_reports_cargo_separately(self):
        env = {'RUNNER_TEMP': str(self.area / 'runner')}
        directory = cargo_cache.state_dir(env)
        directory.mkdir(parents=True)
        record = {'lane': 'fast', 'scope': 'affected', 'identity': 'full', 'toolchain': 'tools',
                  'profile': 'release-fast', 'compatibility': 'compatible'}
        state = {'record': record, 'mode': 'restore', 'target': str(self.target), 'key': 'snapshot-key',
                 'refs_before_restore': None, 'ref': 'refs/pull/1/merge', 'existed': None,
                 'setup_started': 0, 'prepared': 0}
        (directory / 'fast.json').write_text(json.dumps(state))
        with patch.object(cargo_cache, 'ROOT', self.root), redirect_stdout(io.StringIO()) as output:
            cargo_cache.restored(argparse.Namespace(lane='fast', cache_hit='false'), env)
            self.build()
            cargo_cache.report(env)
        reports = [json.loads(line.split(' ', 1)[1]) for line in output.getvalue().splitlines()
                   if line.startswith('CI_CACHE_REPORT ')]
        self.assertEqual([report['key'] for report in reports], ['snapshot-key'])
        self.assertIn('CI_CARGO_ARTIFACTS ', output.getvalue())
        self.assertTrue((self.target / workspace_cache.METADATA).is_file())
        self.checkout()
        self.restore()
        self.assertTrue(all(entry['fresh'] for entry in self.build()[0]))

    def test_target_ownership_rejected_before_prepare_and_after_tar_extraction(self):
        # A tracked source link requests the cold-miss rename. No rejected
        # output alias may move its destination or normalize checkout sources.
        (self.root / 'src/link').symlink_to('main.rs')
        self.commit('source link')
        foreign = self.area / 'foreign'
        foreign.mkdir()
        sentinel = foreign / 'sentinel'
        sentinel.write_bytes(b'separately owned bytes')
        sentinel.chmod(0o640)
        os.utime(sentinel, ns=(123456789, 123456789))
        original = (sentinel.read_bytes(), sentinel.stat().st_mode, sentinel.stat().st_mtime_ns)
        source_time = (self.root / 'src/main.rs').stat().st_mtime_ns
        env = {'RUNNER_TEMP': str(self.area / 'runner')}
        directory = cargo_cache.state_dir(env)
        directory.mkdir(parents=True)
        for kind in ('target-link', 'parent-link', 'checkout', 'ancestor', 'source-overlap'):
            with self.subTest(kind=kind):
                lane = self.area / ('case-' + kind)
                lane.mkdir()
                target = lane / 'fast'
                if kind == 'target-link':
                    target.symlink_to(foreign, target_is_directory=True)
                elif kind == 'parent-link':
                    (lane / 'parent').symlink_to(foreign, target_is_directory=True)
                    target = lane / 'parent/fast'
                elif kind == 'checkout':
                    target = self.root
                elif kind == 'ancestor':
                    target = self.area
                else:
                    target = self.root / 'src'
                before = sorted(str(p.relative_to(self.area)) for p in self.area.rglob('*'))
                args = argparse.Namespace(lane='fast', scope='affected', target_dir=str(target),
                                          persistent_root=None, lookup_only=False)
                with patch.object(cargo_cache, 'ROOT', self.root):
                    with self.assertRaises(ValueError):
                        cargo_cache.prepare(args, env)
                self.assertEqual(sorted(str(p.relative_to(self.area)) for p in self.area.rglob('*')), before)
                # POSIX tar can introduce an alias AFTER a valid prepare. Use
                # the owning restored lifecycle, never call only the helper.
                if kind in ('target-link', 'parent-link'):
                    archive = lane / 'alias.tar'
                    member = 'fast' if kind == 'target-link' else 'parent'
                    subprocess.run(['tar', '--posix', '-cf', str(archive), '-C', str(lane), member], check=True)
                    (lane / member).rename(lane / 'before-extraction')
                    subprocess.run(['tar', '-xf', str(archive), '-C', str(lane)], check=True)
                state = {'record': {'compatibility': 'compatible'}, 'mode': 'restore', 'target': str(target)}
                (directory / 'fast.json').write_text(json.dumps(state))
                before = sorted(str(p.relative_to(self.area)) for p in self.area.rglob('*'))
                with patch.object(cargo_cache, 'ROOT', self.root):
                    with self.assertRaises(ValueError):
                        cargo_cache.restored(argparse.Namespace(lane='fast', cache_hit='true'), env)
                self.assertEqual(sorted(str(p.relative_to(self.area)) for p in self.area.rglob('*')), before)
                self.assertEqual((sentinel.read_bytes(), sentinel.stat().st_mode, sentinel.stat().st_mtime_ns), original)
                self.assertEqual((self.root / 'src/main.rs').stat().st_mtime_ns, source_time)
                self.assertFalse((directory / 'workspace-pending').exists())

    def test_actual_git_directories_rejected_before_setup_and_on_restored_lifecycle(self):
        (self.root / 'src/link').symlink_to('main.rs')
        self.commit('source link requests cold target move')
        original_root = self.root
        linked = self.area / 'linked'
        self.git('worktree', 'add', '--detach', str(linked), 'HEAD')
        separate = self.area / 'separate'
        subprocess.run(['git', 'clone', '-q', '--separate-git-dir', str(self.area / 'separate-admin'),
                        str(original_root), str(separate)], check=True)
        for checkout in (linked, separate):
            self.root = checkout
            for option in ('--git-dir', '--git-common-dir'):
                with self.subTest(checkout=checkout.name, directory=option):
                    target = Path(self.git('rev-parse', '--path-format=absolute', option).decode().strip())
                    sentinel = target / 'sentinel'
                    sentinel.write_bytes(b'Git administrative directory is separately owned')
                    sentinel.chmod(0o640)
                    os.utime(sentinel, ns=(123456789, 123456789))
                    sentinel_before = (sentinel.read_bytes(), sentinel.stat().st_mode, sentinel.stat().st_mtime_ns)
                    head, inventory = self.git('rev-parse', 'HEAD'), self.git('ls-files', '-z')
                    sources = {name: (os.lstat(checkout / os.fsdecode(name)).st_mtime_ns,
                                      os.readlink(checkout / os.fsdecode(name))
                                      if (checkout / os.fsdecode(name)).is_symlink() else None)
                               for name in inventory.split(b'\0') if name}
                    env = {'RUNNER_TEMP': str(self.area / (checkout.name + option))}
                    directory = cargo_cache.state_dir(env)
                    args = argparse.Namespace(lane='fast', scope='affected', target_dir=str(target),
                                              persistent_root=None, lookup_only=False)
                    before = sorted(str(p.relative_to(self.area)) for p in self.area.rglob('*'))
                    with patch.object(cargo_cache, 'ROOT', checkout), patch.object(cargo_cache, 'probe') as probe:
                        with self.assertRaises(ValueError):
                            cargo_cache.prepare(args, env)
                        probe.assert_not_called()
                    self.assertFalse(directory.exists())
                    self.assertEqual(sorted(str(p.relative_to(self.area)) for p in self.area.rglob('*')), before)
                    # An unsafe receipt from the old prepare must also fail at
                    # the owning restore boundary after POSIX-tar extraction.
                    directory.mkdir(parents=True)
                    state = {'record': {'compatibility': 'compatible'}, 'mode': 'restore', 'target': str(target)}
                    receipt = directory / 'fast.json'
                    receipt.write_text(json.dumps(state))
                    archive = directory / 'restored.tar'
                    subprocess.run(['tar', '--posix', '-cf', str(archive), '-C', str(target), 'sentinel'], check=True)
                    subprocess.run(['tar', '-xf', str(archive), '-C', str(target)], check=True)
                    before = sorted(str(p.relative_to(self.area)) for p in self.area.rglob('*'))
                    receipt_before = receipt.read_bytes()
                    with patch.object(cargo_cache, 'ROOT', checkout):
                        with self.assertRaises(ValueError):
                            cargo_cache.restored(argparse.Namespace(lane='fast', cache_hit='true'), env)
                    self.assertEqual(receipt.read_bytes(), receipt_before)
                    self.assertFalse((directory / 'workspace-pending').exists())
                    self.assertEqual(sorted(str(p.relative_to(self.area)) for p in self.area.rglob('*')), before)
                    self.assertEqual((sentinel.read_bytes(), sentinel.stat().st_mode, sentinel.stat().st_mtime_ns),
                                     sentinel_before)
                    self.assertEqual(self.git('rev-parse', 'HEAD'), head)
                    self.assertEqual(self.git('ls-files', '-z'), inventory)
                    for name, (mtime, link) in sources.items():
                        path = checkout / os.fsdecode(name)
                        self.assertEqual(os.lstat(path).st_mtime_ns, mtime)
                        if link is not None:
                            self.assertEqual(os.readlink(path), link)
        self.root = original_root

    def test_managed_external_target_still_prepares_restores_and_builds(self):
        self.target = self.area / 'managed-external' / 'fast'
        self.env['CARGO_TARGET_DIR'] = str(self.target)
        env = dict(os.environ, RUNNER_TEMP=str(self.area / 'runner-external'))
        args = argparse.Namespace(lane='fast', scope='affected', target_dir=str(self.target),
                                  persistent_root=None, lookup_only=False)
        with patch.dict(os.environ), patch.object(cargo_cache, 'ROOT', self.root), redirect_stdout(io.StringIO()):
            cargo_cache.prepare(args, env)
            cargo_cache.restored(argparse.Namespace(lane='fast', cache_hit='false'), env)
        self.assertEqual(self.build()[1], 'rust-A|include-A|build-A')

    def test_fresh_checkout_reuses_real_archived_outputs(self):
        self.baseline()
        # GNU tar's POSIX format is also used by actions/cache. Exercise its
        # timestamp fidelity, including Cargo fingerprint nanoseconds.
        subprocess.run(['tar', '--posix', '-cf', str(self.area / 'cache.tar'), '-C', str(self.area), 'target'], check=True)
        self.target.rename(self.area / 'original-target')
        subprocess.run(['tar', '-xf', str(self.area / 'cache.tar'), '-C', str(self.area)], check=True)
        self.checkout()
        self.restore()
        artifacts, behavior = self.build()
        self.assertTrue(artifacts and all(entry['fresh'] for entry in artifacts), artifacts)
        self.assertEqual(behavior, 'rust-A|include-A|build-A')

    def test_without_timestamp_restore_identical_checkout_recompiles(self):
        self.baseline()
        self.checkout()
        artifacts, behavior = self.build()
        self.assertTrue(any(not entry['fresh'] for entry in artifacts))
        self.assertEqual(behavior, 'rust-A|include-A|build-A')

    def test_legacy_metadata_without_modes_is_a_safe_miss(self):
        self.baseline()
        metadata = self.target / workspace_cache.METADATA
        envelope = json.loads(metadata.read_text())
        envelope['record']['schema'] = 1
        for entry in envelope['record']['files'].values():
            entry.pop()
        envelope['checksum'] = workspace_cache.digest(workspace_cache.encoded(envelope['record']))
        metadata.write_text(json.dumps(envelope))
        self.checkout()
        for name in ('src/main.rs', 'src/included.txt', 'build.rs', 'build.txt'):
            os.utime(self.root / name, ns=(1, 1))
        self.restore()
        artifacts, behavior = self.build()
        self.assertTrue(any(not entry['fresh'] for entry in artifacts))
        self.assertEqual(behavior, 'rust-A|include-A|build-A')

    def test_missing_corrupt_wrong_identity_and_stale_metadata_are_safe_misses(self):
        self.baseline()
        metadata = self.target / workspace_cache.METADATA
        original = metadata.read_bytes()
        for value, identity in [(b'', 'compatible'), (b'{broken', 'compatible'),
                                (original.replace(b'checksum', b'invalid!'), 'compatible'),
                                (original, 'other-compiler')]:
            with self.subTest(identity=identity, data=value[:15]):
                if value:
                    metadata.write_bytes(value)
                else:
                    metadata.unlink()
                for name in ('src/main.rs', 'src/included.txt', 'build.rs', 'build.txt'):
                    os.utime(self.root / name, ns=(1, 1))
                self.restore(identity)
                artifacts, behavior = self.build()
                self.assertTrue(any(not entry['fresh'] for entry in artifacts))
                self.assertEqual(behavior, 'rust-A|include-A|build-A')
        # A well-formed old record cannot bless outputs compiled later from
        # different source bytes, even when their Cargo fingerprint bytes match.
        path = self.root / 'src/main.rs'
        path.write_text(path.read_text().replace('rust-A', 'rust-B'))
        self.assertEqual(self.build()[1], 'rust-B|include-A|build-A')
        metadata.write_bytes(original)
        self.checkout()
        self.restore()
        artifacts, behavior = self.build()
        self.assertTrue(any(not entry['fresh'] for entry in artifacts))
        self.assertEqual(behavior, 'rust-A|include-A|build-A')



class WorkspaceCacheOutputIsolationTests(unittest.TestCase):
    def test_fixture_cannot_change_outer_github_environment_or_timing_files(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            names = ['GITHUB_ENV', 'GITHUB_OUTPUT', 'GITHUB_STEP_SUMMARY',
                     'WALLET_PIR_STAGE_LOG', 'WALLET_PIR_ARTIFACT_LOG', 'WALLET_PIR_CASE_LOG']
            env = os.environ.copy()
            for name in names:
                path = root / name
                path.write_text('outer job\n')
                env[name] = str(path)
            # This stdlib-only child must not install inherited sitecustomize
            # timing hooks against the sentinel paths. The outer case still
            # times all of its real Cargo work, including setup and teardown.
            result = subprocess.run(
                [sys.executable, '-S', '-m', 'unittest',
                 'test_cargo_workspace_cache.WorkspaceCacheOracle.test_managed_external_target_still_prepares_restores_and_builds'],
                cwd=ROOT / 'tools/tests', env=env, capture_output=True, text=True, timeout=60)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            for name in names:
                self.assertEqual((root / name).read_text(), 'outer job\n', name)



if __name__ == '__main__':
    unittest.main()
