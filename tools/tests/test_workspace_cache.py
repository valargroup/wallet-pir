"""Real Cargo checkout/cache oracle: actual fresh flags and executable behavior."""
from contextlib import redirect_stdout
import argparse
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'tools/ci'))
import workspace_cache
import cargo_cache


class WorkspaceCacheOracle(unittest.TestCase):
    def setUp(self):
        # prepare() appends to GitHub's real environment/output files as well
        # as changing os.environ. Keep fixture lifecycle effects in the fixture.
        env = {key: value for key, value in os.environ.items()
               if key not in {'GITHUB_ENV', 'GITHUB_OUTPUT', 'GITHUB_STEP_SUMMARY',
                              'WALLET_PIR_STAGE_LOG', 'WALLET_PIR_ARTIFACT_LOG',
                              'WALLET_PIR_CASE_LOG', 'WALLET_PIR_STAGE_PARENT'}}
        isolation = patch.dict(os.environ, env, clear=True)
        isolation.start()
        self.addCleanup(isolation.stop)
        (ROOT / '.local-pir').mkdir(exist_ok=True)
        self.tmp = tempfile.TemporaryDirectory(prefix='workspace-cache-', dir=ROOT / '.local-pir')
        # The local output directory may itself be a symlink to a retained cache.
        # Give the oracle real paths; alias rejection is tested separately.
        self.area = Path(self.tmp.name).resolve()
        self.root = self.area / 'checkout'
        self.root.mkdir()
        (self.root / 'src').mkdir()
        (self.root / 'Cargo.toml').write_text('[package]\nname="cache-oracle"\nversion="0.1.0"\nedition="2021"\n'
                                            '[workspace]\n[profile.release-fast]\ninherits="release"\nopt-level=0\n')
        (self.root / 'src/main.rs').write_text('fn main() { println!("rust-A|{}|{}", '
                                              'include_str!("included.txt"), env!("BUILT")); }\n')
        (self.root / 'src/included.txt').write_text('include-A')
        (self.root / 'build.txt').write_text('build-A')
        (self.root / 'build.rs').write_text('fn main() { println!("cargo::rerun-if-changed=build.txt"); '
                                           'println!("cargo::rustc-env=BUILT={}", '
                                           'std::fs::read_to_string("build.txt").unwrap()); }\n')
        shutil.copyfile(ROOT / 'rust-toolchain.toml', self.root / 'rust-toolchain.toml')
        self.git('init', '-q')
        self.git('add', '.')
        self.git('-c', 'user.name=Oracle', '-c', 'user.email=oracle@example.invalid', 'commit', '-qm', 'fixture')
        self.target = self.area / 'target'
        self.pending = self.area / 'pending.json'
        self.env = dict(os.environ, CARGO_TARGET_DIR=str(self.target), CARGO_INCREMENTAL='0')
        self.sequence = 0

    def tearDown(self):
        self.tmp.cleanup()

    def git(self, *args):
        return subprocess.check_output(['git', *args], cwd=self.root, stderr=subprocess.PIPE)

    def build(self, failure=None):
        toolchain = __import__('tomllib').loads((ROOT / 'rust-toolchain.toml').read_text())['toolchain']['channel']
        command = ['rustup', 'run', toolchain, 'cargo'] if shutil.which('rustup') else ['cargo']
        result = subprocess.run([*command, 'build', '--profile', 'release-fast', '--offline', '--message-format=json'],
                                cwd=self.root, env=self.env, capture_output=True, text=True, timeout=25)
        if failure:
            self.assertNotEqual(result.returncode, 0)
            self.assertIn(failure, result.stderr)
            return
        self.assertEqual(result.returncode, 0, result.stderr)
        artifacts = [json.loads(line) for line in result.stdout.splitlines() if line.startswith('{')]
        artifacts = [entry for entry in artifacts if entry.get('reason') == 'compiler-artifact']
        executable = next(entry['executable'] for entry in artifacts if entry.get('executable'))
        behavior = subprocess.check_output([executable], text=True, timeout=2).strip()
        return artifacts, behavior

    def checkout(self):
        # A real fresh Git checkout at the same path used by hosted Actions.
        self.sequence += 1
        old = self.area / f'checkout-{self.sequence}'
        self.root.rename(old)
        subprocess.run(['git', 'clone', '-q', str(old), str(self.root)], check=True, timeout=5)

    def restore(self, identity='compatible'):
        workspace_cache.restore(self.root, self.target, identity, self.pending)

    def seal(self):
        workspace_cache.seal(self.root, self.target, 'compatible', self.pending)

    def baseline(self):
        self.restore()
        artifacts, behavior = self.build()
        self.assertTrue(any(not entry['fresh'] for entry in artifacts))
        self.assertEqual(behavior, 'rust-A|include-A|build-A')
        self.seal()

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

    def archive_roundtrip(self):
        self.sequence += 1
        archive = self.area / f'cache-{self.sequence}.tar'
        subprocess.run(['tar', '--posix', '-cf', str(archive), '-C', str(self.area), 'target'], check=True)
        self.target.rename(self.area / f'original-target-{self.sequence}')
        subprocess.run(['tar', '-xf', str(archive), '-C', str(self.area)], check=True)

    def commit(self, message):
        self.git('add', '.')
        self.git('-c', 'user.name=Oracle', '-c', 'user.email=oracle@example.invalid', 'commit', '-qm', message)

    def symlink_input(self, name, alternate, before, after, expected):
        path = self.root / name
        destination = path.with_name(alternate)
        destination.write_text(path.read_text().replace(before, after))
        self.commit('tracked alternate before baseline')
        self.baseline()
        self.archive_roundtrip()
        path.unlink()
        path.symlink_to(alternate)
        self.commit('replace regular input with symlink')
        self.checkout()
        self.restore()
        artifacts, behavior = self.build()
        self.assertEqual(behavior, expected)
        self.assertTrue(artifacts and all(not entry['fresh'] for entry in artifacts), artifacts)

    def test_symlink_main_rebuilds_after_clone_and_tar(self):
        self.symlink_input('src/main.rs', 'alternate.rs', 'rust-A', 'rust-LINK',
                           'rust-LINK|include-A|build-A')

    def test_symlink_include_rebuilds_after_clone_and_tar(self):
        self.symlink_input('src/included.txt', 'alternate.txt', 'include-A', 'include-LINK',
                           'rust-A|include-LINK|build-A')

    def test_symlink_build_input_rebuilds_after_clone_and_tar(self):
        self.symlink_input('build.txt', 'alternate.txt', 'build-A', 'build-LINK',
                           'rust-A|include-A|build-LINK')

    def untracked_alias(self, name, alternate, replacement, expected):
        path = self.root / name
        destination = path.with_name(alternate)
        destination.write_text(replacement)
        self.git('rm', '--cached', name)
        (self.root / '.gitignore').write_text('/' + name + '\n')
        self.commit('untracked input and tracked alternate')
        self.baseline()
        self.archive_roundtrip()
        archive = self.area / f'cache-{self.sequence}.tar'
        self.checkout()
        path.symlink_to(alternate)
        # Independent Cargo control: with normal checkout mtimes, following the
        # new alias changes behavior. Then restore the SAME baseline archive.
        artifacts, behavior = self.build()
        self.assertEqual(behavior, expected)
        self.assertTrue(any(not entry['fresh'] for entry in artifacts), artifacts)
        self.target.rename(self.area / 'control-target')
        subprocess.run(['tar', '-xf', str(archive), '-C', str(self.area)], check=True)
        self.restore()
        artifacts, behavior = self.build()
        self.assertEqual(behavior, expected)
        self.assertTrue(artifacts and all(not entry['fresh'] for entry in artifacts), artifacts)

    def test_untracked_include_alias_rebuilds_after_clone_and_tar(self):
        self.untracked_alias('src/included.txt', 'alternate.txt', 'include-LINK',
                             'rust-A|include-LINK|build-A')

    def test_untracked_build_alias_rebuilds_after_clone_and_tar(self):
        self.untracked_alias('build.txt', 'alternate.txt', 'build-LINK',
                             'rust-A|include-A|build-LINK')

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

    def test_executable_mode_loss_reruns_generator_and_fails_after_clone_and_tar(self):
        generator = self.root / 'generator.sh'
        generator.write_text('#!/bin/sh\nprintf build-A\n')
        generator.chmod(0o755)
        (self.root / 'build.rs').write_text(
            'fn main() { println!("cargo::rerun-if-changed=generator.sh"); '
            'let out = std::process::Command::new("./generator.sh").output().unwrap(); '
            'assert!(out.status.success()); '
            'println!("cargo::rustc-env=BUILT={}", String::from_utf8(out.stdout).unwrap()); }')
        self.commit('executable generator')
        self.baseline()
        self.archive_roundtrip()
        generator.chmod(0o644)
        self.commit('generator loses executable permission')
        self.checkout()
        self.restore()
        self.build(failure='Permission denied')

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

    def test_source_include_and_build_inputs_recompile_and_change_behavior(self):
        self.baseline()
        changes = [('src/main.rs', 'rust-A', 'rust-B', 'rust-B|include-A|build-A'),
                   ('src/included.txt', 'include-A', 'include-B', 'rust-B|include-B|build-A'),
                   ('build.txt', 'build-A', 'build-B', 'rust-B|include-B|build-B'),
                   ('build.rs', 'BUILT={}', 'BUILT=script-{}', 'rust-B|include-B|script-build-B')]
        for name, before, after, expected in changes:
            with self.subTest(name=name):
                path = self.root / name
                path.write_text(path.read_text().replace(before, after))
                # Deliberately old timestamps must not hide changed bytes.
                os.utime(path, ns=(1, 1))
                self.restore()
                artifacts, behavior = self.build()
                self.assertTrue(any(not entry['fresh'] for entry in artifacts))
                self.assertEqual(behavior, expected)
                self.seal()
                self.git('add', '.')
                self.git('-c', 'user.name=Oracle', '-c', 'user.email=oracle@example.invalid', 'commit', '-qm', name)
                self.checkout()
                self.restore()
                artifacts, behavior = self.build()
                self.assertTrue(all(entry['fresh'] for entry in artifacts), artifacts)
                self.assertEqual(behavior, expected)

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
            result = subprocess.run(
                [sys.executable, '-m', 'unittest',
                 'test_workspace_cache.WorkspaceCacheOracle.test_managed_external_target_still_prepares_restores_and_builds'],
                cwd=ROOT / 'tools/tests', env=env, capture_output=True, text=True, timeout=60)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            for name in names:
                self.assertEqual((root / name).read_text(), 'outer job\n', name)


if __name__ == '__main__':
    unittest.main()
