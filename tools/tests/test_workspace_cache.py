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
        (ROOT / '.local-pir').mkdir(exist_ok=True)
        self.tmp = tempfile.TemporaryDirectory(prefix='workspace-cache-', dir=ROOT / '.local-pir')
        self.area = Path(self.tmp.name)
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


if __name__ == '__main__':
    unittest.main()
