"""Fresh checkouts and independent Cargo targets for the cache oracles."""
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


class WorkspaceCacheFixture(unittest.TestCase):
    """Give every case its own checkout, target, receipt and environment."""

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

    def build(self, failure=None, selection=()):
        toolchain = __import__('tomllib').loads((ROOT / 'rust-toolchain.toml').read_text())['toolchain']['channel']
        command = ['rustup', 'run', toolchain, 'cargo'] if shutil.which('rustup') else ['cargo']
        result = subprocess.run([*command, 'build', '--profile', 'release-fast', '--offline', '--message-format=json', *selection],
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

