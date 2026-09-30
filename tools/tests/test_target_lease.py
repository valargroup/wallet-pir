"""Kernel ownership, reuse and subprocess-lifetime regressions for local lanes."""
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'shared/dev'))
import target_lease as lease


class TargetLeaseTests(unittest.TestCase):
    def test_busy_lane_is_skipped_and_released_lane_is_reused(self):
        with tempfile.TemporaryDirectory() as tmp:
            first, fd1 = lease.acquire(tmp)
            second, fd2 = lease.acquire(tmp)
            self.assertNotEqual(first, second)
            os.close(fd1)
            reused, fd3 = lease.acquire(tmp)
            self.assertEqual(reused, first)
            os.close(fd2)
            os.close(fd3)

    def test_alias_paths_cannot_acquire_the_same_lane(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            alias = root / 'alias'
            alias.symlink_to(root / 'pool', target_is_directory=True)
            first, fd1 = lease.acquire(root / 'pool')
            second, fd2 = lease.acquire(alias)
            self.assertNotEqual(first, second)
            os.close(fd1)
            os.close(fd2)

    def test_exception_restores_environment_and_releases_lane(self):
        with tempfile.TemporaryDirectory() as tmp, \
             patch.dict(os.environ, {'CARGO_TARGET_DIR': tmp, 'GITHUB_ACTIONS': 'false'}), \
             patch.object(lease, 'compatibility', return_value='fixture'):
            with self.assertRaisesRegex(RuntimeError, 'failed'):
                with lease.local_target(ROOT):
                    target = Path(os.environ['CARGO_TARGET_DIR'])
                    result = subprocess.run([sys.executable, '-c', 'raise SystemExit(2)'],
                                            **lease.inherited_fds())
                    self.assertEqual(result.returncode, 2)
                    raise RuntimeError('failed')
            self.assertEqual(os.environ['CARGO_TARGET_DIR'], tmp)
            reused, fd = lease.acquire(target.parent)
            self.assertEqual(reused, target)
            os.close(fd)

    def test_ci_and_helper_only_checks_do_not_allocate_or_require_rust(self):
        with patch.object(lease, 'compatibility', side_effect=AssertionError('compiler')):
            with lease.local_target(ROOT, enabled=False):
                pass
            with patch.dict(os.environ, {'GITHUB_ACTIONS': 'true'}):
                with lease.local_target(ROOT):
                    pass

    def test_flags_partition_pools_but_target_root_does_not(self):
        with tempfile.TemporaryDirectory() as tmp, \
             patch.object(lease.subprocess, 'check_output', return_value=b'rustc fixture'):
            root = Path(tmp)
            env = {'CARGO_HOME': str(root / 'home')}
            base = lease.compatibility(root, env)
            self.assertEqual(base, lease.compatibility(root, dict(env, CARGO_TARGET_DIR='elsewhere')))
            self.assertNotEqual(base, lease.compatibility(root, dict(env, RUSTFLAGS='-C target-cpu=native')))
            (root / '.cargo').mkdir()
            (root / '.cargo/config.toml').write_text('[build]\njobs=2\n')
            self.assertNotEqual(base, lease.compatibility(root, env))

    def test_stale_inherited_descriptor_fails_closed(self):
        with patch.dict(os.environ, {lease.LEASE_FD: '999999'}):
            with self.assertRaises(OSError):
                lease.inherited_fds()

    def test_sigkill_wrapper_keeps_lease_through_shell_and_grandchild(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            ready, release = root / 'ready', root / 'release'
            child_code = (
                'import pathlib,time; '
                f'pathlib.Path({str(ready)!r}).touch(); '
                f'\nwhile not pathlib.Path({str(release)!r}).exists(): time.sleep(.01)'
            )
            # stage.run -> shell -> Python -> grandchild; the intermediate Python
            # exits immediately. SIGKILL the wrapper while the last child is live.
            child = root / 'child.py'
            child.write_text('import subprocess,sys,os\n'
                             f'subprocess.Popen([sys.executable,"-c",{child_code!r}], '
                             f'pass_fds=(int(os.environ[{lease.LEASE_FD!r}]),))\n')
            wrapper = root / 'wrapper.py'
            wrapper.write_text(
                f'import sys,os,time; sys.path.insert(0,{str(ROOT / "tools/ci")!r})\n'
                'from stage import run\nfrom target_lease import acquire,LEASE_FD\n'
                f'target,fd=acquire({str(root / "pool")!r})\n'
                'os.environ[LEASE_FD]=str(fd)\n'
                f'run(["bash","-c",\'exec "$1" "$2"\',"bash",sys.executable,{str(child)!r}])\n'
                'time.sleep(30)\n')
            proc = subprocess.Popen([sys.executable, str(wrapper)], stdout=subprocess.DEVNULL)
            try:
                deadline = time.monotonic() + 5
                while not ready.exists() and time.monotonic() < deadline:
                    time.sleep(.01)
                self.assertTrue(ready.exists(), 'child never inherited lease')
                proc.kill()
                proc.wait(timeout=5)
                target, fd = lease.acquire(root / 'pool')
                self.assertEqual(target.name, 'lane-1')
                os.close(fd)
                release.touch()
                deadline = time.monotonic() + 5
                while time.monotonic() < deadline:
                    target, fd = lease.acquire(root / 'pool')
                    os.close(fd)
                    if target.name == 'lane-0':
                        break
                    time.sleep(.01)
                self.assertEqual(target.name, 'lane-0')
            finally:
                release.touch()
                if proc.poll() is None:
                    proc.kill()
                proc.wait(timeout=5)


if __name__ == '__main__':
    unittest.main()
