"""The one-line query node installer refuses hosts below the required hardware."""
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[3]
INSTALLER = ROOT / 'tools/deploy/query-server/install.sh'
GIB = 1024 ** 3
V3_FLAGS = 'fpu sse2 avx avx2 bmi1 bmi2 f16c fma abm movbe xsave pclmulqdq'
ENOUGH = {'cpus': 16, 'memory': 92 * GIB, 'disk': 500 * 10 ** 9}


class Host:
    """A fake host: stubbed uname, nproc and df, and fake /proc and cgroup files."""

    def __init__(self, directory, *, cpus, memory, disk, flags=V3_FLAGS, system='Linux',
                 cgroup_memory=None, cgroup_cpu=None):
        root = Path(directory)
        proc, sys_root, bin_dir = root / 'proc', root / 'sys', root / 'bin'
        (proc / 'self').mkdir(parents=True)
        group = sys_root / 'fs/cgroup/node.slice'
        group.mkdir(parents=True)
        bin_dir.mkdir()
        (proc / 'cpuinfo').write_text(f'processor\t: 0\nflags\t\t: {flags}\n')
        (proc / 'meminfo').write_text(f'MemTotal:       {memory // 1024} kB\n')
        (proc / 'self/cgroup').write_text('0::/node.slice\n')
        (group / 'memory.max').write_text(f'{cgroup_memory}\n' if cgroup_memory else 'max\n')
        (group / 'cpu.max').write_text(f'{cgroup_cpu * 100000} 100000\n' if cgroup_cpu else 'max 100000\n')
        stubs = {'uname': f'[ "$1" = -m ] && echo x86_64 || echo {system}',
                 'nproc': f'echo {cpus}',
                 'df': f'echo "Filesystem 1-blocks Used Available Capacity Mounted"; echo "/dev/vda1 {disk}B 0 0 0% /"'}
        for name, body in stubs.items():
            path = bin_dir / name
            path.write_text('#!/bin/sh\n' + body + '\n')
            path.chmod(0o755)
        self.env = {**os.environ, 'PATH': f'{bin_dir}:{os.environ["PATH"]}',
                    'WALLET_PIR_INSTALL_PROC': str(proc), 'WALLET_PIR_INSTALL_SYS': str(sys_root)}

    def install(self, *args):
        return subprocess.run(['bash', str(INSTALLER), *args], env=self.env, text=True,
                              capture_output=True, timeout=30)


class QueryInstallerTest(unittest.TestCase):
    def check(self, *args, **host):
        with tempfile.TemporaryDirectory() as directory:
            return Host(directory, **{**ENOUGH, **host}).install('--check-only', *args)

    def test_exact_minimum_passes(self):
        result = self.check()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.splitlines()[-1], 'wallet-pir-install: ok check-only cpus_ram_disk=met')

    def test_each_shortfall_refuses_the_host(self):
        for name, value, words in [('cpus', 15, 'found 15 vCPUs'),
                                   ('memory', 92 * GIB - 1024, 'of RAM'),
                                   ('disk', 500 * 10 ** 9 - 1, 'need at least 500 GB')]:
            with self.subTest(name):
                result = self.check(**{name: value})
                self.assertEqual(result.returncode, 3)
                self.assertIn(words, result.stderr)
                self.assertTrue(result.stdout.startswith('wallet-pir-install: error hardware:'))

    def test_every_shortfall_is_reported_together(self):
        result = self.check(cpus=8, memory=64 * GIB, disk=200 * 10 ** 9)
        self.assertEqual(result.returncode, 3)
        self.assertEqual(result.stderr.count('requirement not met'), 3)
        self.assertIn('3 hardware requirement(s) not met', result.stdout)

    def test_container_limits_count_instead_of_host_totals(self):
        result = self.check(cpus=32, memory=128 * GIB, cgroup_memory=64 * GIB, cgroup_cpu=8)
        self.assertEqual(result.returncode, 3)
        self.assertIn('found 8 vCPUs', result.stderr)
        self.assertIn('found 64.0 GiB of RAM', result.stderr)

    def test_unsupported_platforms_are_refused(self):
        for host, words in [({'flags': V3_FLAGS.replace(' pclmulqdq', '')}, 'pclmulqdq'),
                            ({'flags': 'fpu sse2 avx'}, 'avx2'),
                            ({'system': 'Darwin'}, 'Linux only')]:
            with self.subTest(words):
                result = self.check(**host)
                self.assertEqual(result.returncode, 4)
                self.assertIn(words, result.stdout)

    def test_unknown_options_are_usage_errors(self):
        result = self.check('--profile', 'small')
        self.assertEqual(result.returncode, 2)
        self.assertIn('unknown option --profile', result.stdout)

    @unittest.skipUnless(shutil.which('shellcheck'), 'shellcheck is not installed')
    def test_shellcheck(self):
        subprocess.run(['shellcheck', str(INSTALLER)], check=True)


if __name__ == '__main__':
    unittest.main()
