"""The query-host rustc wrapper caps codegen threads for that crate only."""
import os
from pathlib import Path
import stat
import subprocess
import sys
import tempfile
import textwrap
import unittest

ROOT = Path(__file__).resolve().parents[2]
WRAPPER = ROOT / 'tools/ci/rustc_threads.py'


class RustcThreadWrapper(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.compiler = Path(self.directory.name) / 'rustc'
        self.compiler.write_text(textwrap.dedent('''\
            #!/usr/bin/env python3
            import pathlib, sys
            pathlib.Path(sys.argv[1]).write_text('\\n'.join(sys.argv[2:]))
        '''))
        self.compiler.chmod(self.compiler.stat().st_mode | stat.S_IEXEC)

    def compile(self, args, threads=None):
        recorded = Path(self.directory.name) / 'args'
        env = os.environ.copy()
        env.pop('WALLET_PIR_QUERY_RUSTC_THREADS', None)
        if threads is not None:
            env['WALLET_PIR_QUERY_RUSTC_THREADS'] = threads
        subprocess.run([sys.executable, str(WRAPPER), str(self.compiler), str(recorded), *args],
                       check=True, env=env)
        return recorded.read_text().splitlines()

    def test_query_host_gets_four_threads_and_other_crates_do_not(self):
        self.assertEqual(
            self.compile(['--crate-name', 'pir_query_server', '--edition', '2021'])[-2:],
            ['-C', 'threads=4'])
        self.assertNotIn('threads=4', self.compile(['--crate-name', 'enhance_shared']))
        self.assertNotIn('threads=4', self.compile(['-vV']))

    def test_thread_count_comes_from_the_environment(self):
        self.assertEqual(self.compile(['--crate-name', 'pir_query_server'], threads='2')[-2:],
                         ['-C', 'threads=2'])
        refused = subprocess.run(
            [sys.executable, str(WRAPPER), str(self.compiler), 'unused',
             '--crate-name', 'pir_query_server'],
            env={**os.environ, 'WALLET_PIR_QUERY_RUSTC_THREADS': '0'},
            capture_output=True, text=True, check=False)
        self.assertNotEqual(refused.returncode, 0)
        self.assertIn('positive integer', refused.stderr)
