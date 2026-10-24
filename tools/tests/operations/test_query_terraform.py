"""Exercise the locked Terraform entrypoint without cloud or systemd access."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[3]


@unittest.skipUnless(shutil.which('flock') and shutil.which('jq'), 'requires Linux deployment tools')
class QueryTerraform(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.live = self.root / 'live'
        self.live.mkdir()
        self.staged = self.root / 'staged'
        self.staged.mkdir()
        (self.staged / 'backend.tf').write_text('terraform {}\n')
        self.plan = self.root / 'reviewed.tfplan'
        self.plan.write_bytes(b'saved-plan-placeholder')
        self.state = self.root / 'autoscale.json'
        self.state.write_text('{"desired_groups":2}')
        runtime = self.root / 'runtime.py'
        runtime.write_text('import os,sys\n'
                           'os.environ.update({k:"test-placeholder" for k in '
                           '["DO_TOKEN_NEW_ORG","CF_API_TOKEN","WALLET_PIR_TF_STATE_ACCESS_KEY",'
                           '"WALLET_PIR_TF_STATE_SECRET_KEY"]})\n'
                           'os.execvp(sys.argv[1],sys.argv[1:])\n')
        self.write_program('systemd-run', '#!/bin/sh\nwhile [ "$1" != /usr/bin/python3 ]; do shift; done\nexec "$@"\n')
        self.write_program('terraform', '#!/usr/bin/env python3\nimport json,sys\nprint(json.dumps(sys.argv[1:]))\n')
        script = (ROOT / 'tools/scripts/wallet-pir-terraform.sh').read_text()
        for original, replacement in {
            '/opt/enhance-pir/infra/production': self.live,
            '/opt/enhance-pir/ops/wallet-pir-runtime.py': runtime,
            '/run/lock/wallet-pir-production.lock': self.root / 'production.lock',
            '/srv/enhance-pir/autoscale/state.json': self.state,
            '/etc/enhance-pir/production.tfvars': self.root / 'production.tfvars',
        }.items():
            script = script.replace(original, str(replacement))
        self.script = self.root / 'terraform-wrapper.sh'
        self.script.write_text(script)

    def write_program(self, name, text):
        p = self.root / name
        p.write_text(text)
        p.chmod(0o755)

    def run_wrapper(self, *args):
        return subprocess.run(['bash', str(self.script), *map(str, args)],
                              env={**os.environ, 'PATH': str(self.root) + ':' + os.environ['PATH']},
                              capture_output=True, text=True, timeout=5)

    def test_staged_plan_keeps_autoscale_count_and_production_variables(self):
        result = self.run_wrapper('--root', self.staged, 'plan', '-out=' + str(self.plan))
        self.assertEqual(result.returncode, 0, result.stderr)
        args = json.loads(result.stdout)
        self.assertEqual(args[:2], ['-chdir=' + str(self.staged), 'plan'])
        self.assertIn('-var=enhance_group_count=2', args)
        self.assertIn('-var-file=' + str(self.root / 'production.tfvars'), args)

    def test_apply_accepts_only_one_existing_saved_plan(self):
        result = self.run_wrapper('--root', self.staged, 'apply', self.plan)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout),
                         ['-chdir=' + str(self.staged), 'apply', '-input=false', str(self.plan)])
        for arguments in [('apply',), ('apply', '-auto-approve'), ('apply', self.plan, '-auto-approve')]:
            with self.subTest(arguments=arguments):
                result = self.run_wrapper('--root', self.staged, *arguments)
                self.assertNotEqual(result.returncode, 0)
                self.assertFalse(result.stdout)

    def test_bad_roots_and_concurrent_writer_are_refused(self):
        for root in ('relative', self.root / 'missing'):
            self.assertNotEqual(self.run_wrapper('--root', root, 'plan').returncode, 0)
        import fcntl
        with (self.root / 'production.lock').open('w') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            result = self.run_wrapper('--root', self.staged, 'plan')
            self.assertNotEqual(result.returncode, 0)
            self.assertFalse(result.stdout)
