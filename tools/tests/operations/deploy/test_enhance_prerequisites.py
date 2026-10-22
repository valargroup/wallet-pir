"""Exercise cache preparation and access checks against real OS permissions."""
import grp
import json
import os
from pathlib import Path
import pwd
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest

ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / 'tools/lib'))
from tools.enhance.lib.deploy_prerequisites import Guard, PROGRAM
from wallet_pir_ops.role_config import RenderedRole


class Prerequisites(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name).resolve()
        self.user = pwd.getpwuid(os.geteuid()).pw_name
        self.group = grp.getgrgid(os.getegid()).gr_name
        self.cache = self.root / 'parent' / 'cache'

    def tearDown(self):
        self.temporary.cleanup()

    def run_helper(self, **overrides):
        value = {'user': self.user, 'group': self.group, 'directories': [str(self.cache)],
                 'credentials': [], 'prepare': False}
        value.update(overrides)
        return subprocess.run([sys.executable, '-c', PROGRAM, json.dumps(value)],
                              capture_output=True, text=True, timeout=10)

    def test_plan_is_read_only_and_activation_creates_service_owned_cache(self):
        self.assertEqual(self.run_helper().returncode, 0)
        self.assertFalse(self.cache.parent.exists())
        result = self.run_helper(prepare=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual((self.cache.stat().st_uid, self.cache.stat().st_gid), (os.geteuid(), os.getegid()))
        self.assertEqual(self.cache.stat().st_mode & 0o777, 0o750)
        marker = self.cache / 'retained'
        marker.write_text('retained')
        before = (self.cache.stat().st_ino, self.cache.stat().st_mode, marker.stat().st_ino)
        self.assertEqual(self.run_helper(prepare=True).returncode, 0)
        self.assertEqual(before, (self.cache.stat().st_ino, self.cache.stat().st_mode, marker.stat().st_ino))

    def test_missing_account_or_group_refuses_without_creating_directories(self):
        for override in ({'user': 'wallet-pir-nonexistent-test-user'}, {'group': 'wallet-pir-nonexistent-test-group'}):
            with self.subTest(override=override):
                self.assertNotEqual(self.run_helper(**override).returncode, 0)
                self.assertFalse(self.cache.parent.exists())

    def test_missing_required_credentials_refuse_but_optional_files_can_be_absent(self):
        missing = str(self.root / 'missing.env')
        self.assertNotEqual(self.run_helper(credentials=[missing]).returncode, 0)
        self.assertEqual(self.run_helper(credentials=['-' + missing]).returncode, 0)
        self.assertFalse(self.cache.parent.exists())

    def test_inaccessible_parent_is_checked_as_the_service_account(self):
        self.cache.parent.mkdir(mode=0o700)
        service = 'nobody' if os.geteuid() == 0 else self.user
        if os.geteuid() != 0:
            self.cache.parent.chmod(0)
        try:
            result = self.run_helper(user=service, group='', prepare=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertRegex(result.stderr, 'service account cannot access|Permission denied')
        finally:
            self.cache.parent.chmod(0o700)
        self.assertFalse(self.cache.exists())

    def test_existing_cache_with_another_owner_is_never_chowned(self):
        self.cache.mkdir(parents=True)
        other = 'nobody' if pwd.getpwnam('nobody').pw_uid != os.geteuid() else 'root'
        before = self.cache.stat()
        result = self.run_helper(user=other, group='', prepare=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('foreign owner', result.stderr)
        after = self.cache.stat()
        self.assertEqual((before.st_uid, before.st_gid, before.st_mode), (after.st_uid, after.st_gid, after.st_mode))

    def test_symlink_cache_is_refused_without_mutating_its_target(self):
        target = self.root / 'retained'
        target.mkdir()
        self.cache.parent.mkdir()
        self.cache.symlink_to(target)
        self.assertNotEqual(self.run_helper(prepare=True).returncode, 0)
        self.assertEqual(list(target.iterdir()), [])

    def test_explicit_inventory_identity_and_credentials_override_installed_defaults(self):
        target = SimpleNamespace(settings={'limits': {'User': 'new-user', 'Group': 'new-group'}, 'credentials': []})
        state = {'load_state': 'loaded', 'fragment_path': '/unit', 'drop_ins': [],
                 'fragment_text': '[Service]\nUser=old-user\nGroup=old-group\nEnvironmentFile=/old.env\n'}
        rendered = RenderedRole('[Service]\nUser=new-user\nGroup=new-group\n', {})
        self.assertEqual(Guard().rendered_role(target, state, rendered), rendered)


if __name__ == '__main__':
    unittest.main()
