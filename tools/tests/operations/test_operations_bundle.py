"""Operations releases must work without Git or the original checkout."""
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile
import unittest
import shutil
import uuid

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / 'tools/lib'))
from wallet_pir_ops import bundle  # noqa: E402
from wallet_pir_ops.deploy import descriptors  # noqa: E402


class OperationsBundleTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory()
        cls.root = Path(cls.temp.name)
        cls.sha = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip()
        cls.archive = cls.root / 'operations.tar.gz'
        cls.manifest = bundle.assemble(cls.archive, cls.sha)

    @classmethod
    def tearDownClass(cls):
        cls.temp.cleanup()

    def rewrite(self, *, omit=None, change=None, append=None, manifest_change=None):
        with tarfile.open(self.archive) as source:
            payload = {m.name: source.extractfile(m).read() for m in source}
        if omit:
            payload.pop(omit)
        if change:
            payload[change] += b'changed'
        if manifest_change:
            manifest = json.loads(payload['operations.json'])
            manifest_change(manifest)
            payload['operations.json'] = json.dumps(manifest).encode()
        result = self.root / ('bad-' + uuid.uuid4().hex + '.tar.gz')
        with tarfile.open(result, 'w:gz') as target:
            for name, data in payload.items():
                member = tarfile.TarInfo(name)
                member.size = len(data)
                target.addfile(member, io.BytesIO(data))
            if append:
                target.addfile(append, io.BytesIO(b'x' * append.size))
        return result

    def test_relocated_commands_resources_and_product_imports(self):
        destination = self.root / 'unpacked'
        bundle.extract(self.archive, destination, self.sha)
        self.addCleanup(lambda: shutil.rmtree(destination))
        environment = {**os.environ, 'PYTHONPATH': '', 'PATH': '/usr/bin:/bin'}
        for script in ('wallet-pir-deploy.py', 'wallet-pir-bundle.py', 'wallet-pir-roles.py'):
            result = subprocess.run([sys.executable, str(destination / 'tools/scripts' / script), '--help'],
                                    cwd=self.root, env=environment, capture_output=True, text=True, timeout=15)
            self.assertEqual(result.returncode, 0, result.stderr)
        # Every external worker renders from the relocated package alone.
        sys.path.insert(0, str(Path(__file__).resolve().parent))
        from worker_settings import SETTINGS
        for (service, role), settings in SETTINGS.items():
            path = self.root / ('settings-%s.json' % service)
            path.write_text(json.dumps(settings))
            result = subprocess.run([sys.executable, str(destination / 'tools/scripts/wallet-pir-roles.py'),
                                     '--service', service, '--role', role, '--settings', str(path),
                                     '--release', '/opt/example/releases/x'],
                                    cwd=self.root, env=environment, capture_output=True, text=True, timeout=15)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn('/opt/example/releases/x/', json.loads(result.stdout)['unit'])
        registry = descriptors.load_descriptors(destination / 'tools/deploy/services.toml')
        self.assertEqual(set(registry), {'publisher', 'enhance', 'status', 'receiver', 'transparent-history', 'transparent-display'})
        self.assertTrue(all(r.template is None or r.template.is_relative_to(destination.resolve())
                            for s in registry.values() for r in s.roles.values()))
        probe = ("from wallet_pir_ops import release; "
                 "from tools.txid.lib import txid_display_poc; "
                 "from tools.transparent.lib import activity_deployed_qualification as qualification; "
                 "assert release.ROOT == txid_display_poc.ROOT; "
                 "assert qualification.HERE == release.ROOT / 'tools/transparent/lib'")
        result = subprocess.run([sys.executable, '-B', '-c', probe], cwd=self.root,
                                env={**environment, 'PYTHONPATH': str(destination / 'tools/lib')},
                                capture_output=True, text=True, timeout=15)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertFalse(list(destination.rglob('*.pyc')))
        self.assertFalse(any('.git' in p.parts for p in destination.rglob('*')))


    def test_archive_refusals_leave_destination_absent(self):
        special = tarfile.TarInfo('tools/lib/linked.py')
        special.type, special.linkname = tarfile.SYMTYPE, '/etc/passwd'
        traversal = tarfile.TarInfo('../escaped.py')
        duplicate = tarfile.TarInfo('tools/scripts/wallet-pir-deploy.py')
        archives = [self.rewrite(omit='tools/deploy/services.toml'),
                    self.rewrite(change='tools/lib/wallet_pir_ops/deploy/host_helper.py'),
                    self.rewrite(append=special), self.rewrite(append=traversal),
                    self.rewrite(append=duplicate),
                    self.rewrite(manifest_change=lambda m: m.update(source_revision='f' * 40))]
        for i, archive in enumerate(archives):
            with self.subTest(archive=i), self.assertRaises(ValueError):
                bundle.extract(archive, self.root / 'refused', self.sha)
            self.assertFalse((self.root / 'refused').exists())

    def test_dirty_and_changed_installed_resources_refuse_mutation(self):
        destination = self.root / 'dirty'
        bundle.extract(self.archive, destination, self.sha)
        self.addCleanup(lambda: shutil.rmtree(destination))
        path = destination / 'operations.json'
        manifest = json.loads(path.read_text())
        manifest['source_dirty'] = True
        path.write_text(json.dumps(manifest))
        bundle.check_installed(root=destination)
        with self.assertRaisesRegex(ValueError, 'inspection-only'):
            bundle.check_installed(root=destination, mutation=True)
        with self.assertRaisesRegex(ValueError, 'same source revision'):
            bundle.check_installed(root=destination, revision='f' * 40)
        (destination / 'tools/deploy/services.toml').write_text('changed')
        with self.assertRaisesRegex(ValueError, 'resource changed'):
            bundle.check_installed(root=destination)

    def test_inventory_excludes_runtime_credentials_and_evidence(self):
        self.assertFalse(any(not bundle.allowed(name) for name in self.manifest['files']))
        infrastructure = ROOT / 'tools/infra/digitalocean'
        for path in infrastructure.rglob('*.tf'):
            self.assertIn(path.relative_to(ROOT).as_posix(), self.manifest['files'])
        for name in ('tools/deploy/.env', 'tools/infra/production/secrets.tfvars',
                     'evidence/transparent/run/raw.py', 'tools/lib/__pycache__/a.pyc',
                     'tools/deploy/id_ed25519', 'tools/deploy/inventory.json',
                     'tools/infra/digitalocean/fleet/terraform.tfstate',
                     'tools/infra/digitalocean/fleet/saved.tfplan',
                     'tools/infra/digitalocean/fleet/secrets.tfvars',
                     'tools/infra/digitalocean/fleet/settings.auto.tfvars.json',
                     'tools/infra/digitalocean/fleet/.terraform/terraform.tfstate'):
            self.assertFalse(bundle.allowed(name), name)

    def test_registry_refuses_cycles_and_duplicate_product_ownership(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / 'registry.toml'
            path.write_text('include = ["registry.toml"]\n')
            with self.assertRaisesRegex(ValueError, 'cyclic'):
                descriptors.load_descriptors(path)
            source = str(ROOT / 'tools/enhance/deploy/deploy.toml')
            path.write_text('include = '+json.dumps([source, source])+'\n')
            with self.assertRaisesRegex(ValueError, 'duplicate'):
                descriptors.load_descriptors(path)


if __name__ == '__main__':
    unittest.main()
