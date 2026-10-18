"""Pin the pre-extraction configuration, without retaining private state or plans."""
import hashlib
import json
from pathlib import Path
import re
import unittest

ROOT = Path(__file__).resolve().parents[2]
INFRA = ROOT / 'ops/infra/digitalocean'
CONTRACT = json.loads((Path(__file__).parent / 'fixtures/digitalocean-layout-contract.json').read_text())
BLOCKS = re.compile(r'^(resource|variable|output|locals|moved|terraform|provider)\b[^\n]*\{.*?^\}', re.M | re.S)


def canonical(text):
    return re.sub(r'\s+', '', re.sub(r'^\s*#.*$', '', text, flags=re.M))


def digest(text):
    return hashlib.sha256(canonical(text).encode()).hexdigest()


def original_references(text):
    for directory in ('enhance', 'status', 'transparent', 'transparent-tx-detail'):
        module = directory.replace('-', '_')
        for match in re.finditer(r'output "([^"]+)" \{\s*value\s*=\s*([\w.]+)', (INFRA / directory / 'outputs.tf').read_text()):
            text = text.replace(f'module.{module}.{match[1]}', match[2])
    return text


class InfrastructureLayout(unittest.TestCase):
    def test_root_interfaces_and_all_resource_arguments_are_preserved(self):
        actual = {}
        for directory in ('fleet', 'enhance', 'status', 'transparent', 'transparent-tx-detail'):
            for path in (INFRA / directory).glob('*.tf'):
                for match in BLOCKS.finditer(path.read_text()):
                    kind, text = match[1], match[0]
                    if kind not in ('resource', 'variable', 'output'):
                        continue
                    if kind != 'resource' and directory != 'fleet':
                        continue
                    text = original_references(text)
                    if directory != 'fleet':
                        for new, old in {'var.vpc_id': 'digitalocean_vpc.wallet.id',
                                         'var.coordinator_id': 'digitalocean_droplet.coordinator.id',
                                         'var.coordinator_tag': 'digitalocean_tag.coordinator.name',
                                         'var.transparent_worker_tag': 'digitalocean_tag.transparent_worker.name'}.items():
                            text = text.replace(new, old)
                        template_dir = {'enhance': 'enhance/', 'status': 'status/',
                                        'transparent': 'transparent/', 'transparent-tx-detail': 'transparent/'}[directory]
                        text = text.replace('${path.module}/', '${path.module}/' + template_dir)
                    # The receiver cloud-init moved from receiver/ops/ to ops/receiver/;
                    # its bytes, and so the rendered user data, are unchanged.
                    text = text.replace('${path.module}/../../../receiver/digitalocean/',
                                        '${path.module}/../../../../receiver/ops/digitalocean/')
                    labels = re.findall(r'"([^"]+)"', text.split('\n')[0])
                    key = kind + ':' + '.'.join(labels)
                    self.assertNotIn(key, actual)
                    actual[key] = digest(text)
        expected = {key: value for key, value in CONTRACT.items() if ':' in key}
        self.assertEqual(actual, expected)

    def test_moves_cover_exactly_the_extracted_resources(self):
        extracted = {}
        for directory in ('enhance', 'status', 'transparent', 'transparent-tx-detail'):
            for match in re.finditer(r'^resource "([^"]+)" "([^"]+)"', (INFRA / directory / 'main.tf').read_text(), re.M):
                address = match[1] + '.' + match[2]
                extracted[address] = 'module.' + directory.replace('-', '_') + '.' + address
        moves = dict(re.findall(r'from\s*=\s*([\w.]+)\s+to\s*=\s*([\w.]+)', (INFRA / 'fleet/moves.tf').read_text()))
        self.assertEqual(moves, extracted)
        shared = (INFRA / 'fleet/shared.tf').read_text()
        self.assertIn('from = digitalocean_droplet.transparent_archive[0]', shared)
        self.assertIn('to   = digitalocean_droplet.transparent_archive["transparent-pir-archive-01"]', shared)
        self.assertIn('from = digitalocean_droplet.worker', shared)
        self.assertIn('to   = digitalocean_droplet.enhance_worker', shared)

    def test_templates_and_independent_roots_are_unchanged(self):
        for old, expected in CONTRACT['templates'].items():
            new = old if '/' in old else 'fleet/' + old
            self.assertEqual(hashlib.sha256((INFRA / new).read_bytes()).hexdigest(), expected)
        self.assertEqual((INFRA / 'transparent/cloud-init-transparent-worker.yaml.tftpl').read_bytes(),
                         (INFRA / 'transparent-tx-detail/cloud-init-transparent-worker.yaml.tftpl').read_bytes())
        for old, expected in CONTRACT['pool_files'].items():
            new = old.replace('enhance-v4/', 'enhance/worker-pool/').replace('transparent-elastic/', 'transparent/worker-pool/')
            self.assertEqual(hashlib.sha256((INFRA / new).read_bytes()).hexdigest(), expected)

    def test_operations_bundle_contains_complete_modules_and_no_private_inputs(self):
        import sys
        sys.path.insert(0, str(ROOT / 'ops/lib'))
        from wallet_pir_ops.bundle import contents, allowed
        files = contents(ROOT)
        for path in INFRA.rglob('*.tf'):
            self.assertIn(path.relative_to(ROOT).as_posix(), files)
        for name in ('terraform.tfstate', 'saved.tfplan', 'secrets.tfvars', 'settings.auto.tfvars.json', '.terraform/terraform.tfstate'):
            self.assertFalse(allowed('ops/infra/digitalocean/fleet/' + name))


if __name__ == '__main__':
    unittest.main()
