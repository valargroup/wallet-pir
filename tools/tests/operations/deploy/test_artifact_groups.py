"""A regular Enhance deploy covers a GPU worker that runs a different release artifact."""
import argparse
import json
from pathlib import Path
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT / 'tools/lib'))
from wallet_pir_ops.deploy import cli, descriptors
from wallet_pir_ops.deploy.engine import DeployError

SHA = 'a' * 40


def inventory(gpu_kind='enhance-pir-native-cuda'):
    gpu = {'host': 'gpu-01', 'unit': 'enhance-pir-gpu-worker.service', 'vars': {'listen': '10.0.0.9:8091'}}
    if gpu_kind:
        gpu['artifact_kind'] = gpu_kind
    document = {
        'lock': {'type': 'remote', 'host': 'coordinator'},
        'ssh': {'mode': 'config', 'config_file': '/dev/null'},
        'hosts': {name: {'address': '10.0.0.%d' % i} for i, name in
                  enumerate(['coordinator', 'router-01', 'worker-01', 'gpu-01'], 1)},
        'services': {'enhance': {'roles': {
            'worker': [{'host': 'worker-01', 'vars': {'listen': '10.0.0.3:8091'}}, gpu],
            'packing-router': [{'host': 'router-01', 'vars': {'control_listen': '10.0.0.2:8093'}}],
            'query-ingress': [{'host': 'coordinator'}],
            'coordinator': [{'host': 'coordinator'}],
        }}},
    }
    directory = tempfile.mkdtemp()
    path = Path(directory) / 'inventory.json'
    path.write_text(json.dumps(document))
    return descriptors.load_inventory(path)


def args(**overrides):
    values = dict(binary=None, archive='native.tar.gz', sha=SHA, kind=None, sha256=None,
                  only=None, extra_archive=None)
    values.update(overrides)
    return argparse.Namespace(**values)


class ArtifactGroups(unittest.TestCase):
    service = descriptors.load_descriptors(cli.DESCRIPTORS)['enhance']

    def test_gpu_worker_cannot_be_skipped_silently(self):
        with self.assertRaisesRegex(DeployError, r"worker@gpu-01.*--extra-archive enhance-pir-native-cuda=FILE"):
            cli.artifact_groups(args(), self.service, inventory())

    def test_gpu_worker_deploys_in_its_own_transaction_in_rollout_order(self):
        groups = cli.artifact_groups(
            args(extra_archive=['enhance-pir-native-cuda=cuda.tar.gz']), self.service, inventory())
        self.assertEqual([keys for _, keys in groups], [
            ['worker@worker-01', 'packing-router@router-01', 'query-ingress@coordinator', 'coordinator@coordinator'],
            ['worker@gpu-01'],
        ])
        main, gpu = groups[0][0], groups[1][0]
        self.assertEqual((main.archive, main.kind), ('native.tar.gz', None))
        self.assertEqual((gpu.archive, gpu.kind, gpu.sha), ('cuda.tar.gz', 'enhance-pir-native-cuda', SHA))

    def test_gpu_group_comes_first_when_only_workers_roll(self):
        groups = cli.artifact_groups(
            args(only=['worker@gpu-01', 'worker@worker-01'], extra_archive=['enhance-pir-native-cuda=c.tgz']),
            self.service, inventory())
        self.assertEqual([keys for _, keys in groups], [['worker@worker-01'], ['worker@gpu-01']])

    def test_only_can_leave_the_gpu_out_deliberately(self):
        groups = cli.artifact_groups(args(only=['worker@worker-01']), self.service, inventory())
        self.assertEqual([keys for _, keys in groups], [['worker@worker-01']])

    def test_main_kind_matching_the_gpu_needs_no_extra_archive(self):
        groups = cli.artifact_groups(args(kind='enhance-pir-native-cuda', only=['worker@gpu-01']),
                                     self.service, inventory())
        self.assertEqual([(g.archive, keys) for g, keys in groups], [('native.tar.gz', ['worker@gpu-01'])])

    def test_a_binary_cannot_reach_the_gpu_and_unused_or_unknown_archives_refuse(self):
        with self.assertRaisesRegex(DeployError, 'worker@gpu-01'):
            cli.artifact_groups(args(archive=None, binary='server'), self.service, inventory())
        with self.assertRaisesRegex(DeployError, 'no selected target runs'):
            cli.artifact_groups(args(only=['coordinator'], extra_archive=['enhance-pir-native-cuda=c.tgz']),
                                self.service, inventory())
        with self.assertRaisesRegex(DeployError, 'KIND=FILE'):
            cli.artifact_groups(args(extra_archive=['status-pir=s.tgz']), self.service, inventory())

    def test_inventory_without_artifact_kinds_is_one_transaction(self):
        groups = cli.artifact_groups(args(), self.service, inventory(gpu_kind=None))
        self.assertEqual(len(groups), 1)
        self.assertIn('worker@gpu-01', groups[0][1])

    def test_unknown_artifact_kind_is_refused(self):
        with self.assertRaisesRegex(descriptors.DescriptorError, 'artifact_kind'):
            descriptors.targets(self.service, inventory(gpu_kind='transparent-shard'))


if __name__ == '__main__':
    unittest.main()
