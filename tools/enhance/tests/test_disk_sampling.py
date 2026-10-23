"""Filesystem and whole-fleet sampling contracts, without SSH or real disks."""
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

SCRIPTS = Path(__file__).resolve().parents[1] / 'scripts'


def load(name):
    spec = importlib.util.spec_from_file_location(name, SCRIPTS / (name + '.py'))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


disks = load('host-disks')
sampler = load('quality-host-sampler')


class DiskSamplingTest(unittest.TestCase):
    def mount(self, target, source, uuid, options='rw', fstype='ext4'):
        return dict(target=target, source=source, uuid=uuid, options=options, fstype=fstype)

    def test_root_and_volumes_are_separate_and_bind_mounts_are_counted_once(self):
        mounts = [self.mount('/', '/dev/vda1', 'root'),
                  self.mount('/mnt/data', '/dev/sda', 'volume'),
                  self.mount('/srv/cache', '/dev/sda[/cache]', 'volume'),
                  self.mount('/run', 'tmpfs', None),
                  self.mount('/proc', 'proc', None),
                  self.mount('/container', 'overlay', None),
                  self.mount('/snap/a', '/dev/loop0', None, 'ro'),
                  self.mount('/backup', '/dev/sdb', 'backup', 'ro')]
        def space(path):
            # Available excludes blocks reserved for root, even though the
            # sampler itself runs as root.
            return SimpleNamespace(f_bavail=0 if path == '/mnt/data' else 50,
                                   f_bfree=90, f_blocks=100, f_frsize=4096)
        with patch.object(subprocess, 'check_output', return_value=json.dumps({'filesystems': mounts})), \
                patch.object(os, 'statvfs', space):
            observed = disks.disk_filesystems()
        self.assertEqual([d['id'] for d in observed], ['root', 'volume'])
        self.assertEqual([d['available_bytes'] for d in observed], [50 * 4096, 0])

    def test_failed_volume_is_reported_without_losing_healthy_root(self):
        mounts = [self.mount('/', '/dev/vda1', None), self.mount('/mnt/data', '/dev/sda', None)]
        def space(path):
            if path != '/':
                raise OSError('unavailable')
            return SimpleNamespace(f_bavail=50, f_blocks=100, f_frsize=1)
        with patch.object(subprocess, 'check_output', return_value=json.dumps({'filesystems': mounts})), \
                patch.object(os, 'statvfs', space):
            observed = disks.disk_filesystems()
        self.assertEqual(observed[0]['available_bytes'], 50)
        self.assertEqual(observed[1]['error'], 'filesystem unavailable')
        self.assertNotIn('available_bytes', observed[1])

    def test_unit_failure_does_not_discard_disk_measurements(self):
        # Execute the same source shipped over SSH, with service reads failing.
        script = sampler.REMOTE.replace("out = {'at': int(time.time())}",
            "pathlib.Path.read_text = lambda self: 'machine'\n"
            "disk_filesystems = lambda: [{'id': 'volume', 'available_bytes': 0, 'total_bytes': 100}]\n"
            "out = {'at': int(time.time())}")
        result = subprocess.run(['python3', '-c', script, 'stopped.service', '/', '-'],
                                capture_output=True, text=True, check=True, timeout=5)
        value = json.loads(result.stdout)
        self.assertEqual(value['filesystems'][0]['available_bytes'], 0)
        self.assertEqual(value['resource_error'], 'service observation unavailable')

    def test_inventory_includes_all_hosts_and_preserves_user_and_jump_routes(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'fleet.json'
            path.write_text(json.dumps({'hosts': {
                'publisher': {'address': '10.0.0.1'},
                'gpu': {'address': '10.0.0.2', 'user': 'paperspace', 'jump': 'publisher'},
                'monitor': {'address': '10.0.0.3'},
                'retired': {'address': '10.0.0.4'}}, 'retired_hosts': {'retired': {}}}))
            targets, retired = sampler.inventory_targets(path)
        self.assertEqual([t['host_id'] for t in targets], ['publisher', 'gpu', 'monitor'])
        self.assertEqual(targets[1]['host'], 'paperspace@10.0.0.2')
        self.assertEqual(targets[1]['jump'], 'root@10.0.0.1')
        self.assertNotIn('unit', targets[2])
        self.assertEqual(retired, ['retired'])
        command = sampler.ssh_command({'key': '/pinned/key', 'known_hosts': '/pinned/hosts'}, targets[1])
        proxy = next(v for v in command if v.startswith('ProxyCommand='))
        self.assertIn('/pinned/key', proxy)
        self.assertIn('StrictHostKeyChecking=yes', proxy)
        self.assertIn('UserKnownHostsFile=/pinned/hosts', proxy)
        self.assertIn('root@10.0.0.1', proxy)

    def test_snapshot_deduplicates_shared_machines_and_marks_failed_hosts(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config = {'targets': [{'sources': ['enhance'], 'host_id': 'shared'},
                                  {'sources': ['status'], 'host_id': 'alias'},
                                  {'sources': ['details'], 'host_id': 'details'}],
                      'ssh': {}, 'output': str(root / 'hosts.json'), 'disk_output': str(root / 'disks.json')}
            def sample(target, ssh):
                if target['host_id'] == 'details':
                    raise OSError('failed')
                return {'at': 100, 'machine_id': 'same-machine', 'filesystems': [{'id': 'root'}]}
            with patch.object(sampler, 'sample', sample):
                sampler.collect(config)
            value = json.loads((root / 'disks.json').read_text())
        self.assertEqual(set(value['hosts']), {'shared', 'details'})
        self.assertEqual(value['hosts']['details']['at'], 0)
        self.assertTrue(value['inventory_ok'])


if __name__ == '__main__':
    unittest.main()
