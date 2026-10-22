import contextlib
import importlib.util
import io
import json
import os
import pathlib
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location('sampler', ROOT / 'scripts/quality-host-sampler.py')
sampler = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(sampler)


class RosterTargetsTest(unittest.TestCase):
    def config(self, directory, roster):
        path = Path(directory) / 'roster.json'
        if roster is not None:
            path.write_text(json.dumps(roster))
        return {'ssh': {'key': 'k', 'known_hosts': 'h'}, 'output': str(Path(directory) / 'hosts.json'),
                'edge_output': str(Path(directory) / 'edge.prom'),
                'roster': {'path': str(path), 'unit': 'transparent-shard-server.service'},
                'targets': [{'sources': ['transparent-publisher'], 'unit': 'transparent-publish-controller.service',
                             'data_dir': '/srv/zakura'}]}

    def run_collect(self, config):
        seen = []

        def sample(target, ssh):
            seen.append((target['sources'][0], target.get('host')))
            return {'at': 1}
        with mock.patch.object(sampler, 'sample', sample):
            sampler.collect(config)
        return seen

    def test_roster_members_are_sampled_under_their_own_names(self):
        roster = [{'id': 'transparent-pir-recent-01', 'ssh_host': '10.0.0.10', 'intent': 'enrolled'},
                  {'id': 'transparent-pir-recent-05', 'ssh_host': '10.0.0.20', 'intent': 'draining'},
                  {'id': 'transparent-pir-archive-03', 'ssh_host': '10.0.0.7', 'intent': 'enrolled'},
                  {'id': 'transparent-pir-recent-06', 'ssh_host': '10.0.0.21', 'intent': 'retired'}]
        with tempfile.TemporaryDirectory() as directory:
            config = self.config(directory, roster)
            seen = self.run_collect(config)
            # Sampled in a thread pool, so in no fixed order.
            self.assertEqual(set(seen), {('transparent-pir-recent-01', 'root@10.0.0.10'),
                                         ('transparent-pir-recent-05', 'root@10.0.0.20'),
                                         ('transparent-pir-archive-03', 'root@10.0.0.7'),
                                         ('transparent-publisher', None)})
            written = json.loads(Path(config['output']).read_text())
            self.assertNotIn('transparent-pir-recent-06', written)

    def test_an_unreadable_roster_keeps_the_explicit_targets(self):
        with tempfile.TemporaryDirectory() as directory:
            seen = self.run_collect(self.config(directory, None))
            self.assertEqual(seen, [('transparent-publisher', None)])

    def test_roster_targets_carry_worker_id_size_and_role_unit(self):
        roster = [{'id': 'transparent-pir-v12-archive', 'role': 'archive-owner', 'ssh_host': '10.0.0.9'},
                  {'id': 'transparent-pir-v12-recent', 'role': 'recent-replica', 'ssh_host': '10.0.0.9'}]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'roster.json'
            path.write_text(json.dumps(roster))
            targets = sampler.roster_targets({
                'path': str(path), 'unit': 'transparent-shard-server.service', 'size': 'm-8vcpu-64gb',
                'role_units': {'archive-owner': 'transparent-v12-archive.service'}})
        self.assertEqual([(t['host_id'], t['unit'], t['size']) for t in targets], [
            ('transparent-pir-v12-archive', 'transparent-v12-archive.service', 'm-8vcpu-64gb'),
            ('transparent-pir-v12-recent', 'transparent-shard-server.service', 'm-8vcpu-64gb')])


class OutputIdentityTest(unittest.TestCase):
    def collect(self, targets, sample):
        with tempfile.TemporaryDirectory() as directory:
            config = {'ssh': {'key': 'k', 'known_hosts': 'h'}, 'output': str(Path(directory) / 'hosts.json'),
                      'edge_output': str(Path(directory) / 'edge.prom'), 'targets': targets}
            with mock.patch.object(sampler, 'sample', sample), contextlib.redirect_stdout(io.StringIO()):
                sampler.collect(config)
            return json.loads(Path(config['output']).read_text())

    def test_entries_carry_host_id_and_size_beside_the_observations(self):
        targets = [{'sources': ['transparent-publisher', 'txid-controller'], 'host_id': 'wallet-pir-coordinator-01',
                    'unit': 'transparent-publish-controller.service', 'data_dir': '/'},
                   {'sources': ['transparent-pir-txid-k8-01'], 'size': 'm-8vcpu-64gb-intel',
                    'unit': 'transparent-txid-display-worker.service', 'data_dir': '/', 'host': 'root@10.0.0.30'},
                   {'sources': ['bad-config'], 'host_id': 'has space', 'size': 'Not A Slug',
                    'unit': 'x.service', 'data_dir': '/', 'host': 'root@10.0.0.31'}]

        def sample(target, ssh):
            if target['sources'][0] == 'transparent-pir-txid-k8-01':
                raise OSError('unreachable')
            return {'at': 5, 'memory_total_bytes': 1}
        written = self.collect(targets, sample)
        self.assertEqual(written['transparent-publisher'],
                         {'at': 5, 'memory_total_bytes': 1, 'host_id': 'wallet-pir-coordinator-01'})
        self.assertEqual(written['txid-controller'], written['transparent-publisher'])
        # A failed host keeps its identity but never a fresh timestamp.
        self.assertEqual(written['transparent-pir-txid-k8-01'],
                         {'at': 0, 'host_id': 'transparent-pir-txid-k8-01', 'size': 'm-8vcpu-64gb-intel'})
        # A malformed host_id or size never reaches the snapshot.
        self.assertEqual(written['bad-config'], {'at': 5, 'memory_total_bytes': 1, 'host_id': 'bad-config'})


    def test_extra_units_are_validated_and_capped(self):
        ok = {'sources': ['a'], 'unit': 'a.service', 'data_dir': '/', 'units': ['b.service', 'pir-apm.service']}
        self.assertEqual(sampler.extra_units(ok), ['b.service', 'pir-apm.service'])
        for units in (['-H'], ['a b.service'], ['x.service'] * (sampler.MAX_UNITS + 1), 'pir-apm.service', [3]):
            with self.assertRaises(ValueError):
                sampler.extra_units(dict(ok, units=units))
        with self.assertRaises(ValueError):
            sampler.sample(dict(ok, unit='--host=x'), {'key': 'k', 'known_hosts': 'h'})


class RemoteScriptTest(unittest.TestCase):
    """Runs the remote script in process against fake /proc, cgroup and systemctl answers."""

    SHOW = {
        'worker.service': 'LoadState=loaded\nNRestarts=2\nControlGroup=/system.slice/worker.service\n'
                          'CPUUsageNSec=3000000000\nMemoryCurrent=4096\nMemoryPeak=8192\nMemoryMax=16384\n',
        'controller.service': 'LoadState=loaded\nControlGroup=/system.slice/controller.service\n'
                              'MemoryCurrent=[not set]\nMemoryMax=infinity\n',
        'missing.service': 'LoadState=not-found\nMemoryMax=infinity\n',
    }
    FILES = {
        '/proc/meminfo': 'MemTotal: 1000 kB\nMemAvailable: 600 kB\nSwapTotal: 200 kB\nSwapFree: 150 kB\n',
        '/sys/fs/cgroup/system.slice/worker.service/memory.events': 'low 0\nhigh 0\nmax 1\noom 1\noom_kill 1\n',
        '/sys/fs/cgroup/system.slice/controller.service/memory.events': 'oom 0\noom_kill 0\n',
        '/sys/fs/cgroup/system.slice/controller.service/memory.peak': '2048\n',
        '/sys/fs/cgroup/system.slice/worker.service/memory.stat': 'anon 1000\nfile 3000\nfile_dirty 10\nshmem 24\n',
        '/sys/fs/cgroup/system.slice/controller.service/memory.stat': 'anon 512\nfile 100\n',
    }

    def run_remote(self, *argv):
        files = self.FILES

        def show(command, **kwargs):
            return self.SHOW[command[2]] if command[:2] == ['systemctl', 'show'] else ''
        statvfs = mock.Mock(f_bavail=10, f_blocks=20, f_frsize=4096)
        out = io.StringIO()
        with mock.patch.object(pathlib.Path, 'read_text', lambda self: files[str(self)]), \
                mock.patch.object(pathlib.Path, 'exists', lambda self: str(self) in files), \
                mock.patch.object(os, 'statvfs', lambda path: statvfs), \
                mock.patch.object(subprocess, 'check_output', show), \
                mock.patch.object(sys, 'argv', ['-', *argv]), contextlib.redirect_stdout(out):
            exec(sampler.REMOTE, {})
        return json.loads(out.getvalue())

    def test_reports_swap_and_per_unit_memory_without_inventing_missing_values(self):
        value = self.run_remote('worker.service', '/', '-', 'controller.service', 'missing.service', 'worker.service')
        self.assertEqual(value['memory_available_bytes'], 600 * 1024)
        self.assertEqual((value['swap_total_bytes'], value['swap_free_bytes']), (200 * 1024, 150 * 1024))
        # The existing per-host fields are unchanged.
        self.assertEqual((value['restarts'], value['cgroup_memory_bytes'], value['cpu_seconds'], value['oom_kills']),
                         (2, 4096, 3, 1))
        # The main unit is named, so its OOM kills are not counted twice.
        self.assertEqual(value['unit'], 'worker.service')
        self.assertEqual(value['units'], {
            # Anonymous memory is anon plus shmem, never the page cache.
            'worker.service': {'memory_current_bytes': 4096, 'memory_anon_bytes': 1024, 'memory_peak_bytes': 8192,
                               'memory_max_bytes': 16384, 'oom_kills': 1},
            # No MemoryPeak property: the cgroup's own peak. No MemoryCurrent: left out.
            # No shmem line: anon alone. A unit systemd has not loaded is not reported at all.
            'controller.service': {'memory_anon_bytes': 512, 'memory_peak_bytes': 2048, 'memory_max_bytes': None,
                                   'oom_kills': 0},
        })
        self.assertNotIn('edge_metrics', value)


if __name__ == '__main__':
    unittest.main()
