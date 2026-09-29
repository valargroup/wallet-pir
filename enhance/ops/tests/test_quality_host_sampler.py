import importlib.util
import json
from pathlib import Path
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


if __name__ == '__main__':
    unittest.main()
