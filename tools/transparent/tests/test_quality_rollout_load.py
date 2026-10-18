import asyncio
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('rollout', Path(__file__).parents[1]/'scripts/run-transparent-hardening-rollout.py')
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)

class LoadRolloutTests(unittest.TestCase):
    def run_case(self, fail=False, latched=False):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            artifacts = root/'artifacts'; artifacts.mkdir()
            (artifacts/'transparent-shard-server').write_bytes(b'candidate')
            sha = hashlib.sha256(b'candidate').hexdigest()
            roster = [dict(id='transparent-pir-recent-01', upstream='worker1:8093'), dict(id='transparent-pir-recent-02', upstream='worker2:8093')]
            (root/'roster.json').write_text(json.dumps(roster))
            (root/'fleet.json').write_text(json.dumps(dict(roster=str(root/'roster.json'))))
            pins = {w['id']: '1'*64 for w in roster}
            (root/'pins.json').write_text(json.dumps(pins))
            if latched: (root/'latched.json').write_text('{}')
            args = SimpleNamespace(out=root/'result', artifacts=artifacts, source_sha='source', fleet_config=root/'fleet.json', observe_installed_canary=False, production_lock=root/'lock', load_service='load.service', load_root=root, load_identity_file=root/'pins.json')
            calls=[]
            async def command(argv, logfile):
                calls.append(list(map(str,argv)))
                if str(argv[1]).endswith('upgrade-transparent-fleet.py'):
                    if fail: raise RuntimeError('upgrade failed')
                    if '--worker' in argv: pins[roster[0]['id']]=sha
                    else:
                        for w in roster: pins[w['id']]=sha
            def urlopen(url, timeout):
                worker=roster[0] if 'worker1:' in url else roster[1]
                return io.StringIO(json.dumps(dict(ready=True,binary_sha256=pins[worker['id']])))
            with patch.object(m,'command',command),patch.object(m.urllib.request,'urlopen',urlopen):
                if fail or latched:
                    with self.assertRaises(RuntimeError): asyncio.run(m.run(args))
                else: asyncio.run(m.run(args))
            controls=[a[:2] for a in calls if a[0]=='systemctl']
            if latched:self.assertEqual(calls,[])
            elif fail:self.assertEqual(controls,[['systemctl','stop']])
            else:
                self.assertEqual(controls,[['systemctl','stop'],['systemctl','start']]*2)
                self.assertEqual(set(json.loads((root/'pins.json').read_text()).values()),{sha})
    def test_success_requalifies_each_phase_before_resuming(self):self.run_case()
    def test_failed_upgrade_does_not_resume_load(self):self.run_case(fail=True)
    def test_existing_latch_prevents_any_mutation(self):self.run_case(latched=True)

if __name__=='__main__': unittest.main()
