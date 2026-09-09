"""Start only the qualified, fixed M1 candidate using the existing gated rollout."""
import hashlib, json, os, shutil, subprocess
from pathlib import Path
ROOT = Path('/opt/transparent-publisher-build/readbatch-20260909')
LIVE = Path('/opt/transparent-publisher')
def sha(p): return hashlib.sha256(p.read_bytes()).hexdigest()
rows = json.loads((ROOT/'comparison.json').read_text())
selected = [r for r in rows if r['variant']=='readbatch' and r['build_slots']==2]
assert len(selected)==3 and all(r['combined_screen_passed'] for r in selected)
assert sha(ROOT/'artifacts/transparent-shard-server')=='c9d88ca7d73014c8217c4240c4af858921fbfaa2848da5a1d49f9fbb7727308f'
assert sha(LIVE/'transparent-live-fleet.py')=='f239f4a33dacf464a4492a97b73ccd0118257b705b77ad9ee97efa4d03e28df6'
active=subprocess.check_output(['systemctl','list-units','--state=running','--no-legend','--plain','transparent*rollout*'],text=True)
assert not active.strip(), 'another rollout is running'
# Preserve the current operator inputs before installing this qualified target.
backup = ROOT/'pre-canary'
backup.mkdir(exist_ok=False)
config=json.loads((LIVE/'fleet.json').read_text())
roster_path=Path(config['roster'])
roster=json.loads(roster_path.read_text())
assert len(roster)==6 and sum(w['role']=='recent-replica' for w in roster)==4
assert config['managed_recent_workers']==['transparent-pir-recent-01']
for name in ['fleet.json','roster.json','deploy-transparent-publisher.py','observe-transparent-hardening.py','upgrade-transparent-fleet.py']:
 shutil.copy2(LIVE/name,backup/name)
for w in roster:
 if w['role']=='recent-replica':w['build_slots']=2
candidate=roster_path.with_suffix('.m1-next')
candidate.write_text(json.dumps(roster,indent=2)+'\n')
os.replace(candidate,roster_path)
for name in ['deploy-transparent-publisher.py','observe-transparent-hardening.py','upgrade-transparent-fleet.py']:
 candidate=LIVE/(name+'.m1-next')
 shutil.copy2(ROOT/'ops'/name,candidate)
 os.replace(candidate,LIVE/name)
subprocess.run(['systemd-run','--unit=transparent-m1-qualified-rollout', '--property=WorkingDirectory='+str(LIVE),
 '/usr/bin/python3',str(LIVE/'run-transparent-hardening-rollout.py'),
 '--artifacts',str(ROOT/'artifacts'),'--source-sha','d778c62',
 '--out',str(ROOT/'rollout')],check=True)
