"""Install the qualified control transport correction without restarting workers."""
import asyncio,datetime,hashlib,importlib.util,json,os,shutil,subprocess
from pathlib import Path
live=Path('/opt/transparent-publisher');release=Path('/opt/transparent-publisher-build/control-20260910')
candidate=Path('/opt/transparent-publisher-build/querywait-20260909/transparent-live-fleet.corrected.py')
old='f239f4a33dacf464a4492a97b73ccd0118257b705b77ad9ee97efa4d03e28df6'
new='34365b000f598c37e13fcd837434203062df8dd4fdb57cc2f8d53760d99a9d7b'
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
async def main():
 assert sha(live/'transparent-live-fleet.py')==old
 assert sha(candidate)==new
 active=subprocess.check_output(['systemctl','list-units','--state=running','--no-legend','--plain','transparent*rollout*'],text=True)
 assert not active.strip(),'another rollout is active'
 release.mkdir(exist_ok=False)
 shutil.copy2(live/'transparent-live-fleet.py',release/'fleet-before.py')
 for name in ['fleet.json','roster.json']:
  shutil.copy2(live/name,release/name)
 spec=importlib.util.spec_from_file_location('fleet',live/'transparent-live-fleet.py');m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
 f=m.Fleet(json.loads((live/'fleet.json').read_text()))
 subprocess.run(['systemctl','stop','transparent-replica-reconciler'],check=True)
 try:
  async with f.lock('routing'):
   tmp=live/'transparent-live-fleet.py.control-next';shutil.copy2(candidate,tmp);tmp.chmod(0o755);os.replace(tmp,live/'transparent-live-fleet.py')
  subprocess.run(['systemctl','start','transparent-replica-reconciler'],check=True)
 except BaseException:
  tmp=live/'transparent-live-fleet.py.control-next';shutil.copy2(release/'fleet-before.py',tmp);os.replace(tmp,live/'transparent-live-fleet.py')
  subprocess.run(['systemctl','start','transparent-replica-reconciler'],check=True)
  raise
 result=dict(utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),before_sha256=old,after_sha256=sha(live/'transparent-live-fleet.py'),worker_restarts=False,controller_restarted=False)
 (release/'installed.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result))
asyncio.run(main())
