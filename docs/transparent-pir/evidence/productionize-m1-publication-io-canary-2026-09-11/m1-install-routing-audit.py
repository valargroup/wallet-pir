"""Install the tested routing audit under the existing two-origin guard."""
import asyncio,hashlib,importlib.util,json,shutil,subprocess,time
from pathlib import Path
LIVE=Path('/opt/transparent-publisher')
STAGE=Path('/opt/transparent-publisher-build/publication-io-20260910')
OLD='b96c007ce76638f829119ab2013308d993cdba5fef76c7a8be2e14b21cf49698'
NEW='12612e9d2e75bf322f90fc9c32f70f7c283c51fbe4016d851a9a35fb64b9d339'
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def load(name,path):
 spec=importlib.util.spec_from_file_location(name,path);m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m);return m
def install(src,dst):
 temp=dst.with_name(dst.name+'.audit-new');shutil.copy2(src,temp);temp.replace(dst)
async def main():
 script=LIVE/'transparent-live-fleet.py';config=LIVE/'fleet.json'
 assert sha(script)==OLD and sha(STAGE/'ops/scripts/transparent-live-fleet.py')==NEW
 assert sha(config)=='9ac6e0eae0a98feb48645698520981a8bf8862fccd92ae9d293e22866bcdffc2'
 assert sha(LIVE/'roster.json')=='f81674cd250e596aead154e579ab24d5c9b3367f088823c94cf0444112ae4db9'
 proof=json.loads((STAGE/'qualification-summary.json').read_text())
 assert len(proof['runs'])==3
 for run in proof['runs']:
  assert run['completed'] and run['exact_queries'] and run['memory_qualification_passed'] and run['worker_budget_passed']
  assert run['external_clients'] and run['client_isolation_passed'] and run['load_overlapped_burst']
 running=subprocess.check_output(['systemctl','list-units','--state=running','--no-legend','--plain','transparent*rollout*'],text=True)
 assert not running.strip(),'an acceptance rollout is active'
 c=json.loads(config.read_text());assert c['status_socket_forwarding'] and c['control_sessions']
 old=load('old_fleet',script).Fleet(c)
 U=load('audit_upgrade',STAGE/'ops/scripts/upgrade-transparent-fleet.py')
 saved=STAGE/'routing-audit-install';saved.mkdir(mode=0o700)
 shutil.copy2(script,saved/script.name)
 services=('transparent-publish-controller','transparent-replica-reconciler')
 await U.maintenance(old,saved)
 try:
  U.service('stop',*services)
  install(STAGE/'ops/scripts/transparent-live-fleet.py',script)
  fleet=load('installed_audit',script).Fleet(c)
  await U.start_authority()
  deadline=time.monotonic()+60
  while True:
   try:
    await U.reopen(fleet,saved);await U.verify_public(fleet);break
   except Exception:
    if time.monotonic()>=deadline:raise
    await asyncio.sleep(1)
  audit=fleet.routing_availability();assert audit['available']
  result={'passed':True,'fleet_script_sha256':sha(script),'fleet_config_sha256':sha(config),'routing_availability':audit,'worker_upgrade_started':False}
  U.L.atomic_json(saved/'result.json',result);print(json.dumps(result),flush=True)
 except BaseException as error:
  U.service('stop',*services)
  U.L.atomic_json(old.root/'maintenance.json',{'enabled':True});await old.route([])
  U.apply_coordinator((saved/'Caddyfile.guarded').read_bytes())
  install(saved/script.name,script)
  await U.start_authority()
  reopened=False;recovery=None
  deadline=time.monotonic()+60
  while time.monotonic()<deadline:
   try:
    await U.reopen(old,saved);await U.verify_public(old);reopened=True;break
   except Exception as e:recovery=str(e);await asyncio.sleep(1)
  U.L.atomic_json(saved/'result.json',{'passed':False,'error':str(error),'rollback_reopened':reopened,'rollback_error':recovery})
  raise
asyncio.run(main())
