import asyncio, hashlib, importlib.util, json, shutil, subprocess, time
from pathlib import Path
root=Path('/opt/transparent-publisher'); stage=Path('/opt/transparent-publisher-build/sessions-20260910')
script=root/'transparent-live-fleet.py'; config=root/'fleet.json'
assert hashlib.sha256(script.read_bytes()).hexdigest()=='34365b000f598c37e13fcd837434203062df8dd4fdb57cc2f8d53760d99a9d7b'
c=json.loads(config.read_text()); assert not c.get('control_sessions',False)
backup=stage/'before-enable'; backup.mkdir()
shutil.copy2(script,backup/script.name); shutil.copy2(config,backup/config.name)
def command(*args): subprocess.run(args,check=True)
def install(source,target):
 temp=target.with_name(target.name+'.sessions-new'); shutil.copy2(source,temp); temp.replace(target)
async def validate():
 spec=importlib.util.spec_from_file_location('fleet',script); m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
 candidate=dict(c,control_sessions=True); fleet=m.Fleet(candidate)
 for w in fleet.roster:
  d=await fleet.control(w,{'operation':'status'})
  assert d.get('warm') and not d.get('invalidated',False),w['id']
 print('All six owned control connections report warm, valid status',flush=True)
 async with fleet.lock('routing'):
  m.atomic_json(config,candidate)
try:
 command('systemctl','stop','transparent-m1-control-session-trial.service')
 install(stage/'fleet.py',script)
 install(stage/'transparent-control-sessions.service',Path('/etc/systemd/system/transparent-control-sessions.service'))
 command('systemctl','daemon-reload')
 command('systemctl','enable','--now','transparent-control-sessions.service')
 time.sleep(3)
 command('systemctl','stop','transparent-replica-reconciler.service')
 asyncio.run(validate())
 command('systemctl','start','transparent-replica-reconciler.service')
 command('systemctl','is-active','transparent-control-sessions.service','transparent-replica-reconciler.service')
 print('Enabled owned control sessions',flush=True)
except BaseException:
 install(backup/'fleet.json',config)
 install(backup/script.name,script)
 command('systemctl','restart','transparent-replica-reconciler.service')
 command('systemctl','disable','--now','transparent-control-sessions.service')
 raise
