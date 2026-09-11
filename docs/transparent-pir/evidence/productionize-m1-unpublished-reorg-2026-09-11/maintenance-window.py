import json,subprocess,datetime
from pathlib import Path
script=r'''
import json,subprocess,os
from pathlib import Path
def run(*args): return subprocess.check_output(args,text=True).strip()
name='apt-daily-upgrade.service'
state=run('systemctl','show',name,'-p','ActiveState','--value')
assert state in ('inactive','failed'),state
mask=Path('/run/systemd/system')/name
assert not mask.exists() and not mask.is_symlink(), 'Existing runtime override; refusing to replace'
assert run('systemctl','show',name,'-p','LoadState','--value')=='loaded'
subprocess.run(['systemd-run','--unit=transparent-m1-maintenance-restore','--on-active=36h','--timer-property=AccuracySec=1s','/usr/bin/systemctl','unmask','--runtime',name],check=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
assert run('systemctl','is-active','transparent-m1-maintenance-restore.timer')=='active'
subprocess.run(['systemctl','mask','--runtime',name],check=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
assert mask.is_symlink() and os.readlink(mask)=='/dev/null'
assert run('systemctl','show',name,'-p','ActiveState','--value') in ('inactive','failed')
print(json.dumps({'host':run('hostname'),'runtime_mask':str(mask),'restore_timer':run('systemctl','show','transparent-m1-maintenance-restore.timer','-p','ActiveState','-p','NextElapseUSecMonotonic'),'upgrade_timer':run('systemctl','show','apt-daily-upgrade.timer','-p','ActiveState','-p','UnitFileState')}))
'''
root=Path('/opt/transparent-publisher-build/unpublished-reorg-20260911')
record=root/'maintenance-window-retry.ndjson'
assert not record.exists()
hosts=['10.142.0.10','10.142.0.8','10.142.0.7','10.142.0.12','10.142.0.6','10.142.0.9','10.142.0.11','10.142.0.13',None]
for host in hosts:
 if host:
  known='/tmp/transparent-loadgen-known-hosts' if host=='10.142.0.13' else '/opt/transparent-publisher/credentials/known_hosts'
  cmd=['ssh','-o','ConnectTimeout=10','-o','StrictHostKeyChecking=yes','-i','/opt/transparent-publisher/credentials/deploy-ssh','-o','UserKnownHostsFile='+known,'root@'+host,'python3','-']
 else: cmd=['python3','-']
 result=subprocess.run(cmd,input=script,text=True,capture_output=True,timeout=45)
 item={'utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'host':host or 'coordinator','exit_code':result.returncode,'stdout':result.stdout,'stderr':result.stderr}
 with record.open('a') as f:f.write(json.dumps(item)+'\n')
 print(json.dumps(item),flush=True)
 if result.returncode: raise RuntimeError('Maintenance window failed; existing holds have automatic expiry')
