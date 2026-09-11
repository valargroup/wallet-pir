import json,subprocess,datetime
from pathlib import Path
script=r'''
import json,subprocess
from pathlib import Path
p=Path('/run/systemd/system/transparent-m1-maintenance-restore.service.d')
p.mkdir(exist_ok=True)
(p/'restore-timer.conf').write_text('[Service]\nExecStart=\nExecStart=/bin/sh -c "systemctl unmask --runtime apt-daily-upgrade.service && systemctl start apt-daily-upgrade.timer"\n')
subprocess.run(['systemctl','daemon-reload'],check=True)
assert subprocess.check_output(['systemctl','is-active','transparent-m1-maintenance-restore.timer'],text=True).strip()=='active'
print(subprocess.check_output(['systemctl','show','transparent-m1-maintenance-restore.service','-p','ExecStart'],text=True))
'''
root=Path('/opt/transparent-publisher-build/unpublished-reorg-20260911')
record=root/'maintenance-window-restore-correction.ndjson'
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
