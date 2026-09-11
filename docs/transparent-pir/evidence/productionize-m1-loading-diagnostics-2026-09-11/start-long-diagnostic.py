import json,pathlib,subprocess,urllib.request
root=pathlib.Path('/opt/transparent-publisher-build/loading-diagnostics-20260911')
unit='transparent-m1-loading-long-diagnostic.service'
ssh=['ssh','-i','/opt/transparent-publisher/credentials/deploy-ssh','-o','UserKnownHostsFile=/opt/transparent-publisher/credentials/known_hosts','root@10.142.0.10']
def state(name,remote=False):
 return subprocess.check_output((ssh if remote else [])+['systemctl','show',name,'-p','ActiveState','--value'],text=True).strip()
assert state('transparent-m1-loading-diagnostic.service')=='inactive'
assert state('transparent-m1-loading-fd-sampler.service',True)=='inactive'
assert state(unit) not in ('active','activating','deactivating')
assert state('transparent-m1-loading-long-sampler.service',True) not in ('active','activating','deactivating')
assert not (root/'diagnostic-long').exists()
assert json.loads((root/'diagnostic/result.json').read_text())['passed']
sha='ddd0259af5b0893e60cecb34fd62cacf03b797788ea9387b37a470d392a67f05'
with urllib.request.urlopen('http://10.142.0.10:8093/v1/ready',timeout=10) as r:ready=json.load(r)
assert ready['ready'] and ready['binary_sha256']==sha
sampler=(root/'long-sampler.py').read_text()
subprocess.run(ssh+['test','!','-e','/tmp/m1-loading-long-fd-samples.ndjson'],check=True)
subprocess.run(ssh+['tee','/tmp/m1-loading-long-sampler.py'],input=sampler,text=True,stdout=subprocess.DEVNULL,check=True)
subprocess.run(ssh+['systemd-run','--unit=transparent-m1-loading-long-sampler','--property=RuntimeMaxSec=14520','--property=Restart=no','--property=StandardOutput=append:/tmp/m1-loading-long-fd-samples.ndjson','python3','/tmp/m1-loading-long-sampler.py'],check=True)
subprocess.run(['systemd-run','--unit='+unit,'--property=RuntimeMaxSec=14520','--property=Restart=no','python3',str(root/'ops/scripts/observe-transparent-hardening.py'),'--worker','transparent-pir-recent-01','--binary-sha256',sha,'--query-binary',str(root/'artifacts/soak-query'),'--seconds','14400','--blocks','1','--out',str(root/'diagnostic-long')],check=True)
print(subprocess.check_output(['systemctl','show',unit,'-p','MainPID','-p','ActiveState'],text=True))
print(subprocess.check_output(ssh+['systemctl','show','transparent-m1-loading-long-sampler.service','-p','MainPID','-p','ActiveState'],text=True))
