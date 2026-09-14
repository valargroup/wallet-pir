import hashlib,shlex,subprocess
from pathlib import Path
root=Path('/opt/transparent-publisher-build/shoup-choice-20260911')
host='root@10.142.0.13';remote='/opt/transparent-shoup-choice-qualification-20260911'
ssh=['ssh','-o','ConnectTimeout=10','-o','BatchMode=yes','-o','UserKnownHostsFile=/tmp/transparent-loadgen-known-hosts','-o','StrictHostKeyChecking=yes','-i','/opt/transparent-publisher/credentials/deploy-ssh']
def run(c):return subprocess.check_output(ssh+[host,c],text=True)
assert '1 passed; 0 failed' in (root/'linux-live-integration.log').read_text()
assert not run('systemctl list-units --state=running --no-legend "transparent*burst*" "transparent*qualification*" "transparent*cpu-profile*"').strip()
run('mkdir '+remote)
subprocess.run(['tar','-czf',str(root/'candidate-source.tar.gz'),'-C',str(root),'Cargo.toml','Cargo.lock','pir','server','ops','dependencies','dependency-manifest.json','build-provenance.json','artifact-sha256.json','linux-reduction-tests.log','linux-live-integration.log'],check=True)
for src,dst in [(root/'candidate-source.tar.gz',remote+'/source.tar.gz'),(root/'artifacts/worker-integration-tests',remote+'/burst-test')]:subprocess.run(['rsync','-a','-e',shlex.join(ssh),str(src),host+':'+dst],check=True)
assert run('sha256sum '+remote+'/burst-test').split()[0]==hashlib.sha256((root/'artifacts/worker-integration-tests').read_bytes()).hexdigest()
run('tar xzf '+remote+'/source.tar.gz -C '+remote)
cmd=['systemd-run','--unit=transparent-m1-shoup-choice-qualification','--property=WorkingDirectory='+remote,'/usr/bin/python3','ops/scripts/run-transparent-burst.py','--systemd','--external-clients','--worker-budget-seconds','14','--host-overhead-bytes','805306368','--fixture','/opt/transparent-full-fixture-20260909/fixture-canonical.json','--test-binary',remote+'/burst-test','--source-sha','3d13da6+shoup-choice-patch','--build-slots','2','--repetitions','3','--out',remote+'/qualification']
print(run(shlex.join(cmd)),flush=True)
