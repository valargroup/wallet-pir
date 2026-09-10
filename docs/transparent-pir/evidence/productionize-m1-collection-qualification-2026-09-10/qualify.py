import hashlib,shlex,subprocess
from pathlib import Path
root=Path('/opt/transparent-publisher-build/collection-20260910')
host='root@10.142.0.13'
remote='/opt/transparent-collection-20260910'
ssh=['ssh','-o','BatchMode=yes','-o','UserKnownHostsFile=/tmp/transparent-loadgen-known-hosts','-o','StrictHostKeyChecking=yes','-i','/opt/transparent-publisher/credentials/deploy-ssh']
def run(command):return subprocess.check_output(ssh+[host,command],text=True)
active=run('systemctl list-units --state=running --no-legend "transparent*"')
assert not active.strip(), 'generator has active transparent jobs'
run('mkdir '+remote)
for src,dst in [('/tmp/m1-collection-source.tar.gz',remote+'/source.tar.gz'),(str(root/'artifacts/burst-test'),remote+'/burst-test')]:
 subprocess.run(['rsync','-a','-e',shlex.join(ssh),src,host+':'+dst],check=True)
expected=hashlib.sha256((root/'artifacts/burst-test').read_bytes()).hexdigest()
assert run('sha256sum '+remote+'/burst-test').split()[0]==expected
run('tar xzf '+remote+'/source.tar.gz -C '+remote)
cmd=['systemd-run','--unit=transparent-m1-collection-qualification','--property=WorkingDirectory='+remote,'/usr/bin/python3','ops/scripts/run-transparent-burst.py','--systemd','--external-clients','--worker-budget-seconds','14','--host-overhead-bytes','805306368','--fixture','/opt/transparent-full-fixture-20260909/fixture-canonical.json','--test-binary',remote+'/burst-test','--source-sha','d8f5217','--build-slots','2','--repetitions','3','--out',remote+'/qualification']
print(run(shlex.join(cmd)),flush=True)
print('Qualification started for burst-test SHA256 '+expected,flush=True)
