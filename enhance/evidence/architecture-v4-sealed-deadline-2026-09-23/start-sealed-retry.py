import subprocess,json,shlex,sys
from pathlib import Path
root=Path(__file__).resolve().parent
ssh=['ssh','-o','BatchMode=yes','-o','StrictHostKeyChecking=yes','-i',str(Path.home()/'.ssh/id_ed25519')]
coor='root@167.99.42.60';workers=['root@10.142.0.15','root@10.142.0.16']
release='/opt/enhance-pir-v4/releases/b1863b1d270df52d213d7dd389e5b8bf96bca224'
def run(host,script):
 p=subprocess.run(ssh+([] if host==coor else ['-J',coor])+[host,'sh','-se'],input=script,text=True,capture_output=True,timeout=300)
 if p.returncode:raise RuntimeError(p.stderr+p.stdout)
 print(host,p.stdout.strip(),flush=True)
 return p.stdout
# Check freshness and space before disrupting canonical serving.
for host in workers:
 run(host,'''test ! -e /srv/enhance-pir-v4/worker.canonical
test ! -e /srv/enhance-pir-v4/worker.sealed-retry-completed
test ! -e /srv/enhance-pir-v4/validation/sealed-retry-samples
python3 - <<'INNER'
from pathlib import Path
import json,shutil
p=Path('/srv/enhance-pir-v4/worker.sealed-completed')
assert p.is_dir() and not p.is_symlink()
v=json.loads((p/'worker-v4.json').read_text())
assert not v['published'] and v.get('candidate') is not None
assert v['candidate']['generation']==1
shutil.rmtree(p)
assert shutil.disk_usage('/srv/enhance-pir-v4').free >= 32*1024**3
print('Failed sealed fixture archive removed; at least 32 GiB free.')
INNER
''')
config={'groups':[{'name':'shard-group-01','replicas':[{'name':'enhance-pir-worker-01','url':'http://10.142.0.15:8091'},{'name':'enhance-pir-worker-02','url':'http://10.142.0.16:8091'}]},{'name':'test-support-active','replicas':[{'name':'coordinator-helper-1','url':'http://127.0.0.1:8191'},{'name':'coordinator-helper-2','url':'http://127.0.0.1:8192'}]}]}
run(coor,"test ! -e /srv/enhance-pir-v4/validation/sealed-retry\n"+"cat > /etc/enhance-pir-v4/sealed-retry-workers.json <<'CONFIG'\n"+json.dumps(config)+"\nCONFIG\n")
for n in [1,2]:
 run(coor,f'''test ! -e /srv/enhance-pir-v4/sealed-retry-helper-{n}
install -d -m 0700 /srv/enhance-pir-v4/sealed-retry-helper-{n}
systemd-run --no-block --unit=enhance-pir-v4-sealed-retry-helper-{n} --property=MemoryMax=8G --property=CPUQuota=200% {release}/enhance-pir-v4 worker --listen 127.0.0.1:{8190+n} --data-dir /srv/enhance-pir-v4/sealed-retry-helper-{n}
''')
run(coor,'systemctl stop enhance-pir-v4-coordinator.service\n')
for host in workers:
 run(host,'''systemctl stop enhance-pir-v4-worker.service
mv /srv/enhance-pir-v4/worker /srv/enhance-pir-v4/worker.canonical
install -d -m 0700 -o enhance-pir-v4 -g enhance-pir-v4 /srv/enhance-pir-v4/worker
systemctl start enhance-pir-v4-worker.service
systemd-run --no-block --unit=enhance-pir-v4-sealed-retry-sampling --property=MemoryMax=256M /usr/bin/python3 /opt/enhance-pir-v4/sample-v4-loop.py --output /srv/enhance-pir-v4/validation/sealed-retry-samples --seconds 32400 --interval 1
''')
run(coor,f'''systemd-run --no-block --unit=enhance-pir-v4-sealed-retry-campaign --property=MemoryMax=24G --property=CPUQuota=400% --property=RuntimeMaxSec=8h {release}/enhance-pir-v4 exercise --isolated-workers --profile sealed --data-dir /srv/enhance-pir-v4/validation/sealed-retry --worker-config /etc/enhance-pir-v4/sealed-retry-workers.json --listen 127.0.0.1:8280 --seconds 21600 --min-publications 300 --publication-interval 60 --concurrency 2
systemctl show enhance-pir-v4-sealed-retry-campaign.service -p ActiveState -p MainPID
''')
with (root/'finish-sealed-retry.log').open('ab') as log:
 p=subprocess.Popen(['/usr/bin/caffeinate','-i','python3','-u',str(root/'finish-sealed-retry.py')],stdin=subprocess.DEVNULL,stdout=log,stderr=log,start_new_session=True)
(root/'finish-sealed-retry.pid').write_text(str(p.pid)+'\n')
print('Sealed supervisor PID',p.pid,flush=True)
