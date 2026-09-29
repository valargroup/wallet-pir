#!/usr/bin/env python3
"""Prepare the new publisher namespace after the fixed fleet has switched."""
import concurrent.futures,json,pathlib,shutil,subprocess
root=pathlib.Path('/opt/transparent-v10-8e69ea75')
sha='8e69ea75b1e0071e3b978b0c78cc0487377f9e82'
assert (root/'candidate-storage.complete').read_text().strip()==sha
assert (root/'rollback-v9/publication-active.json').exists()
for unit in ['transparent-publish-controller','transparent-replica-reconciler','transparent-control-sessions']:
 assert subprocess.run(['systemctl','is-active','--quiet',unit]).returncode!=0
fleet=json.loads(pathlib.Path('/opt/transparent-publisher/fleet.json').read_text())
roster=json.loads((root/'pre-roster.json').read_text())
def worker(w):
 # The fixed fleet must already be warm on the staged binary.
 import urllib.request
 with urllib.request.urlopen('http://'+w['upstream']+'/v1/ready',timeout=15) as f: r=json.load(f)
 assert r['ready'] and r['binary_sha256']=='0ece0ae1ac7f526c8471b01bb06c35c0138c80eb2c46cbaff992fa237c45f36b'
 args=['ssh','-i',fleet['ssh_key'],'-o','BatchMode=yes','-o','IdentitiesOnly=yes','-o','StrictHostKeyChecking=yes','-o','UserKnownHostsFile='+fleet['known_hosts'],'root@'+w['ssh_host'],'set -eu; test -f /opt/transparent-publisher/rollback-v9-8e69ea75/active.json; test ! -e /opt/transparent-publisher/active.json.v9-8e69ea75; mv /opt/transparent-publisher/active.json /opt/transparent-publisher/active.json.v9-8e69ea75; if test -f /opt/transparent-publisher/active.invalid.json; then mv /opt/transparent-publisher/active.invalid.json /opt/transparent-publisher/active.invalid.json.v9-8e69ea75; fi; test ! -e /srv/transparent-pir/publications-v9-8e69ea75; mv /srv/transparent-pir/publications /srv/transparent-pir/publications-v9-8e69ea75; mkdir /srv/transparent-pir/publications']
 subprocess.run(args,check=True)
with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool: list(pool.map(worker,roster))
active=pathlib.Path('/srv/zakura/transparent-publications')
previous=pathlib.Path('/srv/zakura/transparent-publications-v9-8e69ea75')
new=pathlib.Path('/srv/transparent-data-v10/publications')
assert not previous.exists() and (new/'initial/shards.json').is_file() and not active.is_symlink()
active.rename(previous)
active.symlink_to(new,target_is_directory=True)
assert active.resolve()==new
state=pathlib.Path('/opt/transparent-publisher/state')
oldstate=pathlib.Path('/opt/transparent-publisher/state-v9-8e69ea75')
assert not oldstate.exists()
state.rename(oldstate);state.mkdir(mode=0o700)
probe="import os; s='/srv/zakura/transparent-publications/initial/shards.json'; d='/srv/zakura/transparent-publications/.hardlink-probe'; os.link(s,d); assert os.stat(s).st_ino==os.stat(d).st_ino; os.unlink(d); print('publisher sandbox hard-link probe passed')"
subprocess.run(['systemd-run','--unit','wallet-pir-v10-hardlink-probe','--wait','--pipe','--property=ProtectSystem=strict','--property=ReadWritePaths=/srv/zakura/transparent-publications','/usr/bin/python3','-c',probe],check=True)
binary=pathlib.Path('/usr/local/bin/transparent-filter-server')
shutil.copy2(root/'bin/transparent-filter-server',str(binary)+'.v10-next')
pathlib.Path(str(binary)+'.v10-next').replace(binary)
(root/'cutover-paths.complete').write_text(sha+'\n')
print('v9 namespace retained; new v10 publisher paths and filter binary prepared',flush=True)
