#!/usr/bin/env python3
"""Capture schema-cutover rollback material; stop publisher only after readiness gates."""
import concurrent.futures,datetime,json,os,pathlib,shutil,stat,subprocess
root=pathlib.Path('/opt/transparent-v10-8e69ea75')
backup=root/'rollback-v9'
if backup.exists(): raise RuntimeError('rollback directory already exists; inspect before repeating')
assert json.loads((root/'qualification/complete.json').read_text())['source_sha']=='8e69ea75b1e0071e3b978b0c78cc0487377f9e82'
assert (root/'fleet-preflight.complete').exists()
backup.mkdir(mode=0o700)
fleet=json.loads(pathlib.Path('/opt/transparent-publisher/fleet.json').read_text())
roster=json.loads(pathlib.Path('/opt/transparent-publisher/roster.json').read_text())
for name in ['transparent-filter-server','transparent-publish-controller','shard-assign','shard-control']:
 p=pathlib.Path('/usr/local/bin')/name
 if p.exists(): shutil.copy2(p,backup/name)
for name in ['controller.json','fleet.json','roster.json','transparent-live-fleet.py']:
 shutil.copy2(pathlib.Path('/opt/transparent-publisher')/name,backup/name)
for name in ['transparent-filter-server','transparent-publish-controller','transparent-replica-reconciler','transparent-control-sessions']:
 p=pathlib.Path('/etc/systemd/system')/(name+'.service')
 if p.exists(): shutil.copy2(p,backup/p.name)
shutil.copy2('/etc/caddy/Caddyfile',backup/'Caddyfile.coordinator')

def ssh(w,command):
 args=['ssh','-i',fleet['ssh_key'],'-o','BatchMode=yes','-o','IdentitiesOnly=yes','-o','StrictHostKeyChecking=yes','-o','UserKnownHostsFile='+fleet['known_hosts'],'root@'+w['ssh_host'],command]
 subprocess.run(args,check=True)

router=['ssh','-i',fleet['ssh_key'],'-o','BatchMode=yes','-o','IdentitiesOnly=yes','-o','StrictHostKeyChecking=yes','-o','UserKnownHostsFile='+fleet['known_hosts'],'root@'+fleet['router_host'],'cat /etc/caddy/Caddyfile']
(backup/'Caddyfile.router').write_bytes(subprocess.check_output(router))

def worker_backup(w):
 path='/opt/transparent-publisher/rollback-v9-8e69ea75'
 ssh(w,'set -eu; test ! -e '+path+'; mkdir -m700 '+path+'; cp /usr/local/bin/transparent-shard-server '+path+'/; cp /usr/local/bin/shard-control '+path+'/; cp /etc/systemd/system/transparent-shard-server.service '+path+'/; for file in /usr/local/bin/shard-prune /opt/transparent-pir/current-release /opt/transparent-pir/current-unit-digest /opt/transparent-publisher/active.invalid.json; do if test -f $file; then cp $file '+path+'/; fi; done; if test -e /opt/transparent-publisher/active.json; then cp /opt/transparent-publisher/active.json '+path+'/; fi')
subprocess.run(['systemctl','stop','transparent-replica-reconciler','transparent-publish-controller','transparent-control-sessions'],check=True)
(root/'cutover-start.txt').write_text(datetime.datetime.now(datetime.timezone.utc).isoformat()+'\n')
try:
 with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool: list(pool.map(worker_backup,roster))
 shutil.copytree('/opt/transparent-publisher/state',backup/'state',ignore=lambda directory,names: [name for name in names if stat.S_ISSOCK(os.lstat(os.path.join(directory,name)).st_mode)])
 shutil.copy2('/srv/zakura/transparent-publications/active.json',backup/'publication-active.json')
except BaseException:
 subprocess.run(['systemctl','start','transparent-publish-controller','transparent-replica-reconciler','transparent-control-sessions'],check=True)
 raise
print('V9 rollback captured; publisher stopped, workers still serve v9',flush=True)
