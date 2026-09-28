#!/usr/bin/env python3
"""Restore the v9 schema cutover snapshot; retain the failed v10 state for diagnosis."""
import concurrent.futures,datetime,json,pathlib,shutil,subprocess
root=pathlib.Path('/opt/transparent-v10-8e69ea75'); saved=root/'rollback-v9'
assert saved.exists()
fleet=json.loads((saved/'fleet.json').read_text());roster=json.loads((saved/'roster.json').read_text())
subprocess.run(['systemctl','stop','transparent-publish-controller','transparent-replica-reconciler','transparent-control-sessions'],check=True)
stamp=datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')
def worker(w):
 remote='/opt/transparent-publisher/rollback-v9-8e69ea75'
 script='set -eu; systemctl stop transparent-shard-server; if test -d /srv/transparent-pir/publications-v9-8e69ea75; then if test -e /srv/transparent-pir/publications; then mv /srv/transparent-pir/publications /srv/transparent-pir/publications-v10-failed-'+stamp+'; fi; mv /srv/transparent-pir/publications-v9-8e69ea75 /srv/transparent-pir/publications; fi; if test -f /opt/transparent-publisher/active.invalid.json; then mv /opt/transparent-publisher/active.invalid.json /opt/transparent-publisher/active.invalid.json.v10-failed-'+stamp+'; fi; install -m755 '+remote+'/transparent-shard-server /usr/local/bin/transparent-shard-server.next; mv /usr/local/bin/transparent-shard-server.next /usr/local/bin/transparent-shard-server; install -m755 '+remote+'/shard-control /usr/local/bin/shard-control; install -m644 '+remote+'/transparent-shard-server.service /etc/systemd/system/transparent-shard-server.service; for file in /usr/local/bin/shard-prune /opt/transparent-pir/current-release /opt/transparent-pir/current-unit-digest /opt/transparent-publisher/active.invalid.json; do name=$(basename $file); if test -f '+remote+'/$name; then cp -p '+remote+'/$name $file; fi; done; if test -f '+remote+'/active.json; then cp '+remote+'/active.json /opt/transparent-publisher/active.json; fi; systemctl daemon-reload; systemctl start transparent-shard-server'
 args=['ssh','-i',fleet['ssh_key'],'-o','BatchMode=yes','-o','IdentitiesOnly=yes','-o','StrictHostKeyChecking=yes','-o','UserKnownHostsFile='+fleet['known_hosts'],'root@'+w['ssh_host'],script]
 subprocess.run(args,check=True)
with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool: list(pool.map(worker,roster))
for name in ['transparent-filter-server','transparent-publish-controller','shard-assign','shard-control']:
 p=saved/name
 if p.exists():
  dest=pathlib.Path('/usr/local/bin')/name
  shutil.copy2(p,str(dest)+'.next');pathlib.Path(str(dest)+'.next').replace(dest)
for name in ['controller.json','fleet.json','roster.json','transparent-live-fleet.py']:
 shutil.copy2(saved/name,pathlib.Path('/opt/transparent-publisher')/name)
for name in ['transparent-filter-server','transparent-publish-controller','transparent-replica-reconciler','transparent-control-sessions']:
 p=saved/(name+'.service')
 if p.exists(): shutil.copy2(p,pathlib.Path('/etc/systemd/system')/p.name)
stamp=datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')
old=pathlib.Path('/srv/zakura/transparent-publications-v9-8e69ea75')
active=pathlib.Path('/srv/zakura/transparent-publications')
if old.exists():
 if active.exists(): active.rename('/srv/zakura/transparent-publications-v10-failed-'+stamp)
 old.rename(active)
state=pathlib.Path('/opt/transparent-publisher/state')
state.rename('/opt/transparent-publisher/state-v10-failed-'+stamp)
shutil.copytree(saved/'state',state)
shutil.copy2(saved/'Caddyfile.coordinator','/etc/caddy/Caddyfile')
router=['ssh','-i',fleet['ssh_key'],'-o','BatchMode=yes','-o','IdentitiesOnly=yes','-o','StrictHostKeyChecking=yes','-o','UserKnownHostsFile='+fleet['known_hosts'],'root@'+fleet['router_host'],'cat > /etc/caddy/Caddyfile.v9-restore && caddy validate --config /etc/caddy/Caddyfile.v9-restore --adapter caddyfile && mv /etc/caddy/Caddyfile.v9-restore /etc/caddy/Caddyfile && systemctl reload caddy']
subprocess.run(router,input=(saved/'Caddyfile.router').read_bytes(),check=True)
subprocess.run(['systemctl','daemon-reload'],check=True)
subprocess.run(['systemctl','restart','transparent-filter-server','transparent-publish-controller','transparent-replica-reconciler','transparent-control-sessions'],check=True)
subprocess.run(['systemctl','reload','caddy'],check=True)
print('v9 restored; inspect worker warmth, controller canonicality and both origins before declaring recovery')
