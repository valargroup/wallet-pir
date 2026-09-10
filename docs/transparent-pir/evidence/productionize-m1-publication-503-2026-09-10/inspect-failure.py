import asyncio,json,importlib.util,shlex,subprocess
from pathlib import Path
spec=importlib.util.spec_from_file_location('fleet','/opt/transparent-publisher/transparent-live-fleet.py');m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
out=Path('/opt/transparent-publisher-build/querywait-20260909/failure-investigation');out.mkdir(exist_ok=True)
def journal(*units):return shlex.join(['journalctl',*[v for u in units for v in ['-u',u]],'--since','2026-09-09 22:01:30 UTC','--until','2026-09-09 22:05:00 UTC','--no-pager','-o','short-iso-precise'])
async def main():
 f=m.Fleet(json.loads(Path('/opt/transparent-publisher/fleet.json').read_text()))
 jobs=[('router.log',f.c['router_host'],journal('caddy')),('router-config.txt',f.c['router_host'],'cat /etc/caddy/Caddyfile'),('recent-01.log',next(w for w in f.roster if w['id']=='transparent-pir-recent-01')['ssh_host'],journal('transparent-shard-server'))]
 for name,host,cmd in jobs:
  (out/name).write_bytes(await f.ssh(host,cmd))
 for unit in ['transparent-publish-controller','transparent-replica-reconciler','transparent-filter-server']:
  (out/(unit+'.log')).write_bytes(subprocess.check_output(shlex.split(journal(unit))))
 for name in ['active.json','withdrawn.json','maintenance.json']:
  p=f.root/name
  if p.exists():(out/('current-'+name)).write_bytes(p.read_bytes())
 print('captured',str(out))
asyncio.run(main())
