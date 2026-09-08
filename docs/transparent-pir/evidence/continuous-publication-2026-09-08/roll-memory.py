import asyncio,datetime,importlib.util,json,pathlib,sys
BASE=pathlib.Path('/opt/transparent-publisher-build/a671e4e')
spec=importlib.util.spec_from_file_location('deploy',BASE/'ops/scripts/deploy-transparent-publisher.py');d=importlib.util.module_from_spec(spec);spec.loader.exec_module(d)
ROOT=pathlib.Path('/opt/transparent-publisher')
def emit(**kw):print(json.dumps(dict(utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),**kw)),flush=True)
async def main():
 f=d.LIVE.Fleet(json.loads((ROOT/'fleet.json').read_text()))
 for name in sys.argv[1:]:
  w=next(w for w in f.roster if w['id']==name)
  route=(await f.ssh(f.c['router_host'],'cat /etc/caddy/Caddyfile')).decode()
  assert w['upstream'] not in route,'refuse to restart a publicly routed worker'
  emit(event='upgrade_start',worker=name)
  await d.install_worker(f,w,BASE/'artifacts','/opt/transparent-publisher/rollback/a671e4eeeb9bfbcb72654004635a749c7eb076d4')
  ready=d.read_json('http://'+w['upstream']+'/v1/ready')
  assert ready['binary_sha256']=='330741c504f92938f2ca0a8d1b45efdd2b973f916f2ff0264b9dd668c237923f'
  emit(event='upgrade_complete',worker=name,ready=ready)
asyncio.run(main())
