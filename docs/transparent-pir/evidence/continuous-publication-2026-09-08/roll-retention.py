import asyncio,datetime,hashlib,importlib.util,json,pathlib,sys
BASE=pathlib.Path('/opt/transparent-publisher-build/6e0c65c')
spec=importlib.util.spec_from_file_location('deploy',BASE/'ops/scripts/deploy-transparent-publisher.py');d=importlib.util.module_from_spec(spec);spec.loader.exec_module(d)
ROOT=pathlib.Path('/opt/transparent-publisher')
def emit(**kw): print(json.dumps(dict(utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),**kw)),flush=True)
async def main():
 f=d.LIVE.Fleet(json.loads((ROOT/'fleet.json').read_text()))
 expected=hashlib.sha256((BASE/'artifacts/transparent-shard-server').read_bytes()).hexdigest()
 args=sys.argv[1:];allow_routed='--allow-routed' in args;parallel='--parallel' in args
 async def upgrade(name):
  w=next(w for w in f.roster if w['id']==name)
  route=(await f.ssh(f.c['router_host'],'cat /etc/caddy/Caddyfile')).decode()
  assert allow_routed or w['upstream'] not in route,'refuse to restart a publicly routed worker without an explicit maintenance selection'
  emit(event='upgrade_start',worker=name,source_sha='6e0c65c1b2cb4f4e4bae1fb4e1b34e282ea19ea2')
  await d.install_worker(f,w,BASE/'artifacts','/opt/transparent-publisher/rollback/6e0c65c1b2cb4f4e4bae1fb4e1b34e282ea19ea2')
  ready=d.read_json('http://'+w['upstream']+'/v1/ready')
  assert ready['binary_sha256']==expected
  emit(event='upgrade_complete',worker=name,ready=ready)
 names=[name for name in args if not name.startswith('--')]
 if parallel: await asyncio.gather(*(upgrade(name) for name in names))
 else:
  for name in names: await upgrade(name)
asyncio.run(main())
