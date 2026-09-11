import asyncio,datetime,hashlib,importlib.util,json,os,shutil,subprocess,time,urllib.request
from pathlib import Path
root=Path('/opt/transparent-publisher-build/unpublished-reorg-20260911')
live=Path('/opt/transparent-publisher')
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def unit(name):return subprocess.check_output(['systemctl','show',name,'-p','MainPID','-p','ActiveState','-p','ExecMainStatus'],text=True)
assert 'ActiveState=inactive' in unit('transparent-m1-unpublished-reorg-build.service')
assert 'ExecMainStatus=0' in unit('transparent-m1-unpublished-reorg-build.service')
assert not subprocess.check_output(['systemctl','list-units','--state=running','--no-legend','transparent*rollout*'],text=True).strip()
for name,digest in json.loads((root/'source-manifest.json').read_text()).items():assert sha(root/name)==digest,name
artifacts=root/'artifacts'
for name,digest in json.loads((root/'artifact-sha256.json').read_text()).items():assert sha(artifacts/name)==digest,name
binary=Path('/usr/local/bin/transparent-publish-controller');script=live/'transparent-live-fleet.py';config=live/'controller.json'
assert sha(binary)=='a9ca07ee752b54e17e7dc854c2a8af5bb5338be9e36c3ebe965b6be9462488ab'
assert sha(script)=='b31402a14c85f3a10beba6f58e76f5cc72134154671c6ba1b32aa2e44fb95427'
assert sha(config)=='02f6277149ecc965a68b637afbc523c1ea77d8747721d335612c44fda56bf2ff'
assert sha(live/'fleet.json')=='ad6048bd12d3472fa5a9632ff275544b360e9509334ddc0a7934580fae22015f'
backup=root/'deployment-backup';backup.mkdir()
for p in [binary,script,config]:shutil.copy2(p,backup/p.name)
expected={'transparent-shard-server':'b2da68f289e4eebd3eafbc5b763963fa87c3daf347b8b2d312e1ac64ea97a38c','soak-query':'3048230c644e3e98831b9e46f52ec8c9d02382c75f7a3c69e58f4c277226771f','shard-control':'c46543dd79ed66dfff72fd846b852fb84961532f97809068c200cd3a022a47dd'}
for name,digest in expected.items():
 p=Path('/opt/transparent-publisher-build/incremental-writeback-20260911/artifacts')/name
 assert sha(p)==digest
 shutil.copy2(p,artifacts/name)
def replace(source,target):
 temporary=target.with_name(target.name+'.unpublished-next')
 shutil.copy2(source,temporary);os.replace(temporary,target)
spec=importlib.util.spec_from_file_location('fleet',root/'ops/scripts/transparent-live-fleet.py');module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
def fetch(url):
 with urllib.request.urlopen(url,timeout=5) as response:return json.load(response)
record={'source_sha':'862239c','worker_source_sha':'7621c34','started_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'before':{p.name:sha(p) for p in [binary,script,config]},'worker_binary_sha256':expected['transparent-shard-server'],'fleet_config_sha256':sha(live/'fleet.json')}
(root/'deployment-start.json').write_text(json.dumps(record,indent=2)+'\n')
services=['transparent-publish-controller','transparent-replica-reconciler']
try:
 subprocess.run(['systemctl','stop',*services],check=True)
 replace(artifacts/'transparent-publish-controller',binary)
 replace(root/'ops/scripts/transparent-live-fleet.py',script)
 value=json.loads(config.read_text());value['source_sha']='862239c'
 next_config=root/'controller-next.json';next_config.write_text(json.dumps(value,indent=2)+'\n');next_config.chmod(config.stat().st_mode & 0o777)
 replace(next_config,config)
 subprocess.run(['systemctl','start','transparent-replica-reconciler','transparent-publish-controller'],check=True)
 deadline=time.monotonic()+300
 while True:
  try:
   status=fetch('http://127.0.0.1:8094/v1/status')
   ready=fetch('http://10.142.0.10:8093/v1/ready')
   first=fetch('https://transparent-pir.valargroup.dev/v1/shards')
   second=fetch('https://enhance-pir.valargroup.dev/v1/filters/shards')
   assert first==second
   fleet=module.Fleet(json.loads((live/'fleet.json').read_text()))
   assert asyncio.run(fleet.node_height())==status['public_height']
   assert asyncio.run(fleet.canonical_hash(status['public_height']))==status['public_hash']
   assert first['shards'][-1]['end_height']==status['public_height']
   assert first['shards'][-1]['terminal_block_hash']==status['public_hash']
   assert status['phase']=='serving' and ready['ready'] and ready['map_sha256']==status['map_sha256']
   assert ready['binary_sha256']==expected['transparent-shard-server']
   break
  except Exception as e:
   if time.monotonic()>=deadline:raise
   print('Waiting for matching warm public service:',type(e).__name__,flush=True);time.sleep(2)
 record.update(completed_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),after={p.name:sha(p) for p in [binary,script,config]},status=status,ready=ready)
 (root/'deployment-result.json').write_text(json.dumps(record,indent=2)+'\n')
except BaseException:
 subprocess.run(['systemctl','stop',*services],check=True)
 for p in [binary,script,config]:replace(backup/p.name,p)
 subprocess.run(['systemctl','start','transparent-replica-reconciler','transparent-publish-controller'],check=True)
 raise
subprocess.run(['systemd-run','--unit=transparent-m1-unpublished-reorg-rollout','/usr/bin/python3',str(root/'ops/scripts/run-transparent-hardening-rollout.py'),'--artifacts',str(artifacts),'--source-sha','862239c','--observe-installed-canary','--out',str(root/'rollout')],check=True)
print('Matching warm service verified; fresh canary supervisor started.',flush=True)
