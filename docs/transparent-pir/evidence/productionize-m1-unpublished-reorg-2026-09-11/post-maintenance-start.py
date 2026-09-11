import asyncio,datetime,hashlib,importlib.util,json,subprocess,urllib.request
from pathlib import Path
root=Path('/opt/transparent-publisher-build/unpublished-reorg-20260911')
live=Path('/opt/transparent-publisher')
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def fetch(url):
 with urllib.request.urlopen(url,timeout=5) as r:return json.load(r)
assert not subprocess.check_output(['systemctl','list-units','--state=running','--no-legend','transparent*rollout*'],text=True).strip()
expected={'/usr/local/bin/transparent-publish-controller':'b191c25abb027378b564d32d62c23c3aee52c300b5506cc0ab259cde57eddcf4','/opt/transparent-publisher/transparent-live-fleet.py':'291b01f320267aa140a00e6b17369481aeb4299af483a94de17b87614fba6e5c','/opt/transparent-publisher/fleet.json':'ad6048bd12d3472fa5a9632ff275544b360e9509334ddc0a7934580fae22015f','/opt/transparent-publisher/controller.json':'5ce43599786b177bb26bfdb991ae07eafa880855294c8053a0fada38d73c7da3'}
for p,d in expected.items():assert sha(p)==d,p
for p,d in json.loads((root/'source-manifest.json').read_text()).items():assert sha(root/p)==d,p
binary='b2da68f289e4eebd3eafbc5b763963fa87c3daf347b8b2d312e1ac64ea97a38c'
assert sha(root/'artifacts/transparent-shard-server')==binary
spec=importlib.util.spec_from_file_location('fleet',root/'ops/scripts/transparent-live-fleet.py');module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
fleet=module.Fleet(json.loads((live/'fleet.json').read_text()))
s=fetch('http://127.0.0.1:8094/v1/status');r=fetch('http://10.142.0.10:8093/v1/ready')
a=fetch('https://transparent-pir.valargroup.dev/v1/shards');b=fetch('https://enhance-pir.valargroup.dev/v1/filters/shards')
assert a==b
assert s['phase']=='serving' and r['ready'] and r['binary_sha256']==binary
assert r['map_sha256']==s['map_sha256']
assert a['shards'][-1]['end_height']==s['public_height']
assert a['shards'][-1]['terminal_block_hash']==s['public_hash']
assert asyncio.run(fleet.canonical_hash(s['public_height']))==s['public_hash']
assert asyncio.run(fleet.node_height())==s['public_height']
record={'utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'digests':expected,'status':s,'ready':r,'prior_run':'rollout','out':'rollout-post-maintenance','source_sha':'862239c'}
(root/'post-maintenance-preflight.json').write_text(json.dumps(record,indent=2)+'\n')
subprocess.run(['systemd-run','--unit=transparent-m1-post-maintenance-rollout','/usr/bin/python3',str(root/'ops/scripts/run-transparent-hardening-rollout.py'),'--artifacts',str(root/'artifacts'),'--source-sha','862239c','--observe-installed-canary','--out',str(root/'rollout-post-maintenance')],check=True)
print(json.dumps({'started':True,'public_height':s['public_height'],'worker_binary_sha256':binary}))
