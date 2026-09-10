import hashlib, json, os, shutil, subprocess
from pathlib import Path
root=Path('/opt/transparent-publisher-build/publication-io-20260910')
os.chdir(root)
manifest=json.loads(Path('source-manifest.json').read_text())
for name,digest in manifest.items():
 assert hashlib.sha256(Path(name).read_bytes()).hexdigest()==digest,name
artifacts=root/'artifacts';artifacts.mkdir()
env=dict(os.environ,CARGO_TARGET_DIR='/opt/transparent-publisher-build/6e0c65c/target',RUSTFLAGS='-C target-cpu=x86-64-v3 -C target-feature=+pclmulqdq',CARGO_BUILD_JOBS='2')
base=['/root/.cargo/bin/cargo']
def build(name,args):
 with open(name+'.jsonl','w') as out:
  subprocess.run(base+args+['--message-format=json'],stdout=out,env=env,check=True)
 return [json.loads(line) for line in Path(name+'.jsonl').read_text().splitlines() if line.startswith('{')]
rows=build('portable-tests',['test','--locked','--release','-p','transparent-shard-server','--features','portable-kernel','--lib','--test','revisions_and_cache','--no-run'])
for row in rows:
 if row.get('executable') and row['profile'].get('test') and row['target']['name'] in ('revisions_and_cache','transparent_shard_server'):
  name='burst-test' if row['target']['name']=='revisions_and_cache' else 'lib-test'
  shutil.copy2(row['executable'],artifacts/name)
with open('linux-tests.log','w') as out:
 for executable,filters in [('burst-test',['blocked_', 'live_prepare_activate_and_invalidate']),('lib-test',['memory::tests','runtime::disk::cache_integration_tests'])]:
  for filt in filters:
   subprocess.run([str(artifacts/executable),filt,'--nocapture'],stdout=out,stderr=subprocess.STDOUT,check=True)
rows=build('worker',['build','--locked','--release','-p','transparent-shard-server','--bin','transparent-shard-server','--example','soak-query'])
for row in rows:
 if row.get('executable'):
  shutil.copy2(row['executable'],artifacts/row['target']['name'])
(root/'artifact-sha256.json').write_text(json.dumps({p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in artifacts.iterdir()},indent=2)+'\n')
print('Linux builds and targeted tests passed',flush=True)
