import hashlib,json,os,shutil,subprocess
from pathlib import Path
root=Path('/opt/transparent-publisher-build/collection-20260910');os.chdir(root)
artifacts=root/'artifacts'
for line in Path('portable-tests.jsonl').read_text().splitlines():
 row=json.loads(line)
 if row.get('executable') and row['profile'].get('test') and row['target']['kind']==['lib'] and row['target']['name']=='transparent_shard_server':
  shutil.copy2(row['executable'],artifacts/'lib-test');break
else:raise RuntimeError('missing library test executable')
with open('linux-tests-v2.log','w') as out:
 for filt in ['memory::tests','runtime::disk::cache_integration_tests']:
  subprocess.run([str(artifacts/'lib-test'),filt,'--nocapture'],stdout=out,stderr=subprocess.STDOUT,check=True)
env=dict(os.environ,CARGO_TARGET_DIR='/opt/transparent-publisher-build/6e0c65c/target',RUSTFLAGS='-C target-cpu=x86-64-v3 -C target-feature=+pclmulqdq',CARGO_BUILD_JOBS='2')
with open('worker.jsonl','w') as out:
 subprocess.run(['/root/.cargo/bin/cargo','build','--locked','--release','-p','transparent-shard-server','--bin','transparent-shard-server','--example','soak-query','--message-format=json'],stdout=out,env=env,check=True)
for line in Path('worker.jsonl').read_text().splitlines():
 row=json.loads(line)
 if row.get('executable') and row['target']['name'] in ('transparent-shard-server','soak-query'):
  shutil.copy2(row['executable'],artifacts/row['target']['name'])
for name in ('lib-test','burst-test','transparent-shard-server','soak-query'):assert (artifacts/name).is_file(),name
(root/'artifact-sha256.json').write_text(json.dumps({p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in artifacts.iterdir()},indent=2)+'\n')
print('Linux builds and targeted tests passed',flush=True)
