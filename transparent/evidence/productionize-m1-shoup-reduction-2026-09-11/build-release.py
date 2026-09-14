import hashlib,json,os,pathlib,shutil,subprocess,tarfile,re
root=pathlib.Path('/opt/transparent-publisher-build/shoup-release-20260911');root.mkdir()
with tarfile.open('/tmp/m1-shoup-release-source.tar.gz') as t:t.extractall(root,filter='data')
shutil.copy2('/tmp/m1-shoup-release-source-manifest.json',root/'source-manifest.json')
for name,digest in json.loads((root/'source-manifest.json').read_text()).items():
 assert hashlib.sha256((root/name).read_bytes()).hexdigest()==digest,name
os.chdir(root);artifacts=root/'artifacts';artifacts.mkdir()
env=dict(os.environ,CARGO_TARGET_DIR='/opt/transparent-publisher-build/6e0c65c/target',RUSTFLAGS='-C target-cpu=x86-64-v3 -C target-feature=+pclmulqdq',CARGO_BUILD_JOBS='2')
(root/'build-provenance.json').write_text(json.dumps(dict(scope='pinned release build; final commit binding required before deployment',ipir_sha='accc424e879d8da425fa620aad80f0f2c4e0defd',spiral_sha='584fee3893b1d2d17b5d774c83bd801fb70d5ace',rustflags=env['RUSTFLAGS']),indent=2)+'\n')
def build(name,args):
 with open(name+'.jsonl','w') as f:subprocess.run(['/root/.cargo/bin/cargo']+args+['--message-format=json'],env=env,stdout=f,check=True)
 return [json.loads(s) for s in pathlib.Path(name+'.jsonl').read_text().splitlines() if s.startswith('{')]
rows=build('tests',['test','--locked','--release','-p','transparent-shard-server','--lib','--test','revisions_and_cache','--no-run'])
for row in rows:
 if row.get('executable') and row.get('profile',{}).get('test'):
  name='worker-integration-tests' if row['target']['name']=='revisions_and_cache' else 'worker-library-tests'
  shutil.copy2(row['executable'],artifacts/name)
for name in ['worker-library-tests','worker-integration-tests']:
 with open('linux-'+name+'.log','w') as f:subprocess.run([str(artifacts/name),'--test-threads=2'],stdout=f,stderr=subprocess.STDOUT,check=True)
 s=pathlib.Path('linux-'+name+'.log').read_text();assert re.search(r'test result: ok\. [1-9][0-9]* passed; 0 failed',s),name
rows=build('release',['build','--locked','--release','-p','transparent-shard-server','--bin','transparent-shard-server','--bin','shard-control','--example','soak-query'])
for row in rows:
 if row.get('executable'):shutil.copy2(row['executable'],artifacts/row['target']['name'])
for name,digest in json.loads((root/'source-manifest.json').read_text()).items():assert hashlib.sha256((root/name).read_bytes()).hexdigest()==digest,name
(root/'artifact-sha256.json').write_text(json.dumps({p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in artifacts.iterdir()},indent=2)+'\n')
print('Pinned Linux release and executed worker suites passed; deployment requires final source commit binding',flush=True)
