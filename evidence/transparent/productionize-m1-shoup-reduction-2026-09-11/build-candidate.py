import hashlib,json,os,pathlib,shutil,subprocess,tarfile
root=pathlib.Path('/opt/transparent-publisher-build/shoup-reduction-20260911')
root.mkdir()
with tarfile.open('/opt/transparent-publisher-build/collection-timing-20260911/diagnostic-source.tar.gz') as t:t.extractall(root,filter='data')
with tarfile.open('/tmp/m1-shoup-dependency.tar.gz') as t:t.extractall(root/'dependencies',filter='data')
shutil.copy2('/tmp/m1-shoup-dependency-manifest.json',root/'dependency-manifest.json')
for name,digest in json.loads((root/'dependency-manifest.json').read_text()).items():
 assert hashlib.sha256((root/'dependencies/spiral-rs'/name).read_bytes()).hexdigest()==digest,name
os.chdir(root)
with open('Cargo.toml','a') as f:f.write('\n[patch."https://github.com/valargroup/spiral-rs.git"]\nvalar-spiral-rs = { path = "dependencies/spiral-rs" }\n')
env=dict(os.environ,CARGO_TARGET_DIR='/opt/transparent-publisher-build/6e0c65c/target',RUSTFLAGS='-C target-cpu=x86-64-v3 -C target-feature=+pclmulqdq',CARGO_BUILD_JOBS='2')
(root/'build-provenance.json').write_text(json.dumps(dict(base_source='3d13da6',dependency_base='6f5b66c6a5a639827c6486c59d31c7ec2d4399a8',scope='isolated path-patched candidate; not a deployment release',rustflags=env['RUSTFLAGS']),indent=2)+'\n')
subprocess.run(['/root/.cargo/bin/cargo','update','--offline','-p','valar-spiral-rs'],env=env,check=True)
with open('linux-reduction-tests.log','w') as f:subprocess.run(['/root/.cargo/bin/cargo','test','--locked','--release','-p','valar-spiral-rs','--lib','reduction_tests'],env=env,stdout=f,stderr=subprocess.STDOUT,check=True)
assert '3 passed; 0 failed' in pathlib.Path('linux-reduction-tests.log').read_text()
artifacts=root/'artifacts';artifacts.mkdir()
with open('worker-integration-build.jsonl','w') as f:
 subprocess.run(['/root/.cargo/bin/cargo','test','--locked','--release','-p','transparent-shard-server','--test','revisions_and_cache','--no-run','--message-format=json'],env=env,stdout=f,check=True)
for line in pathlib.Path('worker-integration-build.jsonl').read_text().splitlines():
 r=json.loads(line)
 if r.get('executable') and r.get('profile',{}).get('test'):shutil.copy2(r['executable'],artifacts/'worker-integration-tests')
with open('linux-live-integration.log','w') as f:subprocess.run([str(artifacts/'worker-integration-tests'),'live_prepare_activate_and_invalidate','--exact','--nocapture'],stdout=f,stderr=subprocess.STDOUT,check=True)
assert '1 passed; 0 failed' in pathlib.Path('linux-live-integration.log').read_text()
(root/'artifact-sha256.json').write_text(json.dumps({p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in artifacts.iterdir()},indent=2)+'\n')
print('Candidate build and executed regression tests passed',flush=True)
