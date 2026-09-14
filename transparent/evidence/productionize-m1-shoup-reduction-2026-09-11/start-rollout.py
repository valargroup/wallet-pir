import datetime,hashlib,json,pathlib,re,subprocess,urllib.request
root=pathlib.Path('/opt/transparent-publisher-build/shoup-release-20260911')
source='2c4a2507523a39e76aef8a6db3076c40d54026ae'
def sha(p):return hashlib.sha256(pathlib.Path(p).read_bytes()).hexdigest()
state=subprocess.check_output(['systemctl','show','transparent-m1-shoup-release-build.service','-p','ActiveState','-p','ExecMainStatus'],text=True)
assert 'ActiveState=inactive' in state and 'ExecMainStatus=0' in state,state
binding=json.loads((root/'source-commit.json').read_text());assert binding['source_sha']==source
assert binding['source_manifest_sha256']==sha(root/'source-manifest.json')
manifest=json.loads((root/'source-manifest.json').read_text());assert len(manifest)==binding['verified_files']==321
for n,d in manifest.items():assert sha(root/n)==d,n
artifacts=json.loads((root/'artifact-sha256.json').read_text())
assert set(artifacts)=={'worker-library-tests','worker-integration-tests','transparent-shard-server','shard-control','soak-query'}
for n,d in artifacts.items():assert sha(root/'artifacts'/n)==d,n
for name in ['worker-library-tests','worker-integration-tests']:
 assert re.search(r'test result: ok\. [1-9][0-9]* passed; 0 failed',(root/('linux-'+name+'.log')).read_text()),name
# Assembly review must name this exact final release binary, not the experimental test executable.
review=json.loads((root/'assembly-review.json').read_text())
assert review['binary_sha256']==artifacts['transparent-shard-server'] and review['passed'] is True
assert not subprocess.check_output(['systemctl','list-units','--state=running','--no-legend','transparent*rollout*'],text=True).strip()
expected={'/usr/local/bin/transparent-publish-controller':'b191c25abb027378b564d32d62c23c3aee52c300b5506cc0ab259cde57eddcf4','/opt/transparent-publisher/fleet.json':'ad6048bd12d3472fa5a9632ff275544b360e9509334ddc0a7934580fae22015f','/opt/transparent-publisher/controller.json':'5ce43599786b177bb26bfdb991ae07eafa880855294c8053a0fada38d73c7da3'}
for n,d in expected.items():assert sha(n)==d,n
assert sha('/opt/transparent-publisher/transparent-live-fleet.py')==sha(root/'ops/scripts/transparent-live-fleet.py')
with urllib.request.urlopen('http://10.142.0.10:8093/v1/ready',timeout=5) as r:ready=json.load(r)
assert ready['binary_sha256']=='46645937893ef38238bce2ef1124b9a468e02742273f0948e6f004206eb16f7e'
assert ready['ready']
(root/'rollout-preflight.json').write_text(json.dumps(dict(utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),source_sha=source,expected=expected,predecessor=ready,artifacts=artifacts),indent=2)+'\n')
subprocess.run(['systemd-run','--unit=transparent-m1-shoup-rollout','--property=Restart=no','/usr/bin/python3',str(root/'ops/scripts/run-transparent-hardening-rollout.py'),'--artifacts',str(root/'artifacts'),'--source-sha',source,'--out',str(root/'rollout')],check=True)
