import hashlib,json,subprocess,shutil,os
from pathlib import Path
root=Path('/opt/transparent-publisher-build/collection-timing-20260911')
for unit in ['transparent-m1-collection-timing-build','transparent-m1-collection-timing-verify']:
 s=subprocess.check_output(['systemctl','show',unit,'-p','ActiveState','-p','ExecMainStatus'],text=True)
 assert 'ActiveState=inactive' in s and 'ExecMainStatus=0' in s,s
assert '1 passed; 0 failed' in (root/'linux-worker-integration-tests.log').read_text()
for n,d in json.loads((root/'source-manifest.json').read_text()).items():assert hashlib.sha256((root/n).read_bytes()).hexdigest()==d,n
for n,d in json.loads((root/'artifact-sha256.json').read_text()).items():assert hashlib.sha256((root/'artifacts'/n).read_bytes()).hexdigest()==d,n
assert not subprocess.check_output(['systemctl','list-units','--state=running','--no-legend','transparent*rollout*'],text=True).strip()
old=Path('/opt/transparent-publisher-build/unpublished-reorg-20260911/artifacts')
for n,d in {'shard-control':'c46543dd79ed66dfff72fd846b852fb84961532f97809068c200cd3a022a47dd','soak-query':'3048230c644e3e98831b9e46f52ec8c9d02382c75f7a3c69e58f4c277226771f'}.items():
 assert hashlib.sha256((old/n).read_bytes()).hexdigest()==d
 shutil.copy2(old/n,root/'artifacts'/n)
live=Path('/opt/transparent-publisher/transparent-live-fleet.py')
assert hashlib.sha256(live.read_bytes()).hexdigest()=='291b01f320267aa140a00e6b17369481aeb4299af483a94de17b87614fba6e5c'
shutil.copy2(live,root/'fleet-before.py')
shutil.copy2(root/'ops/scripts/transparent-live-fleet.py',live.with_suffix('.timing-next'))
os.replace(live.with_suffix('.timing-next'),live)
subprocess.run(['/usr/bin/python3',str(root/'ops/scripts/upgrade-transparent-fleet.py'),'--artifacts',str(root/'artifacts'),'--source-sha','3d13da6','--worker','transparent-pir-recent-01','--query-binary',str(root/'artifacts/soak-query'),'--out',str(root/'diagnostic-upgrade')],check=True)
