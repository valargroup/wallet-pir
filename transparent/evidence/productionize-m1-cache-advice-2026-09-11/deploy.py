SOURCE='be1e5759d89e62fc5801944cfd8d635626cb1199'
import hashlib,json,subprocess,shutil
from pathlib import Path
root=Path('/opt/transparent-publisher-build/cache-advice-20260911')
status=subprocess.check_output(['systemctl','show','transparent-m1-cache-advice-build.service','-p','ActiveState','-p','ExecMainStatus'],text=True)
assert 'ActiveState=inactive' in status and 'ExecMainStatus=0' in status,status
assert '38 passed; 0 failed' in (root/'linux-worker-tests.log').read_text()
for n,d in json.loads((root/'source-manifest.json').read_text()).items():assert hashlib.sha256((root/n).read_bytes()).hexdigest()==d,n
for n,d in json.loads((root/'artifact-sha256.json').read_text()).items():assert hashlib.sha256((root/'artifacts'/n).read_bytes()).hexdigest()==d,n
assert not subprocess.check_output(['systemctl','list-units','--state=running','--no-legend','transparent*rollout*'],text=True).strip()
assert not (root/'candidate-upgrade').exists()
for unit in ['transparent-m1-cache-advice-integration.service','transparent-m1-loading-long-diagnostic.service']:
 state=subprocess.check_output(['systemctl','show',unit,'-p','ActiveState','-p','MainPID'],text=True)
 assert 'ActiveState=inactive' in state and 'MainPID=0' in state,state
assert 'test result: ok. 8 passed; 0 failed' in (root/'linux-integration.log').read_text()
assert 'test result: ok. 18 passed; 0 failed' in (root/'linux-integration.log').read_text()
old=Path('/opt/transparent-publisher-build/shoup-release-20260911/artifacts')
for n,d in {'shard-control':'d8eebd1d5d7e0a4d06a9246d851c6b41f1056fe4effdc258bdf62a820219a45d','soak-query':'209a46565aee21b184e5cc71fb90487f16e753f78684bc46cf751b58de5dfeab'}.items():
 assert hashlib.sha256((old/n).read_bytes()).hexdigest()==d
 shutil.copy2(old/n,root/'artifacts'/n)
(root/'source-commit.json').write_text(json.dumps({'source_sha':SOURCE})+'\n')
subprocess.run(['/usr/bin/python3',str(root/'ops/scripts/upgrade-transparent-fleet.py'),'--artifacts',str(root/'artifacts'),'--source-sha',SOURCE,'--worker','transparent-pir-recent-01','--query-binary',str(root/'artifacts/soak-query'),'--out',str(root/'candidate-upgrade')],check=True)
