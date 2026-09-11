import datetime,json,subprocess
from pathlib import Path
base=Path('/opt/transparent-publisher-build/headless-20260911')
rows=[json.loads(x) for x in (base/'rollout/canary/samples.ndjson').read_text().splitlines()]
ssh=['ssh','-o','ConnectTimeout=10','-i','/opt/transparent-publisher/credentials/deploy-ssh','-o','UserKnownHostsFile=/opt/transparent-publisher/credentials/known_hosts','root@10.142.0.10']
worker=subprocess.check_output(ssh+['python3 -'],input='''import json,subprocess
from pathlib import Path
p=Path('/run/tp-headless-probe')
r={'helper':json.loads(subprocess.check_output(['python3','/usr/local/lib/transparent-pir/headless-console.py','--check'],text=True)),'worker':subprocess.check_output(['systemctl','show','transparent-shard-server.service','-p','MainPID','-p','ExecStartPre','-p','UnitFileState','-p','ActiveState'],text=True),'probe':subprocess.check_output(['systemctl','show','transparent-m1-headless-probe.service','-p','MainPID','-p','ActiveState'],text=True),'probe_start':json.loads((p/'start.json').read_text()),'probe_summary':{}}
for name in ['unix','http']:
 rows=[json.loads(x) for x in (p/(name+'.ndjson')).read_text().splitlines()]
 r['probe_summary'][name]={'samples':len(rows),'errors':sum('error' in x for x in rows),'max_seconds':max(x['seconds'] for x in rows)}
print(json.dumps(r))
''',text=True,timeout=30)
r={'captured_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'partial':True,'acceptance_complete':False,'unit':subprocess.check_output(['systemctl','show','transparent-m1-headless-rollout.service','-p','MainPID','-p','ActiveState','-p','ExecMainStatus'],text=True),'worker':json.loads(worker),'samples_start':rows[:2],'latest_worker':next(x for x in reversed(rows) if x['event']=='worker'),'files':{}}
for path in ['enabled.json','ops-source-manifest.json','rollout/status.json','rollout/canary-upgrade/result.json','rollout/canary-upgrade/verified.json']:
 r['files'][path]=json.loads((base/path).read_text())
print(json.dumps(r,indent=2))
