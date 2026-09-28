#!/usr/bin/env python3
import concurrent.futures,json,pathlib,subprocess
root=pathlib.Path('/opt/transparent-v10-8e69ea75')
fleet=json.loads(pathlib.Path('/opt/transparent-publisher/fleet.json').read_text())
roster=json.loads((root/'pre-roster.json').read_text())
def one(w):
 args=['ssh','-i',fleet['ssh_key'],'-o','BatchMode=yes','-o','IdentitiesOnly=yes','-o','StrictHostKeyChecking=yes','-o','UserKnownHostsFile='+fleet['known_hosts'],'root@'+w['ssh_host'],'tail -n 1 /opt/transparent-publisher/v10-prewarm-8e69ea75/timeline.jsonl']
 p=subprocess.run(args,capture_output=True,text=True,timeout=15)
 if p.returncode: return {'worker':w['id'],'error':p.stderr}
 x=json.loads(p.stdout);r=x.get('ready') or {}
 return {'worker':w['id'],'seconds':round(x['seconds']),'warm':r.get('warm_runtimes'),'target':r.get('target_runtimes'),'failed':r.get('prewarm_failed'),'cache':r.get('runtime_cache'),'available_gib':round(x['available']/(1<<30),2)}
with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
 for item in pool.map(one,roster): print(json.dumps(item))
