import ast,datetime,json,pathlib,subprocess
root=pathlib.Path('/opt/transparent-v10-8e69ea75')
sha='8e69ea75b1e0071e3b978b0c78cc0487377f9e82'
fleet=json.loads(pathlib.Path('/opt/transparent-publisher/fleet.json').read_text())
roster=json.loads((root/'pre-roster.json').read_text())
worker=next(w for w in roster if w['id']=='transparent-pir-archive-02')
args=['ssh','-i',fleet['ssh_key'],'-o','BatchMode=yes','-o','IdentitiesOnly=yes','-o','StrictHostKeyChecking=yes','-o','UserKnownHostsFile='+fleet['known_hosts'],'root@'+worker['ssh_host']]
assert not (root/'prewarm-retry-start.json').exists()
(root/'prewarm-retry-start.json').write_text(json.dumps({'utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'worker':worker['id'],'reason':'Projected single-thread completion exceeds the private 2400-second deadline. Restart private prewarm using saved cache and three low-priority Rayon threads; live v9 unchanged.','source_sha':sha},indent=2)+'\n')
subprocess.run(args+['systemctl stop transparent-v10-prewarm'],check=True)
tree=ast.parse((root/'prewarm.py').read_text())
remote=next(ast.literal_eval(n.value) for n in tree.body if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='remote' for t in n.targets))
remote=remote.replace("unit='transparent-v10-prewarm'","unit='transparent-v10-prewarm-retry'")
remote=remote.replace("'/opt/transparent-publisher/v10-prewarm-'","'/opt/transparent-publisher/v10-prewarm-retry-'")
remote=remote.replace('RAYON_NUM_THREADS=1','RAYON_NUM_THREADS=3')
remote=remote.replace('WORKER',repr(worker)).replace('SHA',repr(sha))
(root/'prewarm-retry-remote.py').write_text(remote)
subprocess.run(args+['python3 -'],input=remote,text=True,check=True)
# Re-read durable success records from all six workers before completing staging.
records=[]
for w in roster:
 a=args[:-1]+['root@'+w['ssh_host']]
 d='v10-prewarm-retry-' if w['id']==worker['id'] else 'v10-prewarm-'
 r=json.loads(subprocess.check_output(a+['cat /opt/transparent-publisher/'+d+sha[:8]+'/complete.json']))
 q=r['ready'];assert q['ready'] and q['prewarm_failed']==0 and q['warm_runtimes']==q['target_runtimes'] and q['runtime_cache']['pending_saves']==0 and q['runtime_cache']['write_failures']==0
 assert q['binary_sha256']=='0ece0ae1ac7f526c8471b01bb06c35c0138c80eb2c46cbaff992fa237c45f36b'
 records.append(r)
(root/'prewarm-results.json').write_text(json.dumps(records,indent=2)+'\n')
for name in ['prewarm.complete','staging.complete']:(root/name).write_text(sha+'\n')
print('All six prewarm success records checked; staging complete',flush=True)
