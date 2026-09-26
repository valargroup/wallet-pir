import argparse,concurrent.futures,json,pathlib,shlex,subprocess
p=argparse.ArgumentParser();p.add_argument('mode',choices=['stage','apply','rollback']);p.add_argument('revision');p.add_argument('roles',nargs='*');a=p.parse_args()
hashes=json.loads(pathlib.Path('/tmp/full-stack-rollout-evidence/binaries.json').read_text())
roles={}
def add(host,ssh,units,stage,gpu=False):
 for unit,binary in units:
  source=stage+'/'+binary
  roles[host+'.'+unit]={'ssh':ssh,'unit':unit,'source':source,'hash':hashes['gpu' if gpu else 'cpu'][binary],'installer':stage.rsplit('/',1)[0]+'/install-release.py' if host=='coordinator' else stage+'/install-release.py'}
add('coordinator',['ssh','root@167.99.42.60'],[(u,b) for u,b in [('enhance-pir-coordinator','enhance-pir-server'),('enhance-pir-query-ingress','enhance-pir-server'),('status-controller-qualification','status-pir'),('pir-apm','pir-apm')]],'/root/full-stack-stage/cpu')
for host,ip,unit in [('router','159.65.203.17','enhance-pir-packing-router'),('worker1','10.142.0.15','enhance-pir-worker'),('worker2','10.142.0.16','enhance-pir-worker')]:
 add(host,['ssh','-J','root@167.99.42.60','root@'+ip],[(unit,'enhance-pir-server')],'/root/full-stack-stage')
add('gpu',['ssh','-J','root@167.99.42.60','paperspace@10.250.90.3','sudo'],[('enhance-pir-gpu-worker','enhance-pir-server')],'/home/paperspace/full-stack-stage',True)
add('status-gpu',['ssh','status-pir-p4000-ams1','sudo'],[(u,'status-pir') for u in ['status-worker','status-router','status-pir']],'/home/paperspace/full-stack-stage',True)
add('monitor',['ssh','-J','root@167.99.42.60','root@167.71.84.17'],[('pir-monitor','pir-monitor')],'/root/full-stack-stage')
selected=a.roles or list(roles)
assert a.mode=='stage' or a.roles,'explicit roles required for mutations'
def one(key):
 r=roles[key];cmd=['flock','-w','30','/run/lock/wallet-pir-production.lock','python3',r['installer'],a.mode,r['unit'],r['source'],r['hash'],a.revision]
 result=subprocess.run(r['ssh']+[shlex.join(cmd)],capture_output=True,text=True,timeout=180)
 out={'role':key,'mode':a.mode,'returncode':result.returncode,'stdout':result.stdout,'stderr':result.stderr}
 pathlib.Path('/tmp/full-stack-rollout-evidence',a.mode+'-'+key+'.json').write_text(json.dumps(out,indent=2)+'\n')
 print(json.dumps(out),flush=True)
 return result.returncode
if a.mode=='stage':
 with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:codes=list(pool.map(one,selected))
else:
 codes=[]
 for key in selected:
  code=one(key);codes.append(code)
  if code:break
raise SystemExit(1 if any(codes) else 0)
