#!/usr/bin/env python3
"""Persistent exact-query load with local health gates; never mutates production services."""
import collections,concurrent.futures,datetime,gzip,json,os,pathlib,re,signal,subprocess,threading,time,urllib.request,urllib.error
ROOT=pathlib.Path('/opt/transparent-5qps-20260929')
FLEET={};NODES=[]
LOCK=threading.Lock();COUNTS=collections.Counter();WINDOW=collections.deque();LATCH=[];PROC=None;STOP=False;LAST_READY=None

def utc():return datetime.datetime.now(datetime.timezone.utc).isoformat()
def atomic(name,data):
 p=ROOT/name;q=p.with_suffix(p.suffix+'.tmp');q.write_text(json.dumps(data,indent=2)+'\n');os.replace(q,p)
def record(kind,data):
 hour=datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H')
 with gzip.open(ROOT/f'{kind}-{hour}.jsonl.gz','at') as f:f.write(json.dumps(data,separators=(',',':'))+'\n')
def fetch(url):
 try:
  with urllib.request.urlopen(url,timeout=5) as r:return {'http':r.status,'body':json.load(r)}
 except urllib.error.HTTPError as e:return {'http':e.code}
 except Exception as e:return {'error':str(e)}
def worker(w):
 service=w.get('service','transparent-shard-server')
 command=f'cat /proc/meminfo; systemctl show {service} -p ActiveState -p NRestarts -p MemoryCurrent -p MemoryPeak -p CPUUsageNSec; cat /sys/fs/cgroup/system.slice/{service}.service/memory.events; cat /proc/loadavg'
 args=['ssh','-i',FLEET['ssh_key'],'-o','BatchMode=yes','-o','IdentitiesOnly=yes','-o','StrictHostKeyChecking=yes','-o','UserKnownHostsFile='+FLEET['known_hosts'],'-o','ConnectTimeout=4','root@'+w.get('ssh_host','localhost'),command]
 if w.get('local'):args=['/bin/sh','-c',command]
 result={'id':w['id'],'ready':fetch('http://'+w['upstream']+'/v1/ready') if 'upstream' in w else {}}
 try:
  p=subprocess.run(args,capture_output=True,text=True,timeout=8);result.update(host=p.stdout,ssh_exit=p.returncode)
  if 'upstream' not in w:result['ready']={'http':200,'body':{'ready':'ActiveState=active' in p.stdout,'mode':'warm','probe':'systemd '+service}}
  for key,pattern in [('available_kib',r'^MemAvailable:\s+(\d+)'),('total_kib',r'^MemTotal:\s+(\d+)'),('oom',r'^oom (\d+)'),('oom_kill',r'^oom_kill (\d+)'),('restarts',r'^NRestarts=(\d+)'),('cpu_ns',r'^CPUUsageNSec=(\d+)')]:
   m=re.search(pattern,p.stdout,re.M);result[key]=int(m[1]) if m else None
 except Exception as e:result['error']=str(e)
 return result

def persist_latch(reason):
 try:
  with (ROOT/'latched.json').open('x') as f:json.dump({'utc':utc(),'reason':reason},f)
 except FileExistsError:pass

def correctness_failure(v):
 error=v.get('error','').lower()
 return v.get('event')=='error' and (v.get('kind')=='prepare' or 'differs from independent plaintext' in error or error.startswith('response:') or error.startswith('pir:'))

def reader(proc):
 global LAST_READY
 for line in proc.stdout:
  try:v=json.loads(line)
  except ValueError:v={'event':'unparsed','text':line[:1000],'unix':time.time()}
  record('queries',v)
  with LOCK:
   event=v.get('event','unknown');COUNTS[event]+=1
   if event=='ready':LAST_READY=v
   if event in ('query','error','missed_slot','schedule_reset'):WINDOW.append((time.time(),v))
   if correctness_failure(v):
    LATCH.append(v);persist_latch(v);proc.terminate()
  if event not in ('query',):print(json.dumps(v),flush=True)

def stop(signum,frame):
 global STOP
 STOP=True
 (ROOT/'permit').write_text('deny\n')
 if PROC and PROC.poll() is None:PROC.terminate()
signal.signal(signal.SIGTERM,stop);signal.signal(signal.SIGINT,stop)

def main():
 global PROC,FLEET,NODES
 FLEET=json.loads(pathlib.Path('/opt/transparent-publisher/fleet.json').read_text())
 roster=json.loads(pathlib.Path('/opt/transparent-v10-8e69ea75/pre-roster.json').read_text())
 NODES=roster+[{'id':'transparent-router','ssh_host':FLEET['router_host'],'service':'caddy'},{'id':'transparent-coordinator','local':True,'service':'transparent-publish-controller'}]
 (ROOT/'permit').write_text('deny\n')
 if (ROOT/'latched.json').exists():LATCH.append({'event':'health_latch','reason':'persisted critical incident; inspect latched.json'})
 args=[str(ROOT/'rate-query'),'--url','https://transparent-pir.valargroup.dev','--fixture',str(ROOT/'fixture.json'),'--qps','5','--workers','8','--permit',str(ROOT/'permit')]
 started=time.time();baseline={};bad_streak=0;healthy_streak=0;mode='starting';incidents=0
 with (ROOT/'client-stderr.log').open('a') as err:
  PROC=subprocess.Popen(args,stdout=subprocess.PIPE,stderr=err,text=True,bufsize=1)
  record('lifecycle',{'utc':utc(),'event':'start','query_pid':PROC.pid,'args':args})
  thread=threading.Thread(target=reader,args=(PROC,),daemon=True);thread.start()
  while not STOP:
   tick=time.monotonic()
   with concurrent.futures.ThreadPoolExecutor(max_workers=10) as pool:
    futures=[pool.submit(worker,w) for w in NODES]
    c=pool.submit(fetch,'http://127.0.0.1:8094/v1/status');public=pool.submit(fetch,'https://transparent-pir.valargroup.dev/v1/shards/init')
    workers=[f.result() for f in futures];controller=c.result();init=public.result()
   if STOP:break
   health={'utc':utc(),'workers':workers,'controller':controller,'public_init':init}
   record('health',health)
   reasons=[];critical=[]
   for w in workers:
    name=w['id'];body=w['ready'].get('body',{})
    if w.get('ssh_exit')!=0 or w.get('available_kib') is None or w.get('total_kib') is None:reasons.append(name+': resource probe unavailable')
    elif w['available_kib']/w['total_kib']<.15:critical.append(name+': available memory below 15%')
    if w['ready'].get('http')!=200 or not body.get('ready') or body.get('mode')!='warm':reasons.append(name+': not warm/ready')
    for key in ['oom','oom_kill','restarts']:
     value=w.get(key)
     if value is None:reasons.append(name+': missing '+key)
     elif (name,key) not in baseline:baseline[name,key]=value
     elif value>baseline[name,key]:critical.append(name+': '+key+' increased')
    if body.get('runtime_cache',{}).get('write_failures',0)>0:reasons.append(name+': cache write failures')
    if 'binary_sha256' in body and body['binary_sha256']!='0ece0ae1ac7f526c8471b01bb06c35c0138c80eb2c46cbaff992fa237c45f36b':critical.append(name+': worker binary changed from qualified v10 source')
   c=controller.get('body',{})
   if controller.get('http')!=200 or c.get('phase')!='serving':reasons.append('publisher not serving')
   if c.get('node_height',0)-c.get('public_height',0)>2:reasons.append('publication more than two blocks behind node')
   if init.get('http')!=200:reasons.append('public init unavailable')
   disk=os.statvfs(ROOT)
   if disk.f_bavail/disk.f_blocks<.1:critical.append('load log filesystem below 10% free')
   with LOCK:
    while WINDOW and WINDOW[0][0]<time.time()-60:WINDOW.popleft()
    recent=list(WINDOW);counts=dict(COUNTS);latch=list(LATCH);ready=LAST_READY
   successes=[v for _,v in recent if v['event']=='query'];errors=[v for _,v in recent if v['event']=='error'];misses=[v for _,v in recent if v['event']=='missed_slot']
   if len(errors)>=3:reasons.append('at least three query errors in trailing minute')
   if len(misses)>=3:reasons.append('client cannot sustain scheduled rate')
   if any(v.get('event')=='error' for v in latch):critical.append('incorrect decoded response: manual investigation required')
   if PROC.poll() is not None:critical.append('query process exited: '+str(PROC.returncode))
   if ready and time.time()-ready['unix']>90 and not successes and mode=='running':reasons.append('no successful query in trailing minute')
   bad_streak=bad_streak+1 if reasons else 0;healthy_streak=healthy_streak+1 if not reasons and not critical else 0
   old=mode
   if critical:mode='latched';persist_latch(critical);LATCH.extend({'event':'health_latch','reason':r} for r in critical if r not in [v.get('reason') for v in latch])
   elif latch:mode='latched'
   elif bad_streak>=2:mode='paused'
   elif mode in ('starting','paused') and healthy_streak>=2:mode='running'
   permit='allow' if mode=='running' else 'deny';(ROOT/'permit.tmp').write_text(permit+'\n');os.replace(ROOT/'permit.tmp',ROOT/'permit')
   if old!=mode:
    incidents+=mode in ('paused','latched');record('incidents',{'utc':utc(),'from':old,'to':mode,'reasons':reasons+critical});print(json.dumps({'event':'mode','from':old,'to':mode,'reasons':reasons+critical}),flush=True)
   times=sorted(v['http_seconds'] for v in successes)
   def pct(p):return times[min(len(times)-1,int((len(times)-1)*p))] if times else None
   summary={'utc':utc(),'started_unix':started,'mode':mode,'target_query_qps':5,'client_location':'coordinator; shares host with ingest/publication; Nice=10 CPUQuota=200%','workload':'80% recent, 20% archive; directory/pages equally split; sealed revisions; fresh keys; exact row hashes; separate moving-tail health probes','ready':ready,'counts':counts,'trailing_60s':{'exact':len(successes),'errors':len(errors),'missed_slots':len(misses),'completed_qps':len(successes)/60,'http_p50_seconds':pct(.5),'http_p95_seconds':pct(.95),'http_p99_seconds':pct(.99),'max_schedule_lag_seconds':max((v['schedule_lag_seconds'] for v in successes),default=None)},'reasons':reasons+critical,'incidents':incidents,'publisher':c,'workers':[{'id':w['id'],'available_memory_percent':100*w['available_kib']/w['total_kib'] if w.get('available_kib') and w.get('total_kib') else None,'restarts':w.get('restarts'),'oom':w.get('oom'),'oom_kill':w.get('oom_kill'),'cpu_ns':w.get('cpu_ns'),'ready':w['ready'].get('body',{}).get('ready')} for w in workers]}
   atomic('status.json',summary);record('summaries',summary)
   if PROC.poll() is not None:raise RuntimeError('query child exited; retained logs, no automatic restart')
   for _ in range(max(0,int(15-(time.monotonic()-tick)))):
    if STOP:break
    time.sleep(1)
  if PROC.poll() is None:PROC.terminate()
  try:PROC.wait(timeout=20)
  except subprocess.TimeoutExpired:PROC.kill();PROC.wait()
  thread.join(timeout=5)
if __name__=='__main__':
 try:main()
 finally:
  (ROOT/'permit').write_text('deny\n')
  if PROC and PROC.poll() is None:PROC.terminate()
