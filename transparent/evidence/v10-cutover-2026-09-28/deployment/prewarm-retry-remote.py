
import datetime,json,pathlib,shlex,subprocess,time,urllib.request,urllib.error
worker={'cache_bytes': 51539607552, 'id': 'transparent-pir-archive-02', 'memory_max': '56G', 'replica_group': None, 'role': 'archive-owner', 'ssh_host': '10.142.0.9', 'upstream': '10.142.0.9:8093', 'build_slots': 1}
sha='8e69ea75b1e0071e3b978b0c78cc0487377f9e82'
staged=pathlib.Path('/tmp/transparent-pir-'+sha)
unit='transparent-v10-prewarm-retry'
logdir=pathlib.Path('/opt/transparent-publisher/v10-prewarm-retry-'+sha[:8])
logdir.mkdir(exist_ok=False)
mem={k:int(v.split()[0])*1024 for k,v in (line.split(':',1) for line in pathlib.Path('/proc/meminfo').read_text().splitlines())}
limit=(24 if worker['role']=='archive-owner' else 3)*(1<<30)
assert mem['MemAvailable']-limit>=mem['MemTotal']//5, 'insufficient spare RAM for private prewarm'
args=None
for line in (staged/'unit.rendered').read_text().splitlines():
 if line.startswith('ExecStart='): args=shlex.split(line[len('ExecStart='):])
assert args and '--control-socket' not in args and '--active-record' not in args
args[0]=str(staged/'transparent-shard-server')
args[args.index('--listen')+1]='127.0.0.1:18092'
args[args.index('--assignment')+1]=str(staged/'assignment.json')
if '--build-slots' in args: args[args.index('--build-slots')+1]='1'
else: args+=['--build-slots','1']
assert '--runtime-cache-dir' in args and '--runtime-cache-prune-set' not in args
assert args[args.index('--runtime-cache-dir')+1]=='/srv/transparent-pir/runtime-cache-v10', 'v9 and v10 collectors must use separate caches'
assert args[args.index('--shard-dir')+1].startswith('/srv/transparent-pir/sets-v10/'), 'static cutover sets need an isolated parent'
record={'worker':worker['id'],'started_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'memory_max':limit,'mem_before':mem,'args':args}
(logdir/'start.json').write_text(json.dumps(record,indent=2)+'\n')
subprocess.run(['systemd-run','--unit',unit,'--property=MemoryMax='+str(limit),'--property=MemorySwapMax=0','--property=Nice=15','--property=CPUWeight=10','--property=IOWeight=10','--property=Restart=no','--property=RuntimeMaxSec=2700','--property=StandardOutput=append:'+str(logdir/'server.log'),'--property=StandardError=append:'+str(logdir/'server.log'),'--setenv=RAYON_NUM_THREADS=3']+args,check=True)
start=time.monotonic()
try:
 with (logdir/'timeline.jsonl').open('w') as stream:
  while time.monotonic()-start<2400:
   status=subprocess.check_output(['systemctl','show',unit,'-p','ActiveState','-p','MemoryCurrent','-p','MemoryPeak','-p','ExecMainStatus'],text=True)
   if 'ActiveState=failed' in status or 'ActiveState=inactive' in status: raise RuntimeError(status)
   mem={k:int(v.split()[0])*1024 for k,v in (line.split(':',1) for line in pathlib.Path('/proc/meminfo').read_text().splitlines())}
   assert mem['MemAvailable']>=mem['MemTotal']//5, 'host spare RAM below 20%; stop private prewarm'
   ready=None
   try:
    with urllib.request.urlopen('http://127.0.0.1:18092/v1/ready',timeout=5) as f: ready=json.load(f)
   except urllib.error.HTTPError as e:
    try: ready=json.load(e)
    except ValueError: pass
   except Exception: pass
   stream.write(json.dumps({'utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'seconds':time.monotonic()-start,'status':status,'available':mem['MemAvailable'],'ready':ready})+'\n');stream.flush()
   if ready and ready.get('prewarm_failed'): raise RuntimeError('candidate runtime prewarm failed')
   if ready and ready.get('ready') and ready['runtime_cache']['pending_saves']==0:
    assert ready['runtime_cache']['write_failures']==0
    assert ready['binary_sha256']=='0ece0ae1ac7f526c8471b01bb06c35c0138c80eb2c46cbaff992fa237c45f36b'
    record.update(completed_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),seconds=time.monotonic()-start,ready=ready,status=status)
    (logdir/'complete.json').write_text(json.dumps(record,indent=2)+'\n')
    print(json.dumps(record),flush=True)
    break
   time.sleep(5)
  else: raise RuntimeError('private prewarm deadline')
finally:
 subprocess.run(['systemctl','stop',unit],check=True)
