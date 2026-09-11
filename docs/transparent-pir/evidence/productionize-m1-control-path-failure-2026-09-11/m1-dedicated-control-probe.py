import concurrent.futures,datetime,json,socket,subprocess,time,urllib.request
from pathlib import Path
root=Path('/run/transparent-dedicated-control-probe');root.mkdir(mode=0o700)
pid=int(subprocess.check_output(['systemctl','show','transparent-replica-reconciler','-p','MainPID','--value']));assert pid>0
(root/'start.json').write_text(json.dumps({'utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'worker_pid':pid,'seconds':3600,'acceptance':False})+'\n')
end=time.monotonic()+3600
import importlib.util
spec=importlib.util.spec_from_file_location('fleet_probe','/opt/transparent-publisher/transparent-live-fleet.py')
module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
fleet=module.Fleet(json.loads(Path('/opt/transparent-publisher/fleet.json').read_text()))
worker=next(w for w in fleet.roster if w['id']=='transparent-pir-recent-01')
forward='/run/transparent-reconciler-probe/dedicated.sock'
def unix():
 start=time.monotonic()
 with socket.socket(socket.AF_UNIX) as s:
  s.settimeout(4);s.connect(forward);s.sendall(b'{"operation":"status"}\n');data=b''
  while b'\n' not in data:
   chunk=s.recv(65536)
   if not chunk:break
   data+=chunk
   if len(data)>1024*1024:raise ValueError('oversized response')
  received=time.monotonic()
 value=json.loads(data)
 return {'socket_seconds':received-start,'ok':value.get('ok'),'warm':value.get('result',{}).get('warm')}
def http():
 start=time.monotonic()
 with urllib.request.urlopen('http://10.142.0.10:8093/v1/ready',timeout=4) as response:
  raw=response.read();received=time.monotonic();value=json.loads(raw)
 return {'socket_seconds':received-start,'ready':value.get('ready'),'map_sha256':value.get('map_sha256')}
def loop(name,fn,interval):
 previous=None
 with (root/(name+'.ndjson')).open('x') as output:
  while time.monotonic()<end:
   start=time.monotonic();row={'utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'probe':name,'start_gap_seconds':None if previous is None else start-previous};previous=start
   try:row['value']=fn()
   except Exception as e:row['error']=type(e).__name__+': '+str(e)
   row['seconds']=time.monotonic()-start
   output.write(json.dumps(row)+'\n');output.flush()
   time.sleep(max(.01,interval-(time.monotonic()-start)))
with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
 futures=[pool.submit(loop,name,fn,interval) for name,fn,interval in [('unix',unix,.5)]]
 for future in futures:future.result()
