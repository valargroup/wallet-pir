import asyncio,datetime,importlib.util,json,socket,sys,time,urllib.request
from pathlib import Path
DURATION=7200
async def sample_loop(name,fn,out,interval=.5):
 end=time.monotonic()+DURATION
 while time.monotonic()<end:
  start=time.monotonic();row={'utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'probe':name}
  try:row['value']=await fn()
  except Exception as e:row['error']=type(e).__name__+': '+str(e)
  row['seconds']=time.monotonic()-start
  with out.open('a') as f:f.write(json.dumps(row)+'\n')
  await asyncio.sleep(max(.05,interval-row['seconds']))
def http(url):
 with urllib.request.urlopen(url,timeout=4) as r:
  d=json.load(r);return {'ready':d.get('ready'),'map_sha256':d.get('map_sha256'),'status':r.status}
def status():
 with socket.socket(socket.AF_UNIX) as s:
  s.settimeout(4);s.connect('/run/transparent-pir/control.sock');s.sendall(b'{"operation":"status"}\n');data=b''
  while b'\n' not in data:
   chunk=s.recv(65536)
   if not chunk:break
   data+=chunk
  d=json.loads(data);return {'ok':d.get('ok'),'warm':d.get('result',{}).get('warm')}
async def main():
 if '--worker' in sys.argv:
  out=Path('/tmp/m1-status-stall-worker.ndjson')
  async def facts():return {n:Path('/proc/'+n).read_text() for n in ['stat','pressure/cpu','pressure/memory','pressure/io']}
  await asyncio.gather(sample_loop('local_unix',lambda:asyncio.to_thread(status),out),sample_loop('local_http',lambda:asyncio.to_thread(http,'http://127.0.0.1:8093/v1/ready'),out),sample_loop('host',facts,out,1))
 else:
  spec=importlib.util.spec_from_file_location('fleet','/opt/transparent-publisher/transparent-live-fleet.py');m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
  f=m.Fleet(json.loads(Path('/opt/transparent-publisher/fleet.json').read_text()));w=next(w for w in f.roster if w['id']=='transparent-pir-recent-01')
  out=Path('/opt/transparent-publisher-build/sessions-20260910/stall-probe.ndjson')
  await f.ssh(w['ssh_host'],'cat > /tmp/m1-status-stall-probe.py',Path(__file__).read_bytes())
  await f.ssh(w['ssh_host'],'systemd-run --unit=transparent-m1-stall-probe --property=Restart=no /usr/bin/python3 /tmp/m1-status-stall-probe.py --worker')
  async def noop():await m.run(f.control_session_args(w)+['root@'+w['ssh_host'],'true'],timeout=4,file_output=True);return True
  async def control():
   d=await f.control(w,{'operation':'status'});return {'warm':d.get('warm'),'invalidated':d.get('invalidated')}
  await asyncio.gather(sample_loop('remote_http',lambda:asyncio.to_thread(http,'http://'+w['upstream']+'/v1/ready'),out),sample_loop('ssh_noop',noop,out),sample_loop('control',control,out))
asyncio.run(main())
