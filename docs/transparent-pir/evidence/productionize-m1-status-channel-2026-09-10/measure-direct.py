import asyncio,importlib.util,json,shlex,time,datetime,urllib.request
from pathlib import Path
s=importlib.util.spec_from_file_location('fleet','/opt/transparent-publisher/transparent-live-fleet.py');m=importlib.util.module_from_spec(s);s.loader.exec_module(m)
remote='''import socket,time,json
s=socket.socket(socket.AF_UNIX);s.settimeout(7)
t=time.monotonic();s.connect('/run/transparent-pir/control.sock');s.sendall(b'{"operation":"status"}\\n');data=b''
while not data.endswith(b'\\n'):data+=s.recv(65536)
d=json.loads(data);r=d.get('result',{})
print(json.dumps({'socket_seconds':time.monotonic()-t,'ok':d.get('ok'),'warm':r.get('warm'),'map':r.get('active',{}).get('map_sha256')}))
'''
async def main():
 f=m.Fleet(json.loads(Path('/opt/transparent-publisher/fleet.json').read_text()));w=next(w for w in f.roster if w['id']=='transparent-pir-recent-01')
 out=Path('/opt/transparent-publisher-build/status-latency-20260910');out.mkdir(exist_ok=True)
 async def sample(kind):
  t=time.monotonic();r=dict(utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),kind=kind)
  try:
   if kind=='ssh_status':r.update(json.loads(await f.ssh(w['ssh_host'],shlex.join(['python3','-c',remote]),timeout=9,multiplex=False)))
   elif kind=='ssh_noop':await f.ssh(w['ssh_host'],'true',timeout=9,multiplex=False)
   else:
    def http():
     with urllib.request.urlopen('http://'+w['upstream']+'/v1/ready',timeout=9) as response:return json.load(response)
    d=await asyncio.to_thread(http);r.update(warm=d.get('ready'),map=d.get('map_sha256'))
  except Exception as e:r['error']=type(e).__name__+': '+str(e)
  r['total_seconds']=time.monotonic()-t
  return r
 with (out/'samples.ndjson').open('w',buffering=1) as log:
  for i in range(90):
   for r in await asyncio.gather(*(sample(k) for k in ['ssh_status','ssh_noop','http_ready'])):log.write(json.dumps(r)+'\n')
   await asyncio.sleep(1)
asyncio.run(main())
