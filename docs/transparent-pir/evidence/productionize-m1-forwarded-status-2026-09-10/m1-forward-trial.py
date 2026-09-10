import asyncio,datetime,importlib.util,json,os,stat,time
from pathlib import Path
s=importlib.util.spec_from_file_location('fleet','/opt/transparent-publisher/transparent-live-fleet.py');m=importlib.util.module_from_spec(s);s.loader.exec_module(m)
async def main():
 f=m.Fleet(json.loads(Path('/opt/transparent-publisher/fleet.json').read_text()));w=next(w for w in f.roster if w['id']=='transparent-pir-recent-01')
 directory=Path('/run/transparent-m1-forward');directory.mkdir(mode=0o700)
 socket=directory/'status.sock'
 args=f.direct_ssh_args+['-oControlMaster=no','-oControlPath=none','-oStreamLocalBindMask=0177','-oExitOnForwardFailure=yes','-oServerAliveInterval=2','-oServerAliveCountMax=3','-N','-L',str(socket)+':/run/transparent-pir/control.sock','root@'+w['ssh_host']]
 p=await asyncio.create_subprocess_exec(*args,stdin=asyncio.subprocess.DEVNULL,stdout=asyncio.subprocess.DEVNULL)
 async def status():
  reader,writer=await asyncio.open_unix_connection(socket,limit=1024*1024)
  try:
   writer.write(b'{"operation":"status"}\n');await writer.drain()
   d=json.loads(await reader.readline());assert d.get('ok'),d.get('error')
   return {'warm':d['result']['warm'],'map_sha256':d['result']['active']['map_sha256']}
  finally:
   writer.close();await writer.wait_closed()
 try:
  for _ in range(100):
   if socket.exists():break
   await asyncio.sleep(.1)
  assert socket.is_socket() and stat.S_IMODE(socket.stat().st_mode)==0o600
  end=time.monotonic()+1800
  out=Path('/opt/transparent-publisher-build/sessions-20260910/forward-trial.ndjson')
  with out.open('x') as stream:
   while time.monotonic()<end:
    start=time.monotonic();r={'utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'probe':'forwarded_status'}
    try:r['value']=await asyncio.wait_for(status(),4)
    except Exception as e:r['error']=type(e).__name__+': '+str(e)
    r['seconds']=time.monotonic()-start;stream.write(json.dumps(r)+'\n');stream.flush()
    await asyncio.sleep(max(.05,.5-r['seconds']))
 finally:
  if p.returncode is None:
   p.terminate();await p.wait()
  socket.unlink(missing_ok=True)
asyncio.run(main())
