import asyncio,importlib.util,json,time
from pathlib import Path
s=importlib.util.spec_from_file_location('fleet','/opt/transparent-publisher-build/sessions-20260910/fleet.py');m=importlib.util.module_from_spec(s);s.loader.exec_module(m)
async def main():
 c=json.loads(Path('/opt/transparent-publisher/fleet.json').read_text());c['control_sessions']=True;f=m.Fleet(c);w=next(w for w in f.roster if w['id']=='transparent-pir-recent-01')
 args=f.control_session_args(w);dest='root@'+w['ssh_host'];result={}
 async def cancelled_client():
  t=time.monotonic()
  try:await m.run(args+[dest,'sleep 2'],timeout=.1,file_output=True)
  except TimeoutError:return time.monotonic()-t
  raise AssertionError('test client did not time out')
 task=asyncio.create_task(cancelled_client());await asyncio.sleep(.15)
 t=time.monotonic();d=await f.control(w,{'operation':'status'});result['concurrent_status_seconds']=time.monotonic()-t
 assert d['warm'];assert result['concurrent_status_seconds']<1
 result['cancelled_client_seconds']=await task
 assert result['cancelled_client_seconds']<.5
 await m.run(args+['-O','check',dest],timeout=2)
 # Only the isolated trial owns these new c-* control sockets; production is
 # still configured for direct connections at this point.
 assert not json.loads(Path('/opt/transparent-publisher/fleet.json').read_text()).get('control_sessions',False)
 await m.run(args+['-O','exit',dest],timeout=2)
 t=time.monotonic();deadline=t+8
 while True:
  try:
   d=await f.control(w,{'operation':'status'})
   if d['warm']:break
  except (RuntimeError,TimeoutError):pass
  assert time.monotonic()<deadline,'trial master did not recover'
  await asyncio.sleep(.1)
 result['reconnect_seconds']=time.monotonic()-t
 Path('/opt/transparent-publisher-build/sessions-20260910/lifecycle-file-output.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result))
asyncio.run(main())
