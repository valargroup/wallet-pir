import asyncio,hashlib,importlib.util,json,stat,time
from pathlib import Path
source=Path('/opt/transparent-publisher-build/sessions-20260910/transparent-live-fleet.forwarding.py')
s=importlib.util.spec_from_file_location('fleet',source);m=importlib.util.module_from_spec(s);s.loader.exec_module(m)
async def main():
 root=Path('/run/tp-forward-test');root.mkdir(mode=0o700)
 c=json.loads(Path('/opt/transparent-publisher/fleet.json').read_text())
 roster=json.loads(Path(c['roster']).read_text());w=next(w for w in roster if w['id']=='transparent-pir-recent-01')
 (root/'roster.json').write_text(json.dumps([w]))
 c.update(roster=str(root/'roster.json'),state_dir=str(root),control_sessions=True,status_socket_forwarding=True)
 f=m.Fleet(c);owner=asyncio.create_task(f.serve_control_sessions())
 results={'source_sha256':hashlib.sha256(source.read_bytes()).hexdigest()}
 async def available():
  deadline=time.monotonic()+10
  while time.monotonic()<deadline:
   if owner.done():await owner
   try:
    status=await f.control(w,{'operation':'status'})
    if status['warm']:return status
   except (OSError,RuntimeError,TimeoutError):pass
   await asyncio.sleep(.05)
  raise TimeoutError('forward did not become available')
 try:
  start=time.monotonic();await available();results['startup_seconds']=time.monotonic()-start
  results['socket_mode']=oct(stat.S_IMODE(f.status_forward_path(w).stat().st_mode))
  assert results['socket_mode']=='0o600'
  samples=[]
  for _ in range(20):
   start=time.monotonic();value=await f.control(w,{'operation':'status'});samples.append(time.monotonic()-start);assert value['warm']
  results['status_seconds']=samples
  await m.run(f.control_session_args(w)+['-O','exit','root@'+w['ssh_host']],timeout=3)
  start=time.monotonic();await available();results['reconnect_seconds']=time.monotonic()-start
  assert results['reconnect_seconds']<3
 finally:
  owner.cancel()
  try:await owner
  except asyncio.CancelledError:pass
 # No listener may survive its owner, even if its filesystem name remains stale.
 try:
  _,writer=await asyncio.open_unix_connection(f.status_forward_path(w))
 except (FileNotFoundError,ConnectionRefusedError):results['owner_shutdown_closed_forward']=True
 else:
  writer.close();await writer.wait_closed();raise AssertionError('forward outlived owner')
 out=Path('/opt/transparent-publisher-build/sessions-20260910/forward-integration.json')
 out.write_text(json.dumps(results,indent=2)+'\n');print(json.dumps(results),flush=True)
asyncio.run(main())
