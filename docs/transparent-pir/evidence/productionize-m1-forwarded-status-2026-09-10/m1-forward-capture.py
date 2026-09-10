import asyncio,datetime,importlib.util,json,shlex
from pathlib import Path
s=importlib.util.spec_from_file_location('fleet','/opt/transparent-publisher/transparent-live-fleet.py');m=importlib.util.module_from_spec(s);s.loader.exec_module(m)
root=Path('/opt/transparent-publisher-build/sessions-20260910')
out=root/'forward-capture';out.mkdir()
code='''import json
from pathlib import Path
result={}
for name in ['m1-status-stall-worker.ndjson','m1-thread-probe.ndjson']:
 with Path('/tmp',name).open() as stream:
  result[name]=''.join(line for line in stream if '\"utc\": \"2026-09-10T21:00:0' in line)
print(json.dumps(result))'''
async def main():
 f=m.Fleet(json.loads(Path('/opt/transparent-publisher/fleet.json').read_text()));w=next(w for w in f.roster if w['id']=='transparent-pir-recent-01')
 rows=json.loads(await f.ssh(w['ssh_host'],'python3 -c '+shlex.quote(code)))
 for name,value in rows.items():(out/name).write_text(value)
 with (root/'stall-probe.ndjson').open() as stream:
  (out/'coordinator-window.ndjson').write_text(''.join(line for line in stream if '\"utc\": \"2026-09-10T21:00:0' in line))
 for name in ['forward-trial.ndjson','owned-forward-trial-v2.ndjson']:
  lines=[]
  for line in (root/name).read_text().splitlines():
   try:json.loads(line)
   except ValueError:continue
   lines.append(line)
  (out/name).write_text('\n'.join(lines)+'\n')
 (out/'capture.json').write_text(json.dumps({'captured_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'scope':'fixed pause window plus initial forwarding samples; trials still running'},indent=2)+'\n')
asyncio.run(main())
