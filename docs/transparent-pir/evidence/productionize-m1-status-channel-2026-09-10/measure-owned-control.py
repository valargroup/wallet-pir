import asyncio,importlib.util,json,time,statistics,datetime
from pathlib import Path
s=importlib.util.spec_from_file_location('fleet','/opt/transparent-publisher-build/sessions-20260910/fleet.py');m=importlib.util.module_from_spec(s);s.loader.exec_module(m)
async def main():
 c=json.loads(Path('/opt/transparent-publisher/fleet.json').read_text());c['control_sessions']=True;f=m.Fleet(c)
 rows=[]
 for _ in range(3):
  for w in f.roster:
   t=time.monotonic();d=await f.control(w,{'operation':'status'});rows.append(dict(worker=w['id'],seconds=time.monotonic()-t,warm=d.get('warm'),invalidated=d.get('invalidated',False)))
 r=dict(utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),samples=rows,maximum=max(x['seconds'] for x in rows),median=statistics.median(x['seconds'] for x in rows))
 Path('/opt/transparent-publisher-build/sessions-20260910/probe.json').write_text(json.dumps(r,indent=2)+'\n');print(json.dumps({k:v for k,v in r.items() if k!='samples'}));print('warm',sum(x['warm'] and not x['invalidated'] for x in rows),'/',len(rows))
asyncio.run(main())
