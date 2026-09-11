import datetime,json
from pathlib import Path
cutoff='2026-09-11T00:28:36.552'
out={'utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'cutoff':cutoff}
for root,names in [('tp-publication-io-probe',['unix','http']),('transparent-control-path-probe',['unix','http']),('transparent-dedicated-control-probe',['unix']),('tp-tcp-stages',['samples'])]:
 for name in names:
  path=Path('/run',root,name+'.ndjson')
  if not path.exists():continue
  rows=[]
  for line in path.read_text().splitlines():
   try:rows.append(json.loads(line))
   except ValueError:pass
  groups={}
  for label,subset in [('before',[r for r in rows if r['utc']<cutoff]),('after',[r for r in rows if r['utc']>=cutoff])]:
   groups[label]={'samples':len(subset),'errors':sum('error'in r for r in subset),'max_seconds':max((r.get('value',{}).get('socket_seconds',r['seconds']) for r in subset),default=None)}
  out[root+'/'+name]=groups
print(json.dumps(out,indent=2))
