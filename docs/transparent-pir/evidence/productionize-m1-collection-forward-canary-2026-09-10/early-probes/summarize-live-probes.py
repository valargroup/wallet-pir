import json, pathlib, datetime
out={'utc':datetime.datetime.now(datetime.timezone.utc).isoformat()}
for name in ['unix','http']:
 rows=[]
 for line in pathlib.Path('/run/tp-probe-candidate/'+name+'.ndjson').read_text().splitlines():
  try: rows.append(json.loads(line))
  except ValueError: pass
 good=[r for r in rows if 'value' in r]
 worst=sorted(good,key=lambda r:r['value']['socket_seconds'],reverse=True)[:3]
 out[name]={'samples':len(rows),'errors':[r for r in rows if 'error' in r],'worst':worst}
print(json.dumps(out,indent=2))
