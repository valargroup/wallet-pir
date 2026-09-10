import json,pathlib,datetime
out={'captured_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'window_from':'2026-09-10T22:46:45','window_to_exclusive':'2026-09-10T22:47:05','probes':{}}
for name in ['unix','http','threads','pressure','runnable']:
 rows=[]
 for line in pathlib.Path('/run/tp-probe-candidate/'+name+'.ndjson').read_text().splitlines():
  try:r=json.loads(line)
  except ValueError:continue
  if out['window_from']<=r['utc']<out['window_to_exclusive']:rows.append(r)
 out['probes'][name]=rows
print(json.dumps(out))
