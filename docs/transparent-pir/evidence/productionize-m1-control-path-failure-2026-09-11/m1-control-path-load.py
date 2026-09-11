import datetime,json,pathlib,subprocess,time
out=pathlib.Path('/opt/transparent-publisher-build/publication-io-20260910/control-path-load');out.mkdir()
base=['/opt/transparent-publisher-build/publication-io-20260910/artifacts/soak-query','--url','http://10.142.0.10:8093','--publications','/srv/zakura/transparent-publications','--shard','173','--seconds','600']
logs=[];jobs=[]
(out/'start.json').write_text(json.dumps({'utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'acceptance':False,'clients':2,'seconds':600,'command':base},indent=2)+'\n')
try:
 for i in range(2):
  log=(out/f'query-{i}.ndjson').open('wb');logs.append(log);jobs.append(subprocess.Popen(base,stdout=log,stderr=log))
 codes=[p.wait(timeout=660) for p in jobs]
 (out/'result.json').write_text(json.dumps({'utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'acceptance':False,'exit_codes':codes},indent=2)+'\n')
finally:
 for p in jobs:
  if p.poll() is None:p.terminate()
 for p in jobs:
  try:p.wait(timeout=5)
  except subprocess.TimeoutExpired:p.kill();p.wait()
 for log in logs:log.close()
