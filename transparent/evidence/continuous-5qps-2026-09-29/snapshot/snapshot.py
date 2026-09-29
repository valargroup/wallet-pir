import datetime,gzip,hashlib,json,pathlib,shutil,subprocess,time
root=pathlib.Path('/opt/transparent-5qps-20260929');now=datetime.datetime.now(datetime.timezone.utc)
d=root/'snapshots'/now.strftime('%Y%m%dT%H%M%SZ');d.mkdir(parents=True)
files=[p for p in root.iterdir() if p.is_file() and p.name not in ['rate-query','rate-query-initial','rate-query-next','permit']]
for p in files:
 for attempt in range(20):
  raw=p.read_bytes()
  try:
   if p.name.endswith('.jsonl.gz'):
    decoded=gzip.decompress(raw)
    for line in decoded.splitlines():json.loads(line)
   if p.suffix=='.json':json.loads(raw)
   break
  except (EOFError,OSError,json.JSONDecodeError):time.sleep(.05)
 else:raise RuntimeError('unable to snapshot complete artifact '+p.name)
 (d/p.name).write_bytes(raw)
service=subprocess.check_output(['systemctl','show','transparent-5qps-continuous','-p','ActiveState','-p','NRestarts','-p','MemoryCurrent','-p','MemoryPeak','-p','CPUUsageNSec','-p','ExecMainStatus'],text=True)
meta={'captured_utc':now.isoformat(),'ongoing':True,'query_binary_sha256':hashlib.sha256((root/'rate-query').read_bytes()).hexdigest(),'service':service,'enabled':subprocess.check_output(['systemctl','is-enabled','transparent-5qps-continuous'],text=True).strip(),'server_source_sha':'8e69ea75b1e0071e3b978b0c78cc0487377f9e82','client_build_base_sha':'8e69ea75b1e0071e3b978b0c78cc0487377f9e82','snapshot_directory':str(d)}
(d/'snapshot.json').write_text(json.dumps(meta,indent=2)+'\n')
archive=shutil.make_archive(str(d),'gztar',d)
print(json.dumps({'snapshot':str(d),'archive':archive,'metadata':meta}))
