import pathlib,json,hashlib,os,subprocess,datetime
root=pathlib.Path('/srv/zakura/transparent-publications')
out=pathlib.Path('/srv/zakura/m1-full-fixture-20260909-4');out.mkdir(exist_ok=False)
worker='transparent-pir-recent-01'
candidates=[]
for d in root.glob('candidate-*'):
 try:
  raw=(d/'shards.json').read_bytes();m=json.loads(raw);sha=hashlib.sha256(raw).hexdigest()
  a=pathlib.Path('/opt/transparent-publisher/state')/(sha+'.assignment.json')
  candidates.append((m['shards'][-1]['end_height'],d,sha,a,m))
 except FileNotFoundError:pass
selected=sorted(candidates,key=lambda x:x[0])[-3:]
assert len(selected)==3
records=[]
for index,(height,d,sha,a,m) in enumerate(selected):
 dest=out/str(index);dest.mkdir()
 if not a.exists():
  a=out/(str(index)+'-assignment.json')
  subprocess.run(['/usr/local/bin/shard-assign','plan','--shard-dir',str(d),'--roster','/opt/transparent-publisher/roster.json','--recent-from-height','3262749','--headroom','0.05','--out-assignment',str(a)],check=True,stdout=subprocess.DEVNULL)
 assignment=json.loads(a.read_text());w=next(w for w in assignment['workers'] if w['id']==worker)
 assert len(w['shards'])==14
 files=subprocess.check_output(['/usr/local/bin/shard-assign','files','--shard-dir',str(d),'--assignment',str(a),'--worker-id',worker],text=True).splitlines()
 for name in files:
  src=d/name
  sources=list(src.rglob('*')) if src.is_dir() else [src]
  for source in sources:
   if not source.is_file():continue
   target=dest/source.relative_to(d);target.parent.mkdir(parents=True,exist_ok=True)
   if not target.exists():os.link(source,target)
 (dest/'assignment.json').write_bytes(a.read_bytes())
 records.append({'directory':str(index),'assignment':str(index)+'/assignment.json','map_sha256':assignment['set']['map_sha256'],'source_map_file_sha256':sha,'height':height,'hash':m['shards'][-1]['terminal_block_hash'],'assigned_shards':w['shards']})
assert all(r['assigned_shards']==records[0]['assigned_shards'] for r in records)
assert len({r['height'] for r in records})==3
(out/'fixture.json').write_text(json.dumps({'worker_id':worker,'query_shard':records[-1]['assigned_shards'][-1],'publications':records,'captured_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'scope':'frozen recent assignment; no live authority'},indent=2)+'\n')
print((out/'fixture.json').read_text())
