import datetime,hashlib,json,pathlib
root=pathlib.Path('/srv/zakura/transparent-shards-v10-full')
out=pathlib.Path('/opt/transparent-5qps-20260929');out.mkdir(exist_ok=True)
m=json.loads((root/'shards.json').read_text());tables=[]
for e in m['shards']:
 if not e['sealed']:continue
 d=root/e['manifest_digest'];manifest=json.loads((d/'manifest.json').read_text());assert manifest['schema']=='transparent-shard-v10' and manifest['sealed']
 for name,key in [('directory','directory_segments'),('pages','page_segments')]:
  segments=manifest[key];rows=segments[0]['rows'];width=segments[0]['row_bytes']
  assert all(s['rows']==rows and s['row_bytes']==width for s in segments)
  files=[(d/f'{name}.{i}.bin').open('rb') for i in range(len(segments))]
  indices={0,rows-1,*[i*(rows-1)//31 for i in range(32)]}
  # Include an occupied row independently of the evenly spaced sample.
  first=None
  for i in range(rows):
   files[0].seek(i*width)
   if any(files[0].read(width)):first=i;indices.add(i);break
  assert first is not None
  samples=[]
  for i in sorted(indices):
   data=[]
   for f in files:
    f.seek(i*width);b=f.read(width);assert len(b)==width;data.append(b)
   samples.append({'row':i,'sha256':[hashlib.sha256(b).hexdigest() for b in data],'nonempty':any(any(b) for b in data)})
  for f in files:f.close()
  tables.append({'shard_id':e['shard_id'],'revision':e['manifest_digest'],'geometry':e['geometry'],'table':name,'rows':rows,'row_bytes':width,'segments':len(segments),'samples':samples,'source_table_sha256':[s['sha256'] for s in segments]})
fixture={'schema':'transparent-shard-v10','created_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'source':str(root),'source_map_sha256':hashlib.sha256((root/'shards.json').read_bytes()).hexdigest(),'oracle':'Row hashes read directly from previously verified published plaintext; sealed revisions only. No row bytes logged.','tables':tables}
p=out/'fixture.json';assert not p.exists();p.write_text(json.dumps(fixture,separators=(',',':'))+'\n')
print(json.dumps({'tables':len(tables),'samples':sum(len(t['samples']) for t in tables),'fixture_sha256':hashlib.sha256(p.read_bytes()).hexdigest()}))
