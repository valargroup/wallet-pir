import json,pathlib,urllib.request,hashlib,datetime,subprocess
base=pathlib.Path('/srv/zakura/transparent-publications')
active=json.load(open(base/'active.json'));root=pathlib.Path(active['directory'])
mp=json.load(urllib.request.urlopen('http://10.142.0.10:8093/v1/shards'))
result={'utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'active':active,'map':mp,'shards':[],'expected':{}}
for entry in mp['shards']:
 d=root/entry['manifest_digest']
 if not d.exists():
  d=next(x/entry['manifest_digest'] for x in base.iterdir() if (x/entry['manifest_digest']/'manifest.json').exists())
 manifest=json.load(open(d/'manifest.json'))
 files=[]
 for f in d.iterdir():
  if f.is_file():
   s=f.stat();files.append({'name':f.name,'bytes':s.st_size,'allocated':s.st_blocks*512})
 result['shards'].append({'entry':entry,'manifest':manifest,'files':files})
 if entry['shard_id'] in [0,80,160]:
  hashes={}
  for table,field in [('directory','directory_segments'),('pages','page_segments')]:
   hashes[table]=[]
   for segment in range(entry[field]):
    with open(d/f'{table}.{segment}.bin','rb') as f:
     hashes[table].append([hashlib.sha256(f.read(3584)).hexdigest() for _ in range(30)])
  result['expected'][str(entry['shard_id'])]=hashes
f=pathlib.Path('/srv/zakura/enhance-data-v7/enhance/records.bin');s=f.stat()
result['enhance']={'record_file_bytes':s.st_size,'allocated_bytes':s.st_blocks*512}
with open(f,'rb') as stream:result['enhance']['hashes']=[hashlib.sha256(stream.read(6633)).hexdigest() for _ in range(30)]
print(json.dumps(result))
