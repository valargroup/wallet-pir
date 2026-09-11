import json,pathlib,collections,datetime
root=pathlib.Path('/srv/transparent-pir/publications')
seen=set()
groups=collections.defaultdict(list)
for p in root.glob('*/*/manifest.json'):
 try:m=json.loads(p.read_text())
 except (FileNotFoundError,json.JSONDecodeError):continue
 if p.parent.name in seen:continue
 seen.add(p.parent.name)
 if m['geometry'].startswith('recent'):
  groups[m['shard_id']].append((m,p.parent.name))
rows=[]
for sid,ms in groups.items():
 ms.sort(key=lambda x:(x[0]['end_height'],x[0]['revision'],x[1]))
 for (a,ad),(b,bd) in zip(ms,ms[1:]):
  rows.append(dict(shard=sid,old=ad,new=bd,old_height=a['end_height'],new_height=b['end_height'],supersedes_previous=b['supersedes']==ad,directory_unchanged=a['directory_segments']==b['directory_segments'],pages_unchanged=a['page_segments']==b['page_segments'],geometry_unchanged=a['geometry']==b['geometry']))
print(json.dumps(dict(utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),scope='retained on-disk recent manifests only; not a complete history or throughput benchmark',recent_manifests=sum(map(len,groups.values())),comparisons=rows),indent=2))
