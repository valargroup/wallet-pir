import concurrent.futures,datetime,hashlib,json,pathlib,time,urllib.request
root=pathlib.Path('/opt/transparent-v10-8e69ea75')
urls={'shard_map':'https://transparent-pir.valargroup.dev/v1/shards','filter_map':'https://enhance-pir.valargroup.dev/v1/filters/shards','init':'https://transparent-pir.valargroup.dev/v1/shards/init'}
def one(item):
 name,url=item
 try:
  with urllib.request.urlopen(url,timeout=5) as r:body=r.read();code=r.status
  data=json.loads(body)
  result={'http':code,'sha256':hashlib.sha256(body).hexdigest(),'schema':data.get('schema')}
  if isinstance(data.get('shards'),list):result.update(shards=len(data['shards']),through=data['shards'][-1]['end_height'])
  else:result.update(shards=data.get('shards'),through=data.get('covered_through'))
  return name,result
 except Exception as e:return name,{'error':str(e)}
with (root/'public-availability.jsonl').open('a',buffering=1) as f,concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
 for _ in range(720):
  start=time.monotonic();f.write(json.dumps({'utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'origins':dict(pool.map(one,urls.items()))})+'\n');time.sleep(max(0,5-(time.monotonic()-start)))
