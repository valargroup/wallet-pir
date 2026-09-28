import concurrent.futures,datetime,json,pathlib,time,urllib.request
root=pathlib.Path('/opt/transparent-v10-8e69ea75')
roster=json.loads((root/'pre-roster.json').read_text())
urls={'controller':'http://127.0.0.1:8094/v1/status','public_shard':'https://transparent-pir.valargroup.dev/v1/shards/init'}
urls.update({w['id']:'http://'+w['upstream']+'/v1/ready' for w in roster})
def fetch(item):
 name,url=item
 try:
  with urllib.request.urlopen(url,timeout=5) as r: body=json.load(r)
  return name,{'http':200,'body':body}
 except Exception as e: return name,{'error':str(e)}
with (root/'health-timeline.jsonl').open('a',buffering=1) as output,concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
 for _ in range(1080):
  started=time.monotonic()
  row={'utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'services':dict(pool.map(fetch,urls.items()))}
  output.write(json.dumps(row,separators=(',',':'))+'\n')
  time.sleep(max(0,10-(time.monotonic()-started)))
