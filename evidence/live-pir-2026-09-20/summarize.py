import pathlib,json,statistics,math,collections,gzip
p=pathlib.Path(__file__).resolve().parent

def stats(values):
 if not values:return None
 a=sorted(values)
 return {'n':len(a),'mean':statistics.mean(a),'p50':statistics.median(a),'p95':a[math.ceil(len(a)*.95)-1],'min':a[0],'max':a[-1]}

def delta(v,term,endpoint=None):
 def total(side,suffix):
  return sum(value for key,value in v[side].items() if term+suffix+'{' in key and (endpoint is None or f'endpoint="{endpoint}"' in key))
 return total('after','_count')-total('before','_count'),1000*(total('after','_sum')-total('before','_sum'))

out={}
for file in sorted(p.glob('*-r[123].jsonl*')):
 name=file.name.rsplit('-r',1)[0];r=out.setdefault(name,{'runs':[],'queries':[],'errors':[],'setup_bytes':[]})
 r['runs'].append(file.name)
 for l in (gzip.open(file,'rt').read() if file.suffix=='.gz' else file.read_text()).splitlines():
  v=json.loads(l)
  if v['event']=='setup':r['setup_bytes'].append(v['bytes'])
  if v['event']=='init':r['setup_bytes'].append(v['bytes']);r['generation']=v['session']['generation']
  if v['event']=='query_error':r['errors'].append(v)
  if v['event']=='query' and not v['warmup']:r['queries'].append(v)
 # Early version terminates on transport error and logs it separately.
 err=p/(file.name.split('.jsonl')[0]+'.stderr')
 if err.exists() and err.stat().st_size:r['errors'].append({'fatal':err.read_text()})
for name,r in out.items():
 q=r.pop('queries');r['successful_measured_queries']=len(q);r['all_exact']=all(v['exact'] for v in q)
 r['upload_bytes']=sorted(set(v['upload'] for v in q));r['download_bytes']=sorted(set(v['download'] for v in q))
 for key in ['prepare_s','http_s','decode_s','total_s']:r[key.replace('_s','_ms')]=stats([1000*v[key] for v in q])
 terms={'post_body_server':'enhance_http_request_processing_duration_seconds','worker_rpc':'enhance_worker_replica_request_duration_seconds'} if name=='enhance' else {'evaluation':'transparent_shard_evaluation_seconds','handler':'transparent_shard_query_seconds','queue':'transparent_shard_queue_wait_seconds'}
 for key,term in terms.items():
  ds=[delta(v,term,'query' if key=='post_body_server' else None) for v in q]
  r[key+'_ms']=stats([ms for n,ms in ds if n==1 and ms>=0]);r[key+'_counter_deltas']=dict(collections.Counter(str(n) for n,ms in ds))
(p/'summary.json').write_text(json.dumps(out,indent=2)+'\n')
for name,r in out.items():
 k='post_body_server_ms' if name=='enhance' else 'evaluation_ms'
 print(name,'n=',r['successful_measured_queries'],'errors=',len(r['errors']),'up/down=',r['upload_bytes'],r['download_bytes'],'server=',r[k],'total=',r['total_ms'])
