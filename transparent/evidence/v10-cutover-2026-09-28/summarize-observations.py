import datetime,gzip,json,pathlib,re,sys
root=pathlib.Path(sys.argv[1]);ops=root/'deployment';load=root/'load'
commands=[json.loads(x) for x in (load/'commands.jsonl').read_text().splitlines()]
start=next(x['utc'] for x in commands if x.get('start')=='regression')
end=json.loads((load/'complete.json').read_text())['completed_utc']
load_start=next(x['utc'] for x in commands if x.get('start')=='load-4')
def rows(name):
 with gzip.open(ops/(name+'.jsonl.gz'),'rt') as f:
  for line in f:
   yield json.loads(line)
def summarize(from_utc):
 result={}
 for row in rows('resource-timeline'):
  if not from_utc<=row['utc']<=end:continue
  for w in row['workers']:
   name=w['worker'];s=result.setdefault(name,{'samples':0,'minimum_available_memory_percent':100,'maximum_memory_current_bytes':0,'maximum_memory_peak_since_service_start_bytes':0,'max_auto_restarts':0,'max_oom':0,'max_oom_kill':0,'readiness_non_200_samples':0,'readiness_false_samples':0,'max_cache_write_failures':0,'host_capture_errors':0})
   h=w.get('host','');s['samples']+=1
   a=re.search(r'^MemAvailable:\s+(\d+)',h,re.M);t=re.search(r'^MemTotal:\s+(\d+)',h,re.M)
   if a and t:s['minimum_available_memory_percent']=min(s['minimum_available_memory_percent'],100*int(a[1])/int(t[1]))
   else:s['host_capture_errors']+=1
   for field,pattern in [('maximum_memory_current_bytes',r'^MemoryCurrent=(\d+)'),('maximum_memory_peak_since_service_start_bytes',r'^MemoryPeak=(\d+)'),('max_auto_restarts',r'^NRestarts=(\d+)'),('max_oom',r'^oom (\d+)'),('max_oom_kill',r'^oom_kill (\d+)')]:
    m=re.search(pattern,h,re.M)
    if m:s[field]=max(s[field],int(m[1]))
   r=w.get('v1/ready',{})
   if r.get('http')!=200:s['readiness_non_200_samples']+=1
   try:
    q=json.loads(r.get('body','{}'))
    if q.get('ready') is not True:s['readiness_false_samples']+=1
    s['max_cache_write_failures']=max(s['max_cache_write_failures'],q.get('runtime_cache',{}).get('write_failures',0))
   except ValueError:s['readiness_false_samples']+=1
 fresh=[];controller_errors=0
 for r in rows('health-timeline'):
  if not from_utc<=r['utc']<=end:continue
  c=r['services']['controller']
  if c.get('http')!=200:controller_errors+=1
  f=c.get('body',{}).get('freshness_seconds')
  if f is not None:fresh.append(f)
 public={'samples':0,'http_error_samples':0,'map_digest_skew_samples':0}
 for r in rows('public-availability'):
  if not from_utc<=r['utc']<=end:continue
  public['samples']+=1;o=r['origins']
  if any(v.get('http')!=200 for v in o.values()):public['http_error_samples']+=1
  if o['shard_map'].get('http')==200 and o['filter_map'].get('http')==200 and o['shard_map']['sha256']!=o['filter_map']['sha256']:public['map_digest_skew_samples']+=1
 return {'from_utc':from_utc,'through_utc':end,'workers':result,'controller_http_error_samples':controller_errors,'maximum_reported_freshness_seconds':max(fresh) if fresh else None,'public_origins':public}
out={'all_client_validation':summarize(start),'four_client_load':summarize(load_start),'interpretation':'Host resources sampled about every 30 seconds, controller/worker health every 10 seconds, public origins every 5 seconds. MemoryPeak is cumulative since service start; MemoryCurrent is sampled in this window. These are bounded observations, not sustained capacity or uninterrupted-availability proof.'}
(root/'observation-summary.json').write_text(json.dumps(out,indent=2)+'\n')
print(json.dumps(out,indent=2))
