"""Summarize a frozen prefix of the continuing query/health logs."""
import bisect,collections,gzip,json,pathlib,sys
root=pathlib.Path(sys.argv[1]);events=[];health=[]
for pattern,items in [('queries-*.jsonl.gz',events),('health-*.jsonl.gz',health)]:
 for p in sorted(root.glob(pattern)):
  with gzip.open(p,'rt') as f:items.extend(json.loads(line) for line in f)
starts=sorted(e['unix'] for e in events if e['event']=='ready')
queries=[e for e in events if e['event']=='query'];errors=[e for e in events if e['event']=='error']
def summarize(qs):
 if not qs:return {'queries':0}
 ts=sorted(e['unix'] for e in qs);lat=sorted(e['http_seconds'] for e in qs)
 def pct(p):return lat[int((len(lat)-1)*p)]
 return {'queries':len(qs),'exact':sum(e.get('exact') is True for e in qs),'nonempty_exact':sum(e.get('nonempty',False) for e in qs),'start_unix':ts[0],'last_start_unix':ts[-1],'start_rate_qps':(len(ts)-1)/(ts[-1]-ts[0]) if len(ts)>1 else None,'minimum_start_spacing_seconds':min((b-a for a,b in zip(ts,ts[1:])),default=None),'http_p50_seconds':pct(.5),'http_p95_seconds':pct(.95),'http_p99_seconds':pct(.99),'http_max_seconds':max(lat),'shards_queried':len(set(e['shard'] for e in qs)),'recovered_queries':sum(e.get('recovered',False) for e in qs),'class_counts':dict(collections.Counter(e['geometry']+'/'+e['table'] for e in qs)),'max_schedule_lag_seconds':max(e['schedule_lag_seconds'] for e in qs)}
runs=[summarize([e for e in queries if bisect.bisect_right(starts,e['unix'])-1==i]) for i in range(len(starts))]
workers={};max_fresh=0;max_cycle=0;public_errors=0;heights=[]
for sample in health:
 c=sample['controller'].get('body',{});max_fresh=max(max_fresh,c.get('freshness_seconds',0));max_cycle=max(max_cycle,c.get('cycle_seconds',0))
 if 'public_height' in c:heights.append(c['public_height'])
 if sample['public_init'].get('http')!=200:public_errors+=1
 for w in sample['workers']:
  v=workers.setdefault(w['id'],{'samples':0,'minimum_available_memory_percent':100,'max_restarts':0,'max_oom':0,'max_oom_kill':0,'unavailable_resource_samples':0,'not_ready_samples':0})
  v['samples']+=1
  if w.get('available_kib') is not None and w.get('total_kib'):v['minimum_available_memory_percent']=min(v['minimum_available_memory_percent'],100*w['available_kib']/w['total_kib'])
  else:v['unavailable_resource_samples']+=1
  for field in ('restarts','oom','oom_kill'):
   if w.get(field) is not None:v['max_'+field]=max(v['max_'+field],w[field])
  if not w.get('ready',{}).get('body',{}).get('ready'):v['not_ready_samples']+=1
out={'ongoing':True,'events':dict(collections.Counter(e['event'] for e in events)),'errors':errors,'totals':{'queries':len(queries),'exact':sum(e.get('exact') is True for e in queries),'covered_shards':len(set(e['shard'] for e in queries))},'runs':runs,'health':{'samples':len(health),'first_utc':health[0]['utc'] if health else None,'last_utc':health[-1]['utc'] if health else None,'public_height_range':[min(heights),max(heights)] if heights else None,'maximum_reported_cycle_seconds':max_cycle,'maximum_reported_freshness_seconds':max_fresh,'public_init_error_samples':public_errors,'nodes':workers},'limits':'Frozen initial observation prefix; load continues. Separate process runs include intentional operator restarts to install/test the watchdog. Rates exclude those control gaps and preparation. Every logged POST uses a fresh key. The initial runner did not retry; the final runner schedules up to three attempts for a transport interruption, all within the existing 5 QPS budget. Raw failures are retained. Sealed rows only, 80% recent / 20% archive; not whole-wallet recovery or WAN-client latency. Client runs on coordinator under a 2-core quota and Nice=10.'}
(root/'analysis.json').write_text(json.dumps(out,indent=2)+'\n');print(json.dumps(out,indent=2))
