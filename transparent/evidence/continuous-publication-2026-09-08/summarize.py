import json,pathlib,re,statistics
root=pathlib.Path('/tmp/transparent-publish-task')
rows=[json.loads(line) for line in (root/'live-blocks.jsonl').read_text().splitlines() if line.startswith('{')]
visible=[x for x in rows if x.get('event')=='block_visible'];samples=[x for x in rows if x.get('event')=='worker_metrics']
result=next((x for x in rows if x.get('event')=='result'),None)
workers={}
for sample in samples:
 for worker,values in sample['workers'].items():
  if not isinstance(values,list):continue
  out=workers.setdefault(worker,{})
  for line in values:
   name=line.split('{')[0];value=float(line.rsplit(' ',1)[1]);aggregate=out.setdefault(name,{'min':value,'max':value,'first':value,'last':value});aggregate['min']=min(aggregate['min'],value);aggregate['max']=max(aggregate['max'],value);aggregate['last']=value
controls={}
for sample in samples:
 for worker,values in sample.get('control',{}).items():
  out=controls.setdefault(worker,{})
  for name in ['retired_snapshots','revision_count']:
   if name not in values:continue
   value=values[name];aggregate=out.setdefault(name,{'min':value,'max':value,'first':value,'last':value});aggregate['min']=min(aggregate['min'],value);aggregate['max']=max(aggregate['max'],value);aggregate['last']=value
heights=[x['height'] for x in visible]
output={'start':next(x for x in rows if x.get('event')=='start'),'result':result,'height_start':min(heights) if heights else None,'height_end':max(heights) if heights else None,'consecutive':sorted(heights)==list(range(min(heights),max(heights)+1)) if heights else False,'latencies':[{'height':x['height'],'seconds':x['seconds'],'ready_replicas':x['status'].get('ready_replicas')} for x in visible],'exceptions':[x for x in rows if x.get('event') in ['error','orphan_advertised','origins_differ_during_sample']],'worker_metric_samples':len(samples),'sampled_worker_metrics':workers,'control_counts':controls}
(root/'live-results.json').write_text(json.dumps(output,indent=2)+'\n')
print(json.dumps({k:output[k] for k in ['result','height_start','height_end','consecutive','worker_metric_samples']},indent=2))
