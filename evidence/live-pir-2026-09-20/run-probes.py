import pathlib,json,subprocess,datetime,time,hashlib
p=pathlib.Path(__file__).resolve().parent;root=p.parents[1];inv=json.loads((p/'inventory.json').read_text());binary=root/'target/release/live-pir-probe'
cases=[('enhance',{'mode':'enhance','url':'https://enhance-pir.valargroup.dev','metrics':['http://127.0.0.1:18080/metrics'],'hashes':inv['enhance']['hashes']})]
for shard in [0,160]:
 entry=next(x['entry'] for x in inv['shards'] if x['entry']['shard_id']==shard)
 for table in ['directory','pages']:
  cfg={'mode':'transparent','url':'https://transparent-pir.valargroup.dev','shard':shard,'revision':entry['manifest_digest'],'geometry':entry['geometry'],'table':table,'segments':entry['directory_segments' if table=='directory' else 'page_segments'],'init_file':str(p/'transparent-worker-init.json'),'hashes':inv['expected'][str(shard)][table],'metrics':[f'http://127.0.0.1:{port}/metrics' for port in ([18094] if shard==0 else [18093,18096,18097,18098])]}
  cases.append((f'transparent-{shard}-{table}',cfg))
manifest={'started':datetime.datetime.now(datetime.timezone.utc).isoformat(),'binary_sha256':hashlib.sha256(binary.read_bytes()).hexdigest(),'steps':[]}
for rep in range(3):
 for name,cfg in cases:
  tag=f'{name}-r{rep+1}';
  if (p/f'{tag}.jsonl').exists():continue
  cfg['count']=30;(p/f'{tag}.config.json').write_text(json.dumps(cfg,indent=2));t=time.monotonic()
  step={'case':tag,'started':datetime.datetime.now(datetime.timezone.utc).isoformat()};manifest['steps'].append(step)
  with open(p/f'{tag}.jsonl','w') as out,open(p/f'{tag}.stderr','w') as err:
   r=subprocess.run([str(binary),str(p/f'{tag}.config.json')],stdout=out,stderr=err,timeout=180)
  step.update(exit_code=r.returncode,seconds=time.monotonic()-t);(p/'run-manifest-continuation.json').write_text(json.dumps(manifest,indent=2));print(tag,step,flush=True)
  if r.returncode:print((p/f'{tag}.stderr').read_text(),flush=True)
print('COMPLETE',flush=True)
