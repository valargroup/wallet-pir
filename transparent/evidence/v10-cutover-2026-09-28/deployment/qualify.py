#!/usr/bin/env python3
"""Build and verify an isolated v10 publication. Never activates services."""
import concurrent.futures,datetime,hashlib,json,os,pathlib,subprocess,time,threading
root=pathlib.Path('/opt/transparent-v10-8e69ea75')
bins=root/'bin'
pub=pathlib.Path('/srv/zakura/transparent-shards-v10-full')
journal=pathlib.Path('/srv/zakura/transparent-event-snapshot-v10-8e69ea75')
sha='8e69ea75b1e0071e3b978b0c78cc0487377f9e82'
through=3499341
anchor='0000000000941f2c717bea13c62af6ef787e4a5b9e3fd90fd304d746b10d63de'
cutoff=3262749
out=root/'qualification'
out.mkdir(exist_ok=True)

def run(name,args,env=None):
 print(json.dumps({'start':name,'utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'command':list(map(str,args))}),flush=True)
 start=time.time()
 with (out/(name+'.log')).open('w') as stream:
  subprocess.run(list(map(str,args)),stdout=stream,stderr=subprocess.STDOUT,check=True,env=env)
 print(json.dumps({'complete':name,'seconds':time.time()-start}),flush=True)

cert=pathlib.Path('/root/.cargo/git/checkouts/ipir-sp-ecbe94d2bf94a34c/1f2aec6/reinspiring/tools/security/certify_native.py')
certdir=out/'certificates';certdir.mkdir(exist_ok=False)
def certify(item):
 entry,path,table=item
 name=f"s{entry['shard_id']}-{table}-{path.stem.split('.')[-1]}"
 report=certdir/(name+'.report.json'); result=certdir/(name+'.certificate.json')
 env=dict(os.environ,RAYON_NUM_THREADS='1')
 with report.open('w') as stdout,(certdir/(name+'.log')).open('w') as stderr:
  subprocess.run([str(bins/'native_certificate'),'segment','--geometry',entry['geometry'],'--table',table,'--rows-bin',str(path)],stdout=stdout,stderr=stderr,check=True,env=env)
 with result.open('w') as stdout:
  subprocess.run(['python3',str(cert),str(report),'--require-bits','83' if entry['geometry']=='archive-wide' and table=='pages' else '128'],stdout=stdout,stderr=subprocess.PIPE,check=True)
 c=json.loads(result.read_text()); r=json.loads(report.read_text())
 assert r['database_sha256']=='rows_sha256:'+entry['expected_sha256'], (name,'table hash mismatch')
 bits=c['actual_profile']['certified_failure_bits']
 assert bits>=83, (name,bits)
 if entry['geometry']!='archive-wide' or table!='pages': assert bits>=128,(name,bits)
 return {'name':name,'shard_id':entry['shard_id'],'manifest_digest':entry['manifest_digest'],'geometry':entry['geometry'],'table':table,'file':str(path),'rows_sha256':r['database_sha256'],'public_sha256':r['served_public_sha256'],'failure_bits':bits,'meets_128':bits>=128}
if pub.exists(): raise RuntimeError('candidate path already exists; inspect it before choosing a new attempt')
stop=threading.Event()
pending={}
watch_errors=[]
pool=concurrent.futures.ThreadPoolExecutor(max_workers=2)
def discover():
 if not pub.exists(): return
 for manifest_path in sorted(pub.glob('*/manifest.json')):
  entry=json.loads(manifest_path.read_text())
  entry['manifest_digest']=manifest_path.parent.name
  for table,field in [('directory','directory_segments'),('pages','page_segments')]:
   for index,segment in enumerate(entry[field]):
    path=manifest_path.parent/(table+'.'+str(index)+'.bin')
    key=str(path)
    if key in pending or not path.exists(): continue
    if path.stat().st_size != segment['rows']*segment['row_bytes']: continue
    item=dict(entry,expected_sha256=segment['sha256'])
    pending[key]=pool.submit(certify,(item,path,table))
def watch():
 try:
  while not stop.wait(2): discover()
 except BaseException as error:
  watch_errors.append(error)
watcher=threading.Thread(target=watch)
watcher.start()
try:
 run('publish',['/usr/bin/time','-v','-o',out/'publish-time.txt',bins/'shard-publish','--data-dir',journal,'--output',pub,'--through',through,'--recent-geometry','recent-4k-8k','--archive-geometry','archive-wide','--recent-from',cutoff,'--range-profile','zcash-transparent-range-v2','--directory-choice','all','--zakura-cookie','/dev/null','--record',out/'publication.json','--source-sha',sha])
 raw=(pub/'shards.json').read_bytes(); m=json.loads(raw)
 assert m['shards'][0]['start_height']==0 and m['shards'][-1]['end_height']==through
 assert m['shards'][-1]['terminal_block_hash']==anchor
 census=[json.loads(line) for line in pathlib.Path('/tmp/wallet-pir-v10-layout-census.jsonl').read_text().splitlines()]
 assert census[-1]['type']=='summary' and census[-1]['end']==through
 assert len(m['shards'])==census[-1]['v10_shards']
 table_bytes=0
 for entry,expected in zip(m['shards'],census[:-1]):
  assert (entry['shard_id'],entry['start_height'],entry['end_height'],entry['geometry'],entry['directory_segments'],entry['page_segments'])==(expected['id'],expected['start'],expected['end'],expected['geometry'],expected['directory_segments'],expected['page_segments'])
  manifest=json.loads((pub/entry['manifest_digest']/'manifest.json').read_text())
  assert manifest['occupancy']['events']==expected['events']
  assert manifest['occupancy']['page_rows']==expected['page_rows']
  table_bytes+=sum(path.stat().st_size for table in ['directory','pages'] for path in (pub/entry['manifest_digest']).glob(table+'.*.bin'))
 assert table_bytes==census[-1]['v10_allocated_plaintext_bytes']
 (out/'census-publication-match.json').write_text(json.dumps({'source_sha':sha,'all_shards_match_census':True,'shards':len(m['shards']),'events':census[-1]['events'],'allocated_plaintext_table_bytes':table_bytes,'range_inclusive':[0,through]},indent=2)+'\n')
 run('verify',[bins/'shard-verify','--shard-dir',pub,'--publication',out/'publication.json','--data-dir',journal,'--rebuild','8','--seed','10','--source-sha',sha,'--out',out/'verify.json'])
finally:
 stop.set();watcher.join()
if watch_errors: raise watch_errors[0]
discover()
results=[]
expected_paths=set()
for entry in m['shards']:
 manifest=json.loads((pub/entry['manifest_digest']/'manifest.json').read_text())
 for table,field in [('directory','directory_segments'),('pages','page_segments')]:
  for index in range(len(manifest[field])):
   expected_paths.add(str(pub/entry['manifest_digest']/(table+'.'+str(index)+'.bin')))
assert set(pending)==expected_paths, 'certified segment inventory differs from final publication'
for key in sorted(pending):
 result=pending[key].result()
 results.append(result)
 print(json.dumps({'certificate':result['name'],'failure_bits':result['failure_bits'],'done':len(results),'total':len(pending)}),flush=True)
pool.shutdown()
(certdir/'summary.json').write_text(json.dumps({'source_sha':sha,'certificate_script_sha256':hashlib.sha256(cert.read_bytes()).hexdigest(),'segments':results,'minimum_bits':min(r['failure_bits'] for r in results),'below_128':[r for r in results if not r['meets_128']]},indent=2)+'\n')
run('regression-export',[bins/'regression-export','--data-dir',journal,'--map',pub/'shards.json','--cases',root/'source/transparent/tools/transparent-regression/fixtures/mainnet-cases.json','--cutoff-height',cutoff,'--source-sha',sha,'--out',out/'mainnet-v10.json'])
run('fixture-compare',['python3',root/'source/transparent/ops/scripts/compare-regression-fixtures.py','--previous',root/'source/transparent/tools/transparent-regression/fixtures/mainnet.json','--next',out/'mainnet-v10.json','--out',out/'fixture-compare.json'])
comparison=json.loads((out/'fixture-compare.json').read_text())
assert not comparison['blocking'] and not comparison['review'], comparison
(out/'complete.json').write_text(json.dumps({'source_sha':sha,'shards':len(m['shards']),'certificate_segments':len(results),'completed_utc':datetime.datetime.now(datetime.timezone.utc).isoformat()},indent=2)+'\n')
print('QUALIFICATION COMPLETE',flush=True)
