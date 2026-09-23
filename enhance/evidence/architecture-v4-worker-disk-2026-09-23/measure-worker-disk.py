import hashlib,json,os,subprocess,tempfile,time
from pathlib import Path
out=Path('enhance/evidence/architecture-v4-worker-disk-2026-09-23')
artifacts=[json.loads(line) for line in Path('/tmp/v4-disk-build.jsonl').read_text().splitlines()]
binary=Path(next(v['executable'] for v in artifacts if v.get('reason')=='compiler-artifact' and v.get('target',{}).get('name')=='v4_http' and v.get('executable')))
peaks={}
command=[str(binary),'--ignored','--test-threads=1','--nocapture']
started=time.time()
with tempfile.TemporaryDirectory(prefix='v4-disk-measure-') as temp, (out/'tests.log').open('w') as log, (out/'samples.jsonl').open('w') as samples:
 env=dict(os.environ,TMPDIR=temp,RUST_TEST_THREADS='1')
 child=subprocess.Popen(command,env=env,stdout=log,stderr=subprocess.STDOUT)
 print(json.dumps({'pid':child.pid,'temporary_directory':temp}),flush=True)
 while child.poll() is None:
  totals={}
  for directory in Path(temp).glob('*/*'):
   if not directory.is_dir(): continue
   key=directory.name
   size=blocks=files=0
   for folder,_,names in os.walk(directory):
    for name in names:
     try: st=(Path(folder)/name).stat()
     except FileNotFoundError: continue
     size+=st.st_size;blocks+=st.st_blocks*512;files+=1
   totals[key]={'logical_bytes':size,'allocated_bytes':blocks,'files':files}
   peak=peaks.setdefault(key,{'logical_bytes':0,'allocated_bytes':0})
   for metric in peak: peak[metric]=max(peak[metric],totals[key][metric])
  samples.write(json.dumps({'elapsed_seconds':round(time.time()-started,2),'directories':totals})+'\n');samples.flush()
  time.sleep(1)
 code=child.wait()
source={str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(Path('enhance/services/enhance-pir-server/src/v4').glob('*.rs'))}
result={'command':command,'binary_sha256':hashlib.sha256(binary.read_bytes()).hexdigest(),'source_sha256':source,'exit_code':code,'elapsed_seconds':time.time()-started,'peaks':peaks,'sample_period_seconds':1,'qualification':'unqualified','limitations':['macOS filesystem and native tests; not c-4 hardware','one-second sampling can miss short-lived peaks','full-size integration scenarios, not six-hour campaign']}
(out/'results.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps(result),flush=True)
raise SystemExit(code)
