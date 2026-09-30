import concurrent.futures, json, os, platform, subprocess, threading, time
from pathlib import Path
root=Path.cwd()
out=Path.home()/'.ai-runbook/dev-ux/checks/wallet-pir-122-followup'
command=['make','check-package','PACKAGE=pir-control','TEST=identity','OFFLINE=1']
env=os.environ.copy(); env['CARGO_TARGET_DIR']=str(out/'target')
def pair(name):
 barrier=threading.Barrier(2)
 epoch=time.monotonic()
 def run(index):
  barrier.wait(); start=time.monotonic()
  p=subprocess.Popen(command,env=env,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True)
  lines=[]; rows=[]
  for line in p.stdout:
   elapsed=round(time.monotonic()-epoch,6)
   lines.append(line); rows.append({'at_seconds':elapsed,'line':line.rstrip()})
  status=p.wait(); end=time.monotonic()
  (out/f'{name}-{index}.log').write_text(''.join(lines))
  targets=[json.loads(r['line'].removeprefix('TARGET_LEASE ')) for r in rows if r['line'].startswith('TARGET_LEASE ')]
  cargo=[]; active=None
  for row in rows:
   if row['line'].startswith('+ cargo test'):
    active=row['at_seconds']
   elif row['line'].startswith('CHECK_STAGE ') and active is not None:
    record=json.loads(row['line'].removeprefix('CHECK_STAGE '));cargo.append({'start_seconds':active,'end_seconds':row['at_seconds'],**record});active=None
  waits=[r for r in rows if 'Blocking waiting for file lock' in r['line']]
  return {'index':index,'start_seconds':round(start-epoch,6),'end_seconds':round(end-epoch,6),'elapsed_seconds':round(end-start,6),'exit':status,'targets':targets,'cargo_intervals':cargo,'lock_waits':waits}
 with concurrent.futures.ThreadPoolExecutor(2) as pool:
  runs=list(pool.map(run,range(2)))
 record={'sha':subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),'host':platform.platform(),'architecture':platform.machine(),'command':command,'environment':{'CARGO_TARGET_DIR':str(out/'target'),'RUSTFLAGS':env.get('RUSTFLAGS'),'CFLAGS':env.get('CFLAGS'),'CXXFLAGS':env.get('CXXFLAGS')},'runs':runs}
 record['cargo_overlap_seconds']=[round(min(a['end_seconds'],b['end_seconds'])-max(a['start_seconds'],b['start_seconds']),6) for a in runs[0]['cargo_intervals'] for b in runs[1]['cargo_intervals'] if min(a['end_seconds'],b['end_seconds'])>max(a['start_seconds'],b['start_seconds'])]
 (out/f'{name}.json').write_text(json.dumps(record,indent=2)+'\n')
 print(json.dumps(record,indent=2),flush=True)
 if any(r['exit'] for r in runs): raise SystemExit(1)
pair('after-warmup-pair')
pair('after-concurrent')
