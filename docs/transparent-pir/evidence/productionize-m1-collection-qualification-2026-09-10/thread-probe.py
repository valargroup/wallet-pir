import datetime,json,subprocess,time
from pathlib import Path
pid=int(subprocess.check_output(['systemctl','show','transparent-shard-server','-p','MainPID','--value']))
assert pid>0
root=Path('/proc')/str(pid)/'task'
out=Path('/tmp/m1-thread-probe.ndjson')
end=time.monotonic()+5400
with out.open('x') as f:
 while time.monotonic()<end and root.exists():
  start=time.monotonic();rows=[]
  for task in root.iterdir():
   try:
    fields=(task/'stat').read_text().rsplit(')',1)[1].split()
    row={'tid':int(task.name),'state':fields[0],'wchan':(task/'wchan').read_text().strip(),'schedstat':(task/'schedstat').read_text().strip()}
    if fields[0]=='D':
     try:row['kernel_stack']=(task/'stack').read_text()
     except OSError:pass
    rows.append(row)
   except (OSError,IndexError):pass
  f.write(json.dumps({'utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'pid':pid,'threads':rows,'sample_seconds':time.monotonic()-start})+'\n');f.flush()
  time.sleep(max(.01,.25-(time.monotonic()-start)))
