import datetime,json,pathlib,subprocess,time
pid=int(subprocess.check_output(['systemctl','show','transparent-shard-server','-p','MainPID','--value']));assert pid>0
root=pathlib.Path('/run/tp-probe-candidate')
end=time.monotonic()+3600
with (root/'runnable.ndjson').open('x') as out:
 while time.monotonic()<end:
  start=time.monotonic();rows=[]
  for task in pathlib.Path('/proc',str(pid),'task').iterdir():
   try:
    fields=(task/'stat').read_text().rsplit(')',1)[1].split()
    if fields[0]!='R':continue
    rows.append({'tid':int(task.name),'cpu':int(fields[36]),'schedstat':(task/'schedstat').read_text().strip(),'stack':(task/'stack').read_text()})
   except OSError:pass
  if rows:
   out.write(json.dumps({'utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'pid':pid,'seconds':time.monotonic()-start,'threads':rows})+'\n');out.flush()
  time.sleep(max(.01,.5-(time.monotonic()-start)))
