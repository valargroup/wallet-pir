import concurrent.futures,datetime,json,socket,subprocess,time,urllib.request
from pathlib import Path
root=Path('/run/tp-probe-ram');root.mkdir(mode=0o700)
pid=int(subprocess.check_output(['systemctl','show','transparent-shard-server','-p','MainPID','--value']));assert pid>0
end=time.monotonic()+3600
def unix():
 start=time.monotonic()
 with socket.socket(socket.AF_UNIX) as s:
  s.settimeout(4);s.connect('/run/transparent-pir/control.sock');s.sendall(b'{"operation":"status"}\n');data=b''
  while b'\n' not in data:
   chunk=s.recv(65536)
   if not chunk:break
   data+=chunk
   if len(data)>1024*1024:raise ValueError('oversized response')
  received=time.monotonic()
 value=json.loads(data)
 return {'socket_seconds':received-start,'ok':value.get('ok'),'warm':value.get('result',{}).get('warm')}
def http():
 start=time.monotonic()
 with urllib.request.urlopen('http://127.0.0.1:8093/v1/ready',timeout=4) as response:
  raw=response.read();received=time.monotonic();value=json.loads(raw)
 return {'socket_seconds':received-start,'ready':value.get('ready'),'map_sha256':value.get('map_sha256')}
def threads():
 rows=[]
 for task in (Path('/proc')/str(pid)/'task').iterdir():
  try:
   fields=(task/'stat').read_text().rsplit(')',1)[1].split()
   r={'tid':int(task.name),'state':fields[0],'wchan':(task/'wchan').read_text().strip(),'schedstat':(task/'schedstat').read_text().strip()}
   if fields[0]=='D':
    try:r['stack']=(task/'stack').read_text()
    except OSError:pass
   rows.append(r)
  except OSError:pass
 return {'pid':pid,'threads':rows}
def pressure():return {name:Path('/proc',name).read_text() for name in ['stat','pressure/cpu','pressure/memory','pressure/io']}
def loop(name,fn,interval):
 previous=None
 with (root/(name+'.ndjson')).open('x') as output:
  while time.monotonic()<end:
   start=time.monotonic();row={'utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'probe':name,'start_gap_seconds':None if previous is None else start-previous};previous=start
   try:row['value']=fn()
   except Exception as e:row['error']=type(e).__name__+': '+str(e)
   row['seconds']=time.monotonic()-start
   output.write(json.dumps(row)+'\n');output.flush()
   time.sleep(max(.01,interval-(time.monotonic()-start)))
with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
 futures=[pool.submit(loop,name,fn,interval) for name,fn,interval in [('unix',unix,.5),('http',http,.5),('threads',threads,.25),('pressure',pressure,1)]]
 for future in futures:future.result()
