import datetime,json,socket,sys,time
from pathlib import Path
root=Path('/run/tp-tcp-stages');root.mkdir(mode=0o700)
host=sys.argv[1]
end=time.monotonic()+900
(root/'start.json').write_text(json.dumps({'utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'monotonic_ns':time.monotonic_ns(),'host':host,'duration':900})+'\n')
with (root/'samples.ndjson').open('x') as out:
 while time.monotonic()<end:
  start=time.monotonic();row={'utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'monotonic_ns':time.monotonic_ns()};stage='connect'
  try:
   with socket.socket() as sock:
    sock.settimeout(4);sock.connect((host,8093));row['connect_seconds']=time.monotonic()-start
    stage='send';sock.sendall(b'GET /v1/ready HTTP/1.0\r\nHost: worker\r\nConnection: close\r\n\r\n');row['sent_seconds']=time.monotonic()-start
    stage='response';data=b''
    while True:
     chunk=sock.recv(65536)
     if not chunk:break
     if not data:row['first_byte_seconds']=time.monotonic()-start
     data+=chunk
     if len(data)>1024*1024:raise ValueError('oversized response')
    row['status']=data.split(b'\r\n',1)[0].decode()
  except Exception as error:row.update(error=type(error).__name__+': '+str(error),stage=stage)
  row['seconds']=time.monotonic()-start
  out.write(json.dumps(row)+'\n');out.flush()
  time.sleep(max(.01,.5-(time.monotonic()-start)))
