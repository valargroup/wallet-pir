import argparse, tempfile, os, subprocess, time, ssl, json, urllib.request, threading, collections
from pathlib import Path
args=argparse.ArgumentParser();args.add_argument('--binary',required=True);args.add_argument('--reloads',type=int,default=100);a=args.parse_args()
root=Path(tempfile.mkdtemp(prefix='m1-tls-stress-'));env=dict(os.environ,XDG_DATA_HOME=str(root/'data'),XDG_CONFIG_HOME=str(root/'config'));config=root/'Caddyfile'
def write(n):
 config.write_text('{\n admin 127.0.0.1:12019\n skip_install_trust\n auto_https disable_redirects\n}\nhttps://localhost:19443 {\n bind 127.0.0.1\n tls internal\n respond "'+str(n)+'"\n}\n')
write(0);log=(root/'caddy.log').open('w');p=subprocess.Popen([a.binary,'run','--config',str(config),'--adapter','caddyfile'],env=env,stdout=log,stderr=log);ctx=ssl._create_unverified_context();stop=threading.Event();lock=threading.Lock();counts=collections.Counter();errors=[];threads=[]
def query():
 while not stop.is_set():
  try:
   with urllib.request.urlopen('https://localhost:19443',context=ctx,timeout=3) as r:r.read()
   with lock:counts['ok']+=1
  except Exception as e:
   with lock:counts['failed']+=1;errors.append({'time':time.time(),'error':repr(e)})
try:
 for i in range(50):
  try:
   with urllib.request.urlopen('https://localhost:19443',context=ctx,timeout=1) as r:r.read()
   break
  except Exception:time.sleep(.1)
 else:raise RuntimeError('startup failed')
 threads=[threading.Thread(target=query) for _ in range(4)]
 for t in threads:t.start()
 for n in range(1,a.reloads+1):
  write(n);subprocess.run([a.binary,'reload','--config',str(config),'--adapter','caddyfile','--address','127.0.0.1:12019'],env=env,stdout=log,stderr=log,check=True,timeout=15);counts['reloads']+=1
finally:
 stop.set()
 for t in threads:t.join(timeout=5)
 p.terminate();p.wait(timeout=10);log.close();result={'root':str(root),'binary':a.binary,'counts':dict(counts),'errors':errors};(root/'result.json').write_text(json.dumps(result,indent=2));print(json.dumps(result))
