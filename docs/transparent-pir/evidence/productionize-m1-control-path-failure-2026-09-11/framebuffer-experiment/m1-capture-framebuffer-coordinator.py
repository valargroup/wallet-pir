import datetime,io,json,subprocess,sys,tarfile
from pathlib import Path
files={}
for name in ['transparent-control-path-probe','transparent-dedicated-control-probe','tp-tcp-stages','transparent-reconciler-probe']:
 for p in Path('/run',name).iterdir():
  if p.is_file():files[name+'/'+p.name]=p.read_bytes()
root=Path('/opt/transparent-publisher-build/publication-io-20260910/control-path-load-2')
for p in root.iterdir():
 if p.is_file():files['load-2/'+p.name]=p.read_bytes()
files['capture.json']=(json.dumps({'utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'live_endpoint_probes':True,'units':subprocess.check_output(['systemctl','show','transparent-m1-control-path-load-2.service','transparent-m1-reconciler-fsync.service','transparent-m1-tcp-stages.service','transparent-m1-control-path-probe.service','transparent-m1-dedicated-control-probe.service','-p','Id','-p','MainPID','-p','ActiveState','-p','ExecMainStatus'],text=True)},indent=2)+'\n').encode()
with tarfile.open(fileobj=sys.stdout.buffer,mode='w|gz') as out:
 for name,data in files.items():
  info=tarfile.TarInfo(name);info.size=len(data);out.addfile(info,io.BytesIO(data))
