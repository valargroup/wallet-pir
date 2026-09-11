import datetime,io,json,subprocess,sys,tarfile
from pathlib import Path
root=Path('/run/tp-publication-io-probe')
files={p.name:p.read_bytes() for p in root.iterdir() if p.is_file()}
for p in Path('/run/tp-tcp-stages').iterdir():
 if p.is_file():files['tcp-stages/'+p.name]=p.read_bytes()
files['capture.json']=(json.dumps({'utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'live_endpoint_probes':True,'bind':Path('/sys/class/vtconsole/vtcon0/bind').read_text().strip(),'units':subprocess.check_output(['systemctl','show','transparent-m1-cpu-gaps-v3.service','transparent-m1-worker-advice.service','transparent-m1-tcp-stages.service','transparent-m1-publication-io-probe.service','-p','Id','-p','MainPID','-p','ActiveState','-p','ExecMainStatus'],text=True)},indent=2)+'\n').encode()
with tarfile.open(fileobj=sys.stdout.buffer,mode='w|gz') as out:
 for name,data in files.items():
  info=tarfile.TarInfo(name);info.size=len(data);out.addfile(info,io.BytesIO(data))
