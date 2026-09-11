import datetime,json,time
from pathlib import Path
out=Path('/run/tp-publication-io-probe/framebuffer-experiment.json')
assert not out.exists()
consoles=Path('/proc/consoles').read_text()
assert any(line.split()[0]=='ttyS0' for line in consoles.splitlines()), 'serial console must remain available'
paths=[p for p in Path('/sys/class/vtconsole').glob('vtcon*') if 'frame buffer device' in (p/'name').read_text() and (p/'bind').read_text().strip()=='1']
assert len(paths)==1
p=paths[0]/'bind'
record={'before_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'before_monotonic_ns':time.monotonic_ns(),'bind_path':str(p),'before_bind':p.read_text().strip(),'consoles':consoles,'framebuffer':Path('/proc/fb').read_text(),'scope':'runtime diagnostic only; no acceptance run; restore with bind=1'}
out.write_text(json.dumps(record,indent=2)+'\n')
p.write_text('0\n')
record.update(after_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),after_monotonic_ns=time.monotonic_ns(),after_bind=p.read_text().strip())
out.write_text(json.dumps(record,indent=2)+'\n')
assert record['after_bind']=='0'
print(json.dumps(record,indent=2))
