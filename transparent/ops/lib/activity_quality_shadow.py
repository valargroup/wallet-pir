"""Read-only proof of the existing APM quality family's loaded shadow mode.

Artifact provenance: enhance/evidence/shared-layers-2026-09-30/manifest.json,
source aeaee4b7d661266f60e680226e2bf6ec49a8c7cf, CI 36678333148.
Only that binary and an explicit loaded shadow value authorize this proof.
No environment operands or configuration contents are returned, even on failure.
"""
import os
from pathlib import Path
import time
from wallet_pir_ops import ancillary_baseline as A

UNIT='pir-apm.service'
BINARY_SHA256='0d516a796402927ff5efe4589deb9622bccc6ae2c6d521ad69c2b95d6222697e'
EXECUTABLE='/opt/pir-quality/releases/apm-aeaee4b7/pir-apm'
KEY=b'PIR_APM_QUALITY_ALERT_MODE'
SECONDS=10


def observe(commands, *, proc=Path('/proc'), machine_path=Path('/etc/machine-id')):
    deadline=time.monotonic()+SECONDS
    def tick():A.require(time.monotonic()<deadline,'quality shadow observation deadline exceeded')
    tick()
    A.require(os.geteuid()==0 and machine_path.read_text().strip()==A.COORDINATOR,
              'quality shadow requires the pinned root coordinator')
    def state():
        tick()
        raw=commands.run(['systemctl','show',UNIT,
            '--property=ActiveState,SubState,MainPID,Result,NRestarts,FragmentPath,DropInPaths,ControlGroup'],
            timeout=min(5,deadline-time.monotonic()))
        A.require(len(raw)<=65536,'quality APM unit observation exceeds bound')
        return dict(line.split('=',1) for line in raw.decode().splitlines() if '=' in line)
    before=state();tick()
    A.require(before.get('ActiveState')=='active' and before.get('SubState')=='running' and
              before.get('ControlGroup')=='/system.slice/'+UNIT,
              'quality APM is not the running main unit')
    pid=before.get('MainPID','')
    A.require(isinstance(pid,str) and pid.isdigit() and int(pid)>1,'quality APM PID invalid')
    pid=int(pid);identity=A.kernel(pid,proc)
    A.require(identity is not None and identity['state']!='Z','quality APM process absent')
    boot=(proc/'sys/kernel/random/boot_id').read_text().strip()
    A.require(A.BOOT.fullmatch(boot) is not None,'quality APM boot identity invalid')
    base=proc/str(pid)
    A.require(os.readlink(base/'exe')==EXECUTABLE and
              (base/'cgroup').read_text().strip()=='0::/system.slice/'+UNIT,
              'quality APM executable or cgroup differs')
    A.require(A.hashed(base/'exe',A.MAX_EXE,tick)==BINARY_SHA256,'quality APM binary differs from reviewed provenance')
    with (base/'environ').open('rb') as source:raw=source.read((1<<20)+1)
    A.require(len(raw)<=1<<20,'quality APM environment exceeds bound')
    values=[entry.partition(b'=')[2] for entry in raw.split(b'\0') if entry.partition(b'=')[0]==KEY]
    A.require(values==[b'shadow'],'quality APM requires one explicit loaded shadow value')
    del raw,values
    tick()
    A.require(A.kernel(pid,proc)==identity and state()==before and
              (proc/'sys/kernel/random/boot_id').read_text().strip()==boot,
              'quality APM identity changed during observation')
    tick()
    return dict(identity,unit=UNIT,boot_id=boot,binary_sha256=BINARY_SHA256,
                quality_alert_mode='shadow',observed_unix=time.time())


def same_process(before,after):
    return all(before.get(k)==after.get(k) for k in ('pid','start_ticks','boot_id','binary_sha256','unit','quality_alert_mode'))
