import subprocess,json
cmd=['ssh','-o','ConnectTimeout=10','-i','/opt/transparent-publisher/credentials/deploy-ssh','-o','UserKnownHostsFile=/opt/transparent-publisher/credentials/known_hosts','root@10.142.0.10']
script=r"""
import subprocess
from pathlib import Path
pid=int(subprocess.check_output(['systemctl','show','transparent-shard-server.service','-p','MainPID','--value'],text=True))
assert pid>0
p=Path('/tmp/m1-cache-lock-20260911.bt')
p.write_text('''tracepoint:syscalls:sys_enter_flock /pid == PID/ { @lock[tid] = nsecs; }
tracepoint:syscalls:sys_exit_flock /pid == PID && @lock[tid]/ { $ms=(nsecs-@lock[tid])/1000000; if ($ms > 100) { printf("flock tid=%d ms=%llu ret=%d\\n",tid,$ms,args->ret); } delete(@lock[tid]); }
tracepoint:syscalls:sys_enter_fsync,tracepoint:syscalls:sys_enter_fdatasync /pid == PID/ { @sync[tid]=nsecs; }
tracepoint:syscalls:sys_exit_fsync,tracepoint:syscalls:sys_exit_fdatasync /pid == PID && @sync[tid]/ { $ms=(nsecs-@sync[tid])/1000000; if ($ms > 100) { printf("sync tid=%d ms=%llu ret=%d\\n",tid,$ms,args->ret); } delete(@sync[tid]); }
interval:s:180 { exit(); }
END { clear(@lock); clear(@sync); }
'''.replace('PID',str(pid)))
with open('/tmp/m1-cache-lock-20260911.log','w') as f:
 subprocess.run(['bpftrace','-q',str(p)],stdout=f,stderr=subprocess.STDOUT,check=True)
print(Path('/tmp/m1-cache-lock-20260911.log').read_text())
"""
r=subprocess.run(cmd+['python3','-'],input=script,text=True,capture_output=True,timeout=210)
print(r.stdout);print(r.stderr)
raise SystemExit(r.returncode)
