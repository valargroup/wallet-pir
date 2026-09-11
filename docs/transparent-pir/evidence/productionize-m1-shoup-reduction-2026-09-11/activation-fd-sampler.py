import datetime,json,pathlib,subprocess,time,os
end=time.monotonic()+600
while time.monotonic()<end:
    pid=subprocess.check_output(['systemctl','show','transparent-shard-server.service','-p','MainPID','--value'],text=True).strip()
    root=pathlib.Path('/proc')/pid
    row={'utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'pid':pid,'blocked':[]}
    for task in (root/'task').glob('*'):
        try:
            state=next(l for l in (task/'status').read_text().splitlines() if l.startswith('State:'))
            if 'D (disk sleep)' not in state: continue
            info={'tid':task.name,'wchan':(task/'wchan').read_text(),'stack':(task/'stack').read_text()}
            call=(task/'syscall').read_text().split()
            # x86-64 fsync/fdatasync/write: the first argument is a file descriptor.
            if call and call[0] in ('1','74','75'):
                info['syscall_number']=int(call[0]);fd=int(call[1],16)
                info['fd']=fd
                try:info['path']=os.readlink(root/'fd'/str(fd))
                except OSError as e:info['path_error']=str(e)
            row['blocked'].append(info)
        except OSError:pass
    print(json.dumps(row),flush=True)
    time.sleep(1)
