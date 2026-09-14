import datetime,json,pathlib,subprocess,time
end=time.monotonic()+900
while time.monotonic()<end:
    pid=subprocess.check_output(['systemctl','show','transparent-shard-server.service','-p','MainPID','--value'],text=True).strip()
    root=pathlib.Path('/proc')/pid
    row={'utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'pid':pid,'threads':[]}
    for name,path in [('io',root/'io'),('pressure_io',pathlib.Path('/proc/pressure/io')),('diskstats',pathlib.Path('/proc/diskstats'))]:
        try: row[name]=path.read_text()
        except OSError as e: row[name]=str(e)
    for task in (root/'task').glob('*'):
        try:
            state=next(l for l in (task/'status').read_text().splitlines() if l.startswith('State:'))
            wchan=(task/'wchan').read_text()
            if 'D (disk sleep)' in state or any(s in wchan for s in ['sync','journal','folio','writeback']):
                row['threads'].append({'tid':task.name,'state':state,'wchan':wchan,'stack':(task/'stack').read_text()})
        except OSError: pass
    print(json.dumps(row),flush=True)
    time.sleep(1)
