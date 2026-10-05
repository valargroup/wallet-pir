"""Read-only exact retained ancillary identities, never a unit-name exemption.

Root-reviewed historical prototype is loopback-only and excluded from loaded
coordinator/router routes. The existing continuous load remains attributable
only to its immutable unit/command/executable, including after an owned restart.
No source, service, route, cache or process is modified by this module. Stdlib
only so the deployment transport can embed its exact reviewed bytes.
"""
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import time
import urllib.request

KIND = 'retained-activity-ancillary-v1'
COORDINATOR = '6a16bce88fb2493681d327344090293a'
ROUTER = '6a384cb26a354c4285198bab6a1c9713'
PROTOTYPE = 'transparent-activity-prototype-server-e47bdf79.service'
LOAD = 'transparent-5qps-continuous.service'
UNITS = (PROTOTYPE, LOAD)
PINS = {
    PROTOTYPE: {'pid':1512286,'start_ticks':244751095,'boot_id':'38feb427-561c-4b5f-b164-aca94f4e310a',
        'fragment':'/run/systemd/transient/'+PROTOTYPE,
        'fragment_sha256':'9ea7f038af91b3563ddc4375b15090356ce077123757fe5623696efe6fe4c108',
        'exe_sha256':'9515bb1e4275a31a587696e5dc3ee806414d8b7bd99da19c4bdc53e62139876e',
        'command_sha256':'c26a2d48788990a07251c9de943453bbc646c48cea9980e5857319ee7602c8d1'},
    LOAD: {'fragment':'/etc/systemd/system/'+LOAD,
        'fragment_sha256':'b610cb49fbf3fe27c41a7accd45894246064af4ae96cbeda4e5a18b4f98b0013',
        'exe_sha256':'e50d468e8b0adfb05733f5b87b3cff34829c4a8c1aea50c865aa8bdfe4bb150f',
        'command_sha256':'cc591974c7784df6e28663fe60b3d68ed1cdd22ab4efd3e2dc4680160f5b0d67'},
}
PINS_SHA256 = hashlib.sha256(json.dumps(PINS,sort_keys=True,separators=(',',':')).encode()).hexdigest()
BOOT = re.compile('[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}')
MAX_EXE = 128 << 20
SECONDS = 30


def require(ok,message):
    if not ok:raise ValueError(message)


def kernel(pid,proc=Path('/proc')):
    try:fields=(proc/str(pid)/'stat').read_text().rsplit(')',1)[1].split()
    except (FileNotFoundError,ProcessLookupError):return None
    return {'pid':pid,'start_ticks':int(fields[19]),'state':fields[0]}


def hashed(path,limit,tick):
    tick();info=path.stat();require(stat.S_ISREG(info.st_mode) and info.st_size<=limit,'ancillary file exceeds bound')
    sha=hashlib.sha256();size=0
    with path.open('rb') as f:
        while chunk:=f.read(1<<20):
            tick();size+=len(chunk);require(size<=limit,'ancillary file grew beyond bound');sha.update(chunk)
    tick();after=path.stat()
    require((info.st_dev,info.st_ino,info.st_size,info.st_mtime_ns)==
            (after.st_dev,after.st_ino,after.st_size,after.st_mtime_ns),'ancillary file changed during verification')
    return sha.hexdigest()


def properties(unit):
    keys=('Id','ActiveState','SubState','MainPID','NRestarts','ControlGroup','FragmentPath','DropInPaths','NeedDaemonReload')
    result=subprocess.run(['systemctl','show',unit,'--property='+','.join(keys)],capture_output=True,timeout=5)
    require(result.returncode==0 and len(result.stdout)<=65536,'ancillary unit observation failed')
    return dict(line.split('=',1) for line in result.stdout.decode().splitlines() if '=' in line)


def listeners(pid,proc):
    sockets=set()
    for count,path in enumerate((proc/str(pid)/'fd').iterdir(),1):
        require(count<=4096,'ancillary descriptor count exceeds bound')
        value=os.readlink(path)
        if value.startswith('socket:['):sockets.add(value[8:-1])
    require(len(sockets)<=4096,'ancillary socket count exceeds bound')
    found=[]
    for table in ('tcp','tcp6'):
        with (proc/'net'/table).open('rb') as source:raw=source.read((8<<20)+1)
        require(len(raw)<=8<<20,'ancillary socket table exceeds bound')
        for row in raw.decode().splitlines()[1:]:
            fields=row.split()
            if fields[3]=='0A' and fields[9] in sockets:
                address,port=fields[1].split(':');found.append([table,address,int(port,16)])
    return sorted(found)


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self,*args,**kwargs):raise ValueError('ancillary route redirect refused')


def route():
    opener=urllib.request.build_opener(urllib.request.ProxyHandler({}),NoRedirect())
    with opener.open('http://127.0.0.1:2019/config/',timeout=3) as response:
        require(response.status==200,'ancillary loaded route unavailable')
        raw=response.read((8<<20)+1)
    require(len(raw)<=8<<20 and b'8192' not in raw,'loaded route references historical prototype or exceeds bound')
    parsed=json.loads(raw)
    require(isinstance(parsed,dict),'ancillary loaded route is not an object')
    require('8192' not in json.dumps(parsed),'loaded route references historical prototype')
    return {'sha256':hashlib.sha256(raw).hexdigest(),'bytes':len(raw),'prototype_port_excluded':True}


def service(unit,boot,tick,proc):
    before=properties(unit);tick();pid=int(before.get('MainPID','-1'))
    if before.get('ActiveState') in ('inactive','failed') and pid==0:
        return {'status':'absent'}
    pin=PINS[unit]
    require(before.get('Id')==unit and before.get('ActiveState')=='active' and before.get('SubState')=='running' and
            before.get('NRestarts')=='0' and before.get('DropInPaths')=='' and before.get('NeedDaemonReload')=='no' and
            before.get('ControlGroup')=='/system.slice/'+unit and before.get('FragmentPath')==pin['fragment'] and pid>1,
            'ancillary loaded unit differs from reviewed identity')
    current=kernel(pid,proc);require(current is not None and current['state']!='Z','ancillary process absent')
    if unit==PROTOTYPE:
        require(pid==pin['pid'] and current['start_ticks']==pin['start_ticks'] and boot==pin['boot_id'],
                'historical prototype process identity differs')
    fragment=Path(pin['fragment'])
    require(all(not p.is_symlink() for p in (fragment,*fragment.parents)),'ancillary unit path contains link')
    require(hashed(fragment,1<<20,tick)==pin['fragment_sha256'],'ancillary unit bytes differ')
    base=proc/str(pid)
    with (base/'cmdline').open('rb') as source:command=source.read(4097)
    require(len(command)<=4096,'ancillary command exceeds bound')
    require(hashlib.sha256(command).hexdigest()==pin['command_sha256'],'ancillary command differs')
    require(hashed(base/'exe',MAX_EXE,tick)==pin['exe_sha256'],'ancillary executable differs')
    group=(base/'cgroup').read_text().strip()
    require(group=='0::/system.slice/'+unit,'ancillary process cgroup differs')
    result=dict(current,status='verified',unit=unit,boot_id=boot,cgroup='/system.slice/'+unit,
                exe=os.readlink(base/'exe'),exe_sha256=pin['exe_sha256'],command_sha256=pin['command_sha256'],
                fragment_sha256=pin['fragment_sha256'])
    if unit==PROTOTYPE:
        require(listeners(pid,proc)==[['tcp','0100007F',8192]],'historical prototype is not exclusively loopback8192')
        result['listeners']=[['tcp','0100007F',8192]]
    tick();require(kernel(pid,proc)==current and properties(unit)==before,'ancillary identity changed during observation')
    return result


def observe(machine,*,tick=lambda:None,proc=Path('/proc')):
    deadline=time.monotonic()+SECONDS
    def check():
        tick();require(time.monotonic()<deadline,'ancillary observation deadline exceeded')
    if machine not in (COORDINATOR,ROUTER):
        return {'kind':KIND,'pins_sha256':PINS_SHA256,'machine_id':machine,'boot_id':None,
                'units':{},'route':None,'observed_unix':time.time()}
    check();require(os.geteuid()==0,'ancillary observation requires root')
    boot=(proc/'sys/kernel/random/boot_id').read_text().strip()
    require(BOOT.fullmatch(boot) is not None,'ancillary boot identity invalid')
    result={'kind':KIND,'pins_sha256':PINS_SHA256,'machine_id':machine,'boot_id':boot,'units':{},'route':None}
    if machine==COORDINATOR:
        result['units']={unit:service(unit,boot,check,proc) for unit in UNITS}
    if machine in (COORDINATOR,ROUTER):result['route']=route()
    check();result['observed_unix']=time.time()
    return result


def verify(proof,machine):
    require(isinstance(proof,dict) and proof.get('kind')==KIND and proof.get('pins_sha256')==PINS_SHA256 and
            proof.get('machine_id')==machine and
            ((isinstance(proof.get('boot_id'),str) and BOOT.fullmatch(proof['boot_id'])) if machine in (COORDINATOR,ROUTER) else proof.get('boot_id') is None) and
            type(proof.get('observed_unix')) in (int,float) and 0<=time.time()-proof['observed_unix']<=300 and
            set(proof.get('units',{}))==(set(UNITS) if machine==COORDINATOR else set()),'ancillary proof partial, stale or foreign')
    require(proof.get('route') is None if machine not in (COORDINATOR,ROUTER) else
            isinstance(proof.get('route'),dict) and proof['route'].get('prototype_port_excluded') is True and
            type(proof['route'].get('bytes')) is int and 0<=proof['route']['bytes']<=8<<20 and
            isinstance(proof['route'].get('sha256'),str) and re.fullmatch('[0-9a-f]{64}',proof['route']['sha256']),
            'ancillary route exclusion proof missing')
    for unit,value in proof['units'].items():
        require(isinstance(value,dict) and value.get('status') in ('absent','verified'),'ancillary unit proof invalid')
        if value['status']=='absent':require(value=={'status':'absent'},'ancillary absent proof differs');continue
        pin=PINS[unit]
        require(value.get('unit')==unit and value.get('boot_id')==proof['boot_id'] and value.get('cgroup')=='/system.slice/'+unit and
                type(value.get('pid')) is int and value['pid']>1 and type(value.get('start_ticks')) is int and value['start_ticks']>0 and
                value.get('state')!='Z' and isinstance(value.get('exe'),str) and value['exe'].startswith('/') and
                all(value.get(k)==pin[k] for k in ('exe_sha256','command_sha256','fragment_sha256')),
                'ancillary unit provenance differs')
        if unit==PROTOTYPE:
            require(all(value.get(k)==pin[k] for k in ('pid','start_ticks','boot_id')) and
                    value.get('listeners')==[['tcp','0100007F',8192]],'prototype proof differs')
    return proof


def authorities(proof,machine):
    verify(proof,machine)
    return {v['pid']:v for v in proof['units'].values() if v['status']=='verified'}


def authorized(proof,machine,item):
    value=authorities(proof,machine).get(item['pid'])
    if value is None:return False
    group=item.get('cgroup','').strip()
    return item.get('start_ticks',item.get('start'))==value['start_ticks'] and item.get('exe')==value['exe'] and \
           group in (value['cgroup'],'0::'+value['cgroup']) and item.get('command_sha256')==value['command_sha256']
