"""One mapped public runtime cache entry, retained/restored by an owned unit fault.

Only qualification imports this module. Paths and corruption bytes are fixed;
existing cache inodes are never modified, including while native code maps them.
"""
from contextlib import contextmanager
import fcntl
import hashlib
import os
from pathlib import Path
import re
import stat

CACHE = Path('/srv/transparent-pir/v11/runtime-cache')
RECOVERY = CACHE.parent/'cache-faults'
NAME = re.compile(r'([0-9a-f]{64})-([0-9a-f]{64})\.runtime')
MAX_BYTES = 8 << 30
MAX_MAPS = 4 << 20


def require(ok, message):
    if not ok:raise ValueError(message)


def no_links(path):
    require(not any(p.is_symlink() for p in (path,*path.parents)), 'cache fault path contains a link')


def sync(path):
    fd=os.open(path,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
    try:os.fsync(fd)
    finally:os.close(fd)


def metadata(info):
    return {'device':info.st_dev,'inode':info.st_ino,'bytes':info.st_size,
            'mtime_ns':info.st_mtime_ns,'ctime_ns':info.st_ctime_ns}


class CacheFault:
    def __init__(self, identifier, record, save, deadline, lock, stopped, proc=Path('/proc')):
        require(re.fullmatch('[0-9a-f]{64}',identifier) is not None,'invalid cache fault owner')
        self.identifier,self.record,self.save,self.deadline,self.lock,self.stopped,self.proc=identifier,record,save,deadline,lock,stopped,proc
        self.directory=RECOVERY/identifier

    def check(self):
        self.deadline.need(.01,'cache fault IO');self.lock.verify()

    def phase(self, phase):
        self.record['cache']['phase']=phase;self.save()

    def paths(self):
        record=self.record['cache'];name=record['name']
        require(record.get('kind')=='owned-cache-corruption-v1' and isinstance(name,str) and
                NAME.fullmatch(name) is not None,'invalid retained cache name or kind')
        paths=(CACHE/name,self.directory/'original',self.directory/'corrupt',self.directory/'displaced')
        for path in paths:no_links(path)
        return paths

    def seal(self, path, valid=True):
        self.check();no_links(path)
        fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW)
        with os.fdopen(fd,'rb') as stream:
            info=os.fstat(stream.fileno())
            require(stat.S_ISREG(info.st_mode) and info.st_nlink==1 and 64<=info.st_size<=MAX_BYTES,'cache entry size or type invalid')
            header=stream.read(64);whole=hashlib.sha256(header);body=hashlib.sha256()
            while data:=stream.read(1<<20):self.check();whole.update(data);body.update(data)
            require(metadata(os.fstat(stream.fileno()))==metadata(info),'cache inode changed while hashing')
        require(metadata(path.stat())==metadata(info),'cache path changed while hashing')
        if valid:require(body.digest()==header[32:],'original cache checksum invalid')
        return dict(metadata(info),sha256=whole.hexdigest(),header=header.hex())

    @contextmanager
    def cache_lock(self):
        self.check();path=CACHE/'.lock';no_links(path)
        fd=os.open(path,os.O_RDWR|os.O_NOFOLLOW)
        try:
            info=os.fstat(fd)
            require(stat.S_ISREG(info.st_mode) and info.st_nlink==1,'native cache lock is not regular or exclusive')
            fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
            no_links(path)
            require((path.stat().st_dev,path.stat().st_ino)==(info.st_dev,info.st_ino),
                    'native cache lock path changed')
            self.check();yield
        finally:os.close(fd)

    def capture(self, pid, manifests):
        require('cache' not in self.record,'cache fault already captured')
        require(type(pid) is int and pid>1,'invalid cache worker identity')
        prefixes={hashlib.sha256(m.encode()).hexdigest() for m in manifests}
        self.check();path=self.proc/str(pid)/'maps'
        with path.open('rb') as stream:raw=stream.read(MAX_MAPS+1)
        require(len(raw)<=MAX_MAPS,'worker maps exceed bound')
        selected={}
        for line in raw.decode().splitlines():
            fields=line.split(maxsplit=5)
            if len(fields)!=6:continue
            mapped=Path(fields[5]);match=NAME.fullmatch(mapped.name)
            if mapped.parent!=CACHE or match is None or match[1] not in prefixes:continue
            require(fields[1].startswith('r') and 'w' not in fields[1],'cache mapping is not immutable read-only')
            no_links(mapped);info=mapped.stat()
            device=fields[3].split(':')
            require(len(device)==2 and info.st_ino==int(fields[4]) and
                    os.major(info.st_dev)==int(device[0],16) and os.minor(info.st_dev)==int(device[1],16),
                    'mapped cache inode differs from path')
            selected[mapped.name]=metadata(info)
        require(selected,'no current sealed cache inode mapped by the warm worker')
        name=min(selected);seal=self.seal(CACHE/name)
        require(all(seal[k]==v for k,v in selected[name].items()),'mapped cache changed before capture')
        require(seal['header'][:64]==NAME.fullmatch(name)[2],'cache identity differs from filename')
        self.record['cache']={'kind':'owned-cache-corruption-v1','name':name,'original':seal,'phase':'captured',
                              'worker_pid':pid,'manifest_prefix':NAME.fullmatch(name)[1]}
        self.save()

    def corrupt(self):
        self.check();require(self.stopped(),'cache fault requires the owned worker stopped and cgroup empty')
        target,original,temp,displaced=self.paths()
        with self.cache_lock():
            require(self.record['cache']['phase']=='captured','cache corruption is not replayable')
            seal=self.record['cache']['original']
            current=metadata(target.stat())
            require(current=={k:seal[k] for k in current},'cache changed since capture')
            disk=os.statvfs(CACHE);reserve=seal['bytes']+(1<<30)
            require((disk.f_bavail*disk.f_frsize-reserve)*5>=disk.f_blocks*disk.f_frsize,'cache recovery below20percent disk headroom')
            no_links(RECOVERY);RECOVERY.mkdir(mode=0o700,exist_ok=True)
            no_links(self.directory);self.directory.mkdir(mode=0o700)
            require(CACHE.stat().st_dev==self.directory.stat().st_dev,'cache recovery must share filesystem')
            require(not any(p.exists() for p in (original,temp,displaced)),'cache recovery paths already exist')
            bad=bytearray.fromhex(seal['header']);bad[-1]^=1
            self.record['cache']['corrupt_sha256']=hashlib.sha256(bad).hexdigest()
            self.phase('retain-intent')
            os.rename(target,original);sync(CACHE);sync(self.directory);self.phase('retained')
            fd=os.open(temp,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
            with os.fdopen(fd,'wb') as stream:stream.write(bad);stream.flush();os.fsync(stream.fileno())
            self.phase('install-intent');require(not target.exists(),'cache target reappeared before corruption')
            os.rename(temp,target);sync(CACHE);sync(self.directory);self.phase('corrupted')
        return self.record['cache']

    def restore(self):
        self.check();target,original,temp,displaced=self.paths();record=self.record['cache'];seal=record['original']
        with self.cache_lock():
            if not original.exists():
                current=self.seal(target)
                require(current['sha256']==seal['sha256'] and current['inode']==seal['inode'] and
                        current['device']==seal['device'],'original cache is missing or changed')
                self.phase('restored');return record
            retained=self.seal(original)
            require(retained['sha256']==seal['sha256'] and retained['bytes']==seal['bytes'] and
                    retained['inode']==seal['inode'] and retained['device']==seal['device'],'retained original cache changed')
            if target.exists():
                current=self.seal(target,valid=False)
                require(current['sha256'] in (record.get('corrupt_sha256'),seal['sha256']),
                        'unexpected replacement cache remains fenced')
                require(not displaced.exists(),'cache displacement already exists while target remains')
                record['displaced_sha256']=current['sha256'];self.phase('displace-intent')
                os.rename(target,displaced);sync(CACHE);sync(self.directory)
            elif displaced.exists():
                current=self.seal(displaced,valid=False)
                require(current['sha256']==record.get('displaced_sha256'),'displaced cache changed')
            self.phase('restore-intent')
            require(not target.exists(),'cache target reappeared during restoration')
            os.rename(original,target);sync(CACHE);sync(self.directory)
            current=self.seal(target)
            require(current['sha256']==seal['sha256'],'restored cache hash differs')
            self.phase('restored')
        return record
