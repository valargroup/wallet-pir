#!/usr/bin/env python3
"""Private fleet adapter for transparent-publish-controller.

One JSON request on stdin, one JSON result on stdout. Logs go to stderr.
Artifact transfer and warm preparation run concurrently; activation requires all
archive owners and at least one recent replica. SSH authenticates every operation.
"""
import asyncio
from contextlib import asynccontextmanager
import fcntl
import json
import os
from pathlib import Path
import re
import shlex
import sys
import tempfile
import time


def atomic_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix('.tmp')
    with tmp.open('w') as f:
        json.dump(value, f)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)
    fd = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


async def run(args, data=None, timeout=25):
    started = time.monotonic()
    proc = await asyncio.create_subprocess_exec(*map(str, args), stdin=asyncio.subprocess.PIPE,
                                              stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    try:
        out, err = await asyncio.wait_for(proc.communicate(data), timeout)
    except BaseException:
        proc.kill()
        await proc.wait()
        raise
    print(json.dumps({'event':'fleet_command','tool':Path(str(args[0])).name,
                      'seconds':round(time.monotonic()-started,6),'exit_code':proc.returncode}),file=sys.stderr)
    if proc.returncode:
        raise RuntimeError(f'{args[0]} failed: {err.decode(errors="replace")[-2000:]}')
    if err:
        print(err.decode(errors='replace'), file=sys.stderr, end='')
    return out


class Fleet:
    def __init__(self, config):
        self.c = config
        self.roster = json.loads(Path(config['roster']).read_text())
        self.root = Path(config['state_dir'])
        self.root.mkdir(parents=True, exist_ok=True)
        control_dir = self.root / 'ssh'
        control_dir.mkdir(mode=0o700, exist_ok=True)
        control_dir.chmod(0o700)
        self.canonical = {}
        self.rpc_slots = asyncio.Semaphore(8)
        self.direct_ssh_args = ['ssh', '-oBatchMode=yes', '-oConnectTimeout=3', '-oStrictHostKeyChecking=yes',
                         '-o', 'UserKnownHostsFile=' + config['known_hosts'], '-i', config['ssh_key']]
        self.ssh_args = self.direct_ssh_args + ['-oControlMaster=auto', '-oControlPersist=60',
                         '-o', 'ControlPath=' + str(control_dir / '%C')]
        for worker in self.roster:
            for field in ['id', 'ssh_host', 'upstream']:
                if not re.fullmatch(r'[A-Za-z0-9_.:-]+', worker[field]):
                    raise ValueError(f'invalid roster {field}')

    @asynccontextmanager
    async def lock(self, name, wait=True):
        # Shared with the reconciler process. Never block the event loop on flock.
        with (self.root / (name + '.lock')).open('a') as stream:
            while True:
                try:
                    fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    if not wait:
                        raise RuntimeError('worker preparation already in progress')
                    await asyncio.sleep(0.05)
            try:
                yield
            finally:
                fcntl.flock(stream, fcntl.LOCK_UN)

    async def canonical_hash(self, height):
        if height not in self.canonical:
            async def fetch():
                import urllib.request
                import urllib.error
                import base64
                async with self.rpc_slots:
                    def request():
                        cookie = Path(self.c.get('rpc_cookie','/root/.cache/zakura/.cookie')).read_text().strip()
                        data = json.dumps({'jsonrpc':'2.0','id':1,'method':'getblockhash','params':[height]}).encode()
                        req = urllib.request.Request(self.c.get('rpc_url','http://127.0.0.1:8232'),data,{'Content-Type':'application/json','Authorization':'Basic '+base64.b64encode(cookie.encode()).decode()})
                        try:
                            with urllib.request.urlopen(req,timeout=5) as response:
                                body=json.load(response)
                        except urllib.error.HTTPError as error:
                            if error.code != 500:
                                raise
                            body=json.load(error)
                        if body.get('error'):
                            # A height above a regressed tip is not canonical.
                            error = body['error']
                            if error.get('code') == -8 or (error.get('code') == -1 and error.get('message') == 'Provided index is greater than the current tip'):
                                return None
                            raise RuntimeError('node refused canonical endpoint lookup')
                        return body['result']
                    return await asyncio.to_thread(request)
            self.canonical[height] = asyncio.create_task(fetch())
        # Cancelling a slow replica must not cancel a lookup shared by owners.
        return await asyncio.shield(self.canonical[height])

    async def node_height(self):
        # Sample the node independently of the controller's ingest loop.
        def fetch():
            import urllib.request
            import base64
            cookie = Path(self.c.get('rpc_cookie','/root/.cache/zakura/.cookie')).read_text().strip()
            data = json.dumps({'jsonrpc':'2.0','id':1,'method':'getblockcount','params':[]}).encode()
            request = urllib.request.Request(self.c.get('rpc_url','http://127.0.0.1:8232'),data,{'Content-Type':'application/json','Authorization':'Basic '+base64.b64encode(cookie.encode()).decode()})
            with urllib.request.urlopen(request, timeout=5) as response:
                value = json.load(response)
            if value.get('error'):
                raise RuntimeError('node height lookup failed')
            return int(value['result'])
        return await asyncio.to_thread(fetch)

    async def revoke_orphans(self, worker, status, from_height=0, known=()):
        revisions = status.get('revisions',[])
        async def accepted(revision):
            return revision['digest'] if revision['end_height']<from_height or revision['digest'] in known or await self.canonical_hash(revision['end_height'])==revision['terminal_block_hash'] else None
        keep = [digest for digest in await asyncio.gather(*(accepted(r) for r in revisions)) if digest]
        if len(keep)!=len(revisions) or status.get("invalidated"):
            await self.control(worker,{'operation':'invalidate','expected':status['active']['map_sha256'],'from_height':from_height,'keep_digests':keep})
        return keep

    async def ssh(self, host, command, data=None, timeout=25, multiplex=True):
        args = self.ssh_args if multiplex else self.direct_ssh_args + [
            '-oControlMaster=no', '-oControlPersist=no', '-oControlPath=none']
        return await run(args + ['root@' + host, command], data, timeout)

    async def control(self, worker, value):
        command = shlex.join([self.c.get('control_binary', '/usr/local/bin/shard-control'),
                              self.c.get('control_socket', '/run/transparent-pir/control.sock')])
        # A rejected command is JSON on stdout with exit status 1. Preserve
        # that diagnostic; transport failures and crashes must still fail SSH.
        command += ' || [ "$?" -eq 1 ]'
        # A multiplex master retains a long command's pipe descriptors after
        # its client is killed. Use a direct connection for preparation so
        # cancelling a slower replica cannot delay an already warm quorum.
        options = {'multiplex':False, 'timeout':120} if value.get('operation') == 'prepare' else {}
        result = json.loads(await self.ssh(worker['ssh_host'], command, json.dumps(value).encode(), **options))
        if not result.get('ok'):
            raise RuntimeError(f'{worker["id"]}: {result.get("error")}')
        return result['result']

    def quorum(self, ids):
        return all(w['id'] in ids for w in self.roster if w['role'] == 'archive-owner') and any(
            w['id'] in ids for w in self.roster if w['role'] == 'recent-replica')

    async def collect(self, operation, workers, early=False):
        tasks = {asyncio.create_task(operation(w)): w for w in workers}
        done_values = {}
        try:
            while tasks:
                done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                for task in done:
                    worker = tasks.pop(task)
                    try:
                        done_values[worker['id']] = task.result()
                    except Exception as exc:
                        print(f'{worker["id"]}: {exc}', file=sys.stderr)
                if early and self.quorum(done_values):
                    # Give other healthy replicas a short opportunity to finish,
                    # without letting a failed replica hold the public pointer.
                    if tasks:
                        finished, _ = await asyncio.wait(tasks, timeout=0.5)
                        for task in finished:
                            worker = tasks.pop(task)
                            try:
                                done_values[worker['id']] = task.result()
                            except Exception as exc:
                                print(f'{worker["id"]}: {exc}', file=sys.stderr)
                    break
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
        return done_values

    async def stage(self, worker, req, assignment):
        source = Path(req['directory']).resolve(strict=True)
        digest = req['map_sha256']
        remote_dir = self.c.get('worker_root', '/srv/transparent-pir/publications') + '/' + digest
        remote_assignment = remote_dir + '/assignment.json'
        async with self.lock('worker-' + worker['id'], wait=False):
            status = await self.control(worker, {'operation': 'status'})
            before = status['active']
            current_digests = {e['manifest_digest'] for e in json.loads((source/'shards.json').read_text())['shards']}
            await self.revoke_orphans(worker,status,known=current_digests)
            await self.control(worker, {'operation':'collect'})
            if worker['id'] in self.managed_ids():
                self.job(worker, req, 'transferring')
            files = (await run([self.c['assign_binary'], 'files', '--shard-dir', source,
                                '--assignment', assignment, '--worker-id', worker['id']])).decode().splitlines()
            # Only hard-link the digest directories this worker is assigned.
            # This also preserves inode identity for worker verification reuse.
            directories = sorted({f.split('/')[0] for f in files if '/' in f})
            commands = ['set -eu', 'mkdir -p ' + shlex.quote(remote_dir)]
            for d in directories:
                if not re.fullmatch('[0-9a-f]{64}', d):
                    raise ValueError('unexpected artifact path')
                old, new = str(Path(before['directory']) / d), remote_dir + '/' + d
                commands.append(f'if [ ! -e {shlex.quote(new)} ] && [ -d {shlex.quote(old)} ]; then cp -al {shlex.quote(old)} {shlex.quote(new)}; fi')
            await self.ssh(worker['ssh_host'], 'sh -s', ('\n'.join(commands)+'\n').encode())
            with tempfile.NamedTemporaryFile(mode='w') as listing:
                listing.write('\n'.join(files)+'\n'); listing.flush()
                await run(['rsync', '-a', '--ignore-existing', '--files-from', listing.name,
                           '-e', shlex.join(self.ssh_args), str(source)+'/', 'root@'+worker['ssh_host']+':'+remote_dir+'/'])
            await self.ssh(worker['ssh_host'], 'cat > ' + shlex.quote(remote_assignment), assignment.read_bytes())
            publication = {'directory': remote_dir, 'assignment': remote_assignment, 'map_sha256': digest}
            if worker['id'] in self.managed_ids():
                self.job(worker, req, 'warming')
            await self.control(worker, {'operation':'prepare','expected':before['map_sha256'],'publication':publication})
            return {'expected':before['map_sha256'], 'publication':publication}

    async def prepare(self, req):
        digest = req['map_sha256']
        if not re.fullmatch('[0-9a-f]{64}', digest):
            raise ValueError('invalid map digest')
        source = Path(req['directory']).resolve(strict=True)
        assignment = self.root / (digest + '.assignment.json')
        # Same map retries preserve assignment provenance and its digest.
        if not assignment.exists():
            await run([self.c['assign_binary'], 'plan', '--shard-dir', source, '--roster', self.c['roster'],
                       '--recent-from-height', str(req['recent_from']), '--headroom', '0.05',
                       '--out-assignment', assignment, '--source-sha', req['source_sha']])
        req = {**req, 'assignment':str(assignment)}
        atomic_json(self.root/(digest+'.request.json'), req)
        atomic_json(self.root/'desired.json', req)
        async def bounded_stage(worker):
            operation = self.wait_managed(worker, req) if worker['id'] in self.managed_ids() else self.stage(worker, req, assignment)
            return await asyncio.wait_for(operation, 24)
        prepared = await self.collect(bounded_stage, self.roster, early=True)
        if not self.quorum(prepared):
            raise RuntimeError('warm prepare quorum unavailable')
        atomic_json(self.root / (digest+'.prepared.json'), prepared)
        return {'ok':True,'workers':prepared,'assignment':str(assignment)}

    async def route(self, workers, assignment=None):
        # This is the dedicated transparent router, never the Enhance Caddyfile.
        # Public metadata always comes from the one atomic publication authority.
        host = self.c['public_host']
        authority = self.c['authority_upstream']
        if not re.fullmatch(r'[A-Za-z0-9_.:-]+', host) or not re.fullmatch(r'(https://)?[A-Za-z0-9_.:-]+', authority):
            raise ValueError('invalid router addresses')
        body = ['\trequest_body {\n\t\tmax_size 1MB\n\t}']
        if workers:
            groups = []
            recent = [w for w in workers if w['role'] == 'recent-replica']
            by_id = {w['id']:w for w in assignment['workers']}
            if recent:
                groups.append(('recent', by_id[recent[0]['id']]['shards'], [w['upstream'] for w in recent]))
            for worker in workers:
                if worker['role'] == 'archive-owner':
                    groups.append((worker['id'].replace('-','_'),by_id[worker['id']]['shards'],[worker['upstream']]))
            for name, ids, upstreams in groups:
                if not ids:
                    continue
                pattern = '|'.join(map(str,ids))
                body += [f'\t@{name} path_regexp ^/v1/shards/({pattern})/revisions/[0-9a-f]{{64}}/(setup|query)/',
                         f'\thandle @{name} {{\n\t\treverse_proxy {" ".join(upstreams)} {{\n\t\t\tlb_policy round_robin\n\t\t\tlb_try_duration 2s\n\t\t\thealth_uri /v1/ready\n\t\t\thealth_interval 1s\n\t\t}}\n\t}}']
            body += ['\t@metadata path /v1/shards /v1/shards/init /v1/filters/shards /v1/filters/shards/* /v1/shards/*/revisions/*/manifest',
                     f'\thandle @metadata {{\n\t\treverse_proxy {authority} {{\n\t\t\theader_up Host {{upstream_hostport}}\n\t\t}}\n\t}}', '\thandle {\n\t\trespond 404\n\t}']
        else:
            body += ['\thandle {\n\t\theader Retry-After 1\n\t\trespond "transparent publication reconciling" 503\n\t}']
        sites = [host]
        if self.c.get('internal_listen'):
            sites.append('http://' + self.c['internal_listen'])
        maintenance = self.root/'maintenance.json'
        guarded = maintenance.exists() and json.loads(maintenance.read_text()).get('enabled', False)
        unavailable = '\thandle {\n\t\theader Retry-After 1\n\t\trespond "transparent fleet maintenance" 503\n\t}'
        # During a batch restart the private router can exercise the new fleet,
        # while controller retries cannot accidentally reopen the public site.
        text = '\n'.join(site+' {\n'+(unavailable if guarded and site == host else '\n'.join(body))+'\n}\n' for site in sites)
        target = self.c.get('router_file','/etc/caddy/Caddyfile')
        quoted = shlex.quote(target)
        # Record successful application separately: a crash after rename but before
        # reload must not turn a retry into a false no-op. Caddy reload is atomic.
        command = f'set -eu\ncat > {quoted}.live-next\nif cmp -s {quoted}.live-next {quoted} && sha256sum {quoted} | cmp -s - {quoted}.live-applied.sha256; then rm {quoted}.live-next; exit 0; fi\ncaddy validate --config {quoted}.live-next --adapter caddyfile >&2\ncp {quoted} {quoted}.live-previous\nmv {quoted}.live-next {quoted}\nif ! systemctl reload caddy; then cp {quoted}.live-previous {quoted}; systemctl reload caddy; exit 1; fi\nsha256sum {quoted} > {quoted}.live-applied.sha256.next\nmv {quoted}.live-applied.sha256.next {quoted}.live-applied.sha256'
        await self.ssh(self.c['router_host'], command, text.encode())

    async def _activate(self, req):
        digest = req['map_sha256']
        prepared = req['prepared']['workers']
        workers = [w for w in self.roster if w['id'] in prepared]
        async def activate_worker(worker):
            async with self.lock('worker-' + worker['id']):
                await self.control(worker, {'operation':'activate','expected':prepared[worker['id']]['expected'],'map_sha256':digest})
                status = await self.control(worker, {'operation':'status'})
                if not self.attests(status, digest):
                    raise RuntimeError('worker activation did not attest warm candidate')
                return True
        active = await self.collect(activate_worker, workers)
        if not self.quorum(active):
            await self.route([])
            raise RuntimeError('activation quorum unavailable; router withdrawn')
        workers = [w for w in workers if w['id'] in active]
        assignment = json.loads(Path(req['prepared']['assignment']).read_text())
        await self.route(workers, assignment)
        atomic_json(self.root/'active.json',{'map_sha256':digest,'workers':list(active),'assignment':req['prepared']['assignment']})
        atomic_json(self.root/'withdrawn.json', {'withdrawn':False})
        return {'ok':True,'upstreams':[w['upstream'] for w in workers], 'recent_replicas':sum(w['role']=='recent-replica' for w in workers)}

    async def _invalidate(self, req):
        atomic_json(self.root/'withdrawn.json', {'withdrawn':True})
        # Withdraw routing before contacting workers, including unreachable ones.
        await self.route([])
        async def revoke(worker):
            status = await self.control(worker, {'operation':'status'})
            if 'revisions' not in status:
                return await self.control(worker, {'operation':'invalidate','expected':status['active']['map_sha256'],'from_height':req['from_height']})
            return await self.revoke_orphans(worker,status,req['from_height'])
        revoked = await self.collect(revoke,self.roster)
        return {'ok':True,'invalidated':list(revoked)}

    async def activate(self, req):
        async with self.lock('routing'):
            return await self._activate(req)

    async def invalidate(self, req):
        async with self.lock('routing'):
            return await self._invalidate(req)

    def reconciliation_target(self):
        if (self.root/'withdrawn.json').exists() and json.loads((self.root/'withdrawn.json').read_text())['withdrawn']:
            return None
        try:
            active = json.loads((self.root/'active.json').read_text())
            # A newer candidate does not supersede the public authority until
            # activation. Catch up to what clients can read now, even while
            # the next publication is being prepared. Otherwise short block
            # intervals can starve a fully warmed replica indefinitely.
            request = self.root/(active['map_sha256']+'.request.json')
            desired = json.loads((request if request.exists() else self.root/'desired.json').read_text())
        except FileNotFoundError:
            return None
        return (active, desired) if active['map_sha256'] == desired['map_sha256'] else None

    async def reconcile(self):
        target = self.reconciliation_target()
        if target is None:
            return
        active, req = target
        digest = active['map_sha256']
        assignment_path = Path(active['assignment'])
        self.canonical.clear()
        async def catch_up(worker):
            allowed = self.c.get('reconcile_workers')
            if worker['id'] in self.managed_ids() or (allowed is not None and worker['id'] not in allowed) or worker['role'] != 'recent-replica' or worker['id'] in active['workers']:
                return
            try:
                prepared = await asyncio.wait_for(self.stage(worker, req, assignment_path), 60)
                async with self.lock('routing'):
                    current = self.reconciliation_target()
                    if current is None or current[0]['map_sha256'] != digest:
                        return
                    tail = json.loads((Path(req['directory'])/'shards.json').read_text())['shards'][-1]
                    self.canonical.pop(tail['end_height'], None)
                    if await self.canonical_hash(tail['end_height']) != tail['terminal_block_hash']:
                        raise RuntimeError('catch-up publication is no longer canonical')
                    # Serialize worker activation with foreground preparation too.
                    async with self.lock('worker-' + worker['id'], wait=False):
                        await self.control(worker, {'operation':'activate', 'expected':prepared['expected'], 'map_sha256':digest})
                        status = await self.control(worker, {'operation':'status'})
                        if status['active']['map_sha256'] != digest or not status['warm'] or status.get('invalidated'):
                            raise RuntimeError('catch-up did not attest current warm revision')
                        latest = current[0]
                        ids = set(latest['workers']) | {worker['id']}
                        await self.route([w for w in self.roster if w['id'] in ids], json.loads(assignment_path.read_text()))
                        latest['workers'] = sorted(ids)
                        atomic_json(self.root/'active.json', latest)
            except Exception as exc:
                print(json.dumps({'event':'replica_catch_up_failed','worker':worker['id'],'error':str(exc)}), file=sys.stderr)
        await asyncio.gather(*(catch_up(w) for w in self.roster))

    def managed_ids(self):
        return set(self.c.get('managed_recent_workers', []))

    @staticmethod
    def attests(status, digest):
        return (status['active']['map_sha256'] == digest and status['warm']
                and not status.get('invalidated', False))

    @staticmethod
    def prepared_status(status, digest):
        candidate = status.get('candidate') or {}
        return Fleet.attests(status, digest) or (candidate.get('map_sha256') == digest and candidate.get('warm') is True)

    def job(self, worker, req, phase, **extra):
        value = dict(worker=worker['id'], map_sha256=req.get('map_sha256'),
                     phase=phase, monotonic=time.monotonic(), **extra)
        path = self.root/('job-'+worker['id']+'.json')
        previous = json.loads(path.read_text()) if path.exists() else {}
        identity = ('map_sha256', 'phase', 'expected', 'error', 'reason')
        if all(previous.get(k) == value.get(k) for k in identity):
            return
        atomic_json(path, value)
        print(json.dumps({'event':'worker_preparation', **value}), file=sys.stderr)

    async def wait_managed(self, worker, req):
        # Only the daemon starts preparation. A foreground timeout or quorum
        # cancellation abandons this waiter, never the daemon's owned work.
        digest = req['map_sha256']
        while True:
            try:
                heartbeat = json.loads((self.root/'reconciler-heartbeat.json').read_text())
            except FileNotFoundError:
                heartbeat = {}
            age = time.monotonic()-heartbeat.get('monotonic', 0)
            if not 0 <= age <= 5 or worker['id'] not in heartbeat.get('workers', []):
                raise RuntimeError('managed preparation owner unavailable: '+worker['id'])
            path = self.root/('job-'+worker['id']+'.json')
            record = json.loads(path.read_text()) if path.exists() else {}
            if record.get('phase') == 'prepared' and record.get('map_sha256') == digest:
                status = await self.control(worker, {'operation':'status'})
                if self.prepared_status(status, digest):
                    return {'expected':status['active']['map_sha256']}
            await asyncio.sleep(0.1)

    async def reconcile_member(self, worker):
        # Membership is derived from live status, not a historical list of
        # successful activations. Restarted or unreachable workers are removed.
        async with self.lock('routing'):
            target = self.reconciliation_target()
            if target is None:
                return
            active, req = target
            digest = active['map_sha256']
            ids = set(active['workers'])
            async with self.lock('worker-'+worker['id'], wait=False):
                try:
                    status = await asyncio.wait_for(self.control(worker, {'operation':'status'}), 3)
                except Exception:
                    status = None
                valid = status is not None and self.attests(status, digest)
                if status is not None and self.prepared_status(status, digest):
                    tail = json.loads((Path(req['directory'])/'shards.json').read_text())['shards'][-1]
                    self.canonical.pop(tail['end_height'], None)
                    try:
                        canonical = await self.canonical_hash(tail['end_height']) == tail['terminal_block_hash']
                    except Exception as exc:
                        canonical = False
                        print(json.dumps({'event':'membership_canonical_check_failed','worker':worker['id'],'error':str(exc)}),file=sys.stderr)
                    if canonical and not valid:
                        await self.control(worker, {'operation':'activate', 'expected':status['active']['map_sha256'], 'map_sha256':digest})
                        status = await self.control(worker, {'operation':'status'})
                        valid = self.attests(status, digest)
                    valid = valid and canonical
                updated = ids | {worker['id']} if valid else ids - {worker['id']}
                if updated != ids:
                    await self.route([w for w in self.roster if w['id'] in updated] if self.quorum(updated) else [], json.loads(Path(active['assignment']).read_text()))
                    atomic_json(self.root/'active.json', {**active, 'workers':sorted(updated)})
                    print(json.dumps({'event':'worker_membership','worker':worker['id'],'map_sha256':digest,'eligible':valid}),file=sys.stderr)

    async def managed_once(self, worker):
        await self.reconcile_member(worker)
        path = self.root/'desired.json'
        if not path.exists():
            return
        req = json.loads(path.read_text())
        digest = req['map_sha256']
        status = await self.control(worker, {'operation':'status'})
        if self.prepared_status(status, digest):
            self.job(worker, req, 'prepared', expected=status['active']['map_sha256'])
            return
        if status.get('preparing'):
            # A control client/daemon restart does not cancel server-owned work.
            # Observe that work rather than enqueue another blocking command.
            self.job(worker, req, 'recovering', running=status['preparing'])
            return
        assignment = Path(req.get('assignment', self.root/(digest+'.assignment.json')))
        if not assignment.exists():
            return
        self.canonical.clear()
        tail = json.loads((Path(req['directory'])/'shards.json').read_text())['shards'][-1]
        if await self.canonical_hash(tail['end_height']) != tail['terminal_block_hash']:
            self.job(worker, req, 'rejected', reason='noncanonical target')
            return
        self.job(worker, req, 'staging')
        started = time.monotonic()
        await self.stage(worker, req, assignment)
        status = await self.control(worker, {'operation':'status'})
        if not self.prepared_status(status, digest):
            raise RuntimeError('completed preparation did not attest requested revision')
        self.job(worker, req, 'prepared', expected=status['active']['map_sha256'], seconds=time.monotonic()-started)
        await self.reconcile_member(worker)
        # The next iteration reads the latest desired target; no obsolete
        # intermediate targets accumulate in a queue.

    async def serve_reconciler(self):
        managed = [w for w in self.roster if w['id'] in self.managed_ids()]
        if any(w['role'] != 'recent-replica' for w in managed) or len(managed) != len(self.managed_ids()):
            raise ValueError('managed preparation requires known recent replicas')
        async def worker_loop(worker):
            while True:
                try:
                    await self.managed_once(worker)
                except Exception as exc:
                    self.job(worker, {}, 'failed', error=type(exc).__name__+': '+str(exc))
                await asyncio.sleep(0.5)
        async def heartbeat():
            while True:
                atomic_json(self.root/'reconciler-heartbeat.json', {'monotonic':time.monotonic(), 'workers':sorted(self.managed_ids())})
                await asyncio.sleep(1)
        async def legacy():
            while True:
                try:
                    await self.reconcile()
                except Exception as exc:
                    print(str(exc),file=sys.stderr)
                await asyncio.sleep(2)
        async with self.lock('reconciler', wait=False):
            async with asyncio.TaskGroup() as group:
                group.create_task(heartbeat())
                group.create_task(legacy())
                for worker in managed:
                    group.create_task(worker_loop(worker))

    async def request(self, req):
        if req['operation']=='prepare':
            return await self.prepare(req)
        if req['operation']=='activate':
            return await self.activate(req)
        if req['operation']=='invalidate':
            return await self.invalidate(req)
        if req['operation']=='withdraw':
            async with self.lock('routing'):
                atomic_json(self.root/'withdrawn.json', {'withdrawn':True})
                await self.route([])
                return {'ok':True}
        raise ValueError('unknown fleet operation')


async def main():
    config = json.loads(Path(sys.argv[1]).read_text())
    if '--reconcile' in sys.argv[2:]:
        fleet = Fleet(config)
        await fleet.serve_reconciler()
        return
    request = json.load(sys.stdin)
    print(json.dumps(await Fleet(config).request(request)))


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except Exception as exc:
        print(str(exc),file=sys.stderr)
        sys.exit(1)
