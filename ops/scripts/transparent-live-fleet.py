#!/usr/bin/env python3
"""Private fleet adapter for transparent-publish-controller.

One JSON request on stdin, one JSON result on stdout. Logs go to stderr.
Artifact transfer and warm preparation run concurrently; activation requires all
archive owners and at least one recent replica. SSH authenticates every operation.
"""
import asyncio
import json
import os
from pathlib import Path
import re
import shlex
import sys
import tempfile


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
    proc = await asyncio.create_subprocess_exec(*map(str, args), stdin=asyncio.subprocess.PIPE,
                                              stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    try:
        out, err = await asyncio.wait_for(proc.communicate(data), timeout)
    except BaseException:
        proc.kill()
        await proc.wait()
        raise
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
        self.canonical = {}
        self.rpc_slots = asyncio.Semaphore(8)
        self.ssh_args = ['ssh', '-oBatchMode=yes', '-oConnectTimeout=3', '-oStrictHostKeyChecking=yes',
                         '-o', 'UserKnownHostsFile=' + config['known_hosts'], '-i', config['ssh_key']]
        for worker in self.roster:
            for field in ['id', 'ssh_host', 'upstream']:
                if not re.fullmatch(r'[A-Za-z0-9_.:-]+', worker[field]):
                    raise ValueError(f'invalid roster {field}')

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

    async def revoke_orphans(self, worker, status, from_height=0, known=()):
        revisions = status.get('revisions',[])
        async def accepted(revision):
            return revision['digest'] if revision['end_height']<from_height or revision['digest'] in known or await self.canonical_hash(revision['end_height'])==revision['terminal_block_hash'] else None
        keep = [digest for digest in await asyncio.gather(*(accepted(r) for r in revisions)) if digest]
        if len(keep)!=len(revisions) or status.get("invalidated"):
            await self.control(worker,{'operation':'invalidate','expected':status['active']['map_sha256'],'from_height':from_height,'keep_digests':keep})
        return keep

    async def ssh(self, host, command, data=None, timeout=25):
        return await run(self.ssh_args + ['root@' + host, command], data, timeout)

    async def control(self, worker, value):
        command = shlex.join([self.c.get('control_binary', '/usr/local/bin/shard-control'),
                              self.c.get('control_socket', '/run/transparent-pir/control.sock')])
        # A rejected command is JSON on stdout with exit status 1. Preserve
        # that diagnostic; transport failures and crashes must still fail SSH.
        command += ' || [ "$?" -eq 1 ]'
        result = json.loads(await self.ssh(worker['ssh_host'], command, json.dumps(value).encode()))
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
        remote_dir = self.c.get('worker_root', '/srv/transparent-pir/publications') + '/' + digest
        remote_assignment = remote_dir + '/assignment.json'
        async def stage(worker):
            status = await self.control(worker, {'operation': 'status'})
            before = status['active']
            current_digests = {e['manifest_digest'] for e in json.loads((source/'shards.json').read_text())['shards']}
            await self.revoke_orphans(worker,status,known=current_digests)
            await self.control(worker, {'operation':'collect'})
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
            await self.control(worker, {'operation':'prepare','expected':before['map_sha256'],'publication':publication})
            return {'expected':before['map_sha256'], 'publication':publication}
        async def bounded_stage(worker):
            return await asyncio.wait_for(stage(worker), 24)
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
        text = '\n'.join(site+' {\n'+'\n'.join(body)+'\n}\n' for site in sites)
        target = self.c.get('router_file','/etc/caddy/Caddyfile')
        quoted = shlex.quote(target)
        # Durable candidate before validate/rename; Caddy reload is atomic.
        command = f'set -eu\ncat > {quoted}.live-next\ncaddy validate --config {quoted}.live-next --adapter caddyfile >&2\ncp {quoted} {quoted}.live-previous\nmv {quoted}.live-next {quoted}\nif ! systemctl reload caddy; then cp {quoted}.live-previous {quoted}; systemctl reload caddy; exit 1; fi'
        await self.ssh(self.c['router_host'], command, text.encode())

    async def activate(self, req):
        digest = req['map_sha256']
        prepared = req['prepared']['workers']
        workers = [w for w in self.roster if w['id'] in prepared]
        async def activate_worker(worker):
            await self.control(worker, {'operation':'activate','expected':prepared[worker['id']]['expected'],'map_sha256':digest})
            status = await self.control(worker, {'operation':'status'})
            if status['active']['map_sha256'] != digest or not status['warm']:
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
        return {'ok':True,'upstreams':[w['upstream'] for w in workers], 'recent_replicas':sum(w['role']=='recent-replica' for w in workers)}

    async def invalidate(self, req):
        # Withdraw routing before contacting workers, including unreachable ones.
        await self.route([])
        async def revoke(worker):
            status = await self.control(worker, {'operation':'status'})
            if 'revisions' not in status:
                return await self.control(worker, {'operation':'invalidate','expected':status['active']['map_sha256'],'from_height':req['from_height']})
            return await self.revoke_orphans(worker,status,req['from_height'])
        revoked = await self.collect(revoke,self.roster)
        return {'ok':True,'invalidated':list(revoked)}

    async def request(self, req):
        if req['operation']=='prepare':
            return await self.prepare(req)
        if req['operation']=='activate':
            return await self.activate(req)
        if req['operation']=='invalidate':
            return await self.invalidate(req)
        if req['operation']=='withdraw':
            await self.route([])
            return {'ok':True}
        raise ValueError('unknown fleet operation')


async def main():
    config = json.loads(Path(sys.argv[1]).read_text())
    request = json.load(sys.stdin)
    print(json.dumps(await Fleet(config).request(request)))


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except Exception as exc:
        print(str(exc),file=sys.stderr)
        sys.exit(1)
