#!/usr/bin/env python3
"""Command transport between txid-display-controller and remote display workers.

    txid-display-fleet.py CONFIG [REQUEST [REPLY]]

Reads one JSON request (from REQUEST, else stdin) and writes one JSON reply (to
REPLY, else stdout); exits 0 only when the reply says `"ok": true`. Mirrors
the history adapter's contract with its controller (`controller::fleet`):

    {"operation":"ship","worker":W,"kind":"candidate"|"staged","source":DIR,
     "name":DIGEST,"link_dest":DIR|null,"deadline_seconds":N?}
        -> {"ok":true,"directory":DIR,"seconds":S}
    {"operation":"control","worker":W,"command":{...},"deadline_seconds":N?}
        -> {"ok":true,"reply":{...},"seconds":S}

`deadline_seconds` is an extension for the deploy, which ships a whole
bootstrap candidate and polls long prepares; the controller never sends it.

`ship` copies a coordinator directory into the worker's publications (a
candidate, named by its map digest) or staged (a sealed revision, named by
its digest) root. rsync writes `.tmp-<name>` and only a rename publishes it,
so a worker never sees a partial directory; names are content digests, so an
existing directory is complete and is reused. Candidates hard-link unchanged
files from the previous candidate and from staged revisions (`--link-dest`),
which keeps inode identity for the worker's reuse checks.

`control` runs `txid-control SOCKET` on the worker with the command on stdin.
A worker refusal (`"ok": false` from the socket) fails the call and carries
the worker's reply and error, so no caller can mistake it for success.

SSH reuses the history deploy key and pinned known hosts, read-only, with one
persistent ControlMaster for transfers and another for control, so a large
rsync never delays a control round trip.
"""
import argparse
import json
from pathlib import Path
import re
import shlex
import subprocess
import sys
import time

SCHEMA = 'txid-display-fleet-v1'
DIGEST = re.compile('[0-9a-f]{64}')
NAME = re.compile('[a-z][a-z0-9-]{0,31}')
HOST = re.compile(r'[A-Za-z0-9_.:-]+')
PATH = re.compile(r'/[A-Za-z0-9._/-]+')
OPERATIONS = ('status', 'stage', 'unstage', 'prepare', 'activate', 'invalidate', 'discard', 'collect')
# Below the controller's timeouts (90 s per call; 600 s to ship and stage a
# sealed revision) so this process reaps its rsync and ssh children before the
# controller kills it. A ship's deadline covers its probe, copy and rename.
DEADLINES = {'control': 85, 'stage': 590, 'candidate': 85, 'staged': 590}
# txid-control itself waits at most 600 s for the worker's reply.
MAX_CONTROL_DEADLINE = 600
MAX_SHIP_DEADLINE = 3600
# History's namespaces. Display paths must never point into them.
HISTORY_ROOTS = ('/srv/transparent-pir', '/opt/transparent-publisher', '/srv/transparent-activity',
                 '/run/transparent-pir')


class FleetError(RuntimeError):
    pass


def require(ok, message):
    if not ok:
        raise FleetError(message)


def plain(path, what):
    require(isinstance(path, str) and PATH.fullmatch(path) and '..' not in path.split('/')
            and '//' not in path and not path.endswith('/'), '%s must be a plain absolute path' % what)
    return path


def under(path, root):
    return path.startswith(root + '/')


def load_config(path):
    config = json.loads(Path(path).read_text())
    require(isinstance(config, dict) and config.get('schema') == SCHEMA, 'fleet config schema must be ' + SCHEMA)
    require(set(config) == {'schema', 'ssh_key', 'known_hosts', 'control_dir', 'workers'}, 'unexpected fleet config')
    for key in ('ssh_key', 'known_hosts', 'control_dir'):
        plain(config[key], key)
    require(isinstance(config['workers'], dict) and config['workers'], 'fleet config names no workers')
    for name, worker in config['workers'].items():
        require(NAME.fullmatch(name), 'invalid worker name %r' % name)
        require(isinstance(worker, dict) and set(worker) == {'ssh_host', 'user', 'publications', 'staged',
                                                            'control_socket', 'control_binary'},
                'worker %s has unexpected fields' % name)
        require(HOST.fullmatch(worker['ssh_host']) and re.fullmatch('[a-z_][a-z0-9_-]{0,31}', worker['user']),
                'worker %s has an invalid SSH destination' % name)
        for key in ('publications', 'staged', 'control_socket', 'control_binary'):
            plain(worker[key], '%s.%s' % (name, key))
            require(not any(worker[key] == root or under(worker[key], root) for root in HISTORY_ROOTS),
                    '%s.%s points into a history path' % (name, key))
        require(worker['publications'] != worker['staged'], 'worker %s roots must differ' % name)
    return config


class Fleet:
    def __init__(self, config, run=subprocess.run, clock=time.monotonic):
        self.c = config
        self.run = run
        self.clock = clock
        Path(config['control_dir']).mkdir(mode=0o700, parents=True, exist_ok=True)

    def ssh_options(self, channel):
        """Pinned, non-interactive SSH with a persistent master per channel."""
        return ['-oBatchMode=yes', '-oConnectTimeout=5', '-oStrictHostKeyChecking=yes', '-oIdentitiesOnly=yes',
                '-oServerAliveInterval=5', '-oServerAliveCountMax=3',
                '-o', 'UserKnownHostsFile=' + self.c['known_hosts'], '-i', self.c['ssh_key'],
                '-oControlMaster=auto', '-oControlPersist=300',
                '-o', 'ControlPath=%s/%s-%%C' % (self.c['control_dir'], channel)]

    def destination(self, worker):
        return '%s@%s' % (worker['user'], worker['ssh_host'])

    def execute(self, argv, data, deadline):
        try:
            result = self.run(argv, input=data, capture_output=True, timeout=deadline)
        except subprocess.TimeoutExpired:
            raise FleetError('%s timed out after %ss' % (Path(argv[0]).name, deadline)) from None
        if result.returncode:
            raise FleetError('%s exited %d: %s' % (Path(argv[0]).name, result.returncode,
                                                   result.stderr.decode(errors='replace').strip()[-2000:]))
        return result.stdout.decode()

    def ssh(self, worker, command, data=b'', deadline=60, channel='control'):
        return self.execute(['ssh', *self.ssh_options(channel), self.destination(worker), command], data, deadline)

    def worker(self, request):
        name = request.get('worker')
        require(isinstance(name, str) and name in self.c['workers'], 'unknown worker %r' % (name,))
        return self.c['workers'][name]

    def ship(self, request):
        require(set(request) - {'deadline_seconds'} == {'operation', 'worker', 'kind', 'source', 'name', 'link_dest'},
                'ship takes worker, kind, source, name, link_dest and an optional deadline_seconds')
        worker = self.worker(request)
        require(request['kind'] in ('candidate', 'staged'), 'ship kind is candidate or staged')
        require(isinstance(request['name'], str) and DIGEST.fullmatch(request['name']), 'ship name must be a digest')
        source = Path(plain(request['source'], 'source'))
        require(source.is_dir() and not source.is_symlink(), 'ship source %s is not a directory' % source)
        root = worker['publications'] if request['kind'] == 'candidate' else worker['staged']
        final, partial = '%s/%s' % (root, request['name']), '%s/.tmp-%s' % (root, request['name'])
        link = request['link_dest']
        if link is not None:
            plain(link, 'link_dest')
            require(under(link, root) and DIGEST.fullmatch(link.rsplit('/', 1)[1]),
                    'link_dest must be a digest directory under %s' % root)
        deadline = request.get('deadline_seconds', DEADLINES[request['kind']])
        require(type(deadline) is int and 0 < deadline <= MAX_SHIP_DEADLINE, 'deadline_seconds must be 1..%d'
                % MAX_SHIP_DEADLINE)
        end = self.clock() + deadline

        def left(cap=None):
            remaining = end - self.clock()
            require(remaining > 0, 'ship deadline of %ds exhausted' % deadline)
            return remaining if cap is None else min(cap, remaining)

        quoted = {key: shlex.quote(value) for key, value in
                  (('root', root), ('staged', worker['staged']), ('final', final), ('link', link or ''))}
        probe = ('set -eu; mkdir -p {root} {staged}; if [ -d {final} ]; then echo present; '
                 'elif [ -n {link} ] && [ -d {link} ]; then echo link; else echo nolink; fi').format(**quoted)
        state = self.ssh(worker, probe, deadline=left(60), channel='ship').strip()
        if state == 'present':
            return {'ok': True, 'directory': final, 'reused': True}
        links = (['--link-dest=' + link] if state == 'link' else [])
        if request['kind'] == 'candidate':
            # A candidate's digest directories match staged revisions by relative path.
            links.append('--link-dest=' + worker['staged'])
        # Resumes into an existing partial copy; --delete drops anything stale.
        self.execute(['rsync', '-a', '--delete', '--numeric-ids', *links,
                      '-e', shlex.join(['ssh', *self.ssh_options('ship')]),
                      str(source) + '/', '%s:%s/' % (self.destination(worker), partial)], b'', left())
        publish = 'set -eu; test ! -e {final}; mv -T {partial} {final}; sync -f {final}'.format(
            final=quoted['final'], partial=shlex.quote(partial))
        self.ssh(worker, publish, deadline=left(60), channel='ship')
        return {'ok': True, 'directory': final}

    def control(self, request):
        require(set(request) <= {'operation', 'worker', 'command', 'deadline_seconds'} and 'command' in request,
                'control takes worker, command and an optional deadline_seconds')
        worker = self.worker(request)
        command = request['command']
        require(isinstance(command, dict) and command.get('operation') in OPERATIONS,
                'unknown worker control operation %r' % (command.get('operation') if isinstance(command, dict)
                                                         else command,))
        # Paths a worker opens must sit in its own display roots.
        if command['operation'] == 'stage':
            plain(command.get('directory'), 'stage directory')
            require(under(command['directory'], worker['staged']), 'stage directory must be under staged')
        if command['operation'] == 'prepare':
            directory = (command.get('publication') or {}).get('directory')
            plain(directory, 'prepare directory')
            require(under(directory, worker['publications']), 'prepare directory must be under publications')
        deadline = request.get('deadline_seconds', DEADLINES['stage' if command['operation'] == 'stage'
                                                             else 'control'])
        require(type(deadline) is int and 0 < deadline <= MAX_CONTROL_DEADLINE, 'deadline_seconds must be 1..%d'
                % MAX_CONTROL_DEADLINE)
        # txid-control prints a refusal as JSON and exits 1; keep that reply
        # while transport failures and crashes still fail the SSH call.
        remote = shlex.join([worker['control_binary'], worker['control_socket']]) + ' || [ "$?" -eq 1 ]'
        output = self.ssh(worker, remote, (json.dumps(command) + '\n').encode(), deadline, channel='control')
        lines = output.strip().splitlines()
        require(lines, 'worker sent no control reply')
        reply = json.loads(lines[0])
        require(isinstance(reply, dict) and isinstance(reply.get('ok'), bool), 'worker control reply is malformed')
        result = {'ok': reply['ok'], 'reply': reply}
        if not reply['ok']:
            result['error'] = reply.get('error', 'worker refused the command')
        return result

    def handle(self, request):
        require(isinstance(request, dict), 'request must be a JSON object')
        started = self.clock()
        if request.get('operation') == 'ship':
            result = self.ship(request)
        elif request.get('operation') == 'control':
            result = self.control(request)
        else:
            raise FleetError('unknown fleet operation %r' % (request.get('operation'),))
        result['seconds'] = round(self.clock() - started, 6)
        print(json.dumps({'event': 'txid_fleet', 'operation': request['operation'], 'worker': request['worker'],
                          'ok': result['ok'], 'seconds': result['seconds']}), file=sys.stderr)
        return result


def main(argv=None, run=subprocess.run):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('config')
    parser.add_argument('request', nargs='?', help='request file (default stdin)')
    parser.add_argument('reply', nargs='?', help='reply file (default stdout)')
    args = parser.parse_args(argv)
    try:
        raw = Path(args.request).read_text() if args.request else sys.stdin.read()
        result = Fleet(load_config(args.config), run=run).handle(json.loads(raw))
    except (FleetError, OSError, ValueError, KeyError) as error:
        result = {'ok': False, 'error': '%s: %s' % (type(error).__name__, error)}
    if result.get('ok') is not True:
        # A caller that stops at the exit status still logs why.
        print(json.dumps({'event': 'txid_fleet_failed', 'error': result.get('error')}), file=sys.stderr)
    text = json.dumps(result, sort_keys=True) + '\n'
    if args.reply:
        Path(args.reply).write_text(text)
    else:
        sys.stdout.write(text)
    return 0 if result.get('ok') is True else 1


if __name__ == '__main__':
    sys.exit(main())
