#!/usr/bin/env python3
"""Point Transparent quality monitoring at a new shard schema's files.

`apm`, on the coordinator, rewrites only `roster` and `synthetic_status` in the
APM Transparent config and `roster.path` in the host sampler config, after
checking that the new roster parses and the load status is current.
`monitor`, on the monitor host, rewrites only the Transparent probe's canary
binary, fixture and pins (from transparent-canary-pins.py) in the service-probe
config, after the new command passes once.

Read-only unless --apply. --apply copies each file it changes into --backup-dir
first, writes in place atomically with the original mode and owner, and
restarts only the service that reads that file at start. It never changes an
alert mode, credential, incident database or serving process.
"""
import argparse
import datetime
import hashlib
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

ROLES = {'archive-owner', 'recent-replica'}
PIN_FLAGS = ('--fixture', '--fixture-sha256', '--anchor-height', '--anchor-hash')


def load(path):
    return json.loads(Path(path).read_text())


def check_roster(path):
    roster = load(path)
    if not (isinstance(roster, list) and 1 <= len(roster) <= 32 and all(
            isinstance(w, dict) and isinstance(w.get('id'), str) and w.get('role') in ROLES
            and isinstance(w.get('upstream'), str) and isinstance(w.get('ssh_host'), str) for w in roster)):
        raise ValueError(f'{path} is not a worker roster APM and the host sampler can read')
    return sorted(w['id'] for w in roster)


def check_status(path, now, max_age):
    status = load(path)
    written = datetime.datetime.fromisoformat(status['utc']).timestamp()
    if not 0 <= now - written <= max_age:
        raise ValueError(f'{path} was last written {now - written:.0f} s ago')
    return status.get('mode')


def apm_changes(apm, hosts, roster, status):
    changes = {}
    config = load(apm)
    if set(config) - {'publisher_url', 'roster', 'router_metrics_url', 'synthetic_status',
                      'host_snapshot', 'public_origins'} or 'roster' not in config:
        raise ValueError(f'{apm} is not an APM Transparent config')
    updated = {**config, 'roster': str(roster), 'synthetic_status': str(status)}
    if updated != config:
        changes[Path(apm)] = (config, updated)
    sampler = load(hosts)
    if sampler.get('roster') is not None:
        moved = {**sampler, 'roster': {**sampler['roster'], 'path': str(roster)}}
        if moved != sampler:
            changes[Path(hosts)] = (sampler, moved)
    return changes


def monitor_changes(probes, canary, fixture, pins):
    configs = load(probes)
    entries = [c for c in configs if c.get('service') == 'transparent']
    if len(entries) != 1:
        raise ValueError(f'{probes} has no single transparent probe')
    command = list(entries[0]['command'])
    values = {'--fixture': str(fixture), '--fixture-sha256': pins['fixture_sha256'],
              '--anchor-height': str(pins['anchor_height']), '--anchor-hash': pins['anchor_hash']}
    for flag in PIN_FLAGS:
        if command.count(flag) != 1 or any(a.startswith(flag + '=') for a in command):
            raise ValueError(f'transparent probe does not pass {flag} exactly once as a separate argument')
        command[command.index(flag) + 1] = values[flag]
    command[0] = str(canary)
    updated = [{**c, 'command': command} if c is entries[0] else c for c in configs]
    return ({Path(probes): (configs, updated)} if updated != configs else {}), command


def sha256(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def backup_path(path, backup_dir):
    return backup_dir / str(path.resolve()).lstrip('/').replace('/', '__')


def write(path, value, backup_dir):
    stat = path.stat()
    shutil.copy2(path, backup_path(path, backup_dir))
    staged = path.with_name(path.name + '.retarget')
    staged.write_text(json.dumps(value, indent=2) + '\n')
    os.chown(staged, stat.st_uid, stat.st_gid)
    os.chmod(staged, stat.st_mode & 0o7777)
    os.replace(staged, path)


def show(changes):
    for path, (old, new) in changes.items():
        print(json.dumps({'file': str(path), 'before': old, 'after': new}, indent=2))


def main(argv=None, now=time.time, run=subprocess.run):
    parser = argparse.ArgumentParser(description=__doc__.split('\n')[0])
    sub = parser.add_subparsers(dest='role', required=True)
    apm = sub.add_parser('apm')
    apm.add_argument('--apm-config', type=Path, required=True)
    apm.add_argument('--hosts-config', type=Path, default=Path('/etc/pir-quality/hosts.json'))
    apm.add_argument('--roster', type=Path, default=Path('/opt/transparent-publisher/v11/roster.json'))
    apm.add_argument('--load-status', type=Path,
                     default=Path('/srv/transparent-activity/canonical-load/v11/status.json'))
    apm.add_argument('--max-status-age', type=int, default=120)
    monitor = sub.add_parser('monitor')
    monitor.add_argument('--probes-config', type=Path, required=True)
    monitor.add_argument('--canary', type=Path, required=True)
    monitor.add_argument('--canary-sha256', required=True)
    monitor.add_argument('--fixture', type=Path, required=True)
    monitor.add_argument('--pins', type=Path, required=True, help='transparent-canary-pins.py output')
    for p in (apm, monitor):
        p.add_argument('--apply', action='store_true')
        p.add_argument('--backup-dir', type=Path)
    args = parser.parse_args(argv)
    if args.apply and args.backup_dir is None:
        parser.error('--apply requires --backup-dir')
    if args.role == 'apm':
        workers = check_roster(args.roster)
        mode = check_status(args.load_status, now(), args.max_status_age)
        changes = apm_changes(args.apm_config, args.hosts_config, args.roster, args.load_status)
        print(json.dumps({'roster_workers': workers, 'load_mode': mode,
                          'sampler_follows_roster': load(args.hosts_config).get('roster') is not None}))
        show(changes)
        restart = 'pir-apm' if Path(args.apm_config) in changes else None
    else:
        pins = load(args.pins)
        if sha256(args.canary) != args.canary_sha256 or sha256(args.fixture) != pins['fixture_sha256']:
            raise ValueError('canary or fixture does not match its pinned SHA-256')
        changes, command = monitor_changes(args.probes_config, args.canary, args.fixture, pins)
        show(changes)
        probe = run(command, capture_output=True, text=True, timeout=45)
        result = json.loads(probe.stdout.strip().splitlines()[-1]) if probe.stdout.strip() else {}
        print(json.dumps({'probe': result, 'exit': probe.returncode}))
        if probe.returncode != 0 or result.get('passed') is not True:
            print('new transparent probe did not pass; nothing changed', file=sys.stderr)
            return 1
        restart = 'pir-monitor' if changes else None
    if not args.apply:
        print(json.dumps({'apply': False, 'would_restart': restart}))
        return 0
    args.backup_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    if any(backup_path(p, args.backup_dir).exists() for p in changes):
        raise ValueError(f'{args.backup_dir} already holds a backup of a file to change')
    for path, (_, new) in changes.items():
        write(path, new, args.backup_dir)
    if restart:
        run(['systemctl', 'restart', restart], check=True)
    print(json.dumps({'apply': True, 'changed': [str(p) for p in changes], 'restarted': restart,
                      'backup_dir': str(args.backup_dir)}))
    return 0


if __name__ == '__main__':
    sys.exit(main())
