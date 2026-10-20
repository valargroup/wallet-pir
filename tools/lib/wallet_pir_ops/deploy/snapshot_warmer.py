"""Host-side immutable snapshot warming, owned by the publisher transaction.

The temporary service owns only pinned public files and its query-server child.
It opens no publisher journal and never changes a serving publication record.
"""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import time
import urllib.error
import urllib.request
import uuid

WARMUP_SECONDS = 3600
SHARD_WARMUP_SECONDS = 900


def sync_directory(path):
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def pin(source, destination):
    source, destination = Path(source), Path(destination)
    if source.is_symlink() or not source.is_dir() or destination.exists():
        raise ValueError('snapshot pin needs a regular source and a new destination')
    if destination.resolve().is_relative_to(source.resolve()):
        raise ValueError('snapshot pin must be outside its source')
    destination.mkdir(parents=True, mode=0o700)
    count = 0
    for directory, directories, files in os.walk(source, followlinks=False):
        relative = Path(directory).relative_to(source)
        for name in directories:
            entry = Path(directory) / name
            if entry.is_symlink():
                raise ValueError('snapshot contains a symbolic link')
            (destination / relative / name).mkdir(mode=0o700)
        for name in files:
            entry = Path(directory) / name
            if not stat.S_ISREG(entry.lstat().st_mode):
                raise ValueError('snapshot contains a non-regular file')
            if not name.endswith('.partial'):
                os.link(entry, destination / relative / name, follow_symlinks=False)
                count += 1
    for directory, _, _ in os.walk(destination, topdown=False):
        sync_directory(directory)
    return {'pinned_files': count}


def completed_runtimes(source):
    source = Path(source)
    if source.is_symlink() or not source.is_dir():
        raise ValueError('prepared cache source must be a regular directory')
    for entry in source.iterdir():
        if not re.fullmatch(r'[0-9a-f]{64}-[0-9a-f]{64}\.runtime', entry.name):
            continue
        if not stat.S_ISREG(entry.lstat().st_mode):
            raise ValueError('prepared runtime is not a regular file')
        yield entry


def reuse(source, destination):
    source, destination = Path(source), Path(destination)
    if destination.is_symlink() or not destination.is_dir():
        raise ValueError('prepared cache destination must be a regular directory')
    count = 0
    entries = completed_runtimes(source) if source.exists() or source.is_symlink() else []
    for entry in entries:
        target = destination / entry.name
        if not target.exists():
            os.link(entry, target, follow_symlinks=False)
            count += 1
    sync_directory(destination)
    return {'reused_runtimes': count,
            'runtime_bytes': sum(entry.stat().st_size for entry in completed_runtimes(destination))}


def seed(source, record, root):
    root = Path(root)
    destination = Path(json.loads(Path(record).read_text())['directory'])
    if not destination.resolve().is_relative_to(root.resolve()):
        raise ValueError('active publication escapes its root')
    count = size = 0
    for entry in completed_runtimes(source):
        target = destination / entry.name
        if target.is_symlink():
            raise ValueError('prepared runtime target is a symbolic link')
        if not target.exists() or not target.samefile(entry):
            temporary = destination / ('.cutover-link-' + uuid.uuid4().hex)
            os.link(entry, temporary, follow_symlinks=False)
            try:
                os.replace(temporary, target)
            finally:
                temporary.unlink(missing_ok=True)
        count += 1
        size += entry.stat().st_size
    sync_directory(destination)
    return {'linked_runtimes': count, 'runtime_bytes': size}


def write_json(path, value):
    path = Path(path)
    temporary = path.with_suffix('.partial')
    with temporary.open('w') as stream:
        os.fchmod(stream.fileno(), 0o600)
        json.dump(value, stream, separators=(',', ':'))
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)
    sync_directory(path.parent)


def assignment(identity, shard, budget):
    # Keep the struct field order used by the product's canonical serialization.
    keys = ('shard_schema', 'map_sha256', 'network', 'genesis_hash', 'shards',
            'start_height', 'covered_through', 'recent_from_shard')
    return {
        'schema': 'transparent-assignment-v1',
        'set': {key: identity[key] for key in keys},
        'generated_by': {'tool': 'publisher-snapshot-validation', 'source_sha': None,
                         'generated_at': datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')},
        'workers': [{'id': 'snapshot-validator', 'role': 'archive-owner', 'replica_group': None,
                     'upstream': '127.0.0.1:18990', 'cache_bytes': budget, 'shards': [shard],
                     'estimated_resident_bytes': budget}],
        'unassigned': [i for i in range(identity['shards']) if i != shard],
    }


def owns_listener(pid, port, proc_root=Path('/proc')):
    """Bind a readiness response to our child, including on a shared host."""
    process = proc_root / str(pid)
    sockets = set()
    try:
        for descriptor in (process / 'fd').iterdir():
            try:
                link = os.readlink(descriptor)
            except OSError:
                continue
            if link.startswith('socket:[') and link.endswith(']'):
                sockets.add(link[8:-1])
        for line in (process / 'net/tcp').read_text().splitlines()[1:]:
            fields = line.split()
            if (len(fields) >= 10 and fields[1] == '0100007F:%04X' % port
                    and fields[3] == '0A' and fields[9] in sockets):
                return True
    except OSError:
        pass
    return False


def ready_report(specification, assignment_digest, child):
    if child.poll() is not None:
        raise RuntimeError('snapshot query server exited before runtime readiness')
    if not owns_listener(child.pid, 18990):
        return None
    try:
        try:
            response = urllib.request.urlopen('http://127.0.0.1:18990/v1/ready', timeout=2)
        except urllib.error.HTTPError as error:
            response = error
        with response:
            status = response.status
            report = json.load(response)
    except (OSError, ValueError):
        return None
    if report.get('prewarm_finished') and not report.get('ready'):
        raise RuntimeError('snapshot prewarm finished without full readiness: ' + str(report.get('reason')))
    if status != 200 or not report.get('ready') or not report.get('prewarm_finished'):
        return None
    if (report.get('map_sha256') != specification['map_sha256']
            or report.get('binary_sha256') != specification['binary_sha256']
            or report.get('prewarm_failed') != 0
            or report.get('target_runtimes', 0) <= 0
            or report.get('warm_runtimes') != report.get('target_runtimes')):
        raise RuntimeError('snapshot runtime readiness differs from the staged identity')
    if assignment_digest is not None:
        if (report.get('mode') != 'warm' or report.get('assigned_shards') != 1
                or report.get('assignment_sha256') != assignment_digest):
            raise RuntimeError('snapshot worker did not warm its exact assignment')
    elif report.get('role') != 'combined' or report.get('warm') is not True:
        raise RuntimeError('snapshot worker did not warm combined transaction-detail holdings')
    with urllib.request.urlopen('http://127.0.0.1:18990/metrics', timeout=2) as response:
        metrics = response.read().decode()
    values = {}
    for line in metrics.splitlines():
        if line and not line.startswith('#'):
            name = line.split()[0].split('{')[0]
            if name in ('transparent_shard_disk_write_failures_total', 'transparent_shard_disk_save_pending'):
                values[name] = values.get(name, 0) + float(line.split()[-1])
    if values.get('transparent_shard_disk_write_failures_total') != 0:
        raise RuntimeError('snapshot runtime cache could not persist every prepared file')
    if values.get('transparent_shard_disk_save_pending') != 0:
        return None
    with open('/proc/%d/exe' % child.pid, 'rb') as executable:
        if hashlib.file_digest(executable, 'sha256').hexdigest() != specification['binary_sha256']:
            raise RuntimeError('snapshot query executable changed')
    if child.poll() is not None or not owns_listener(child.pid, 18990):
        raise RuntimeError('snapshot query process stopped during readiness verification')
    return report


def stop_child(child):
    if child.poll() is None:
        child.terminate()
        try:
            child.wait(timeout=10)
        except subprocess.TimeoutExpired:
            child.kill()
            child.wait(timeout=30)


def verify_history_capacity(specification):
    """Check the complete archive's memory and remaining disk needs before warming."""
    root = Path(specification['work'])
    budget = specification['verification_cache_bytes']
    assigned = assignment(specification['identity'], 0, budget)
    assigned['workers'][0]['shards'] = list(range(specification['identity']['shards']))
    assigned['unassigned'] = []
    assignment_path = root / 'capacity-assignment.json'
    write_json(assignment_path, assigned)
    settings = {'shard_dir': specification['pin'], 'cache_bytes': budget,
                'assignment': str(assignment_path), 'worker_id': 'snapshot-validator',
                'pilot_cold': False, 'build_slots': 1,
                'runtime_cache_dir': specification['pin'],
                'runtime_cache_max_bytes': specification['disk_bytes']}
    path = root / 'capacity-query.json'
    write_json(path, {'management_listen': '127.0.0.1:18991',
                      'transparent_history': {'enabled': True, 'settings': settings}})
    result = subprocess.run([specification['binary'], '--config', str(path), '--verify-only'],
                            capture_output=True, text=True, timeout=300)
    output = result.stdout + result.stderr
    with (root / 'capacity.log').open('w') as log:
        os.fchmod(log.fileno(), 0o600)
        log.write(output)
    if result.returncode:
        raise RuntimeError('complete history cache does not fit: ' + output[-2000:])
    report = json.loads(result.stdout)
    if report.get('verified') is not True or not isinstance(report.get('runtime_cache'), dict):
        raise RuntimeError('complete history cache verification returned no sizing report')
    return report['runtime_cache']


def warm(specification):
    root = Path(specification['work'])
    section = specification['section']
    history = section == 'transparent_history'
    shards = range(specification['identity']['shards']) if history else [None]
    result = {'map_sha256': specification['map_sha256'],
              'binary_sha256': specification['binary_sha256'], 'complete': False, 'checks': []}
    deadline = time.monotonic() + WARMUP_SECONDS
    if history:
        result['capacity'] = verify_history_capacity(specification)
    for shard in shards:
        shard_deadline = min(deadline, time.monotonic() + SHARD_WARMUP_SECONDS)
        settings = {
            'listen': '127.0.0.1:18990', 'cache_bytes': specification['memory_bytes'],
            'runtime_cache_dir': specification['pin'],
            'runtime_cache_max_bytes': specification['disk_bytes'],
            'runtime_restore_slots': 1, 'build_slots': 1,
            'active_record': str(root / 'validation-active.json'),
        }
        digest = None
        if history:
            assigned = assignment(specification['identity'], shard, specification['memory_bytes'])
            raw = json.dumps(assigned, separators=(',', ':'))
            digest = hashlib.sha256(raw.encode()).hexdigest()
            assignment_path = root / 'assignment.json'
            write_json(assignment_path, assigned)
            settings.update(shard_dir=specification['pin'], assignment=str(assignment_path),
                            worker_id='snapshot-validator', pilot_cold=False)
        else:
            settings.update(publication_dir=specification['pin'], role='combined', loaded_only=False)
        config = {'management_listen': '127.0.0.1:18991',
                  section: {'enabled': True, 'settings': settings}}
        config_path = root / 'query.json'
        write_json(config_path, config)
        log_path = root / ('query-%s.log' % shard)
        with log_path.open('w') as log:
            os.fchmod(log.fileno(), 0o600)
            child = subprocess.Popen([specification['binary'], '--config', str(config_path)],
                                     stdin=subprocess.DEVNULL, stdout=log, stderr=log)
            try:
                while True:
                    report = ready_report(specification, digest, child)
                    if report is not None:
                        break
                    if time.monotonic() >= shard_deadline:
                        raise RuntimeError('production snapshot warming deadline exceeded')
                    time.sleep(.25)
            except Exception as error:
                raise RuntimeError(str(error) + ': ' + log_path.read_text()[-2000:]) from error
            finally:
                stop_child(child)
        result['checks'].append({'shard': shard, 'ready': report})
        write_json(root / 'report.json', result)
    result['complete'] = True
    write_json(root / 'report.json', result)
    return {'validated': len(result['checks']), 'complete': True}


def main(argv=None):
    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument('operation', choices=('pin', 'reuse', 'seed', 'warm'))
    parser.add_argument('arguments', nargs='+')
    arguments = parser.parse_args(argv)
    expected = {'pin': 2, 'reuse': 2, 'seed': 3, 'warm': 1}[arguments.operation]
    if len(arguments.arguments) != expected:
        parser.error('%s needs exactly %d arguments' % (arguments.operation, expected))
    if arguments.operation == 'pin':
        result = pin(*arguments.arguments)
    elif arguments.operation == 'reuse':
        result = reuse(*arguments.arguments)
    elif arguments.operation == 'seed':
        result = seed(*arguments.arguments)
    else:
        result = warm(json.loads(Path(arguments.arguments[0]).read_text()))
    print(json.dumps(result))


if __name__ == '__main__':
    main()
