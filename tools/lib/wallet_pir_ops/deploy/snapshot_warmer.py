"""Pin immutable public files and reuse caches without starting serving processes."""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import uuid


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


def verify_history_capacity(specification):
    """Check the complete archive's memory and remaining disk needs before loading the real services."""
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


def main(argv=None):
    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument('operation', choices=('pin', 'reuse', 'seed', 'capacity'))
    parser.add_argument('arguments', nargs='+')
    arguments = parser.parse_args(argv)
    expected = {'pin': 2, 'reuse': 2, 'seed': 3, 'capacity': 1}[arguments.operation]
    if len(arguments.arguments) != expected:
        parser.error('%s needs exactly %d arguments' % (arguments.operation, expected))
    if arguments.operation == 'pin':
        result = pin(*arguments.arguments)
    elif arguments.operation == 'reuse':
        result = reuse(*arguments.arguments)
    elif arguments.operation == 'seed':
        result = seed(*arguments.arguments)
    else:
        result = verify_history_capacity(json.loads(Path(arguments.arguments[0]).read_text()))
    print(json.dumps(result))


if __name__ == '__main__':
    main()
