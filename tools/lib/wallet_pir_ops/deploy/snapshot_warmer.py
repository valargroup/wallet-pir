"""Pin immutable public files and reuse caches without starting serving processes."""
import argparse
import json
import os
from pathlib import Path
import re
import stat
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


def same(source, pinned):
    """Whether every pinned file is still the same inode, with the same size, in its source."""
    source, pinned = Path(source), Path(pinned)
    if any(path.is_symlink() or not path.is_dir() for path in (source, pinned)):
        raise ValueError('snapshot comparison needs regular directories')
    compared, differs = 0, []
    for directory, _, files in os.walk(pinned, followlinks=False):
        relative = Path(directory).relative_to(pinned)
        live = source / relative
        for name in files:
            compared += 1
            expected = (Path(directory) / name).lstat()
            try:
                observed = None if relative.parts and live.is_symlink() else (live / name).lstat()
            except FileNotFoundError:
                observed = None
            if observed is None or not stat.S_ISREG(observed.st_mode) or (
                    (observed.st_dev, observed.st_ino, observed.st_size)
                    != (expected.st_dev, expected.st_ino, expected.st_size)):
                differs.append(str(relative / name))
    return {'compared': compared, 'same': not differs, 'differs': sorted(differs)[:16]}


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


def main(argv=None):
    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument('operation', choices=('pin', 'same', 'reuse', 'seed'))
    parser.add_argument('arguments', nargs='+')
    arguments = parser.parse_args(argv)
    expected = {'pin': 2, 'same': 2, 'reuse': 2, 'seed': 3}[arguments.operation]
    if len(arguments.arguments) != expected:
        parser.error('%s needs exactly %d arguments' % (arguments.operation, expected))
    if arguments.operation == 'pin':
        result = pin(*arguments.arguments)
    elif arguments.operation == 'same':
        result = same(*arguments.arguments)
    elif arguments.operation == 'reuse':
        result = reuse(*arguments.arguments)
    else:
        result = seed(*arguments.arguments)
    print(json.dumps(result))


if __name__ == '__main__':
    main()
