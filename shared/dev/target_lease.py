"""Reusable local Cargo lanes, owned by kernel leases rather than PID files.

Never unlink or explicitly unlock a lease: children share its open description
and may outlive the Python wrapper. Closing the last inherited fd releases it.
"""
from contextlib import contextmanager
import fcntl
import hashlib
import json
import os
from pathlib import Path
import subprocess

LEASE_FD = 'WALLET_PIR_DEV_LEASE_FD'


def inherited_fds(env=None):
    """Pass the lease through every Python subprocess boundary, including shells."""
    value = (os.environ if env is None else env).get(LEASE_FD)
    if value is None:
        return {}
    fd = int(value)
    os.fstat(fd)  # A stale descriptor must fail, never silently drop ownership.
    return {'pass_fds': (fd,)}


def build_flags(env):
    """Environment that changes compiler output; runtime-only variables are excluded."""
    flags = {key: value for key, value in env.items()
             if key in {'RUSTFLAGS', 'CARGO_ENCODED_RUSTFLAGS', 'RUSTDOCFLAGS',
                        'RUSTC', 'RUSTC_WRAPPER', 'RUSTC_WORKSPACE_WRAPPER',
                        'CC', 'CXX', 'CFLAGS', 'CXXFLAGS', 'AR', 'NVCC',
                        'CUDA_HOME', 'CUDA_PATH'}
             or key.startswith(('CARGO_PROFILE_', 'CARGO_BUILD_', 'CARGO_TARGET_',
                                'CC_', 'CXX_', 'CFLAGS_', 'CXXFLAGS_', 'CUDA_'))}
    flags.pop('CARGO_TARGET_DIR', None)
    return flags


def compatibility(root, env):
    """Cargo handles feature/profile/dependency changes within a compatible lane."""
    compiler = subprocess.check_output(['rustc', '-vV'], cwd=root, env=env)
    flags = build_flags(env)
    configs = [parent / '.cargo' / name for parent in (root, *root.parents)
               for name in ('config', 'config.toml')]
    configs += [Path(env.get('CARGO_HOME', Path.home() / '.cargo')) / name
                for name in ('config', 'config.toml')]
    digest = hashlib.sha256(compiler + json.dumps(flags, sort_keys=True).encode())
    for config in configs:
        if config.is_file():
            digest.update(str(config).encode())
            digest.update(config.read_bytes())
    return digest.hexdigest()[:20]


def acquire(base):
    """Choose the first released lane; allocation races also use nonblocking locks."""
    base = Path(base).resolve()
    index = 0
    while True:
        target = base / f'lane-{index}'
        target.mkdir(parents=True, exist_ok=True)
        fd = os.open(target / '.lease', os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            os.close(fd)
            index += 1
            continue
        except BaseException:
            os.close(fd)
            raise
        return target, fd


@contextmanager
def local_target(root, *, enabled=True):
    """Reserve one lane for the whole local check, including discovery/execution.

CI already owns isolated targets and keeps its existing cache contract. A local
CARGO_TARGET_DIR override is a pool root, not permission to share a writer lane.
"""
    if not enabled or os.environ.get('GITHUB_ACTIONS') == 'true':
        yield
        return
    env = os.environ.copy()
    key = compatibility(root, env)
    pool = Path(env.get('CARGO_TARGET_DIR', root / 'target'))
    if not pool.is_absolute():
        pool = root / pool
    target, fd = acquire(pool / 'dev-lanes' / key)
    previous = {name: os.environ.get(name) for name in ('CARGO_TARGET_DIR', LEASE_FD)}
    os.environ['CARGO_TARGET_DIR'] = str(target)
    os.environ[LEASE_FD] = str(fd)
    print('TARGET_LEASE ' + json.dumps({'target': str(target), 'compatibility': key,
                                      'owner_pid': os.getpid()}), flush=True)
    try:
        yield
    finally:
        for name, value in previous.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value
        os.close(fd)
