#!/usr/bin/env python3
"""Build the qualification tools missing from an exact-main CI release.

Exact-main CI already built transparent-filter-server, transparent-shard-server,
transparent-publish-controller, shard-control, shard-assign and shard-prune. This
driver builds only the remaining 13 tools of the 18-artifact activity inventory
from an immutable `git archive` export of one commit, in one persistent target
lane with sequential Cargo writers. It retains owner, logs, result, artifacts and
a deterministic archive under --root (outside Git) and writes a sanitized manifest.

Run through the task's tool-exec so Cargo uses the task CARGO_HOME, e.g.
  tool-exec --repo wallet-pir -- python3 build-tools.py --repo . --sha <sha> \\
    --root <dir> --target "$CARGO_TARGET_DIR" --lane-lock <lane lock> --manifest <json>
"""
import argparse
import fcntl
import gzip
import hashlib
import io
import json
import os
from pathlib import Path
import platform
import re
import shutil
import subprocess
import tarfile
import time
import tomllib

TOOLCHAIN = '1.97.1'
PROFILE = 'release'
RUSTFLAGS = '-C target-cpu=x86-64-v3 -C target-feature=+pclmulqdq'
FILTER_BINS = ['transparent-event-ingest', 'shard-publish', 'event-spotcheck', 'shard-verify',
               'script-sample', 'shard-cutoff', 'journal-inventory', 'shard-census']
TOOL_BINS = ['transparent-loadtest', 'transparent-measure']
EXAMPLES = ['examples/rate-query', 'examples/native_certificate', 'examples/activity-reopen']
CI_BUILT = ['transparent-filter-server', 'transparent-shard-server', 'transparent-publish-controller',
            'shard-control', 'shard-assign', 'shard-prune']
CARGO = ['cargo', '+' + TOOLCHAIN, 'build', '--locked', '--profile', PROFILE, '-v']
STAGES = [
    ('bins', CARGO + ['-p', 'transparent-filter-server', '-p', 'transparent-loadtest', '-p', 'transparent-measure']
     + [arg for name in FILTER_BINS + TOOL_BINS for arg in ('--bin', name)]),
    ('shard-server-examples', CARGO + ['-p', 'transparent-shard-server', '--example', 'rate-query',
                                       '--example', 'native_certificate']),
    ('wallet-store-example', CARGO + ['-p', 'transparent-wallet-store', '--example', 'activity-reopen']),
]
# Argument handling only: clap help, or the documented refusal without arguments.
PROBES = {name: (['--help'], 0, 'Usage:') for name in FILTER_BINS + TOOL_BINS + ['examples/rate-query']}
PROBES['examples/native_certificate'] = ([], 101, 'segment|synthetic')
PROBES['examples/activity-reopen'] = ([], 1, 'store root required')
INPUTS = ['Cargo.lock', 'Cargo.toml', 'rust-toolchain.toml', '.cargo/config.toml',
          'transparent/services/transparent-filter-server/Cargo.toml',
          'transparent/services/transparent-shard-server/Cargo.toml',
          'transparent/tools/transparent-loadtest/Cargo.toml',
          'transparent/tools/transparent-measure/Cargo.toml',
          'transparent/crates/transparent-wallet-store/Cargo.toml']
ARCHIVED = ['owner.json', 'result.json', 'source.tar.sha256', 'logs', 'artifacts', 'sanity']
CONTEXT = dict(
    purpose='Supplemental fat-LTO tools completing the 18-artifact activity inventory; '
            'not a native, hardware or production certification.',
    ci_release=dict(run='37173250956', roles_not_rebuilt=CI_BUILT,
                    inventory_roles_from_ci=['transparent-filter-server', 'transparent-shard-server',
                                             'transparent-publish-controller', 'shard-control', 'shard-assign']),
    transparent_regression=(
        'Not built: the reviewed activity qualification path (transparent/ops/lib/activity_schema_product.py) '
        'reads only release-<sha>/artifacts/<name> and artifacts/examples/rate-query, and the earlier '
        'activity release builds compiled transparent-regression without retaining it. Its only callers '
        'are the superseded v10 cutover scripts and the manual public regression workflow and make target.'),
    probes='Each artifact was run once on the build host with --help, or without arguments for the two '
           'non-clap examples, to show it loads and reaches argument handling. Nothing else was executed.')


def sha256(path):
    with open(path, 'rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def git(repo, *args, text=True):
    return subprocess.run(['git', '-C', str(repo), *args], check=True, capture_output=True, text=text).stdout


def export_source(repo, sha, root):
    """Export the commit with git archive and prove every file equals its Git blob."""
    tar = root / 'source.tar'
    with tar.open('xb') as out:
        subprocess.run(['git', '-C', str(repo), 'archive', '--format=tar', sha], check=True, stdout=out)
    source = root / 'source'
    source.mkdir()
    subprocess.run(['tar', '-xf', str(tar), '-C', str(source)], check=True)
    verify_source(repo, sha, source)
    for path in sorted(source.rglob('*'), reverse=True):
        if not path.is_symlink():
            path.chmod(path.stat().st_mode & ~0o222)
    source.chmod(source.stat().st_mode & ~0o222)
    return source, sha256(tar)


def verify_source(repo, sha, source):
    expected = {}
    for entry in git(repo, 'ls-tree', '-r', '-z', '--full-tree', sha).split('\0'):
        if entry:
            meta, path = entry.split('\t', 1)
            mode, kind, blob = meta.split()
            if kind != 'blob':
                raise RuntimeError(f'unsupported tree entry {kind} {path}')
            expected[path] = (mode, blob)
    actual = {str(p.relative_to(source)) for p in source.rglob('*') if p.is_file() or p.is_symlink()}
    if actual != set(expected):
        raise RuntimeError('source export file set differs from the commit tree')
    for path, (mode, blob) in expected.items():
        file = source / path
        data = os.readlink(file).encode() if mode == '120000' else file.read_bytes()
        if hashlib.sha1(b'blob %d\0' % len(data) + data).hexdigest() != blob:
            raise RuntimeError(f'source export differs from commit at {path}')
    return len(expected)


def elf_header(path):
    head = path.read_bytes()[:20]
    if head[:4] != b'\x7fELF':
        return dict(elf=False)
    return dict(elf=True, elf_class={1: 'ELF32', 2: 'ELF64'}.get(head[4], head[4]),
                data={1: 'little-endian', 2: 'big-endian'}.get(head[5], head[5]),
                os_abi={0: 'SYSV', 3: 'GNU/Linux'}.get(head[7], head[7]),
                type={2: 'EXEC', 3: 'DYN'}.get(int.from_bytes(head[16:18], 'little'), head[16:18].hex()),
                machine={62: 'x86-64'}.get(int.from_bytes(head[18:20], 'little'), head[18:20].hex()))


def inspect(path):
    info = elf_header(path)
    program = subprocess.run(['readelf', '-lW', str(path)], capture_output=True, text=True).stdout
    interpreter = re.search(r'Requesting program interpreter: ([^\]]+)\]', program)
    symbols = subprocess.run(['objdump', '-T', str(path)], capture_output=True, text=True).stdout
    glibc = sorted({tuple(map(int, v.split('.'))) for v in re.findall(r'GLIBC_(\d+(?:\.\d+)+)', symbols)})
    ldd = subprocess.run(['ldd', str(path)], capture_output=True, text=True)
    needed = re.findall(r'\(NEEDED\)\s+Shared library: \[([^\]]+)\]',
                        subprocess.run(['readelf', '-dW', str(path)], capture_output=True, text=True).stdout)
    info.update(interpreter=interpreter.group(1) if interpreter else None, needed=needed,
                max_glibc='.'.join(map(str, glibc[-1])) if glibc else None,
                ldd_exit=ldd.returncode, unresolved='not found' in ldd.stdout,
                executable=os.access(path, os.X_OK), bytes=path.stat().st_size)
    info['linux_x86_64'] = (info.get('elf_class') == 'ELF64' and info.get('machine') == 'x86-64'
                            and info.get('os_abi') in ('SYSV', 'GNU/Linux') and info.get('type') in ('EXEC', 'DYN')
                            and info['executable'] and info['ldd_exit'] == 0 and not info['unresolved'])
    return info


def probe(name, path, sanity):
    args, code, marker = PROBES[name]
    work = sanity / name.replace('/', '-')
    work.mkdir(parents=True)
    env = {'PATH': '/usr/bin:/bin', 'HOME': str(work), 'LANG': 'C.UTF-8'}
    started = time.monotonic()
    try:
        done = subprocess.run([str(path), *args], cwd=work, env=env, capture_output=True, timeout=30)
        exit_code, out, err = done.returncode, done.stdout, done.stderr
    except subprocess.TimeoutExpired as error:
        exit_code, out, err = 'timeout', error.stdout or b'', error.stderr or b''
    (work / 'stdout').write_bytes(out)
    (work / 'stderr').write_bytes(err)
    return dict(args=args, exit_code=exit_code, expected_exit=code, marker=marker,
                seconds=round(time.monotonic() - started, 3),
                passed=exit_code == code and marker.encode() in out + err)


def deterministic_archive(root, archive):
    """gzip(tar) of the retained outputs with normalized ownership and a zero gzip mtime."""
    raw = io.BytesIO()
    with tarfile.open(fileobj=raw, mode='w', format=tarfile.PAX_FORMAT) as tar:
        members = []
        for name in ARCHIVED:
            path = root / name
            members.append(path)
            if path.is_dir():
                members.extend(sorted(path.rglob('*')))
        for path in members:
            info = tar.gettarinfo(str(path), arcname=str(Path(root.name) / path.relative_to(root)))
            info.uid = info.gid = 0
            info.uname = info.gname = ''
            if info.isfile():
                with path.open('rb') as stream:
                    tar.addfile(info, stream)
            else:
                tar.addfile(info)
    with archive.open('xb') as out, gzip.GzipFile(fileobj=out, mode='wb', mtime=0, filename='') as stream:
        stream.write(raw.getvalue())


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--repo', type=Path, required=True)
    parser.add_argument('--sha', required=True)
    parser.add_argument('--root', type=Path, required=True, help='new retained output directory, outside Git')
    parser.add_argument('--target', type=Path, required=True, help='persistent leased Cargo target lane')
    parser.add_argument('--lane-lock', type=Path, required=True, help="the lane's managed lease lock file")
    parser.add_argument('--jobs', type=int, default=3)
    parser.add_argument('--manifest', type=Path, required=True)
    args = parser.parse_args()
    if not re.fullmatch(r'[0-9a-f]{40}', args.sha):
        parser.error('a full source SHA is required')
    if not 1 <= args.jobs <= 4:
        parser.error('at most 4 build jobs')
    root, target = args.root.resolve(), args.target.resolve()
    root.mkdir(parents=True)  # refuse to reuse an earlier owner's outputs
    target.mkdir(parents=True, exist_ok=True)
    args.lane_lock.parent.mkdir(parents=True, exist_ok=True)
    lane = args.lane_lock.open('a+')
    fcntl.flock(lane, fcntl.LOCK_EX | fcntl.LOCK_NB)
    own = (target / '.qualification-tools.lock').open('a+')
    fcntl.flock(own, fcntl.LOCK_EX | fcntl.LOCK_NB)
    if git(args.repo, 'rev-parse', '--verify', args.sha + '^{commit}').strip() != args.sha:
        parser.error('source commit is not present')

    source, tar_sha = export_source(args.repo, args.sha, root)
    (root / 'source.tar.sha256').write_text(f'{tar_sha}  source.tar\n')
    release = tomllib.loads((source / 'Cargo.toml').read_text())['profile'][PROFILE]
    if release.get('lto') != 'fat' or release.get('codegen-units') != 1:
        parser.error('the release profile is not fat LTO with one codegen unit')
    if (source / 'rust-toolchain.toml').exists() and \
            tomllib.loads((source / 'rust-toolchain.toml').read_text())['toolchain']['channel'] != TOOLCHAIN:
        parser.error('source toolchain pin differs from the driver')

    env = {k: v for k, v in os.environ.items()
           if k not in ('CARGO_ENCODED_RUSTFLAGS', 'RUSTC_WRAPPER', 'RUSTC_WORKSPACE_WRAPPER', 'CARGO_BUILD_RUSTFLAGS')}
    env.update(CARGO_TARGET_DIR=str(target), CARGO_BUILD_JOBS=str(args.jobs), RUSTFLAGS=RUSTFLAGS,
               CARGO_INCREMENTAL='0', CARGO_TERM_COLOR='never')
    cargo_home = Path(env['CARGO_HOME']) if 'CARGO_HOME' in env else Path.home() / '.cargo'
    owner = dict(source_sha=args.sha, source_tree=git(args.repo, 'rev-parse', args.sha + '^{tree}').strip(),
                 source=str(source), source_tar_sha256=tar_sha, profile=PROFILE, toolchain=TOOLCHAIN,
                 rustflags=RUSTFLAGS, jobs=args.jobs, target=str(target), lane_lock=str(args.lane_lock),
                 cargo_home=str(cargo_home), root=str(root),
                 driver_sha256=sha256(__file__), pid=os.getpid(), started_at=time.time())
    (root / 'owner.json').write_text(json.dumps(owner, indent=2) + '\n')

    toolchain = dict(
        rustc=subprocess.check_output(['rustc', '+' + TOOLCHAIN, '-Vv'], cwd=source, env=env, text=True),
        cargo=subprocess.check_output(['cargo', '+' + TOOLCHAIN, '-V'], cwd=source, env=env, text=True).strip())
    inputs = {name: sha256(source / name) for name in INPUTS if (source / name).exists()}
    config = cargo_home / 'config.toml'
    if config.exists():
        inputs['$CARGO_HOME/config.toml'] = sha256(config)
    lock = tomllib.loads((source / 'Cargo.lock').read_text())['package']
    dependencies = dict(packages=len(lock), registry=sum('checksum' in p for p in lock),
                        git=sorted({p['source'] for p in lock if p.get('source', '').startswith('git+')}))

    logs = root / 'logs'
    logs.mkdir()
    stages, failure = [], None
    for name, command in STAGES:
        log = logs / f'{name}.log'
        started_at, started = time.time(), time.monotonic()
        with log.open('x') as stream:
            done = subprocess.run(command, cwd=source, env=env, stdout=stream, stderr=subprocess.STDOUT,
                                  pass_fds=(lane.fileno(), own.fileno()))
        text = log.read_text(errors='replace')
        stages.append(dict(name=name, command=command, cwd='<source>', started_at=started_at,
                           seconds=round(time.monotonic() - started, 3), exit_code=done.returncode,
                           log=str(log), log_sha256=sha256(log),
                           rustc_invocations=text.count('Running `'),
                           native_cpu_flag_seen='target-cpu=native' in text,
                           requested_flags_seen='target-cpu=x86-64-v3' in text and '+pclmulqdq' in text))
        if done.returncode:
            failure = done.returncode
            break

    artifacts, sanity = {}, root / 'sanity'
    if failure is None:
        for name in FILTER_BINS + TOOL_BINS + EXAMPLES:
            built = target / PROFILE / name
            retained = root / 'artifacts' / name
            retained.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(built, retained)
            digest = sha256(built)
            if sha256(retained) != digest:
                raise RuntimeError('retained artifact checksum mismatch: ' + name)
            artifacts[name] = dict(built_path=str(built), retained_path=str(retained), sha256=digest,
                                   **inspect(retained), probe=probe(name, retained, sanity))
    source_files = verify_source(args.repo, args.sha, source)
    ci_built = {name: (target / PROFILE / name).exists() for name in CI_BUILT}
    passed = (failure is None and all(a['linux_x86_64'] and a['probe']['passed'] for a in artifacts.values())
              and not any(ci_built.values()) and not any(s['native_cpu_flag_seen'] for s in stages)
              and all(s['requested_flags_seen'] for s in stages))
    result = dict(**owner, ended_at=time.time(), status='passed' if passed else 'failed',
                  build_exit=failure or 0, toolchain_versions=toolchain, input_sha256=inputs,
                  dependencies=dependencies, source_files_verified_after_build=source_files,
                  ci_built_roles_present_in_lane=ci_built, stages=stages, artifacts=artifacts)
    (root / 'result.json').write_text(json.dumps(result, indent=2) + '\n')

    archive = root.parent / (root.name + '.tar.gz')
    deterministic_archive(root, archive)
    archive_sha = sha256(archive)
    (root.parent / (archive.name + '.sha256')).write_text(f'{archive_sha}  {archive.name}\n')
    host = dict(platform=platform.platform(), cpu=next((l.split(':', 1)[1].strip() for l in
                Path('/proc/cpuinfo').read_text().splitlines() if l.startswith('model name')), None),
                logical_cpus=os.cpu_count())
    for key, name in (('cgroup_memory_max', 'memory.max'), ('cgroup_cpu_max', 'cpu.max'),
                      ('cgroup_memory_peak', 'memory.peak')):
        group = Path('/sys/fs/cgroup') / Path('/proc/self/cgroup').read_text().split('::', 1)[1].strip().lstrip('/')
        host[key] = (group / name).read_text().strip() if (group / name).exists() else None
    manifest = {k: v for k, v in result.items() if k not in ('pid', 'lane_lock')}
    manifest.update(context=CONTEXT, archive=dict(path=str(archive), sha256=archive_sha, members=ARCHIVED), host=host)
    args.manifest.parent.mkdir(parents=True, exist_ok=True)
    args.manifest.write_text(json.dumps(manifest, indent=2) + '\n')
    return 0 if passed else (failure or 1)


if __name__ == '__main__':
    raise SystemExit(main())
