#!/usr/bin/env python3
"""Cargo cache identity, restore status and artifact reuse for CI jobs.

The identity names every input that makes Cargo output incompatible: the actual
compiler, host OS/ABI and target, lane, package scope, profile and features,
compile flags, Cargo configuration and the locked dependency graph. Checkout
SHA, workflow text and runtime-only variables such as RUST_TEST_THREADS are
excluded. Inside a compatible cache Cargo's fingerprints still decide which
units are fresh, so a restored cache can only save work, never skip a rebuild.

Persistent self-hosted targets are partitioned by lane and toolchain identity
only; Cargo tracks dependency, feature and profile changes inside them. Hosted
caches use the full identity. GitHub scopes PR saves to the merge ref; PRs may
restore main entries, and main never reads PR entries.

Records are sanitized: names, versions and digests, never environment values,
tokens or absolute paths.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess
import sys
import time
import urllib.parse
import urllib.request

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'shared/dev'))
from target_lease import build_flags  # noqa: E402

SCHEMA = 1
PREFIX = 'v1-wallet-pir'
# What each lane/scope compiles. Kept here rather than read from workflow text;
# unknown pairs fail so that a new job chooses its partition explicitly.
SCOPES = {
    ('fast', 'affected'): ('release-fast', 'selected packages with classified features'),
    ('full-lint', 'transparent'): ('dev', 'clippy all-targets all-features'),
    ('full-lint', 'enhance'): ('dev', 'clippy all-targets all-features'),
    ('full-test', 'transparent'): ('release-fast', 'transparent-filter/cli'),
    ('full-test', 'enhance'): ('release-fast', 'enhance-pir/cli; native-reinspiring; cuda checks'),
    ('full-test', 'shared'): ('dev,release-fast', 'clippy all-features; default'),
    ('release', 'q48'): ('release', 'enhance-pir/cli'),
    ('release-native', 'native'): ('release', 'enhance-pir/cli,native-reinspiring'),
    ('native-cuda', 'cuda'): ('release', 'enhance-pir/cli,native-reinspiring,cuda'),
}
CONFIG_FILES = ['.cargo/config', '.cargo/config.toml', 'rust-toolchain', 'rust-toolchain.toml']
COMPILATION = ('compile', 'lint')
EXECUTION = ('tests', 'doctests', 'integration', 'reclaim', 'format', 'helpers', 'artifact')


def digest(value, length=None):
    data = value if isinstance(value, bytes) else json.dumps(value, sort_keys=True).encode()
    return hashlib.sha256(data).hexdigest()[:length]


def first_line(text):
    return text.strip().splitlines()[0] if text and text.strip() else None


def probe(root=ROOT, env=None):
    """Versions of the tools that actually build this checkout."""
    env = dict(os.environ if env is None else env)

    def output(*command):
        try:
            result = subprocess.run(command, cwd=root, env=env, capture_output=True, text=True, timeout=120)
        except (FileNotFoundError, subprocess.TimeoutExpired):
            return None
        return result.stdout if result.returncode == 0 else None

    release = {}
    try:
        for line in Path('/etc/os-release').read_text().splitlines():
            key, _, value = line.partition('=')
            release[key] = value.strip('"')
    except OSError:
        pass
    try:
        libc = os.confstr('CS_GNU_LIBC_VERSION')
    except (AttributeError, ValueError, OSError):
        libc = None
    return {
        'rustc': output('rustc', '-vV'),
        'cargo': first_line(output('cargo', '-V')),
        'os': f"{release.get('ID', platform.system())} {release.get('VERSION_ID', '')}".strip(),
        'libc': libc,
        'machine': platform.machine(),
        'native': {name: first_line(output(*command)) for name, command in {
            'cc': [env.get('CC', 'cc'), '--version'], 'cxx': [env.get('CXX', 'c++'), '--version'],
            'clang': ['clang', '--version'], 'protoc': ['protoc', '--version']}.items()},
    }


def compiler(rustc):
    fields = dict(line.split(': ', 1) for line in (rustc or '').splitlines() if ': ' in line)
    return {key: fields.get(key) for key in ('release', 'commit-hash', 'host', 'LLVM version')}


def identity(lane, scope, tools, env, root=ROOT):
    """Return (toolchain components, full components) for a lane/scope.

    `tools` comes from probe(); tests substitute fixed values.
    """
    if (lane, scope) not in SCOPES:
        raise ValueError(f'unclassified Cargo cache lane/scope: {lane}/{scope}')
    if not tools.get('rustc'):
        raise ValueError('actual compiler unavailable: rustc -vV failed')
    profile, features = SCOPES[lane, scope]
    rust = compiler(tools['rustc'])
    home = Path(env.get('CARGO_HOME') or Path(env.get('HOME', '~')).expanduser() / '.cargo')
    configs = {name: digest((root / name).read_bytes()) for name in CONFIG_FILES if (root / name).is_file()}
    configs.update({'CARGO_HOME/' + name: digest((home / name).read_bytes())
                    for name in ('config', 'config.toml') if (home / name).is_file()})
    toolchain = {
        'schema': SCHEMA,
        'compiler': tools['rustc'].strip(),
        'cargo': tools.get('cargo'),
        'target': env.get('CARGO_BUILD_TARGET') or rust['host'],
        'os': tools.get('os'), 'libc': tools.get('libc'), 'machine': tools.get('machine'),
        'native': tools.get('native', {}),
        'flags': build_flags(env),
        'cargo_config': configs,
    }
    lock = root / 'Cargo.lock'
    full = {**toolchain, 'lane': lane, 'scope': scope, 'profile': profile, 'features': features,
            'lock': digest(lock.read_bytes()) if lock.is_file() else None}
    return toolchain, full


def sanitized(toolchain, full):
    """Names, versions and digests only; flag values are hashed."""
    rust = compiler(toolchain['compiler'])
    return {
        'identity': digest(full, 16), 'toolchain': digest(toolchain, 16),
        'lane': full['lane'], 'scope': full['scope'], 'profile': full['profile'], 'features': full['features'],
        'rustc': rust, 'cargo': toolchain['cargo'], 'target': toolchain['target'],
        'os': toolchain['os'], 'libc': toolchain['libc'], 'machine': toolchain['machine'],
        'native': toolchain['native'],
        'flags': {key: digest(value.encode(), 12) for key, value in sorted(toolchain['flags'].items())},
        'cargo_config': {key: value[:12] for key, value in sorted(toolchain['cargo_config'].items())},
        'lock': full['lock'][:12] if full['lock'] else None,
    }


def hosted_key(record):
    """The `shared-key` passed to rust-cache, which appends only OS/arch."""
    return f"{record['lane']}-{record['scope']}-{record['identity']}"


def api(path, env):
    token = env.get('GH_TOKEN') or env.get('GITHUB_TOKEN')
    if not token:
        return None
    request = urllib.request.Request(
        env.get('GITHUB_API_URL', 'https://api.github.com') + '/' + path,
        headers={'Authorization': 'Bearer ' + token, 'Accept': 'application/vnd.github+json',
                 'X-GitHub-Api-Version': '2022-11-28'})
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            return json.load(response)
    except Exception:
        return None


def cache_refs(key, env):
    """Refs holding this exact key before restore; None when it cannot be read."""
    repo = env.get('GITHUB_REPOSITORY')
    if not repo:
        return None
    data = api(f'repos/{repo}/actions/caches?per_page=100&key=' + urllib.parse.quote(key), env)
    if data is None:
        return None
    return sorted({entry['ref'] for entry in data.get('actions_caches', []) if entry.get('key') == key})


def restore_status(mode, cache_hit, refs, ref, default_ref='refs/heads/main', existed=None):
    """Which state this job starts from. Never inferred from timing."""
    if mode == 'persistent':
        return 'persistent-existing' if existed else 'persistent-new'
    if cache_hit != 'true':
        return 'miss'
    if mode == 'lookup':
        return 'primed'
    if refs is None:
        return 'hit-unknown-ref'
    # Actions searches the current ref before the default branch.
    if ref in refs:
        return 'hit-current-ref'
    if default_ref in refs:
        return 'hit-main'
    return 'hit-unknown-ref'


def snapshot(target):
    """Latest file mtime for every Cargo unit fingerprint in a target directory."""
    target = Path(target)
    units = {}
    for directory in [*target.glob('*/.fingerprint/*'), *target.glob('*/*/.fingerprint/*')]:
        try:
            if not directory.is_dir():
                continue
            times = [entry.stat().st_mtime_ns for entry in directory.iterdir() if entry.is_file()]
        except OSError:
            continue
        units[directory.relative_to(target).as_posix()] = max(times, default=0)
    return units


def unit_name(path):
    name = path.rsplit('/', 1)[-1]
    base, _, suffix = name.rpartition('-')
    return base if base and len(suffix) == 16 else name


def classify(before, after, workspace):
    """Compare fingerprints at restore and job end.

    `reused` means present at restore and not rewritten: Cargo found it fresh or
    this job did not need it. `rebuilt` units were restored but stale.
    """
    workspace = {name.replace('_', '-') for name in workspace}
    counts = {'restored': len(before), 'reused': 0, 'rebuilt': 0, 'new': 0}
    third_party = {'rebuilt': 0, 'new': 0}
    names = {'rebuilt': set(), 'new': set()}
    for path, mtime in after.items():
        kind = 'new' if path not in before else 'rebuilt' if mtime != before[path] else 'reused'
        counts[kind] += 1
        if kind != 'reused':
            name = unit_name(path)
            names[kind].add(name)
            if name.replace('_', '-') not in workspace:
                third_party[kind] += 1
    counts['missing_at_end'] = len(set(before) - set(after))
    return {'units': counts, 'third_party': third_party,
            'workspace_names': {kind: sorted(n for n in values if n.replace('_', '-') in workspace)
                                for kind, values in names.items()}}


def phases(records):
    """Leaf stage durations by class; nested wrappers are not double counted."""
    parents = {record.get('parent') for record in records}
    result = {'compilation_seconds': 0.0, 'execution_seconds': 0.0, 'other_seconds': 0.0, 'failed_stages': []}
    for record in records:
        if record.get('id') in parents:
            continue
        stage = record['stage']
        kind = ('compilation_seconds' if stage.startswith(COMPILATION) else
                'execution_seconds' if stage.startswith(EXECUTION) else 'other_seconds')
        result[kind] = round(result[kind] + record['seconds'], 3)
        if record.get('exit'):
            result['failed_stages'].append(stage)
    return result


def state_dir(env):
    return Path(env.get('RUNNER_TEMP') or '/tmp') / 'wallet-pir-cargo'


def write_env(env, **values):
    path = env.get('GITHUB_ENV')
    if path:
        with open(path, 'a') as out:
            out.writelines(f'{key}={value}\n' for key, value in values.items())
    os.environ.update(values)


def prepare(args, env):
    started = float(env.get('WALLET_PIR_SETUP_STARTED') or time.time())
    toolchain, full = identity(args.lane, args.scope, probe(ROOT, env), env)
    record = sanitized(toolchain, full)
    if args.persistent_root:
        target = Path(args.persistent_root) / args.lane / record['toolchain']
        existed = target.is_dir()
        target.mkdir(parents=True, exist_ok=True)
        mode, refs, key = 'persistent', None, None
        write_env(env, WALLET_PIR_BUILD_CACHE_ROOT=args.persistent_root)
    else:
        target = Path(args.target_dir).resolve()
        existed = None
        mode = 'lookup' if args.lookup_only else 'restore'
        key = hosted_key(record)
        # rust-cache appends the runner OS and Node architecture to `shared-key`.
        refs = cache_refs(f'{PREFIX}-{key}-Linux-x64', env)
    directory = state_dir(env)
    directory.mkdir(parents=True, exist_ok=True)
    stages = directory / 'stages.jsonl'
    write_env(env, CARGO_TARGET_DIR=str(target), WALLET_PIR_STAGE_LOG=str(stages))
    state = {'record': record, 'mode': mode, 'key': key, 'refs_before_restore': refs, 'ref': env.get('GITHUB_REF'),
             'target': str(target), 'existed': existed, 'setup_started': started, 'prepared': time.time()}
    (directory / f'{args.lane}.json').write_text(json.dumps(state))
    print('CI_CACHE_IDENTITY ' + json.dumps({'key': key, 'mode': mode, **record}, sort_keys=True), flush=True)
    if env.get('GITHUB_OUTPUT') and key:
        with open(env['GITHUB_OUTPUT'], 'a') as out:
            out.write(f'shared-key={key}\n')


def restored(args, env):
    path = state_dir(env) / f'{args.lane}.json'
    state = json.loads(path.read_text())
    state['restored'] = time.time()
    state['status'] = restore_status(state['mode'], args.cache_hit, state['refs_before_restore'],
                                     state['ref'], existed=state['existed'])
    state['before'] = snapshot(state['target'])
    path.write_text(json.dumps(state))
    print(f"CI_CACHE_RESTORE {json.dumps({'lane': args.lane, 'status': state['status'], 'restored_units': len(state['before'])})}", flush=True)


def workspace_names():
    try:
        sys.path.insert(0, str(ROOT / 'tools/ci'))
        from fast import workspace_packages
        return {package['name'] for package in workspace_packages()}
    except Exception:
        return set()


def report(env):
    directory = state_dir(env)
    lanes = sorted(directory.glob('*.json')) if directory.is_dir() else []
    stages = []
    log = directory / 'stages.jsonl'
    if log.is_file():
        stages = [json.loads(line) for line in log.read_text().splitlines() if line.strip()]
    workspace = workspace_names()
    rows = []
    for path in lanes:
        state = json.loads(path.read_text())
        restored_at = state.get('restored', state['prepared'])
        result = {
            **{key: state['record'][key] for key in ('lane', 'scope', 'identity', 'toolchain', 'profile')},
            'key': state['key'], 'restore': state.get('status', 'not-recorded'),
            'refs_before_restore': state['refs_before_restore'],
            'setup_seconds': round(state['prepared'] - state['setup_started'], 3),
            'restore_seconds': round(restored_at - state['prepared'], 3),
            **classify(state.get('before', {}), snapshot(state['target']), workspace),
        }
        rows.append(result)
        print('CI_CACHE_REPORT ' + json.dumps(result, sort_keys=True), flush=True)
    summary = {'stages': phases(stages)}
    print('CI_STAGE_REPORT ' + json.dumps(summary, sort_keys=True), flush=True)
    if env.get('GITHUB_STEP_SUMMARY'):
        with open(env['GITHUB_STEP_SUMMARY'], 'a') as out:
            out.write('\n## Cargo cache reuse\n\n| Lane | Scope | Restore | Setup | Restore time | Restored | Reused | Rebuilt | New | Third-party rebuilt/new |\n|---|---|---|---|---|---|---|---|---|---|\n')
            for row in rows:
                units = row['units']
                out.write(f"| {row['lane']} | {row['scope']} | {row['restore']} | {row['setup_seconds']:.1f}s | {row['restore_seconds']:.1f}s"
                          f" | {units['restored']} | {units['reused']} | {units['rebuilt']} | {units['new']}"
                          f" | {row['third_party']['rebuilt']}/{row['third_party']['new']} |\n")
            stage = summary['stages']
            out.write(f"\nLeaf stages: compilation {stage['compilation_seconds']:.1f}s, execution {stage['execution_seconds']:.1f}s, "
                      f"other {stage['other_seconds']:.1f}s. Reused units were present at restore and not rewritten.\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    prep = commands.add_parser('prepare')
    prep.add_argument('--lane', required=True)
    prep.add_argument('--scope', required=True)
    where = prep.add_mutually_exclusive_group(required=True)
    where.add_argument('--persistent-root')
    where.add_argument('--target-dir')
    prep.add_argument('--lookup-only', action='store_true')
    done = commands.add_parser('restored')
    done.add_argument('--lane', required=True)
    done.add_argument('--cache-hit', default='')
    commands.add_parser('report')
    args = parser.parse_args()
    env = dict(os.environ)
    if args.command == 'prepare':
        prepare(args, env)
    elif args.command == 'restored':
        restored(args, env)
    else:
        report(env)


if __name__ == '__main__':
    main()
