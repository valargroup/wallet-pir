#!/usr/bin/env python3
"""Cargo cache identity, restore status and artifact reuse for CI jobs.

The identity names every input that makes Cargo output incompatible: the actual
compiler, host OS/ABI and target, lane, package scope, profile and features,
compile flags, every Cargo configuration file Cargo reads (checkout and ancestor
directories, CARGO_HOME and their includes), the workspace profile definitions,
any compiler wrapper and the locked dependency graph. Checkout
SHA, workflow text and runtime-only variables such as RUST_TEST_THREADS are
excluded. Inside a compatible cache Cargo's fingerprints still decide which
units are fresh, so a restored cache can only save work, never skip a rebuild.

Persistent self-hosted targets are partitioned by lane and toolchain identity
only; Cargo tracks dependency, feature and profile changes inside them. Hosted
caches use the full identity. GitHub scopes PR saves to the merge ref; PRs may
restore main entries, and main never reads PR entries.

Unit attribution comes from Cargo itself: in CI, stage.py adds
--message-format=json to each Cargo build/check/clippy/test command it runs and
logs only package name, version, source kind and the `fresh` flag of every
compiler-artifact message. The fingerprint inventory (restored and remaining
unit directories compared by content) is reported separately; it is not
attribution, because restored units a job never needed stay unchanged.

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

try:
    import tomllib
except ImportError:  # Python < 3.11 (Ubuntu 22.04 CUDA container)
    tomllib = None

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
TOOLCHAIN_FILES = ['rust-toolchain', 'rust-toolchain.toml']
CONFIG_NAMES = ('config', 'config.toml')
# Programs Cargo runs to compile, from the environment or `[build]` configuration.
TOOLS = {'rustc': ('RUSTC', 'CARGO_BUILD_RUSTC'),
         'rustc-wrapper': ('RUSTC_WRAPPER', 'CARGO_BUILD_RUSTC_WRAPPER'),
         'rustc-workspace-wrapper': ('RUSTC_WORKSPACE_WRAPPER', 'CARGO_BUILD_RUSTC_WORKSPACE_WRAPPER')}
# Leaf stage classes. A compile/lint stage is the wall time of a Cargo command
# that only builds, including dependency resolution, downloads, build scripts and
# linking. A test stage runs tests and may also build units no earlier compile
# stage needed; the artifact records say which.
COMPILATION = ('compile', 'lint')
EXECUTION = ('tests', 'doctests', 'integration', 'reclaim', 'format', 'helpers', 'artifact')


def digest(value, length=None):
    data = value if isinstance(value, bytes) else json.dumps(value, sort_keys=True).encode()
    return hashlib.sha256(data).hexdigest()[:length]


def first_line(text):
    return text.strip().splitlines()[0] if text and text.strip() else None


def load_toml(path):
    """Parsed TOML, or None. Without tomllib callers fall back to whole-file
    digests: Cargo.toml as a whole, configs without following `include` or
    reading [build] tool settings (environment overrides still apply)."""
    if tomllib is None:
        return None
    try:
        return tomllib.loads(path.read_text())
    except (OSError, UnicodeDecodeError, ValueError):
        return None


def cargo_configs(root, env):
    """Return ([label, digest] for every config file Cargo reads, parsed configs).

    Cargo merges .cargo/config(.toml) from the working directory and every
    ancestor, then CARGO_HOME, plus files named by `include`, in that order.
    Labels are relative (`../` per ancestor level) so records hold no absolute
    paths. Parsed configs are listed highest precedence first.
    """
    home = Path(env.get('CARGO_HOME') or Path(env.get('HOME', '~')).expanduser() / '.cargo')
    candidates = [('../' * depth + '.cargo/' + name, directory / '.cargo' / name)
                  for depth, directory in enumerate((root, *root.parents)) for name in CONFIG_NAMES]
    candidates += [('CARGO_HOME/' + name, home / name) for name in CONFIG_NAMES]
    seen, files, parsed = set(), [], []

    def add(label, path, depth=0):
        try:
            resolved = path.resolve()
            data = path.read_bytes()
        except OSError:
            files.append([label, 'unreadable'])
            return
        if resolved in seen or depth > 8:
            return
        seen.add(resolved)
        files.append([label, digest(data)])
        config = load_toml(path)
        if config is None:
            return
        parsed.append(config)
        includes = config.get('include', [])
        for index, entry in enumerate([includes] if isinstance(includes, (str, dict)) else includes):
            name = entry.get('path') if isinstance(entry, dict) else entry
            if isinstance(name, str):
                add(f'{label} include {index}', path.parent / name, depth + 1)

    for label, path in candidates:
        if path.is_file():
            add(label, path)
    return files, parsed


def tool_programs(configs, env):
    """Compiler and wrappers Cargo will run; the environment overrides configuration."""
    programs = {}
    for name, variables in TOOLS.items():
        value = next((env[key] for key in variables if env.get(key)), None)
        for config in configs:
            if value is None and isinstance(config.get('build'), dict):
                value = config['build'].get(name)
        programs[name] = value if isinstance(value, str) and value else ('rustc' if name == 'rustc' else None)
    return programs


def profiles(root):
    """Digest of the workspace [profile.*] definitions (only the root manifest's apply)."""
    manifest = load_toml(root / 'Cargo.toml')
    if manifest is None:
        path = root / 'Cargo.toml'
        return digest(path.read_bytes() if path.is_file() else b'')
    return digest(manifest.get('profile', {}))


def probe(root=ROOT, env=None):
    """Versions of the tools that actually build this checkout."""
    env = dict(os.environ if env is None else env)
    programs = tool_programs(cargo_configs(root, env)[1], env)

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
        'rustc': output(programs['rustc'], '-vV'),
        # A wrapper is identified by its own version output; its path is not recorded.
        'wrappers': {name: first_line(output(programs[name], '--version')) or 'unversioned'
                     for name in ('rustc-wrapper', 'rustc-workspace-wrapper') if programs[name]},
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
    toolchain = {
        'schema': SCHEMA,
        'compiler': tools['rustc'].strip(),
        'cargo': tools.get('cargo'),
        'target': env.get('CARGO_BUILD_TARGET') or rust['host'],
        'os': tools.get('os'), 'libc': tools.get('libc'), 'machine': tools.get('machine'),
        'native': tools.get('native', {}),
        'wrappers': tools.get('wrappers', {}),
        'flags': build_flags(env),
        'toolchain_files': {name: digest((root / name).read_bytes())
                            for name in TOOLCHAIN_FILES if (root / name).is_file()},
        # Ordered: Cargo's merge precedence depends on position.
        'cargo_config': cargo_configs(root, env)[0],
    }
    lock = root / 'Cargo.lock'
    # Profile definitions split hosted entries. Persistent targets rely on
    # Cargo's fingerprints, which include each unit's resolved profile.
    full = {**toolchain, 'lane': lane, 'scope': scope, 'profile': profile, 'features': features,
            'profiles': profiles(root), 'lock': digest(lock.read_bytes()) if lock.is_file() else None}
    return toolchain, full


def sanitized(toolchain, full):
    """Names, versions and digests only; flag values are hashed."""
    rust = compiler(toolchain['compiler'])
    return {
        'identity': digest(full, 16), 'toolchain': digest(toolchain, 16),
        'lane': full['lane'], 'scope': full['scope'], 'profile': full['profile'], 'features': full['features'],
        'rustc': rust, 'cargo': toolchain['cargo'], 'target': toolchain['target'],
        'os': toolchain['os'], 'libc': toolchain['libc'], 'machine': toolchain['machine'],
        'native': toolchain['native'], 'wrappers': toolchain['wrappers'],
        'flags': {key: digest(value.encode(), 12) for key, value in sorted(toolchain['flags'].items())},
        'toolchain_files': {key: value[:12] for key, value in sorted(toolchain['toolchain_files'].items())},
        'cargo_config': [[label, value[:12]] for label, value in toolchain['cargo_config']],
        'profiles': full['profiles'][:12],
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
    """Content digest of every Cargo unit fingerprint directory in a target."""
    target = Path(target)
    units = {}
    for directory in [*target.glob('*/.fingerprint/*'), *target.glob('*/*/.fingerprint/*')]:
        try:
            if not directory.is_dir():
                continue
            content = hashlib.sha256()
            for entry in sorted(directory.iterdir()):
                if entry.is_file():
                    content.update(entry.name.encode() + b'\0' + entry.read_bytes() + b'\0')
        except OSError:
            continue
        units[directory.relative_to(target).as_posix()] = content.hexdigest()[:16]
    return units


def inventory(before, after):
    """Fingerprint directories at restore and job end, compared by content.

    This is an inventory, not attribution: a unit Cargo rebuilt to identical
    fingerprints counts as unchanged, and restored units the job never needed
    are unchanged too. Use artifacts() for what Cargo actually compiled.
    """
    return {'restored': len(before), 'present_at_end': len(after),
            'unchanged': sum(1 for path, value in after.items() if before.get(path) == value),
            'changed': sum(1 for path, value in after.items() if path in before and before[path] != value),
            'added': sum(1 for path in after if path not in before),
            'missing_at_end': len(set(before) - set(after))}


def artifact_unit(message):
    """Sanitized identity of one compiler-artifact message, or None."""
    if message.get('reason') != 'compiler-artifact':
        return None
    package = message.get('package_id', '')
    source, _, fragment = package.partition('+')
    location, _, version = package.rpartition('#')
    if '@' in version:
        name, _, version = version.partition('@')
    else:
        # path+file:///.../name#version form: the name is the last path segment.
        name = location.rstrip('/').rsplit('/', 1)[-1]
    target = message.get('target') or {}
    profile = message.get('profile') or {}
    return {'package': name, 'version': version, 'source': source if fragment else 'unknown',
            'target': target.get('name'), 'kind': sorted(target.get('kind') or []),
            'unit': digest([package, target.get('name'), target.get('kind'), profile,
                            sorted(message.get('features') or [])], 16),
            'fresh': bool(message.get('fresh'))}


def artifacts(records):
    """Units Cargo reported in this job's executed commands.

    A unit is `compiled` if any command reported it not fresh, otherwise `fresh`
    (Cargo reused it). Units no command needed do not appear. Third party means
    a registry or git source; workspace means a path source.
    """
    units = {}
    for record in records:
        unit = units.setdefault(record['unit'], dict(record, fresh=True))
        unit['fresh'] = unit['fresh'] and record['fresh']
    result = {'commands': len({record.get('command') for record in records}) if records else 0,
              'units': len(units), 'fresh': 0, 'compiled': 0,
              'third_party': {'fresh': 0, 'compiled': 0}, 'workspace': {'fresh': 0, 'compiled': 0},
              'compiled_workspace_packages': set()}
    for unit in units.values():
        kind = 'fresh' if unit['fresh'] else 'compiled'
        result[kind] += 1
        group = 'workspace' if unit['source'] == 'path' else 'third_party'
        result[group][kind] += 1
        if group == 'workspace' and kind == 'compiled':
            result['compiled_workspace_packages'].add(unit['package'])
    result['compiled_workspace_packages'] = sorted(result['compiled_workspace_packages'])
    return result


def phases(records):
    """Leaf stage wall time by class; nested wrappers are not double counted.

    compile_stage_seconds covers whole compile/lint Cargo commands, including
    dependency resolution, downloads, build scripts and linking, not only rustc.
    A class with no recorded stage is null (unknown), not zero.
    """
    parents = {record.get('parent') for record in records}
    result = {'compile_stage_seconds': None, 'execution_stage_seconds': None, 'other_stage_seconds': None,
              'failed_stages': []}
    for record in records:
        if record.get('id') in parents:
            continue
        stage = record['stage']
        kind = ('compile_stage_seconds' if stage.startswith(COMPILATION) else
                'execution_stage_seconds' if stage.startswith(EXECUTION) else 'other_stage_seconds')
        result[kind] = round((result[kind] or 0.0) + record['seconds'], 3)
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
    write_env(env, CARGO_TARGET_DIR=str(target), WALLET_PIR_STAGE_LOG=str(stages),
              WALLET_PIR_ARTIFACT_LOG=str(directory / 'artifacts.jsonl'))
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


def read_jsonl(path):
    if not path.is_file():
        return []
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def seconds(value):
    return round(value, 3) if value is not None else None


def report(env):
    directory = state_dir(env)
    lanes = sorted(directory.glob('*.json')) if directory.is_dir() else []
    rows = []
    for path in lanes:
        state = json.loads(path.read_text())
        before = state.get('before')
        result = {
            **{key: state['record'][key] for key in ('lane', 'scope', 'identity', 'toolchain', 'profile')},
            'key': state['key'], 'restore': state.get('status', 'not-recorded'),
            'refs_before_restore': state['refs_before_restore'],
            'setup_seconds': seconds(state['prepared'] - state['setup_started']),
            'restore_seconds': seconds(state['restored'] - state['prepared']) if 'restored' in state else None,
            'fingerprint_inventory': inventory(before, snapshot(state['target'])) if before is not None else None,
        }
        rows.append(result)
        print('CI_CACHE_REPORT ' + json.dumps(result, sort_keys=True), flush=True)
    built = artifacts(read_jsonl(directory / 'artifacts.jsonl'))
    print('CI_CARGO_ARTIFACTS ' + json.dumps(built, sort_keys=True), flush=True)
    summary = {'stages': phases(read_jsonl(directory / 'stages.jsonl'))}
    print('CI_STAGE_REPORT ' + json.dumps(summary, sort_keys=True), flush=True)
    if env.get('GITHUB_STEP_SUMMARY'):
        def text(value):
            return 'unknown' if value is None else f'{value:.1f}s'
        with open(env['GITHUB_STEP_SUMMARY'], 'a') as out:
            out.write('\n## Cargo cache reuse\n\n| Lane | Scope | Restore | Setup | Restore time |\n|---|---|---|---|---|\n')
            for row in rows:
                out.write(f"| {row['lane']} | {row['scope']} | {row['restore']} | {text(row['setup_seconds'])}"
                          f" | {text(row['restore_seconds'])} |\n")
            out.write(f"\nCargo artifacts in {built['commands']} commands: {built['units']} units, "
                      f"{built['fresh']} fresh, {built['compiled']} compiled (third-party "
                      f"{built['third_party']['fresh']} fresh / {built['third_party']['compiled']} compiled).\n")
            stage = summary['stages']
            out.write(f"\nLeaf stages: compile {text(stage['compile_stage_seconds'])} (whole Cargo commands), "
                      f"execution {text(stage['execution_stage_seconds'])}, other {text(stage['other_stage_seconds'])}.\n")


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
