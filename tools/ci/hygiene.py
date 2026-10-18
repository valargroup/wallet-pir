#!/usr/bin/env python3
"""Reject captured run output; report change size without imposing a line budget.

Read paths and blobs from the selected Git revision, or include local edits and
untracked files for developer checks. Only the optional Actions job summary is written; no network or build
tool is invoked. The policy declares bounded locations for useful data files.
"""
import argparse
from collections import Counter
from fnmatch import fnmatchcase
import hashlib
from pathlib import Path, PurePosixPath
import os
import posixpath
import re
import subprocess
import sys
import tomllib

ROOT = Path(__file__).resolve().parents[2]
POLICY = 'tools/ci/hygiene.toml'
EVIDENCE = ('evidence/', 'evidence/enhance/', 'evidence/transparent/', 'evidence/receiver/')
STRUCTURED = {'.json', '.jsonl', '.ndjson', '.csv', '.tsv'}
CAPTURED = {'.log', '.stdout', '.stderr', '.err', '.time', '.nohup', '.gz', '.tgz', '.xz', '.zip'}
SCRIPTS = {'.py', '.sh', '.rs', '.dart', '.go', '.c', '.proto'}
REPORT = re.compile(r'^(?:self[-_]review|task[-_]report|handoff[-_]report|agent[-_]report|transcript)(?:[-_.]|$)', re.I)


def git(root, *args):
    return subprocess.check_output(['git', *args], cwd=root, stderr=subprocess.PIPE)


def names(root, *args):
    return [p.decode() for p in git(root, *args).split(b'\0') if p]


def blob(root, revision, path):
    result = subprocess.run(['git', 'show', f'{revision}:{path}'], cwd=root, capture_output=True)
    return result.stdout if result.returncode == 0 else None


def read_policy(root, revision=None):
    source = blob(root, revision, POLICY) if revision else (root / POLICY).read_bytes()
    if source is None:
        raise ValueError(f'{revision} has no hygiene policy; update the branch onto main')
    policy = tomllib.loads(source.decode())
    if set(policy) != {'warnings', 'allow_data'} or set(policy['warnings']) != {'added_lines', 'files', 'large_bytes'}:
        raise ValueError('policy requires warnings and allow_data only')
    if any(type(n) is not int or n <= 0 for n in policy['warnings'].values()):
        raise ValueError('warning thresholds must be positive integers')
    for entry in policy['allow_data']:
        if set(entry) != {'pattern', 'reason'} or not isinstance(entry['reason'], str) or not entry['reason'].strip():
            raise ValueError('each data allowance needs a pattern and a reason')
        pattern = entry['pattern']
        if (not isinstance(pattern, str) or pattern.startswith(('/', '*')) or '..' in pattern.split('/')
                or not PurePosixPath(pattern).suffix or ('*' in pattern and len(pattern.split('/')) < 3)):
            raise ValueError('data allowances must name a bounded relative location and extension')
    return policy


def violation(path, data, policy):
    """Classify concrete artifacts, never the author or perceived style of prose."""
    p = PurePosixPath(path)
    evidence = path.startswith(EVIDENCE)
    if REPORT.match(p.name):
        return 'agent report: put findings in chat or the PR description'
    if any(part in {'__pycache__', 'target', 'node_modules', '.local-pir'} for part in p.parts) or p.suffix == '.pyc':
        return 'generated output: keep this file outside Git'
    allowed = any(fnmatchcase(path, entry['pattern']) for entry in policy['allow_data'])
    # A declared independent oracle generator may stay beside its frozen output.
    if evidence and p.suffix in SCRIPTS and not allowed:
        return 'experiment script: promote reusable code into a maintained tool with tests'
    if p.suffix in CAPTURED and not allowed:
        return 'captured output: use .local-pir/ or CI artifacts'
    if p.suffix in STRUCTURED and not allowed:
        return 'run data: declare a bounded configuration, fixture, or essential-proof location'
    if evidence and p.suffix != '.md' and p.name != 'SHA256SUMS' and not allowed:
        return 'evidence: retain a concise note or declare an essential proof'
    if b'\0' in data[:8192] and not allowed:
        return 'binary output: declare an essential fixture or keep outside Git'
    return None


def size_warnings(added, files, large, thresholds):
    messages = []
    if added > thresholds['added_lines'] or files > thresholds['files']:
        messages.append(f'{added:,} added text lines in {files} added/modified files; split independent outcomes')
    messages.extend(f'{path}: {size:,} bytes; retain only the necessary input or proof' for path, size in large)
    return messages


def inspect(root=ROOT, *, base='', head='HEAD', local=True, all_files=False):
    policy = read_policy(root, None if local else head)
    # Reuse the affected-check planner's inclusion of staged, unstaged and
    # untracked paths. Importing it reads no Cargo metadata and runs no tools.
    from fast import changed_paths
    paths = ((names(root, 'ls-files', '--cached', '--others', '--exclude-standard', '-z') if local else
              names(root, 'ls-tree', '-r', '--name-only', '-z', head)) if all_files else
             changed_paths(base, head, local=local, root=root))
    tree = [] if local else names(root, 'ls-tree', '-r', '-z', head)
    existing = None if local else {entry.split('\t', 1)[1] for entry in tree}
    symlinks = {entry.split('\t', 1)[1] for entry in tree if entry.startswith('120000 ')}
    renames = {}
    if base and not all_files:
        changes = iter(names(root, 'diff', '--name-status', '-z', '--find-renames', base, *([] if local else [head])))
        for status in changes:
            source = next(changes)
            if status.startswith(('R', 'C')):
                destination = next(changes)
                if status == 'R100':
                    renames[destination] = source
    errors, areas, added, modified, large = [], Counter(), 0, 0, []
    checksum_path = root / 'evidence/SHA256SUMS'
    checksum_data = (checksum_path.read_bytes() if checksum_path.is_file() else b'') if local else (
        blob(root, head, 'evidence/SHA256SUMS') or b'')
    digests = {}
    for line in checksum_data.decode().splitlines():
        if not re.fullmatch(r'[0-9a-f]{64}  [^\r\n]+', line):
            raise ValueError('evidence/SHA256SUMS has a malformed entry')
        digest, path = line.split('  ', 1)
        if not path.startswith(EVIDENCE) or '..' in PurePosixPath(path).parts:
            raise ValueError('evidence/SHA256SUMS must name repository-relative evidence files')
        if path in digests:
            raise ValueError('evidence/SHA256SUMS has a duplicate path')
        digests[path] = digest
    if all_files or 'evidence/SHA256SUMS' in paths:
        current = (names(root, 'ls-files', '--cached', '--others', '--exclude-standard', '-z', '--',
                         *[prefix.rstrip('/') for prefix in EVIDENCE]) if local else list(existing))
        paths = set(paths) | set(digests) | {p for p in current if p.startswith(EVIDENCE)}
    for path in sorted(set(paths)):
        if existing is not None and path not in existing:
            if path in digests:
                errors.append(f'{path}: checksum names a missing proof')
            continue
        file = root / path
        if local:
            if file.is_symlink():
                if not file.resolve().is_relative_to(root.resolve()):
                    errors.append(f'{path}: symlink leaves the repository')
                continue
            data = file.read_bytes() if file.is_file() else None
        else:
            data = blob(root, head, path)
        if data is None:
            if path in digests:
                errors.append(f'{path}: checksum names a missing proof')
            continue  # Deletions are welcome; they do not count toward size warnings.
        if path in symlinks:
            target = posixpath.normpath(str(PurePosixPath(path).parent / data.decode()))
            if target == '..' or target.startswith(('../', '/')):
                errors.append(f'{path}: symlink leaves the repository')
            continue
        old = blob(root, base, renames.get(path, path)) if base else None
        if reason := violation(path, data, policy):
            errors.append(f'{path}: {reason}')
        if path.startswith(EVIDENCE) and PurePosixPath(path).suffix != '.md' and PurePosixPath(path).name != 'SHA256SUMS':
            if digests.get(path) != hashlib.sha256(data).hexdigest():
                errors.append(f'{path}: essential proof needs its exact digest in evidence/SHA256SUMS')
        if (old is not None and old != data and path.startswith(EVIDENCE)
                and PurePosixPath(path).suffix != '.md' and PurePosixPath(path).name != 'SHA256SUMS'):
            errors.append(f'{path}: captured proof is immutable; add a correction note or new capture')
        if all_files or old == data:
            continue
        # An unchanged rename adds no review content. Compare blob identities
        # without treating arbitrary binary data as text lines.
        modified += 1
        areas['/'.join(PurePosixPath(path).parts[:2])] += 1
        if len(data) > policy['warnings']['large_bytes']:
            large.append((path, len(data)))
    if base and not all_files:
        args = ['diff', '--numstat', '-z', '--find-renames', base]
        if not local:
            args.append(head)
        tracked = names(root, *args)
        # Rename entries have empty path and two following NUL-delimited names.
        for row in tracked:
            fields = row.split('\t', 2)
            if len(fields) == 3 and fields[0].isdigit():
                added += int(fields[0])
        if local:
            for path in names(root, 'ls-files', '--others', '--exclude-standard', '-z'):
                file = root / path
                if file.is_file() and not file.is_symlink():
                    data = file.read_bytes()
                    if b'\0' not in data[:8192]:
                        added += len(data.splitlines())
    warnings = size_warnings(added, modified, large, policy['warnings'])
    return errors, warnings, areas


def check(root=ROOT, **options):
    errors, warnings, areas = inspect(root, **options)
    for message in errors:
        print(f'hygiene error: {message}')
    for message in warnings:
        print(f'hygiene warning: {message}')
    if warnings:
        print('Changed areas: ' + ', '.join(f'{name} ({n})' for name, n in areas.most_common()))
    print(f'hygiene: {len(errors)} error(s), {len(warnings)} warning(s)')
    if summary := os.environ.get('GITHUB_STEP_SUMMARY'):
        with open(summary, 'a') as output:
            output.write(f'\n### Repository hygiene\n\n{len(errors)} errors; {len(warnings)} warnings.\n')
            for message in [*errors, *warnings]:
                output.write(f'\n- {message.replace(chr(10), " ")}\n')
    return 1 if errors else 0


def pre_push(root, updates):
    status = 0
    for line in updates.splitlines():
        fields = line.split()
        if len(fields) != 4:
            raise ValueError('invalid pre-push ref update')
        _, head, remote_ref, base = fields
        if not remote_ref.startswith('refs/heads/') or set(head) == {'0'}:
            continue
        if set(base) == {'0'}:
            base = git(root, 'merge-base', head, 'origin/main').decode().strip()
        status |= check(root, base=base, head=head, local=False)
    return status


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--base', default='')
    parser.add_argument('--head', default='HEAD')
    parser.add_argument('--committed-only', action='store_true')
    parser.add_argument('--all', action='store_true', dest='all_files')
    parser.add_argument('--pre-push', action='store_true')
    args = parser.parse_args(argv)
    if args.pre_push:
        return pre_push(ROOT, sys.stdin.read())
    all_files = args.all_files or (args.committed_only and (not args.base or set(args.base) == {'0'}))
    base = '' if all_files else args.base or 'HEAD'
    return check(base=base, head=args.head, local=not args.committed_only, all_files=all_files)


if __name__ == '__main__':
    try:
        sys.exit(main())
    except (OSError, ValueError, subprocess.CalledProcessError) as error:
        print(f'hygiene: cannot verify inputs ({error})', file=sys.stderr)
        sys.exit(1)
