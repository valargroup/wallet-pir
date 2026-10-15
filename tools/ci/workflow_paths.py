#!/usr/bin/env python3
"""Fail when a workflow or local action runs a repository script that no longer exists.

Workflow steps run scripts by path, and no make target exercises those inline
steps, so deleting or moving a script used to surface only as a red main
(2026-10-14: ci-full.yml kept running a removed deploy script for eight
pushes). Only script paths under a tracked top-level directory are checked;
files a step writes (JSON outputs, artifacts) are not scripts and are ignored.
"""
from pathlib import Path
import re
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = re.compile(r'(?<![\w/$.{}@:-])(?:\./)?((?:[\w.-]+/)+[\w.-]+\.(?:sh|py|cjs|mjs|js|service))(?![\w/-])')


def present(root=ROOT):
    """Tracked and untracked (not ignored) files that exist on disk, as repository-relative paths."""
    listed = subprocess.check_output(['git', 'ls-files', '-z', '--cached', '--others', '--exclude-standard'],
                                     cwd=root).decode().split('\0')
    return {p for p in listed if p and (root / p).is_file()}


def missing(root=ROOT):
    files = present(root)
    tops = {p.split('/')[0] for p in files if '/' in p}
    found = []
    for workflow in sorted(p for p in files if p.startswith(('.github/workflows/', '.github/actions/'))
                           and p.endswith(('.yml', '.yaml'))):
        text = (root / workflow).read_text()
        directories = re.findall(r'working-directory:\s*([\w./-]+)', text)
        for number, line in enumerate(text.splitlines(), 1):
            if line.lstrip().startswith('#'):
                continue
            for match in SCRIPT.finditer(line):
                path = match[1]
                if path.split('/')[0] not in tops or path in files \
                        or any(f'{d.rstrip("/")}/{path}' in files for d in directories):
                    continue
                found.append(f'{workflow}:{number}: runs {path}, which does not exist')
    return found


def main():
    problems = missing()
    for problem in problems:
        print(problem, file=sys.stderr)
    if problems:
        print('Restore the script or update the workflow step that runs it.', file=sys.stderr)
        raise SystemExit(1)
    print('Workflow script paths exist')


if __name__ == '__main__':
    main()
