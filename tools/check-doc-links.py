#!/usr/bin/env python3
"""Check local Markdown targets and heading anchors with one cached file scan."""
from pathlib import Path
import hashlib
import json
import re
import subprocess

ROOT = Path(__file__).resolve().parents[1]
LINK = re.compile(r'\]\(([^)\n]+)\)')
HEADING = re.compile(r'^#{1,6} +(.+)$', re.MULTILINE)


# These links belong to an immutable assurance capture. Resolve only its two
# removed oracle files at the exact tested source recorded in results.md, and
# verify their bytes against the capture's original manifest.
HISTORICAL_REPOSITORIES = frozenset({
    'https://github.com/valargroup/wallet-pir.git',
    'git@github.com:valargroup/wallet-pir.git',
})
HISTORICAL_SOURCES = {
    'transparent/evidence/prewarm-hint-assurance-2026-10-04/README.md': (
        '2bb5c0299eb752b68a5ad5fad9b156b046bef8c9',
        frozenset({
            'transparent/crates/transparent-native/testdata/batched_hint_kat.py',
            'transparent/crates/transparent-native/testdata/batched_hint_kat.json',
        })),
}


def historical_source_exists(document, resolved):
    capture = HISTORICAL_SOURCES.get(document)
    if capture is None:
        return False
    revision, targets = capture
    try:
        source = resolved.relative_to(ROOT.resolve()).as_posix()
        if source not in targets:
            return False
        bundle = (ROOT / document).parent
        if f'`{revision}`' not in (bundle / 'results.md').read_text():
            return False
        expected = json.loads((bundle / 'manifest.json').read_text())['sources']['source_sha256'][source]
        # CI checks out a shallow tree. Fetch only this policy's fixed captured
        # revision from the owning repository, never a ref or URL from Markdown.
        present = subprocess.run(['git', 'cat-file', '-e', f'{revision}^{{commit}}'],
                                 cwd=ROOT, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        if present.returncode:
            remote = subprocess.check_output(['git', 'remote', 'get-url', 'origin'],
                                             cwd=ROOT, text=True).strip()
            if remote not in HISTORICAL_REPOSITORIES:
                return False
            print(f'check-doc-links: fetching captured source {revision}', flush=True)
            subprocess.run(['git', 'fetch', '--no-tags', '--depth=1', remote, revision],
                           cwd=ROOT, check=True, stdout=subprocess.DEVNULL)
        blob = subprocess.check_output(['git', 'show', f'{revision}:{source}'],
                                       cwd=ROOT, stderr=subprocess.DEVNULL)
        return hashlib.sha256(blob).hexdigest() == expected
    except (ValueError, OSError, KeyError, TypeError, subprocess.SubprocessError):
        return False


def slug(text):
    return re.sub(r'[^a-z0-9 _-]', '', text.lower().replace('`', '')).replace(' ', '-')


def main():
    tracked = subprocess.check_output(['git', 'ls-files', '--cached', '--others', '--exclude-standard', '-z',
                                     '--', '*.md', ':!demos/**', ':!target/**'], cwd=ROOT)
    paths = sorted({p.decode() for p in tracked.split(b'\0') if p and '/node_modules/' not in p.decode()})
    texts, headings, broken, count = {}, {}, [], 0
    def read(path):
        path = path.resolve()
        if path not in texts:
            texts[path] = path.read_text()
        return texts[path]
    for name in paths:
        file = ROOT / name
        if not file.is_file():
            continue  # cached deletion
        count += 1
        for match in LINK.finditer(read(file)):
            target = re.sub(r' "[^\"]*"$', '', match[1])
            if target.startswith('<') and target.endswith('>'):
                target = target[1:-1]
            if target.startswith(('http://', 'https://', 'mailto:')):
                continue
            path, _, fragment = target.partition('#')
            resolved = (file.parent / path if path else file).resolve()
            if not resolved.exists() and not (not fragment and historical_source_exists(name, resolved)):
                broken.append(f"{name}: missing target '{target}'")
            elif fragment and resolved.suffix == '.md':
                if resolved not in headings:
                    headings[resolved] = {slug(h) for h in HEADING.findall(read(resolved))}
                if fragment not in headings[resolved]:
                    broken.append(f"{name}: no heading '#{fragment}' in '{path}'")
    if broken:
        print('\n'.join(broken))
        raise SystemExit(f'check-doc-links: {len(broken)} broken link(s)')
    print(f'check-doc-links: {count} files, no broken links')


if __name__ == '__main__':
    main()
