#!/usr/bin/env python3
"""Check local Markdown targets and heading anchors with one cached file scan."""
from pathlib import Path
import re
import subprocess

ROOT = Path(__file__).resolve().parents[1]
LINK = re.compile(r'\]\(([^)\n]+)\)')
HEADING = re.compile(r'^#{1,6} +(.+)$', re.MULTILINE)


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
            if not resolved.exists():
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
