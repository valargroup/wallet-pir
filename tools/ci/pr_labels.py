#!/usr/bin/env python3
"""Select up to three primary PR labels, plus an area when three risks apply.

Reads the checked-in catalog and uses the authenticated GitHub CLI. Run from a
trusted checkout: pull-request changes must never supply this code or catalog
to a workflow with write permissions.
"""
import argparse
from functools import lru_cache
import json
from pathlib import Path
import re
import subprocess

ROOT = Path(__file__).resolve().parents[2]
CATALOG = ROOT / '.github/labels.json'
RULES = ROOT / '.github/labeler.yml'
LIMIT = 4
PRIMARY_LIMIT = 3
RISK_GROUPS = (('migration', 'journal-breaking'),
               ('wallet-api-breaking', 'publish-api-breaking'),
               ('pir-scheme', 'security-review', 'privacy-review'))
RISK_PRIORITY = tuple(name for group in RISK_GROUPS for name in group)


def catalog():
    labels = json.loads(CATALOG.read_text())
    if not isinstance(labels, dict) or not labels:
        raise ValueError('label catalog must be a nonempty object')
    for name, label in labels.items():
        if (not isinstance(label, dict) or set(label) != {'color', 'description'}
                or not re.fullmatch(r'[a-z][a-z0-9:-]*', name)
                or not isinstance(label['color'], str)
                or not re.fullmatch(r'[0-9a-fA-F]{6}', label['color'])
                or not isinstance(label['description'], str)
                or not 1 <= len(label['description']) <= 100):
            raise ValueError(f'invalid label definition: {name}')
    return labels


def sync(repository):
    # Validate the entire catalog before making any changes. --force updates
    # existing definitions by name; it never deletes or renames other labels.
    labels = catalog()
    for name, label in labels.items():
        subprocess.run(['gh', 'label', 'create', name, '--repo', repository,
                        '--color', label['color'], '--description', label['description'],
                        '--force'], check=True, timeout=30)
    print(f'Synchronized {len(labels)} catalog labels in {repository}')


@lru_cache(maxsize=None)
def path_pattern(glob):
    """Match the catalog's literal paths, single-segment *, and recursive **."""
    parts = glob.split('/')
    expression = ''
    for index, part in enumerate(parts):
        last = index == len(parts) - 1
        if part == '**':
            expression += '.*' if last else '(?:[^/]+/)*'
        else:
            expression += re.escape(part).replace(r'\*', '[^/]*')
            if not last:
                expression += '/'
    return re.compile(expression + r'\Z')


def select(files, existing, rules):
    """Prioritize migrations, API breaks, and security while retaining an area.

    Cover each risk category before extra signals from the same category. Three
    risks may use the primary slots; their main area then occupies slot four.
    Other PRs retain the usual three-label budget, including at least one area.
    """
    manual = set(existing) - set(rules)
    risks = []
    for group in RISK_GROUPS:
        present = next((name for name in group if name in manual), None)
        if present:
            risks.append(present)
    risks.extend(name for name in RISK_PRIORITY if name in manual and name not in risks)
    selected = risks[:PRIMARY_LIMIT]
    budget = min(LIMIT, max(PRIMARY_LIMIT, len(selected) + 1))
    ordinary = sorted(manual - set(RISK_PRIORITY))
    selected.extend(ordinary[:budget - len(selected) - 1])
    ranked = []
    for name, matches in rules.items():
        globs = matches[0]['changed-files'][0]['any-glob-to-any-file']
        score = sum(max(1, file['changes']) for file in files
                    if any(path_pattern(glob).fullmatch(file['filename']) for glob in globs))
        if score:
            category = 0 if name.startswith('product:') else 1 if name in {
                'area:publisher', 'area:query-server'} else 2
            ranked.append((-score, category, name))
    components = [name for _, _, name in sorted(ranked)]
    area = next((name for name in components if name.startswith('area:')
                 or name == 'documentation'), 'area:shared')
    remaining = components[:budget - len(selected)]
    if area not in remaining:
        if len(remaining) == budget - len(selected):
            remaining[-1] = area
        else:
            remaining.append(area)
    return selected + remaining


def api(repository, path, *, data=None, paginate=False):
    command = ['gh', 'api', f'repos/{repository}/{path}']
    if paginate:
        command.extend(['--paginate', '--slurp'])
    if data is not None:
        command.extend(['--method', 'PUT', '--input', '-'])
    result = subprocess.run(command, input=json.dumps(data) if data is not None else None,
                            text=True, capture_output=True, check=True, timeout=30)
    return json.loads(result.stdout)


def label_pr(repository, number):
    rules = json.loads(RULES.read_text())
    pr = api(repository, f'pulls/{number}')
    if pr['state'] != 'open':
        print(f'PR #{number}: skipped closed PR')
        return
    existing = [label['name'] for label in pr['labels']]
    pages = api(repository, f'pulls/{number}/files?per_page=100', paginate=True)
    selected = select([file for page in pages for file in page], existing, rules)
    if set(selected) != set(existing):
        # Replacing in one request keeps the final set within the limit instead
        # of first attaching every matching component and trimming afterwards.
        api(repository, f'issues/{number}/labels', data={'labels': selected})
    print(f'PR #{number}: ' + ', '.join(selected))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo', required=True, help='GitHub owner/repository')
    parser.add_argument('--pr-numbers', help='Positive PR numbers separated by newlines')
    args = parser.parse_args(argv)
    if not re.fullmatch(r'[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+', args.repo):
        parser.error('--repo must be owner/repository')
    numbers = args.pr_numbers.splitlines() if args.pr_numbers is not None else []
    if args.pr_numbers is not None and (not numbers or any(
            not re.fullmatch(r'[1-9][0-9]*', number.strip()) for number in numbers)):
        parser.error('--pr-numbers must contain positive integers separated by newlines')
    sync(args.repo)
    for number in dict.fromkeys(int(number.strip()) for number in numbers):
        label_pr(args.repo, number)


if __name__ == '__main__':
    main()
