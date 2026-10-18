"""Keep full-only unit tests out of fast builds and verify full discovery."""
import json

from stage import ROOT


def classifications():
    return json.loads((ROOT / 'tools/ci/slow-tests.json').read_text())


def gated_cases(package):
    return {name for name, entry in classifications().get(package, {}).items()
            if isinstance(entry, dict) and entry.get('feature') == 'full-tests'}


def full_features(packages):
    return [name + '/full-tests' for name in sorted(packages) if gated_cases(name)]


def check_discovery(package, names, *, full):
    expected = gated_cases(package)
    mismatch = expected - set(names) if full else expected & set(names)
    if mismatch:
        detail = 'missing full-only tests' if full else 'full-only tests compiled in fast build'
        raise ValueError(f'{package}: {detail}: {sorted(mismatch)}')
