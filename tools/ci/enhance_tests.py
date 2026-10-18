#!/usr/bin/env python3
"""Run the Enhance publisher's tiers from the workspace integration registry."""
from integration_tests import cargo_args as workspace_args, execute, inventory, validate as workspace_validate
import argparse

PACKAGE = 'enhance-publisher'


def validate(metadata, registry):
    package = next(p for p in metadata['packages'] if p['name'] == PACKAGE)
    names = {t['name'] for t in package['targets'] if 'test' in t['kind']}
    workspace_validate({PACKAGE: names}, {PACKAGE: registry})


def cargo_args(registry, tier):
    return workspace_args(PACKAGE, registry, tier)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--tier', choices=['fast', 'full'])
    parser.add_argument('--offline', action='store_true')
    parser.add_argument('--compile-only', action='store_true')
    args = parser.parse_args()
    inventory()
    if args.tier:
        execute([PACKAGE], args.tier, offline=args.offline, compile_only=args.compile_only)


if __name__ == '__main__':
    main()
