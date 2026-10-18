#!/usr/bin/env python3
"""Reject fleet extraction plans with any change beyond declared state moves."""
import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'lib'))
from wallet_pir_ops.infra_layout import compare, declared_moves


def main():
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument('--before', type=Path, required=True)
    parser.add_argument('--after', type=Path, required=True)
    parser.add_argument('--root', type=Path, default=Path(__file__).resolve().parents[1] / 'infra/digitalocean/fleet')
    args = parser.parse_args()
    try:
        result = compare(json.loads(args.before.read_text()), json.loads(args.after.read_text()), declared_moves(args.root))
    except (ValueError, KeyError, TypeError, OSError):
        raise SystemExit('fleet extraction rejected; inspect private plans and reconcile drift') from None
    print(json.dumps(result, sort_keys=True))


if __name__ == '__main__':
    main()
