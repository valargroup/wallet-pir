#!/usr/bin/env python3
"""Inspect a saved initial query-node plan without exposing Terraform variables.

Run on the coordinator, using its normal Terraform credentials and production
writer lock. This reads the plan only; apply it with wallet-pir-terraform.sh
after checking the returned file digest. No state or provider changes occur.
"""
import argparse
import json
from pathlib import Path
import subprocess
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'lib'))
from wallet_pir_ops.query_infra import initial_plan
from wallet_pir_ops.terraform import file_sha256


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', required=True, type=Path)
    parser.add_argument('--plan', required=True, type=Path)
    parser.add_argument('--project-id', required=True)
    args = parser.parse_args()
    saved = args.plan.resolve()
    digest = file_sha256(saved)
    result = subprocess.run(['terraform', '-chdir=' + str(args.root.resolve()),
                             'show', '-json', str(saved)], capture_output=True, timeout=60)
    if result.returncode:
        parser.exit(1, 'Terraform could not inspect the saved plan; nothing was applied\n')
    try:
        addresses = initial_plan(json.loads(result.stdout), args.project_id)
        if file_sha256(saved) != digest:
            raise ValueError('saved plan changed during inspection')
    except (ValueError, KeyError, TypeError):
        parser.exit(1, 'Saved plan does not match the approved initial query node; nothing was applied\n')
    print(json.dumps({'saved_plan_sha256': digest, 'creates': addresses,
                      'existing_fleet_actions': 0}, sort_keys=True))


if __name__ == '__main__':
    main()
