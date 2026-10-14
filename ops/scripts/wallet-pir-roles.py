#!/usr/bin/env python3
"""Render a Transparent publisher's configuration and systemd unit for review."""
import argparse
import json
from pathlib import Path
import sys

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'lib'))
from wallet_pir_ops.paths import ROOT  # noqa: F401,E402
from transparent.ops.lib.roles import publisher  # noqa: E402


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path, required=True)
    parser.add_argument('--config-path', required=True)
    parser.add_argument('--state-dir', required=True)
    parser.add_argument('--executable', default='/usr/local/bin/transparent-publish-controller')
    args = parser.parse_args(argv)
    role = publisher(json.loads(args.config.read_text()), config_path=args.config_path,
                     state_dir=args.state_dir, executable=args.executable)
    print(json.dumps(role.describe(), sort_keys=True, indent=2))


if __name__ == '__main__':
    main()
