#!/usr/bin/env python3
"""Render one role's configuration and systemd unit for review.

`--service S --role R --settings FILE --release DIR` renders an external
worker role from the registry (its product adapter, template and the
inventory-style settings in FILE) and prints the unit, its configuration
files, its writable directories and the product's preflight check.
Without --service, the Transparent publisher is rendered from --config,
--config-path and --state-dir, as before.
"""
import argparse
import json
from pathlib import Path
import sys

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'lib'))
from wallet_pir_ops.paths import ROOT  # noqa: E402
from wallet_pir_ops.deploy import descriptors  # noqa: E402
from tools.transparent.lib.roles import publisher  # noqa: E402

REGISTRY = ROOT / 'tools/deploy/services.toml'


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--service', help='registry service, e.g. receiver, enhance, transparent-history')
    parser.add_argument('--role', help='role of --service; "publisher" for transparent-history')
    parser.add_argument('--settings', type=Path, help='JSON settings of one role instance')
    parser.add_argument('--release', help='release directory the unit runs from')
    parser.add_argument('--config', type=Path)
    parser.add_argument('--config-path')
    parser.add_argument('--state-dir')
    parser.add_argument('--executable', default='/usr/local/bin/transparent-publish-controller')
    args = parser.parse_args(argv)
    if args.service is None or (args.service, args.role) == ('transparent-history', 'publisher'):
        if args.service is None and args.role is not None:
            parser.error('--role needs --service')
        missing = [flag for flag, value in (('--config', args.config), ('--config-path', args.config_path),
                                            ('--state-dir', args.state_dir)) if value is None]
        if missing:
            parser.error('the Transparent publisher needs ' + ', '.join(missing))
        role = publisher(json.loads(args.config.read_text()), config_path=args.config_path,
                         state_dir=args.state_dir, executable=args.executable)
    else:
        services = descriptors.load_descriptors(REGISTRY)
        if args.service not in services:
            parser.error('unknown service %r; known: %s' % (args.service, sorted(services)))
        roles = services[args.service].roles
        rendered = sorted(name for name, r in roles.items() if r.mode == 'role')
        if args.role not in rendered:
            parser.error('%s renders roles %s; other roles keep their installed units' % (args.service, rendered))
        if args.settings is None or args.release is None:
            parser.error('--settings and --release are required for a rendered role')
        role = roles[args.role].render(json.loads(args.settings.read_text()), args.release)
    print(json.dumps(role.describe(), sort_keys=True, indent=2))


if __name__ == '__main__':
    main()
