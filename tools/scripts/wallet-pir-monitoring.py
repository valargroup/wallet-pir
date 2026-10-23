#!/usr/bin/env python3
"""Per-host APM observer inventory, coverage and credentials; see `--help`.

The sidecars themselves are installed, updated, checked and rolled back by
`wallet-pir-deploy.py` as the `apm-sidecar.observer` role. This tool renders
what the central APM needs from the same deployment inventory, reports
coverage gaps, issues monitoring certificates on an operator machine, and
installs credential files or the observer inventory under the production lock.
"""
import argparse
import json
import os
from pathlib import Path
import sys

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'lib'))
from wallet_pir_ops import monitoring, monitoring_certs  # noqa: E402

OBSERVERS = '/etc/pir-apm/observers.json'


def _json(path):
    return json.loads(Path(path).read_text()) if path else None


def render_observers(args):
    edge = dict(item.split('=', 1) for item in args.edge_output)
    document = monitoring.observers(_json(args.inventory), mode=args.mode, edge_outputs=edge)
    print(json.dumps(document, indent=2, sort_keys=True))


def coverage(args):
    report = monitoring.coverage(_json(args.inventory), terraform_outputs=_json(args.terraform_outputs),
                                 external_hosts=_json(args.external_hosts) or {}, roster=_json(args.roster),
                                 expected_volumes=_json(args.expected_volumes))
    print(json.dumps(report, indent=2, sort_keys=True))
    gaps = report['missing_declarations'] or report['missing_volumes'] or report['not_provisioned']
    return 1 if gaps or report['partial'] else 0


def issue(args):
    if args.command == 'issue-ca':
        print(monitoring_certs.issue_ca(args.output))
    elif args.command == 'issue-host':
        print(monitoring_certs.issue_leaf(args.ca, args.output, monitoring.tls_name(args.host), 'server'))
    else:
        print(monitoring_certs.issue_leaf(args.ca, args.output, monitoring.COLLECTOR_IDENTITY, 'client'))


def check_expiry(args):
    status = monitoring_certs.expiry_status(Path(args.cert).read_text())
    print(status)
    return 0 if status == 'healthy' else 1


def _executor(args):
    from wallet_pir_ops.deploy import descriptors, remote
    inventory = descriptors.load_inventory(args.inventory)
    executor = remote.SSHExecutor(inventory)
    return executor, remote.production_lock(inventory, executor)


def install_credentials(args):
    """Install from a private directory, e.g. a tmpfs filled from Infisical."""
    source = Path(args.source)
    collector = args.role == 'collector'
    names = monitoring_certs.COLLECTOR_FILES if collector else monitoring_certs.SIDECAR_FILES
    files = {name: (source / name).read_bytes() for name in names}
    executor, lock = _executor(args)
    changed = monitoring_certs.install(
        executor, lock, args.host, files,
        directory=monitoring_certs.COLLECTOR_CREDENTIALS if collector else monitoring_certs.SIDECAR_CREDENTIALS,
        unit='pir-apm.service' if collector else monitoring_certs.SIDECAR_UNIT,
        health='http://127.0.0.1:3002/healthz' if collector else monitoring_certs.SIDECAR_HEALTH)
    print(json.dumps({'host': args.host, 'changed': changed}))


def install_observers(args):
    document = monitoring.observers(_json(args.inventory), mode=args.mode,
                                    edge_outputs=dict(item.split('=', 1) for item in args.edge_output))
    executor, lock = _executor(args)
    changed = monitoring_certs.install(
        executor, lock, args.host, {os.path.basename(OBSERVERS): (json.dumps(document, indent=2, sort_keys=True)
                                                                  + '\n').encode()},
        directory=os.path.dirname(OBSERVERS), unit='pir-apm.service', health='http://127.0.0.1:3002/healthz',
        mode=0o644)
    print(json.dumps({'host': args.host, 'changed': changed}))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest='command', required=True)
    for name, handler in (('render-observers', render_observers), ('install-observers', install_observers)):
        child = sub.add_parser(name)
        child.add_argument('--inventory', required=True)
        child.add_argument('--mode', choices=('shadow', 'active'), required=True)
        child.add_argument('--edge-output', action='append', default=[], metavar='HOST=PATH')
        if name == 'install-observers':
            child.add_argument('--host', required=True, help='the inventory host running pir-apm')
        child.set_defaults(handler=handler)
    child = sub.add_parser('coverage')
    child.add_argument('--inventory', required=True)
    child.add_argument('--terraform-outputs')
    child.add_argument('--external-hosts', help='JSON map of declared external hosts to their addresses')
    child.add_argument('--roster')
    child.add_argument('--expected-volumes')
    child.set_defaults(handler=coverage)
    child = sub.add_parser('issue-ca')
    child.add_argument('--output', required=True)
    child.set_defaults(handler=issue)
    for name in ('issue-host', 'issue-collector'):
        child = sub.add_parser(name)
        child.add_argument('--ca', required=True)
        child.add_argument('--output', required=True)
        if name == 'issue-host':
            child.add_argument('--host', required=True)
        child.set_defaults(handler=issue)
    child = sub.add_parser('check-expiry')
    child.add_argument('--cert', required=True)
    child.set_defaults(handler=check_expiry)
    child = sub.add_parser('install-credentials')
    child.add_argument('--inventory', required=True)
    child.add_argument('--host', required=True)
    child.add_argument('--role', choices=('sidecar', 'collector'), default='sidecar')
    child.add_argument('--source', required=True, help='private directory holding the credential files')
    child.set_defaults(handler=install_credentials)
    args = parser.parse_args(argv)
    return args.handler(args) or 0


if __name__ == '__main__':
    sys.exit(main())
