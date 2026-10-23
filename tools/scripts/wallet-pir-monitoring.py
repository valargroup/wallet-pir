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


def _observers(args):
    return monitoring.observers(_json(args.inventory), mode=args.mode,
                                edge_outputs=dict(item.split('=', 1) for item in args.edge_output),
                                roster=args.roster, host_snapshot=args.host_snapshot)


def render_observers(args):
    print(json.dumps(_observers(args), indent=2, sort_keys=True))


def coverage(args):
    # Volumes that cache-storage activations bound are always expected.
    expected = monitoring.activated_volumes(args.state_dir or default_state_dir())
    for host, volumes in (_json(args.expected_volumes) or {}).items():
        expected.setdefault(host, []).extend(volumes)
    report = monitoring.coverage(_json(args.inventory), terraform_outputs=_json(args.terraform_outputs),
                                 external_hosts=_json(args.external_hosts) or {}, roster=_json(args.roster),
                                 expected_volumes=expected)
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


def default_state_dir():
    from wallet_pir_ops.deploy import transaction
    return transaction.default_state_dir()


def _executor(args):
    from wallet_pir_ops.deploy import descriptors, remote
    inventory = descriptors.load_inventory(args.inventory)
    executor = remote.SSHExecutor(inventory)
    return executor, remote.production_lock(inventory, executor)


def _role(args, executor):
    """Directory, unit and readiness contract of the credential `--role` on `--host`."""
    if args.role == 'sidecar':
        return monitoring_certs.SIDECAR_CREDENTIALS, monitoring_certs.SIDECAR_UNIT, monitoring_certs.sidecar_ready()
    installed = executor.read(args.host, OBSERVERS)
    if installed is None:
        raise SystemExit('install the observer inventory before the collector credentials')
    revision = json.loads(installed).get('revision')
    return (monitoring_certs.COLLECTOR_CREDENTIALS, monitoring_certs.COLLECTOR_UNIT,
            monitoring_certs.collector_ready(revision))


def install_credentials(args):
    """Install from a private directory, e.g. a tmpfs filled from Infisical."""
    source = Path(args.source)
    names = monitoring_certs.COLLECTOR_FILES if args.role == 'collector' else monitoring_certs.SIDECAR_FILES
    files = {name: (source / name).read_bytes() for name in names}
    executor, lock = _executor(args)
    directory, unit, ready = _role(args, executor)
    changed = monitoring_certs.install(executor, lock, args.host, files, state_dir=default_state_dir(),
                                       directory=directory, unit=unit, ready=ready)
    print(json.dumps({'host': args.host, 'changed': changed}))


def install_observers(args):
    document = _observers(args)
    executor, lock = _executor(args)
    changed = monitoring_certs.install(
        executor, lock, args.host, {os.path.basename(OBSERVERS): (json.dumps(document, indent=2, sort_keys=True)
                                                                  + '\n').encode()},
        state_dir=default_state_dir(), directory=os.path.dirname(OBSERVERS), unit=monitoring_certs.COLLECTOR_UNIT,
        ready=monitoring_certs.collector_ready(document['revision']), mode=0o644)
    print(json.dumps({'host': args.host, 'changed': changed}))


def recover(args):
    """`reconcile` finishes an interrupted install; `rollback` undoes the latest committed one."""
    executor, lock = _executor(args)
    if args.role == 'observers':
        directory, unit = os.path.dirname(OBSERVERS), monitoring_certs.COLLECTOR_UNIT
        installed = executor.read(args.host, OBSERVERS)
        ready = monitoring_certs.collector_ready(json.loads(installed).get('revision') if installed else None)
    else:
        directory, unit, ready = _role(args, executor)
    operation = monitoring_certs.reconcile if args.command == 'reconcile' else monitoring_certs.rollback
    print(json.dumps({'host': args.host, 'unit': unit,
                      'transaction': operation(executor, lock, args.host, state_dir=default_state_dir(),
                                               directory=directory, ready=ready)}))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest='command', required=True)
    for name, handler in (('render-observers', render_observers), ('install-observers', install_observers)):
        child = sub.add_parser(name)
        child.add_argument('--inventory', required=True)
        child.add_argument('--mode', choices=('shadow', 'active'), required=True)
        child.add_argument('--edge-output', action='append', default=[], metavar='HOST=PATH')
        child.add_argument('--roster', help='Transparent roster whose members are expected hosts')
        child.add_argument('--host-snapshot', help='where the APM writes the capacity host snapshot')
        if name == 'install-observers':
            child.add_argument('--host', required=True, help='the inventory host running pir-apm')
        child.set_defaults(handler=handler)
    child = sub.add_parser('coverage')
    child.add_argument('--inventory', required=True)
    child.add_argument('--terraform-outputs')
    child.add_argument('--external-hosts', help='JSON map of declared external hosts to their addresses')
    child.add_argument('--roster')
    child.add_argument('--expected-volumes')
    child.add_argument('--state-dir', help='deploy state directory holding cache-data receipts')
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
    for name in ('reconcile', 'rollback'):
        child = sub.add_parser(name, help='%s the latest monitoring file transaction on a host' % name)
        child.add_argument('--inventory', required=True)
        child.add_argument('--host', required=True)
        child.add_argument('--role', choices=('sidecar', 'collector', 'observers'), default='sidecar')
        child.set_defaults(handler=recover)
    args = parser.parse_args(argv)
    return args.handler(args) or 0


if __name__ == '__main__':
    sys.exit(main())
