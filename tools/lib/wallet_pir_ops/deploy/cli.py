"""Command line for `tools/scripts/wallet-pir-deploy.py`."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys

from . import descriptors
from .engine import DeployError
from .remote import LockHeld, RemoteError, SSHExecutor
from .transaction import default_state_dir

from ..paths import ROOT
DESCRIPTORS = ROOT / 'tools/deploy/services.toml'

USAGE = """\
Fleet data operations over SSH that are not binary rollouts. Binary rollouts
of every service use tools/deploy/pir-deploy, which runs tools/deploy/pirctl on
each host. Transparent schema recipes run on the pinned coordinator under the
production lock.

  schema-plan --recipe FILE
  schema-preflight --recipe FILE
  schema-deploy --recipe FILE --expect-recipe-sha256 HASH
  schema-status [--transaction ID]
  schema-rollback [--transaction ID]
      Journaled Transparent cutover phases, verified inputs and bounded rollback.
      Recipes contain no credentials. Deployment requires the reviewed plan hash.

  schema-source-{plan,preflight,stage,status} --source-sha REV --sha256 HASH
      --archive FILE is required except for status. Bootstrap immutable reviewed
      sources over SSH under the coordinator lock; this never activates services.

  schema-publication-{plan,preflight,start,status} --source-sha REV
      --release-result-sha256 HASH [--expect-plan-sha256 HASH]
      Prepare the approved full v11 publication on the pinned coordinator.
      Requires completed ingestion; preserves canonical service and all partial output.

  txid-display-{plan,preflight,status,ingest-status} --request F --request-sha256 H
  txid-display-deploy --request F --request-sha256 H --expect-plan-sha256 H
      --phase stage|workers|route|controller
      [workers: --publication-map-sha256 H [--replace-active H]]
  txid-display-{ingest-start,bootstrap,verify,measure-start,retire} ... --expect-plan-sha256 H
  txid-display-{ingest-stop,measure-stop,stop} --request F --request-sha256 H
  txid-display-rollback --request F --request-sha256 H [--transaction ID]
      One txid display site beside history (tools/txid/lib/txid_display_poc.py).
      Every mutation holds the production lock, journals prior bytes and prints the
      rollback command.

  transparent-binary-{plan,preflight,baseline,status} --request F --request-sha256 H
  transparent-binary-deploy --request F --request-sha256 H [--binary F] [--skip-exact-check]
  transparent-binary-rollback --request F --request-sha256 H [--transaction ID]
      Worker binary upgrade of the v12 history providers or 4k8 display workers
      under the unified publisher (tools/transparent/lib/binary_upgrade.py).
      Publication identity is pinned and the current map comes from the
      publisher, all re-checked under the lock, after every restart and after
      rollback. Against a retired standalone controller only status and
      rollback of a recorded transaction run; that rollback re-pins history's
      continuous load and leaves it stopped.

  transparent-load-repair-{plan,preflight,status} --request F --request-sha256 H
  transparent-load-repair-deploy --request F --request-sha256 H --expect-plan-sha256 H
  transparent-load-repair-rollback --request F --request-sha256 H [--transaction ID] [--force]
      Install this release's reviewed continuous-load script and start only the stopped
      transparent-v2-5qps-continuous.service, after an independent encrypted-answer
      check; refuses a critical latch or changed pinned binaries
      (tools/transparent/lib/load_repair.py).

  status-routing-{plan,preflight,deploy,status,rollback} --request F --request-sha256 H
  coordinator-edge-{plan,preflight,deploy,status,rollback,verify,dns-plan,dns-stage,
                    history-relay-plan,history-relay-repair} --request F --request-sha256 H
      Status wallet-route cutover and the unified wallet site on the coordinator Caddy.
      Every change names the reviewed plan with --expect-plan-sha256.

  cache-storage-{plan,prune}, cache-volume-{plan,provision},
  cache-data-{plan,prepare,activate,status}
      Retire an unused transaction-detail cache, create its approved data volume,
      and move the Details cache onto it, each under the production lock.

  worker-report SERVICE [--only ROLE[@HOST]]
      Read-only private report from existing endpoints: process, executable,
      assigned and loaded publication, preparation, readiness axes.
  fleet [--complete]
      Configured roles and ancillary units; --complete also requires the whole fleet.
  ancillary-status NAME
      Read-only systemd state of one shared routing or supervision unit.

  ops-package-{plan,preflight} --sha REV --ci-run-id N --fleet F [--known-hosts F] [--archive F] [--host H]
  ops-package-install ...same... --expect-plan-sha256 H
  ops-package-status [--host H]
  ops-package-reconcile [--transaction ID] [--archive F] [--fleet F] [--known-hosts F]
  ops-package-rollback [--transaction ID]
      Install the operations package on the coordinator as root-owned
      /opt/wallet-pir-ops/releases/<sha> selected by the `current` link, with the
      operator inventory /etc/wallet-pir/fleet.json and optional public host-key pins
      /etc/wallet-pir/known_hosts. The archive is the wallet-pir-ops-<sha> artifact of
      full-CI run N on main, downloaded through gh; a local --archive must equal it.
      Journaled under the production lock; never touches a unit.

The inventory (hosts, SSH, lock) is --inventory or WALLET_PIR_DEPLOY_INVENTORY.
"""


def transparent_cli():
    from tools.transparent.lib import deploy_cli
    return deploy_cli


def parser():
    top = argparse.ArgumentParser(prog='wallet-pir-deploy.py', description=USAGE,
                                  formatter_class=argparse.RawDescriptionHelpFormatter)
    top.add_argument('--json', action='store_true', help='structured results for ops-package commands')
    top.add_argument('--inventory', default=os.environ.get('WALLET_PIR_DEPLOY_INVENTORY'))
    top.add_argument('--descriptors', default=str(DESCRIPTORS))
    top.add_argument('--state-dir', default=str(default_state_dir()))
    top.add_argument('--baseline', help='baseline file (default <state-dir>/baselines/<service>.json)')
    commands = top.add_subparsers(dest='command', required=True)
    command = commands.add_parser('fleet', help='read configured hosts and roles; makes no host connections')
    command.add_argument('--complete', action='store_true',
                         help='also require every registered service, role, responsibility and ancillary kind')
    for operation in ('plan', 'preflight', 'deploy', 'status', 'rollback'):
        command = commands.add_parser('status-routing-' + operation,
                                      help='Status wallet-route cutover only')
        command.add_argument('--request', type=Path, required=True)
        command.add_argument('--request-sha256', required=True)
        if operation == 'deploy':
            command.add_argument('--expect-plan-sha256', required=True)
        if operation in ('status', 'rollback'):
            command.add_argument('--transaction')
    for operation in ('plan', 'preflight', 'deploy', 'status', 'rollback', 'verify', 'dns-plan', 'dns-stage',
                      'history-relay-plan', 'history-relay-repair'):
        command = commands.add_parser('coordinator-edge-' + operation,
                                      help='add or update only the unified wallet site on the coordinator Caddy')
        command.add_argument('--request', type=Path, required=True)
        command.add_argument('--request-sha256', required=True)
        if operation in ('deploy', 'dns-stage', 'history-relay-repair'):
            command.add_argument('--expect-plan-sha256', required=True)
        if operation == 'deploy':
            command.add_argument('--preserve-on-failure', action='store_true',
                                 help='retain the installed candidate on failure; finish with coordinator-edge-verify')
        if operation in ('status', 'rollback', 'verify'):
            command.add_argument('--transaction')
    command = commands.add_parser('ancillary-status', help='read-only state of ancillary units')
    command.add_argument('name', nargs='?', help='one ancillary unit (default: all)')
    for name in ('ops-package-plan', 'ops-package-preflight', 'ops-package-install'):
        command = commands.add_parser(name)
        command.add_argument('--sha', required=True, help='full source revision of the package')
        command.add_argument('--ci-run-id', type=int,
                             help='the successful full-CI run on main whose wallet-pir-ops-<sha> artifact installs')
        command.add_argument('--archive', help='a local copy; accepted only if it equals the artifact\'s archive')
        command.add_argument('--fleet', required=True, help='the complete operator inventory to install')
        command.add_argument('--known-hosts', help='reviewed public host-key pins for /etc/wallet-pir/known_hosts')
        command.add_argument('--host', help='coordinator inventory host (default: the remote lock host)')
        if name == 'ops-package-install':
            command.add_argument('--expect-plan-sha256', required=True)
    for name in ('ops-package-status', 'ops-package-reconcile', 'ops-package-rollback'):
        command = commands.add_parser(name)
        command.add_argument('--host', help='coordinator inventory host (default: the remote lock host)')
        if name != 'ops-package-status':
            command.add_argument('--transaction')
        if name == 'ops-package-reconcile':
            command.add_argument('--archive', help='the planned archive (default: this runner\'s artifact copy)')
            command.add_argument('--fleet', help='the planned inventory, if it is not yet installed')
            command.add_argument('--known-hosts', help='the planned host-key pins, if not yet installed')
    transparent_cli().register(commands)
    from . import cache_storage
    cache_storage.register(commands)
    from . import cache_volume
    cache_volume.register(commands)
    from . import cache_data
    cache_data.register(commands)
    command = commands.add_parser('worker-report', help='read-only private report of workers from their endpoints')
    command.add_argument('service')
    command.add_argument('--only', action='append', metavar='ROLE[@HOST]')
    return top


def run_package(args, inventory, executor, layout, github, out):
    """One `ops-package-*` command; see `package.Package`."""
    from . import package
    log = (lambda message: None) if args.json else out
    target = package.Package(inventory, executor, args.state_dir, args.host, layout, out=log,
                             inventory_path=args.inventory, github=github)
    command = args.command[len('ops-package-'):]
    inputs = {}
    if command in ('plan', 'preflight', 'install'):
        inputs = {'run_id': args.ci_run_id, 'archive': args.archive, 'known_hosts': args.known_hosts}
    if command in ('plan', 'preflight'):
        if command == 'plan':
            target.ex.check_identity(target.host)
            plan, problems, _ = target.build_plan(args.sha, args.fleet, **inputs)
        else:
            plan, problems = target.preflight(args.sha, args.fleet, **inputs)
        digest = package.durable.digest(plan)
        install = target.command('ops-package-install', '--sha', args.sha, '--fleet', args.fleet,
                                 *(['--ci-run-id', str(args.ci_run_id)] if args.ci_run_id else []),
                                 *(['--known-hosts', args.known_hosts] if args.known_hosts else []),
                                 '--expect-plan-sha256', digest)
        if args.json:
            out(json.dumps({'operation': args.command, 'plan': plan, 'plan_sha256': digest,
                            'problems': problems, 'install': install}, sort_keys=True))
        else:
            target.describe(plan, problems)
            out('plan sha256: ' + digest)
            out('install: ' + install)
        return 1 if command == 'preflight' and problems else 0
    if command == 'status':
        result = target.status()
        if args.json:
            out(json.dumps(result, sort_keys=True))
        return 0
    if command == 'install':
        journal = target.install(args.sha, args.fleet, args.expect_plan_sha256, **inputs)
    elif command == 'reconcile':
        journal = target.reconcile(args.transaction, args.archive, args.fleet, args.known_hosts)
    else:
        journal = target.rollback(args.transaction)
    result = {'operation': args.command, 'transaction': journal.id if journal else None,
              'phase': journal.status if journal else 'no-op',
              'rollback': journal and target.command('ops-package-rollback', '--transaction', journal.id),
              'status': target.command('ops-package-status')}
    if args.json:
        out(json.dumps(result, sort_keys=True))
    else:
        out('status: ' + result['status'])
        if journal and journal.status == 'committed':
            out('rollback: ' + result['rollback'])
    return 0


def main(argv=None, executor=None, out=print, package_layout=None, package_github=None, **options):
    """`executor` and `options` (passed to the product handlers) let tests substitute a fake fleet and clock."""
    args = parser().parse_args(argv)
    from .deployment_alerts import Notifications
    # Read-only commands never open a window. Existing deploy handlers keep their
    # own single validation pass and production lock.
    deploying = not args.command.startswith('ops-package-') and args.command.endswith(
        ('-deploy', '-rollback', '-restart', '-verify'))
    if not deploying:
        return run(args, executor, out, package_layout, package_github, **options)
    try:
        with Notifications(getattr(args, 'service', None)) as notifications:
            code = run(args, executor, out, package_layout, package_github, **options)
            notifications.finish(failed=code != 0)
            return code
    except (RemoteError, ValueError, OSError, subprocess.TimeoutExpired) as error:
        out('error: ' + str(error))
        return 1


def run(args, executor, out, package_layout, package_github, **options):
    def error_result(message, phase='failed'):
        if args.json:
            out(json.dumps({'service': getattr(args, 'service', None), 'operation': args.command,
                            'targets': [], 'problems': [message], 'transaction': None, 'phase': phase}, sort_keys=True))
        else:
            out('error: ' + message)
    try:
        from ..bundle import check_installed
        product_command = args.command.startswith(('schema-', 'txid-display-', 'transparent-binary-', 'transparent-alerts-',
                                                   'transparent-load-repair-', 'transparent-provider-maintenance-',
                                                   'ops-package-', 'status-routing-',
                                                   'coordinator-edge-', 'cache-storage-', 'cache-volume-', 'cache-data-'))
        mutation = (getattr(args, 'stage', False)
                    or product_command and not args.command.endswith(('-plan', '-preflight', '-status', '-summary', '-recipe')))
        # A clean installed package may install a newer package revision; every
        # binary change still names the same source revision as the package.
        package_command = args.command.startswith('ops-package-')
        check_installed(mutation=mutation, revision=None if package_command else getattr(args, 'sha', None))
        if args.command.startswith('cache-data-'):
            from . import cache_data
            out(json.dumps(cache_data.dispatch(args, executor), sort_keys=True))
            return 0
        if args.command.startswith('cache-volume-'):
            from . import cache_volume
            out(json.dumps(cache_volume.dispatch(args, executor), sort_keys=True))
            return 0
        if args.command.startswith('cache-storage-'):
            from . import cache_storage
            out(json.dumps(cache_storage.dispatch(args, executor), sort_keys=True))
            return 0
        if args.command.startswith(('schema-', 'txid-display-', 'transparent-binary-', 'transparent-alerts-',
                                     'transparent-load-repair-', 'transparent-provider-maintenance-')):
            if args.json:
                raise DeployError('product recipes already provide their own JSON receipts; omit --json')
            return transparent_cli().dispatch(args, executor=executor, out=out, **options)
        if not args.inventory:
            raise DeployError('pass --inventory or set WALLET_PIR_DEPLOY_INVENTORY')
        if args.command.startswith('status-routing-'):
            from .engine import mutation_fence
            from tools.status.lib.routing import Routing, load_request, digest
            prefix = 'status-routing-'
            inventory = descriptors.load_inventory(args.inventory)
            request = load_request(args.request, args.request_sha256)
            routing = Routing(inventory, executor or SSHExecutor(inventory), args.state_dir, request,
                              lock=options.get('lock'))
            operation = args.command.removeprefix(prefix)
            if operation in ('plan', 'preflight'):
                mutation_fence(inventory, routing.ex)
                plan, _before, _after = routing.plan(ready=operation == 'preflight')
                result = {'plan': plan, 'plan_sha256': digest(json.dumps(plan, sort_keys=True))}
            elif operation == 'deploy':
                result = routing.deploy(args.expect_plan_sha256)
            elif operation == 'rollback':
                result = routing.rollback(args.transaction)
            else:
                from .transaction import Journal
                journal = Journal.load(args.state_dir, getattr(routing, 'service_name', 'status-routing'), args.transaction)
                result = {'transaction': journal.id if journal else None,
                          'phase': journal.status if journal else None,
                          'caddyfile_sha256': routing.ex.sha256(routing.host, '/etc/caddy/Caddyfile')}
            out(json.dumps(result, sort_keys=True))
            return 0
        if args.command.startswith('coordinator-edge-'):
            from .engine import mutation_fence
            from .unified_edge import SERVICE, UnifiedEdge, load_request, digest
            inventory = descriptors.load_inventory(args.inventory)
            request = load_request(args.request, args.request_sha256)
            edge = UnifiedEdge(inventory, executor or SSHExecutor(inventory), args.state_dir, request,
                               lock=options.get('lock'))
            operation = args.command.removeprefix('coordinator-edge-')
            if operation == 'history-relay-plan':
                mutation_fence(inventory, edge.ex)
                plan, _before, _after = edge.history_relay_plan()
                result = {'plan': plan, 'plan_sha256': digest(json.dumps(plan, sort_keys=True))}
            elif operation == 'history-relay-repair':
                result = edge.repair_history_relay(args.expect_plan_sha256)
            elif operation == 'dns-plan':
                mutation_fence(inventory, edge.ex)
                plan, _after = edge.dns_plan()
                result = {'plan': plan, 'plan_sha256': digest(json.dumps(plan, sort_keys=True))}
            elif operation == 'dns-stage':
                result = edge.stage_dns(args.expect_plan_sha256)
            elif operation in ('plan', 'preflight'):
                mutation_fence(inventory, edge.ex)
                plan, _before, _after = edge.plan()
                result = {'plan': plan, 'plan_sha256': digest(json.dumps(plan, sort_keys=True))}
            elif operation == 'deploy':
                result = edge.deploy(args.expect_plan_sha256, preserve_on_failure=args.preserve_on_failure)
            elif operation == 'verify':
                result = edge.verify(args.transaction)
            elif operation == 'rollback':
                result = edge.rollback(args.transaction)
            else:
                from .transaction import Journal
                journal = Journal.load(args.state_dir, SERVICE, args.transaction)
                result = {'transaction': journal.id if journal else None,
                          'phase': journal.status if journal else None,
                          'caddyfile_sha256': edge.ex.sha256(edge.host, '/etc/caddy/Caddyfile')}
            out(json.dumps(result, sort_keys=True))
            return 0
        if args.command == 'fleet':
            from ..fleet import Fleet
            out(json.dumps(Fleet.load(args.inventory, args.descriptors).describe(args.complete), sort_keys=True))
            return 0
        if package_command:
            inventory = descriptors.load_inventory(args.inventory)
            return run_package(args, inventory, executor or SSHExecutor(inventory), package_layout, package_github, out)
        if args.command == 'ancillary-status':
            inventory = descriptors.load_inventory(args.inventory)
            names = [args.name] if args.name else sorted(inventory.ancillary)
            unknown = [n for n in names if n not in inventory.ancillary]
            if unknown:
                raise DeployError('unknown ancillary unit %r; known: %s' % (unknown[0], sorted(inventory.ancillary)))
            executor = executor or SSHExecutor(inventory)
            results = []
            for name in names:
                entry = inventory.ancillary[name]
                # Every unit is reported; an unreachable host is a result, not an omission.
                try:
                    state = executor.probe_unit(entry['host'], entry['unit'])
                    observed = {'active_state': state['active_state'], 'load_state': state['load_state'],
                                'binary_sha256': state['exe_sha256']}
                except (RemoteError, OSError, subprocess.TimeoutExpired, ValueError) as error:
                    observed = {'error': str(error)}
                results.append({'name': name, 'host': entry['host'], 'unit': entry['unit'],
                                'consumers': entry.get('consumers'), **observed})
            out(json.dumps(results[0] if args.name else {'ancillary': results}, sort_keys=True))
            return 1 if any('error' in r for r in results) else 0
        services = descriptors.load_descriptors(args.descriptors)
        if args.service not in services:
            raise DeployError('unknown service %r; known: %s' % (args.service, sorted(services)))
        service = services[args.service]
        inventory = descriptors.load_inventory(args.inventory)
        if args.command == 'worker-report':
            from .. import worker_report
            executor = executor or SSHExecutor(inventory)
            selected = descriptors.select(descriptors.targets(service, inventory), args.only)
            out(json.dumps({'service': service.name, 'workers': worker_report.collect(service, selected, executor)},
                           sort_keys=True))
            return 0
        raise DeployError('unknown command %r' % args.command)
    except subprocess.TimeoutExpired:
        error_result('command timed out; reconcile the recorded transaction and inspect its private logs', 'unknown')
        return 75 if args.command.startswith('schema-') else 1
    except (DeployError, RemoteError, LockHeld, descriptors.DescriptorError, OSError, ValueError) as error:
        error_result(str(error))
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
