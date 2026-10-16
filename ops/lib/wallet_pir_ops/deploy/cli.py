"""Command line for `ops/scripts/wallet-pir-deploy.py`."""
import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import shlex
import sys
import tempfile

from . import descriptors
from .engine import Deployer, DeployError, file_sha256
from .remote import LockHeld, RemoteError, SSHExecutor
from .transaction import default_state_dir

from ..paths import ROOT
DESCRIPTORS = ROOT / 'ops/deploy/services.toml'
SHA256 = re.compile('^[0-9a-f]{64}$')

USAGE = """\
Transactional deploys of Enhance, Status and Receiver PIR over SSH.
Transparent schema recipes run on the pinned coordinator under the same lock.

  plan SERVICE (--binary F | --archive F --sha REV | --sha256 H)
      Read-only: per-target action, drop-in handling and unit drift.
      Targets whose inventory entry names another artifact_kind (the GPU
      worker) need --extra-archive KIND=FILE and roll in their own transaction.
  preflight SERVICE (...same...) [--stage]
      Read-only unless --stage: identity on every host, baseline, drift,
      disk space and, where the release is staged, its self-check.
  deploy SERVICE (...same...) [--allow-unit-drift] [--retire-historical]
      Holds the production lock; changes only targets that differ.
  rollback SERVICE [--transaction ID] [--force]
      Restores the touched targets of the latest (or named) transaction.
  status SERVICE
  capture-baseline SERVICE [--output FILE]

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

  schema-candidate-{plan,preflight,stage,status,reconcile} --request F --request-sha256 H
      [--expect-plan-sha256 H]  Verify and retain the changed-native candidate's 18
      artifacts under /srv/transparent-activity/candidates; installs and runs nothing.
  schema-candidate-upload-{plan,preflight,stage} --source-sha REV --attempt N
      --transparent-filter F --transparent-publisher F --supplemental F [--expect-plan-sha256 H]
  schema-candidate-upload-{status,reconcile} --source-sha REV --request-sha256 H
      From root's workstation (remote-lock inventory, pinned SSH): stream exactly the three
      pinned candidate archives to the coordinator's fixed candidate archive namespace.
  schema-candidate-worker-build --host H --source-sha REV --publication-request-sha256 H
      Render the candidate worker executable pair for schema-input-{plan,...,stage}.
  schema-qualify-{plan,preflight,run} --request F --request-sha256 H [--expect-plan-sha256 H]
  schema-qualify-{status,reconcile} --request-sha256 H
      Deployed candidate staged load, freshness, capacity trial or one lifecycle fault,
      run as one detached owner under the lock; run requires the reviewed plan digest.
      Reconcile restores an owned unit effect that did not finish before releasing the fence.
  schema-qualify-summary --transaction ID
      Read-only capacity decision from reconciled trials of one transaction.

  schema-snapshot-{plan,preflight,stage,status,reconcile} --request F --request-sha256 H
      [--expect-plan-sha256 H]  On the pinned coordinator, quiesce only the reviewed
      full-journal writer, copy its committed prefix to a private immutable snapshot
      and restore that writer. The snapshot qualifies no oracle or candidate gate.
  schema-candidate-execute-{plan,preflight,stage} --source-sha REV
      --mode artifact-verification|native-certificates --attempt N
      --preparation-request-sha256 H [--expect-plan-sha256 H]
  schema-candidate-execute-{status,reconcile} --source-sha REV --request-sha256 H
      From root's workstation (remote-lock inventory, pinned SSH, every host machine_id-pinned):
      under the coordinator lock, survey every host, then run the closed candidate shard-verify
      or all 180 native certificate segments and retain private raw evidence. Native exit zero
      is not a gate pass.

  txid-display-{plan,preflight,status,ingest-status} --request F --request-sha256 H
  txid-display-deploy --request F --request-sha256 H --expect-plan-sha256 H
      --phase stage|workers|route|controller
      [workers: --publication-map-sha256 H [--replace-active H]]
  txid-display-{ingest-start,bootstrap,verify,measure-start,retire} ... --expect-plan-sha256 H
  txid-display-{ingest-stop,measure-stop,stop} --request F --request-sha256 H
  txid-display-rollback --request F --request-sha256 H [--transaction ID]
      One txid display site beside history (transparent/ops/lib/txid_display_poc.py).
      Every mutation holds the production lock, journals prior bytes and prints the
      rollback command.

  transparent-binary-{plan,preflight,baseline,status} --request F --request-sha256 H
  transparent-binary-deploy --request F --request-sha256 H [--binary F] [--skip-exact-check]
  transparent-binary-rollback --request F --request-sha256 H [--transaction ID]
      Worker or controller binary upgrade (the request's component) of the current
      v12 history or 4k8 display fleet (transparent/ops/lib/binary_upgrade.py).
      Publication identity is pinned, current map and assignment come from the
      controller, all re-checked under the lock, after every restart and after
      rollback; history's continuous load is paused and re-pinned.

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


def load_release():
    from .. import release
    return release


def transparent_cli():
    from transparent.ops.lib import deploy_cli
    return deploy_cli


def target_binary(args, service, state_dir):
    """`(sha256, local binary or None, source record)` from exactly one of the source flags."""
    chosen = [flag for flag in ('binary', 'archive', 'sha256') if getattr(args, flag)]
    if len(chosen) != 1:
        raise DeployError('give exactly one of --binary, --archive (with --sha) or --sha256')
    if args.binary:
        path = Path(args.binary).resolve()
        return file_sha256(path), path, {'binary': str(path)}
    if args.sha256:
        if not SHA256.match(args.sha256):
            raise DeployError('--sha256 must be 64 lowercase hex digits')
        return args.sha256, None, {'sha256': args.sha256}
    if not args.sha:
        raise DeployError('--archive requires --sha, the full source revision')
    kind = args.kind or (service.artifact_kinds[0] if service.artifact_kinds else None)
    if kind not in service.artifact_kinds:
        raise DeployError('%s deploys artifact kinds %s' % (service.name, list(service.artifact_kinds)))
    work = Path(state_dir) / 'artifacts'
    work.mkdir(mode=0o700, parents=True, exist_ok=True)
    destination = Path(tempfile.mkdtemp(prefix='%s-%s-' % (kind, args.sha[:12]), dir=work)) / 'bundle'
    # Verifies the revision, the checksum inventory and every file's digest.
    load_release().extract(Path(args.archive), destination, args.sha, kind)
    path = destination / service.binary
    return file_sha256(path), path, {'archive': str(Path(args.archive).resolve()), 'revision': args.sha, 'kind': kind}


def artifact_groups(args, service, inventory):
    """Split the selected targets by the release artifact each one runs, in rollout order.

    A target whose inventory entry names an `artifact_kind` (a GPU worker's
    CUDA build) cannot take the transaction's binary. It is deployed in its own
    transaction from `--extra-archive KIND=FILE`, with the same `--sha`. Without
    that archive the command refuses, so a regular deploy never skips such a
    target silently; `--only` excludes it deliberately.
    Returns `[(args for that group, target keys)]`, one entry per transaction.
    """
    selected = descriptors.select(descriptors.targets(service, inventory), getattr(args, 'only', None))
    main_kind = (args.kind or (service.artifact_kinds[0] if service.artifact_kinds else None)) if args.archive else None
    extra = {}
    for item in getattr(args, 'extra_archive', None) or ():
        kind, sep, path = item.partition('=')
        if not sep or kind not in service.artifact_kinds or kind in extra or kind == main_kind:
            raise DeployError('--extra-archive is KIND=FILE, once per kind other than the main one; kinds are %s'
                              % list(service.artifact_kinds))
        extra[kind] = path
    groups = {}
    for target in selected:
        kind = target.artifact_kind if target.artifact_kind not in (None, main_kind) else None
        groups.setdefault(kind, []).append(target.key)
    missing = [k for k in groups if k is not None and k not in extra]
    if missing:
        raise DeployError('%s run the %s artifact: add --extra-archive %s=FILE with the same --sha, '
                          'or leave them out with --only'
                          % ([key for k in missing for key in groups[k]], missing[0], missing[0]))
    unused = set(extra) - set(groups)
    if unused:
        raise DeployError('no selected target runs %s' % sorted(unused))
    if any(k is not None for k in groups) and not args.sha:
        raise DeployError('--extra-archive requires --sha, the full source revision')
    order = [t.key for t in selected]
    result = []
    for kind, keys in sorted(groups.items(), key=lambda item: order.index(item[1][0])):
        group = argparse.Namespace(**vars(args))
        if kind is not None:
            group.binary, group.sha256, group.archive, group.kind = None, None, extra[kind], kind
        result.append((group, keys))
    return result


def run_release(command, args, service, inventory, executor, deployer, out):
    """Execute one artifact group through the shared lifecycle API."""
    from .lifecycle import Lifecycle
    sha, binary, source = target_binary(args, service, args.state_dir)
    lifecycle = Lifecycle(deployer)
    if command == 'deploy':
        result = lifecycle.deploy(sha, binary, source, allow_drift=args.allow_unit_drift,
                                  retire_historical=args.retire_historical,
                                  skip_exact_check=args.skip_exact_check)
    else:
        result = lifecycle.inspect(command, sha, binary, allow_drift=args.allow_unit_drift,
                                   retire_historical=args.retire_historical,
                                   stage=command == 'preflight' and args.stage)
    if args.json:
        out(json.dumps(result.describe(), sort_keys=True))
    return 1 if command == 'preflight' and result.problems else 0


def external_workers(args, service, inventory):
    """Whether a command touches only a product's role-mode external workers.

    Product services keep their recipes for publications and every other role;
    the shared engine may install, verify and roll back an external worker's
    rendered unit, which never carries publication authority.
    """
    from .transaction import Journal
    if args.command == 'rollback':
        journal = Journal.load(args.state_dir, service.name, args.transaction)
        return journal is not None and all(service.roles[r['role']].mode == 'role' for r in journal.hosts)
    only = getattr(args, 'only', None)
    if not only:
        return False
    selected = descriptors.select(descriptors.targets(service, inventory), only)
    return all(t.role.mode == 'role' for t in selected)


def parser():
    top = argparse.ArgumentParser(prog='wallet-pir-deploy.py', description=USAGE,
                                  formatter_class=argparse.RawDescriptionHelpFormatter)
    top.add_argument('--json', action='store_true', help='structured results for role lifecycle commands')
    top.add_argument('--inventory', default=os.environ.get('WALLET_PIR_DEPLOY_INVENTORY'))
    top.add_argument('--descriptors', default=str(DESCRIPTORS))
    top.add_argument('--state-dir', default=str(default_state_dir()))
    top.add_argument('--baseline', help='baseline file (default <state-dir>/baselines/<service>.json)')
    commands = top.add_subparsers(dest='command', required=True)
    command = commands.add_parser('fleet', help='read configured hosts and roles; makes no host connections')
    command.add_argument('--complete', action='store_true',
                         help='also require every registered service, role, responsibility and ancillary kind')
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
    for name in ('plan', 'preflight', 'deploy'):
        command = commands.add_parser(name)
        command.add_argument('service')
        command.add_argument('--binary', help='local binary to deploy')
        command.add_argument('--archive', help='release bundle from tools/ci/release.py')
        command.add_argument('--sha', help='full source revision of --archive')
        command.add_argument('--kind', help='artifact kind of --archive')
        command.add_argument('--extra-archive', action='append', metavar='KIND=FILE',
                             help='bundle for inventory targets that declare another artifact_kind '
                                  '(e.g. a GPU worker), deployed in their own transaction')
        command.add_argument('--sha256', help='binary digest only; for a no-op check or an already staged release')
        command.add_argument('--allow-unit-drift', action='store_true',
                             help='accept a new unit that differs from the live one beyond the binary')
        command.add_argument('--only', action='append', metavar='ROLE[@HOST]',
                             help='limit to these targets (repeatable), e.g. worker@worker-01')
        command.add_argument('--retire-historical', action='store_true',
                             help='move historical ExecStart-only drop-ins into the transaction directory')
        if name == 'preflight':
            command.add_argument('--stage', action='store_true',
                                 help='under the lock, upload the release directory and run its self-check')
        if name == 'deploy':
            command.add_argument('--skip-exact-check', action='store_true')
    command = commands.add_parser('rollback')
    command.add_argument('service')
    command.add_argument('--transaction')
    command.add_argument('--force', action='store_true', help='restore even if unit files changed after the transaction')
    command = commands.add_parser('worker-report', help='read-only private report of workers from their endpoints')
    command.add_argument('service')
    command.add_argument('--only', action='append', metavar='ROLE[@HOST]')
    for name in ('status', 'capture-baseline'):
        command = commands.add_parser(name)
        command.add_argument('service')
        if name == 'capture-baseline':
            command.add_argument('--output')
        command.add_argument('--only', action='append', metavar='ROLE[@HOST]',
                             help='only these targets (repeatable)')
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
    """`executor` and `options` (passed to `Deployer`) let tests substitute a fake fleet and clock."""
    args = parser().parse_args(argv)
    def error_result(message, phase='failed'):
        if args.json:
            out(json.dumps({'service': getattr(args, 'service', None), 'operation': args.command,
                            'targets': [], 'problems': [message], 'transaction': None, 'phase': phase}, sort_keys=True))
        else:
            out('error: ' + message)
    try:
        from ..bundle import check_installed
        product_command = args.command.startswith(('schema-', 'txid-display-', 'transparent-binary-', 'ops-package-'))
        mutation = (args.command in ('deploy', 'rollback') or getattr(args, 'stage', False)
                    or product_command and not args.command.endswith(('-plan', '-preflight', '-status', '-summary', '-recipe')))
        # A clean installed package may install a newer package revision; every
        # binary change still names the same source revision as the package.
        package_command = args.command.startswith('ops-package-')
        check_installed(mutation=mutation, revision=None if package_command else getattr(args, 'sha', None))
        if args.command.startswith(('schema-', 'txid-display-', 'transparent-binary-')):
            if args.json:
                raise DeployError('product recipes already provide their own JSON receipts; omit --json')
            return transparent_cli().dispatch(args, executor=executor, out=out, **options)
        if not args.inventory:
            raise DeployError('pass --inventory or set WALLET_PIR_DEPLOY_INVENTORY')
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
        log = (lambda message: None) if args.json else out
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
        if service.driver != 'binary' and args.command != 'status' and not external_workers(args, service, inventory):
            raise DeployError('%s uses the %s product recipes; ordinary binary deployment cannot change its publications'
                              % (service.name, service.driver))
        executor = executor or SSHExecutor(inventory)
        if args.command in ('plan', 'preflight', 'deploy'):
            groups = artifact_groups(args, service, inventory)
            for group, keys in groups:
                if len(groups) > 1:
                    label = (group.kind or 'default artifact') if group.archive else 'given binary'
                    log('== %s: %s' % (label, ', '.join(keys)))
                deployer = Deployer(service, inventory, executor, args.state_dir, args.baseline, out=log,
                                    only=keys, **options)
                code = run_release(args.command, group, service, inventory, executor, deployer, out)
                if code:
                    return code
            return 0
        deployer = Deployer(service, inventory, executor, args.state_dir, args.baseline, out=log,
                            only=getattr(args, 'only', None), **options)
        if args.command == 'capture-baseline':
            baseline = deployer.capture_baseline(args.output)
            if args.json:
                out(json.dumps({'service': service.name, 'operation': args.command,
                                'baseline': str(args.output or deployer.baseline_path),
                                'targets': sorted(baseline['targets'])}, sort_keys=True))
        elif args.command == 'status':
            from .lifecycle import Lifecycle
            result = Lifecycle(deployer).status()
            if args.json:
                out(json.dumps(result.describe(), sort_keys=True))
        elif args.command == 'rollback':
            from .lifecycle import Lifecycle
            result = Lifecycle(deployer).rollback(args.transaction, args.force)
            if args.json:
                out(json.dumps(result.describe(), sort_keys=True))
    except subprocess.TimeoutExpired:
        error_result('command timed out; reconcile the recorded transaction and inspect its private logs', 'unknown')
        return 75 if args.command.startswith('schema-') else 1
    except (DeployError, RemoteError, LockHeld, descriptors.DescriptorError, OSError, ValueError) as error:
        error_result(str(error))
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
