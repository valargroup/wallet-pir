"""Transparent cutover, display and qualification adapters for the deploy CLI.

The product owns these command families and their existing recovery journals.
Ordinary product imports are distinct from explicitly pinned source validation.
"""
import argparse
import asyncio
from importlib import import_module
import json
from pathlib import Path
import shlex
import subprocess
import sys

from wallet_pir_ops.paths import ROOT
from wallet_pir_ops.deploy import descriptors
from wallet_pir_ops.deploy.engine import DeployError

TXID_DISPLAY = ('plan', 'preflight', 'deploy', 'status', 'stop', 'rollback', 'retire', 'ingest-start',
                'ingest-status', 'ingest-stop', 'bootstrap', 'verify', 'measure-start', 'measure-stop')
TXID_DISPLAY_PHASES = ('stage', 'workers', 'route', 'controller')
BINARY_UPGRADE = ('plan', 'preflight', 'baseline', 'deploy', 'status', 'rollback')
ALERT_ACTIVATION = ('plan', 'preflight', 'deploy', 'status', 'rollback', 'test')
LOAD_REPAIR = ('plan', 'preflight', 'deploy', 'status', 'rollback')


def register(commands):
    for kind in ('service', 'cutover'):
        for name in ('plan','preflight','stage','status','reconcile'):
            command = commands.add_parser('schema-input-'+kind+'-'+name, help='retain reviewed v11 inputs without installation')
            command.add_argument('--request', required=True)
            command.add_argument('--request-sha256', required=True)
            if name == 'stage': command.add_argument('--expect-plan-sha256', required=True)
    for name in ('plan','preflight','stage','status','reconcile'):
        command = commands.add_parser('schema-input-prepare-'+name, help='prepare native assignment and immutable worker units on coordinator')
        command.add_argument('--request', required=True)
        command.add_argument('--request-sha256', required=True)
        if name == 'stage': command.add_argument('--expect-plan-sha256', required=True)
    for name in ('plan','preflight','stage','status','reconcile'):
        command = commands.add_parser('schema-input-'+name, help='immutable v11 worker input staging on pinned coordinator')
        command.add_argument('--request', required=True)
        command.add_argument('--request-sha256', required=True)
        command.add_argument('--host', required=True)
    command = commands.add_parser('schema-input-build', help='render the native-assigned worker input request after full publication')
    command.add_argument('--host', required=True)
    command.add_argument('--source-sha', required=True)
    command.add_argument('--assignment', required=True)
    command.add_argument('--unit', required=True)
    command.add_argument('--worker-id', required=True)
    command.add_argument('--release-result-sha256', required=True)
    command.add_argument('--cache-bytes', type=int, required=True)
    command.add_argument('--attempt', type=int, default=1)
    command = commands.add_parser('schema-input-receive', help=argparse.SUPPRESS)
    command.add_argument('--action', choices=('preflight','stage','status','reconcile'), required=True)
    command.add_argument('--request-sha256', required=True)
    for name in ('plan','preflight','qualify','status','reconcile','relay'):
        command = commands.add_parser('schema-lock-'+name, help='bounded SSH descendant lock qualification' if name != 'relay' else argparse.SUPPRESS)
        command.add_argument('--source-sha', required=True)
        command.add_argument('--host', required=True)
        command.add_argument('--attempt', type=int, default=1)
        if name == 'qualify': command.add_argument('--expect-plan-sha256', required=True)
    command = commands.add_parser('schema-lock-remote', help=argparse.SUPPRESS)
    command.add_argument('--action', choices=('preflight','parent','child','status','probe','reconcile'), required=True)
    command.add_argument('--request-sha256', required=True)
    for name in ('run', 'status', 'reconcile'):
        command = commands.add_parser('schema-host-'+name, help='pinned remote host owner; invoked by the coordinator schema recipe')
        command.add_argument('--request-sha256', required=True)
    for name in ('recipe', 'preflight', 'phase'):
        command = commands.add_parser('schema-product-'+name, help='complete reviewed product service orchestration')
        command.add_argument('--spec', required=True)
        command.add_argument('--spec-sha256', required=True)
        if name == 'phase':
            command.add_argument('--phase', required=True)
            command.add_argument('--transaction', required=True)
            command.add_argument('--journal', required=True)
    for name in ('plan', 'preflight', 'start', 'status', 'run'):
        command = commands.add_parser('schema-publication-'+name,
            help='fixed full v11 preparation job' if name != 'run' else argparse.SUPPRESS)
        command.add_argument('--source-sha', required=True)
        command.add_argument('--release-result-sha256', required=True)
        if name == 'start':
            command.add_argument('--expect-plan-sha256', required=True)
    for name in ('plan', 'preflight', 'stage', 'status','reconcile'):
        command = commands.add_parser('schema-source-'+name)
        command.add_argument('--source-sha', required=True)
        command.add_argument('--attempt',type=int,default=1)
        command.add_argument('--sha256', required=True)
        command.add_argument('--host', help='stage another pinned host from the root coordinator under its production lock')
        command.add_argument('--recovery-transaction', help='inert source repair for this failed transaction only')
        command.add_argument('--recovery-recipe-sha256')
        if name == 'preflight':
            command.add_argument('--private-evidence-file', help='exclusive local private file for exact fleet replies, including refusal')
        if name not in ('status','reconcile'):
            command.add_argument('--archive', required=True)
    for name in ('schema-plan', 'schema-preflight', 'schema-deploy'):
        command = commands.add_parser(name, help='Transparent multi-component schema transaction')
        command.add_argument('--recipe', required=True, help='reviewed coordinator-only cutover recipe; no credentials')
        if name == 'schema-deploy':
            command.add_argument('--expect-recipe-sha256', required=True)
    for name in ('schema-rollback', 'schema-status'):
        command = commands.add_parser(name)
        command.add_argument('--transaction')
    command = commands.add_parser('schema-repair-rollback', help='reviewed source repair of a failed product rollback')
    command.add_argument('--transaction', required=True)
    command.add_argument('--expect-recipe-sha256', required=True)
    for name in ('plan','preflight','deploy','resume-plan','resume-preflight','resume-deploy','resume-prepare-plan','resume-prepare-preflight','resume-prepare-deploy','resume-prepare-reconcile'):
        command = commands.add_parser('schema-reconcile-'+name, help='recover a retained newer v10 activation after an unstaged cutover')
        command.add_argument('--transaction', required=True)
        command.add_argument('--expect-recipe-sha256', required=True)
        command.add_argument('--map-sha256', required=True, help='map digest, or original adoption plan digest for resume')
        if name in ('deploy','resume-deploy','resume-prepare-deploy'):
            command.add_argument('--expect-plan-sha256', required=True)
    for name in TXID_DISPLAY:
        command = commands.add_parser('txid-display-' + name, help='txid display proof of concept beside history')
        command.add_argument('--request', required=True)
        command.add_argument('--request-sha256', required=True)
        if name in ('deploy', 'ingest-start', 'bootstrap', 'verify', 'measure-start', 'retire'):
            command.add_argument('--expect-plan-sha256', required=True)
        if name == 'deploy':
            command.add_argument('--phase', choices=TXID_DISPLAY_PHASES, required=True)
            command.add_argument('--archive', help='stage: the CI transparent-txid-display bundle')
            command.add_argument('--publication-map-sha256', help='workers: the bootstrap candidate map')
            command.add_argument('--replace-active',
                                 help='workers: the map of the lineage a fresh one replaces on each worker')
        elif name == 'plan':
            command.add_argument('--render-dir', help='also write every rendered file here for review')
        elif name == 'rollback':
            command.add_argument('--transaction')
        elif name == 'ingest-start':
            command.add_argument('--smoke', action='store_true', help='ten blocks into a separate directory')
        elif name == 'bootstrap':
            command.add_argument('--start-height', type=int)
            command.add_argument('--through-height', type=int)

    for name in BINARY_UPGRADE:
        command = commands.add_parser('transparent-binary-' + name,
                                      help='worker or controller binary upgrade of the current v12 history or 4k8 display fleet')
        command.add_argument('--request', required=True)
        command.add_argument('--request-sha256', required=True)
        command.add_argument('--staging-adapter', help='checksum-bound History staging adapter for an explicit controller update')
        if name in ('preflight', 'deploy'):
            command.add_argument('--binary', help="local binary of the request's component; omit when already staged")
        if name == 'preflight':
            command.add_argument('--stage', action='store_true',
                                 help='under the lock, upload the immutable release directory and run its self-check')
        if name == 'deploy':
            command.add_argument('--skip-exact-check', action='store_true')
        if name == 'rollback':
            command.add_argument('--transaction')
            command.add_argument('--force', action='store_true')

    for name in ALERT_ACTIVATION:
        command = commands.add_parser('transparent-alerts-' + name,
                                      help='locked pir-apm upgrade and Transparent/txid/monitor alert-mode activation')
        command.add_argument('--request', required=True)
        command.add_argument('--request-sha256', required=True)
        if name in ('preflight', 'deploy', 'test'):
            command.add_argument('--binary', help="local pir-apm binary of the request's release; omit when already staged")
        if name == 'rollback':
            command.add_argument('--transaction')
            command.add_argument('--force', action='store_true')
        if name == 'test':
            command.add_argument('--component', choices=('apm', 'monitor'), required=True)

    for name in LOAD_REPAIR:
        command = commands.add_parser('transparent-load-repair-' + name,
                                      help='locked reinstall of the stopped History continuous load script')
        command.add_argument('--request', required=True)
        command.add_argument('--request-sha256', required=True)
        if name == 'deploy':
            command.add_argument('--expect-plan-sha256', required=True)
        if name == 'rollback':
            command.add_argument('--transaction')
            command.add_argument('--force', action='store_true')


def dispatch(args, executor=None, out=print, **options):
    if args.command.startswith('transparent-binary-'):
        return binary_upgrade(args, executor, out, **options)
    if args.command.startswith('transparent-alerts-'):
        return alert_activation(args, executor, out, **options)
    if args.command.startswith('transparent-load-repair-'):
        return load_repair(args, executor, out, **options)
    if args.command.startswith('schema-lock-'):
        module = import_module('tools.transparent.lib.activity_lock_qualification')
        if args.command == 'schema-lock-remote':
            data = sys.stdin.buffer.read(8193)
            if len(data) > 8192: raise ValueError('qualification request exceeds bound')
            request = module.validate(json.loads(data))
            if module.durable.digest(request) != args.request_sha256: raise ValueError('qualification request checksum differs')
            result = module.remote_run(request,args.action)
        else:
            if not args.inventory: raise ValueError('qualification requires retained coordinator inventory')
            result = module.Client(descriptors.load_inventory(args.inventory),args.source_sha,args.host,args.attempt,args.inventory).run(
                args.command.removeprefix('schema-lock-'),getattr(args,'expect_plan_sha256',None))
        out(json.dumps(result,sort_keys=True))
        return 0
    if args.command == 'schema-input-receive':
        module = import_module('tools.transparent.lib.activity_input_stage')
        request = module.read_request(sys.stdin.buffer, args.request_sha256)
        receiver = module.Receiver(request); receiver.identity()
        try:
            result = receiver.stage(sys.stdin.buffer) if args.action == 'stage' else getattr(receiver,args.action)()
        except BaseException as error:
            out(json.dumps({'request_sha256':module.digest(request),'status':'interrupted' if isinstance(error,(subprocess.TimeoutExpired,KeyboardInterrupt,SystemExit)) else 'failed'}))
            return 75 if isinstance(error,(subprocess.TimeoutExpired,KeyboardInterrupt,SystemExit)) else 1
        out(json.dumps({k:v for k,v in result.items() if k != 'request'}, sort_keys=True))
        return 0
    if args.command.startswith('schema-host-'):
        module = import_module('tools.transparent.lib.activity_schema_dispatch')
        request = module.read_request(sys.stdin.buffer, args.request_sha256)
        actor = module.Actor(request)
        actor.identity()
        action = args.command.removeprefix('schema-host-')
        try:
            result = getattr(actor, action)()
        except Exception as error:
            result = actor.status()
            status = result['status'] if result['status'] != 'absent' else 'failed'
            out(json.dumps({'request_sha256':module.digest(request), 'status':status, 'result':None}))
            return 75 if isinstance(error, subprocess.TimeoutExpired) else 1
        # Private plans/baseline bytes never travel in the printed reply.
        if action == 'run' and request['action'] in module.READ_ONLY:
            result = {'status':'passed', 'result':result}
        out(json.dumps({'request_sha256':module.digest(request), 'status':result['status'], 'result':result.get('result')}))
        return 0
    if not args.inventory:
        raise DeployError('pass --inventory or set WALLET_PIR_DEPLOY_INVENTORY')
    if args.command.startswith('txid-display-'):
        module = import_module('tools.txid.lib.txid_display_poc')
        return module.main(args, descriptors.load_inventory(args.inventory), out, executor=executor, **options)
    if args.command.startswith('schema-input-'):
        module = import_module('tools.transparent.lib.activity_input_stage')
        inventory = descriptors.load_inventory(args.inventory)
        if args.command.startswith(('schema-input-prepare-', 'schema-input-service-', 'schema-input-cutover-')):
            with Path(args.request).open('rb') as stream: raw=stream.read(module.MAX_REQUEST+1)
            if len(raw)>module.MAX_REQUEST: raise ValueError('preparation request exceeds bound')
            request=json.loads(raw,object_pairs_hook=module.unique)
            kind=args.command.split('-')[2]
            preparation={'prepare':module.Preparation,'service':module.ServicePreparation,'cutover':module.CutoverPreparation}[kind]
            result=preparation(inventory,request,args.request_sha256).run(
                args.command.removeprefix('schema-input-'+kind+'-'),getattr(args,'expect_plan_sha256',None))
        elif args.command == 'schema-input-build':
            result = module.build(inventory,args.host,args.source_sha,args.assignment,args.unit,
                                  args.release_result_sha256,args.cache_bytes,args.attempt,worker_id=args.worker_id)
        else:
            with Path(args.request).open('rb') as stream:
                request = module.read_request(stream,args.request_sha256)
                if stream.read(1): raise ValueError('extra input request bytes')
            result = module.Client(inventory,args.host,request).run(args.command.removeprefix('schema-input-'))
        out(json.dumps(result,sort_keys=True))
        return 0
    if args.command.startswith('schema-product-'):
        module = import_module('tools.transparent.lib.activity_schema_product')
        path = module.checked({'path':args.spec, 'sha256':args.spec_sha256})
        specification = module.validate(module.H.load(path))
        product = module.Product(specification, spec_sha256=args.spec_sha256)
        product.source_identity(ROOT, args.transaction if args.command == 'schema-product-phase' else None,
                                args.phase if args.command == 'schema-product-phase' else None,
                                args.journal if args.command == 'schema-product-phase' else None)
        if args.command == 'schema-product-recipe':
            out(json.dumps(module.recipe(path, args.spec_sha256), sort_keys=True))
            return 0
        if args.command == 'schema-product-preflight':
            result = product.preflight()
        else:
            result = asyncio.run(product.phase(args.transaction, args.phase, args.journal))
        out(json.dumps(result, sort_keys=True))
        return 0
    if args.command.startswith('schema-publication-'):
        module = import_module('tools.transparent.lib.activity_publication_job')
        job = module.PublicationJob(descriptors.load_inventory(args.inventory), args.source_sha,
                                    args.release_result_sha256, out)
        action = args.command.removeprefix('schema-publication-')
        if action == 'start':
            job.start(args.expect_plan_sha256)
        elif action == 'preflight':
            job.preflight()
            out('full publication preflight passed')
        else:
            getattr(job, action)()
        return 0
    if args.command.startswith('schema-source-'):
        module = import_module('tools.transparent.lib.activity_source_stage')
        inventory = descriptors.load_inventory(args.inventory)
        if bool(args.recovery_transaction) != bool(args.recovery_recipe_sha256):
            raise ValueError('recovery source staging needs both transaction and recipe identity')
        recovery = ({'transaction':args.recovery_transaction,'recipe_sha256':args.recovery_recipe_sha256}
                    if args.recovery_transaction else None)
        try:
            module.SourceStage(inventory, out, target=args.host, recovery=recovery,attempt=args.attempt,
                private_evidence_file=getattr(args,'private_evidence_file',None)).run(args.command.removeprefix('schema-source-'),
                args.source_sha, args.sha256, getattr(args, 'archive', None))
        except module.Unknown as error:
            out(str(error))
            return 75
        return 0
    if args.command.startswith('schema-'):
        module = import_module('tools.transparent.lib.activity_schema_operation')
        inventory = descriptors.load_inventory(args.inventory)
        runner = module.Runner(inventory, args.state_dir, out=out)
        if args.command == 'schema-status':
            runner.status(args.transaction)
        elif args.command == 'schema-rollback':
            runner.rollback(args.transaction)
        elif args.command == 'schema-repair-rollback':
            runner.repair_rollback(args.transaction, args.expect_recipe_sha256)
        elif args.command.startswith('schema-reconcile-'):
            runner.reconcile(args.command.removeprefix('schema-reconcile-'), args.transaction,
                             args.expect_recipe_sha256, args.map_sha256, getattr(args,'expect_plan_sha256',None))
        else:
            recipe = module.load_recipe(args.recipe)
            if args.command == 'schema-plan':
                runner.plan(recipe)
            elif args.command == 'schema-preflight':
                runner.preflight(recipe)
            else:
                try:
                    runner.deploy(recipe, args.expect_recipe_sha256)
                finally:
                    if runner.active_id:
                        runner.status(runner.active_id)
                        command = [sys.executable, str(ROOT/'tools/scripts/wallet-pir-deploy.py'),
                            '--inventory', args.inventory, '--state-dir', args.state_dir,
                            'schema-rollback', '--transaction', runner.active_id]
                        out('rollback: '+shlex.join(command))
        return 0
    raise DeployError("unknown Transparent operation: " + args.command)


def alert_activation(args, executor, out, lock=None, sleep=None, clock=None, **_options):
    """`transparent-alerts-*`: the locked APM and monitor alert activation."""
    from wallet_pir_ops.bundle import check_installed
    from wallet_pir_ops.deploy.remote import SSHExecutor
    module = import_module('tools.transparent.lib.alert_activation')
    if not args.inventory:
        raise DeployError('pass --inventory or set WALLET_PIR_DEPLOY_INVENTORY')
    request = module.load_request(args.request, args.request_sha256)
    if not args.command.endswith(('-plan', '-preflight', '-status')):
        check_installed(mutation=True, revision=request['apm']['release']['git_sha'])
    inventory = descriptors.load_inventory(args.inventory)
    timing = {k: v for k, v in (('sleep', sleep), ('clock', clock)) if v is not None}
    return module.run(args, executor or SSHExecutor(inventory), out, inventory, lock=lock, **timing)


def load_repair(args, executor, out, **options):
    """`transparent-load-repair-*`: the locked continuous load script repair."""
    from wallet_pir_ops.bundle import check_installed
    from wallet_pir_ops.deploy.remote import SSHExecutor
    module = import_module('tools.transparent.lib.load_repair')
    if not args.inventory:
        raise DeployError('pass --inventory or set WALLET_PIR_DEPLOY_INVENTORY')
    request = module.load_request(args.request, args.request_sha256)
    if args.command.endswith('-deploy'):
        # The installed script is this operations release's own copy.
        check_installed(mutation=True, revision=request['script']['git_sha'])
    inventory = descriptors.load_inventory(args.inventory)
    return module.run(args, executor or SSHExecutor(inventory), out, inventory, **options)


def binary_upgrade(args, executor, out, **options):
    """`transparent-binary-*`: the shared engine on the request's component with product refusals."""
    from wallet_pir_ops.bundle import check_installed
    from wallet_pir_ops.deploy.engine import file_sha256
    from wallet_pir_ops.deploy.lifecycle import Lifecycle
    from wallet_pir_ops.deploy.remote import SSHExecutor
    module = import_module('tools.transparent.lib.binary_upgrade')
    action = args.command.removeprefix('transparent-binary-')
    if not args.inventory:
        raise DeployError('pass --inventory or set WALLET_PIR_DEPLOY_INVENTORY')
    request = module.load_request(args.request, args.request_sha256)
    release = request['release']
    if action in ('deploy', 'rollback') or getattr(args, 'stage', False):
        # Every binary change names the installed operations package's revision.
        check_installed(mutation=True, revision=release['git_sha'])
    services = descriptors.load_descriptors(args.descriptors)
    inventory = descriptors.load_inventory(args.inventory)
    executor = executor or SSHExecutor(inventory)
    upgrade = module.Upgrade(request, services[request['service']], inventory, services.get('publisher'))
    targets, component = upgrade.component(executor)
    deployer = module.UpgradeDeployer(upgrade, action, component, inventory, executor, args.state_dir, args.baseline,
                                      out=out, **options)
    deployer.staging_adapter = getattr(args, 'staging_adapter', None)
    # The baseline records both components, so either transaction is covered;
    # a unified publisher is not a component of this transaction.
    both = upgrade.workers + ([] if upgrade.unified else [upgrade.controller])
    deployer.targets = both if action == 'baseline' else targets
    workers = upgrade.workers
    sha, binary = release['binary_sha256'], getattr(args, 'binary', None)
    if binary and file_sha256(Path(binary).resolve()) != sha:
        raise DeployError('--binary does not hash to release.binary_sha256')
    out('release: %s git %s binary %s as %s; publication: %s'
        % (request['component'], release['git_sha'], sha, component.release_binary(sha),
           json.dumps(request['publication'], sort_keys=True)))
    if action == 'status':
        Lifecycle(deployer).status()
        problems = upgrade.problems(executor, workers) + upgrade.load_problems(executor, check_pin=False)
    elif action == 'baseline':
        deployer.capture_baseline()
        problems = []
    elif action == 'rollback':
        Lifecycle(deployer).rollback(args.transaction, args.force)
        problems = upgrade.problems(executor, workers) + upgrade.load_problems(executor, check_pin=False)
    elif action == 'deploy':
        Lifecycle(deployer).deploy(sha, binary, {'request_sha256': args.request_sha256, 'git_sha': release['git_sha']},
                                   skip_exact_check=args.skip_exact_check)
        problems = upgrade.problems(executor, workers) + upgrade.load_problems(executor, check_pin=False)
    else:
        result = Lifecycle(deployer).inspect(action, sha, binary, stage=getattr(args, 'stage', False))
        problems = (list(result.problems) + upgrade.problems(executor, workers)
                    + upgrade.load_problems(executor, check_pin=True))
    if action in ('plan', 'preflight', 'status'):
        for line in module.notification_state(executor, request['controller']['host'], request['service']):
            out(line)
    for problem in problems:
        out('problem: ' + problem)
    if action in ('plan', 'preflight', 'deploy'):
        out('rollback: ' + shlex.join([sys.executable, str(ROOT / 'tools/scripts/wallet-pir-deploy.py'), '--inventory',
                                       args.inventory, '--state-dir', args.state_dir, 'transparent-binary-rollback',
                                       '--request', args.request, '--request-sha256', args.request_sha256]))
    return 1 if problems else 0
