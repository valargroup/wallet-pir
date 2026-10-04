"""Command line for `ops/scripts/wallet-pir-deploy.py`."""
import argparse
import asyncio
import importlib.util
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

ROOT = Path(__file__).resolve().parents[4]
DESCRIPTORS = ROOT / 'enhance/ops/deploy/deploy.toml'
SHA256 = re.compile('^[0-9a-f]{64}$')

USAGE = """\
Transactional deploys of Enhance PIR and Status PIR over SSH.
Transparent schema recipes run on the pinned coordinator under the same lock.

  plan SERVICE (--binary F | --archive F --sha REV | --sha256 H)
      Read-only: per-target action, drop-in handling and unit drift.
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

The inventory (hosts, SSH, lock) is --inventory or WALLET_PIR_DEPLOY_INVENTORY.
"""


def load_release():
    spec = importlib.util.spec_from_file_location('release', ROOT / 'tools/ci/release.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


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


def parser():
    top = argparse.ArgumentParser(prog='wallet-pir-deploy.py', description=USAGE,
                                  formatter_class=argparse.RawDescriptionHelpFormatter)
    top.add_argument('--inventory', default=os.environ.get('WALLET_PIR_DEPLOY_INVENTORY'))
    top.add_argument('--descriptors', default=str(DESCRIPTORS))
    top.add_argument('--state-dir', default=str(default_state_dir()))
    top.add_argument('--baseline', help='baseline file (default <state-dir>/baselines/<service>.json)')
    commands = top.add_subparsers(dest='command', required=True)
    for kind in ('service', 'cutover'):
        for name in ('plan','preflight','stage','status','reconcile'):
            command = commands.add_parser('schema-input-'+kind+'-'+name, help='retain reviewed v11 inputs without installation')
            command.add_argument('--request', required=True)
            command.add_argument('--request-sha256', required=True)
            if name == 'stage': command.add_argument('--expect-plan-sha256', required=True)
    for name in ('plan','preflight','stage','status','reconcile'):
        command = commands.add_parser('schema-candidate-'+name, help='retain the verified changed-native candidate bundle without installation')
        command.add_argument('--request', required=True)
        command.add_argument('--request-sha256', required=True)
        if name == 'stage': command.add_argument('--expect-plan-sha256', required=True)
    for name in ('plan','preflight','stage','status','reconcile'):
        command = commands.add_parser('schema-candidate-upload-'+name, help='transfer the three pinned candidate archives to the coordinator')
        command.add_argument('--source-sha', required=True)
        if name in ('status','reconcile'):
            command.add_argument('--request-sha256', required=True)
            continue
        command.add_argument('--attempt', type=int, required=True)
        for kind in ('transparent-filter','transparent-publisher','supplemental'):
            command.add_argument('--'+kind, required=True, metavar='ARCHIVE')
        if name == 'stage': command.add_argument('--expect-plan-sha256', required=True)
    command = commands.add_parser('schema-candidate-receive', help=argparse.SUPPRESS)
    command.add_argument('--action', choices=('preflight','stage','status','reconcile'), required=True)
    command.add_argument('--request-sha256', required=True)
    command = commands.add_parser('schema-candidate-worker-build', help='render candidate worker executables for an already staged publication')
    command.add_argument('--host', required=True)
    command.add_argument('--source-sha', required=True)
    command.add_argument('--publication-request-sha256', required=True)
    command.add_argument('--attempt', type=int, default=1)
    for name in ('plan','preflight','run','status','reconcile'):
        command = commands.add_parser('schema-qualify-'+name, help='deployed candidate load/freshness/capacity/fault qualification')
        if name in ('plan','preflight','run'):
            command.add_argument('--request', required=True)
        command.add_argument('--request-sha256', required=True)
        if name == 'run': command.add_argument('--expect-plan-sha256', required=True)
    command = commands.add_parser('schema-qualify-summary', help='read-only capacity decision from reconciled trials')
    command.add_argument('--transaction', required=True)
    command = commands.add_parser('schema-qualify-exec', help=argparse.SUPPRESS)
    command.add_argument('--request-sha256', required=True)
    command = commands.add_parser('schema-qualify-remote', help=argparse.SUPPRESS)
    command.add_argument('--action', choices=('probe','act','status','reconcile'), required=True)
    command.add_argument('--request-sha256', required=True)
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
    for name in ('plan', 'preflight', 'stage', 'status'):
        command = commands.add_parser('schema-source-'+name)
        command.add_argument('--source-sha', required=True)
        command.add_argument('--sha256', required=True)
        command.add_argument('--host', help='stage another pinned host from the root coordinator under its production lock')
        command.add_argument('--recovery-transaction', help='inert source repair for this failed transaction only')
        command.add_argument('--recovery-recipe-sha256')
        if name != 'status':
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
    for name in ('plan', 'preflight', 'deploy'):
        command = commands.add_parser(name)
        command.add_argument('service')
        command.add_argument('--binary', help='local binary to deploy')
        command.add_argument('--archive', help='release bundle from tools/ci/release.py')
        command.add_argument('--sha', help='full source revision of --archive')
        command.add_argument('--kind', help='artifact kind of --archive')
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
    for name in ('status', 'capture-baseline'):
        command = commands.add_parser(name)
        command.add_argument('service')
        if name == 'capture-baseline':
            command.add_argument('--output')
    return top


def main(argv=None, executor=None, out=print, **options):
    """`executor` and `options` (passed to `Deployer`) let tests substitute a fake fleet and clock."""
    args = parser().parse_args(argv)
    try:
        if args.command.startswith('schema-lock-'):
            spec = importlib.util.spec_from_file_location('activity_lock_qualification', ROOT/'transparent/ops/lib/activity_lock_qualification.py')
            module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
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
            spec = importlib.util.spec_from_file_location('activity_input_stage', ROOT/'transparent/ops/lib/activity_input_stage.py')
            module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
            request = module.read_request(sys.stdin.buffer, args.request_sha256)
            receiver = module.Receiver(request); receiver.identity()
            try:
                result = receiver.stage(sys.stdin.buffer) if args.action == 'stage' else getattr(receiver,args.action)()
            except BaseException as error:
                out(json.dumps({'request_sha256':module.digest(request),'status':'interrupted' if isinstance(error,(subprocess.TimeoutExpired,KeyboardInterrupt,SystemExit)) else 'failed'}))
                return 75 if isinstance(error,(subprocess.TimeoutExpired,KeyboardInterrupt,SystemExit)) else 1
            out(json.dumps({k:v for k,v in result.items() if k != 'request'}, sort_keys=True))
            return 0
        if args.command == 'schema-candidate-receive':
            spec = importlib.util.spec_from_file_location('activity_candidate_upload', ROOT/'transparent/ops/lib/activity_candidate_upload.py')
            module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
            return module.receive(args.action, args.request_sha256, sys.stdin.fileno(), out)
        if args.command.startswith('schema-qualify-'):
            spec = importlib.util.spec_from_file_location('activity_deployed_qualification', ROOT/'transparent/ops/lib/activity_deployed_qualification.py')
            module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
            if args.command == 'schema-qualify-remote':
                data = sys.stdin.buffer.read(8193)
                if len(data) > 8192: raise ValueError('remote qualification request exceeds bound')
                request = module.remote_validate(json.loads(data))
                if module.digest(request) != args.request_sha256: raise ValueError('remote qualification request checksum differs')
                out(json.dumps(module.remote_run(request, args.action), sort_keys=True))
                return 0
            if args.command == 'schema-qualify-summary':
                out(json.dumps(module.summary_report(args.transaction), sort_keys=True))
                return 0
            if not args.inventory: raise ValueError('qualification requires the pinned coordinator inventory')
            inventory = descriptors.load_inventory(args.inventory)
            if args.command in ('schema-qualify-plan','schema-qualify-preflight','schema-qualify-run'):
                with Path(args.request).open('rb') as stream: request = module.read_request(stream, args.request_sha256)
            else:
                request = module.retained_request(args.request_sha256)
            qualification = module.Qualification(inventory, request, args.request_sha256)
            if args.command == 'schema-qualify-exec':
                qualification.execute()
                return 0
            action = args.command.removeprefix('schema-qualify-')
            result = qualification.run(action, getattr(args,'expect_plan_sha256',None))
            out(json.dumps(result, sort_keys=True))
            if action == 'plan':
                out('plan sha256: '+module.digest(result))
            return 0
        if args.command.startswith('schema-host-'):
            spec = importlib.util.spec_from_file_location('activity_schema_dispatch', ROOT/'transparent/ops/lib/activity_schema_dispatch.py')
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
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
        if args.command.startswith('schema-candidate-upload-'):
            spec = importlib.util.spec_from_file_location('activity_candidate_upload', ROOT/'transparent/ops/lib/activity_candidate_upload.py')
            module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
            action = args.command.removeprefix('schema-candidate-upload-')
            archives = ({kind:getattr(args, kind.replace('-','_')) for kind in module.ORDER}
                        if action not in ('status','reconcile') else None)
            upload = module.Upload(descriptors.load_inventory(args.inventory), args.source_sha,
                                   attempt=getattr(args,'attempt',None), archives=archives,
                                   request_sha256=getattr(args,'request_sha256',None))
            try:
                result = upload.run(action, getattr(args,'expect_plan_sha256',None))
            except module.Unknown as error:
                out('error: outcome unknown: %s' % error)
                return 75
            out(json.dumps(result,sort_keys=True))
            if action == 'plan':
                out('plan sha256: '+module.digest(result))
            return 0
        if args.command.startswith('schema-candidate-'):
            spec = importlib.util.spec_from_file_location('activity_input_stage', ROOT/'transparent/ops/lib/activity_input_stage.py')
            module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
            inventory = descriptors.load_inventory(args.inventory)
            if args.command == 'schema-candidate-worker-build':
                result = module.build_candidate(inventory,args.host,args.source_sha,args.publication_request_sha256,args.attempt)
            else:
                with Path(args.request).open('rb') as stream: raw=stream.read(module.MAX_REQUEST+1)
                if len(raw)>module.MAX_REQUEST: raise ValueError('candidate request exceeds bound')
                request=json.loads(raw,object_pairs_hook=module.unique)
                result=module.CandidatePreparation(inventory,request,args.request_sha256).run(
                    args.command.removeprefix('schema-candidate-'),getattr(args,'expect_plan_sha256',None))
            out(json.dumps(result,sort_keys=True))
            return 0
        if args.command.startswith('schema-input-'):
            spec = importlib.util.spec_from_file_location('activity_input_stage', ROOT/'transparent/ops/lib/activity_input_stage.py')
            module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
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
            spec = importlib.util.spec_from_file_location('activity_schema_product', ROOT/'transparent/ops/lib/activity_schema_product.py')
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
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
            spec = importlib.util.spec_from_file_location('activity_publication_job',
                ROOT/'transparent/ops/lib/activity_publication_job.py')
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
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
            spec = importlib.util.spec_from_file_location('activity_source_stage',
                ROOT/'transparent/ops/lib/activity_source_stage.py')
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            inventory = descriptors.load_inventory(args.inventory)
            if bool(args.recovery_transaction) != bool(args.recovery_recipe_sha256):
                raise ValueError('recovery source staging needs both transaction and recipe identity')
            recovery = ({'transaction':args.recovery_transaction,'recipe_sha256':args.recovery_recipe_sha256}
                        if args.recovery_transaction else None)
            module.SourceStage(inventory, out, target=args.host, recovery=recovery).run(args.command.removeprefix('schema-source-'),
                args.source_sha, args.sha256, getattr(args, 'archive', None))
            return 0
        if args.command.startswith('schema-'):
            spec = importlib.util.spec_from_file_location('activity_schema_operation',
                ROOT/'transparent/ops/lib/activity_schema_operation.py')
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
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
                            command = [sys.executable, str(ROOT/'ops/scripts/wallet-pir-deploy.py'),
                                '--inventory', args.inventory, '--state-dir', args.state_dir,
                                'schema-rollback', '--transaction', runner.active_id]
                            out('rollback: '+shlex.join(command))
            return 0
        services = descriptors.load_descriptors(args.descriptors)
        if args.service not in services:
            raise DeployError('unknown service %r; known: %s' % (args.service, sorted(services)))
        service = services[args.service]
        inventory = descriptors.load_inventory(args.inventory)
        executor = executor or SSHExecutor(inventory)
        deployer = Deployer(service, inventory, executor, args.state_dir, args.baseline, out=out,
                            only=getattr(args, 'only', None), **options)
        if args.command == 'capture-baseline':
            deployer.capture_baseline(args.output)
        elif args.command == 'status':
            deployer.status()
        elif args.command == 'rollback':
            deployer.rollback(args.transaction, args.force)
        elif args.command == 'deploy':
            sha, binary, source = target_binary(args, service, args.state_dir)
            deployer.deploy(sha, binary, source, args.allow_unit_drift, args.retire_historical, args.skip_exact_check)
        else:
            sha, binary, _ = target_binary(args, service, args.state_dir)
            deployer.check_identities([inventory.lock.get('host')])
            if args.command == 'preflight' and args.stage:
                with deployer.lock_factory() as held:
                    deployer.lock = held
                    try:
                        deployer.schema_fence()
                        plans, _ = deployer.assess(sha, binary, args.allow_unit_drift, args.retire_historical,
                                                   require_baseline=False)
                        deployer.stage(list(dict.fromkeys(p.target.host for p in plans if p.action == 'restart')),
                                       sha, binary)
                    finally:
                        deployer.lock = None
            plans, problems = deployer.assess(sha, binary, args.allow_unit_drift, args.retire_historical,
                                              require_baseline=args.command == 'preflight')
            deployer.describe(plans, sha)
            if args.command == 'preflight':
                for host in dict.fromkeys(p.target.host for p in plans if p.action == 'restart'):
                    path = service.release_binary(sha)
                    if executor.sha256(host, path) != sha:
                        out('%s: release not staged yet; deploy uploads it' % host)
                        continue
                    code, output = executor.run(host, [path, *service.self_check], 60)
                    if code:
                        problems.append('%s: staged binary failed its self-check (exit %d): %s' % (host, code, output[-300:]))
                    else:
                        out('%s: staged release runs' % host)
            for problem in problems:
                out('problem: ' + problem)
            if problems and args.command == 'preflight':
                return 1
            out('%d target(s) to restart, %d unchanged' % (sum(p.action == 'restart' for p in plans),
                                                           sum(p.action == 'skip' for p in plans)))
    except subprocess.TimeoutExpired:
        out('error: command timed out; reconcile the recorded transaction and inspect its private logs')
        return 75 if args.command.startswith('schema-') else 1
    except (DeployError, RemoteError, LockHeld, descriptors.DescriptorError, OSError, ValueError) as error:
        out('error: %s' % error)
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
