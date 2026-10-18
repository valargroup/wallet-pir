"""Structured lifecycle results over the existing transaction engine.

Commands and a future fleet frontend share this interface. It does not replace
product publication state machines or change their authority and rollback rules.
"""
from dataclasses import asdict, dataclass

from .engine import CHANGING, Deployer


@dataclass(frozen=True)
class OperationResult:
    service: str
    operation: str
    targets: tuple
    problems: tuple = ()
    transaction: str = None
    phase: str = None
    binary_sha256: str = None

    def describe(self):
        return asdict(self)


class Lifecycle:
    def __init__(self, deployer: Deployer):
        self.deployer = deployer

    def inspect(self, operation, sha, binary=None, *, allow_drift=False, retire_historical=False, stage=False):
        if operation not in ('plan', 'preflight') or (stage and operation != 'preflight'):
            raise ValueError('inspection must be plan or preflight; only preflight may stage')
        d = self.deployer
        d.check_identities([d.inventory.lock.get('host')])
        if stage:
            with d.lock_factory() as held:
                d.lock = held
                try:
                    d.schema_fence()
                    plans, problems = d.assess(sha, binary, allow_drift, retire_historical, require_baseline=True)
                    # A staging preflight must honor refusals before any upload.
                    if problems:
                        return self._inspection(operation, plans, problems, sha)
                    d.stage(list(dict.fromkeys(p.target.host for p in plans if p.action in CHANGING)), sha, binary)
                finally:
                    d.lock = None
        plans, problems = d.assess(sha, binary, allow_drift, retire_historical,
                                   require_baseline=operation == 'preflight')
        d.describe(plans, sha)
        if operation == 'preflight':
            for host in dict.fromkeys(p.target.host for p in plans if p.action in CHANGING):
                path = d.service.release_binary(sha)
                if d.ex.sha256(host, path) != sha:
                    d.out('%s: release not staged yet; deploy uploads it' % host)
                    continue
                code, output = d.ex.run(host, [path, *d.service.self_check], 60)
                if code:
                    problems.append('%s: staged binary failed its self-check (exit %d): %s' % (host, code, output[-300:]))
                else:
                    d.out('%s: staged release runs' % host)
            for plan in plans:
                # The product's own loader, over configuration already in place;
                # rendered configuration not yet installed is checked by deploy,
                # from a staged copy, before any unit changes.
                if plan.action not in CHANGING or not plan.check:
                    continue
                if plan.target.role.guard is not None:
                    d.out('%s: product check runs at deploy, in disposable state' % plan.target.key)
                    continue
                pending = [c['path'] for c in plan.changes
                           if c['op'] == 'write' and not c['path'].startswith('/etc/systemd/')]
                release = d.service.release_binary(sha)
                if pending or d.ex.sha256(plan.target.host, release) != sha:
                    d.out('%s: product check runs at deploy, against staged configuration' % plan.target.key)
                    continue
                code, output = d.ex.run(plan.target.host, [release, *plan.check], 900)
                if code:
                    problems.append('%s: product check refused its input (exit %d): %s'
                                    % (plan.target.key, code, output[-300:]))
                else:
                    d.out('%s: product check passed' % plan.target.key)
        for problem in problems:
            d.out('problem: ' + problem)
        d.out('%d target(s) to restart or install, %d unchanged' % (sum(p.action in CHANGING for p in plans),
                                                       sum(p.action == 'skip' for p in plans)))
        return self._inspection(operation, plans, problems, sha)

    def _inspection(self, operation, plans, problems, sha):
        return OperationResult(self.deployer.service.name, operation, tuple({
            'role': p.target.role.name, 'host': p.target.host, 'unit': p.target.unit,
            'action': p.action, 'drift': p.drift, 'refusals': p.refusals,
            'changes': [{'operation': c['op'], 'path': c['path']} for c in p.changes]
        } for p in plans), tuple(problems), binary_sha256=sha)

    def deploy(self, sha, binary=None, source=None, *, allow_drift=False, retire_historical=False, skip_exact_check=False):
        journal = self.deployer.deploy(sha, binary, source, allow_drift, retire_historical, skip_exact_check)
        return self._journal('deploy', journal, sha)

    def rollback(self, transaction=None, force=False):
        return self._journal('rollback', self.deployer.rollback(transaction, force))

    def _journal(self, operation, journal, sha=None):
        targets = tuple({'role': r['role'], 'host': r['host'], 'unit': r['unit'],
                         'action': r['action'], 'phase': r['phase'],
                         'binary_sha256': r['previous']['exe_sha256'] if operation == 'rollback' else journal.data['binary_sha256']}
                        for r in journal.hosts) if journal else tuple(
            {'role': t.role.name, 'host': t.host, 'unit': t.unit, 'action': 'skip', 'phase': 'unchanged'}
            for t in self.deployer.targets)
        return OperationResult(self.deployer.service.name, operation, targets,
                               transaction=journal.id if journal else None,
                               phase=journal.status if journal else 'unchanged', binary_sha256=sha)

    def status(self):
        observed = self.deployer.status()
        return OperationResult(self.deployer.service.name, 'status', tuple(observed['targets']),
                               transaction=observed['transaction'], phase=observed['phase'])
