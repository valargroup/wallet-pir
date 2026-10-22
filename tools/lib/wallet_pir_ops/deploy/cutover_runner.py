"""Finish one already launched cutover with a separate, boot-persistent rollback owner.

New supervised cutovers are retired; this keeps saved transactions recoverable.
The forward unit holds the normal production lock. The independent watchdog
fences and stops only that unit before invoking the same CLI rollback path; it
never steals another transaction's lock.
"""
import json
import os
from pathlib import Path
import stat
import subprocess
import sys
import time

from .. import bundle, durable
from ..paths import ROOT
from .cutover_guard import Decision, unit_name
from .publisher import Cutover, ENHANCE_HTTPS_SCHEMA, ROOT_DIR, load_request, require
from .remote import SSHExecutor

SYSTEM = Path('/etc/systemd/system')


def systemctl(*arguments):
    result = subprocess.run(['systemctl', *arguments], capture_output=True, text=True, timeout=65)
    require(result.returncode == 0, 'supervised cutover systemd operation failed: ' + result.stderr[-1000:])
    return result.stdout


def observe(identifier):
    text = systemctl('show', unit_name(identifier) + '.service', '--property=ActiveState,MainPID,InvocationID')
    return dict(line.split('=', 1) for line in text.splitlines() if '=' in line)


def private_file(path):
    info = path.lstat()
    require(stat.S_ISREG(info.st_mode) and info.st_uid == 0 and not info.st_mode & 0o077,
            'supervised input must be a private root-owned regular file')


def write_text(path, text, mode):
    durable.atomic_text(path, text, prefix='.cutover-', mode=mode)


def require_coordinator(request):
    require(sys.platform == 'linux' and os.geteuid() == 0,
            'supervised deployment must run as root on the existing coordinator')
    require(Path('/etc/machine-id').read_text().strip() == request['machine_id'],
            'supervised deployment is not on the reviewed coordinator')
    require((ROOT / 'operations.json').is_file(),
            'supervised deployment requires an immutable verified operations release')
    bundle.check_installed(mutation=True, revision=request['release']['git_sha'])


class Runner:
    def __init__(self, identifier):
        name = unit_name(identifier)
        self.root = Path(ROOT_DIR) / 'transactions' / identifier
        private_file(self.root / 'runner.json')
        private_file(self.root / 'request.json')
        self.metadata = json.loads((self.root / 'runner.json').read_text())
        require(self.metadata['id'] == identifier and self.metadata['ops_root'] == str(ROOT.resolve()),
                'supervised operations identity changed')
        self.request, inventory = load_request(self.root / 'request.json', self.metadata['request_sha256'], recovery=True)
        require(self.request['schema'] == ENHANCE_HTTPS_SCHEMA, 'supervisor supports only the Enhance HTTPS cutover')
        require_coordinator(self.request)
        self.target = Cutover(self.request, inventory, SSHExecutor(inventory), self.metadata['state_dir'])
        self.target.serving.binary = self.metadata['cpu']
        self.target.serving.cuda_binary = self.metadata.get('cuda')
        self.target.identifier = identifier
        self.decision = Decision(self.root, identifier)
        self.name = name
        self.verify_units()

    def verify_units(self):
        for name, text in self.metadata['units'].items():
            info = (SYSTEM / name).lstat()
            require(stat.S_ISREG(info.st_mode) and info.st_uid == 0 and not info.st_mode & 0o022
                    and (SYSTEM / name).read_text() == text,
                    'transaction-owned supervised unit changed')

    def arm(self):
        self.verify_units()
        require(systemctl('is-active', self.name + '-rollback.service').strip() == 'active',
                'independent watchdog is unavailable before interruption')
        return self.decision.arm()

    def guard(self):
        self.decision.guard()

    def accept(self, evidence):
        require(set(evidence['products']) == set(self.request['checks']),
                'acceptance omits an enabled publisher product')
        self.decision.accept(evidence)

    def run(self):
        observed = observe(self.target.identifier)
        invocation = os.environ.get('INVOCATION_ID')
        require(invocation and observed.get('InvocationID') == invocation
                and observed.get('MainPID') == str(os.getpid()), 'forward command must run in its owned systemd unit')
        self.decision.started(invocation, os.getpid())
        self.target.supervision = self
        try:
            return self.target.deploy(self.metadata['publisher'], self.metadata['plan_sha256'], self.metadata['cpu'])
        except Exception:
            # A crashed process or a lost shell also takes this path through the
            # independent service. Do not wait for this runner to recover itself.
            raise

    def observe_forward(self):
        return observe(self.target.identifier)

    def stop_forward(self):
        systemctl('stop', self.name + '.service')

    def watchdog(self):
        while True:
            state = json.loads(self.decision.path.read_text())
            if state['phase'] in ('accepted', 'restored'):
                return {'transaction': self.target.identifier, 'phase': state['phase']}
            observed = self.observe_forward()
            active = observed.get('MainPID', '0') != '0'
            if not active and not state['runner_started'] and observed.get('ActiveState') == 'failed':
                # A unique unit can fail before Python reaches started(). An
                # unlaunched unit is inactive, so it cannot trigger this path.
                self.decision.cancel_before_start()
            if self.decision.claim_rollback(runner_active=active):
                state = json.loads(self.decision.path.read_text())
                self.verify_units()
                if active:
                    require(observed.get('InvocationID') == state.get('runner_invocation')
                            and observed['MainPID'] == str(state.get('runner_pid')),
                            'forward invocation changed; refusing to stop another owner')
                    # TimeoutStopSec and KillMode bound and contain termination;
                    # no PID outside this transaction's cgroup is targeted.
                    self.stop_forward()
                    require(self.observe_forward().get('MainPID') == '0', 'forward runner did not stop')
                latest = self.target.latest()
                if latest is None or latest['id'] != self.target.identifier:
                    require(state['interrupted_unix'] is None,
                            'interrupted cutover lost its pinned rollback journal')
                else:
                    require(latest['request'] == self.request, 'rollback journal request changed')
                    self.target.rollback()
                self.decision.restored()
                return {'transaction': self.target.identifier, 'phase': 'restored'}
            time.sleep(0.2)


def dispatch(args):
    runner = Runner(args.transaction)
    return runner.run() if args.command.endswith('-run') else runner.watchdog()
