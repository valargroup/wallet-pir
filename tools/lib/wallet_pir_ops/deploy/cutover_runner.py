"""Execute one reviewed cutover with a separate, boot-persistent rollback owner.

The public deploy command must run on the existing coordinator from a verified
operations release. It stages immutable inputs and two transaction-owned units,
then returns their identities. The forward unit holds the normal production
lock. The independent watchdog fences and stops only that unit before invoking
the same CLI rollback path; it never steals another transaction's lock.
"""
import json
import os
from pathlib import Path
import shutil
import stat
import subprocess
import sys
import tempfile
import time

from .. import bundle, durable
from ..paths import ROOT
from .cutover_guard import Decision, services, unit_name
from .engine import DeployError, file_sha256
from .publisher import Cutover, ENHANCE_HTTPS_SCHEMA, ROOT_DIR, load_request, require
from .remote import SSHExecutor, production_lock
from .transaction import new_id

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
    descriptor, temporary = tempfile.mkstemp(dir=path.parent, prefix='.cutover-')
    try:
        with os.fdopen(descriptor, 'w') as handle:
            handle.write(text)
            os.fchmod(handle.fileno(), mode)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def require_coordinator(request):
    require(sys.platform == 'linux' and os.geteuid() == 0,
            'supervised deployment must run as root on the existing coordinator')
    require(Path('/etc/machine-id').read_text().strip() == request['machine_id'],
            'supervised deployment is not on the reviewed coordinator')
    require((ROOT / 'operations.json').is_file(),
            'supervised deployment requires an immutable verified operations release')
    bundle.check_installed(mutation=True, revision=request['release']['git_sha'])


def copy_executable(source, target, expected):
    require(source is not None and file_sha256(source) == expected['binary_sha256']
            and Path(source).stat().st_size == expected['binary_bytes'],
            'supervised executable differs from the reviewed release')
    with target.open('xb') as handle, Path(source).open('rb') as original:
        shutil.copyfileobj(original, handle)
        os.fchmod(handle.fileno(), 0o755)
        handle.flush()
        os.fsync(handle.fileno())
    require(file_sha256(target) == expected['binary_sha256'], 'supervised executable copy differs')


def launch(target, args, publisher_binary):
    request = target.request
    require_coordinator(request)
    with production_lock(target.inventory, target.ex) as target.lock:
        pointer = Path(ROOT_DIR) / 'supervised-latest.json'
        if pointer.is_file():
            previous = json.loads(pointer.read_text())
            unit_name(previous['id'])
            decision = json.loads((Path(ROOT_DIR) / 'transactions' / previous['id'] / 'decision.json').read_text())
            require(decision['phase'] in ('accepted', 'restored'), 'another supervised cutover is unfinished')
        plan = target.capture()
        require(durable.digest(plan) == args.expect_plan_sha256, 'publisher plan is stale; review a fresh plan')
        identifier = new_id('publisher', request['release']['binary_sha256'])
        root = Path(ROOT_DIR) / 'transactions' / identifier
        root.mkdir(mode=0o700, parents=True, exist_ok=False)
        decision = Decision(root, identifier)
        decision.create(args.request_sha256)
        raw = Path(args.request).read_text()
        write_text(root / 'request.json', raw, 0o600)
        metadata = {'id': identifier, 'ops_root': str(ROOT.resolve()),
            'request_sha256': args.request_sha256, 'plan_sha256': args.expect_plan_sha256,
            'state_dir': str(Path(args.state_dir).resolve())}
        for name, source, release in [('publisher', publisher_binary, request['release']),
                ('cpu', args.serving_binary, request['serving']),
                ('cuda', args.cuda_binary, request['enhance_https']['cuda_release'])]:
            if release is not None:
                path = root / ('executable-' + name)
                copy_executable(source, path, release)
                metadata[name] = str(path)
        script = ROOT.resolve() / 'tools/scripts/wallet-pir-deploy.py'
        prefix = [sys.executable, str(script), '--state-dir', metadata['state_dir']]
        units = services(identifier,
            [*prefix, 'publisher-cutover-run', '--transaction', identifier],
            [*prefix, 'publisher-cutover-watchdog', '--transaction', identifier])
        metadata['units'] = units
        durable.atomic_json(root / 'runner.json', metadata, mode=0o600)
        durable.atomic_json(pointer, {'id': identifier, 'request_sha256': args.request_sha256}, mode=0o600)
        for name, text in units.items():
            require(not (SYSTEM / name).exists(), 'supervised unit already exists')
            write_text(SYSTEM / name, text, 0o644)
        systemctl('daemon-reload')
        # Enable the independent watchdog before the forward owner can stop
        # anything. It resumes on boot using the same durable deadline.
        name = unit_name(identifier)
        systemctl('enable', '--now', name + '-rollback.service')
        require(systemctl('is-active', name + '-rollback.service').strip() == 'active',
                'independent rollback watchdog did not start')
    # The child takes the same production lock itself. Release ours before it
    # starts; its fresh capture refuses any intervening change.
    try:
        systemctl('start', '--no-block', name + '.service')
    except Exception:
        decision.cancel_before_start()
        raise
    return {'transaction': identifier, 'phase': 'supervised',
            'forward_unit': name + '.service', 'watchdog_unit': name + '-rollback.service',
            'decision': str(decision.path)}


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

    def watchdog(self):
        while True:
            state = json.loads(self.decision.path.read_text())
            if state['phase'] in ('accepted', 'restored'):
                return {'transaction': self.target.identifier, 'phase': state['phase']}
            observed = observe(self.target.identifier)
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
                    systemctl('stop', self.name + '.service')
                    require(observe(self.target.identifier).get('MainPID') == '0', 'forward runner did not stop')
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
