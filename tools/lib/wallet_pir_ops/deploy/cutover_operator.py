"""Supervise an Enhance cutover on the authenticated Mac, without host keys.

The launch command retains immutable inputs and installs two private LaunchAgents.
The forward agent uses the ordinary locked deployment; the independent watchdog
uses its ordinary rollback. The Mac must stay connected until acceptance or
restoration. Network loss leaves restoration pending and retryable, never accepted.
"""
import json
import os
from pathlib import Path
import plistlib
import re
import subprocess
import sys
import time
import uuid

from .. import bundle, durable
from ..paths import ROOT
from .cutover_guard import Decision, unit_name
from .cutover_runner import Runner, copy_executable, write_text
from .engine import file_sha256
from .publisher import Cutover, ENHANCE_HTTPS_SCHEMA, load_request, require
from .remote import SSHExecutor, production_lock
from .transaction import new_id


def private(path, *, directory=False):
    info = path.lstat()
    require(not path.is_symlink() and info.st_uid == os.getuid() and not info.st_mode & 0o077
            and (path.is_dir() if directory else path.is_file()),
            'operator input must be private and owned by the invoking user: ' + str(path))


def home(state_dir):
    return Path(state_dir).absolute() / 'operator-supervision'


def launchctl(*arguments, required=True):
    result = subprocess.run(['/bin/launchctl', *arguments], capture_output=True, text=True, timeout=30)
    require(not required or result.returncode == 0,
            'operator launchctl operation failed (' + str(result.returncode) + ')')
    return result


def domain():
    return 'gui/' + str(os.getuid())


def job_state(label):
    result = launchctl('print', domain() + '/' + label, required=False)
    if result.returncode:
        return {'MainPID': '0', 'ActiveState': 'inactive', 'InvocationID': None}
    pid = re.search(r'^\s*pid = (\d+)\s*$', result.stdout, re.MULTILINE)
    invocation = re.search(r'^\s*WALLET_PIR_CUTOVER_INVOCATION => (\S+)\s*$', result.stdout, re.MULTILINE)
    failed = re.search(r'^\s*last exit code = (\d+)\s*$', result.stdout, re.MULTILINE)
    return {'MainPID': pid.group(1) if pid else '0',
            'InvocationID': invocation.group(1) if invocation else None,
            'ActiveState': 'active' if pid else 'failed' if failed else 'inactive'}


def operator_decision(root, identifier):
    boot = subprocess.run(['/usr/sbin/sysctl', '-n', 'kern.boottime'],
                          capture_output=True, text=True, timeout=10)
    require(boot.returncode == 0 and boot.stdout.strip(), 'cannot identify the operator boot')
    return Decision(root, identifier, boot_id=boot.stdout.strip(), elapsed=time.monotonic)


def require_operator(request):
    require(sys.platform == 'darwin', 'operator supervision requires macOS')
    require((ROOT / 'operations.json').is_file(), 'operator supervision requires a verified operations bundle')
    bundle.check_installed(mutation=True, revision=request['release']['git_sha'])
    launchctl('print', domain())
    require(Path('/usr/bin/caffeinate').is_file(), 'operator sleep prevention is unavailable')


def status(state_dir, request_sha256):
    pointer = home(state_dir) / 'latest.json'
    if not pointer.is_file():
        return None
    private(pointer)
    value = json.loads(pointer.read_text())
    if value['request_sha256'] != request_sha256:
        return None
    unit_name(value['id'])
    root = pointer.parent / value['id']
    private(root, directory=True)
    private(root / 'decision.json')
    return {'decision': json.loads((root / 'decision.json').read_text()),
            'forward': job_state(unit_name(value['id'])),
            'watchdog': job_state(unit_name(value['id']) + '-rollback')}


def launch(target, args, publisher_binary):
    request = target.request
    require_operator(request)
    base = home(args.state_dir)
    base.mkdir(mode=0o700, parents=True, exist_ok=True)
    private(base, directory=True)
    agents = Path.home() / 'Library/LaunchAgents'
    agents.mkdir(parents=True, exist_ok=True)
    with production_lock(target.inventory, target.ex) as target.lock:
        pointer = base / 'latest.json'
        if pointer.exists():
            private(pointer)
            previous = json.loads(pointer.read_text())
            unit_name(previous['id'])
            previous_root = base / previous['id']
            private(previous_root, directory=True)
            private(previous_root / 'decision.json')
            require(json.loads((previous_root / 'decision.json').read_text())['phase'] in ('accepted', 'restored'),
                    'another operator cutover is unfinished')
        require(durable.digest(target.capture()) == args.expect_plan_sha256, 'publisher plan is stale')
        identifier = new_id('publisher', request['release']['binary_sha256'])
        root = base / identifier
        root.mkdir(mode=0o700)
        decision = operator_decision(root, identifier)
        decision.create(args.request_sha256)
        write_text(root / 'request.json', Path(args.request).read_text(), 0o600)
        metadata = {'id': identifier, 'ops_root': str(ROOT.resolve()),
                    'state_dir': str(Path(args.state_dir).absolute()),
                    'request_sha256': args.request_sha256, 'plan_sha256': args.expect_plan_sha256,
                    'invocation': str(uuid.uuid4()), 'created_unix': time.time(), 'jobs': {}}
        for name, source, release in [('publisher', publisher_binary, request['release']),
                                     ('cpu', args.serving_binary, request['serving']),
                                     ('cuda', args.cuda_binary, request['enhance_https']['cuda_release'])]:
            if release is not None:
                path = root / ('executable-' + name)
                copy_executable(source, path, release)
                metadata[name] = str(path)
        prefix = [str(Path(sys.executable).resolve()), '-B', str(ROOT / 'tools/scripts/wallet-pir-deploy.py'),
                  '--state-dir', metadata['state_dir']]
        for suffix, operation in [('', 'run'), ('-rollback', 'watchdog')]:
            label = unit_name(identifier) + suffix
            path = agents / (label + '.plist')
            require(not path.exists() and not path.is_symlink(), 'operator LaunchAgent already exists')
            environment = {'WALLET_PIR_CUTOVER_INVOCATION': metadata['invocation'],
                           'PYTHONDONTWRITEBYTECODE': '1', 'PATH': os.environ.get('PATH', '/usr/bin:/bin')}
            if os.environ.get('SSH_AUTH_SOCK'):
                environment['SSH_AUTH_SOCK'] = os.environ['SSH_AUTH_SOCK']
            job = {'Label': label, 'ProgramArguments': [*prefix, 'publisher-cutover-' + operation,
                    '--transaction', identifier, '--supervision', 'operator'],
                   'EnvironmentVariables': environment, 'WorkingDirectory': str(ROOT),
                   'RunAtLoad': bool(suffix), 'ThrottleInterval': 15,
                   'StandardOutPath': str(root / (operation + '.log')),
                   'StandardErrorPath': str(root / (operation + '.log')),
                   'Umask': 0o077}
            if suffix:
                job['KeepAlive'] = {'SuccessfulExit': False}
            text = plistlib.dumps(job).decode()
            write_text(path, text, 0o600)
            metadata['jobs'][label] = {'path': str(path), 'text': text}
        durable.atomic_json(root / 'runner.json', metadata, mode=0o600)
        durable.atomic_json(pointer, {'id': identifier, 'request_sha256': args.request_sha256}, mode=0o600)
        watchdog = unit_name(identifier) + '-rollback'
        launchctl('bootstrap', domain(), metadata['jobs'][watchdog]['path'])
        deadline = time.monotonic() + 20
        while not (root / 'watchdog-ready.json').exists():
            require(time.monotonic() < deadline, 'independent operator watchdog did not become ready')
            time.sleep(.1)
        ready = json.loads((root / 'watchdog-ready.json').read_text())
        require(job_state(watchdog)['MainPID'] == str(ready['pid']), 'operator watchdog is not running')
    try:
        forward = unit_name(identifier)
        launchctl('bootstrap', domain(), metadata['jobs'][forward]['path'])
        launchctl('kickstart', domain() + '/' + forward)
    except Exception:
        decision.cancel_before_start()
        raise
    return {'transaction': identifier, 'phase': 'supervised', 'supervision': 'operator',
            'forward_job': forward, 'watchdog_job': watchdog, 'decision': str(decision.path)}


class OperatorRunner(Runner):
    def __init__(self, identifier, state_dir):
        self.name = unit_name(identifier)
        self.root = home(state_dir) / identifier
        private(self.root, directory=True)
        for name in ('runner.json', 'request.json', 'decision.json'):
            private(self.root / name)
        self.metadata = json.loads((self.root / 'runner.json').read_text())
        require(self.metadata['id'] == identifier and self.metadata['ops_root'] == str(ROOT.resolve())
                and self.metadata['state_dir'] == str(Path(state_dir).absolute()), 'operator inputs changed')
        self.request, inventory = load_request(self.root / 'request.json', self.metadata['request_sha256'], recovery=True)
        require(self.request['schema'] == ENHANCE_HTTPS_SCHEMA, 'operator supervisor requires Enhance HTTPS')
        require_operator(self.request)
        for name, release in [('publisher', self.request['release']), ('cpu', self.request['serving']),
                              ('cuda', self.request['enhance_https']['cuda_release'])]:
            if release is not None:
                path = Path(self.metadata[name])
                require(path.parent == self.root and not path.is_symlink() and path.is_file()
                        and path.stat().st_uid == os.getuid()
                        and path.stat().st_size == release['binary_bytes']
                        and file_sha256(path) == release['binary_sha256'], 'operator executable changed')
        self.target = Cutover(self.request, inventory, SSHExecutor(inventory), state_dir)
        self.target.serving.binary = self.metadata['cpu']
        self.target.serving.cuda_binary = self.metadata.get('cuda')
        self.target.identifier = identifier
        self.decision = operator_decision(self.root, identifier)
        self.verify_units()

    def verify_units(self):
        for job in self.metadata['jobs'].values():
            path = Path(job['path'])
            private(path)
            require(path.read_text() == job['text'], 'transaction-owned operator LaunchAgent changed')

    def observe_forward(self):
        observed = job_state(self.name)
        if observed['MainPID'] == '0' and time.time() >= self.metadata['created_unix'] + 60:
            observed['ActiveState'] = 'failed'
        return observed

    def arm(self):
        self.verify_units()
        ready = self.root / 'watchdog-ready.json'
        private(ready)
        observed = job_state(self.name + '-rollback')
        require(observed['MainPID'] == str(json.loads(ready.read_text())['pid'])
                and observed['MainPID'] != '0' and observed['InvocationID'] == self.metadata['invocation'],
                'independent operator watchdog is unavailable')
        return self.decision.arm()

    def run(self):
        observed = self.observe_forward()
        require(observed['MainPID'] == str(os.getpid())
                and observed['InvocationID'] == self.metadata['invocation']
                and os.environ.get('WALLET_PIR_CUTOVER_INVOCATION') == self.metadata['invocation'],
                'forward command must run in its owned operator job')
        self.decision.started(self.metadata['invocation'], os.getpid())
        self.target.supervision = self
        return self.target.deploy(self.metadata['publisher'], self.metadata['plan_sha256'], self.metadata['cpu'])

    def stop_forward(self):
        for signal, limit in [('SIGTERM', 15), ('SIGKILL', 5)]:
            observed = self.observe_forward()
            state = json.loads(self.decision.path.read_text())
            if observed['MainPID'] == '0':
                return
            require(observed['MainPID'] == str(state['runner_pid'])
                    and observed['InvocationID'] == state['runner_invocation'],
                    'forward invocation changed; refusing to stop another owner')
            launchctl('kill', signal, domain() + '/' + self.name)
            deadline = time.monotonic() + limit
            while time.monotonic() < deadline:
                if self.observe_forward()['MainPID'] == '0':
                    return
                time.sleep(.2)
        require(False, 'operator forward job did not stop')

    def watchdog(self):
        observed = job_state(self.name + '-rollback')
        require(observed['MainPID'] == str(os.getpid())
                and observed['InvocationID'] == self.metadata['invocation'], 'watchdog must run in its owned operator job')
        durable.atomic_json(self.root / 'watchdog-ready.json', {'pid': os.getpid()}, mode=0o600)
        return super().watchdog()


def dispatch(args):
    runner = OperatorRunner(args.transaction, args.state_dir)
    hold = subprocess.Popen(['/usr/bin/caffeinate', '-is', '-w', str(os.getpid())],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        require(hold.poll() is None, 'operator sleep prevention failed')
        return runner.run() if args.command.endswith('-run') else runner.watchdog()
    finally:
        hold.terminate()
        hold.wait(timeout=5)
