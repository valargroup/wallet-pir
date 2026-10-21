"""Explicit Linux qualification of the real transaction-owned systemd units.

Run only on a disposable development host with WALLET_PIR_SYSTEMD_REHEARSAL=1.
The source and watchdog use isolated state and a local rollback receipt; no
production inventory, service, credential or publisher data is opened. The
deadline fixture represents an hour that is already nearly elapsed.
"""
import fcntl
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
import uuid

ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT / 'tools/lib'))
from wallet_pir_ops import durable
from wallet_pir_ops.deploy.cutover_guard import Decision, services, unit_name
from wallet_pir_ops.deploy.cutover_runner import Runner, SYSTEM, observe, systemctl, write_text


class IsolatedRollback:
    def __init__(self, root, identifier):
        self.root, self.identifier = root, identifier

    def latest(self):
        return {'id': self.identifier, 'request': {'isolated_rehearsal': True}, 'phase': 'installing'}

    def rollback(self):
        # Exercise serialized execution after termination, using a dedicated
        # test lock instead of opening the real production lock or any fleet.
        with (self.root / 'isolated.lock').open('a') as handle:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            if observe(self.identifier)['MainPID'] != '0':
                raise RuntimeError('forward owner remains alive during rollback')
            durable.atomic_json(self.root / 'rollback.json', {'restored': True}, mode=0o600)


def worker(mode, root, identifier, nearly_elapsed):
    if mode == 'fail':
        durable.atomic_json(root / 'forward.json', {'failed_before_started': True}, mode=0o600)
        raise SystemExit(7)
    if mode == 'forward':
        decision = Decision(root, identifier,
                            clock=lambda: time.time() - (3596 if nearly_elapsed else 0))
        decision.started(os.environ['INVOCATION_ID'], os.getpid())
        decision.arm()
        child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(120)'])
        durable.atomic_json(root / 'forward.json', {'pid': os.getpid(), 'child': child.pid}, mode=0o600)
        while True:
            time.sleep(1)
    runner = Runner.__new__(Runner)
    runner.root, runner.name = root, unit_name(identifier)
    runner.metadata = json.loads((root / 'units.json').read_text())
    runner.target = IsolatedRollback(root, identifier)
    runner.request = {'isolated_rehearsal': True}
    runner.decision = Decision(root, identifier)
    runner.watchdog()


@unittest.skipUnless(sys.platform == 'linux' and os.geteuid() == 0
                     and os.environ.get('WALLET_PIR_SYSTEMD_REHEARSAL') == '1',
                     'explicit root systemd rehearsal on an isolated development host')
class SystemdRehearsal(unittest.TestCase):
    def run_case(self, *, nearly_elapsed, failed_start=False):
        root = Path(tempfile.mkdtemp(prefix='wallet-pir-watchdog-rehearsal-'))
        identifier = 'publisher-rehearsal-' + uuid.uuid4().hex
        name = unit_name(identifier)
        decision = Decision(root, identifier)
        decision.create('a' * 64)
        prefix = [sys.executable, str(Path(__file__).resolve())]
        extra = ['--nearly-elapsed'] if nearly_elapsed else []
        units = services(identifier, [*prefix, 'fail' if failed_start else 'forward', str(root), identifier, *extra],
                         [*prefix, 'watchdog', str(root), identifier])
        durable.atomic_json(root / 'units.json', {'units': units}, mode=0o600)
        sibling = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(120)'])
        try:
            for unit, text in units.items():
                self.assertFalse((SYSTEM / unit).exists())
                write_text(SYSTEM / unit, text, 0o644)
            systemctl('daemon-reload')
            systemctl('enable', '--now', name + '-rollback.service')
            self.assertEqual(systemctl('is-enabled', name + '-rollback.service').strip(), 'enabled')
            systemctl('start', '--no-block', name + '.service')
            deadline = time.monotonic() + 10
            while not (root / 'forward.json').is_file():
                self.assertLess(time.monotonic(), deadline, 'forward fixture did not start')
                time.sleep(0.1)
            before = json.loads(decision.path.read_text())['deadline_unix']
            if failed_start:
                self.assertIsNone(before, 'no serving interruption occurred')
            elif nearly_elapsed:
                systemctl('restart', name + '-rollback.service')
                self.assertEqual(json.loads(decision.path.read_text())['deadline_unix'], before)
            else:
                systemctl('kill', '--kill-whom=main', '--signal=SIGKILL', name + '.service')
            deadline = time.monotonic() + 35
            while not (root / 'rollback.json').is_file():
                self.assertLess(time.monotonic(), deadline, 'independent watchdog did not execute rollback')
                time.sleep(0.1)
            state = json.loads(decision.path.read_text())
            self.assertEqual(state['phase'], 'restored')
            self.assertEqual(observe(identifier)['MainPID'], '0')
            self.assertIsNone(sibling.poll(), 'an unrelated process was terminated')
            self.assertEqual(state['deadline_unix'], before)
            print(json.dumps({'case': 'failed-start' if failed_start else 'elapsed-deadline' if nearly_elapsed else 'lost-runner',
                              'restored': True, 'unrelated_process_preserved': True,
                              'original_deadline_preserved': True}), flush=True)
        finally:
            sibling.terminate()
            sibling.wait(timeout=5)
            for unit in units:
                systemctl('stop', unit)
            systemctl('disable', name + '-rollback.service')
            loaded = systemctl('list-units', '--all', '--plain', '--no-legend', *units)
            for line in loaded.splitlines():
                unit = line.split()[0]
                if unit in units:
                    systemctl('reset-failed', unit)
            for unit, text in units.items():
                self.assertEqual((SYSTEM / unit).read_text(), text)
                (SYSTEM / unit).unlink()
            systemctl('daemon-reload')
            # Evidence stays under the reported, private temporary root.
            print('rehearsal evidence: ' + str(root), flush=True)

    def test_deadline_and_watchdog_restart_execute_rollback(self):
        self.run_case(nearly_elapsed=True)

    def test_lost_forward_runner_executes_rollback_before_deadline(self):
        self.run_case(nearly_elapsed=False)

    def test_failed_service_before_runner_start_finishes_recovery_without_a_downtime_clock(self):
        self.run_case(nearly_elapsed=False, failed_start=True)


if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] in ('forward', 'watchdog', 'fail'):
        worker(sys.argv[1], Path(sys.argv[2]), sys.argv[3], '--nearly-elapsed' in sys.argv)
    else:
        unittest.main()
