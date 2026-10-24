"""Tie one CLI deployment to APM's durable, self-expiring notification window.

The production lock starts the window before the first service change. The CLI
finishes it after its existing validation returns. A lost runner needs no cleanup:
the running monitor enforces the original one-hour deadline.
"""
from contextvars import ContextVar
import json
import uuid
import sys

CURRENT = ContextVar('deployment_notifications', default=None)

# Use the running monitor's executable and state, without exporting its environment
# or loading Slack credentials into the deploy runner.
CONTROL = r'''
import json, os, subprocess, sys
pid = int(subprocess.check_output(['systemctl', 'show', 'pir-apm.service', '-p', 'MainPID', '--value'], text=True).strip())
if pid <= 0:
    raise SystemExit('deployment notifications require the running pir-apm service')
environment = dict(entry.split(b'=', 1) for entry in open('/proc/%d/environ' % pid, 'rb').read().split(b'\0') if b'=' in entry)
state = environment.get(b'PIR_APM_STATE_PATH')
if not state or environment.get(b'PIR_APM_ALERT_MODE') != b'active':
    raise SystemExit('deployment notifications require active APM and its durable state path')
command = ['/proc/%d/exe' % pid, 'deployment', '--state', os.fsdecode(state), '--id', sys.argv[1], *sys.argv[2:]]
result = subprocess.run(command, capture_output=True, text=True, timeout=15)
if result.returncode:
    raise SystemExit('APM refused deployment notification control: ' + result.stderr[-500:])
print(result.stdout.strip())
'''


class Notifications:
    def __init__(self, service=None):
        self.id = 'deploy-' + uuid.uuid4().hex
        self.services = [service] if service else None
        self.executor = None
        self.host = None
        self.finished = False

    def __enter__(self):
        self.token = CURRENT.set(self)
        return self

    def begin(self, inventory, executor):
        if self.executor is not None:
            return
        host = inventory.lock.get('host')
        if host is None:
            host = next((name for name, config in inventory.hosts.items()
                         if config.get('machine_id') == inventory.lock.get('machine_id')), None)
        if host is None:
            raise ValueError('deployment notifications need the coordinator in the inventory')
        services = self.services or sorted(inventory.services) or ['fleet']
        self.control(executor, host, ['begin', *services])
        self.executor, self.host = executor, host

    def control(self, executor, host, arguments):
        code, output = executor.run(host, ['python3', '-c', CONTROL, self.id, *arguments], 30)
        if code:
            raise ValueError('deployment notification control failed: ' + output[-700:])
        result = json.loads(output)
        if arguments[0] == 'begin' and result.get('outcome') is not None:
            raise ValueError('deployment notification window already ended')

    def finish(self, failed=False):
        if self.executor is not None and not self.finished:
            self.control(self.executor, self.host, ['finish', *(['--failed'] if failed else [])])
            self.finished = True

    def __exit__(self, kind, _error, _traceback):
        try:
            self.finish(failed=kind is not None)
        finally:
            CURRENT.reset(self.token)


def failed():
    """Restore alerts before a failed deployment enters automatic rollback."""
    notification = CURRENT.get()
    if notification is not None:
        try:
            notification.finish(failed=True)
        except Exception as error:
            # Failure reporting must never prevent the existing rollback. The
            # monitor still owns the fixed expiry if the coordinator is lost.
            print('deployment notification finish failed: ' + str(error), file=sys.stderr)
