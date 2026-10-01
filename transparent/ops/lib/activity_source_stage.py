"""Bootstrap immutable reviewed operation sources through the deployment wrapper.

The root helper owns the production lock in the same process that receives and
extracts the archive. No service, config, binary or public route is activated.
"""
import hashlib
import json
from pathlib import Path
import re
import shlex
import subprocess

from wallet_pir_ops import hostlock
from wallet_pir_ops.deploy.remote import SSHExecutor

HELPER_PATH = Path(__file__).with_name('activity_source_stage_host.py')
HELPER = Path(hostlock.__file__).read_text()+'\n'+HELPER_PATH.read_text()
MAX_COMPRESSED = 64 << 20  # v1 bound shared with the transmitted root helper.


class SourceStage:
    def __init__(self, inventory, out=print):
        self.inventory, self.out = inventory, out
        lock = inventory.lock
        if lock.get('type') != 'remote':
            raise ValueError('source bootstrap requires a remote coordinator inventory')
        self.host = lock['host']
        entry = inventory.hosts[self.host]
        self.machine = entry.get('machine_id')
        if not isinstance(self.machine, str) or not re.fullmatch('[0-9a-f]{32}', self.machine):
            raise ValueError('source bootstrap requires the coordinator machine_id pin')
        if entry.get('user', inventory.ssh.get('user', 'root')) != 'root' and not entry.get('sudo'):
            raise ValueError('source bootstrap requires root or the configured sudo identity')
        self.executor = SSHExecutor(inventory)

    def request(self, mode, source, checksum):
        if not re.fullmatch('[0-9a-f]{40}', source) or not re.fullmatch('[0-9a-f]{64}', checksum):
            raise ValueError('source bootstrap requires exact source SHA and archive SHA-256')
        return {'mode': mode, 'source_sha': source, 'sha256': checksum, 'machine_id': self.machine}

    def call(self, request, archive=None):
        entry = self.inventory.hosts[self.host]
        prefix = ['sudo', '-n', '--'] if entry.get('sudo') else []
        command = shlex.join([*prefix, '/usr/bin/python3', '-c', HELPER, json.dumps(request)])
        handle = Path(archive).open('rb') if archive else subprocess.DEVNULL
        try:
            result = subprocess.run(self.executor.transport(self.host)+[command], stdin=handle,
                                    capture_output=True, timeout=1800)
        finally:
            if archive:
                handle.close()
        # Keep arbitrary remote stderr/argv out of user-facing errors.
        if result.returncode:
            raise ValueError('source staging helper failed; inspect its retained coordinator receipt')
        reply = json.loads(result.stdout)
        if not reply.get('ok'):
            raise ValueError('source staging refused: '+reply['error'])
        self.out(json.dumps(reply['result'], sort_keys=True))
        return reply['result']

    def run(self, mode, source, checksum, archive=None):
        request = self.request(mode, source, checksum)
        if mode in ('plan', 'preflight', 'stage'):
            if archive is None:
                raise ValueError('pass the git archive for source staging')
            if not Path(archive).is_file() or Path(archive).is_symlink():
                raise ValueError('source archive must be a regular file, not a symlink')
            if Path(archive).stat().st_size > MAX_COMPRESSED:
                raise ValueError('source archive exceeds the received-archive bound; export only operation sources')
            # Stream without buffering or exporting file contents to logs.
            with Path(archive).open('rb') as stream:
                actual = hashlib.file_digest(stream, 'sha256').hexdigest()
            if actual != checksum:
                raise ValueError('local source archive checksum differs from reviewed plan')
        if mode == 'plan':
            self.out('source '+source+' archive '+checksum+' coordinator '+self.host)
            self.out('immutable staging only; no service activation; preflight required before stage')
            return
        if mode == 'stage':
            self.call(self.request('preflight', source, checksum))
        return self.call(request, archive if mode == 'stage' else None)
