"""SSH and SCP to one host whose key was verified out of band.

There is no trust on first use: the known-hosts file is pinned by digest, and
the digest is checked again before every connection, so a host key swapped in
mid-operation stops the operation before anything runs remotely. No agent is
forwarded and no user or system SSH configuration applies.
"""
import hashlib
import ipaddress
from pathlib import Path
import shlex
import subprocess


def sha256(data):
    return hashlib.sha256(data).hexdigest()


class PinnedSSH:
    def __init__(self, host, key, known_hosts, known_hosts_sha256, user='root'):
        self.host = str(ipaddress.IPv4Address(host))
        self.user = user
        self.key = Path(key).resolve()
        self.known_hosts = Path(known_hosts).resolve()
        self.known_hosts_sha256 = known_hosts_sha256
        if sha256(self.known_hosts.read_bytes()) != known_hosts_sha256:
            raise ValueError('SSH host-key inventory differs from the verified pin')
        self.options = ['-F', '/dev/null', '-i', str(self.key), '-o', 'BatchMode=yes',
                        '-o', 'IdentitiesOnly=yes', '-o', 'ForwardAgent=no', '-o', 'ConnectTimeout=10',
                        '-o', 'StrictHostKeyChecking=yes', '-o', 'UserKnownHostsFile=' + str(self.known_hosts),
                        '-o', 'GlobalKnownHostsFile=/dev/null']

    def check_host_keys(self):
        if sha256(self.known_hosts.read_bytes()) != self.known_hosts_sha256:
            raise ValueError('SSH host-key inventory changed during bootstrap')

    def command(self, arguments, timeout=120):
        """Run `arguments` remotely, quoted as one shell word each; return stdout."""
        self.check_host_keys()
        result = subprocess.run(['ssh', *self.options, self.user + '@' + self.host, shlex.join(arguments)],
                                capture_output=True, timeout=timeout)
        if result.returncode:
            raise RuntimeError('remote bootstrap command failed')
        return result.stdout

    def copy(self, paths, destination):
        self.check_host_keys()
        result = subprocess.run(['scp', *self.options, *[str(p) for p in paths], self.user + '@' + self.host + ':' + destination],
                                capture_output=True, timeout=300)
        if result.returncode:
            raise RuntimeError('candidate transfer failed')
