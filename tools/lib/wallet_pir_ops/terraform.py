"""Run one Terraform root through reviewed, digest-pinned saved plans.

Only a saved plan is ever applied, and only if its file still has the digest
recorded when it was validated. When a `PinnedHostLock` guards the state,
every Terraform child inherits its descriptor, so killing the caller cannot
release the lock while Terraform still writes.
"""
import hashlib
import json
import os
from pathlib import Path
import subprocess

# Ambient variables that could redirect Terraform or override its arguments:
# TF_CLI_ARGS*, TF_WORKSPACE, TF_VAR_*, DIGITALOCEAN_API_URL and the like.
AMBIENT_PREFIXES = ('TF_', 'DIGITALOCEAN_')


def clean_environment(source=None, strip=AMBIENT_PREFIXES, **values):
    """`source` (default `os.environ`) minus `strip`-prefixed names, for automation."""
    source = os.environ if source is None else source
    env = {k: v for k, v in source.items() if not k.startswith(tuple(strip))}
    env.update(TF_IN_AUTOMATION='1', TF_INPUT='0')
    env.update(values)
    return env


def file_sha256(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


class Terraform:
    """One root, one environment, optionally one inherited writer lock.

    `env` is passed as the whole child environment; build it with
    `clean_environment`. `lock` is a held `PinnedHostLock` or None.
    """
    PLAN_TIMEOUT = 300
    APPLY_TIMEOUT = 900

    def __init__(self, root, env=None, lock=None, executable='terraform'):
        self.root = Path(root).resolve()
        self.env = clean_environment() if env is None else dict(env)
        self.lock = lock
        self.executable = executable

    def descriptors(self):
        """Descriptors every child inherits; refuses if a required lock is lost."""
        return () if self.lock is None else self.lock.descriptors()

    def run(self, arguments, timeout=120):
        descriptors = self.descriptors()
        result = subprocess.run([self.executable, f'-chdir={self.root}', *arguments], env=self.env,
                                capture_output=True, timeout=timeout, pass_fds=descriptors)
        if result.returncode:
            raise RuntimeError('Terraform command failed; operation remains fenced')
        return result.stdout

    def save_plan(self, path, arguments=(), timeout=None):
        """Write a private saved plan to `path` and return its sha256."""
        self.run(['plan', '-input=false', '-lock-timeout=60s', '-out=' + str(path), *arguments],
                 timeout=timeout or self.PLAN_TIMEOUT)
        os.chmod(path, 0o600)
        return file_sha256(path)

    def show_json(self, plan):
        """`terraform show -json` of a saved plan. It holds variable values, secrets included."""
        return json.loads(self.run(['show', '-json', str(plan)]))

    def apply(self, path, expected_digest, timeout=None):
        """Apply exactly the saved plan that was validated, or nothing."""
        if file_sha256(path) != expected_digest:
            raise ValueError('saved plan changed before apply')
        self.run(['apply', '-input=false', '-lock-timeout=60s', str(path)],
                 timeout=timeout or self.APPLY_TIMEOUT)

    def state_pull(self):
        """The raw remote state, with lineage and serial."""
        return json.loads(self.run(['state', 'pull']))

    def show_state_json(self):
        """The current state in Terraform's JSON representation."""
        return json.loads(self.run(['show', '-json']))

    def output_json(self):
        return json.loads(self.run(['output', '-json']))
