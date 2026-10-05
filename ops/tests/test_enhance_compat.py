"""Enhance's expansion scripts keep their names, bytes and behavior after the
shared primitives moved to ops/lib.

`expansion-journal.py`, `provision.py` and `bootstrap-pair.py` run from a
checkout (none is in a release bundle), so they re-export `wallet_pir_ops`.
The reference functions below are verbatim copies of the pre-extraction code;
journals, digests and evidence already on disk depend on their exact output.
"""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shlex
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = ROOT / 'enhance/ops/scripts'


def load(name, filename):
    spec = importlib.util.spec_from_file_location(name, SCRIPTS / filename)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


journal = load('compat_expansion_journal', 'expansion-journal.py')
provision = load('compat_provision', 'provision.py')
pair = load('compat_bootstrap_pair', 'bootstrap-pair.py')
from wallet_pir_ops import digitalocean, durable, hostlock, pinned_ssh, terraform  # noqa: E402


def reference_digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()).hexdigest()


def reference_atomic(path, value):
    descriptor, temporary = tempfile.mkstemp(prefix='.journal-', dir=path.parent)
    try:
        with os.fdopen(descriptor, 'w') as handle:
            json.dump(value, handle, sort_keys=True, indent=2, allow_nan=False)
            handle.write('\n')
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


VALUES = [
    {'version': 1, 'operation': None, 'completed': {}},
    {'z': [3, 2.5, -0.0, 10 ** 30], 'a': {'é': '☃', 'nested': [None, True, False]}, 'm': ''},
    [], 'text', 0,
]

# argparse surfaces of the three CLIs before the extraction:
# [option strings, dest, required, default, type, choices, help].
PATH = 'Path'
SURFACES = {
    'expansion-journal': [
        [['--state-dir'], 'state_dir', True, None, PATH, None, None],
        [['--health'], 'health', False, None, PATH, None, 'Captured coordinator health response'],
        [['--coordinator-url'], 'coordinator_url', False, None, None, None, 'Read current health from the coordinator'],
        [['--inventory'], 'inventory', True, None, PATH, None, None],
        [['--policy'], 'policy', True, None, PATH, None, None]],
    'provision': [
        [['--state-dir'], 'state_dir', True, None, PATH, None, None],
        [['--terraform-dir'], 'terraform_dir', True, None, PATH, None, None],
        [['--policy'], 'policy', True, None, PATH, None, None],
        [['--inventory'], 'inventory', True, None, PATH, None, None],
        [['--token-env'], 'token_env', False, 'DIGITALOCEAN_ACCESS_TOKEN', None, None, None]],
    'bootstrap-pair': [
        *[[['--' + name], name.replace('-', '_'), True, None, PATH, None, None]
          for name in ('state-dir', 'terraform-dir', 'policy', 'inventory', 'bootstrap-policy', 'bundle', 'ssh-key', 'known-hosts')],
        [['--token-env'], 'token_env', False, 'DIGITALOCEAN_ACCESS_TOKEN', None, None, None]],
}
DESCRIPTIONS = {
    'expansion-journal': 'Durable, serialized expansion demand and infrastructure identities.',
    'provision': 'Execute an isolated expansion using a pinned Terraform root and journal.',
    'bootstrap-pair': 'Bootstrap a provisioned replica pair, preserving per-host receipts on retry.',
}


class Captured(Exception):
    pass


def surface(module):
    """The parser `main()` builds, captured before it parses anything."""
    def capture(parser, *_args, **_kwargs):
        raise Captured(parser)
    with patch.object(argparse.ArgumentParser, 'parse_args', capture):
        try:
            module.main()
        except Captured as captured:
            parser = captured.args[0]
    actions = [[a.option_strings, a.dest, a.required, a.default, getattr(a.type, '__name__', a.type), a.choices, a.help]
               for a in parser._actions if not isinstance(a, argparse._HelpAction)]
    return parser.description, actions


class DurableCompatibility(unittest.TestCase):
    def test_digest_and_journal_files_are_byte_identical(self):
        self.assertIs(journal.digest, durable.digest)
        for value in VALUES:
            self.assertEqual(journal.digest(value), reference_digest(value))
        with tempfile.TemporaryDirectory() as temp:
            for index, value in enumerate(VALUES):
                expected, actual = Path(temp) / f'reference-{index}', Path(temp) / f'shared-{index}'
                reference_atomic(expected, value)
                journal.atomic(actual, value)
                self.assertEqual(actual.read_bytes(), expected.read_bytes())
                self.assertEqual(actual.stat().st_mode, expected.stat().st_mode)
            with patch.object(durable.tempfile, 'mkstemp', wraps=durable.tempfile.mkstemp) as mkstemp:
                journal.atomic(Path(temp) / 'prefixed', {})
                self.assertEqual(mkstemp.call_args.kwargs['prefix'], '.journal-')
        for bad in [{'x': float('nan')}, {'x': float('inf')}]:
            with self.assertRaises(ValueError):
                journal.digest(bad)

    def test_scripts_import_this_checkouts_library(self):
        self.assertEqual(Path(durable.__file__).resolve().parent, ROOT / 'ops/lib/wallet_pir_ops')
        self.assertIs(provision.journal_module.digest, durable.digest)


class ProvisionCompatibility(unittest.TestCase):
    def test_reexported_names_and_lock_path(self):
        self.assertIs(provision.DigitalOcean, digitalocean.DigitalOcean)
        self.assertTrue(issubclass(provision.StateLock, hostlock.PinnedHostLock))
        self.assertEqual(provision.StateLock.PATH, Path('/run/lock/enhance-pir-terraform.lock'))
        self.assertEqual(provision.StateLock.MACHINE_ID, Path('/etc/machine-id'))
        self.assertEqual(provision.StateLock.ROOT_UID, 0)
        policy = {'state_lock': {'type': 'pinned_host', 'machine_id': 'a' * 32}}
        self.assertEqual(provision.StateLock(policy).config, policy['state_lock'])
        self.assertIsNone(provision.StateLock({}).config)
        self.assertTrue(issubclass(provision.Terraform, terraform.Terraform))
        self.assertTrue(issubclass(pair.Remote, pinned_ssh.PinnedSSH))

    def test_state_lock_refusals_are_unchanged(self):
        with tempfile.TemporaryDirectory() as temp:
            identity = Path(temp) / 'machine-id'
            identity.write_text('a' * 32)
            policy = {'state_lock': {'type': 'pinned_host', 'machine_id': 'b' * 32}}
            with patch.object(provision.StateLock, 'MACHINE_ID', identity):
                with self.assertRaisesRegex(ValueError, '^state writer must run as root on the pinned host$'):
                    with provision.StateLock(policy):
                        pass
            with provision.StateLock({}) as lock:
                self.assertIsNone(lock.fd)

    def test_terraform_commands_environment_and_timeouts_are_unchanged(self):
        policy = {'region': 'ams3', 'vpc_id': 'v', 'project_id': 'p', 'ssh_key_ids': ['1'],
                  'coordinator_private_ipv4': '192.0.2.10', 'operator_ssh_cidrs': ['192.0.2.20/32']}
        with tempfile.TemporaryDirectory() as temp, patch.dict(os.environ, {
                'PATH': '/bin', 'TF_CLI_ARGS': '-x', 'TF_VAR_group_count': '9', 'DIGITALOCEAN_TOKEN': 't'}, clear=True):
            directory = Path(temp)
            tf = provision.Terraform(directory, directory, policy, 2, 'fixture-token')
            self.assertEqual(tf.env, {'PATH': '/bin', 'TF_IN_AUTOMATION': '1', 'TF_INPUT': '0',
                                      'TF_VAR_digitalocean_token': 'fixture-token'})
            self.assertEqual(tf.root, directory.resolve())
            self.assertEqual(json.loads(tf.variables.read_text()), policy)

            def run(command, **kwargs):
                if command[2] == 'plan':
                    Path(command[5][len('-out='):]).write_bytes(b'plan')
                return SimpleNamespace(returncode=0, stdout=b'{"lineage": "x"}')
            chdir = f'-chdir={directory.resolve()}'
            with patch.object(tf, 'verify'), patch.object(provision.subprocess, 'run', side_effect=run) as call:
                path, _ = tf.plan('attempt-1')
                tf.apply(path, hashlib.sha256(b'plan').hexdigest())
                self.assertEqual(tf.state(), {'lineage': 'x'})
            commands = [(c.args[0], c.kwargs) for c in call.call_args_list]
            self.assertEqual([c for c, _ in commands], [
                ['terraform', chdir, 'plan', '-input=false', '-lock-timeout=60s', '-out=' + str(path),
                 '-var-file=' + str(tf.variables), '-var=group_count=2'],
                ['terraform', chdir, 'show', '-json', str(path)],
                ['terraform', chdir, 'apply', '-input=false', '-lock-timeout=60s', str(path)],
                ['terraform', chdir, 'state', 'pull']])
            self.assertEqual([k['timeout'] for _, k in commands], [300, 120, 900, 120])
            for _, kwargs in commands:
                self.assertEqual(kwargs['pass_fds'], ())
                self.assertIs(kwargs['capture_output'], True)
                self.assertEqual(kwargs['env'], tf.env)
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
            self.assertEqual(json.loads((directory / 'attempt-1.plan.json').read_text()), {'lineage': 'x'})

    def test_pinned_policy_requires_the_matching_held_lock(self):
        with tempfile.TemporaryDirectory() as temp:
            policy = {key: 'x' for key in ('region', 'vpc_id', 'project_id', 'ssh_key_ids', 'coordinator_private_ipv4', 'operator_ssh_cidrs')}
            policy['state_lock'] = {'type': 'pinned_host', 'machine_id': 'a' * 32}
            other = provision.StateLock({'state_lock': {'type': 'pinned_host', 'machine_id': 'b' * 32}})
            for lock in (None, other):
                tf = provision.Terraform(Path(temp), Path(temp), policy, 2, 'fixture', lock)
                with patch.object(provision.subprocess, 'run') as run:
                    with self.assertRaisesRegex(ValueError, '^pinned-host state lock is not held$'):
                        tf.run(['workspace', 'show'])
                    run.assert_not_called()
            held = provision.StateLock(policy)
            held.fd = 99
            with patch.object(held, 'verify') as verify, \
                    patch.object(provision.subprocess, 'run', return_value=SimpleNamespace(returncode=0, stdout=b'')) as run:
                provision.Terraform(Path(temp), Path(temp), policy, 2, 'fixture', held).run(['workspace', 'show'])
                verify.assert_called_once_with()
                self.assertEqual(run.call_args.kwargs['pass_fds'], (99,))
            held.fd = None


class BootstrapPairCompatibility(unittest.TestCase):
    def test_remote_invocations_are_unchanged(self):
        with tempfile.TemporaryDirectory() as temp:
            known = Path(temp) / 'known_hosts'
            known.write_text('fixture-host-key')
            digest = pair.installer.sha256(known.read_bytes())
            key = Path(temp) / 'key'
            remote = pair.Remote({'private_ipv4': '10.0.0.3'}, key, known, digest)
            options = ['-F', '/dev/null', '-i', str(key.resolve()), '-o', 'BatchMode=yes',
                       '-o', 'IdentitiesOnly=yes', '-o', 'ForwardAgent=no', '-o', 'ConnectTimeout=10',
                       '-o', 'StrictHostKeyChecking=yes', '-o', 'UserKnownHostsFile=' + str(known.resolve()),
                       '-o', 'GlobalKnownHostsFile=/dev/null']
            ok = SimpleNamespace(returncode=0, stdout=b'ok')
            with patch.object(pair.subprocess, 'run', return_value=ok) as run:
                remote.command(['cloud-init', 'status', '--wait'], timeout=600)
                self.assertEqual(run.call_args.args[0], ['ssh', *options, 'root@10.0.0.3', shlex.join(['cloud-init', 'status', '--wait'])])
                self.assertEqual(run.call_args.kwargs, {'capture_output': True, 'timeout': 600})
                remote.copy([Path('/a'), Path('/b')], '/opt/x/')
                self.assertEqual(run.call_args.args[0], ['scp', *options, '/a', '/b', 'root@10.0.0.3:/opt/x/'])
                self.assertEqual(run.call_args.kwargs, {'capture_output': True, 'timeout': 300})
            with patch.object(pair.subprocess, 'run', return_value=SimpleNamespace(returncode=1, stdout=b'')):
                with self.assertRaisesRegex(RuntimeError, '^remote bootstrap command failed$'):
                    remote.command(['true'])
                with self.assertRaisesRegex(RuntimeError, '^candidate transfer failed$'):
                    remote.copy([], '/x')
            known.write_text('changed')
            with self.assertRaisesRegex(ValueError, '^SSH host-key inventory changed during bootstrap$'):
                remote.command(['true'])
            with self.assertRaisesRegex(ValueError, '^SSH host-key inventory differs from the verified pin$'):
                pair.Remote({'private_ipv4': '10.0.0.3'}, key, known, digest)


class CommandLineCompatibility(unittest.TestCase):
    def test_argument_surfaces_are_unchanged(self):
        for name, module in (('expansion-journal', journal), ('provision', provision), ('bootstrap-pair', pair)):
            with self.subTest(script=name):
                description, actions = surface(module)
                self.assertEqual(description.splitlines()[0], DESCRIPTIONS[name])
                self.assertEqual(actions, SURFACES[name])

    def test_help_runs_from_any_working_directory(self):
        import subprocess
        with tempfile.TemporaryDirectory() as temp:
            for name in SURFACES:
                result = subprocess.run([sys.executable, str(SCRIPTS / (name + '.py')), '--help'],
                                        cwd=temp, capture_output=True, text=True, env=dict(os.environ, COLUMNS='100'))
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertIn(DESCRIPTIONS[name], result.stdout)


if __name__ == '__main__':
    unittest.main()
