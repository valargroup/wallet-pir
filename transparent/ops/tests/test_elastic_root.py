"""The elastic root runner: private variables, digest-bound apply under an
inherited pinned-host lock, credential mapping, state readback and provider
reconciliation. Terraform is a fake executable on PATH; nothing reaches a cloud.

The static checks at the end keep the Terraform root itself within its
contract: recent members only, its own state and lock, the production worker
template plus a pinned host key.
"""
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import stat
import sys
import tempfile
import textwrap
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[3]
SPEC = importlib.util.spec_from_file_location('elastic', ROOT / 'transparent/ops/scaler/elastic.py')
M = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(M)
from wallet_pir_ops import hostlock  # noqa: E402  (elastic.py put ops/lib on sys.path)

TF_ROOT = ROOT / 'ops/infra/digitalocean/transparent-elastic'
MACHINE = 'c' * 32
R5, R6, R7 = (f'transparent-pir-recent-0{i}' for i in (5, 6, 7))
MEMBER = {'size': 's-4vcpu-8gb', 'image': 'ubuntu-24-04-x64'}
KEY = {'private': '-----BEGIN OPENSSH PRIVATE KEY-----\nfixture-private\n-----END OPENSSH PRIVATE KEY-----\n',
       'public': 'ssh-ed25519 AAAAfixture'}
EXTRA = {'region': 'ams3', 'vpc_uuid': '00000000-0000-4000-8000-00000000a142',
         'project_id': '85639967-fecb-4c8d-88be-c0e3dee3f86c', 'ssh_key_ids': ['12345']}
RUNTIME = {'DO_TOKEN_NEW_ORG': 'fixture-do-token', 'WALLET_PIR_TF_STATE_ACCESS_KEY': 'fixture-access',
           'WALLET_PIR_TF_STATE_SECRET_KEY': 'fixture-secret', 'CF_API_TOKEN': 'fixture-cf',
           'WALLET_PIR_DEPLOY_SSH_KEY': 'fixture-deploy-key', 'PIR_APM_SLACK_WEBHOOK_URL': 'https://hooks.invalid/x'}

# Records every call with what it could see: the variables file, the lock and
# the environment. `plan` writes a saved plan; `output` prints FAKE_OUTPUTS.
FAKE_TERRAFORM = textwrap.dedent('''\
    #!{python}
    import fcntl, json, os, stat, sys
    args = sys.argv[1:]
    record = {{'args': args, 'env': {{k: v for k, v in os.environ.items() if not k.startswith('FAKE_')}}}}
    lock = os.environ['FAKE_LOCK']
    if os.path.exists(lock):
        target = os.stat(lock)
        record['inherited'] = False
        for fd in range(3, 256):
            try:
                info = os.fstat(fd)
            except OSError:
                continue
            if (info.st_dev, info.st_ino) == (target.st_dev, target.st_ino):
                record['inherited'] = True
        probe = os.open(lock, os.O_RDONLY)
        try:
            fcntl.flock(probe, fcntl.LOCK_EX | fcntl.LOCK_NB)
            record['held'] = False
        except BlockingIOError:
            record['held'] = True
        finally:
            os.close(probe)
    for arg in args:
        if arg.startswith('-var-file='):
            path = arg[len('-var-file='):]
            record['var_file'] = {{'path': path, 'mode': stat.S_IMODE(os.stat(path).st_mode),
                                  'value': json.load(open(path))}}
    with open(os.environ['FAKE_LOG'], 'a') as log:
        log.write(json.dumps(record) + '\\n')
    command = args[1]
    if command == os.environ.get('FAKE_FAIL'):
        sys.exit(1)
    if command == 'plan':
        out = next(a for a in args if a.startswith('-out='))[len('-out='):]
        with open(out, 'wb') as handle:
            handle.write(b'saved plan ' + json.dumps(record.get('var_file', {{}}).get('value'), sort_keys=True).encode())
    elif command == 'output':
        with open(os.environ['FAKE_OUTPUTS']) as handle:
            sys.stdout.write(handle.read())
    elif command == 'show':
        print(json.dumps({{'format_version': '1.2', 'resource_changes': []}}))
''')


def outputs(members):
    return {'members': {'sensitive': False, 'type': ['map', ['object', {}]], 'value': members}}


def state_member(identity, address):
    return {'id': str(identity), 'ipv4_private': address, 'urn': f'do:droplet:{identity}',
            'size': 's-4vcpu-8gb', 'image': '171234567'}


class Harness:
    """A fake elastic root, lock host and Terraform in one temporary directory."""

    def __init__(self, directory, backend=None):
        self.directory = Path(directory)
        self.root = self.directory / 'root'
        (self.root / '.terraform').mkdir(parents=True)
        config = dict(backend or {'bucket': 'enhance-pir-terraform', 'key': 'transparent-elastic/terraform.tfstate',
                                  'use_lockfile': False})
        (self.root / '.terraform/terraform.tfstate').write_text(json.dumps({'backend': {'type': 's3', 'config': config}}))
        bin_dir = self.directory / 'bin'
        bin_dir.mkdir()
        fake = bin_dir / 'terraform'
        fake.write_text(FAKE_TERRAFORM.format(python=sys.executable))
        fake.chmod(0o755)
        self.log = self.directory / 'terraform.log'
        self.outputs = self.directory / 'outputs.json'
        self.set_state({})
        self.lock = self.directory / 'elastic.lock'
        self.machine_id = self.directory / 'machine-id'
        self.machine_id.write_text(MACHINE + '\n')
        self.env = {**RUNTIME, 'PATH': str(bin_dir) + os.pathsep + os.environ.get('PATH', ''),
                    'FAKE_LOG': str(self.log), 'FAKE_OUTPUTS': str(self.outputs), 'FAKE_LOCK': str(self.lock),
                    'TF_CLI_ARGS_apply': '-auto-approve -lock=false', 'TF_WORKSPACE': 'production',
                    'AWS_PROFILE': 'someone-else', 'AWS_SESSION_TOKEN': 'ambient', 'DIGITALOCEAN_TOKEN': 'ambient'}
        self.work = self.directory / 'work'

    def set_state(self, members):
        self.outputs.write_text(json.dumps(outputs(members) if members is not None else {}))

    def elastic(self, **kwargs):
        kwargs.setdefault('env', self.env)
        kwargs.setdefault('machine_id', MACHINE)
        kwargs.setdefault('work_dir', self.work)
        return M.ElasticRoot(self.root, lock_path=self.lock, **kwargs)

    def calls(self, command=None):
        if not self.log.exists():
            return []
        calls = [json.loads(line) for line in self.log.read_text().splitlines()]
        return [c for c in calls if command is None or c['args'][1] == command]


class ElasticRootTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.h = Harness(self.temp.name)
        # The production lock, with this user standing in for root on a pinned test host.
        for name, value in (('MACHINE_ID', self.h.machine_id), ('ROOT_UID', os.geteuid())):
            patcher = patch.object(hostlock.PinnedHostLock, name, value)
            patcher.start()
            self.addCleanup(patcher.stop)

    def test_plan_writes_private_variables_and_removes_them(self):
        root = self.h.elastic()
        plan_path, digest = root.plan({R5: MEMBER}, {R5: KEY}, EXTRA)
        call, = self.h.calls('plan')
        variables = call['var_file']
        self.assertEqual(variables['mode'], 0o600)
        self.assertEqual(variables['value'], {**EXTRA, 'members': {R5: MEMBER}, 'host_keys': {R5: KEY}})
        self.assertFalse(Path(variables['path']).exists())
        self.assertEqual(Path(variables['path']).parent, self.h.work.resolve())
        self.assertEqual(stat.S_IMODE(self.h.work.stat().st_mode), 0o700)
        self.assertEqual(stat.S_IMODE(plan_path.stat().st_mode), 0o600)
        self.assertEqual(digest, hashlib.sha256(plan_path.read_bytes()).hexdigest())
        self.assertEqual(call['args'][:5], [f'-chdir={self.h.root.resolve()}', 'plan', '-input=false',
                                            '-lock-timeout=60s', '-out=' + str(plan_path)])
        self.assertTrue(call['inherited'] and call['held'])
        self.assertEqual(sorted(p.name for p in self.h.work.iterdir()), [plan_path.name])

    def test_failed_plan_removes_its_files(self):
        root = self.h.elastic(env=dict(self.h.env, FAKE_FAIL='plan'))
        with self.assertRaisesRegex(RuntimeError, 'fenced'):
            root.plan({R5: MEMBER}, {R5: KEY}, EXTRA)
        self.assertEqual(list(self.h.work.iterdir()), [])

    def test_apply_is_bound_to_the_plan_digest_under_the_inherited_lock(self):
        root = self.h.elastic()
        plan_path, digest = root.plan({R5: MEMBER}, {R5: KEY}, EXTRA)
        with self.assertRaisesRegex(ValueError, 'saved plan changed'):
            root.apply(plan_path, 'f' * 64)
        original = plan_path.read_bytes()
        plan_path.write_bytes(original + b' tampered')
        with self.assertRaisesRegex(ValueError, 'saved plan changed'):
            root.apply(plan_path, digest)
        self.assertEqual(self.h.calls('apply'), [])
        plan_path.write_bytes(original)
        root.apply(plan_path, digest)
        call, = self.h.calls('apply')
        self.assertEqual(call['args'], [f'-chdir={self.h.root.resolve()}', 'apply', '-input=false',
                                        '-lock-timeout=60s', str(plan_path)])
        self.assertTrue(call['inherited'], 'Terraform must inherit the lock descriptor')
        self.assertTrue(call['held'], 'the lock must be held while Terraform applies')
        self.assertIsNone(root.runner.lock)
        # Control: reads run without the lock, and the probe sees it released.
        root.state_members()
        read = self.h.calls('output')[-1]
        self.assertEqual((read['inherited'], read['held']), (False, False))

    def test_a_second_writer_is_refused_while_the_lock_is_held(self):
        first, second = self.h.elastic(), self.h.elastic()
        plan_path, digest = first.plan({R5: MEMBER}, {R5: KEY}, EXTRA)
        with first.locked():
            with self.assertRaises(BlockingIOError):
                second.apply(plan_path, digest)
            # Nested use by the holder shares the same descriptor.
            first.apply(plan_path, digest)
        self.assertEqual(len(self.h.calls('apply')), 1)
        with self.assertRaisesRegex(ValueError, 'machine id'):
            self.h.elastic(machine_id=None).apply(plan_path, digest)
        with self.assertRaisesRegex(ValueError, 'pinned host'):
            self.h.elastic(machine_id='d' * 32).apply(plan_path, digest)
        self.assertEqual(len(self.h.calls('apply')), 1)

    def test_credentials_are_mapped_and_ambient_overrides_stripped(self):
        self.h.elastic().state_members()
        env = self.h.calls('output')[0]['env']
        self.assertEqual(env['TF_VAR_digitalocean_token'], 'fixture-do-token')
        self.assertEqual(env['AWS_ACCESS_KEY_ID'], 'fixture-access')
        self.assertEqual(env['AWS_SECRET_ACCESS_KEY'], 'fixture-secret')
        self.assertEqual(env['AWS_EC2_METADATA_DISABLED'], 'true')
        self.assertEqual((env['TF_IN_AUTOMATION'], env['TF_INPUT']), ('1', '0'))
        for name in ('TF_CLI_ARGS_apply', 'TF_WORKSPACE', 'AWS_PROFILE', 'AWS_SESSION_TOKEN', 'DIGITALOCEAN_TOKEN',
                     *RUNTIME):
            self.assertNotIn(name, env)
        for name in ('DO_TOKEN_NEW_ORG', 'WALLET_PIR_TF_STATE_SECRET_KEY'):
            with self.subTest(missing=name), self.assertRaisesRegex(ValueError, name):
                self.h.elastic(env={k: v for k, v in self.h.env.items() if k != name})

    def test_new_members_need_pinned_host_keys(self):
        self.h.set_state({R5: state_member(512345678, '10.142.0.20')})
        root = self.h.elastic()
        with self.assertRaisesRegex(ValueError, 'new members need pinned host keys: ' + R6):
            root.plan({R5: MEMBER, R6: MEMBER}, {}, EXTRA)
        self.assertEqual(self.h.calls('plan'), [])
        # An existing member needs no key; its user data is ignored after creation.
        root.plan({R5: MEMBER, R6: MEMBER}, {R6: KEY}, EXTRA)
        self.assertEqual(self.h.calls('plan')[0]['var_file']['value']['host_keys'], {R6: KEY})

    def test_inputs_are_limited_to_elastic_recent_members(self):
        root = self.h.elastic()
        cases = [({'transparent-pir-archive-01': MEMBER}, {}, EXTRA, 'not an elastic recent replica'),
                 ({'transparent-pir-recent-5': MEMBER}, {}, EXTRA, 'not an elastic recent replica'),
                 ({R5: dict(MEMBER, backups='true')}, {}, EXTRA, 'exactly a size and an image'),
                 ({R5: MEMBER}, {R6: KEY}, EXTRA, 'belong to listed members'),
                 ({R5: MEMBER}, {R5: dict(KEY, public='ssh-rsa AAAA')}, EXTRA, 'ed25519'),
                 ({R5: MEMBER}, {R5: KEY}, dict(EXTRA, members={}), 'extra variables'),
                 ({R5: MEMBER}, {R5: KEY}, dict(EXTRA, digitalocean_token='x'), 'extra variables')]
        for members, keys, extra, message in cases:
            with self.subTest(message=message), self.assertRaisesRegex(ValueError, message):
                root.plan(members, keys, extra)
        self.assertEqual(self.h.calls(), [])

    def test_only_the_elastic_backend_is_accepted(self):
        backends = [({'bucket': 'enhance-pir-terraform', 'key': 'production/terraform.tfstate', 'use_lockfile': False}, 'not the elastic root'),
                    ({'bucket': 'other', 'key': 'transparent-elastic/terraform.tfstate', 'use_lockfile': False}, 'not the elastic root'),
                    ({'bucket': 'enhance-pir-terraform', 'key': 'transparent-elastic/terraform.tfstate', 'use_lockfile': True}, 'locking')]
        for index, (config, message) in enumerate(backends):
            with self.subTest(config=config), tempfile.TemporaryDirectory() as temp:
                harness = Harness(temp, backend=config)
                with self.assertRaisesRegex(ValueError, message):
                    harness.elastic().state_members()
                self.assertEqual(harness.calls(), [])
        root = self.h.elastic()
        (self.h.root / '.terraform/environment').write_text('production')
        with self.assertRaisesRegex(ValueError, 'default workspace'):
            root.state_members()
        (self.h.root / '.terraform/environment').write_text('default')
        for name in ('terraform.tfvars', 'x.auto.tfvars.json', 'override.tf', 'main_override.tf'):
            with self.subTest(name=name):
                (self.h.root / name).write_text('')
                with self.assertRaisesRegex(ValueError, 'unexpected Terraform input'):
                    root.plan({R5: MEMBER}, {R5: KEY}, EXTRA)
                (self.h.root / name).unlink()
        self.assertEqual(self.h.calls(), [])
        (self.h.root / '.terraform/terraform.tfstate').unlink()
        with self.assertRaisesRegex(ValueError, 'not initialized'):
            root.state_members()

    def test_work_directory_must_be_private(self):
        shared = Path(self.temp.name) / 'shared'
        shared.mkdir(mode=0o755)
        shared.chmod(0o755)
        with self.assertRaisesRegex(ValueError, 'private'):
            self.h.elastic(work_dir=shared)
        default = self.h.elastic(work_dir=None)
        self.addCleanup(lambda: default.work_dir.rmdir())
        self.assertEqual(stat.S_IMODE(default.work_dir.stat().st_mode), 0o700)

    def test_state_members_and_their_validation(self):
        self.h.set_state(None)
        self.assertEqual(self.h.elastic().state_members(), {})
        self.h.set_state({})
        self.assertEqual(self.h.elastic().state_members(), {})
        self.h.set_state({R5: state_member(512345678, '10.142.0.20')})
        self.assertEqual(self.h.elastic().state_members(), {R5: state_member(512345678, '10.142.0.20')})
        for bad in [{R5: dict(state_member(1, '10.142.0.20'), urn='do:droplet:2')},
                    {R5: state_member(0, '10.142.0.20')},
                    {R5: state_member(1, '8.8.4.4')},
                    {R5: state_member(1, '')},
                    {'transparent-pir-archive-01': state_member(1, '10.142.0.20')}]:
            with self.subTest(bad=bad):
                self.h.set_state(bad)
                with self.assertRaises(ValueError):
                    self.h.elastic().state_members()
        self.h.outputs.write_text(json.dumps({'other': {'value': 1}}))
        with self.assertRaisesRegex(ValueError, 'members output'):
            self.h.elastic().state_members()

    def test_show_returns_plan_json(self):
        root = self.h.elastic()
        plan_path, _ = root.plan({R5: MEMBER}, {R5: KEY}, EXTRA)
        self.assertEqual(root.show(plan_path), {'format_version': '1.2', 'resource_changes': []})


class Provider:
    def __init__(self, droplets, existing=()):
        self.tagged = droplets
        self.existing = set(existing)
        self.tags = []

    def droplets(self, tag=None):
        self.tags.append(tag)
        return self.tagged

    def droplet(self, identity):
        return {'id': int(identity)} if str(identity) in self.existing else None


def droplet(identity, name, address, status='active'):
    return {'id': identity, 'name': name, 'status': status,
            'networks': {'v4': [{'type': 'public', 'ip_address': '203.0.113.1'},
                                {'type': 'private', 'ip_address': address}]}}


class ReconcileTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.h = Harness(self.temp.name)

    def test_consistent_fleet_ignores_static_and_archive_droplets(self):
        self.h.set_state({R5: state_member(5, '10.142.0.20')})
        provider = Provider([droplet(5, R5, '10.142.0.20'), droplet(1, 'transparent-pir-recent-01', '10.142.0.7'),
                             droplet(9, 'transparent-pir-archive-01', '10.142.0.6')])
        report = self.h.elastic().reconcile(provider, static_ids=[1])
        self.assertTrue(report['consistent'], report)
        self.assertEqual(report['members'], {R5: {'id': '5', 'status': 'active'}})
        self.assertEqual(provider.tags, ['transparent-pir-worker'])

    def test_unknown_missing_mismatched_and_duplicate_droplets_are_reported(self):
        self.h.set_state({R5: state_member(5, '10.142.0.20'), R6: state_member(6, '10.142.0.21'),
                          R7: state_member(7, '10.142.0.22'), 'transparent-pir-recent-08': state_member(8, '10.142.0.23')})
        provider = Provider([droplet(5, R5, '10.142.0.99'),                  # moved address
                             droplet(8, 'transparent-pir-recent-08', '10.142.0.23'),
                             droplet(11, 'transparent-pir-recent-09', '10.142.0.30', 'new'),   # orphan of an apply
                             droplet(12, 'transparent-pir-recent-09', '10.142.0.31')],
                            existing=['7'])                                  # 6 is gone, 7 lost its tag
        report = self.h.elastic().reconcile(provider, static_ids=['8'])
        self.assertFalse(report['consistent'])
        self.assertEqual(report['missing'], [R6])
        self.assertEqual(report['mismatched'], [R5, R7, 'transparent-pir-recent-08'])
        self.assertEqual(report['unknown'], [{'id': '11', 'name': 'transparent-pir-recent-09', 'status': 'new'},
                                             {'id': '12', 'name': 'transparent-pir-recent-09', 'status': 'active'}])
        self.assertEqual(report['duplicates'], ['transparent-pir-recent-09'])


class ElasticTerraformRootTests(unittest.TestCase):
    """The root's source keeps to its contract; `terraform test` covers planning."""

    def test_template_is_the_production_worker_template_plus_a_pinned_host_key(self):
        production = (ROOT / 'ops/infra/digitalocean/production/transparent/cloud-init-transparent-worker.yaml.tftpl').read_text()
        elastic = (TF_ROOT / 'cloud-init-transparent-worker.yaml.tftpl').read_text()
        start = elastic.index('%{ if host_key != null ~}\n')
        end = elastic.index('%{ endif ~}\n', start) + len('%{ endif ~}\n')
        block = elastic[start:end]
        self.assertEqual(elastic[:start] + elastic[end:], production)
        for line in ('ssh_deletekeys: true', 'ed25519_private: ${jsonencode(host_key.private)}',
                     'ed25519_public: ${jsonencode(host_key.public)}'):
            self.assertIn(line, block)

    def test_root_manages_only_member_droplets_and_project_entries(self):
        source = '\n'.join(path.read_text() for path in sorted(TF_ROOT.glob('*.tf')))
        resources = sorted(line.split('"')[1] + '.' + line.split('"')[3]
                           for line in source.splitlines() if line.startswith('resource "'))
        self.assertEqual(resources, ['digitalocean_droplet.recent', 'digitalocean_project_resources.recent'])
        self.assertNotIn('data "', source)
        self.assertNotIn('module "', source)
        self.assertIn('ignore_changes = [image, user_data]', source)
        self.assertIn('tags       = [var.worker_tag]', source)
        self.assertNotIn('prevent_destroy', source)

    def test_backend_is_its_own_state_without_native_locking(self):
        for name in ('backend.tf', 'backend.tf.example'):
            with self.subTest(file=name):
                settings = {line.split('=')[0].strip(): line.split('=', 1)[1].split('#')[0].strip()
                            for line in (TF_ROOT / name).read_text().splitlines()
                            if '=' in line and not line.lstrip().startswith('#')}
                self.assertEqual(settings['key'], '"transparent-elastic/terraform.tfstate"')
                self.assertEqual(settings['bucket'], '"enhance-pir-terraform"')
                self.assertEqual(settings['use_lockfile'], 'false')
                self.assertIn(M.LOCK_PATH, (TF_ROOT / name).read_text())
        self.assertEqual(M.BACKEND['key'], 'transparent-elastic/terraform.tfstate')


if __name__ == '__main__':
    unittest.main()
