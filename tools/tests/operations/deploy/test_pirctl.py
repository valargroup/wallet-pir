"""pirctl and pir-deploy against a fake host: releases, restarts, probes and rollback.

Each host is a directory standing in for `/`. A fake `systemctl` starts a unit by
resolving its ExecStart under that directory, records the process as
`proc/<pid>/exe`, and writes the health document the executable's marker files ask
for. Releases are served from a local directory through `file://` URLs.
"""
import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import tarfile
import tempfile
import time
import unittest

ROOT = Path(__file__).resolve().parents[4]
PIRCTL = ROOT / 'tools/deploy/pirctl'
DRIVER = ROOT / 'tools/deploy/pir-deploy'
SHA_A = 'a' * 40
SHA_B = 'b' * 40

SYSTEMCTL = r'''#!/usr/bin/env python3
import json, os, re, sys
from pathlib import Path
root = Path(os.environ['PIRCTL_ROOT'])
proc = Path(os.environ['PIRCTL_PROC'])
state_file = root / 'state.json'
state = json.loads(state_file.read_text()) if state_file.exists() else {}
args = [a for a in sys.argv[1:] if a not in ('--quiet', '--value')]
units = root / 'etc/systemd/system'

def unit_state(unit):
    return state.setdefault(unit, {'pid': 0, 'failed': False, 'restarts': 0})

def exec_start(unit):
    lines = [l for l in (units / unit).read_text().splitlines() if l.startswith('ExecStart=') and l != 'ExecStart=']
    for dropin in sorted((units / (unit + '.d')).glob('*.conf')) if (units / (unit + '.d')).exists() else []:
        lines += [l for l in dropin.read_text().splitlines() if l.startswith('ExecStart=') and l != 'ExecStart=']
    words = lines[-1][len('ExecStart='):].split()
    # A credential wrapper runs `python3 <wrapper> <binary>` and execs the binary.
    return words[2] if words[0].endswith('python3') else words[0]

command = args[0]
if command == 'restart':
    unit = args[1]
    s = unit_state(unit)
    binary = Path(os.path.realpath(str(root) + exec_start(unit)))
    s['pid'] = max(v['pid'] for v in state.values()) + 1
    s['restarts'] += 1
    s['failed'] = (binary.parent / 'CRASH').exists()
    (proc / str(s['pid'])).mkdir(parents=True, exist_ok=True)
    os.symlink(binary, proc / str(s['pid']) / 'exe')
    ready = not s['failed'] and not (binary.parent / 'NOT_READY').exists()
    (root / 'health').mkdir(exist_ok=True)
    (root / 'health' / (unit + '.json')).write_text(json.dumps({'ready': ready}))
elif command == 'show':
    prop, unit = args[2], args[3]
    if prop == 'MainPID':
        print(unit_state(unit)['pid'])
    elif prop == 'FragmentPath':
        print('/etc/systemd/system/' + unit)
    elif prop == 'DropInPaths':
        d = units / (unit + '.d')
        print(' '.join('/etc/systemd/system/%s.d/%s' % (unit, p.name) for p in sorted(d.glob('*.conf'))) if d.exists() else '')
elif command == 'is-failed':
    sys.exit(0 if unit_state(args[1])['failed'] else 1)
elif command == 'is-active':
    s = unit_state(args[1])
    print('active' if s['pid'] and not s['failed'] else 'inactive')
    sys.exit(0 if s['pid'] and not s['failed'] else 3)
elif command == 'cat':
    unit = args[1]
    print((units / unit).read_text())
    d = units / (unit + '.d')
    for p in sorted(d.glob('*.conf')) if d.exists() else []:
        print(p.read_text())
elif command == 'daemon-reload':
    pass
else:
    sys.exit('fake systemctl: unsupported ' + ' '.join(sys.argv[1:]))
state_file.write_text(json.dumps(state))
'''

EXECUTABLE = '''#!/bin/sh
dir=$(cd "$(dirname "$0")" && pwd)
if [ "$3" = --check ]; then test ! -e "$dir/BAD_CONFIG"; exit $?; fi
if [ "$1" = --contract-info ]; then echo '{"contract_major": 1}'; fi
'''

PROBE = '''#!/bin/sh
dir=$(cd "$(dirname "$0")" && pwd)
echo "probe $*" >> "${PROBE_LOG:-/dev/null}"
test ! -e "$dir/PROBE_FAILS"
'''


def write_executable(path, text):
    path.write_text(text)
    path.chmod(0o755)


class Fleet:
    """Fake hosts, a release directory and the PATH the scripts run with."""

    def __init__(self, base):
        self.base = Path(os.path.realpath(base))
        self.bin = self.base / 'bin'
        self.bin.mkdir()
        write_executable(self.bin / 'systemctl', SYSTEMCTL)
        if not shutil.which('flock'):
            write_executable(self.bin / 'flock', '#!/bin/sh\nexit 0\n')
        self.releases = self.base / 'releases'
        self.hosts = self.base / 'hosts'

    def release(self, sha, *, markers=(), probe=True, major=1):
        directory = self.releases / ('build-' + sha[:12])
        directory.mkdir(parents=True)
        archives = {asset: {asset: EXECUTABLE + '# ' + sha + '\n', 'revision': sha + '\n'}
                    for asset in ('pir-publisher', 'pir-query-server', 'pir-apm')}
        if probe:
            archives['pir-probe'] = {'pir-probe': PROBE}
        for marker in markers:
            archives['pir-probe' if marker == 'PROBE_FAILS' else 'pir-query-server'][marker] = ''
        sums = []
        for asset, members in archives.items():
            data = io.BytesIO()
            with tarfile.open(fileobj=data, mode='w:gz') as archive:
                for name, text in members.items():
                    info = tarfile.TarInfo(name)
                    info.size = len(text.encode())
                    info.mode = 0o755
                    archive.addfile(info, io.BytesIO(text.encode()))
            (directory / (asset + '.tar.gz')).write_bytes(data.getvalue())
            sums.append('%s  %s.tar.gz' % (hashlib.sha256(data.getvalue()).hexdigest(), asset))
        contract = {'publisher': {'contract_major': major}, 'query_server': {'contract_major': major}}
        (directory / 'contract.json').write_text(json.dumps(contract))
        (directory / 'SHA256SUMS').write_text('\n'.join(sums) + '\n')

    def host(self, name, components):
        root = self.hosts / name
        units = root / 'etc/systemd/system'
        units.mkdir(parents=True)
        (root / 'etc/wallet-pir').mkdir(parents=True)
        lines = []
        for component, asset, products in components:
            unit = component + '.service'
            (units / unit).write_text('[Service]\nExecStart=/opt/wallet-pir/%s/current/%s --config /etc/%s.json\n'
                                      % (component, asset, component))
            line = '%s unit=%s asset=%s config=/etc/%s.json health=file://%s/health/%s.json' % (
                component, unit, asset, component, root, unit)
            if products:
                line += ' probe=http://127.0.0.1:8090 products=' + products
            lines.append(line)
        (root / 'etc/wallet-pir/pirctl.conf').write_text('\n'.join(lines) + '\n')
        return root

    def env(self, root):
        return dict(os.environ, PATH='%s:%s' % (self.bin, os.environ['PATH']), PIRCTL_ROOT=str(root),
                    PIRCTL_PROC=str(root / 'proc'), PIRCTL_RELEASES='file://%s' % self.releases,
                    PIRCTL_READY_SECONDS='2', PROBE_LOG=str(root / 'probes.log'))

    def pirctl(self, root, *args):
        return subprocess.run([str(PIRCTL), *args], env=self.env(root), capture_output=True, text=True, timeout=60)

    def current(self, root, component):
        link = root / 'opt/wallet-pir' / component / 'current'
        return Path(os.readlink(link)).name if link.is_symlink() else None

    def restarts(self, root, unit):
        state = json.loads((root / 'state.json').read_text()) if (root / 'state.json').exists() else {}
        return state.get(unit, {}).get('restarts', 0)


class PirctlTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.fleet = Fleet(self.temp.name)
        self.host = self.fleet.host('query', [('query', 'pir-query-server', 'enhance,status')])

    def tearDown(self):
        self.temp.cleanup()

    def deploy(self, sha):
        return self.fleet.pirctl(self.host, 'deploy', 'query', sha)

    def test_a_release_without_a_probe_is_verified_by_readiness_alone(self):
        self.fleet.release(SHA_A, probe=False)
        result = self.deploy(SHA_A)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('no pir-probe; readiness only', result.stdout)

    def test_a_service_without_config_or_ready_field_deploys_on_a_healthy_answer(self):
        units = self.host / 'etc/systemd/system'
        (units / 'apm.service').write_text('[Service]\nExecStart=/opt/wallet-pir/apm/current/pir-apm\n')
        (self.host / 'health').mkdir(exist_ok=True)
        (self.host / 'health/apm.txt').write_text('ok\n')
        with open(self.host / 'etc/wallet-pir/pirctl.conf', 'a') as conf:
            conf.write('apm unit=apm.service asset=pir-apm health=file://%s/health/apm.txt\n' % self.host)
        self.fleet.release(SHA_A, markers=['BAD_CONFIG'])
        result = self.fleet.pirctl(self.host, 'deploy', 'apm', SHA_A)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.fleet.current(self.host, 'apm'), SHA_A[:12])

    def test_deploy_switches_restarts_and_probes_the_new_release(self):
        self.fleet.release(SHA_A)
        result = self.deploy(SHA_A)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.fleet.current(self.host, 'query'), SHA_A[:12])
        self.assertIn('probe passed', result.stdout)
        self.assertIn('--products enhance,status', (self.host / 'probes.log').read_text())

    def test_redeploying_the_running_release_does_not_restart(self):
        self.fleet.release(SHA_A)
        self.assertEqual(self.deploy(SHA_A).returncode, 0)
        result = self.deploy(SHA_A)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('already running', result.stdout)
        self.assertEqual(self.fleet.restarts(self.host, 'query.service'), 1)

    def test_a_refused_configuration_never_restarts_the_service(self):
        self.fleet.release(SHA_A)
        self.fleet.release(SHA_B, markers=['BAD_CONFIG'])
        self.assertEqual(self.deploy(SHA_A).returncode, 0)
        result = self.deploy(SHA_B)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('refuses', result.stderr)
        self.assertEqual(self.fleet.restarts(self.host, 'query.service'), 1)
        self.assertEqual(self.fleet.current(self.host, 'query'), SHA_A[:12])

    def test_a_failing_probe_restores_the_previous_release(self):
        self.fleet.release(SHA_A)
        self.fleet.release(SHA_B, markers=['PROBE_FAILS'])
        self.assertEqual(self.deploy(SHA_A).returncode, 0)
        result = self.deploy(SHA_B)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('rolled back', result.stderr)
        self.assertEqual(self.fleet.current(self.host, 'query'), SHA_A[:12])

    def test_a_release_that_never_becomes_ready_is_rolled_back(self):
        self.fleet.release(SHA_A)
        self.fleet.release(SHA_B, markers=['NOT_READY'])
        self.assertEqual(self.deploy(SHA_A).returncode, 0)
        started = time.monotonic()
        result = self.deploy(SHA_B)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('not ready within 2s', result.stderr)
        self.assertLess(time.monotonic() - started, 10)
        self.assertEqual(self.fleet.current(self.host, 'query'), SHA_A[:12])

    def test_a_crashing_release_is_rolled_back_without_waiting(self):
        self.fleet.release(SHA_A)
        self.fleet.release(SHA_B, markers=['CRASH'])
        self.assertEqual(self.deploy(SHA_A).returncode, 0)
        result = self.deploy(SHA_B)
        self.assertIn('failed to start', result.stderr)
        self.assertEqual(self.fleet.current(self.host, 'query'), SHA_A[:12])

    def test_rollback_swaps_current_and_previous(self):
        self.fleet.release(SHA_A)
        self.fleet.release(SHA_B)
        self.deploy(SHA_A)
        self.deploy(SHA_B)
        result = self.fleet.pirctl(self.host, 'rollback', 'query')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.fleet.current(self.host, 'query'), SHA_A[:12])
        self.fleet.pirctl(self.host, 'rollback', 'query')
        self.assertEqual(self.fleet.current(self.host, 'query'), SHA_B[:12])

    def test_a_tampered_archive_is_refused(self):
        self.fleet.release(SHA_A)
        archive = self.fleet.releases / ('build-' + SHA_A[:12]) / 'pir-query-server.tar.gz'
        archive.write_bytes(archive.read_bytes() + b'x')
        result = self.deploy(SHA_A)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('checksum mismatch', result.stderr)
        self.assertIsNone(self.fleet.current(self.host, 'query'))

    def test_only_the_three_newest_unused_releases_are_kept(self):
        shas = [c * 40 for c in 'abcde']
        for sha in shas:
            self.fleet.release(sha)
            self.assertEqual(self.deploy(sha).returncode, 0)
            time.sleep(0.01)
        kept = sorted(p.name for p in (self.host / 'opt/wallet-pir/pir-query-server/releases').iterdir())
        self.assertEqual(kept, [s[:12] for s in shas[2:]])

    def test_lease_refuses_a_second_owner_until_it_expires(self):
        self.assertEqual(self.fleet.pirctl(self.host, 'lease', 'take', 'mac', '60').returncode, 0)
        refused = self.fleet.pirctl(self.host, 'lease', 'take', 'ci', '60')
        self.assertEqual(refused.returncode, 3)
        self.assertIn('held by mac', refused.stderr)
        self.assertEqual(self.fleet.pirctl(self.host, 'lease', 'drop', 'mac').returncode, 0)
        self.assertEqual(self.fleet.pirctl(self.host, 'lease', 'take', 'ci', '0').returncode, 0)
        self.assertEqual(self.fleet.pirctl(self.host, 'lease', 'take', 'mac', '60').returncode, 0)

    def test_adopt_moves_a_running_unit_onto_the_release_layout(self):
        old = self.host / 'opt/enhance-pir/releases/9b89/pir-query-server'
        old.parent.mkdir(parents=True)
        write_executable(old, EXECUTABLE)
        units = self.host / 'etc/systemd/system'
        (units / 'query.service').write_text(
            '[Service]\nExecStart=/opt/enhance-pir/releases/9b89/pir-query-server --config /etc/query.json\n')
        (units / 'query.service.d').mkdir()
        (units / 'query.service.d/zz-release.conf').write_text(
            '[Service]\nExecStart=\nExecStart=/opt/enhance-pir/releases/9b89/pir-query-server --config /etc/query.json\n')
        (units / 'query.service.d/limits.conf').write_text('[Service]\nMemoryMax=10G\n')
        subprocess.run(['systemctl', 'restart', 'query.service'], env=self.fleet.env(self.host), check=True)

        result = self.fleet.pirctl(self.host, 'adopt', 'query')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('ExecStart=/opt/wallet-pir/query/current/pir-query-server --config /etc/query.json',
                      (units / 'query.service').read_text())
        self.assertEqual(sorted(p.name for p in (units / 'query.service.d').iterdir()), ['limits.conf'])
        self.assertTrue((self.host / 'etc/wallet-pir/adopted/query.service/zz-release.conf').exists())
        self.assertTrue(self.fleet.current(self.host, 'query').startswith('adopted-'))
        self.assertEqual(self.fleet.restarts(self.host, 'query.service'), 1)

        self.fleet.release(SHA_A)
        self.assertEqual(self.deploy(SHA_A).returncode, 0)
        self.assertEqual(self.fleet.pirctl(self.host, 'rollback', 'query').returncode, 0)
        self.assertTrue(self.fleet.current(self.host, 'query').startswith('adopted-'))

    def unit_with_release_drop_in(self, drop_in):
        for release in ('d374', '9b89'):
            binary = self.host / 'opt/old/releases' / release / 'pir-query-server'
            binary.parent.mkdir(parents=True)
            write_executable(binary, EXECUTABLE + '# ' + release + '\n')
        units = self.host / 'etc/systemd/system'
        (units / 'query.service').write_text(
            '[Service]\nExecStartPre=\nExecStart=/opt/old/releases/d374/pir-query-server --config /etc/query.json\n'
            'MemoryMax=10G\n')
        (units / 'query.service.d').mkdir()
        (units / 'query.service.d/zz-release.conf').write_text(drop_in)
        subprocess.run(['systemctl', 'restart', 'query.service'], env=self.fleet.env(self.host), check=True)
        return units

    def test_adopt_keeps_the_command_a_release_drop_in_overrides(self):
        units = self.unit_with_release_drop_in(
            '[Service]\nExecStart=\nExecStart=/opt/old/releases/9b89/pir-query-server --config /etc/query.json\n')
        result = self.fleet.pirctl(self.host, 'adopt', 'query')
        self.assertEqual(result.returncode, 0, result.stderr)
        unit = (units / 'query.service').read_text()
        self.assertEqual([l for l in unit.splitlines() if l.startswith('ExecStart=')],
                         ['ExecStart=/opt/wallet-pir/query/current/pir-query-server --config /etc/query.json'])
        self.assertIn('MemoryMax=10G', unit)
        self.assertEqual(list((units / 'query.service.d').iterdir()), [])
        adopted = self.host / 'opt/wallet-pir/query/current/pir-query-server'
        self.assertIn('# 9b89', adopted.read_text())

    def test_adopt_keeps_the_other_settings_of_a_drop_in_that_also_sets_the_command(self):
        drop_in = ('[Service]\nExecStart=\nExecStart=/opt/old/releases/9b89/pir-query-server --config /etc/query.json\n'
                   'Nice=5\n')
        units = self.unit_with_release_drop_in(drop_in)
        result = self.fleet.pirctl(self.host, 'adopt', 'query')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual((units / 'query.service.d/zz-release.conf').read_text(), '[Service]\nNice=5\n')
        self.assertEqual((self.host / 'etc/wallet-pir/adopted/query.service/zz-release.conf').read_text(), drop_in)
        self.assertIn('ExecStart=/opt/wallet-pir/query/current/pir-query-server --config /etc/query.json',
                      (units / 'query.service').read_text())

    def test_adopt_keeps_a_credential_wrapper_in_front_of_the_binary(self):
        units = self.unit_with_release_drop_in(
            '[Service]\nExecStart=\nExecStart=/usr/bin/python3 /opt/old/credential-exec.py '
            '/opt/old/releases/9b89/pir-query-server\n')
        result = self.fleet.pirctl(self.host, 'adopt', 'query')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('ExecStart=/usr/bin/python3 /opt/old/credential-exec.py '
                      '/opt/wallet-pir/query/current/pir-query-server\n', (units / 'query.service').read_text())
        self.assertIn('# 9b89', (self.host / 'opt/wallet-pir/query/current/pir-query-server').read_text())


class DriverTest(unittest.TestCase):
    """pir-deploy over a fake ssh that runs pirctl against each host's directory."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.fleet = Fleet(self.temp.name)
        self.coordinator = self.fleet.host('coordinator', [('publisher', 'pir-publisher', '')])
        self.query = self.fleet.host('query', [('query', 'pir-query-server', 'enhance')])
        self.worker = self.fleet.host('worker', [('enhance-worker', 'pir-query-server', '')])
        self.router = self.fleet.host('router', [('enhance-router', 'pir-query-server', 'enhance')])
        ssh = self.fleet.bin / 'ssh'
        write_executable(ssh, '''#!/bin/sh
while [ "$#" -gt 0 ]; do case $1 in -o|-F) shift 2 ;; *) break ;; esac; done
host=$1; shift 2
PIRCTL_ROOT=%(hosts)s/$host PIRCTL_PROC=%(hosts)s/$host/proc exec %(pirctl)s "$@"
''' % {'hosts': self.fleet.hosts, 'pirctl': PIRCTL})
        self.inventory = self.fleet.base / 'inventory'
        self.inventory.write_text('# destination component group wave\n'
                                  'coordinator publisher publisher\n'
                                  'query query unified\n'
                                  'worker enhance-worker enhance 1\n'
                                  'router enhance-router enhance 2\n')
        discovery = self.fleet.base / 'discovery.json'
        discovery.write_text(json.dumps({'service': 'pir-publisher', 'version': '1.0.0'}))
        self.env = dict(self.fleet.env(self.fleet.hosts), PIR_INVENTORY=str(self.inventory),
                        PIR_SSH=str(ssh), PIR_DISCOVERY='file://%s' % discovery,
                        PIR_RELEASES='file://%s' % self.fleet.releases, PIR_PUBLIC='')
        del self.env['PIRCTL_ROOT'], self.env['PIRCTL_PROC']

    def tearDown(self):
        self.temp.cleanup()

    def run_driver(self, *args):
        return subprocess.run([str(DRIVER), *args], env=self.env, capture_output=True, text=True, timeout=120)

    def test_all_rolls_every_component_and_releases_the_lease(self):
        self.fleet.release(SHA_A)
        result = self.run_driver('all', SHA_A)
        self.assertEqual(result.returncode, 0, result.stderr)
        for root, component in ((self.coordinator, 'publisher'), (self.query, 'query'),
                                (self.worker, 'enhance-worker'), (self.router, 'enhance-router')):
            self.assertEqual(self.fleet.current(root, component), SHA_A[:12])
        self.assertIn('== done', result.stdout)
        self.assertFalse((self.coordinator / 'run/wallet-pir/deploy.lease').exists())

    def test_a_deploy_ends_with_the_build_probing_each_public_origin(self):
        self.fleet.release(SHA_A)
        self.env['PIR_PUBLIC'] = 'https://one.example=enhance,status https://two.example=receiver'
        result = self.run_driver('product', 'enhance', SHA_A)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('== public probes passed', result.stdout)
        probes = (self.fleet.hosts / 'probes.log').read_text()
        self.assertIn('--server https://one.example --products enhance,status', probes)
        self.assertIn('--server https://two.example --products receiver', probes)
        self.assertTrue((self.coordinator / 'opt/wallet-pir/pir-probe/releases' / SHA_A[:12] / 'probe/pir-probe').exists())

    def test_a_rollback_on_a_publisher_only_host_probes_with_a_fetched_probe(self):
        self.fleet.release(SHA_A)
        self.fleet.release(SHA_B)
        self.env['PIR_PUBLIC'] = 'https://one.example=enhance'
        for sha in (SHA_A, SHA_B):
            self.assertEqual(self.run_driver('publisher', sha).returncode, 0)
        result = self.run_driver('rollback', 'publisher')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.fleet.current(self.coordinator, 'publisher'), SHA_A[:12])
        self.assertIn('== public probes passed', result.stdout)

    def test_product_touches_only_that_product(self):
        self.fleet.release(SHA_A)
        result = self.run_driver('product', 'enhance', SHA_A)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.fleet.current(self.router, 'enhance-router'), SHA_A[:12])
        self.assertIsNone(self.fleet.current(self.query, 'query'))
        self.assertIsNone(self.fleet.current(self.coordinator, 'publisher'))

    def test_a_contract_major_change_needs_all_or_breaking(self):
        self.fleet.release(SHA_A, major=2)
        refused = self.run_driver('unified', SHA_A)
        self.assertNotEqual(refused.returncode, 0)
        self.assertIn('changes the contract major', refused.stderr)
        self.assertIsNone(self.fleet.current(self.query, 'query'))
        result = self.run_driver('product', 'enhance', SHA_A, '--breaking')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.fleet.current(self.coordinator, 'publisher'), SHA_A[:12])
        self.assertEqual(self.fleet.current(self.query, 'query'), SHA_A[:12])

    def test_a_failed_wave_stops_the_deploy_and_later_waves_never_start(self):
        self.fleet.release(SHA_A)
        self.assertEqual(self.run_driver('product', 'enhance', SHA_A).returncode, 0)
        self.fleet.release(SHA_B, markers=['CRASH'])
        result = self.run_driver('product', 'enhance', SHA_B)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('wave 1 failed on: worker enhance-worker', result.stderr)
        self.assertIn('== FAILED', result.stdout)
        self.assertEqual(self.fleet.current(self.worker, 'enhance-worker'), SHA_A[:12])
        self.assertEqual(self.fleet.current(self.router, 'enhance-router'), SHA_A[:12])
        self.assertEqual(self.fleet.restarts(self.router, 'enhance-router.service'), 1)

    def restart_counts(self):
        return [self.fleet.restarts(root, unit) for root, unit in (
            (self.coordinator, 'publisher.service'), (self.query, 'query.service'),
            (self.worker, 'enhance-worker.service'), (self.router, 'enhance-router.service'))]

    def test_a_dry_run_checks_every_host_and_restarts_nothing(self):
        self.fleet.release(SHA_A)
        self.assertEqual(self.run_driver('all', SHA_A).returncode, 0)
        before = self.restart_counts()
        self.fleet.release(SHA_B)
        result = self.run_driver('--dry-run', 'all', SHA_B)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('== checked 3 components', result.stdout)
        self.assertIn('wave 1 (enhance unified) would restart: query query, worker enhance-worker', result.stdout)
        self.assertIn('enhance-router: probe passed', result.stderr)
        self.assertIn('== dry run passed', result.stdout)
        self.assertEqual(self.restart_counts(), before)
        self.assertEqual(self.fleet.current(self.router, 'enhance-router'), SHA_A[:12])
        self.assertFalse((self.coordinator / 'run/wallet-pir/deploy.lease').exists())

    def test_a_dry_run_names_a_host_whose_config_the_new_build_refuses(self):
        self.fleet.release(SHA_A)
        self.assertEqual(self.run_driver('all', SHA_A).returncode, 0)
        before = self.restart_counts()
        self.fleet.release(SHA_B, markers=['BAD_CONFIG'])
        result = self.run_driver('--dry-run', 'product', 'enhance', SHA_B)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('check failed on: worker enhance-worker router enhance-router', result.stderr)
        self.assertIn('== dry run FAILED', result.stdout)
        self.assertEqual(self.restart_counts(), before)

    def test_a_dry_run_fails_on_a_service_that_stays_not_ready(self):
        self.fleet.release(SHA_A)
        self.assertEqual(self.run_driver('all', SHA_A).returncode, 0)
        (self.router / 'health/enhance-router.service.json').write_text(json.dumps({'ready': False}))
        before = self.restart_counts()
        self.fleet.release(SHA_B)
        result = self.run_driver('--dry-run', 'product', 'enhance', SHA_B)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('enhance-router has not been ready for 2s', result.stderr)
        self.assertIn('check failed on: router enhance-router', result.stderr)
        self.assertEqual(self.restart_counts(), before)

    def test_components_sharing_a_host_deploy_in_turn_and_each_keeps_its_probe(self):
        # The worker unpacks the release first and has no probe of its own.
        self.fleet.host('status', [('status-worker', 'pir-query-server', ''),
                                   ('status-router', 'pir-query-server', 'status')])
        with self.inventory.open('a') as inventory:
            inventory.write('status status-worker status 1\nstatus status-router status 2\n')
        self.fleet.release(SHA_A)
        result = self.run_driver('product', 'status', SHA_A)
        self.assertEqual(result.returncode, 0, result.stderr)
        status = self.fleet.hosts / 'status'
        self.assertEqual(self.fleet.current(status, 'status-worker'), SHA_A[:12])
        self.assertEqual(self.fleet.current(status, 'status-router'), SHA_A[:12])
        self.assertIn('status-router: probe passed', result.stderr)

    def test_a_held_lease_blocks_a_second_deploy(self):
        self.fleet.release(SHA_A)
        self.fleet.pirctl(self.coordinator, 'lease', 'take', 'ci', '600')
        result = self.run_driver('unified', SHA_A)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('lease held by ci', result.stderr)
        self.assertIsNone(self.fleet.current(self.query, 'query'))


if __name__ == '__main__':
    unittest.main()
