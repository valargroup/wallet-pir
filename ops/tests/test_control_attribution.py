"""Fictional worker/coordinator process trees for reconciler control attribution.

The trees are built as /proc-shaped directories so every lineage, identity and
socket mismatch can be exercised without root, sshd or a reconciler; one test
reads a real kernel socket table on Linux.
"""
import hashlib
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]/'lib'))
from wallet_pir_ops import control_attribution as CA, owner_survey as S  # noqa: E402

COORDINATOR_IP, WORKER_IP = '10.142.0.3', '10.142.0.5'
SCOPE = '0::/user.slice/user-0.slice/session-5.scope'
UNIT_SCOPE = '0::/system.slice/'+CA.UNIT


def endpoint(ip, port):
    return socket.inet_aton(ip)[::-1].hex().upper()+':%04X' % port


class Proc:
    """A minimal /proc: stat, status, cmdline, exe, cgroup, children, fds, net."""
    def __init__(self, root):
        self.root = Path(root)
        self.rows = []
        (self.root/'self/ns').mkdir(parents=True)
        os.symlink('net:[4026531840]', self.root/'self/ns/net')
        (self.root/'stat').write_text('cpu 0\nbtime %d\n' % int(time.time()+3600))
        (self.root/'sys/kernel/random').mkdir(parents=True)
        (self.root/'sys/kernel/random/boot_id').write_text('00000000-0000-0000-0000-000000000001\n')
        self.pids = set()

    def add(self, pid, *, ppid, start, exe, argv, cgroup, pgid=None, session=None, uid=0, children=(), sockets=()):
        base = self.root/str(pid)
        if base.exists():
            for path in sorted(base.rglob('*'), reverse=True):
                path.unlink() if path.is_file() or path.is_symlink() else path.rmdir()
            base.rmdir()
        (base/'task'/str(pid)).mkdir(parents=True)
        (base/'fd').mkdir()
        (base/'ns').mkdir()
        (base/'net').mkdir()
        os.symlink('net:[4026531840]', base/'ns/net')
        fields = ['S', ppid, pgid or pid, session or pid, 0, -1, 4194560]+[0]*12+[start, 0, 0]
        (base/'stat').write_text('%d (%s) %s\n' % (pid, Path(exe).name[:15], ' '.join(map(str, fields))))
        (base/'status').write_text('Name:\tx\nUid:\t%d\t%d\t%d\t%d\n' % (uid, uid, uid, uid))
        (base/'cmdline').write_bytes(b''.join(a.encode()+b'\0' for a in argv) if isinstance(argv, list) else argv)
        os.symlink(exe, base/'exe')
        (base/'cgroup').write_text(cgroup+'\n')
        (base/'task'/str(pid)/'children').write_text(''.join('%d ' % c for c in children))
        for number, inode in enumerate(sockets, 3):
            os.symlink('socket:[%d]' % inode, base/'fd'/str(number))
        self.pids.add(pid)
        self.flush()

    def tcp(self, inode, local, remote, state=CA.ESTABLISHED):
        self.rows.append('%4d: %s %s %s 00000000:00000000 00:00000000 00000000     0        0 %d 1 0 20 4 30 10 -1'
                         % (len(self.rows), endpoint(*local), endpoint(*remote), state, inode))
        self.flush()

    def flush(self):
        header = '  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode\n'
        for pid in self.pids:
            (self.root/str(pid)/'net/tcp').write_text(header+''.join(r+'\n' for r in self.rows))
            (self.root/str(pid)/'net/tcp6').write_text(header)


class Worker(unittest.TestCase):
    """A reconciler-issued control on a worker: sshd session -> bash -c -> shard-control."""
    def setUp(self):
        scratch = tempfile.TemporaryDirectory()
        self.addCleanup(scratch.cleanup)
        self.proc = Proc(scratch.name)
        self.build()

    def build(self, *, control=None, shell=None, scope=SCOPE, parent='/usr/sbin/sshd', starts=(1000, 1001, 1002),
              control_children=(), uid=0):
        p = self.proc
        p.add(100, ppid=99, start=starts[0], exe=parent, argv=['sshd: root@notty'], cgroup=scope, session=99, pgid=99,
              children=(101,), sockets=(555,))
        p.add(101, ppid=100, start=starts[1], exe='/usr/bin/bash', argv=shell or ['bash', '-c', CA.CONTROL_COMMAND],
              cgroup=scope, children=(102,))
        p.add(102, ppid=101, start=starts[2], exe=CA.CONTROL[0], argv=control or list(CA.CONTROL), cgroup=scope,
              pgid=101, session=101, children=control_children, uid=uid)
        # The fixture's exe is a link name only; its bytes are this fictional binary.
        self.binary = p.root/'shard-control-bytes'
        self.binary.write_bytes(b'fictional shard-control')
        if not p.rows:
            p.tcp(555, (WORKER_IP, 22), (COORDINATOR_IP, 40000))

    def observe(self, digest=None, pids=None):
        def bytes_of(pid, proc, tick):
            return hashlib.sha256(self.binary.read_bytes()).hexdigest()
        with patch.object(CA, 'CONTROL_EXE_SHA256', digest or hashlib.sha256(b'fictional shard-control').hexdigest()), \
                patch.object(CA, 'executable_sha256', bytes_of):
            return CA.controls(proc=self.proc.root, shell='/usr/bin/bash',pids=pids)

    def rejected(self, reason):
        found, rejected = self.observe()
        self.assertEqual(found, [])
        self.assertEqual([r['pid'] for r in rejected], [102])
        self.assertIn(reason, rejected[0]['reason'])

    def test_exact_reconciler_shaped_control_is_pending_with_its_connection(self):
        found, rejected = self.observe()
        self.assertEqual(rejected, [])
        self.assertEqual(found, [{'pid': 102, 'start_ticks': 1002, 'exe': CA.CONTROL[0],
                                  'command_sha256': CA.CONTROL_SHA256, 'cgroup': SCOPE,
                                  'shell': {'pid': 101, 'start_ticks': 1001}, 'sshd': {'pid': 100, 'start_ticks': 1000},
                                  'connection': {'local': [WORKER_IP, 22], 'remote': [COORDINATOR_IP, 40000]}}])
        CA.verify_controls(found)

    def test_controls_use_the_processes_from_the_complete_owner_scan(self):
        # This client may have started after a preliminary candidate listing.
        found,rejected=self.observe(pids=[102])
        self.assertEqual(([c['pid'] for c in found],rejected),([102],[]))

    def test_extra_or_changed_control_argv_is_rejected(self):
        self.build(control=[*CA.CONTROL, '--extra'])
        self.rejected('control argv differs')
        self.build(control=b'/usr/local/bin/shard-control\0/run/transparent-pir/control.sock')
        self.rejected('control argv differs')

    def test_unrelated_caller_shell_or_session_is_rejected(self):
        self.build(shell=['bash', '-c', CA.CONTROL_COMMAND+'; true'])
        self.rejected('shell command differs')
        self.build(shell=['sh', '-c', CA.CONTROL_COMMAND])
        self.rejected('shell command differs')
        self.build(scope='0::/system.slice/cron.service')
        self.rejected('root SSH session scope')
        self.build(parent='/usr/sbin/cron')
        self.rejected('root sshd session')
        self.build(uid=1000)
        self.rejected('not root')
        self.build(control_children=(103,))
        self.rejected('has children')

    def test_root_observed_worker_shape_joins_its_coordinator_client(self):
        # Root's read-only worker-1 sample (diagnostic, not authorization):
        # listener sshd in ssh.service -> session sshd -> bash -> shard-control.
        scope = '0::/user.slice/user-0.slice/session-216762.scope'
        p = self.proc
        p.rows.clear()
        p.add(224472, ppid=1, start=10, exe='/usr/sbin/sshd', argv=['sshd: /usr/sbin/sshd -D [listener]'],
              cgroup='0::/system.slice/ssh.service', children=(3462661,))
        p.add(3462661, ppid=224472, start=2000, exe='/usr/sbin/sshd', argv=['sshd: root@notty'], cgroup=scope,
              children=(3462706,), sockets=(56255379,))
        p.add(3462706, ppid=3462661, start=2001, exe='/usr/bin/bash', argv=['bash', '-c', CA.CONTROL_COMMAND],
              cgroup=scope, children=(3462707,))
        p.add(3462707, ppid=3462706, start=2002, exe=CA.CONTROL[0], argv=list(CA.CONTROL), cgroup=scope,
              pgid=3462706, session=3462706)
        p.rows.append('   0: 0A008E0A:0016 03008E0A:CE46 01 00000000:00000000 00:00000000 00000000     0        0 56255379 1')
        p.flush()
        found = [f for f in self.observe()[0] if f['pid'] == 3462707]
        self.assertEqual(found[0]['connection'], {'local': ['10.142.0.10', 22], 'remote': ['10.142.0.3', 52806]})
        self.assertEqual(hashlib.sha256(b'bash\0-c\0'+CA.CONTROL_COMMAND.encode()+b'\0').hexdigest(),
                         'b331347d4557cd941ec8874a5d273cc1eeba928106073e6384feb2b548ed00b8')
        snapshot = {'kind': CA.KIND, 'status': 'verified', 'machine_id': CA.COORDINATOR, 'monotonic': time.monotonic(),
                    'main': {'pid': 4015133, 'start_ticks': 270361979},
                    'clients': [{'pid': 4100000, 'start_ticks': 270400000, 'connection':
                                 {'local': ['10.142.0.3', 52806], 'remote': ['10.142.0.10', 22]}}]}
        self.assertEqual(CA.attribute(found, [snapshot])[0]['reconciler'], {'pid': 4015133, 'start_ticks': 270361979})
        with self.assertRaises(ValueError):
            CA.attribute(found, [dict(snapshot, clients=[])])

    def test_other_control_binary_bytes_are_rejected(self):
        found, rejected = self.observe(digest='0'*64)
        self.assertEqual((found, [r['reason'] for r in rejected]), ([], ['control executable differs']))

    def test_reused_pid_lineage_is_rejected(self):
        self.build(starts=(1000, 1003, 1002))
        self.rejected('lineage differs')
        self.build(starts=(1004, 1001, 1002))
        self.rejected('sshd lineage differs')

    def test_connection_must_be_one_established_tcp_socket(self):
        self.proc.rows.clear()
        self.build()
        self.proc.rows.clear()
        self.proc.flush()
        self.rejected('exactly one established')
        self.proc.tcp(555, (WORKER_IP, 22), (COORDINATOR_IP, 40000), state='06')
        self.rejected('exactly one established')
        self.proc.rows.clear()
        self.proc.tcp(555, (WORKER_IP, 22), (COORDINATOR_IP, 40000))
        self.proc.add(100, ppid=99, start=1000, exe='/usr/sbin/sshd', argv=['sshd: root@notty'], cgroup=SCOPE,
                      session=99, pgid=99, children=(101,), sockets=(555, 556))
        self.proc.tcp(556, (WORKER_IP, 22), (COORDINATOR_IP, 40001))
        self.rejected('exactly one established')

    def test_one_sshd_connection_cannot_carry_two_controls(self):
        p = self.proc
        p.add(100, ppid=99, start=1000, exe='/usr/sbin/sshd', argv=['sshd: root@notty'], cgroup=SCOPE, session=99,
              pgid=99, children=(101, 111), sockets=(555,))
        p.add(111, ppid=100, start=1001, exe='/usr/bin/bash', argv=['bash', '-c', CA.CONTROL_COMMAND], cgroup=SCOPE,
              children=(112,))
        p.add(112, ppid=111, start=1002, exe=CA.CONTROL[0], argv=list(CA.CONTROL), cgroup=SCOPE, pgid=111, session=111)
        found, rejected = self.observe()
        self.assertEqual([f['pid'] for f in found], [102])
        self.assertEqual([(r['pid'], r['reason']) for r in rejected],
                         [(112, 'one sshd connection carries more than one control')])


class Coordinator(unittest.TestCase):
    """The running reconciler and its direct, unmultiplexed control clients."""
    def setUp(self):
        scratch = tempfile.TemporaryDirectory()
        self.addCleanup(scratch.cleanup)
        root = Path(scratch.name).resolve()
        self.proc = Proc(root/'proc')
        self.script = root/'transparent-live-fleet.py'
        self.script.write_bytes(b'# fictional reconciler script\n')
        self.fragment = root/CA.UNIT
        self.argv = ['/usr/bin/python3', '-B', str(self.script), '/opt/transparent-publisher/fleet.json', '--reconcile']
        self.fragment.write_text('[Service]\nType=simple\nExecStart = %s\nRestart=on-failure\n' % ' '.join(self.argv))
        self.machine = root/'machine-id'
        self.machine.write_text(CA.COORDINATOR+'\n')
        self.unit = {'Id': CA.UNIT, 'ActiveState': 'active', 'SubState': 'running', 'MainPID': '200',
                     'ControlGroup': '/system.slice/'+CA.UNIT, 'FragmentPath': str(self.fragment), 'DropInPaths': '',
                     'NeedDaemonReload': 'no'}
        for name, value in (('FRAGMENT', self.fragment), ('SCRIPT', self.script),
                            ('FRAGMENT_SHA256', hashlib.sha256(self.fragment.read_bytes()).hexdigest()),
                            ('SCRIPT_SHA256', hashlib.sha256(self.script.read_bytes()).hexdigest())):
            p = patch.object(CA, name, value)
            p.start()
            self.addCleanup(p.stop)
        p = patch.object(CA.os, 'geteuid', return_value=0)
        p.start()
        self.addCleanup(p.stop)
        self.proc.add(200, ppid=1, start=500, exe=os.path.realpath('/usr/bin/python3'), argv=self.argv, cgroup=UNIT_SCOPE,
                      children=(201,))
        self.client()

    def client(self, argv=None, *, pid=201, ppid=200, cgroup=UNIT_SCOPE, port=40000, start=600):
        self.proc.add(pid, ppid=ppid, start=start, exe='/usr/bin/ssh', cgroup=cgroup, sockets=(700+pid,), argv=argv or [
            'ssh', '-oBatchMode=yes', '-oConnectTimeout=3', '-oStrictHostKeyChecking=yes', '-o',
            'UserKnownHostsFile=/opt/transparent-publisher/credentials/known_hosts', '-i',
            '/opt/transparent-publisher/credentials/id', '-oControlMaster=no', '-oControlPersist=no',
            '-oControlPath=none', 'root@'+WORKER_IP, CA.CONTROL_COMMAND])
        self.proc.tcp(700+pid, (COORDINATOR_IP, port), (WORKER_IP, 22))

    def snapshot(self, **unit):
        return CA.authority(proc=self.proc.root, show=lambda name: dict(self.unit, **unit), machine_path=self.machine)

    def refused(self, reason, **unit):
        value = self.snapshot(**unit)
        self.assertEqual(value['status'], 'refused')
        self.assertIn(reason, value['reason'])

    def test_running_reconciler_and_direct_client_are_verified(self):
        value = self.snapshot()
        self.assertEqual(value['status'], 'verified', value)
        self.assertEqual(value['main']['pid'], 200)
        self.assertEqual(value['clients'], [{'pid': 201, 'start_ticks': 600, 'destination': WORKER_IP,
                                             'connection': {'local': [COORDINATOR_IP, 40000],
                                                            'remote': [WORKER_IP, 22]}}])

    def test_wrong_machine_unit_fragment_or_script_refuses(self):
        self.machine.write_text('f'*32+'\n')
        self.refused('pinned root coordinator')
        self.machine.write_text(CA.COORDINATOR+'\n')
        self.refused('pinned identity', DropInPaths='/etc/systemd/system/%s.d/override.conf' % CA.UNIT)
        self.refused('pinned identity', ActiveState='deactivating')
        self.refused('pinned identity', ControlGroup='/system.slice/other.service')
        self.refused('pinned identity', NeedDaemonReload='yes')
        with patch.object(CA, 'SCRIPT_SHA256', '0'*64):
            self.refused('pinned bytes')
        with patch.object(CA, 'FRAGMENT_SHA256', '0'*64):
            self.refused('pinned bytes')
        # A script replaced after the interpreter started is not the running code.
        (self.proc.root/'stat').write_text('cpu 0\nbtime %d\n' % int(time.time()-3600))
        self.refused('changed after its process started')

    def test_fragment_needs_one_literal_exec_start(self):
        for text in ('ExecStart=%s\nExecStart=%s\n', 'ExecStart=-%s\n%s', 'ExecStart="%s"\n%s', 'ExecStart=%s $X\n%s'):
            fragment = (text % (' '.join(self.argv), '')).encode()
            with self.assertRaises(ValueError):
                CA.exec_start(fragment)
        self.assertEqual(CA.exec_start(self.fragment.read_bytes()), self.argv)

    def test_main_process_must_match_exact_exec_start(self):
        self.proc.add(200, ppid=1, start=500, exe=os.path.realpath('/usr/bin/python3'), argv=[*self.argv, '--extra'],
                      cgroup=UNIT_SCOPE, children=(201,))
        self.refused('differs from its unit')
        self.proc.add(200, ppid=1, start=500, exe=os.path.realpath('/usr/bin/python3'), argv=self.argv,
                      cgroup='0::/system.slice/other.service', children=(201,))
        self.refused('differs from its unit')
        self.proc.add(200, ppid=1, start=500, exe=os.path.realpath('/usr/bin/python3'), argv=self.argv, cgroup=UNIT_SCOPE,
                      children=(201,))
        self.refused('main process absent', MainPID='999')

    def test_lookalike_interpreter_is_not_the_unit_executable(self):
        self.proc.add(200, ppid=1, start=500, exe='/usr/bin/python3-foreign', argv=self.argv,
                      cgroup=UNIT_SCOPE, children=(201,))
        self.refused('differs from its unit')

    def test_multiplexed_foreign_or_reparented_clients_are_not_clients(self):
        argv = ['ssh', '-oBatchMode=yes', '-oConnectTimeout=3', '-oStrictHostKeyChecking=yes', '-o',
                'UserKnownHostsFile=/k', '-i', '/id', '-oControlMaster=no', '-oProxyCommand=false',
                '-oControlPath=/opt/transparent-publisher/state/ssh/c-0', 'root@'+WORKER_IP, CA.CONTROL_COMMAND]
        for case in ({'argv': argv}, {'cgroup': '0::/system.slice/transparent-control-sessions.service'},
                     {'ppid': 1}, {'argv': ['ssh', 'root@'+WORKER_IP, CA.CONTROL_COMMAND]},
                     {'argv': b'ssh\0'+b'x'*5000+b'\0'}):
            self.client(**case)
            self.assertEqual(self.snapshot()['clients'], [], case)
        # A client still connecting is not listed and does not void the snapshot.
        self.proc.rows.clear()
        self.client()
        self.proc.rows[-1] = self.proc.rows[-1].replace(' 01 ', ' 02 ')
        self.proc.flush()
        value = self.snapshot()
        self.assertEqual((value['status'], value['clients']), ('verified', []))
        self.client(start=400)
        self.refused('predates its parent')


class Attribution(unittest.TestCase):
    control = {'pid': 102, 'start_ticks': 1002, 'exe': CA.CONTROL[0], 'command_sha256': CA.CONTROL_SHA256,
               'cgroup': SCOPE, 'shell': {'pid': 101, 'start_ticks': 1001}, 'sshd': {'pid': 100, 'start_ticks': 1000},
               'connection': {'local': [WORKER_IP, 22], 'remote': [COORDINATOR_IP, 40000]}}

    def snapshot(self, port=40000, client=201, main=200, age=0, status='verified'):
        return {'kind': CA.KIND, 'status': status, 'machine_id': CA.COORDINATOR, 'monotonic': time.monotonic()-age,
                'main': {'pid': main, 'start_ticks': 500},
                'clients': [{'pid': client, 'start_ticks': 600,
                             'connection': {'local': [COORDINATOR_IP, port], 'remote': [WORKER_IP, 22]}}]}

    def test_exact_reverse_connection_of_a_verified_client_attributes(self):
        result = CA.attribute([self.control], [self.snapshot(), self.snapshot()])
        self.assertEqual(result[0]['client'], {'pid': 201, 'start_ticks': 600})
        self.assertEqual(result[0]['reconciler'], {'pid': 200, 'start_ticks': 500})
        # A control that ended before the later snapshot is bound by the earlier one.
        CA.attribute([self.control], [self.snapshot(), self.snapshot(port=40001)])

    def test_unrelated_stale_missing_or_ambiguous_evidence_refuses(self):
        other_host = dict(self.control, connection={'local': [WORKER_IP, 22], 'remote': ['10.142.0.9', 40000]})
        cases = ([self.control], [self.snapshot(port=40001)]), ([other_host], [self.snapshot()]), \
                ([self.control], [self.snapshot(age=CA.SECONDS+1)]), ([self.control], []), \
                ([self.control], [self.snapshot(status='refused')]), \
                ([self.control], [self.snapshot(), self.snapshot(client=202)]), \
                ([self.control], [self.snapshot(), self.snapshot(main=300)]), \
                ([self.control, dict(self.control, pid=112, sshd={'pid': 110, 'start_ticks': 1})], [self.snapshot()])
        for found, snapshots in cases:
            with self.assertRaises(ValueError):
                CA.attribute(found, snapshots)
        with self.assertRaises(ValueError):
            CA.attribute([self.control], [dict(self.snapshot(), machine_id='f'*32)])

    def test_reported_control_shape_is_closed(self):
        for change in ({'exe': '/tmp/shard-control'}, {'command_sha256': '0'*64}, {'cgroup': UNIT_SCOPE},
                       {'extra': 1}, {'connection': {'local': [WORKER_IP, 22]}}):
            with self.assertRaises(ValueError):
                CA.verify_controls([dict(self.control, **change)])
        with self.assertRaises(ValueError):
            CA.verify_controls([self.control, self.control])


class Survey(unittest.TestCase):
    """owner_survey lists only an exact pending identity apart from unattributed processes."""
    def process(self, **change):
        item = {'pid': 4242, 'comm': 'shard-control', 'state': 'S', 'ppid': 4241, 'pgid': 4241, 'session': 4241,
                'start_ticks': 1002, 'kernel': False, 'uid': 0, 'token': None, 'holds': False, 'receiver': False,
                'unreadable': False, 'cgroup': SCOPE[:-1]+'5.scope', 'exe': CA.CONTROL[0], 'argv': list(CA.CONTROL),
                'command_sha256': CA.CONTROL_SHA256, 'command_bytes': 62, 'arguments_overflow': False}
        item.update(change)
        return item

    def observe(self, process, pending):
        with tempfile.TemporaryDirectory() as directory, patch.object(S, 'scan', return_value=[process]), \
                patch.object(S, 'boot_id', return_value='00000000-0000-0000-0000-000000000001'), \
                patch.object(S, 'booted_unix', return_value=time.time()-3600):
            return S.observe((('owners', Path(directory)/'absent'),), classes={'names': ['shard-control'], 'roots': []},
                             baseline=('transparent-shard-server.service',), binding={}, pending=pending)

    def test_pending_callback_receives_this_scan_not_an_earlier_listing(self):
        item=self.process();observed=[]
        def candidates(processes):
            observed.extend(processes)
            return {p['pid']:{k:p[k] for k in ('start_ticks','exe','command_sha256','cgroup')} for p in processes}
        value=self.observe(item,candidates)
        self.assertEqual(observed,[item])
        self.assertEqual((value['pending_count'],value['unattributed_count']),(1,0))

    def test_exact_pending_is_listed_apart_and_any_difference_is_unattributed(self):
        item = self.process()
        identity = {k: item[k] for k in ('start_ticks', 'exe', 'command_sha256', 'cgroup')}
        value = self.observe(item, {4242: identity})
        self.assertEqual((value['pending_count'], value['unattributed_count'], value['blocked_count']), (1, 0, 0))
        for change in ({'start_ticks': 1003}, {'command_sha256': '0'*64}, {'unreadable': True},
                       {'cgroup': '0::/system.slice/transparent-shard-server.service'}):
            value = self.observe(self.process(**change), {4242: identity})
            self.assertEqual(value['pending_count'], 0, change)
            self.assertTrue(value['blocked_count'] >= 1, change)
        value = self.observe(item, None)
        self.assertNotIn('pending', value)
        self.assertEqual(value['unattributed_count'], 1)


@unittest.skipUnless(Path('/proc/self/net/tcp').is_file(), 'Linux kernel socket table')
class Kernel(unittest.TestCase):
    def test_real_established_connection_is_read_from_the_owning_process(self):
        server = socket.socket()
        server.bind(('127.0.0.1', 0))
        server.listen(1)
        self.addCleanup(server.close)
        code = ('import socket,sys,time\ns=socket.create_connection(("127.0.0.1",%d))\n'
                'sys.stdout.write("ready\\n");sys.stdout.flush();sys.stdin.read()' % server.getsockname()[1])
        child = subprocess.Popen([sys.executable, '-c', code], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                 start_new_session=True)
        self.addCleanup(child.wait)
        self.addCleanup(child.stdin.close)
        self.addCleanup(child.stdout.close)
        accepted, peer = server.accept()
        self.addCleanup(accepted.close)
        self.assertEqual(child.stdout.readline(), b'ready\n')
        link = CA.connection(child.pid, Path('/proc'))
        self.assertEqual(link, {'local': list(peer), 'remote': list(server.getsockname())})
        with open(os.path.realpath(sys.executable), 'rb') as stream:
            expected = hashlib.sha256(stream.read()).hexdigest()
        self.assertEqual(CA.executable_sha256(child.pid, Path('/proc'), lambda: None), expected)
        # The real host has no reconciler-shaped control and never raises.
        found, _ = CA.controls()
        self.assertEqual(found, [])
        self.assertEqual(CA.authority(machine_path=Path('/proc/sys/kernel/random/boot_id'))['status'], 'refused')


if __name__ == '__main__':
    unittest.main()
