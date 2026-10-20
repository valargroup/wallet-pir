#!/usr/bin/env python3
"""Operator rehearsal of every external worker role from unpacked releases.

Run from a copy outside the source checkout:

    rehearse.py --ops UNPACKED_OPERATIONS --binaries DIR --work DIR --out EVIDENCE.json \
        --forbid SOURCE_CHECKOUT [--roles receiver,enhance,history,status,display]

Every deploy, rollback and report goes through the unpacked operations
package's own engine and product adapters; an audit hook refuses any file
open under --forbid, so nothing is read from the source checkout. Binaries
are copies of release builds; fixture controllers, nodes and oracles are the
release's own tools or the rehearsal's fixture programs. Nothing here is a
GPU qualification, and the local executor does not enforce the sandbox.
Status, history and display run --binaries' one `pir-query-server`, so it must
be built with the features of every selected role (`status`,
`transparent-history`, `transparent-detail`); otherwise rehearse those roles
from separate --binaries directories.
"""
import argparse
import http.server
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import threading
import time
import traceback

ARGS = None
# Zcash mainnet's genesis block hash, in RPC display order.
MAINNET_GENESIS = '00040fe8ec8471911baa1db1266ea15dd06b4a8a5c453883c000b031973dce08'


def forbid(prefix):
    prefix = os.path.realpath(prefix) + os.sep

    def hook(event, args):
        if event == 'open' and args and isinstance(args[0], (str, bytes, os.PathLike)):
            path = os.path.realpath(os.fsdecode(args[0]))
            if path.startswith(prefix):
                raise PermissionError('rehearsal must not read the source checkout: %s' % path)
    sys.addaudithook(hook)


def sha256(path):
    import hashlib
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


class Node:
    """A JSON-RPC node: genesis, a tip and the hashes the test sets."""

    def __init__(self, genesis_display):
        self.genesis = genesis_display
        self.hashes = {}
        self.tip = 0
        node = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_POST(self):
                request = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
                method, params = request['method'], request.get('params', [])
                if method == 'getblockcount':
                    result = node.tip
                elif method == 'getblockhash':
                    height = int(params[0])
                    result = node.genesis if height == 0 else node.hashes.get(height, 'ee' * 32)
                else:
                    result = None
                body = json.dumps({'result': result, 'error': None, 'id': request.get('id')}).encode()
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Content-Length', str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args):
                pass

        self.server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        self.url = 'http://127.0.0.1:%d' % self.server.server_address[1]
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def set(self, height, display_hash):
        self.hashes[height] = display_hash
        self.tip = max(self.tip, height + 5)


def last_preparation(status):
    """The worker's last preparation from a control status reply, flat (display) or wrapped (history)."""
    try:
        reply = json.loads(status)
    except ValueError:
        return status
    return reply.get('last_preparation', reply.get('result', {}).get('last_preparation'))


def free_port():
    import socket
    with socket.socket() as s:
        s.bind(('127.0.0.1', 0))
        return s.getsockname()[1]


class Rehearsal:
    def __init__(self, name):
        from wallet_pir_ops.deploy import descriptors
        from local_systemd import LocalSystemd
        self.name = name
        self.work = Path(ARGS.work) / name
        shutil.rmtree(self.work, ignore_errors=True)
        (self.work / 'sys/etc/systemd/system').mkdir(parents=True)
        (self.work / 'sys/opt').mkdir(parents=True)
        (self.work / 'sys/run').mkdir(parents=True)
        self.executor = LocalSystemd(str(self.work / 'sys'), str(self.work / 'logs'))
        self.services = descriptors.load_descriptors(Path(ARGS.ops) / 'tools/deploy/services.toml')
        self.steps = []
        self.started = time.time()

    def step(self, name, **detail):
        detail.update(step=name, seconds=round(time.time() - self.started, 1))
        self.steps.append(detail)
        print('[%s] %s %s' % (self.name, name, json.dumps({k: v for k, v in detail.items() if k != 'step'})),
              flush=True)

    def inventory(self, service, role, settings, listen, exact_check=None, instances=None):
        entries = [{'host': 'local', 'install': True, 'vars': {'listen': l}, 'settings': s,
                    **({'unit': u} if u else {})}
                   for s, l, u in (instances or [(settings, listen, None)])]
        document = {'lock': {'type': 'remote', 'host': 'local'}, 'ssh': {'mode': 'config'}, 'hosts': {'local': {}},
                    'services': {service: {'roles': {role: entries}}}}
        if exact_check:
            document['services'][service]['exact_check'] = {'host': 'local', 'argv': exact_check, 'timeout': 900}
        path = self.work / 'inventory.json'
        path.write_text(json.dumps(document))
        return path

    def deployer(self, service, inventory):
        from wallet_pir_ops.deploy import descriptors
        from wallet_pir_ops.deploy.engine import Deployer
        return Deployer(self.services[service], descriptors.load_inventory(inventory), self.executor,
                        self.work / 'state', lock=lambda: self.executor.hold_lock('local', '/lock'),
                        out=lambda line: print('    ' + line, flush=True), poll_seconds=1.0)

    def report(self, service, inventory):
        from wallet_pir_ops.deploy import descriptors
        from wallet_pir_ops import worker_report
        inventory = descriptors.load_inventory(inventory)
        return worker_report.collect(self.services[service], descriptors.targets(self.services[service], inventory),
                                     self.executor)

    def run(self, *argv, timeout=900, check=True):
        result = subprocess.run([str(a) for a in argv], capture_output=True, text=True, timeout=timeout)
        if check and result.returncode:
            raise RuntimeError('%s failed (%d): %s' % (argv[0], result.returncode, (result.stdout + result.stderr)[-2000:]))
        return result

    def wait(self, what, condition, timeout=600):
        deadline = time.time() + timeout
        while time.time() < deadline:
            value = condition()
            if value:
                return value
            time.sleep(1)
        raise RuntimeError('timed out waiting for ' + what)

    def health(self, url):
        status, body = self.executor.http_get('local', url, timeout=5)
        try:
            return json.loads(body) if status == 200 else None
        except ValueError:
            return None

    def absent(self, unit):
        state = self.executor.probe_unit('local', unit)
        return state['load_state'] == 'not-found' and state['active_state'] != 'active'

    def binary(self, name):
        return str(Path(ARGS.binaries) / name)

    def finish(self):
        self.executor.shutdown()
        return {'role': self.name, 'steps': self.steps}


def receiver():
    r = Rehearsal('receiver-serve-publication')
    fixture = r.binary('rehearsal_fixture')
    binary = r.binary('receiver-directory')
    sha = sha256(binary)
    node = Node(MAINNET_GENESIS)
    publications = r.work / 'publications'
    publications.mkdir()
    stage = lambda name, offset, byte: json.loads(r.run(fixture, 'stage', publications, name, offset, byte).stdout)
    a = stage('a', 1, 161)
    node.set(a['end_height'], a['end_hash'])
    record = r.work / 'input/record/publication.json'
    record.parent.mkdir(parents=True)
    def point(published):
        temporary = record.with_suffix('.tmp')
        temporary.write_text(json.dumps({'directory': published['directory'], 'revision': published['revision']}))
        os.replace(temporary, record)
    point(a)
    port = free_port()
    listen = '127.0.0.1:%d' % port
    origin = 'http://' + listen
    settings = {'config_path': str(r.work / 'input/config.json'), 'publication_record': str(record),
                'publication_root': str(publications), 'state_dir': str(r.work / 'worker-state'),
                'bind': listen, 'rpc_urls': [node.url], 'no_auth': True, 'poll_seconds': 1}
    exact = lambda offset, byte: [fixture, 'lookup', origin, str(offset), str(byte)]
    inventory = r.inventory('receiver', 'serve-publication', settings, listen, exact(1, 161))
    deployer = r.deployer('receiver', inventory)
    baseline = deployer.capture_baseline()
    r.step('baseline', target_load_state=baseline['targets']['serve-publication@local']['load_state'])
    journal = deployer.deploy(sha, binary)
    r.step('fresh-install', transaction=journal.id, status=journal.status, binary_sha256=sha,
           exact_answer='deploy exact_check passed (fixture lookup of the staged payments)')
    health = r.health(origin + '/v1/receiver/health')
    r.step('serving', session=health['serving'], expected_session=a['session'])
    assert health['serving'] == a['session']
    r.executor.systemctl('local', 'restart', 'receiver-serve-publication.service')
    r.wait('restart serving', lambda: (r.health(origin + '/v1/receiver/health') or {}).get('serving') == a['session'])
    r.run(*exact(1, 161))
    r.step('restart', note='re-verified from the record; exact answer after restart')
    b = stage('b', 2, 178)
    node.set(b['end_height'], b['end_hash'])
    point(b)
    r.wait('update serving', lambda: (r.health(origin + '/v1/receiver/health') or {}).get('serving') == b['session'])
    r.run(*exact(2, 178))
    r.step('publication-update', session=b['session'], exact_answer=True)
    record.write_text(json.dumps({'directory': str(publications / 'a'), 'revision': b['revision']}))
    refused = r.wait('refusal', lambda: ((r.health(origin + '/v1/receiver/health') or {}).get('publication') or {}).get('refusal'))
    still = r.health(origin + '/v1/receiver/health')['serving']
    r.run(*exact(2, 178))
    r.step('failed-publication-update', refusal=refused, still_serving=still == b['session'])
    assert still == b['session']
    point(b)
    report = r.report('receiver', inventory)
    r.step('report', report=report)
    # A configuration update the product's own check refuses changes nothing.
    bad = dict(settings, publication_record=str(r.work / 'input/record/missing.json'))
    inventory = r.inventory('receiver', 'serve-publication', bad, listen, exact(2, 178))
    try:
        r.deployer('receiver', inventory).deploy(sha, binary)
        raise AssertionError('a refused configuration deployed')
    except Exception as error:  # noqa: BLE001
        message = str(error)
    r.wait('still serving', lambda: (r.health(origin + '/v1/receiver/health') or {}).get('serving') == b['session'], 30)
    r.step('failed-configuration-update', refused=message[:300], still_serving=True)
    # A real configuration update, then its rollback.
    inventory = r.inventory('receiver', 'serve-publication', dict(settings, poll_seconds=2), listen, exact(2, 178))
    update = r.deployer('receiver', inventory).deploy(sha, binary)
    r.step('configuration-update', transaction=update.id, status=update.status)
    r.deployer('receiver', inventory).rollback(update.id)
    config = json.loads(Path(settings['config_path']).read_text())
    r.wait('serving after rollback', lambda: (r.health(origin + '/v1/receiver/health') or {}).get('serving') == b['session'])
    r.step('rollback-update', poll_seconds=config['poll_seconds'])
    r.deployer('receiver', inventory).rollback(journal.id)
    r.step('rollback-install', absent=r.absent('receiver-serve-publication.service'),
           config_removed=not Path(settings['config_path']).exists(),
           state_kept=(r.work / 'worker-state').exists())
    assert r.absent('receiver-serve-publication.service')
    return r.finish()


def enhance():
    r = Rehearsal('enhance-external-worker')
    binary = r.binary('pir-query-server')
    sha = sha256(binary)
    # The synthetic coordinator needs a replicated group: two external workers.
    listens = ['127.0.0.1:%d' % free_port() for _ in range(2)]
    instances = [({'listen': l, 'data_dir': str(r.work / ('worker-data-%d' % i))}, l, 'enhance-pir-worker-%d.service' % i)
                 for i, l in enumerate(listens)]
    inventory = r.inventory('enhance', 'external-worker', None, None, instances=instances)
    deployer = r.deployer('enhance', inventory)
    deployer.capture_baseline()
    journal = deployer.deploy(sha, binary, skip_exact_check=True)
    r.step('fresh-install', transaction=journal.id, status=journal.status, binary_sha256=sha, workers=2)
    health = r.health('http://%s/internal/health' % listens[0])
    r.step('idle', published=health['published'], incarnation=health['incarnation'])
    r.executor.systemctl('local', 'restart', 'enhance-pir-worker-0.service')
    restarted = r.wait('restart', lambda: r.health('http://%s/internal/health' % listens[0]))
    r.step('restart', new_incarnation=restarted['incarnation'] != health['incarnation'])
    # The fixture controller: the release's own isolated synthetic coordinator
    # reserves, uploads, prepares, activates, commits and checks exact answers.
    groups = r.work / 'workers.json'
    groups.write_text(json.dumps({'groups': [{'name': 'g0', 'replicas': [
        {'name': 'w%d' % i, 'url': 'http://' + l} for i, l in enumerate(listens)]}]}))
    exercise = r.run(r.binary('enhance-pir-exercise'), '--isolated-workers', '--profile', 'smoke', '--data-dir', r.work / 'controller',
                     '--worker-config', groups, '--listen', '127.0.0.1:%d' % free_port(), '--seconds', '120',
                     '--min-publications', '2', '--publication-interval', '20', '--concurrency', '1',
                     timeout=1800, check=False)
    summary = {}
    if (r.work / 'controller/exercise.json').exists():
        summary = json.loads((r.work / 'controller/exercise.json').read_text())
    r.step('controller-publications-and-exact-answers', returncode=exercise.returncode,
           exact_answer=exercise.returncode == 0,
           summary={k: summary.get(k) for k in ('status', 'publications', 'error', 'traffic',
                                                 'exact_boundary_and_retained_probes')},
           output=(exercise.stdout + exercise.stderr)[-600:])
    published = r.health('http://%s/internal/health' % listens[0])
    r.step('published-by-controller', published=published.get('published'), preparation=published.get('preparation'))
    report = r.report('enhance', inventory)
    r.step('report', report=report)
    r.deployer('enhance', inventory).rollback(journal.id)
    r.step('rollback-install', absent=all(r.absent('enhance-pir-worker-%d.service' % i) for i in range(2)))
    return r.finish()


def history():
    r = Rehearsal('transparent-history-external-worker')
    # The release's pir-query-server, built with transparent-history and transparent-detail.
    binary = r.binary('pir-query-server')
    sha = sha256(binary)
    p1 = r.work / 'publications/p1'
    p1.mkdir(parents=True)
    first = json.loads(r.run(r.binary('history_fixture'), p1, 60).stdout)
    port = free_port()
    listen = '127.0.0.1:%d' % port
    url = 'http://' + listen
    settings = {'listen': listen, 'shard_dir': str(p1), 'state_dir': str(r.work / 'worker-state'),
                'cache_bytes': 6 << 30}
    # soak-query reads the expected rows from the publication files under this root.
    exact = [r.binary('soak-query'), '--url', url, '--publications', str(r.work / 'publications'), '--shard', '0',
             '--seconds', '3']
    inventory = r.inventory('transparent-history', 'external-worker', settings, listen, exact)
    deployer = r.deployer('transparent-history', inventory)
    deployer.capture_baseline()
    journal = deployer.deploy(sha, binary)
    r.step('fresh-install', transaction=journal.id, status=journal.status, binary_sha256=sha,
           product_check='pir-query-server --config <staged transparent_history> --verify-only on the staged set',
           exact_answer='deploy exact_check passed (soak-query against the published files)')
    r.executor.systemctl('local', 'restart', 'transparent-history-worker.service')
    r.wait('ready after restart', lambda: r.executor.http_get('local', url + '/v1/ready')[0] == 200)
    r.run(*exact)
    r.step('restart', exact_answer=True)
    # The fixture controller: prepare and activate a new publication over the
    # root-only control socket, as the publication controller does.
    p2 = r.work / 'publications/p2'
    p2.mkdir(parents=True)
    second = json.loads(r.run(r.binary('history_fixture'), p2, 90).stdout)
    socket = r.executor.path('/run/transparent-history-worker/control.sock')
    def control(command):
        result = subprocess.run([r.binary('shard-control'), socket], input=json.dumps(command), capture_output=True,
                                text=True, timeout=600)
        return result.returncode, result.stdout.strip()
    code, prepared = control({'operation': 'prepare', 'expected': first['map_sha256'],
                              'publication': {'directory': str(p2), 'assignment': None,
                                              'map_sha256': second['map_sha256']}})
    r.step('prepare', returncode=code, reply=json.loads(prepared) if prepared.startswith('{') else prepared)
    code, activated = control({'operation': 'activate', 'expected': first['map_sha256'],
                               'map_sha256': second['map_sha256']})
    r.run(*exact)
    r.step('publication-update', returncode=code, exact_answer=True)
    code, refused = control({'operation': 'prepare', 'expected': second['map_sha256'],
                             'publication': {'directory': str(r.work / 'publications/missing'), 'assignment': None,
                                             'map_sha256': '00' * 32}})
    # A prepare evicts the active publication's unpinned runtimes before it
    # loads (foundation behavior), so a loaded-only worker rebuilds them on
    # the next query: the oracle gets a window that covers a rebuild.
    r.run(*exact[:-1], '60')
    status_code, status = control({'operation': 'status'})
    r.step('failed-publication-update', returncode=code, reply=refused[-300:],
           last_preparation=last_preparation(status),
           still_exact=True)
    report = r.report('transparent-history', inventory)
    r.step('report', report=report)
    r.deployer('transparent-history', inventory).rollback(journal.id)
    r.step('rollback-install', absent=r.absent('transparent-history-worker.service'))
    return r.finish()


def status_worker():
    r = Rehearsal('status-external-worker')
    binary = r.binary('pir-query-server')
    sha = sha256(binary)
    port = free_port()
    listen = '127.0.0.1:%d' % port
    artifact_port = free_port()
    settings = {'network_hex': '31' * 32, 'listen': listen, 'publisher': 'https://localhost:%d' % artifact_port,
                'state_dir': str(r.work / 'worker-state')}
    inventory = r.inventory('status', 'external-worker', settings, listen)
    deployer = r.deployer('status', inventory)
    deployer.capture_baseline()
    journal = deployer.deploy(sha, binary, skip_exact_check=True)
    identity = r.health('http://%s/control/identity' % listen)
    r.step('fresh-install', transaction=journal.id, status=journal.status, identity_matches=identity['binary_sha256'] == sha)
    r.executor.systemctl('local', 'restart', 'status-pir-worker.service')
    again = r.wait('restart', lambda: r.health('http://%s/control/identity' % listen))
    r.step('restart', new_incarnation=again['incarnation'] != identity['incarnation'])
    # This rehearsal owns no source snapshot or HTTPS publisher. An empty
    # consumer can prove installation/identity but cannot prove serving authority.
    # The native TLS lifecycle and independent live-source campaign own that gate.
    health = r.health('http://%s/control/health' % listen)
    if health['ready']:
        raise RuntimeError('an unconfigured HTTPS publication unexpectedly became ready')
    r.step('publication-qualification', scope='installation and restart only',
           exact_answer='not-executed', ready=False,
           required='Status native HTTPS lifecycle and independent live-source qualification')
    report = r.report('status', inventory)
    r.step('report', report=report)
    r.deployer('status', inventory).rollback(journal.id)
    r.step('rollback-install', absent=r.absent('status-pir-worker.service'))
    return r.finish()


def display():
    r = Rehearsal('transparent-display-external-worker')
    binary = r.binary('pir-query-server')
    fixture = r.binary('display_fixture')
    sha = sha256(binary)
    port = free_port()
    listen = '127.0.0.1:%d' % port
    url = 'http://' + listen
    root = r.work / 'publications'
    root.mkdir()
    settings = {'listen': listen, 'role': 'combined', 'state_dir': str(r.work / 'worker-state'),
                'publication_root': str(root), 'cache_bytes': 2 << 30}
    inventory = r.inventory('transparent-display', 'external-worker', settings, listen)
    deployer = r.deployer('transparent-display', inventory)
    deployer.capture_baseline()
    journal = deployer.deploy(sha, binary, skip_exact_check=True)
    health = r.health(url + '/v1/health')
    r.step('fresh-install', transaction=journal.id, status=journal.status, phase=health.get('phase'))
    socket = r.executor.path('/run/transparent-display-worker/control.sock')
    def control(command):
        result = subprocess.run([r.binary('txid-control'), socket], input=json.dumps(command), capture_output=True,
                                text=True, timeout=900)
        return result.returncode, result.stdout.strip()
    def publish(expected, candidate):
        code, prepared = control({'operation': 'prepare', 'expected': expected,
                                  'publication': {'directory': candidate['directory'],
                                                  'map_sha256': candidate['map_sha256']}})
        if code:
            return code, prepared
        return control({'operation': 'activate', 'expected': expected, 'map_sha256': candidate['map_sha256']})
    # The fixture controller: stage a candidate and prepare/activate it over the
    # root-only control socket, as the display controller does.
    p0 = json.loads(r.run(fixture, 'stage', root, 'p0', 0).stdout)
    code, reply = publish('', p0)
    exact = json.loads(r.run(fixture, 'lookup', url, 0).stdout)
    r.step('controller-activation', returncode=code, exact_answer=exact)
    r.executor.systemctl('local', 'restart', 'transparent-display-worker.service')
    r.wait('ready after restart', lambda: r.executor.http_get('local', url + '/v1/ready')[0] == 200, 600)
    r.step('restart', exact_answer=json.loads(r.run(fixture, 'lookup', url, 0).stdout),
           note='the active record restored the publication')
    p1 = json.loads(r.run(fixture, 'stage', root, 'p1', 1).stdout)
    code, reply = publish(p0['map_sha256'], p1)
    r.step('publication-update', returncode=code, exact_answer=json.loads(r.run(fixture, 'lookup', url, 1).stdout))
    code, refused = publish(p1['map_sha256'], {'directory': str(root / 'missing'), 'map_sha256': '00' * 32})
    _, status = control({'operation': 'status'})
    r.step('failed-publication-update', returncode=code, reply=refused[-300:],
           last_preparation=last_preparation(status),
           exact_answer=json.loads(r.run(fixture, 'lookup', url, 1).stdout))
    report = r.report('transparent-display', inventory)
    r.step('report', report=report)
    r.deployer('transparent-display', inventory).rollback(journal.id)
    r.step('rollback-install', absent=r.absent('transparent-display-worker.service'))
    return r.finish()


ROLES = {'receiver': receiver, 'enhance': enhance, 'history': history, 'status': status_worker, 'display': display}


def main():
    global ARGS
    parser = argparse.ArgumentParser()
    parser.add_argument('--ops', required=True)
    parser.add_argument('--binaries', required=True)
    parser.add_argument('--work', required=True)
    parser.add_argument('--out', required=True)
    parser.add_argument('--forbid', required=True)
    parser.add_argument('--roles', default=','.join(ROLES))
    ARGS = parser.parse_args()
    sys.path.insert(0, str(Path(ARGS.ops) / 'tools/lib'))
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    forbid(ARGS.forbid)
    import wallet_pir_ops.paths as paths
    assert str(paths.ROOT.resolve()) == str(Path(ARGS.ops).resolve()), paths.ROOT
    results = {'operations_package': str(ARGS.ops),
               'operations_manifest_revision': json.loads((Path(ARGS.ops) / 'operations.json').read_text()).get('source_revision'),
               'binaries': {p.name: sha256(p) for p in sorted(Path(ARGS.binaries).iterdir())},
               'executor': 'local unprivileged systemd emulation: real files, processes and HTTP; sandbox not enforced',
               'roles': []}
    failed = False
    for name in ARGS.roles.split(','):
        try:
            results['roles'].append({**ROLES[name](), 'result': 'pass'})
        except Exception as error:  # noqa: BLE001 - each role's failure is recorded
            failed = True
            results['roles'].append({'role': name, 'result': 'fail', 'error': str(error),
                                     'trace': traceback.format_exc()[-3000:]})
        Path(ARGS.out).write_text(json.dumps(results, indent=1, sort_keys=True))
    print(json.dumps({r['role']: r['result'] for r in results['roles']}))
    sys.exit(1 if failed else 0)


if __name__ == '__main__':
    main()
