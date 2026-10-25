#!/usr/bin/env python3
"""Operator rehearsal of every external worker role from unpacked releases.

Run from a copy outside the source checkout:

    rehearse.py --ops UNPACKED_OPERATIONS --binaries DIR --work DIR --out EVIDENCE.json \
        --forbid SOURCE_CHECKOUT [--roles enhance,status]

Every deploy, rollback and report goes through the unpacked operations
package's own engine and product adapters; an audit hook refuses any file
open under --forbid, so nothing is read from the source checkout. Binaries
are copies of release builds; fixture controllers, nodes and oracles are the
release's own tools or the rehearsal's fixture programs. Receiver process and
configuration checks live with the root publisher and query-server integration
tests; deployment transactions use the shared deploy suite. Nothing here is a
GPU qualification, and the local executor does not enforce the sandbox.
Status runs --binaries' `pir-query-server`, built with its `status` feature.
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
    """A JSON-RPC node: genesis, a tip, and the blocks the test sets with their Ironwood tree sizes.

    `getblock HASH 1` answers `trees.ironwood.size` for a set block, as the receiver reads
    an anchor's tree size, and a node error for any other hash.
    """

    def __init__(self, genesis_display):
        self.genesis = genesis_display
        self.hashes = {}
        self.sizes = {}
        self.tip = 0
        node = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_POST(self):
                request = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
                method, params = request['method'], request.get('params', [])
                result, error = None, None
                if method == 'getblockcount':
                    result = node.tip
                elif method == 'getblockhash':
                    height = int(params[0])
                    result = node.genesis if height == 0 else node.hashes.get(height, 'ee' * 32)
                elif method == 'getblock' and params[1:] == [1]:
                    heights = [h for h, display in node.hashes.items() if display == params[0]]
                    if heights and params[0] in node.sizes:
                        result = {'hash': params[0], 'height': heights[0],
                                  'trees': {'ironwood': {'size': node.sizes[params[0]]}}}
                    else:
                        error = {'code': -8, 'message': 'Block not found'}
                body = json.dumps({'result': result, 'error': error, 'id': request.get('id')}).encode()
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

    def set(self, height, display_hash, size=None):
        self.hashes[height] = display_hash
        if size is not None:
            self.sizes[display_hash] = size
        self.tip = max(self.tip, height + 5)

    def stage(self, published):
        """Puts every block a staged publication depends on on the chain, with its tree size."""
        for anchor in published['anchors']:
            self.set(anchor['height'], anchor['hash'], anchor['size'])


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


def enhance():
    r = Rehearsal('enhance-external-worker')
    binary = r.binary('pir-query-server')
    sha = sha256(binary)
    # Two external worker instances, as a replicated group is deployed.
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
    report = r.report('enhance', inventory)
    r.step('report', report=report)
    r.deployer('enhance', inventory).rollback(journal.id)
    r.step('rollback-install', absent=all(r.absent('enhance-pir-worker-%d.service' % i) for i in range(2)))
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


ROLES = {'enhance': enhance, 'status': status_worker}


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
