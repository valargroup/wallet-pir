"""Disposable autonomous serving processes for local workload evidence.

Creates test-only TLS material and role configurations, starts the query hosts,
then starts the canonical publisher against an isolated trusted-RPC fixture. Every input and process belongs to
the supplied fresh directory; no installed services or production state is used.
"""
import json
import os
from pathlib import Path
import select
import socket
import subprocess
import time
import urllib.request

ROOT = Path(__file__).resolve().parents[3]


def fetch(url):
    with urllib.request.urlopen(url, timeout=5) as response:
        return json.load(response)


class Fleet:
    def __init__(self, binaries, root, output, *, sealed_shards=6, cuda_device=None, publisher=None):
        self.binaries = Path(binaries).resolve()
        self.root, self.output = Path(root), Path(output)
        self.processes, self.logs, self.ports = [], [], set()
        self.sealed_shards = sealed_shards
        self.cuda_device = cuda_device
        self.publisher = publisher or self.binaries / 'pir-publisher'

    def port(self):
        while True:
            with socket.socket() as stream:
                stream.bind(('127.0.0.1', 0))
                port = stream.getsockname()[1]
            if port not in self.ports:
                self.ports.add(port)
                return port

    def start(self, name, command):
        log = (self.output / (name + '.log')).open('w')
        self.logs.append(log)
        process = subprocess.Popen(command, stdout=log, stderr=log, env={**os.environ, 'RUST_LOG': 'info'})
        self.processes.append(process)
        return process

    def role(self, name, settings):
        config = self.root / (name + '.json')
        config.write_text(json.dumps({
            'management_listen': f'127.0.0.1:{self.port()}',
            'enhance': {'enabled': True, 'settings': settings},
        }))
        self.start(name, [str(self.binaries / 'pir-query-server'), '--config', str(config)])

    def start_roles(self):
        self.artifacts = f'127.0.0.1:{self.port()}'
        self.operator = f'127.0.0.1:{self.port()}'
        cert, key = self.root / 'ca.pem', self.root / 'key.pem'
        subprocess.run(['openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes',
                        '-days', '1', '-subj', '/CN=localhost',
                        '-addext', 'subjectAltName=DNS:localhost',
                        '-addext', 'basicConstraints=critical,CA:FALSE',
                        '-keyout', str(key), '-out', str(cert)],
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
        fault = self.root / 'fault'
        fault.write_text('')
        log = (self.output / 'tls.log').open('w')
        self.logs.append(log)
        proxy = subprocess.Popen([
            'python3', str(Path(__file__).with_name('distribution_tls.py') if Path(__file__).with_name('distribution_tls.py').exists()
                           else ROOT / 'src/pir-publisher/tests/support/distribution_tls.py'),
            self.artifacts, str(cert), str(key), str(fault)],
            stdout=subprocess.PIPE, stderr=log, text=True)
        self.processes.append(proxy)
        if not select.select([proxy.stdout], [], [], 5)[0]:
            raise RuntimeError('local TLS proxy did not start')
        source = {'publisher_origin': 'https://localhost:' + proxy.stdout.readline().strip(),
                  'trusted_ca': str(cert), 'download_budget_bytes': 4 << 30,
                  'cache_limit_bytes': 4 << 30}
        self.workers = []
        for index in range(2):
            address = f'127.0.0.1:{self.port()}'
            self.workers.append({'name': f'worker-{index}', 'url': 'http://' + address})
            self.role(f'worker-{index}', {
                'role': 'worker', 'listen': address, 'data_dir': str(self.root / f'worker-{index}'),
                'sealed_shards': self.sealed_shards, 'synchronization': dict(source, shards=[0]),
                'matvec': {'matvec_backend': 'cuda' if self.cuda_device is not None else 'cpu',
                           'cuda_device': self.cuda_device},
            })
        router, control = (f'127.0.0.1:{self.port()}' for _ in range(2))
        self.role('router', {
            'role': 'packing-router', 'listen': router, 'control_listen': control,
            'data_dir': str(self.root / 'router'),
            'synchronization': dict(source, workers={'0': [r['url'] for r in self.workers]}),
        })
        ingress = f'127.0.0.1:{self.port()}'
        self.role('ingress', {
            'role': 'query-ingress', 'listen': ingress,
            'control_listen': f'127.0.0.1:{self.port()}',
            'data_dir': str(self.root / 'ingress'),
            'synchronization': dict(source, routers={'0': [{'control_origin': 'http://' + control, 'query_origin': 'http://' + router}]}),
        })
        self.origin = 'http://' + ingress
        self.consumers = self.root / 'consumers.json'
        self.consumers.write_text(json.dumps({
            'origin': self.origin,
            'groups': [{'name': 'local', 'shards': [0], 'replicas': self.workers}],
        }))

    def start_publisher(self, *, blocks=128):
        from fixture_node import write_oracle
        self.oracle = self.root / 'oracle.json'
        write_oracle(self.oracle)
        log = (self.output / 'node.log').open('w')
        self.logs.append(log)
        node = subprocess.Popen(['python3', str(Path(__file__).with_name('fixture_node.py')),
                                 '--blocks', str(blocks)], stdout=subprocess.PIPE, stderr=log, text=True)
        self.processes.append(node)
        if not select.select([node.stdout], [], [], 5)[0]:
            raise RuntimeError('isolated RPC fixture did not start')
        port = node.stdout.readline().strip()
        if not port.isdecimal():
            raise RuntimeError('isolated RPC fixture exited before reporting its port')
        cookie = self.root / 'cookie'
        cookie.write_text('fixture:fixture')
        config = self.root / 'publisher.json'
        config.write_text(json.dumps({
            'management_listen': f'127.0.0.1:{self.port()}',
            'enhance': {'enabled': True, 'settings': {
                'artifact_listen': self.artifacts, 'listen': self.operator,
                'data_dir': str(self.root / 'publisher'),
                'zakura_rpc_url': 'http://127.0.0.1:' + port,
                'zakura_cookie': str(cookie), 'poll_seconds': 1,
                'prevout_cache_outputs': 1000,
            }},
        }))
        return self.start('publisher', [str(self.publisher), '--config', str(config)])

    def wait_ready(self):
        deadline = time.monotonic() + 120
        while True:
            if any(process.poll() is not None for process in self.processes):
                raise RuntimeError('a fixture process exited; inspect retained logs')
            try:
                return fetch(self.origin + '/v1/enhance/init')
            except (OSError, ValueError):
                if time.monotonic() >= deadline:
                    raise RuntimeError('autonomous fixture did not become ready') from None
                time.sleep(0.1)

    def close(self):
        for process in reversed(self.processes):
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
        for log in self.logs:
            log.close()
