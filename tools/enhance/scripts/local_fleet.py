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
import threading
import time
import urllib.request

ROOT = Path(__file__).resolve().parents[3]


def fetch(url):
    with urllib.request.urlopen(url, timeout=5) as response:
        return json.load(response)


class Publications:
    """Retain publications actually observed at ingress during the load window.

    Reserved generation numbers can have gaps. Their difference cannot count
    completed publications, so the qualification gate uses distinct responses.
    """
    def __init__(self, origin, initial):
        self.origin = origin
        self.observed = {initial['generation']: initial}
        self.failures = 0
        self.stopped = threading.Event()
        self.thread = threading.Thread(target=self.sample, daemon=True)
        self.thread.start()

    def sample(self):
        while not self.stopped.wait(0.2):
            try:
                manifest = fetch(self.origin + '/v1/enhance/init')
                self.observed[manifest['generation']] = manifest
            except (OSError, ValueError, KeyError):
                self.failures += 1

    def close(self):
        self.stopped.set()
        self.thread.join(timeout=6)
        if self.thread.is_alive():
            raise RuntimeError('publication observer failed to stop within its HTTP deadline')

    def report(self):
        manifests = sorted(self.observed.values(), key=lambda m: m['generation'])
        return {'observed_publications': max(0, len(manifests) - 1),
                'distinct_anchors': len({(m['anchor_height'], m['anchor_block_hash']) for m in manifests}),
                'observation_failures': self.failures, 'manifests': manifests}


class Fleet:
    def __init__(self, binaries, root, output, *, sealed_shards=6, cuda_device=None, publisher=None,
                 profile='smoke', block_interval=10):
        from fixture_node import PROFILES
        self.binaries = Path(binaries).resolve()
        self.root, self.output = Path(root), Path(output)
        self.processes, self.logs, self.ports = [], [], set()
        self.named_processes = {}
        self.peak_rss_kib, self.samples, self.sample_failures = {}, 0, 0
        self.sampling_stopped = threading.Event()
        self.sampler = threading.Thread(target=self.sample_resources, daemon=True)
        self.sampler.start()
        self.sealed_shards = sealed_shards
        self.cuda_device = cuda_device
        self.publisher = publisher or self.binaries / 'pir-publisher'
        self.profile, self.block_interval = profile, block_interval
        self.geometry = PROFILES[profile]

    def sample_resources(self):
        while not self.sampling_stopped.wait(1):
            owned = dict(self.named_processes)
            if not owned:
                continue
            try:
                result = subprocess.run(['ps', '-o', 'pid=,rss=', '-p', ','.join(map(str, owned))],
                                        capture_output=True, text=True, timeout=5)
            except (OSError, subprocess.TimeoutExpired):
                self.sample_failures += 1
                continue
            if result.returncode:
                self.sample_failures += 1
                continue
            self.samples += 1
            for line in result.stdout.splitlines():
                pid, rss = map(int, line.split())
                name = owned[pid]
                self.peak_rss_kib[name] = max(self.peak_rss_kib.get(name, 0), rss)

    def resource_report(self):
        return {'peak_rss_kib': self.peak_rss_kib, 'samples': self.samples,
                'sample_failures': self.sample_failures,
                'qualification': 'local RSS samples; not Linux cgroup or hardware qualification'}

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
        self.named_processes[process.pid] = name
        return process

    def role(self, name, settings, public=None):
        config = self.root / (name + '.json')
        document = {'management_listen': f'127.0.0.1:{self.port()}'}
        if public is not None:
            # A wallet-facing role answers on the host's shared listener.
            document['public_listen'] = public
        document['enhance'] = {'enabled': True, 'settings': settings}
        config.write_text(json.dumps(document))
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
                  'trusted_ca': str(cert), 'download_budget_bytes': (4 if self.profile == 'smoke' else 64) << 30,
                  'cache_limit_bytes': (4 if self.profile == 'smoke' else 64) << 30}
        self.workers = []
        domains, per_worker = self.geometry['domains'], self.geometry['worker_shards']
        worker_origins = {str(shard): [] for shard in range(domains)}
        for first in range(0, domains, per_worker):
            shards = list(range(first, min(first + per_worker, domains)))
            for replica in range(2):
                name = f'worker-{first}-{replica}'
                address = f'127.0.0.1:{self.port()}'
                peer = {'name': name, 'url': 'http://' + address}
                self.workers.append(peer)
                for shard in shards:
                    worker_origins[str(shard)].append(peer['url'])
                self.role(name, {
                    'role': 'worker', 'listen': address, 'data_dir': str(self.root / name),
                    'sealed_shards': self.sealed_shards, 'synchronization': dict(source, shards=shards),
                    'matvec': {'matvec_backend': 'cuda' if self.cuda_device is not None else 'cpu',
                               'cuda_device': self.cuda_device},
                })
        router_origins = {}
        for first in range(0, domains, 6):
            router, control = (f'127.0.0.1:{self.port()}' for _ in range(2))
            shards = range(first, min(first + 6, domains))
            name = f'router-{first}'
            self.role(name, {
                'role': 'packing-router', 'listen': router, 'control_listen': control,
                'data_dir': str(self.root / name),
                'synchronization': dict(source, workers={str(shard): worker_origins[str(shard)] for shard in shards}),
            })
            for shard in shards:
                router_origins[str(shard)] = [{'control_origin': 'http://' + control,
                                               'query_origin': 'http://' + router}]
        ingress = f'127.0.0.1:{self.port()}'
        self.role('ingress', {
            'role': 'query-ingress',
            'control_listen': f'127.0.0.1:{self.port()}',
            'data_dir': str(self.root / 'ingress'),
            'synchronization': dict(source, routers=router_origins),
        }, public=ingress)
        self.origin = 'http://' + ingress

    def start_publisher(self, *, blocks=128):
        from fixture_node import write_oracle
        self.oracle = self.root / 'oracle.json'
        write_oracle(self.oracle, 2 * self.geometry['initial_blocks'] * self.geometry['transactions'])
        log = (self.output / 'node.log').open('w')
        self.logs.append(log)
        node = subprocess.Popen(['python3', str(Path(__file__).with_name('fixture_node.py')),
                                 '--blocks', str(blocks), '--profile', self.profile,
                                 '--interval', str(self.block_interval)], stdout=subprocess.PIPE, stderr=log, text=True)
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
            'chain': {'rpc_url': 'http://127.0.0.1:' + port, 'rpc_cookie': str(cookie), 'poll_ms': 1000},
            'enhance': {'enabled': True, 'settings': {
                'artifact_listen': self.artifacts, 'listen': self.operator,
                'data_dir': str(self.root / 'publisher'),
                'prevout_cache_outputs': 1000,
            }},
        }))
        return self.start('publisher', [str(self.publisher), '--config', str(config)])

    def wait_ready(self, timeout=120):
        deadline = time.monotonic() + timeout
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
        self.sampling_stopped.set()
        self.sampler.join(timeout=6)
        if self.sampler.is_alive():
            raise RuntimeError('resource sampler failed to stop within its process deadline')
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
