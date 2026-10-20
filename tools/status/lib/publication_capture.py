"""Independent Status publication oracle, built from node JSON rather than PIR code.

Only public chain data is retained. The node credential is read at runtime and
never written to a capture. This collector does not publish or activate data.
"""
import base64
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
from pathlib import Path
import struct
import time
import urllib.request

ROWS, SLOTS, ROW_BYTES = 8192, 256, 12288


class SourceChanged(ValueError):
    """The node moved while a complete source observation was being read."""


def protocol_hash(display):
    value = bytes.fromhex(display)
    if len(value) != 32:
        raise ValueError('node hash must contain 32 bytes')
    return value[::-1]


def snapshot(blocks, mempool, forks, network, salt):
    """Encode the published fixed-width contract independently of the Rust index."""
    if not blocks or len(network) != 32 or len(salt) != 32:
        raise ValueError('missing chain window or identity')
    for previous, current in zip(blocks, blocks[1:]):
        if current['height'] != previous['height'] + 1 or current['previousblockhash'] != previous['hash']:
            raise ValueError('noncontiguous canonical window')
    first, last = blocks[0]['height'], blocks[-1]['height']
    records = {}
    for txid, height, block in sorted(forks, key=lambda v: (v[1], v[2])):
        if first <= height <= last:
            records[protocol_hash(txid)] = (3, height)
    for txid in mempool:
        records[protocol_hash(txid)] = (1, 0)
    for block in blocks:
        for txid in block['tx']:
            key = protocol_hash(txid)
            if key in records and records[key][0] == 2:
                raise ValueError('duplicate canonical transaction')
            records[key] = (2, block['height'])
    grouped = [[] for _ in range(ROWS)]
    for txid, (tag, height) in sorted(records.items()):
        bucket = int.from_bytes(hashlib.sha256(b'status-pir/v2/bucket\0' + network + salt + txid).digest()[:2], 'little') % ROWS
        grouped[bucket].append((txid, tag, height))
    while len(records) > ROWS * SLOTS * 3 // 4 or any(len(row) > SLOTS for row in grouped):
        if first == last:
            raise ValueError('mempool or final block exceeds Status capacity')
        for row in grouped:
            row[:] = [record for record in row if record[1] == 1 or record[2] > first]
        records = {record[0]: (record[1], record[2]) for row in grouped for record in row}
        first += 1
    digest = hashlib.sha256()
    for row in grouped:
        encoded = b''.join(txid + bytes([tag, 0, 0, 0]) + struct.pack('<I', height) for txid, tag, height in row)
        digest.update(encoded + bytes(ROW_BYTES - len(encoded)))
    return {'coverage_start': first, 'anchor_height': last,
            'anchor_hash': list(protocol_hash(blocks[-1]['hash'])),
            'entries': len(records), 'rows_digest': list(digest.digest()),
            'network': list(network), 'salt': list(salt)}


class Node:
    def __init__(self, origin, cookie):
        self.origin = origin
        self.authorization = 'Basic ' + base64.b64encode(Path(cookie).read_bytes().strip()).decode()

    def call(self, method, *params):
        request = urllib.request.Request(self.origin,
            data=json.dumps({'jsonrpc': '2.0', 'id': 1, 'method': method, 'params': params}).encode(),
            headers={'Authorization': self.authorization, 'Content-Type': 'application/json'})
        with urllib.request.urlopen(request, timeout=120) as response:
            body = response.read((64 << 20) + 1)
        if len(body) > 64 << 20:
            raise ValueError('node response exceeds the capture budget')
        value = json.loads(body)
        if value.get('error') or value.get('id') != 1:
            raise ValueError('node refused ' + method + ': ' + str(value.get('error', {}).get('code') if value.get('error') else 'response ID'))
        return value['result']

    def block(self, height):
        identity = self.call('getblockhash', height)
        value = self.call('getblock', identity, 1)
        if value['hash'] != identity or value['height'] != height:
            raise SourceChanged('node block identity changed')
        return value


def retained_forks(path, node, *, expected_sha256=None):
    """Replay retained fork inputs, checking their transaction membership with the node.

    The checkpoint supplies only historical membership, which cannot be inferred
    from today's canonical chain. No saved row, digest or publication is trusted.
    """
    payload = Path(path).read_bytes()
    if expected_sha256 is not None and hashlib.sha256(payload).hexdigest() != expected_sha256:
        raise ValueError('retained source differs from its frozen pre-cutover input')
    data = memoryview(payload)
    if bytes(data[:8]) != b'STSOBS01':
        raise ValueError('unknown retained source format')
    position = 76
    def number():
        nonlocal position
        value = struct.unpack_from('<I', data, position)[0]
        position += 4
        return value
    blocks = number()
    if blocks > 4096:
        raise ValueError('retained block count exceeds its bound')
    for _ in range(blocks):
        position += 68
        count = number()
        if count > 4000000:
            raise ValueError('retained transaction count exceeds its bound')
        position += count * 32
    count = number()
    if count > 4000000:
        raise ValueError('retained fork count exceeds its bound')
    result, verified = [], {}
    for _ in range(count):
        txid = bytes(data[position:position + 32])[::-1].hex()
        position += 32
        height = number()
        identity = bytes(data[position:position + 32])[::-1].hex()
        position += 32
        block = verified.setdefault(identity, None)
        if block is None:
            try:
                block = node.call('getblock', identity, 1)
            except ValueError as error:
                # Finalized nodes may discard former fork blocks. An explicitly
                # frozen pre-cutover source remains the oracle for those old
                # inputs; it never supplies canonical or mempool membership.
                if expected_sha256 is None or str(error) != 'node refused getblock: -5':
                    raise
                if node.call('getblockhash', height) == identity:
                    raise ValueError('retained fork is canonical') from error
                block = {'hash': identity, 'height': height, 'tx': None}
            verified[identity] = block
        if block['hash'] != identity or block['height'] != height or (block['tx'] is not None and txid not in block['tx']):
            raise ValueError('retained fork input disagrees with node data')
        result.append((txid, height, identity))
    if position != len(data):
        raise ValueError('trailing or truncated retained source bytes')
    return result


class Oracle:
    def __init__(self, node, window, salt, forks=()):
        self.node, self.window, self.salt = node, window, salt
        self.network = protocol_hash(node.call('getblockhash', 0))
        self.blocks, self.forks = [], list(forks)
        self.cached_identity, self.cached_snapshot = None, None

    def observe(self):
        for _ in range(3):
            try:
                return self._observe()
            except SourceChanged:
                continue
        raise SourceChanged('node source did not stabilize after three complete reads')

    def _observe(self):
        tip = self.node.call('getblockcount')
        anchor = self.node.call('getblockhash', tip)
        while self.blocks and (self.blocks[-1]['height'] > tip or
                self.node.call('getblockhash', self.blocks[-1]['height']) != self.blocks[-1]['hash']):
            block = self.blocks.pop()
            self.forks.extend((txid, block['height'], block['hash']) for txid in block['tx'])
        start = self.blocks[-1]['height'] + 1 if self.blocks else max(1, tip - self.window + 1)
        with ThreadPoolExecutor(max_workers=8) as pool:
            self.blocks.extend(pool.map(self.node.block, range(start, tip + 1)))
        self.blocks = self.blocks[-self.window:]
        self.forks = [entry for entry in self.forks if entry[1] >= self.blocks[0]['height']]
        mempool = sorted(self.node.call('getrawmempool', False))
        if self.node.call('getblockcount') != tip or self.node.call('getblockhash', tip) != anchor:
            raise SourceChanged('node tip changed during independent collection')
        observed_ms = int(time.time() * 1000)
        identity = (anchor, tuple(mempool), tuple(self.forks))
        if identity != self.cached_identity:
            self.cached_snapshot = snapshot(self.blocks, mempool, self.forks, self.network, self.salt)
            self.cached_identity = identity
        return {'source_observed_ms': observed_ms, 'snapshot': self.cached_snapshot,
                'mempool_digest': hashlib.sha256(json.dumps(mempool).encode()).hexdigest(),
                'anchor': anchor}


def manifest(origin):
    with urllib.request.urlopen(origin + '/v1/status/init', timeout=5) as response:
        return json.loads(response.read(65537))


def matches(observation, published):
    return all(published.get(key) == value for key, value in observation['snapshot'].items())


def resource(unit):
    import subprocess
    value = subprocess.check_output(['systemctl', 'show', unit,
        '--property=MainPID,ControlGroup,NRestarts,MemoryCurrent,MemoryPeak,InvocationID'], text=True)
    properties = dict(line.split('=', 1) for line in value.splitlines() if '=' in line)
    group = Path('/sys/fs/cgroup' + properties['ControlGroup'])
    events = dict(line.split() for line in (group / 'memory.events').read_text().splitlines())
    return {'sampled_ms': int(time.time() * 1000),
            'oom_events': int(events['oom']) + int(events['oom_kill']),
            'swap_bytes': int((group / 'memory.swap.current').read_text()),
            'process': properties, 'memory_events': events}


def change_kind(previous, current):
    if current['anchor'] != previous['anchor']:
        return 'block'
    return 'mempool' if current['mempool_digest'] != previous['mempool_digest'] else 'unchanged'


def capture(config, origin, directory, seconds, unit='pir-publisher.service', *, retained_source=None, retained_sha256=None):
    """Record complete independent observations and their verified successors.

    Warming the full source window precedes the measurement. A failed capture
    remains failed even if a later observation recovers. Transfer raw reports
    unchanged; the caller aligns them to its independently timed load window.
    """
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=False)
    node = Node(config['rpc_url'], config['cookie'])
    settings = config['publisher']
    seed = Path(retained_source) if retained_source else Path(settings['state_dir']) / 'source/source.bin'
    forks = retained_forks(seed, node, expected_sha256=retained_sha256)
    oracle = Oracle(node, settings['window_blocks'], bytes.fromhex(settings['salt_hex']), forks)
    previous = oracle.observe()
    print(json.dumps({'event': 'oracle_warmed', 'height': previous['snapshot']['anchor_height']}), flush=True)
    last_verified = previous
    pending, observations, failures = [], 0, []
    began = time.monotonic()
    with (directory / 'publications.jsonl').open('x') as publications, \
         (directory / 'resources.jsonl').open('x') as resources, \
         (directory / 'observations.jsonl').open('x') as raw:
        next_resource = began
        def emit(row):
            publications.write(json.dumps(row) + '\n')
            publications.flush()
        while time.monotonic() - began < seconds:
            tick = time.monotonic()
            try:
                observed = oracle.observe()
                kind = change_kind(previous, observed)
                # Six hours of observations followed by an explicit fifteen-second
                # drain lets the final observations reach serving before assessment.
                collecting = seconds <= 15 or time.monotonic() - began < seconds - 15
                observed.update(observation_id=observations if collecting else None, kind=kind, measured=collecting)
                raw.write(json.dumps(observed) + '\n')
                raw.flush()
                if collecting:
                    observations += 1
                    pending.append(observed)
                previous = observed
                published = manifest(origin)
                now = int(time.time() * 1000)
                if published.get('protocol') != 'status-pir-v3-native-two-mask-m29':
                    raise ValueError('capture requires the deployed native Status protocol')
                if protocol_hash(node.call('getblockhash', published['anchor_height'])) != bytes(published['anchor_hash']):
                    raise ValueError('published anchor is no longer canonical')
                verified = next((item for item in reversed(pending) if matches(item, published)
                                 and item['source_observed_ms'] <= published['observed_ms'] <= now
                                 and now - published['observed_ms'] <= 20000), None)
                if verified is not None:
                    for item in pending:
                        if item['observation_id'] > verified['observation_id']:
                            break
                        base = {key: item[key] for key in ('observation_id', 'kind', 'source_observed_ms')}
                        if item is verified:
                            emit({**base, 'kind': change_kind(last_verified, verified),
                                  'disposition': 'published', 'queryable_ms': now,
                                  'oracle_match': True, 'canonical_continuity': True, 'complete_mempool': True,
                                  'generation': published['generation'], 'manifest': published})
                        else:
                            emit({**base, 'disposition': 'superseded', 'superseded_by': verified['observation_id']})
                    last_verified = verified
                    pending = [item for item in pending if item['observation_id'] > verified['observation_id']]
                for item in pending[:]:
                    if now - item['source_observed_ms'] > 20000:
                        emit({**{key: item[key] for key in ('observation_id', 'kind', 'source_observed_ms')},
                              'disposition': 'failed', 'error': 'no independently verified successor within twenty seconds'})
                        failures.append('publication deadline')
                        pending.remove(item)
            except Exception as error:
                # Exception messages may include transport detail; retain only their type.
                failures.append(type(error).__name__)
                raw.write(json.dumps({'capture_error': type(error).__name__, 'at_ms': int(time.time() * 1000)}) + '\n')
                raw.flush()
            if time.monotonic() >= next_resource:
                resources.write(json.dumps(resource(unit)) + '\n')
                resources.flush()
                next_resource = time.monotonic() + 30
                print(json.dumps({'event': 'capture_progress', 'observations': observations,
                                  'failures': len(failures), 'pending': len(pending)}), flush=True)
            time.sleep(max(0, 1 - (time.monotonic() - tick)))
        for item in pending:
            emit({**{key: item[key] for key in ('observation_id', 'kind', 'source_observed_ms')},
                  'disposition': 'incomplete'})
    result = {'passed': not failures and not pending, 'failures': failures, 'source_observations': observations,
              'pending_at_end': len(pending), 'retained_source_sha256': hashlib.sha256(seed.read_bytes()).hexdigest(),
              'retained_fork_oracle': 'frozen_pre_cutover_input' if retained_sha256 else 'node_verified'}
    (directory / 'result.json').write_text(json.dumps(result, indent=2))
    return result
