"""Isolated trusted-RPC fixture for canonical publisher smoke checks.

Repeats a public Ironwood transaction in synthetic, non-consensus blocks. The
publisher still uses its ordinary RPC acquisition and fee decoding. Expected
records come from frozen bytes, independently of the publisher's serializer.
This fixture cannot attest chain consensus or production capacity.
"""
import argparse
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import math
from pathlib import Path
import time

ACTIVATION_HEIGHT = 3_428_143
TRANSACTION_SHA256 = '566aa040e2513afcc1c08818af4ac69b61df32d97a451e6bdad498b3259d5f4c'
RECORD_SHA256 = ['51a0d990e2b52297907170a1ab57b950d4378c5a2213f00eab8754770fcc3c8b',
                 '68a35ee80643b901a70a9d8af20a6ac9f72964f5740bc88cdd1d20df0706952d']
SHARD_RECORDS = 32768 * 33
# Large profiles are explicit resource checks using non-consensus blocks. They
# are separate from the small CPU correctness suite and real-chain evidence.
PROFILES = {
    'smoke': {'initial_blocks': 1, 'transactions': 1, 'confirmations': 0, 'domains': 1, 'worker_shards': 1},
    'full-shard': {'initial_blocks': 2816, 'transactions': 192, 'confirmations': 0, 'domains': 2, 'worker_shards': 2},
    'five-shards': {'initial_blocks': 14079, 'transactions': 192, 'confirmations': 0, 'domains': 5, 'worker_shards': 5},
    'six-sealed': {'initial_blocks': 16896, 'transactions': 192, 'confirmations': 1000, 'domains': 7, 'worker_shards': 6},
    'seven-sealed': {'initial_blocks': 19712, 'transactions': 192, 'confirmations': 1000, 'domains': 8, 'worker_shards': 7},
}
# A public synthetic Sapling coinbase with no shielded actions. Confirmations
# must still contain a decodable transaction; an empty block is rejected by RPC
# acquisition. This does not claim that the synthetic chain satisfies consensus.
CONFIRMATION_TX = bytes.fromhex(
    '0400008085202f89' + '01' + '00' * 32 + 'ffffffff' + '0100' + 'ffffffff'
    + '01' + '00' * 8 + '00' + '00' * 4 + '00' * 4 + '00' * 8 + '000000')


def compact_size(count):
    return bytes([count]) if count < 253 else b'\xfd' + count.to_bytes(2, 'little')


def fixture_path(name):
    bundled = Path(__file__).with_name(name)
    if bundled.is_file():
        return bundled
    return Path(__file__).resolve().parents[2] / 'pir-e2e-tests/tests/fixtures' / name


class Chain:
    def __init__(self, blocks, interval, *, initial_blocks=1, transactions=1, confirmations=0):
        transaction = bytes.fromhex(fixture_path('ironwood-fee-expiry.hex').read_text().strip())
        if hashlib.sha256(transaction).hexdigest() != TRANSACTION_SHA256:
            raise ValueError('public transaction fixture checksum changed')
        self.blocks = []
        self.hashes = {}
        self.interval, self.started = interval, time.monotonic()
        self.initial_blocks, self.transactions, self.confirmations = initial_blocks, transactions, confirmations
        self.initial_tip = initial_blocks + confirmations - 1
        self.bulk = (compact_size(transactions) + transaction * transactions).hex()
        if len(bytes.fromhex(self.bulk)) + 1487 > 2_000_000:
            raise ValueError('fixture block exceeds the ordinary raw-block bound')
        self.single = (b'\x01' + transaction).hex()
        previous = bytes(32)
        for index in range(initial_blocks + confirmations + blocks - 1):
            header = bytearray(140)
            header[:4] = (4).to_bytes(4, 'little')
            header[4:36] = previous
            header[108:112] = index.to_bytes(4, 'little')
            header.extend(b'\xfd\x40\x05' + bytes(1344))
            previous = hashlib.sha256(hashlib.sha256(header).digest()).digest()
            display = previous[::-1].hex()
            self.blocks.append((display, header.hex()))
            self.hashes[display] = index

    def answer(self, request):
        tip = min(len(self.blocks) - 1, self.initial_tip + int((time.monotonic() - self.started) / self.interval))
        method, parameters = request['method'], request.get('params', [])
        if method == 'getblockcount':
            return ACTIVATION_HEIGHT + tip
        value = parameters[0]
        index = self.hashes.get(value) if isinstance(value, str) else None
        if index is None:
            index = int(value) - ACTIVATION_HEIGHT
        if not 0 <= index <= tip:
            raise ValueError('block outside the fixture chain')
        if method == 'getblockhash':
            return self.blocks[index][0]
        if method == 'getblock' and parameters[1] == 0:
            payload = self.bulk if index < self.initial_blocks else (
                (b'\x01' + CONFIRMATION_TX).hex() if index <= self.initial_tip else self.single)
            return self.blocks[index][1] + payload
        if method == 'getblock' and parameters[1] == 2:
            records = 2 * (min(index + 1, self.initial_blocks) * self.transactions
                           + max(0, index - self.initial_tip))
            return {'trees': {'ironwood': {'size': records}}}
        raise ValueError('unexpected fixture RPC method')


def write_oracle(path, count=2):
    records = []
    for index in range(2):
        raw = bytes.fromhex(fixture_path(f'ironwood-canonical-record-{index}.hex').read_text().strip())
        if hashlib.sha256(raw).hexdigest() != RECORD_SHA256[index]:
            raise ValueError('frozen independent record fixture checksum changed')
        record = raw[84:]
        if len(record) != 653:
            raise ValueError('frozen schema-11 record has the wrong length')
        records.append({'position': index, 'record_hex': record.hex()})
    frozen = list(records)
    positions = {0, 1, 32, 33, 65, 66, count - 1}
    # Probe row, unit and shard boundaries independently of producer output.
    for stride in (8192 * 33, SHARD_RECORDS):
        for boundary in range(stride, count, stride):
            positions.update((boundary - 1, boundary))
    records = [{'position': position, 'record_hex': frozen[position % 2]['record_hex']}
               for position in sorted(positions) if 0 <= position < count]
    path.write_text(json.dumps(records) + '\n')


def serve(chain):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def do_POST(self):
            length = int(self.headers.get('Content-Length', '0'))
            if self.headers.get('Authorization') != 'Basic Zml4dHVyZTpmaXh0dXJl' or not 0 < length <= 65536:
                self.send_error(400)
                return
            try:
                request = json.loads(self.rfile.read(length))
                result = {'jsonrpc': '2.0', 'id': request['id'],
                          'result': chain.answer(request), 'error': None}
            except (ValueError, KeyError, TypeError, IndexError):
                self.send_error(400)
                return
            body = json.dumps(result).encode()
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    print(server.server_address[1], flush=True)
    server.serve_forever()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--blocks', type=int, default=128)
    parser.add_argument('--interval', type=float, default=10)
    parser.add_argument('--profile', choices=PROFILES, default='smoke')
    args = parser.parse_args()
    if not 1 <= args.blocks <= 4096 or not math.isfinite(args.interval) or args.interval <= 0:
        parser.error('blocks must be between 1 and 4096 and interval must be positive')
    profile = PROFILES[args.profile]
    serve(Chain(args.blocks, args.interval, initial_blocks=profile['initial_blocks'],
                transactions=profile['transactions'], confirmations=profile['confirmations']))


if __name__ == '__main__':
    main()
