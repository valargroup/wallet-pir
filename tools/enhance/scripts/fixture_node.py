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
from pathlib import Path
import time

ACTIVATION_HEIGHT = 3_428_143
TRANSACTION_SHA256 = '566aa040e2513afcc1c08818af4ac69b61df32d97a451e6bdad498b3259d5f4c'


def fixture_path(name):
    bundled = Path(__file__).with_name(name)
    if bundled.is_file():
        return bundled
    return Path(__file__).resolve().parents[2] / 'pir-e2e-tests/tests/fixtures' / name


class Chain:
    def __init__(self, blocks, interval):
        transaction = bytes.fromhex(fixture_path('ironwood-fee-expiry.hex').read_text().strip())
        if hashlib.sha256(transaction).hexdigest() != TRANSACTION_SHA256:
            raise ValueError('public transaction fixture checksum changed')
        self.blocks = []
        self.hashes = {}
        self.interval, self.started = interval, time.monotonic()
        previous = bytes(32)
        for index in range(blocks):
            header = bytearray(140)
            header[:4] = (4).to_bytes(4, 'little')
            header[4:36] = previous
            header[108:112] = index.to_bytes(4, 'little')
            header.extend(b'\xfd\x40\x05' + bytes(1344))
            previous = hashlib.sha256(hashlib.sha256(header).digest()).digest()
            display = previous[::-1].hex()
            self.blocks.append((display, (header + b'\x01' + transaction).hex()))
            self.hashes[display] = index

    def answer(self, request):
        tip = min(len(self.blocks) - 1, int((time.monotonic() - self.started) / self.interval))
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
            return self.blocks[index][1]
        if method == 'getblock' and parameters[1] == 2:
            return {'trees': {'ironwood': {'size': 2 * (index + 1)}}}
        raise ValueError('unexpected fixture RPC method')


def write_oracle(path):
    records = []
    for index in range(2):
        raw = bytes.fromhex(fixture_path(f'ironwood-canonical-record-{index}.hex').read_text().strip())
        record = raw[84:]
        if len(record) != 653:
            raise ValueError('frozen schema-11 record has the wrong length')
        records.append({'position': index, 'record_hex': record.hex()})
    # Both positions remain valid as subsequent fixture blocks append records.
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
    parser.add_argument('--interval', type=float, default=3)
    args = parser.parse_args()
    if not 1 <= args.blocks <= 4096 or args.interval <= 0:
        parser.error('blocks must be between 1 and 4096 and interval must be positive')
    serve(Chain(args.blocks, args.interval))


if __name__ == '__main__':
    main()
