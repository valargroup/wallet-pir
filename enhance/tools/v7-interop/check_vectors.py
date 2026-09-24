#!/usr/bin/env python3
"""Independent v7 session encoder; frozen fixture is shared with the Rust wallets."""
import hashlib
import json
import pathlib
import struct

ROOT = pathlib.Path(__file__).resolve().parents[3]
fixture = json.loads((ROOT / 'enhance/crates/enhance-pir/tests/fixtures/v7-session.json').read_text())
m = fixture['manifest']
s = m['coverage']['shards'][0]
r = m['sessions'][0]
units = m['unit_identities'][str(s['id'])]
data = bytearray(b'enhance-pir/v7/session\0')
def u64(value):
    data.extend(struct.pack('<Q', value))
def string(value):
    value = value.encode('utf-8')
    u64(len(value))
    data.extend(value)
string(m['protocol_revision'])
for n in (s['id'], int(m['domain_recovery_epochs'][str(s['id'])]), s['logical_rows'], s['records'], len(units)):
    u64(n)
for unit in units:
    for n in (int(unit['recovery_epoch']), unit['local_row_start'], unit['allocated_rows']):
        u64(n)
    for key in ('content_sha256', 'setup_sha256', 'parameter_id'):
        string(unit[key])
for key in ('parameter_id', 'public_params_sha256'):
    string(r[key])
actual = hashlib.sha256(data).hexdigest()
assert actual == fixture['session_id'], (actual, fixture['session_id'])
print(f'v7 session vector verified: {actual} ({len(data)} canonical bytes)')
