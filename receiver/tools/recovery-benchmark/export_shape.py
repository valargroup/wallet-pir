"""Export public index workload counts without reading wallet state."""
import argparse
import hashlib
import json
import sqlite3
from pathlib import Path

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('index', type=Path)
parser.add_argument('output', type=Path)
args = parser.parse_args()
with sqlite3.connect(args.index.resolve().as_uri() + '?mode=ro', uri=True) as db:
    db.execute('BEGIN')
    config = json.loads(db.execute('SELECT value FROM config').fetchone()[0])
    rows = db.execute('SELECT height, end_position FROM blocks ORDER BY height').fetchall()
    positions = [r[0] for r in db.execute('SELECT position FROM payments ORDER BY position')]
    previous = config['start_position']
    counts = []
    for height, end in rows:
        assert height == config['start_height'] + len(counts)
        assert end >= previous
        counts.append(end - previous)
        previous = end
    assert config['start_position'] == 0
    shape = {'source': 'retained public receiver index', 'start_height': rows[0][0],
             'end_height': rows[-1][0], 'action_counts': counts,
             'indexed_positions': positions}
with args.output.open('x') as f:
    json.dump(shape, f, separators=(',', ':'))
print(json.dumps({'blocks': len(counts), 'actions': sum(counts), 'records': len(positions),
                  'sha256': hashlib.sha256(args.output.read_bytes()).hexdigest()}))
