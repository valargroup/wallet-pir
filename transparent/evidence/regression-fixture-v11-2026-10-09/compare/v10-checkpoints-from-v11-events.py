#!/usr/bin/env python3
"""Check every v10 checkpoint's expected events against the v11 export.

For each case and each checkpoint height h in the v10 fixture, the v11 final
events of that case, cut to their legacy 87-byte encoding and filtered to
height <= h, must equal the v10 expected events at h. This reaches the cases
whose checkpoints all moved with the cutoff or anchor, which the fixture
comparison cannot cross-check at shared heights. Reads two files only.
"""
import json
import sys

LEGACY = 87


def height(event_hex):
    raw = bytes.fromhex(event_hex[:2 * LEGACY])
    return int.from_bytes(raw[3:7], 'little')


def main(previous_path, next_path):
    previous = {c['id']: c for c in json.load(open(previous_path))['cases']}
    nxt = {c['id']: c for c in json.load(open(next_path))['cases']}
    checked, failures = 0, []
    for case_id, old in previous.items():
        final = nxt[case_id]['checkpoints'][-1]['expected']['events']
        legacy = [(e['script'], e['event'][:2 * LEGACY]) for e in final]
        if any(len(e['event']) <= 2 * LEGACY for e in final):
            failures.append(f'{case_id}: v11 event without metadata')
        for cp in old['checkpoints']:
            h = cp['anchor']['height']
            want = sorted((e['script'], e['event']) for e in cp['expected']['events'])
            got = sorted(e for e in legacy if height(e[1]) <= h)
            checked += 1
            if want != got:
                failures.append(f'{case_id}@{h}: {len(want)} v10 events, {len(got)} from v11')
    print(json.dumps({'checkpoints_checked': checked, 'failures': failures}, indent=2))
    return 1 if failures else 0


if __name__ == '__main__':
    sys.exit(main(sys.argv[1], sys.argv[2]))
