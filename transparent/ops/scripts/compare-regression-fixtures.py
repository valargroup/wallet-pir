#!/usr/bin/env python3
"""Compare a re-cut regression fixture with the one it replaces, before freezing.

A re-cut fixture is a new release gate. Nothing in the export refuses a case
that quietly stopped testing what it was chosen to test: a script that was
unused when it was picked and has since received funds still exports cleanly, it
just is not an unused-wallet case any more. The export also cannot tell you that
sealed history changed underneath it.

Two kinds of finding, separated because they mean different things:

  BLOCKING  a checkpoint at the same height in both fixtures whose expected
            state differs. Both fixtures derive from a read-only replay of the
            same journal, so a height below both anchors must reduce to the same
            events, UTXOs, spends, history and balance. A difference means the
            chain was rewritten under a sealed height, or ingest changed what it
            records. Freezing over that hides it. Note the check compares
            expectations only where the block hash also matches; a checkpoint
            whose hash changed is reported as a reorg instead.

  REVIEW    a case whose character changed: a profile whose meaning no longer
            holds, an event count or balance that moved by more than the
            threshold, or a case that gained or lost scripts. These are often
            legitimate -- an active wallet keeps transacting -- but each one is a
            decision, not an outcome.

A schema that appends transaction metadata to each event (journal v3, shard
schema v11) changes every event's bytes without changing what it records. With
--event-metadata, events are compared by their legacy 87-byte encoding, which
v3 keeps as a prefix; UTXOs, spends, history and balance still compare exactly.

This reads two files and writes a report. It contacts no service and changes
nothing.
"""
import argparse
import json
from pathlib import Path

# Hex digits in a legacy event record. A v3 record is this prefix, one flags
# byte and the metadata varints.
LEGACY_EVENT_HEX = 2 * 87


def load(path):
    doc = json.loads(Path(path).read_text())
    if doc.get('schema') != 'transparent-regression-v1':
        raise RuntimeError(f'{path}: unsupported fixture schema')
    return doc


def checkpoints_by_height(case):
    return {c['anchor']['height']: c for c in case['checkpoints']}


def summarize(expected):
    return {
        'events': len(expected['events']),
        'utxos': len(expected['utxos']),
        'spends': len(expected['spends']),
        'history': len(expected['history']),
        'balance': expected['confirmed_balance'],
    }


def legacy_events(expected):
    """The expected state with each event cut to its legacy encoding."""
    events = sorted(
        ({'script': e['script'], 'event': e['event'][:LEGACY_EVENT_HEX]}
         for e in expected['events']),
        key=lambda e: (e['script'], e['event']))
    return {**expected, 'events': events}


def compare_case(previous, nxt, tolerance, event_metadata=False):
    """Return (blocking, review) findings for one case present in both fixtures."""
    blocking, review = [], []
    case_id = previous['id']

    if previous['scripts'] != nxt['scripts']:
        gained = sorted(set(nxt['scripts']) - set(previous['scripts']))
        lost = sorted(set(previous['scripts']) - set(nxt['scripts']))
        review.append(
            f'{case_id}: script set changed (+{len(gained)}, -{len(lost)})')
    if previous['profile'] != nxt['profile']:
        review.append(
            f"{case_id}: profile {previous['profile']} -> {nxt['profile']}")

    old_points = checkpoints_by_height(previous)
    new_points = checkpoints_by_height(nxt)
    shared = sorted(set(old_points) & set(new_points))
    for height in shared:
        old, new = old_points[height], new_points[height]
        if old['anchor']['hash'] != new['anchor']['hash']:
            blocking.append(
                f'{case_id}: block {height} hash changed '
                f"{old['anchor']['hash'][:16]}... -> {new['anchor']['hash'][:16]}..., "
                'so sealed history was reorganised')
            continue
        old_expected, new_expected = old['expected'], new['expected']
        if event_metadata:
            if any(len(e['event']) != LEGACY_EVENT_HEX for e in old_expected['events']):
                blocking.append(
                    f'{case_id}: block {height} already carries event metadata '
                    'in the previous fixture; compare without --event-metadata')
                continue
            if any(len(e['event']) < LEGACY_EVENT_HEX for e in new_expected['events']):
                blocking.append(
                    f'{case_id}: block {height} has an event shorter than the '
                    'legacy encoding in the next fixture')
                continue
            old_expected, new_expected = legacy_events(old_expected), legacy_events(new_expected)
        if old_expected != new_expected:
            before, after = summarize(old['expected']), summarize(new['expected'])
            moved = {k: (before[k], after[k]) for k in before if before[k] != after[k]}
            blocking.append(
                f'{case_id}: same block {height}, same hash, different expected '
                f'state {moved or "(field-level difference)"}')

    old_final = summarize(previous['checkpoints'][-1]['expected'])
    new_final = summarize(nxt['checkpoints'][-1]['expected'])
    for field in ('events', 'utxos', 'balance'):
        before, after = old_final[field], new_final[field]
        if before == after:
            continue
        if before == 0 and after != 0:
            review.append(
                f'{case_id}: {field} was 0 at the old anchor and is {after} at '
                'the new one; confirm the case still means what its profile says')
        elif before and abs(after - before) > max(1, abs(before) * tolerance):
            review.append(
                f'{case_id}: {field} {before} -> {after} at the anchor '
                f'(> {tolerance:.0%})')
    if not shared:
        review.append(f'{case_id}: no checkpoint height in common to cross-check')
    return blocking, review


def compare(previous, nxt, tolerance, same_anchor=False, event_metadata=False):
    blocking, review, notes = [], [], []

    old_anchor = max(int(h) for h in previous['accepted_headers'])
    new_anchor = max(int(h) for h in nxt['accepted_headers'])
    notes.append(f'anchor {old_anchor} -> {new_anchor}')
    notes.append(f"cutoff {previous['cutoff_height']} -> {nxt['cutoff_height']}")
    notes.append(f"map {previous['map_sha256'][:16]}... -> {nxt['map_sha256'][:16]}...")
    if previous['map_sha256'] == nxt['map_sha256']:
        review.append('map digest is unchanged; this fixture gates the same '
                      'publication as the one it replaces')
    if same_anchor:
        if new_anchor != old_anchor:
            blocking.append(f'same-anchor comparison requires equal anchors: {old_anchor} -> {new_anchor}')
        if previous['cutoff_height'] != nxt['cutoff_height']:
            blocking.append('same-anchor comparison changed the cutoff height')
        if previous['cases'] != nxt['cases']:
            blocking.append('same-anchor comparison changed case definitions or checkpoint expectations')
        for key in ('genesis_hash', 'network', 'start_height', 'profile'):
            if previous.get('map', {}).get(key) != nxt.get('map', {}).get(key):
                blocking.append(f'same-anchor comparison changed map identity field {key}')
        notes.append('same-anchor schema replacement: case definitions and expectations must be identical')
    elif new_anchor <= old_anchor:
        blocking.append(f'new anchor {new_anchor} is not above {old_anchor}')
    if event_metadata:
        notes.append('event metadata: events compared by their legacy 87-byte '
                     'encoding; UTXOs, spends, history and balance exactly')

    for height, hash_ in previous['accepted_headers'].items():
        other = nxt['accepted_headers'].get(height)
        if other is not None and other != hash_:
            blocking.append(
                f'accepted header {height} changed {hash_[:16]}... -> '
                f'{other[:16]}..., so the two fixtures disagree on the chain')

    old_cases = {c['id']: c for c in previous['cases']}
    new_cases = {c['id']: c for c in nxt['cases']}
    for case_id in sorted(set(old_cases) - set(new_cases)):
        review.append(f'{case_id}: dropped from the specification')
    for case_id in sorted(set(new_cases) - set(old_cases)):
        review.append(f'{case_id}: new case, no previous expectations to compare')
    for case_id in sorted(set(old_cases) & set(new_cases)):
        case_blocking, case_review = compare_case(
            old_cases[case_id], new_cases[case_id], tolerance, event_metadata)
        blocking.extend(case_blocking)
        review.extend(case_review)
    return blocking, review, notes


def table(previous, nxt):
    old_cases = {c['id']: c for c in previous['cases']}
    lines = [f'{"case":<28}{"events":>16}{"utxos":>14}{"balance":>20}']
    for case in nxt['cases']:
        new = summarize(case['checkpoints'][-1]['expected'])
        old_case = old_cases.get(case['id'])
        old = summarize(old_case['checkpoints'][-1]['expected']) if old_case else None
        def cell(field, width):
            after = new[field]
            if old is None:
                return f'{after:>{width}}'
            before = old[field]
            return f'{f"{before}->{after}" if before != after else after:>{width}}'
        lines.append(f'{case["id"]:<28}{cell("events", 16)}{cell("utxos", 14)}'
                     f'{cell("balance", 20)}')
    return '\n'.join(lines)


def main():
    p = argparse.ArgumentParser(description=__doc__.split('\n')[0])
    p.add_argument('--previous', type=Path, required=True)
    p.add_argument('--next', dest='next_', type=Path, required=True)
    p.add_argument('--tolerance', type=float, default=0.25,
                   help='relative anchor-state change reported for review')
    p.add_argument('--same-anchor', action='store_true',
                   help='schema replacement at identical height; require unchanged cases, expectations, cutoff and map identity')
    p.add_argument('--event-metadata', action='store_true',
                   help='the next fixture appends v3 transaction metadata to events; compare events by their legacy 87-byte encoding')
    p.add_argument('--out', type=Path, help='write the findings as JSON')
    a = p.parse_args()

    previous, nxt = load(a.previous), load(a.next_)
    blocking, review, notes = compare(previous, nxt, a.tolerance, a.same_anchor,
                                      a.event_metadata)

    for note in notes:
        print(f'  {note}')
    print()
    print(table(previous, nxt))
    print()
    if blocking:
        print(f'BLOCKING ({len(blocking)}) -- do not freeze:')
        for item in blocking:
            print(f'  - {item}')
    else:
        print('BLOCKING (0): every shared checkpoint reduces identically')
    print()
    if review:
        print(f'REVIEW ({len(review)}) -- decide each before freezing:')
        for item in review:
            print(f'  - {item}')
    else:
        print('REVIEW (0)')

    if a.out:
        if a.out.exists():
            raise RuntimeError('output exists')
        a.out.write_text(json.dumps(
            {'notes': notes, 'blocking': blocking, 'review': review}, indent=2) + '\n')
        print(f'\nwrote {a.out}')
    raise SystemExit(1 if blocking else 0)


if __name__ == '__main__':
    main()
