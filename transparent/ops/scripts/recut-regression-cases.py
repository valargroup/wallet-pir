#!/usr/bin/env python3
"""Re-cut the frozen regression case specification for a new accepted publication.

The deployed regression runner pins the set identity and every sealed entry
of the publication a fixture was exported against. A new schema or a re-cut
rewrites those entries, and even while they stand every checkpoint ages into
settled history. Continuous publication moves both boundaries the cases were
chosen around:

  * the anchor advances with the tip, and
  * the cutoff advances with it, because `shard-cutoff` derives the cutoff from
    the anchor header's time minus six calendar months -- not from a fixed
    height. Five days of new anchor moved roughly eight thousand blocks of
    cutoff at the observed block rate.

So a re-cut is not a search-and-replace of the terminal height. Each checkpoint
was chosen for a reason, and the reason decides whether it moves:

  absolute    a height chosen from one script's actual history (a receive, a
              spend, a coinbase). Sealed archive or recent history that no later
              publication rewrites. It stays put.
  cutoff+d    a tier-boundary probe. `old-receive-recent-spend`,
              `multi-script-self-transfer`, `recent-birthday` and the unused
              cases exist to cross the archive/recent seam at C-1, C and C+1.
              Pinned to the old height they still pass, but they stop testing
              the boundary, which is the whole point of the case.
  anchor+d    a tip-relative checkpoint: the terminal sync at A, and
              `recent-birthday`'s A-1.

ROLES below records that decision per case. It is verified, not trusted:
--self-check regenerates the previous specification from the previous anchor and
cutoff and refuses unless the result is byte-identical to the file on disk. A
mis-classified checkpoint cannot survive that.

This writes a case specification only. It runs no export, reads no journal and
contacts no service. `regression-export` still has to produce expectations from
a read-only journal replay against the accepted publication, and a human still
reviews and freezes the result.
"""
import argparse
import json
from pathlib import Path

# The schema v11 cut of 2026-10-09: anchor 3511700, served `recent_from`.
PREVIOUS_ANCHOR = 3511700
PREVIOUS_CUTOFF = 3289805

# (case id, profile, required_from role, [checkpoint roles])
# A role is ('abs', height) | ('cutoff', delta) | ('anchor', delta).
# The tail checkpoint sampled by one cut is settled history at the next, so it
# stays put like any other absolute height.
SAMPLED = ('abs', 3492693)
ROLES = [
    ('unused-p2pkh', 'unused', ('abs', 0),
     [('abs', 0), ('cutoff', -1), ('cutoff', 1), SAMPLED, ('anchor', 0)]),
    ('unused-p2sh', 'unused-p2sh', ('abs', 0),
     [('abs', 0), ('cutoff', 1), SAMPLED, ('anchor', 0)]),
    ('small-active', 'small-active', ('abs', 0),
     [('abs', 3423034), ('abs', 3423035), ('abs', 3423036), SAMPLED,
      ('anchor', 0)]),
    ('zero-balance', 'zero-balance', ('abs', 0),
     [('abs', 2981966), ('abs', 2981967), ('abs', 3423637), SAMPLED,
      ('anchor', 0)]),
    ('old-receive-recent-spend', 'cross-tier', ('abs', 0),
     [('abs', 1080215), ('cutoff', -1), ('cutoff', 0), ('abs', 3332159),
      ('abs', 3332160), SAMPLED, ('anchor', 0)]),
    ('offline-receive-spend', 'offline', ('abs', 0),
     [('abs', 163548), ('abs', 163726), ('abs', 3439126), ('abs', 3465622),
      ('abs', 3472534), SAMPLED, ('anchor', 0)]),
    ('active-p2sh', 'p2sh', ('abs', 0),
     [('abs', 3460734), ('abs', 3460735), ('abs', 3460736), SAMPLED,
      ('anchor', 0)]),
    ('reused-pages', 'reused-script', ('abs', 3463930),
     [('abs', 3463930), ('abs', 3469904), ('abs', 3469905), SAMPLED,
      ('anchor', 0)]),
    ('multi-script-self-transfer', 'multi-script', ('abs', 0),
     [('cutoff', -1), ('cutoff', 0), ('cutoff', 1), SAMPLED, ('anchor', 0)]),
    ('recent-birthday', 'recent-restore', ('cutoff', 0),
     [('cutoff', 0), ('cutoff', 1), SAMPLED, ('anchor', -1), ('anchor', 0)]),
    # The coinbase heights are real coinbase outputs, not "eight below the tip".
    # Holding them absolute keeps the case meaningful and moves it out of the
    # reorg-exposed window; it stops probing a near-tip coinbase, which is
    # reported as a coverage note rather than silently accepted.
    ('coinbase', 'coinbase', ('abs', 0),
     [('abs', 3473678), ('abs', 3473679), SAMPLED, ('anchor', 0)]),
]


def resolve(role, anchor, cutoff):
    kind, value = role
    if kind == 'abs':
        return value
    if kind == 'cutoff':
        return cutoff + value
    if kind == 'anchor':
        return anchor + value
    raise RuntimeError(f'unknown checkpoint role {kind!r}')


def build(previous, anchor, cutoff, tail_checkpoints):
    """Return the re-cut specification, preserving each case's scripts."""
    by_id = {c['id']: c for c in previous}
    if sorted(by_id) != sorted(r[0] for r in ROLES):
        raise RuntimeError('previous specification does not match ROLES case set')
    out = []
    for case_id, profile, required_role, checkpoint_roles in ROLES:
        source = by_id[case_id]
        if source['profile'] != profile:
            raise RuntimeError(f'{case_id}: profile changed from {profile!r}')
        heights = [resolve(r, anchor, cutoff) for r in checkpoint_roles]
        heights.extend(tail_heights(anchor, tail_checkpoints))
        heights = sorted(set(heights))
        if heights[-1] != anchor:
            raise RuntimeError(f'{case_id}: terminal checkpoint must be the anchor')
        out.append({
            'id': case_id,
            'profile': profile,
            'scripts': list(source['scripts']),
            'required_from': resolve(required_role, anchor, cutoff),
            'heights': heights,
        })
    return out


def tail_heights(anchor, count):
    """Checkpoints inside the range published incrementally since the last cut.

    The frozen fixture only ever covered a single static full-chain publication.
    Sampling the newly published span is what makes the gate cover revisions
    that continuous publication produced.
    """
    if count < 0:
        raise RuntimeError('tail checkpoint count must not be negative')
    span = anchor - PREVIOUS_ANCHOR
    if count == 0:
        return []
    if span <= count + 1:
        raise RuntimeError('new publication span is too short for tail checkpoints')
    step = span // (count + 1)
    return [PREVIOUS_ANCHOR + step * (i + 1) for i in range(count)]


def check(cases, anchor, cutoff):
    """Refuse a specification the exporter or runner would reject later."""
    for case in cases:
        heights = case['heights']
        if heights != sorted(set(heights)):
            raise RuntimeError(f"{case['id']}: heights must be sorted and unique")
        if not case['scripts']:
            raise RuntimeError(f"{case['id']}: no scripts")
        if heights[0] < case['required_from']:
            raise RuntimeError(
                f"{case['id']}: checkpoint precedes required_from")
        if heights[-1] > anchor:
            raise RuntimeError(f"{case['id']}: checkpoint above the anchor")
        for script in case['scripts']:
            raw = bytes.fromhex(script)
            p2pkh = len(raw) == 25 and raw[:3] == b'\x76\xa9\x14' and raw[23:] == b'\x88\xac'
            p2sh = len(raw) == 23 and raw[:2] == b'\xa9\x14' and raw[22] == 0x87
            if not (p2pkh or p2sh) or raw.hex() != script:
                raise RuntimeError(f"{case['id']}: noncanonical script")
    if cutoff >= anchor:
        raise RuntimeError('cutoff must precede the anchor')


def hazards(anchor, cutoff):
    """Conditions only a journal replay can settle, stated before the export."""
    notes = []
    if cutoff != PREVIOUS_CUTOFF:
        notes.append(
            f'recent-birthday required_from moves {PREVIOUS_CUTOFF} -> {cutoff}. '
            'regression-export refuses with "birthday omits earlier activity" if '
            'any of its 10 scripts were active in between. Re-select that case '
            'if it does.')
        notes.append(
            f'cutoff moved {PREVIOUS_CUTOFF} -> {cutoff}; heights in that span '
            'change tier. Confirm the published archive/recent split agrees.')
    notes.append(
        'unused-p2pkh and unused-p2sh must still have no activity at the new '
        'anchor, or the exporter fails their profile check.')
    notes.append(
        'coinbase checkpoints stay at 3473678/3473679 and are now deeply '
        'confirmed; this case no longer probes a near-tip coinbase.')
    notes.append(
        'Compare exported expectations against the previous fixture before '
        'freezing: a case whose event count or balance changed character needs '
        'review, not acceptance.')
    return notes


def main():
    p = argparse.ArgumentParser(description=__doc__.split('\n')[0])
    p.add_argument('--previous-cases', type=Path, required=True)
    p.add_argument('--anchor', type=int)
    p.add_argument('--cutoff', type=int)
    p.add_argument('--cutoff-json', type=Path,
                   help='cutoff.json from the new publication; supplies both heights')
    p.add_argument('--tail-checkpoints', type=int, default=1,
                   help='checkpoints sampled from the newly published span')
    p.add_argument('--out', type=Path)
    p.add_argument('--self-check', action='store_true',
                   help='regenerate the previous specification and compare')
    a = p.parse_args()

    raw = a.previous_cases.read_bytes()
    previous = json.loads(raw)

    if a.self_check:
        again = build(previous, PREVIOUS_ANCHOR, PREVIOUS_CUTOFF, 0)
        if again != previous:
            raise RuntimeError(
                'ROLES does not reproduce the previous specification; a '
                'checkpoint is misclassified')
        print(f'self-check: ROLES reproduces {a.previous_cases} exactly '
              f'({len(previous)} cases)')
        if a.anchor is None and a.cutoff_json is None:
            return

    anchor, cutoff = a.anchor, a.cutoff
    if a.cutoff_json:
        doc = json.loads(a.cutoff_json.read_text())
        anchor, cutoff = doc['anchor']['height'], doc['cutoff']['height']
    if anchor is None or cutoff is None:
        raise RuntimeError('supply --anchor and --cutoff, or --cutoff-json')
    if anchor <= PREVIOUS_ANCHOR:
        raise RuntimeError('new anchor must be above the frozen one')

    cases = build(previous, anchor, cutoff, a.tail_checkpoints)
    check(cases, anchor, cutoff)

    print(f'anchor {PREVIOUS_ANCHOR} -> {anchor}   '
          f'cutoff {PREVIOUS_CUTOFF} -> {cutoff}')
    print(f'{"case":<28}{"req_from":>10}  heights')
    for case in cases:
        print(f'{case["id"]:<28}{case["required_from"]:>10}  {case["heights"]}')
    total = sum(len(c['heights']) for c in cases)
    print(f'\n{len(cases)} cases, {total} checkpoints')
    print('\nreview before export:')
    for note in hazards(anchor, cutoff):
        print(f'  - {note}')

    if a.out:
        if a.out.exists():
            raise RuntimeError('output exists; frozen specifications are not overwritten')
        a.out.write_text(json.dumps(cases, indent=2) + '\n')
        print(f'\nwrote {a.out}')


if __name__ == '__main__':
    main()
