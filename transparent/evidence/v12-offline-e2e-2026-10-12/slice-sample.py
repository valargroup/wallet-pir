#!/usr/bin/env python3
"""Restrict the frozen workload sample to the wallets a recent-slice publication covers.

The hub holds a read-only slice of the v3 journal from the sample's cutoff
(3,262,749) to its anchor (3,473,686). A wallet is kept only when its whole
expected range lies inside that slice, so its expected digest is unchanged and
an exact recovery from the slice means the same thing it means against the
whole chain. Wallets reaching below the cutoff are dropped, not rebased.
Usage: slice-sample.py SAMPLE START OUT
"""
import json
import sys

sample_path, start, out = sys.argv[1], int(sys.argv[2]), sys.argv[3]
sample = json.load(open(sample_path))
kept = [c for c in sample['clients'] if c['required_from'] >= start]
sample['clients'] = kept
sample['start_height'] = start
sample['classes'] = {k: sum(1 for c in kept if c['class'] == k) for k in sample['classes']}
sample['derived_from'] = {'sample': sample_path, 'rule': f'required_from >= {start}'}
json.dump(sample, open(out, 'w'), indent=1, sort_keys=True)
print(json.dumps(sample['classes']))
