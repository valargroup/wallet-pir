# Directory placement at 4,096 rows, 2026-09-28

This note corrects a conclusion in the
[single-lookup census](../single-lookup-census-2026-09-27/README.md), whose
retained text stays as written.

The census reported that `recent-4k-8k` two-choice placement reached **14 of 14
slots** in shard 172. From that it concluded the geometry was "at the edge of
needing a second segment" and not safe without denser placement. That reading was
wrong. A full row is expected under two-choice placement with breadth-first
relocation. The cost event is an **overflow**: a script that no relocation path
can place, which adds a segment. Row load does not measure distance to one.

## Method

`placement.py` reimplements the builder's rule:
- less-loaded candidate first;
- breadth-first relocation capped at 512 visited rows, as `MAX_RELOCATION_VISITS`;
- 4,096 rows × 14 slots (57,344).

It uses synthetic keys hashed with SHA-256. Candidate rows are uniform, as for real
scripts. The script ran on `roman-ipir-bench-8vcpu` (Python 3.12); raw output is in
`results.txt`. It is a model of the Rust placer, not the placer itself. The
[census](../single-lookup-census-2026-09-27/README.md) supplies the real-data
check: all 14 real recent shards fit one 4,096-row segment.

## Results

- **Two choices:** no overflow in any run, from 79% to 96% of capacity.
  - 3 seeds at each of 45,454 (the largest real shard), 49,152, 53,000 and 55,000
    scripts.
  - 40 further seeds each at 49,152 (the `recent-4k-8k` seal target, 85.7%) and
    51,000 (88.9%).
- **Three or four choices:** fewer full rows at the seal target (0–4 against
  87–112), and likewise no overflow.

## Consequence

- `recent-4k-8k` is feasible with the current placement and seal policy.
- More choices, now query-free under a
  [choice table](../single-lookup-measure-2026-09-27/README.md), would add
  headroom but are not required.
- Combined with choice tables, recent-4k-8k is projected at about 111,640 against
  133,144 bytes per directory query, about 4 more points off a six-month restore.
- It is a geometry change, so it belongs to a new publication.
