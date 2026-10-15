# Verification carried across revisions: paired before/after, 2026-10-15

What a worker's prepare spends loading a republished tail, before (foundation
263097cc, which reuses only a whole revision with the same digest) and after
this change (per-segment evidence). The same benchmark source was compiled on
both sides; five interleaved pairs of three loads each, on the shared
development hub at load average 12–48 (other tenants). `manifest.json` holds
binaries, workload, per-run load averages and min/median/max; `raw.jsonl` every
load.

Revision one's tables are 176.2 MB, of which its sealed first page shard (three
segments, hard-linked from revision zero) is about 100 MB.

| Median of 15 loads | Base | Head |
|---|---|---|
| Republished tail, reusing the previous publication: bytes read | 176.2 MB | 75.5 MB |
| … CPU | 1,120 ms | 510 ms |
| … wall | 1,805 ms | 793 ms |
| … peak RSS during the load | 257 MB | 257 MB |
| Cold first publication: bytes read | 142.6 MB | 142.6 MB |
| … CPU | 890 ms | 900 ms |

Supported claims: on this workload the loader no longer reads or hashes the
unchanged sealed page shard, cutting the load's bytes read and CPU by about
57% and 54%, which is the part of activation delay spent loading; a cold load
and peak RSS are unchanged. Wall times are contended and only indicative.
Not measured, so not claimed: query latency quantiles (query evaluation and
runtime builds are untouched and no query workload ran), production-scale
sets, and the publisher's own loads. The zero-hash property itself is proven
by `tests/verification_reuse.rs`, which counts hashed page bytes.
