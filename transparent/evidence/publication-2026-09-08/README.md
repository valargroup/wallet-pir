# Full-chain publication, 2026-09-08

The Gate 2 publication of the pinned dataset into
`/srv/zakura/transparent-shards-v7-full` on the coordinator, both tiers, by
the publish workflow. The set was not activated by this run; the pilot kept
serving its three-shard set. `publication.json` is the publisher's own
record; `cutoff.json` and `journal.json` are the pin the workflow re-derived
before publishing; `publish.log` is the per-shard log; `time.txt` is
`/usr/bin/time -v`; `SHA256SUMS` covers every file here.

| Field | Value |
|---|---|
| Run | [Publish transparent shards, run 34186118364](https://github.com/valargroup/enhance-pir/actions/runs/34186118364) |
| Tool commit | `c069b7d21f763f2522a6bbab609a0c6620a7c5ef`; `shard-publish` release binary sha256 `2fa9fa51a32372cca89d5a86e5a85516d9866071e9d878b97ab29168701a74ce` |
| Host | `enhance-pir-coordinator-01`, `m-8vcpu-64gb-intel`, ams3; the ingest unit was inactive, so this is not the coexistence measurement the gate asks for |
| Journal | heights 0–3,473,686; 352,873,356 events; genesis `00040fe8…dce08` |
| Anchor | 3,473,686, `0000000000755137…0be1d`; cutoff 3,262,749 re-derived from the anchor header time (six calendar months), equal to the [inventory](../inventory-2026-09-08/README.md) |
| Geometries | `archive-wide` 0–3,262,748 at `393216:458752, 63488:65536`; `recent-8k` 3,262,749–3,473,686 at `98304:114688, 7936:8192` |
| Result | 174 shards (160 archive, 14 recent, tail revision 0), 0 multi-segment shards, filters 60.05 MB, map 117,685 B sha256 `06e5fa2bd2547f55d4e17959994820d0bd109f3f7011d4a1105f2e50048ac909` |
| Cost | 14 min 60 s wall, peak RSS 2,588,072 KiB; set 54 GiB on disk; volume 593 → 630 GiB used |

The emitted boundaries match the [census](../census-2026-09-08/README.md)
shard for shard (same sealer, same policies); the machine comparison is the
census run with `--compare-map` against this map, recorded when it lands.
## Verification

`verify.json`, `verify.log` and `verify-time.txt` are the
[Verify transparent shards run 34187641156](https://github.com/valargroup/enhance-pir/actions/runs/34187641156)
at `a8e85bfef9b2e6c308ca9aef1cd380f32ee11937`: 13 checks, 0 failures.
Every one of the 174 manifests, table segments and filters verified against
its digest and the parent chain in 168.6 s; coverage is contiguous 0–3,473,686
from the expected start to the expected anchor hash; 160 `archive-wide`
shards lie below 3,262,749 and 14 `recent-8k` from it; `shards.json` digests
to the recorded value. Six shards chosen by seed 1 (0, 21, 22, 57, 113, 173)
were rebuilt from the journal and matched filter, directory and pages byte
for byte, 3–5 s each for an archive shard. The run took 3 min 8 s wall and
1,940,424 KiB peak RSS. The full-set rebuild (`rebuild_all`) has not been
run; the sampled reconstruction is what this record establishes.

Two earlier attempts of this run failed before writing anything: the
publisher asked the node for the block at height −1 for a set starting at
genesis (fixed in `c069b7d`), and the workflow's main-ancestry check could
not reach the remote from a credential-less checkout (fixed in `b2c91cc`).
