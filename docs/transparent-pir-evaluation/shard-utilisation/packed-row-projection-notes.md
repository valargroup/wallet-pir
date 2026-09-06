# Packed row demand, projected

Measured 2026-09-06 on the coordinator, from commit `922f173`, through the
`census` action of the transparent-event backfill workflow against the journal
at `/srv/zakura/transparent-event-data/superseded-v1` — the same Ironwood
activation through height 3,473,474 as
[the v4 census](census-v4.txt) and [the v4 measurement](measurement-v4.json).

Raw output: [packed-row-projection.txt](packed-row-projection.txt).

## What this is, and is not

It is arithmetic over per-script event counts. At this commit there is no v5
codec, no packer and no published set: the boundaries are the v4 boundaries,
sealed on the unpacked figure exactly as the shipped build does, with packed row
demand carried alongside them. The v4 half of the output is byte-identical to
`census-v4.txt`, which is what establishes that the streaming rewrite of the
census changed no boundary.

So this says what the packing rule *would ask for*. It does not measure emitted
bytes, and it cannot: encoding overhead is modelled as the specified 64-byte
entry header, and directory placement is modelled as `scripts / slots` rather
than run through the real two-choice placer — the latter is true of the archived
v4 census as well, so directory segment counts in both are a lower bound.

## The result

At the 24k script policy, the one adjacent to the shipped 25,000:

| | v4 | packed |
|---|---:|---:|
| Page rows demanded | 40,579 | 20,937 |
| Used rows, how full | 45.2% of slots | 88.1% of bytes |
| Page rows used | 82.6% | 42.6% |
| Live bytes | 98.4 MB | 95.9 MB |
| **Pinned plaintext** | **220.2 MB** | **220.2 MB** |

Packing works, and it saves nothing.

It works in the sense it was meant to: 25,886 short histories that needed 25,886
rows need 6,244, and the rows that are used come out 88.1% full by bytes rather
than 45.2% by slots. That is the padding the change was aimed at, and it is gone.

It saves nothing because the page table is padded to `PAGE_ROWS` whatever it
holds. 6,763 rows and 3,489 rows both round to one segment of 8,192, so the
pinned figure does not move at all. Every recovered row was already padding.
Live bytes fall by 2.5 MB only because the accounting is honest — 64-byte entry
headers where there were 128-byte page headers — which is a reporting change and
not a saving.

Two further findings decide what to do next.

**Long histories are now the whole problem.** 1,701 scripts need 14,693 rows,
which is 70% of all demand, and packing cannot touch them: at 18 paged events an
entry is more than half a row, and from there each fragment takes a row of its
own exactly as in v4. The 25,886 short histories that packing does help account
for 6,244. Shrinking the short-history term further has almost nothing left to
win.

**Page rows stop closing generations.** At every policy in the sweep, zero
shards reach the page-row target of 7,900. The script limit binds alone, and the
page table becomes the mostly-empty one — the same waste as before, moved into
the more expensive table. That is what puts geometry on the critical path:
packing is not itself the saving, it is the precondition for a smaller page
table, and the two have to be measured together.

## Cost of the census

1.63 s wall, 20,196 KB peak resident, for 45,332 blocks and 869,283 events
across five policies. The earlier census held every block before replaying them
per policy; this one streams once and feeds all five from that pass. The old
form is why a full-journal census was not runnable, not a tuning detail.
