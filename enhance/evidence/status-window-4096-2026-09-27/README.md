# Status window raised to 4,096 blocks — 2026-09-27

The live controller's `window_blocks` changed from 64 (about 80 minutes) to
4,096, the source cap (`MAX_INSPECTION_BLOCKS`), about 3.6 days at the
measured 75.5-second spacing. No code or binary changed. The controller
remains `c5a6486`; roles run on the CPU host from
[status-cpu-host-2026-09-27](../status-cpu-host-2026-09-27/README.md).

## Change

The source checkpoint records its window size, and `RollingWindow::open`
rejects a mismatch, so the controller would not start with the old checkpoint.
With APM in shadow mode, the controller was stopped at 16:04:32.9 UTC. The
64-block `source.bin` was archived on the coordinator under
`/root/status-window-4096-20260927/`, with the previous
`controller-native.json`. The config was changed and the controller started.
Archiving the checkpoint also dropped its retained fork observations. The
authority directory was not touched.

The controller refetched 4,096 blocks and published recovery epoch 10 at
16:04:52.9: public Status was unavailable for about 20 seconds. The first
publication needed a full rebuild (worker 8.5 s, router 3.6 s, queryable
13.7 s after observation). Later updates were sparse, as before.

## Result

The public manifest reports `coverage_start` 3,494,078 and `anchor_height`
3,498,173 (4,096 blocks) with 32,331 entries: 2.1% of the 1,572,864-entry
admission limit and 1.5% of the 2,097,152 physical slots. A sample of 201
recent blocks averaged 7.6 transactions (maximum 48).

10 minutes at 20 QPS through the router query origin: 12,000/12,000 correct,
p50 48 ms, p99 169 ms, maximum 380 ms; observation age p50 5.7 s, maximum
17.3 s. 130 publications followed the restart, none with a warning; no role
restarted and there were no OOM kills. The worker's memory peak rose to 4.7 GB
(limit 6 GB).

## Alerting

The outage opened a shadow `status_init_5xx` incident. APM was switched back to
active about a minute after the restart, before that incident recovered, so a
real FIRED (16:05:30) and RECOVERED (16:05:45) pair reached Slack after serving
had already recovered. Wait for shadow recovery before re-arming.

## Limits

Both load probes query only the anchor block's first transaction. No encrypted
query in this record checked an answer deep in the window; those entries use
the same index and database. A cold start with no checkpoint must fetch 4,096
blocks before the tip moves; that took well under the block interval here, but
it is the constraint on any larger window.
