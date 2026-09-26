# Native two-mask production deployment and six-hour load — 2026-09-26

`dc0355b` was deployed over SSH to every Enhance and Status role, with CI skipped
at the operator's request. Enhance moved from `v8-native-poc` to
`ironwood-enhance-pir-v9-native-two-mask-m29`, and Status from `status-pir-v2-q48`
to `status-pir-v3-native-two-mask-m29`. Both use fresh state roots. The v8 and q48
state, binaries and unit drop-ins are preserved. `manifest.json` records binaries,
hosts and commands. Correctness certificates for the live snapshots are in
[native-certificate-2026-09-26](../native-certificate-2026-09-26/README.md).

## Cutover

The Haswell binaries built on the coordinator (Ubuntu 24.04) need glibc 2.39,
which the Ubuntu 22.04 Paperspace hosts lack. A startup check caught this before
the switch, and those binaries were rebuilt on ams1. The new state directories
were created as root, but ingress, router and workers run as dedicated users, so
those roles crash-looped for about three minutes until ownership was fixed. The
unit restart counters (39–57) date from that loop and did not change during the
load runs. The coordinator reported `worker pool capacity exhausted` while no
worker was up, then published normally.

Initial checks over the public origin: 60/60 exact answers at 2 QPS, then
1,200/1,200 at 20 QPS (p99 170 ms, against 112 ms in the v8 trial's 20 QPS
minute). The Status live probe returned 1,200/1,200 correct at 20 QPS (p99 76 ms).
All three Enhance workers served; the GPU took about 45% of evaluations.

## Six-hour load

| Product | Offered | Correct | Incorrect | Errors | Latency |
| --- | ---: | ---: | ---: | --- | --- |
| Enhance, public HTTPS, 28-record oracle | 429,000 | 428,868 | 0 | 130 × 502, 2 × 503 | per-minute p99: median 153 ms, max 245 ms |
| Status, live mined-answer probe | 432,000 | 432,000 | 0 | none | p50 37 ms, p99 77 ms, max 405 ms |

There were no unsent arrivals, no role restarts and no cgroup OOM kills. The
coordinator peaked at 2,756 MiB and the router at 1,202 MiB; the CPU workers
peaked at 2,221 and 2,070 MiB. Enhance published generations 1–234 during the
run, and the anchor advanced to 3,496,753.

The Enhance driver started at 04:13 instead of 03:59: its first launch used a
relative script path under `systemd-run` and failed immediately. Every batch
used a zero-error threshold, so any error marks its batch as failed. 91 of 358
batches recorded at least one error. These are retained; all errors were
transport statuses, and no answer was wrong.

## Enhance 502 root cause

A loopback capture showed ingress answering `409 stale_routing` after only
49–115 KB of the 228,468-byte upload, then resetting the connection. The
deployed ingress reads just the 116-byte routing prefix before comparing
generations. On a mismatch it answers and drops the partly read body; Linux then
resets the connection. When that reset reaches Caddy before the response, Caddy
reports 502. 25 of the first 107 502s fell within 0.2 s after a generation change,
though generation-change windows cover only about 1% of the run. Ingress counted
every arrival as a 200 or 409 response, and none as 429.

Main `65ebcb1` buffers the whole upload before any routing decision, which removes
this path. Its upload-cap rejections leave the body unread. A raw-TCP test on
Linux showed they are answered without a reset, so no further ingress change was
made. Redeploying from main should remove these 502s; this has not been measured.

## Limits

This is one six-hour run of each product from a single client host; it is not
the formal qualification gate. The Enhance oracle covers 28 records, and the
Status oracle covers canonical mined answers only. It does not establish
publication completeness, mempool correctness or multi-client behaviour. Status
public routes remain disabled, and the wallet library speaks only
`status-pir-v2-q48`.

`raw/` holds the batch reports, the Status summary, derived per-minute series
and reduced samples: generation changes and 15-minute router/worker counters.
The complete 33 MB archive stays on the coordinator; its path and SHA-256 are in
`manifest.json`.
