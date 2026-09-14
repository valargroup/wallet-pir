# Parent-filter evaluation, 2026-09-08

Frozen publication: height **3,473,686**. The journal had advanced beyond this anchor; extraction used only the pinned publication ranges. This is a synthetic public-script workload, not an estimate of the wallet population.

## Offline result

All **174** child filters reconstructed byte-for-byte from the complete journal. All **39** parent configurations passed full union inclusion checks and **80,700,000** exactly absent probes were measured. The split contains 80 tuning and 40 held-out wallets for each of nine profiles.

Keep recent filters direct. No tested recent parent configuration improves the primary bandwidth objective. The three tuning-selected archive finalists are K=8, K=16 and K=4, all with M=100 and P=6. K=50 was evaluated and did not make the finalists.

| Configuration | Held-out mixed filter + metadata MiB/wallet | Recent mean KiB/wallet |
| --- | ---: | ---: |
| Direct baseline | 13.583 | 196.937 |
| Archive K=8, M=100, P=6 | 5.203 | 196.937 |
| Archive K=16, M=100, P=6 | 5.280 | 196.937 |
| Archive K=4, M=100, P=6 | 5.397 | 196.937 |
| Archive K=50, M=100, P=6 | 5.723 | 196.937 |

K=8 saves approximately **61.7% of mixed filter/metadata payload**. This is not a claim about total recovery traffic or latency. Its complete archive parent set occupies 12,752,297 bytes. Recent and provisional-tail behavior remains direct.

The 1,000-absent-script archive stress case descends through every parent and adds 12,857,983 bytes, including metadata, over direct retrieval. The 100-script case still skips 56 children. These stress cases have no product weight and must be considered before generalizing the small-wallet result.

## Reproduction and provenance

See [evaluation instructions](../../../docs/transparent-pir/parent-filter-evaluation.md). The remote experiment root is `/srv/zakura/parent-filter-eval-20260908/` on the coordinator. It retains raw per-wallet tables, candidate manifests, digest-addressed filters, extracted script sets, configs and HTTP traces. The local rendered report is `/tmp/transparent-parent-filter-eval-20260908/report.html`.

The source archive was created from commit `22e6becacdd0107683cda7280c5585fac9f4f4b2` and overlaid with experiment files; it is not a clean build of that commit. [remote-provenance.txt](remote-provenance.txt) records archive/overlay and executable hashes and machine details. The HTTP binaries used `release-fast` with `-C target-cpu=x86-64-v3`, four build jobs. The isolated server binds loopback, disables live activation and whole-publication background prewarming, and uses a 28 GiB runtime cache with default concurrency limits. The coordinator has other background workloads; it is not an exclusive benchmark host.

[sweep.json](sweep.json) preserves complete per-group sweep counters and extraction provenance. [offline-summary.json](offline-summary.json) preserves finalists, baseline, archive comparisons and stress rows.

Prior ledger preparation is explicitly journal-verified: all 360 held-out seed files passed sample/map/anchor, measured-window digest/count and independent prior-ledger consistency checks. Measured clients begin with cold filter/setup caches and uncovered recovery intervals. These seeds avoid repeating full-genesis HTTP preparation; they do not replace measured HTTP/PIR recovery.

## HTTP outcome: incomplete, no adoption recommendation

The first baseline warm-up stopped at the unchanged 600-second scenario deadline: **19 exact recoveries, 1 timeout**. Held-out sample index 321 (`reused-tail`) expected 530,514 events. It completed 6,523 private page queries and transferred 2,812,689,592 request payload bytes before the deadline; recorded HTTP requests had no failures or overload responses. This is an unfinished baseline recovery, not evidence of a parent-filter correctness failure.

The runner stopped before any of the 40 planned measured variant waves. Consequently, no paired latency or total-byte improvement is established, and no new production parameter is recommended. K=8/M=100/P=6 remains the offline archive candidate; recent filters remain direct. [http-decision.json](http-decision.json) explicitly records an incomplete decision with a null recommendation. [baseline-warmup-summary.json](baseline-warmup-summary.json) records every outcome and the hash of the full raw report retained remotely and under `/tmp/transparent-parent-filter-eval-20260908/baseline-warmup.json`.

A previous unmeasured attempt under the remote `http/` directory was interrupted during repeated full-history HTTP preparation; its evidence is retained. The journal-seeded attempt above used the same frozen held-out wallets. Neither attempt supplied candidate measurements or affected offline ranking.

The next experiment must first establish a baseline completion budget for the high-event reused-address workload, then run the **same frozen finalists and paired seeds** with identical limits across all variants. Do not omit the slow wallet, treat the timeout as an exact recovery, or claim the offline 61.7% figure as an HTTP result. Retain this failed attempt alongside any repeat.

## Validation

The filter suite, wallet/store suites, existing anchor/continuation/sync suites, loadtest unit and simulation suites, independent journal seed test, and Python selection tests passed. The final HTTP/PIR regression test passed with SQLite restart, accepted-prefix clipping, rollback, script import, and corrupt/missing/stale/wrong-schema/overlapping parent fallback cases. Incomplete HTTP runs cannot emit an adoption recommendation. The final rejected-manifest hardening was tested locally after the baseline run; the recorded remote binaries therefore precede that small fallback-only change.
