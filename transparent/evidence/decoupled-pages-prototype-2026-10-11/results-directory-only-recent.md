Tip catch-up with the run cache, pooled over anchors in catch-up-anchors

Directory-only wallets common to every layout: total MB per wallet, p50 / p95 / p99 (mean); n = wallets in the common set

| Layout | catch-up-1d | catch-up-30d | catch-up-7d |
|---|---|---|---|
| recent-4k-8k | 0.118 / 0.179 / 0.236 (0.113) | 0.285 / 0.507 / 0.532 (0.295) | 0.136 / 0.352 / 0.369 (0.152) |
| coupled-2k-8k | 0.075 / 0.250 / 0.250 (0.085) | 0.241 / 0.414 / 0.414 (0.258) | 0.110 / 0.250 / 0.250 (0.125) |
| bulk-recent-4k-8k | 0.241 / 0.345 / 0.380 (0.206) | 0.321 / 0.613 / 0.646 (0.372) | 0.258 / 0.321 / 0.380 (0.243) |
| decoupled-4k-8k | 0.241 / 0.345 / 0.380 (0.206) | 0.321 / 0.613 / 0.646 (0.372) | 0.258 / 0.321 / 0.380 (0.243) |
| decoupled-8k-8k | 0.277 / 0.616 / 0.616 (0.311) | 0.277 / 0.616 / 0.616 (0.344) | 0.277 / 0.616 / 0.616 (0.311) |
| n (common) | 509 | 786 | 726 |

Share of wallets that are directory-only in each layout (of wallets in range)

| Layout | catch-up-1d | catch-up-30d | catch-up-7d | all |
|---|---|---|---|---|
| recent-4k-8k | 59.6% of 960 | 82.8% of 960 | 78.2% of 960 | 73.5% of 2880 |
| coupled-2k-8k | 61.4% of 960 | 84.0% of 960 | 78.2% of 960 | 74.5% of 2880 |
| bulk-recent-4k-8k | 55.6% of 960 | 82.1% of 960 | 76.4% of 960 | 71.4% of 2880 |
| decoupled-4k-8k | 56.8% of 960 | 82.1% of 960 | 77.5% of 960 | 72.1% of 2880 |
| decoupled-8k-8k | 54.2% of 960 | 82.1% of 960 | 77.4% of 960 | 71.2% of 2880 |

Paged tail (wallets that need pages in that layout, all classes)

| Layout | Wallets | Bytes MB p99 / max | Round trips p99 / max | Within bound |
|---|---:|---|---|---|
| recent-4k-8k | 762 | 1.02 / 1.93 | 2 / 2 | yes (1.00x / 1.00x) |
| coupled-2k-8k | 734 | 0.74 / 1.85 | 2 / 2 | yes (0.72x / 0.96x) |
| bulk-recent-4k-8k | 825 | 1.92 / 2.60 | 2 / 2 | no (1.89x / 1.35x) |
| decoupled-4k-8k | 803 | 1.06 / 2.04 | 4 / 4 | yes (1.04x / 1.05x) |
| decoupled-8k-8k | 829 | 1.11 / 2.04 | 3 / 4 | yes (1.09x / 1.06x) |

One-shot over the window

Directory-only wallets common to every layout: total MB per wallet, p50 / p95 / p99 (mean); n = wallets in the common set

| Layout | catch-up-1d | catch-up-30d | catch-up-7d | multi-script | restore-6m | restore-old (clipped) | reused-tail | reused-tail (clipped) | small-active | small-active (clipped) | unused (clipped) |
|---|---|---|---|---|---|---|---|---|---|---|---|
| recent.recent-4k-8k | 0.171 / 0.171 / 0.171 (0.171) | 0.507 / 0.532 / 0.605 (0.515) | 0.369 / 0.425 / 0.425 (0.364) | — | 1.518 / 1.616 / 1.631 (1.536) | 1.139 / 1.219 / 1.221 (1.145) | — | — | 0.619 / 1.218 / 1.218 (0.675) | 1.220 / 1.221 / 1.221 (1.220) | 1.139 / 1.139 / 1.139 (1.139) |
| recent.coupled-2k-8k | 0.250 / 0.250 / 0.250 (0.250) | 0.414 / 0.414 / 0.459 (0.404) | 0.250 / 0.250 / 0.250 (0.250) | — | 1.484 / 1.552 / 1.599 (1.484) | 1.145 / 1.213 / 1.213 (1.150) | — | — | 0.644 / 1.212 / 1.212 (0.657) | 1.213 / 1.213 / 1.213 (1.213) | 1.145 / 1.145 / 1.145 (1.145) |
| recent.bulk-recent-4k-8k | 0.319 / 0.319 / 0.319 (0.319) | 0.613 / 0.702 / 0.702 (0.627) | 0.319 / 0.319 / 0.319 (0.319) | — | 1.430 / 1.512 / 1.568 (1.441) | 1.052 / 1.141 / 1.141 (1.059) | — | — | 0.857 / 1.141 / 1.230 (0.680) | 1.141 / 1.141 / 1.141 (1.139) | 1.052 / 1.052 / 1.052 (1.052) |
| recent.decoupled-4k-8k | 0.319 / 0.319 / 0.319 (0.319) | 0.613 / 0.702 / 0.702 (0.627) | 0.319 / 0.319 / 0.319 (0.319) | — | 1.430 / 1.512 / 1.568 (1.441) | 1.052 / 1.141 / 1.141 (1.059) | — | — | 0.857 / 1.141 / 1.230 (0.680) | 1.141 / 1.141 / 1.141 (1.139) | 1.052 / 1.052 / 1.052 (1.052) |
| recent.decoupled-8k-8k | 0.616 / 0.616 / 0.616 (0.616) | 0.616 / 0.616 / 0.616 (0.616) | 0.616 / 0.616 / 0.616 (0.616) | — | 1.478 / 1.556 / 1.556 (1.480) | 0.994 / 1.123 / 1.123 (1.004) | — | — | 1.123 / 1.123 / 1.243 (0.861) | 1.123 / 1.123 / 1.123 (1.119) | 0.994 / 0.994 / 0.994 (0.994) |
| n (common) | 40 | 77 | 63 | 0 | 44 | 116 | 0 | 0 | 92 | 14 | 120 |

Share of wallets that are directory-only in each layout (of wallets in range)

| Layout | catch-up-1d | catch-up-30d | catch-up-7d | multi-script | restore-6m | restore-old (clipped) | reused-tail | reused-tail (clipped) | small-active | small-active (clipped) | unused (clipped) | all |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| recent.recent-4k-8k | 57.5% of 120 | 65.8% of 120 | 57.5% of 120 | 0.0% of 120 | 40.8% of 120 | 96.7% of 120 | 0.0% of 10 | 0.0% of 110 | 88.7% of 106 | 100.0% of 14 | 100.0% of 120 | 56.5% of 1080 |
| recent.coupled-2k-8k | 36.7% of 120 | 67.5% of 120 | 54.2% of 120 | 0.0% of 120 | 41.7% of 120 | 96.7% of 120 | 0.0% of 10 | 0.0% of 110 | 88.7% of 106 | 100.0% of 14 | 100.0% of 120 | 54.1% of 1080 |
| recent.bulk-recent-4k-8k | 36.7% of 120 | 64.2% of 120 | 54.2% of 120 | 0.0% of 120 | 38.3% of 120 | 96.7% of 120 | 0.0% of 10 | 0.0% of 110 | 89.6% of 106 | 100.0% of 14 | 100.0% of 120 | 53.4% of 1080 |
| recent.decoupled-4k-8k | 36.7% of 120 | 64.2% of 120 | 54.2% of 120 | 0.0% of 120 | 38.3% of 120 | 96.7% of 120 | 0.0% of 10 | 0.0% of 110 | 89.6% of 106 | 100.0% of 14 | 100.0% of 120 | 53.4% of 1080 |
| recent.decoupled-8k-8k | 33.3% of 120 | 64.2% of 120 | 52.5% of 120 | 0.0% of 120 | 36.7% of 120 | 96.7% of 120 | 0.0% of 10 | 0.0% of 110 | 86.8% of 106 | 100.0% of 14 | 100.0% of 120 | 52.4% of 1080 |

Paged tail (wallets that need pages in that layout, all classes)

| Layout | Wallets | Bytes MB p99 / max | Round trips p99 / max | Within bound |
|---|---:|---|---|---|
| recent.recent-4k-8k | 470 | 83.31 / 564.21 | 2 / 2 | yes (1.00x / 1.00x) |
| recent.coupled-2k-8k | 496 | 83.30 / 564.28 | 2 / 2 | yes (1.00x / 1.00x) |
| recent.bulk-recent-4k-8k | 503 | 208.01 / 1425.65 | 2 / 2 | no (2.50x / 2.53x) |
| recent.decoupled-4k-8k | 503 | 82.20 / 566.59 | 5 / 5 | yes (0.99x / 1.00x) |
| recent.decoupled-8k-8k | 514 | 82.16 / 566.42 | 7 / 7 | yes (0.99x / 1.00x) |
