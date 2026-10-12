One-shot over the window

Directory-only wallets common to every layout: total MB per wallet, p50 / p95 / p99 (mean); n = wallets in the common set

| Layout | restore-old (clipped) | reused-tail | reused-tail (clipped) | unused (clipped) |
|---|---|---|---|---|
| archive.archive-wide | 14.758 / 15.014 / 15.032 (14.779) | — | — | 14.758 / 14.758 / 14.758 (14.758) |
| archive.coupled-16k-64k | 14.758 / 14.924 / 14.942 (14.772) | — | — | 14.758 / 14.758 / 14.758 (14.758) |
| archive.coupled-8k-64k | 15.692 / 15.812 / 15.821 (15.701) | — | — | 15.692 / 15.692 / 15.692 (15.692) |
| archive.bulk-archive-wide | 6.733 / 7.108 / 7.108 (6.762) | — | — | 6.733 / 6.733 / 6.733 (6.733) |
| archive.decoupled-32k-64k | 6.733 / 7.108 / 7.108 (6.762) | — | — | 6.733 / 6.733 / 6.733 (6.733) |
| archive.decoupled-32k-32k | 6.733 / 7.108 / 7.108 (6.762) | — | — | 6.733 / 6.733 / 6.733 (6.733) |
| archive.decoupled-32k-16k | 7.209 / 7.583 / 7.583 (7.238) | — | — | 7.209 / 7.209 / 7.209 (7.209) |
| n (common) | 100 | 0 | 0 | 120 |

Share of wallets that are directory-only in each layout (of wallets in range)

| Layout | restore-old (clipped) | reused-tail | reused-tail (clipped) | unused (clipped) | all |
|---|---|---|---|---|---|
| archive.archive-wide | 85.8% of 120 | 0.0% of 15 | 0.0% of 7 | 100.0% of 120 | 85.1% of 262 |
| archive.coupled-16k-64k | 85.8% of 120 | 0.0% of 15 | 0.0% of 7 | 100.0% of 120 | 85.1% of 262 |
| archive.coupled-8k-64k | 85.8% of 120 | 0.0% of 15 | 0.0% of 7 | 100.0% of 120 | 85.1% of 262 |
| archive.bulk-archive-wide | 83.3% of 120 | 0.0% of 15 | 0.0% of 7 | 100.0% of 120 | 84.0% of 262 |
| archive.decoupled-32k-64k | 83.3% of 120 | 0.0% of 15 | 0.0% of 7 | 100.0% of 120 | 84.0% of 262 |
| archive.decoupled-32k-32k | 83.3% of 120 | 0.0% of 15 | 0.0% of 7 | 100.0% of 120 | 84.0% of 262 |
| archive.decoupled-32k-16k | 83.3% of 120 | 0.0% of 15 | 0.0% of 7 | 100.0% of 120 | 84.0% of 262 |

Paged tail (wallets that need pages in that layout, all classes)

| Layout | Wallets | Bytes MB p99 / max | Round trips p99 / max | Within bound |
|---|---:|---|---|---|
| archive.archive-wide | 39 | 748.72 / 748.72 | 2 / 2 | yes (1.00x / 1.00x) |
| archive.coupled-16k-64k | 39 | 746.28 / 746.28 | 2 / 2 | yes (1.00x / 1.00x) |
| archive.coupled-8k-64k | 39 | 747.17 / 747.17 | 2 / 2 | yes (1.00x / 1.00x) |
| archive.bulk-archive-wide | 42 | 6661.09 / 6661.09 | 2 / 2 | no (8.90x / 8.90x) |
| archive.decoupled-32k-64k | 42 | 737.50 / 737.50 | 13 / 13 | yes (0.99x / 0.99x) |
| archive.decoupled-32k-32k | 42 | 406.70 / 406.70 | 23 / 23 | yes (0.54x / 0.54x) |
| archive.decoupled-32k-16k | 42 | 243.92 / 243.92 | 34 / 34 | yes (0.33x / 0.33x) |
