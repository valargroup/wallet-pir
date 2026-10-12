Runs: 716 recorded, 0 failed.

| Tier | Table | Variant | Tables | Upload (prefix) B | Response B | Published B | Held MiB | Build s | Answer p50 / p99 ms | Paired p50 ms |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| archive | directory | baseline | 2 | 207,880 (207,880) | 5,648 | 14,848 | 155.0 | 2.34 | 4.76 / 5.59 | 9.09 |
| archive | directory | p8-19x2 | 2 | 142,344 (142,344) | 5,136 | 22,564 | 182.1 | 3.01 | 5.03 / 5.22 | 8.61 |
| archive | directory | p8-27x1 | 2 | 128,520 (128,520) | 5,136 | 22,564 | 192.1 | 2.96 | 5.14 / 5.70 | 9.23 |
| archive | pages | baseline | 21 | 207,880 (207,880) | 5,648 | 14,848 | 155.0 | 2.34 | 4.77 / 6.01 | 9.13 |
| archive | pages | p8-19x2 | 21 | 142,344 (142,344) | 5,136 | 22,564 | 182.1 | 2.70 | 5.05 / 5.36 | 8.48 |
| archive | pages | p8-27x1 | 21 | 128,520 (128,520) | 5,136 | 22,564 | 192.1 | 2.62 | 5.17 / 6.31 | 9.08 |
| recent | directory | baseline | 25 | 38,920 (38,920) | 5,648 | 14,848 | 35.0 | 0.69 | 0.96 / 1.82 | 1.69 |
| recent | directory | p8-19x2 | 25 | 34,824 (34,824) | 5,136 | 22,564 | 62.1 | 1.33 | 1.46 / 1.62 | 2.54 |
| recent | directory | p8-27x1 | 25 | 21,000 (21,000) | 5,136 | 22,564 | 72.1 | 1.23 | 1.73 / 2.34 | 3.34 |
| recent | pages | baseline | 30 | 72,712 (67,080) | 5,648 | 14,848 | 59.0 | 0.78 | 1.76 / 3.50 | 3.33 |
| recent | pages | p8-19x2 | 30 | 56,328 (52,744) | 5,136 | 22,564 | 86.1 | 1.43 | 2.20 / 2.48 | 3.84 |
| recent | pages | p8-27x1 | 30 | 42,504 (38,920) | 5,136 | 22,564 | 96.1 | 1.49 | 2.50 / 3.14 | 4.77 |

| Tier | Variant | Tables (timed) | Held GiB | Peak-process max GiB | Cold build s | Open-tail rebuild s | Directory pass upload KB |
|---|---|---:|---:|---:|---:|---:|---:|
| archive | baseline | 186 (23) | 28.16 | 0.55 | 435 | 0.00 (0 tables) | 2,495 |
| archive | p8-19x2 | 186 (23) | 33.07 | 0.67 | 506 | 0.00 (0 tables) | 1,708 |
| archive | p8-27x1 | 186 (23) | 34.89 | 0.52 | 491 | 0.00 (0 tables) | 1,542 |
| recent | baseline | 55 (55) | 2.59 | 0.35 | 41 | 5.87 (8 tables) | 973 |
| recent | p8-19x2 | 55 (55) | 4.04 | 0.61 | 76 | 10.90 (8 tables) | 871 |
| recent | p8-27x1 | 55 (55) | 4.57 | 0.43 | 75 | 10.44 (8 tables) | 525 |

| Tier | Table | Key | Query bits | Tables | Min bits | Median bits | Below 50 | Below 78 | Below 128 | Analyze s (median) |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| archive | directory | 19x2 | 28 | 12 | 276 | 306.0 | 0 | 0 | 0 | 13.3 |
| archive | directory | 27x1 | 28 | 12 | 168 | 182.5 | 0 | 0 | 0 | 13.2 |
| archive | pages | 19x2 | 28 | 174 | 182 | 312.0 | 0 | 0 | 0 | 13.6 |
| archive | pages | 27x1 | 28 | 174 | 125 | 185.0 | 0 | 0 | 4 | 13.1 |
| recent | directory | 19x2 | 28 | 25 | 743 | 771 | 0 | 0 | 0 | 8.2 |
| recent | directory | 27x1 | 28 | 25 | 257 | 268 | 0 | 0 | 0 | 7.9 |
| recent | pages | 19x2 | 28 | 30 | 503 | 686.0 | 0 | 0 | 0 | 8.6 |
| recent | pages | 27x1 | 28 | 30 | 238 | 257.5 | 0 | 0 | 0 | 8.3 |
