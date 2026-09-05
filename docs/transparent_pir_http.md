# Transparent-history retrieval over real transport

Date: 2026-09-05. Status: research service, loopback only, no wallet enabled.

Every previous transparent-history figure came from in-process round trips and
excluded HTTP framing, retries and TLS. This measures the same workloads against
a live service over HTTP, so the comparison against ordinary retrieval charges
transport rather than assuming it away.

## What runs

- `pir/transparent-history` — protocol types and the client.
- `server/transparent-history-server` — serves the `directory` and `pages`
  tables of one generation built by `tools/transparent_pir_incremental.py`.
- `tools/transparent_pir_http_transport.py` — the harness's existing
  `fetch(folder, manifest, table, rows)` seam, over HTTP.
- `tools/transparent_pir_http_run.py` — the workloads below.

Table construction stays in the harness. The service serves generations rather
than building them, so these numbers come from the same generations the earlier
evidence was produced from, and no second implementation can disagree with the
first.

## Result

Generation over the resolved mainnet day, heights 3,470,268–3,471,419 (1,152
blocks): directory 4,096 × 3,584 B, pages 4,096 × 17,920 B, two inline events,
186 events per page. Ordinary retrieval for the same interval is 2,888,097 bytes
(`reuse4/baseline.json`).

| Workload | Queries | Public | Setup | Upload | Download | Total | vs ordinary |
|---|---:|---:|---:|---:|---:|---:|---:|
| 100 unchanged scripts | 0 | 174,466 | 0 | 0 | 0 | 174,466 | 0.06× |
| One script, 2 events | 1 | 174,466 | 114,692 | 106,504 | 5,136 | 400,798 | 0.14× |
| Ten median histories | 10 | 174,466 | 114,692 | 1,065,040 | 51,360 | 1,405,558 | 0.49× |
| Largest history, 9,152 events | 51 | 174,466 | 229,384 | 5,431,704 | 1,285,936 | 7,121,490 | 2.47× |

Every run was checked against the independent ledger oracle before its bytes
were recorded: 0, 2, 20 and 9,152 events recovered exactly, with matching
unspent sets. A byte count for a sync that recovered the wrong history is not a
result.

## What this changes, and what it does not

**Transport is not where the cost is.** Real HTTP totals run 1–3% above the
in-process figures for the same workloads (174,379 → 174,466; 6,976,811 →
7,121,490). Framing is negligible against a 106,504-byte query. The earlier
evidence's byte figures were substantially right, and the shape of the
conclusion is unchanged: sparse wallets win, heavy histories lose.

**Per-query cost is the binding constraint.** A directory query is 106,504 bytes
up and 5,136 down; a pages query is 106,504 up and 25,616 down. Upload
dominates, and it is a property of the geometry rather than of the transport.

That matters against the interval budgets. Subtracting BIP 158 filter delivery
from ordinary retrieval for the same interval leaves what retrieval may spend:

| Interval | Ordinary | Filters | Retrieval budget | Queries it buys |
|---|---:|---:|---:|---:|
| 12 blocks | 9,492 | 1,077 | 8,415 | 0 |
| 288 blocks | 446,142 | 28,655 | 417,487 | ~3, after session setup |
| 1,152 blocks | 2,888,097 | 138,716 | 2,749,381 | ~24 |

This build issues fresh queries with no key reuse, and the session's published
parameters cost 114,692 bytes before any query. At the 288-block interval a
sparse wallet fits, but only just, and the fixed query padding the privacy
requirement demands consumes that margin quickly: padding beyond about two
queries per sync exceeds the budget.

**Key reuse is therefore a requirement, not an optimization.** Four-way reuse
measured about 49 KB of upload per query where this measures 106 KB. Whether the
288-block interval holds under realistic padding depends on it, and that is now
a measured claim rather than an assumption.

## Limitations

- Loopback on one host. Not TLS, not a wide-area network, not a mobile device.
  Latency here says nothing about wallet sync time.
- One bounded-range generation, not lifetime history. Table build takes 7.4
  seconds and the process holds both tables resident.
- No key reuse in this build, so per-query cost is the fresh-query cost.
- Single runs, no repetition; the byte counts are deterministic but the wall
  times are not a timing claim.
- Workload scripts are drawn from one mainnet day by fixed rank, not sampled
  from a wallet population. They are not percentiles.
- Trusted indexer, unchanged. PIR hides which row is selected; it does not
  establish that the database is complete.
