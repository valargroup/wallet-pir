# Bounded key reuse, measured

Date: 2026-09-05. Status: research measurement against a live service. Loopback
only, no wallet enabled. Depends on an unmerged revision of `ipir-sp`
(valargroup/ipir-sp#16), so this configuration is not shippable as it stands.

Packing keys are 86,016 bytes, 80.8% of a 106,504-byte query; the query body is
only 20,480. Four-way reuse shares one set of keys across four queries, at the
cost of the server publishing four public matrix sets per table instead of one.
This measures that trade on the same mainnet-day generation and workloads as
[the real-transport measurement](transparent_pir_http.md).

## Result

Per-query upload falls from 106,504 to **41,988 bytes**, a 2.54x reduction,
matching what the key share predicts to within four bytes.

| Workload | Queries | Fresh | Reuse | Reuse/fresh | vs ordinary |
|---|---:|---:|---:|---:|---:|
| unused_100 | 0 | 174,466 | 174,466 | 1.00x | 0.06x |
| sparse_1 | 4 | 400,798 | 821,684 | 2.05x | 0.28x |
| median_10 | 12 | 1,405,558 | 1,198,584 | 0.85x | 0.42x |
| large_1 | 56 | 7,006,798 | 4,336,494 | 0.62x | 1.50x |

Ordinary retrieval for the same interval is 2,888,097 bytes. Every run was
checked against the independent ledger oracle before its bytes were recorded:
0, 2, 20 and 9,152 events recovered exactly.

## Reuse is worst where the budget is tightest

The plan this work came from assumed key reuse was required because the
288-block interval needed roughly 50 KB per query. That has it backwards.

Reuse front-loads **458,768 bytes** of published parameters, downloaded once per
session: 76,464 for the directory table's four sets and 382,304 for the pages
table's four. A sparse wallet issuing one real query pays all of it and gets one
key upload's worth of saving back, so it spends 2.05x what the fresh
configuration spends. The 288-block retrieval budget is 417,487 bytes; the
published parameters alone exceed it.

Reuse repays only when a sync issues enough queries to amortise those sets.
Comparing the key saving of `(n - ceil(n/4)) * 86,016` against the extra
published bytes puts the break-even at 2 queries for the directory table and 8
for the pages table, and the measured workloads agree: the median profile at 12
queries is 15% cheaper, the heavy profile at 56 queries is 38% cheaper.

Batching also rounds a sync up to a whole number of batches. The sparse profile
issues 4 queries where the fresh configuration issued 1, and those three padding
queries are real cost. That is not waste to be optimised away: a batch that
shrank to fit the number of matches would publish how many blocks matched.

## What follows

Fresh queries remain the right default for sparse wallets, which is the profile
this design targets. Reuse belongs behind a per-table policy that enables it
only once a sync's query count clears that table's break-even, which is what the
earlier reuse experiment's "auto" mode did. A global switch makes the common
case worse.

## Limitations

- Depends on valargroup/ipir-sp#16, which is unmerged and gated behind
  `experimental-key-reuse`. Its security argument is conditional, not a
  reduction, and needs specialist review before any production use.
- Loopback on one host. Not TLS, not a wide-area network, not a mobile device.
- One bounded-range generation over 1,152 blocks, not lifetime history.
- Single runs. Byte counts are deterministic; wall times are not a timing claim.
- Workload scripts are drawn from one mainnet day by fixed rank. They are not
  wallet-population percentiles.
- Trusted indexer, unchanged.

## Accounting corrections made during this measurement

Recorded because each produced plausible numbers rather than an error.

- The fresh baseline this compares against was itself corrected: its
  largest-history total double-charged the published parameters, one session per
  client process. See the correction in
  [the real-transport measurement](transparent_pir_http.md).
- `public_params` became a list of sets, and the client charged `.len()` of that
  list: 4 rather than 458,768 bytes. Setup is exactly what reuse trades against,
  so omitting it flattered reuse. The harness now recomputes the expected charge
  from the session it fetched itself and refuses to run on a mismatch.
- Setup was charged once per batch call rather than once per sync, because each
  call runs a fresh client process. A wallet holds one session and caches
  published parameters across it.
