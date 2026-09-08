# Archive geometry: decision

Date: 2026-09-07. Status: **decided for the archive tier, conditional on one
measurement that has not been taken.** Nothing is published at this geometry and
no wallet has synced from one.

## The decision

**The archive tier uses `archive-wide` — a 32,768-row directory with a
65,536-row page table.** The recent tier stays at `recent-8k`. `archive-32k` is
dominated and should not be deployed.

**The recent tier is open, and this decision does not close it.** An earlier
draft of this document said nothing measured gave a reason to move it. That was
wrong: the median script's cost is exactly twice the directory query, so a
narrower recent directory is worth 17% of what every ordinary wallet pays on
every sync — which is the case the deployment plan's §3.2 makes for 4,096
directory rows over 8,192 pages. That pairing is not in the registry and has
never been scored. See §10.3 of the plan.

## What the decision rests on

Two bodies of evidence, both taken this day, both archived:

- [`fullchain-geometry-comparison.md`](transparent-pir-evaluation/shard-utilisation/fullchain-geometry-comparison.md)
  — three geometries censused over the complete genesis-to-tip journal,
  352,873,356 events, one tool, one anchor.
- [`droplet-c8-measurements.txt`](transparent-pir-evaluation/shard-utilisation/droplet-c8-measurements.txt)
  — saturation, residency and cold-start on a DigitalOcean `c-8`, eight
  dedicated Xeon 8280 cores.

| | `recent-8k` | `archive-32k` | `archive-wide` |
|---|---:|---:|---:|
| Shards | 1,091 | 314 | **162** |
| Resident per shard | 269.4 MiB | 461.4 | 589.4 |
| Fleet, all resident | 287.1 GiB | 141.5 | **93.3** |
| Hosts at 11 GiB usable | 27 | 13 | **9** |
| Monthly, at $84/host | $2,268 | $1,092 | **$756** |
| Pinned plaintext | 64.1 GB | 73.8 | **57.1** |
| Cold start per shard | **7.6 s** | 11.4 | 14.1 |
| Server work per restore | **19.2 ms** | 43.6 | 41.0 |
| Median restore bytes | **0.26 MB** | 0.52 | 0.52 |
| p99 restore bytes | 44.4 MB | 42.3 | **33.5** |

## Why the server cost does not decide it

`archive-wide` does 2.14x the server work per restoration. That sounds
disqualifying and is not, because the archive tier is **RAM-limited, not
bandwidth-limited, at any rate this service will plausibly see.**

Evaluation saturates at **one thread** on a DigitalOcean droplet — eight
threads return 1.00x one thread, with bandwidth pinned near 26 GiB/s — so a
host's serving capacity is fixed by memory bandwidth and is the same however
many cores it has. Nine hosts, which is what `archive-wide` needs to hold its
fleet, will serve **219 full restorations per second** before bandwidth becomes
the binding constraint. `recent-8k` only becomes the cheaper option above about
**660 restorations per second**, which is 57 million restorations a day.

Below that crossover the 2.14x is free: the hosts are already bought, for RAM.
Above it the decision inverts. The crossover is the number to watch, and it is
the reason this decision is stated as conditional on load rather than as a
property of the geometry.

## What is being accepted

- **14.1 seconds of cold start** on an archive shard whose runtime is not
  cached, against 7.6 at the recent geometry. The plan already accepts queued,
  cold archive work; this is what that costs.
- **2.14x the server bandwidth per restoration**, free until the crossover.
- **Double the median restore bytes** — 0.52 MB against 0.26 — for a script
  whose history is in the archive. See the caveat below, which may substantially
  reduce this in the deployed design.

## What is bought

A third of the memory, a third of the hosts, $1,512 a month, the least pinned
plaintext of the three, and a **better p99 restoration** — 33.5 MB against 44.4.
The wide page table is what buys the low shard count, and the low shard count is
what cuts `g` from 6.55 to 2.58 and so cuts the tail.

## Why `archive-32k` is dominated

It costs the median wallet exactly what `archive-wide` costs — both have a
32,768-row directory, and the median script issues two directory queries and
never touches the page table — while doing *more* server work per restore
(43.6 ms against 41.0), holding 48 GiB more, and needing four more hosts. It
wins on nothing.

It is being left in the registry rather than deleted: it is tested, it costs
nothing to keep, and a future change to the page-row width would want it back.
It should not be published. This is a deliberate softening of "drop it" —
deleting working, tested code to express a deployment preference trades a real
thing for a documentation one.

## The condition, stated plainly

**Every figure above is for a *uniform* deployment, and the design is not
uniform.** The proposal is `archive-wide` below a six-month cutoff and
`recent-8k` above it. No row of that table is the cost of that set, because the
mixed set has never been censused.

This matters most for the one metric where `archive-wide` looks worst. In a
two-tier deployment an ordinary wallet syncing recent history queries only
`recent-8k` shards and never touches an archive directory at all — so the
doubled median may be paid by almost nobody, and the objection largely
dissolves. It may also not: a wallet restoring from an old birthday crosses
both tiers. **Nobody knows, and the census that would say is not blocked on
judgement, only on work.**

Two things stand in the way of taking it:

1. **The cutoff has to be derived from chain time.** The plan is explicit that
   it must come from the pinned anchor's chain timestamp rather than a block
   count, because a height standing in for "six months" drifts with the
   interval, and re-deriving it later re-shards the chain. The event journal
   stores no block timestamps, so this needs the node's RPC — the same one
   `shard-publish` already calls once for shard zero's parent hash.
2. **`shard-census` has no two-tier mode.** `shard-publish` grew
   `--archive-geometry` and `--recent-from`; the census did not. It scores one
   geometry over one range, so it cannot currently measure the set the publisher
   would produce. It needs the same forced boundary and policy switch, so that
   what is measured is what is published.

Until that census exists, this decision is sound on everything except the cost
to an ordinary wallet, where it rests on an argument rather than a measurement.

## What would reverse it

- A sustained restoration rate above roughly 660/s, which moves the archive
  tier out of the RAM-limited regime and makes `recent-8k` cheaper.
- Separate droplets turning out **not** to receive independent memory
  bandwidth. Only one host was measured; the plan asks for concurrent
  multi-droplet testing before relying on horizontal scaling, and that is
  still an assumption.
- A mixed-set census showing ordinary wallets pay archive-tier directory costs
  after all.
- Cold-start latency proving unacceptable in the wallet, which no measurement
  here addresses: 14.1 s is server-side runtime construction, not a
  wallet-observed sync time on a real network.

## Operational consequences

- **Derate cache budgets by about 5%.** `reserved_bytes` measures 2–5% *low*
  against Linux RSS across the registry — 128 → 134.72, 224 → 230.69,
  352 → 358.75 MiB. Low is the direction in which a bounded cache exceeds its
  bound.
- **`--query-slots` is not a throughput knob.** At one-thread saturation it
  cannot raise throughput. Leaving it at 2 is harmless and still keeps one slow
  query from blocking another entirely.
- **Buy memory, not cores.** Cores do nothing for evaluation. The cheapest RAM
  per dollar wins, which `s-4vcpu-16gb-amd` at $84 already is.
- **Cold start is 5.7x worse than recorded.** The deploy notes carry 0.67 s per
  segment; a droplet measures 3.81 s. That directly weakens the plan's proposal
  to retain an hour of revision *bytes* with an LRU of built runtimes rather
  than resident runtimes, since rebuilding costs more than assumed.
