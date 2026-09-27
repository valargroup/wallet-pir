# Sync latency under emulated round-trip delay, 2026-09-28

Wallet sync time with and without directory choice tables, and with concurrent
filter prefetch, when every request pays a network round trip. Architecture
update §2 and §6.

| Field | Value |
|---|---|
| Host | `roman-ipir-bench-8vcpu`; server and one client on loopback |
| Delay | `tc qdisc add dev lo root netem delay {25,50}ms`, so a round trip adds 50 or 100 ms; removed after each run |
| Sets | The `off` and `all` publications of the [local measurement](../single-lookup-measure-2026-09-27/README.md) (recent slice, recent-8k) |
| Server | `transparent-shard-server` from `9d47b05b` (x86-64-v3), whole set, runtime cache, warmed before delay was applied |
| Client | `transparent-loadtest` from `9d47b05b`, then from the prefetch commit for the prefetch run. Derived recent sample, classes restore-6m, multi-script and catch-up-1d/7d/30d, one wallet at a time, 10 syncs per class |
| Procedure | `run.sh` for off/all at 25 and 50 ms; the prefetch run repeated the all-50 procedure with the prefetch client |
| Output | `report-*.json`, `loadtest-*.log` |

## Results

All 250 syncs were exact.

**p50 sync seconds at a 100 ms round trip:**

| Class | Without tables | With tables | With tables + filter prefetch |
|---|---:|---:|---:|
| restore-6m | 4.40 | 3.62 (−18%) | **2.51** (−43% overall) |
| multi-script | 20.67 | 13.94 (−33%) | 12.83 |
| catch-up-30d | 1.97 | 1.71 | 1.41 |
| catch-up-7d | 1.77 | 1.51 | 1.41 |
| catch-up-1d | 2.26 | 1.87 | 1.77 |

At a 50 ms round trip, tables cut restore-6m from 2.35 to 1.92 s (−18%) and
multi-script from 12.1 to 8.0 s (−34%).

**Requests per restore-6m sync:**

| Stage | Without tables | With tables |
|---|---:|---:|
| Filters | 14 | 14 |
| Manifests | 4.8 | 4.8 |
| Directory setups | 4.8 | 4.8 |
| Directory queries | 11.6 | 5.8 |
| Page setups and queries | 1.0 | 1.0 |
| **Total** | 36.2 | 30.4 |

Every request was sequential before prefetch, so sync time is close to
requests × RTT plus about 0.6 s of work. Prefetch overlaps the 14 filter
downloads, which the wallet makes anyway; filter bytes per sync are unchanged
(1,293,369 for restore-6m).

## Limits

- Delay is emulated on loopback, with no bandwidth limit, loss or TLS. It is not a
  mobile measurement.
- 10 syncs per class and delay.

## Next

About 16 sequential requests remain per restore-6m sync: manifests, setups,
directory queries and pages for about 5 matched shards. Issuing those
concurrently across matched shards is the next latency step. The request count
does not change, so neither does the privacy boundary. It needs a restructured
per-shard walk, not a transport-level change.
