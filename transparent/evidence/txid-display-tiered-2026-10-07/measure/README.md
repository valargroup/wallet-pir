# Tiered txid display POC: live 20 QPS load and bandwidth, 2026-10-07

Raw outputs only; not evidence yet. The session ended on a hard stop during window A
(history 5 QPS errors = 1 at 14:41:30 UTC). Window B (20 lookups/s) and the bandwidth
measurement were **not run**.

## What ran (all times UTC, 2026-10-07)

| Step | Time | Result |
|---|---|---|
| Supervisor and baseline sampling start | 13:53:25 (restarted 13:53:52 for a CPU parse fix and 14:20:59 for SSH reuse) | |
| V0: validation, `--unit lookup`, 5/s total, 4 processes, 60 s | 14:04:15–14:05:15 | 304/304 exact, 0 errors |
| Trip 1 (no load running) | 14:11:55 | history exact 225: history client self-paused 14:11:32–14:12:02, "publication more than two blocks behind node" |
| Trip 2 (no load running) | 14:18:39 | two 12 s SSH timeouts from this Mac (RTT ~270 ms); coordinator healthy. Fixed with ControlMaster reuse and a 25 s timeout |
| A: `--unit query`, 20/s total, 4 processes, 600 s planned | 14:32:33–14:41:33 (540 s) | 10,800/10,800 exact; **hard stop** |
| Trip 3 (during A) | 14:41:30 sample, load killed 14:41:33 | history errors = 1: router-01 Caddy reloaded twice at 14:41:18 (SSH from 10.142.0.3, routine; 84 reloads today); a history pages query to shard 46 got "Connection reset by peer" at 14:41:18.68 and was retried (recovered 6 -> 7) |

## Tools and hosts

- Source: valargroup/wallet-pir d191f86b2873e4250cee3ae6952f830c0d1e7a55 (`git archive`), built on
  roman-ipir-bench-8vcpu (209.38.33.233, 8 vCPU) with `build/build.sh`:
  `cargo build --locked --release -p transparent-shard-server --example txid-rate --example txid-bandwidth`
  (repo `.cargo/config.toml`: target-cpu=native). Hashes in `build/binaries.sha256`.
- Fixtures: generated read-only on the coordinator with the deployed `txid-inventory`
  (sha256 e05ade09…), output to stdout, `nice -n 19 ionice -c3`:
  - `txid-inventory fixture --publication /srv/zakura/txid-display-poc/root --heights .../tooling/heights.bin --natural 2000 --absent 200 --seed 20261007 --out /dev/stdout`
    -> `fixtures/fixture-natural.json` (4,000 samples: recent 1,955 inline + 45 pages-1; archive 1,921 inline + 79 pages-1; 200 absent).
    `fixture-natural-p{0..3}.json` are the same samples rotated by a quarter per process.
  - `... --per-class 5 --absent 6 ...` -> `fixtures/fixture-perclass.json` (63 samples, for bandwidth; unused).
- Load: `build/run-window.sh NAME UNIT TOTAL_RATE PROCS SECONDS [args]` runs PROCS copies of
  `txid-rate --url https://transparent-pir.valargroup.dev --fixture fixture-natural-p$i.json --rate TOTAL/PROCS --seconds S --permit permit --unit UNIT --mix recent=0.8,archive=0.2 [args]`.
  - V0: `run-window.sh V0-validate-5lps lookup 5 4 60 --cold-every 25` (absent every 20, the default).
  - A: `run-window.sh A-20qps-query query 20 4 600 --cold-every 0`.
- Supervisor: `tools/supervise.py` on this Mac. Every ~15 s it reads, read-only, the history load
  `status.json`, `curl 127.0.0.1:8094/v1/status`, `curl 127.0.0.1:8099/v1/status` and checks for `latched.json` on the
  coordinator; every ~30 s it reads recent-01 `/proc/stat`, loadavg and `CPUUsageNSec` of
  `transparent-txid-display-worker` and `transparent-shard-server`. It applies the brief's stop rules
  (plus a fail-safe: two unreadable samples), refreshes the bench permit (txid-rate pauses when the permit is older than 45 s),
  and on a trip writes deny, kills the txid tools on the bench and latches `TRIPPED`.
- Controller cycles: `tail -n 4000` of the publication `timeline.jsonl` (read-only), `timeline/<window>.jsonl`.
- Analysis: `tools/analyze.py OUT NAME 600` -> `runs/<name>/summary.json`.

## Files

- `history-samples.jsonl`, `recent-cpu.jsonl`, `supervisor.log`, `events.jsonl`, `TRIPPED-*.json`
- `runs/<window>/rate-*.jsonl` (txid-rate JSONL), `window.json`, `summary.json`
- `timeline/<window>.jsonl`, `router-caddy-reload-1441.log`, `history-error-1441.jsonl`
- `fixtures/`, `build/`, `tools/`

## Caveats

- Window A ran 540 s, not 600 s.
- History `trailing_60s` lags by up to 60 s: "during" excludes the first 60 s of a window; "incl tail" adds 60 s after it.
- Only 4 controller cycles fell inside window A (cycles follow blocks), so the rebuild comparison is small.
- No production configuration, unit, file or route was changed. Production commands were read-only:
  status reads, the fixture to stdout, timeline tail, and journal/log reads on router-01 and the coordinator.
