# Transaction-detail restart reuse, 2026-10-26

A restarted transaction-detail (txid display) publisher used to hash every
table, check every row, and load and self-check every runtime before it could
serve again. With restart reuse it adopts the files its saved preparation
record vouches for, serves at once, and checks everything again on one
low-priority thread afterwards
([design](../../../docs/txid/txid-display.md#prepared-material-and-startup)).
This note records the before/after restart cost on a synthetic,
production-shaped fixture. Nothing here was deployed.

## Result

Three cold-cache pairs against the parent commit, alternating order. Seconds
are median (min–max).

| Measure | Before (parent) | After (this change) |
|---|---|---|
| Restart preparation, pass 0 | 14.16 s (13.76–14.63) | 0.022 s (0.022–0.022) |
| Adopted saved checks | — | 3 of 3 restarts |
| Inventory digest | `91034e36…` on every run | `91034e36…` on every run |
| Listed files and bytes | 49, 3,224,074,948 | 49, 3,224,074,948 |
| Background full check, one thread, after serving starts | — | 21.80 s (21.39–21.81) |
| Peak memory | 210.8 MiB | 212.5 MiB |
| First run with no saved record | — | 13.95 s, full check, writes the record |

The earlier baseline `e52f43d8` gave the same picture: 14.12 s (13.75–14.52)
before, 0.021 s (0.021–0.023) after, background check 21.54 s.

The background check takes longer than the old foreground check because it
runs on one low-priority thread rather than the four-thread build pool, so it
does not compete with preparation and serving. Its time grows with the
publication; at production's ~95 tables, roughly six times this fixture, it
would take about two minutes, during which a corrupted file would still be
served. A corrupted file is withdrawn when the check reaches it.

## Source and host

- Change: branch `roman/mac/claude/detail-restart-reuse`, source tree `src` = `1d35ed1c7ce2`
  (binary SHA-256 prefix `14a97def5c831e8c`). The committed code differs from
  that tree only by two no-op error conversions removed for clippy.
- Baseline: its parent, `f4997e681` (`src` = `0f186f2c6f40`; shared preparation record, History only),
  binary `61b65ace5b91351d`. An earlier baseline, `e52f43d8` (`f5c365f23b4d8a8d`),
  ran the same way and is in `raw/ab-e52f43d8.jsonl`; its bench output predates
  the inventory digest, so it compares only file count and bytes.
- Build: `cargo build --profile release-fast -p pir-publisher --features
  transparent-detail`, rustc 1.89.0, `CARGO_INCREMENTAL=0`.
- Host: `roman-ipir-bench-8vcpu`, 8 vCPU, 31 GB, Linux 6.8.0-124, no SHA
  instructions (about 330 MB/s for `sha256sum`). Nothing else ran on it.

## Fixture

`pir-publisher detail synth-journal --start 1 --blocks 20000 --mean-records 200
--seed 7`, then `detail bootstrap --start 4729 --through 19194 --geometry
txid-4k8 --archive-target 180000 --recent-window 180000`. The root holds 16
`txid-4k8` tables (15 sealed archives and one recent), 16 shipped runtimes and
metadata: 49 listed files, 3,224,074,948 bytes. Production holds about 95
tables, so its full check is roughly six times larger.

## Method

`ab.sh` (beside this note) drops the page cache before every run, then runs
`pir-publisher detail bench-restore --root root --passes 2`. That command takes
the root's controller lock, restores the active publication with a fresh
process, and prepares it twice; pass 0 is the restart. The change's version
then saves its record and, if pass 0 adopted saved checks, times the full
background check in the same process. The first run of the change starts with
no record, checks everything and writes one. Three pairs follow, alternating
which binary runs first.

## Failed attempt

The first baseline rebuild produced a binary byte-identical to the change's.
Both sources were unpacked from `git archive`, which stamps files with the
commit time; with a shared build directory, cargo saw the parent's older
files as already built. That run is discarded. The retained parent run was
built after touching every source file, and its binary differs.

## Limits

- Synthetic data at a sixth of production's table count; the production
  restart was not measured. Readiness after adoption does not grow with table
  count much; the background check grows roughly linearly.
- `bench-restore` measures preparation only, not the chain checks and
  activation that also precede serving after a real restart.
- The background check here runs alone. In service it pauses while a
  preparation runs, so it can take longer.
