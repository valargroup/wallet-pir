# Handoff: build the txid display recent runtimes on the coordinator

Repository: `valargroup/wallet-pir`, base `main` at or after `7ed5c87d`. Commit and push
directly to `main` (rebase onto `origin/main` first; focused check `make check-fast
BASE=<base>`). Read `CLAUDE.md` and `AGENTS.md` first.

## Goal

Every block, the txid display recent worker (`transparent-txid-server --role recent-replica`
on recent-01) rebuilds the two `txid-2k` tables of the new recent revision: encode, hint
and two-mask preprocessing. That costs about 12 CPU-seconds per block. It competes with
the worker's own queries: recent-shard query p50 rises from 29 to 75 ms and p99 from 101
to 306 ms during a rebuild, at 20 lookups/s. It is also most of block-to-serving time:
currently p50 8.5 s and p95 12.4 s.

Move that build to the coordinator, as Enhance does for packing state
(`enhance/ops/deploy/prepared-packing.md`). The controller builds each recent table's
runtime once, writes it in the existing runtime disk-cache format, and ships it with the
candidate. The worker loads it, checks it end to end, and serves it. The worker builds
locally only as a fallback.

Background and measurements:
- `transparent/evidence/txid-display-freshness-2026-10-07/README.md`
- `transparent/docs/status.md` ("Txid display block-to-serving change, 2026-10-07")
- incremental alternative: issue valargroup/wallet-pir#128

## Facts the design relies on (verified in source at 7ed5c87d)

- **Build.** `TableRuntime::build(shared, rows)` is in
  `transparent/services/transparent-shard-server/src/runtime.rs:313`. It runs on
  `build_pool()` (`runtime.rs:276`), sized by `TRANSPARENT_BUILD_THREADS`, nice 10.
- **Disk format.**
  - `runtime/disk.rs` `DiskCache::{save,load,path}`. The identity hashes the runtime
    key (revision digest, table, segment), geometry, source table SHA-256, setup seed,
    scheme and transport (`disk.rs:72-101`).
  - `load` checks length, identity, the body SHA-256, the preprocessing re-read, and
    that masks re-derived from the preprocessing equal the stored masks.
  - It does **not** check that the database matches the preprocessing, or that the
    database equals the source rows.
- **Restore path.** `RuntimeCache::load_or_build` (`runtime.rs:~615`) already prefers
  `self.disk` restore over building. On a build it also *saves* to that disk cache.
  So do not just point the worker's general disk cache at shipped files.
- **Display keys and table kinds.** `display::runtime_key(digest, table, segment)` and
  `display::kind(table)` (`display/mod.rs:44-56`). The worker fetches runtimes in
  `DisplayRuntime::runtime` (`display/service.rs:91`), called by prewarm
  (`service.rs:256`) from `DisplayLive::prepare` (`display/live.rs:363`).
- **Candidate layout.**
  - `write_candidate` (`transparent-filter-server/src/txid_display/publisher.rs:375`)
    makes `candidate-<height>-<hash>-<nanos>/`, containing one hard-linked subdirectory
    per revision digest plus the map file.
  - `DisplaySet::open_reusing` (`display/set.rs:~306`) calls `DisplayRevision::read` on
    **every subdirectory** of the candidate.
  - `verify_dir` (`publisher.rs:~515`) requires a revision directory's files to equal
    exactly the files its manifest names.
  - Therefore shipped runtimes must be **plain files at the candidate root**, never a
    subdirectory and never inside a revision directory.
- **Shipping.**
  - `transparent/ops/scripts/txid-display-fleet.py` `ship` (line ~139) runs
    `rsync -a --delete --link-dest=<previous candidate>` over SSH.
  - Files at the candidate root are carried without changes to the script. New
    runtime files transfer in full every block.
- **Controller cycle.** `controller.rs:~1160-1196`: `publish_parts`, then
  `write_candidate`, then `fleet.activate`. `activate_worker` (`serving.rs:501`) ships
  the candidate, then calls `prepare`, then `activate`. Per-cycle fields are recorded at
  `controller.rs:~1230` (`last_cycle`, written to `timeline.jsonl`).
- **Crates.** `transparent-filter-server` already depends on `transparent-shard-server`
  (`Cargo.toml:85`).
- **Hosts.**
  - Coordinator: Xeon Gold 6548N, 8 vCPU, load about 1.2, 54 GB free. The controller
    unit runs at `CPUQuota=200%`, `CPUWeight=20`, `Nice=10`, `MemoryMax=4G`.
  - recent-01: DO-Regular 2.0 GHz, 4 vCPU. The worker unit has a drop-in,
    `zz-latency.conf`: new binary, `CPUQuota=200%`, `CPUWeight=50`, 2 build threads.
- **Measured build cost** of one `txid-2k` table, on roman-dev-2 at 1 thread:
  preprocessing 2.2 s, hint 0.15 s (batched), encode 0.02 s. On recent-01 the whole
  per-block prepare is about 12 CPU-seconds.

## Design

1. **Shared helper, in `transparent-shard-server`.** For example,
   `display::prebuild::build_shipped(revision_dir, out_dir) -> Result<Vec<Shipped>, String>`.
   - It reads the revision (`DisplayRevision::read` + `verify_tables`).
   - For each `(table, segment)` in `targets()`, it builds
     `SharedParams::build(geometry, kind(table))` and `TableRuntime::build` from the
     segment's verified rows.
   - It writes the file with the `DiskCache` writer, using
     `runtime_key(&digest, table, segment)` and the segment's `sha256`.
   - Directory is `out_dir` (the candidate root), name `DiskCache::path`. Write
     `.partial`, fsync, then rename, as `save` already does. Skip the cache's budget
     and lock logic if it does not fit a one-shot write.
   - It returns per-file name, bytes and build seconds.
2. **Controller.**
   - Behind a new flag `--ship-runtimes`, default off: after `write_candidate`, call the
     helper in `spawn_blocking` for the **recent** revision only, writing into the
     candidate root.
   - Record `prebuild_ms` and `shipped_bytes` in `last_cycle`.
   - If the build fails, log it, ship without runtimes (the worker falls back), and
     count it. Never fail the cycle because of it.
   - Collection already deletes whole candidates. Confirm that shipped files go with
     them on the coordinator and on the worker.
3. **Worker.**
   - In `DisplayRuntime::runtime`, for an **unsealed** revision, look for a shipped file
     under the publication directory (the revision directory's parent) before
     `cache.get` builds.
   - Add a `RuntimeCache` path that loads from a **read-only** `DiskCache` view. It
     must use the same memory reservation and restore-slot accounting as the existing
     restore, and it must **never save** a shipped runtime back to disk.
   - Suggested shape: an optional `shipped: Option<DiskCache>` argument through
     `get` → `load_or_build`, tried before `self.disk` and before a build.
   - Keep `source.verify()`.
4. **End-to-end self-check, on the worker, after every shipped load.**
   - For one random row per runtime, at minimum, run the client path the tests use:
     `profile.prepare(row)`, then `runtime.evaluate(&shared, binding, body)`, then
     `profile.decode(..)`.
   - Compare the decoded row with the segment's plaintext row.
   - On mismatch or error: discard the shipped runtime, warn, increment a metric, and
     build locally.
   - This covers the gap in `DiskCache::load`: a database that doesn't match its
     preprocessing.
   - Report `shipped` (count loaded), `shipped_fallbacks` and `self_check_ms` in the
     prepare reply. The reply is built at `live.rs:~370`; the controller already copies
     reply fields into `workers[]`.
5. **Published bytes must not change.** A shipped runtime must publish byte-identical
   `public_params`, `public_params_sha256` and epoch, and give byte-identical answers,
   compared with a locally built one. Wallets and wire format see no difference.

## Tests (focused; `release-fast`)

- A shipped runtime loaded on the worker equals a locally built one: same published
  masks and epoch, byte-identical answers to fixed queries, correct decoded rows. Model
  it on `the_batched_hint_runtime_is_the_reference_runtime` (`runtime.rs`). Cover
  `txid-2k` directory (full) and pages (partly filled).
- These shipped files are ignored and trigger a local build, with a counted fallback:
  - truncated or corrupted file;
  - file for another revision or table (identity mismatch);
  - missing file.
- The self-check rejects a file whose database came from rows A and preprocessing from
  rows B. Build it by hand, save it under A's identity, then load and check.
- A candidate containing root-level runtime files still opens with
  `DisplaySet::open_reusing`, passes `verify_dir` per revision, and is removed whole by
  collection.
- The controller with `--ship-runtimes` writes the files and records `prebuild_ms`. A
  helper failure still publishes.
- Run the ops tests that touch the fleet script and txid deploy:
  `python3 -B -m pytest transparent/ops/tests/test_txid_display_fleet.py transparent/ops/tests/test_txid_display_poc.py`,
  or as `make check-fast` selects.

## Benchmarks before deploying (roman-dev-2)

- Measure per-file bytes, coordinator-class build time at 2 threads, worker load plus
  self-check time, and rsync time for the per-block delta. Ideally measure load time
  with `taskset`/CPU limits that mimic recent-01.
- Record the numbers in the evidence note.
- **Estimates to confirm:**
  - about 40–72 MiB per table: 8 MiB database plus 32–64 MiB preprocessing;
  - about 80–140 MiB shipped per block;
  - under 1 CPU-second of worker load plus check per block.
- If one block's runtimes exceed about 150 MiB, or rsync takes more than 2 s on the
  private network, stop and report before deploying.

## Built and benched, 2026-10-07

Implemented through the benchmarks; the deploy below has not started.
[Bench evidence](../evidence/txid-display-shipped-runtimes-bench-2026-10-07/README.md),
[design as built](txid-display.md#shipped-recent-runtimes). Where source or
measurement differed from this plan:
- The controller copied only `built`, `reused` and `seconds` from a prepare
  reply into `workers[]`, not every field; `shipped`, `shipped_fallbacks` and
  `self_check_ms` are now copied explicitly.
- Shipped files are written without fsync: fsyncing added about 0.3 s per
  file to every block, and a lost file is only a counted fallback build.
- The shipped load reads the segment with `source.load()`, which verifies it
  as `source.verify()` does and supplies the self-check's rows. The self-check
  also compares the whole encoded database with the segment.
- A publication counts as shipped only when its directory holds a `.runtime`
  file, so a controller without `--ship-runtimes` causes no fallbacks.
- `pytest` is not installed on roman-dev-2; the ops tests run with
  `python3 -B -m unittest` (as `make` runs them).
- On equal CPUs the cycle got slower (p50 4.2 → 6.4 s): the prebuild runs on
  the critical path. Block to serving improves only if the coordinator builds
  faster than recent-01 does under history load. Set
  `TRANSPARENT_BUILD_THREADS` on the controller unit deliberately: by default
  the build pool takes half the host's cores.

## Deploy (production; from Roman's Mac over SSH, not from the hub)

This is a gate: production deploy. Roman approved this direction on 2026-10-07; confirm
before starting.

- **Access.**
  - SSH config pattern: inventory `~/.config/wallet-pir-deploy/inventory.json`, user
    root, jump host `coordinator` (167.99.42.60), `recent-01` = 10.142.0.10, pinned
    `known_hosts` in `~/.config/wallet-pir-deploy/known_hosts`.
  - Hold `/run/lock/wallet-pir-production.lock` on the coordinator throughout
    (`flock -n`).
- **Coordinate with the 20 QPS measurement session.** Its state is in
  `~/.config/wallet-pir-deploy/txid-display/measure-20qps-20261007/`: `events.jsonl`,
  `TRIPPED`. Do not restart anything inside one of its load windows.
- **Build** release binaries with the CI flags:
  - `RUSTFLAGS="-C target-cpu=x86-64-v3 -C target-feature=+pclmulqdq" cargo build --locked --release -p transparent-shard-server --bin transparent-txid-server -p transparent-filter-server --bin txid-display-controller`
  - Check the minimum glibc version against the hosts (2.39).
- **Order:**
  1. **Worker first.** Its behavior is unchanged when no shipped files are present.
     - Install under `/opt/transparent-txid-display/releases/<sha>-ship/`.
     - Edit the `ExecStart` in recent-01's
       `/etc/systemd/system/transparent-txid-display-worker.service.d/zz-latency.conf`.
       Keep the other lines; harness `deploy-recent.sh` in the evidence directory
       shows the pattern.
     - Restart right after an activation.
     - Check the worker is warm and the next cycle succeeds.
  2. **Controller.**
     - **Before restarting it, read how `txid-display-controller run --mode
       replay-then-live` resumes from `/srv/zakura/txid-display-poc/root` (active
       record, `timeline.jsonl`).** Confirm a restart resumes live, without
       re-bootstrapping or re-sealing.
     - Add a drop-in `zz-ship.conf` on the coordinator for
       `transparent-txid-display-controller.service`: new binary plus `--ship-runtimes`.
       Keep every other argument from the current `ExecStart`.
     - Restart between cycles.
- **Rollback:** remove `zz-ship.conf` and restart the controller. The worker simply
  builds again. To also roll back the worker, put the previous release
  `07f906753cb0-latency` back into `zz-latency.conf`.

## Acceptance (measure; targets are estimates, report actuals either way)

- **Block to serving over at least 300 live blocks**, using `harness/analyze.py` from
  the 2026-10-07 evidence, adapted: p50 ≤ 6 s, p95 ≤ 8 s, none over 20 s. Report
  `prebuild_ms`, `ship_ms`, `prepare_ms` and `self_check_ms` separately.
- **Worker CPU per block** ≤ 1.5 CPU-seconds, from cgroup `cpu.stat` sampling as in
  that evidence.
- **Query interference.** At 20 lookups/s (`txid-rate`, same fixture and arguments as
  the measurement session's window B), recent-shard query p99 during load windows is
  within 20% of p99 outside them. Correlate client timestamps with prepare intervals
  as in the evidence. Every lookup must be exact.
- **Steady state:** 0 self-check failures, 0 fallbacks.
- **Collateral:**
  - history prewarm on recent-01 unchanged within noise;
  - coordinator CPU per block reported;
  - history p99 alerts unchanged;
  - archive worker untouched.
- **Records.**
  - Retain an evidence directory `transparent/evidence/txid-display-shipped-runtimes-<date>/`:
    manifest, README, raw inputs, `SHA256SUMS`.
  - Add a dated entry to `transparent/docs/status.md`.
  - Update `transparent/docs/txid-display.md` (tiered proof-of-concept section) to
    describe the shipped runtime and its trust model: the coordinator is trusted, as
    in Enhance; checksums detect corruption only; the self-check proves the database
    matches the preprocessing for sampled rows.
  - Comment on #128 that incremental update is now a coordinator CPU saving, not a
    latency fix.

## Not in scope

- Shipping archive (sealed) runtimes. They are built once per seal in the background
  `stage`.
- An HTTP streaming endpoint, or memory-mapped loading.
- Incremental per-block update (#128).
- New hosts, router or route changes, client or wire-format changes.
- Changes to the history worker or its units.
- Reverting the recent worker's 2-CPU drop-in; leave it so fallback builds stay fast.
