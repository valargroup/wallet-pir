# Transparent PIR regression and conformance tests

The production wallet accepts an explicit `Anchor { height, hash }` on every
sync. The wallet's accepted checkpoint and the service's publication tip are
separate authorities. The deployed regression runner exercises this distinction
through the reference HTTP adapters and SQLite persistence.

## Running the deployed suite

Run `make transparent-regression` against the default public origins, or set
`TRANSPARENT_LOAD_URL`, `TRANSPARENT_REGRESSION_FILTER_URL`, and
`TRANSPARENT_REGRESSION_FIXTURE`. Set `TRANSPARENT_REGRESSION_OUT` to a new
directory for each run. The manually dispatched
[workflow](../../.github/workflows/transparent-regression.yml) provides the same
post-deployment release check. It does not deploy or restart anything.

Both origins must serve the fixture's canonical compact map bytes and revision identities. The source publication file has a separate checksum because it may use pretty JSON.
Publication drift, unavailability, a deadline, any incomplete sync, or an
unexecuted required case fails the run. Fixtures never update themselves to
match a service response. Run after deployment reaches a stable publication;
ordinary PR CI uses the local deterministic tests instead.

Each case is one synthetic grouping of public scripts, not an actual wallet or
a claim about wallet population frequencies. Its sequence of exact block
heights includes checkpoints within shards. The runner syncs, closes SQLite,
reopens it and compares the saved state at every checkpoint. It then repeats
the final checkpoint and separately restores it from an empty store.

`report.json` records case outcomes, exact anchors, stage counts, payload bytes,
and elapsed time; `junit.xml` supports CI reporting. A mismatch writes both
expected and actual ledger snapshots. SQLite databases and worker logs remain
beside the report. Transport/TLS overhead and server capacity are not inferred
from payload counters. Cases run sequentially with a 60-second request timeout
and a 30-minute process deadline, configurable through the runner CLI.

## Fixture generation and oracle

`regression-export` reads the journal without changing it. It pins committed
file lengths, verifies block extents and event heights, and checks the
publication's block hashes against journal headers. It has no ingest, node or
RocksDB dependency. A case specification is a JSON list:

```json
[{
  "id": "example",
  "profile": "small-active",
  "scripts": ["76a914000000000000000000000000000000000000000088ac"],
  "required_from": 0,
  "heights": [100, 150]
}]
```

Use actual selected public scripts and heights in the pinned publication:

```sh
cargo run --release -p transparent-regression --bin regression-export -- \
  --data-dir /path/to/transparent-event-data \
  --map /path/to/publication/shards.json --cases cases.json \
  --cutoff-height CUT_OFF_HEIGHT --source-sha SOURCE_COMMIT --out fixture.json
```

The exporter refuses an existing output file, unsupported scripts, missing
checkpoint hashes, invalid height order, birthdays omitting earlier activity,
and oversized selections. Review and freeze the fixture and its checksum
before using it as a release gate. Keep the case specifications alongside it
so a future publication can receive an explicitly reviewed replacement.

Expectations contain exact script-associated events, UTXOs, spends, transaction
summaries, and confirmed balances. The reference reducer uses independent
outpoint bookkeeping, not the wallet's ledger implementation. Both the oracle
and publication share journal/ingest provenance. This detects publication,
retrieval and wallet regressions; independent archive-node extraction remains
necessary for ingest conformance. Fixed-script birthdays do not establish an
address derivation or gap-limit policy, and coinbase recovery does not establish
spendability.

## Re-cutting the fixture for a new publication

The runner compares the served map byte for byte with `map_sha256` and refuses
before any private query, so a fixture frozen against one publication cannot
gate a later one. Continuous publication moves the anchor and, because
`shard-cutoff` derives the cutoff from the anchor header's time rather than a
fixed height, it moves the tier cutoff too.

A re-cut is therefore not a substitution of the terminal height. Each checkpoint
keeps, or follows, whichever boundary it was chosen against: heights taken from a
script's actual history stay put; tier-boundary probes at C-1, C and C+1 follow
the new cutoff; the terminal sync and `recent-birthday`'s A-1 follow the new
anchor. `ops/scripts/recut-regression-cases.py` records that classification and
proves it with `--self-check`, which regenerates the previous specification from
the previous anchor and cutoff and refuses unless it matches the frozen file.

```sh
python3 ops/scripts/recut-regression-cases.py \
  --previous-cases server/transparent-regression/fixtures/mainnet-cases.json \
  --self-check --cutoff-json /path/to/publication/cutoff.json \
  --out cases-next.json
```

It samples `--tail-checkpoints` heights from the span published since the last
cut, so the gate covers revisions continuous publication produced rather than one
static full-chain publication. It writes a case specification only: it runs no
export, reads no journal and contacts no service.

Then export against the accepted publication and compare the result with the
fixture it replaces before freezing:

```sh
python3 ops/scripts/compare-regression-fixtures.py \
  --previous server/transparent-regression/fixtures/mainnet.json \
  --next fixture-next.json --out compare.json
```

It separates two findings. A checkpoint present at the same height in both
fixtures must reduce to the same events, UTXOs, spends, history and balance,
because both derive from a read-only replay of the same journal; a difference
means sealed history was rewritten or ingest changed what it records, and it
exits non-zero. Separately it reports cases whose character changed -- a profile
whose meaning no longer holds, a script set that moved, an anchor state that
moved by more than the tolerance. Those are decisions to make, not failures.

Three further conditions only a journal replay settles, which the re-cut tool
prints before the run:
`recent-birthday`'s birthday moves with the cutoff and the export refuses if any
of its scripts were active in the span it moved across; the unused cases must
still have no activity at the new anchor; and the coinbase checkpoints stay at
their real heights, so that case stops probing a near-tip coinbase.

Run `regression-export` from a committed revision. The 2026-09-08 fixture was
cut with an uncommitted working-tree build and says so; a release gate should not
repeat that.

## Local conformance checks

The [accepted-anchor HTTP tests](../../server/transparent-shard-server/tests/wallet_anchor.rs)
cover arbitrary endpoints, overlap, restart, bounded pagination, changed targets,
future-only discovery, and rollback within a shard. The existing wallet HTTP
suites retain geometry, segment, revision, malformed-response and overload
coverage. The [store tests](../../pir/transparent-wallet-store/tests/store_semantics.rs)
exercise memory/SQLite parity and legacy progress migration. The regression
crate tests the oracle independently.

```sh
cargo test -p transparent-wallet -p transparent-wallet-store --all-features
cargo test -p transparent-shard-server --test wallet_anchor \
  --test wallet_continuation --test wallet_sync
cargo test -p transparent-regression
```

The accepted-anchor path validates complete retrieved histories for a shard,
but persists ledger events only through the requested height. Pagination
validation counts include discarded future events and do not derive from the
ledger. Pending work belongs to a target and revision; changing the target
restarts unfinished retrieval. Coverage carries its accepted endpoint separately
from its full publication endpoint. Explicit rollback takes an accepted ancestor
hash as well as a height; it never puts a shard-end hash on a clipped range.

## Concurrent recovery simulation

The [scenario runner](../../server/transparent-loadtest/README.md) runs saved
20-user mixed recovery waves and sustained concurrent load through the reference
HTTP adapters, with independent SQLite stores, hard per-recovery deadlines, exact
event verification, and saved HTML/JSON reports with worker APM and per-wallet
upload/download totals. Windowed recoveries seed prior ledger events from a
separately validated preparation phase, excluded from measured traffic and latency.
Client filter/setup caches start cold; preparation can warm server caches. This
complements the accepted-anchor regression suite. Local fixture tests are not
full-chain deployed capacity evidence.

## Isolated publication burst comparison

Use `TRANSPARENT_BURST_BUILD_SLOTS=1` for a focused one-slot construction
comparison; the default remains `1 2`. Duplicate configurations are rejected.
The run manifest records Cargo manifests and the lockfile as well as worker
sources and the executable hash, so dependency changes are part of its identity.

Run `make transparent-burst` to compare one and two runtime build slots in separate
release-test processes. By default it runs two repetitions in alternating order;
set `TRANSPARENT_BURST_REPETITIONS` or `TRANSPARENT_BURST_OUT` to override them.
The command prints the report directory. Read `summary.json` first, then the
per-run JSON for exact queries, queue/preparation/activation timings and memory
samples. Every run keeps its log, including failures. Existing reports are never
overwritten. The command exits unsuccessfully if correctness or the worker-stage
30-second budget fails, after attempting both configurations.

The fixture uses real `recent-8k` tables, two HTTP query clients checking occupied
rows against hashed plaintext, and two candidate arrivals one second apart.
Candidates are prepared and activated sequentially; the second candidate's
latency includes its time waiting behind the first. The service binds only to
loopback and its artifacts and activation file live in a temporary directory.
It cannot update a deployed worker or mainnet authority.

The default is a **single-shard worker-stage experiment**, not fleet acceptance. Artifact
construction is excluded; there is no ingestion, SSH/rsync, router, archive quorum
or 14-shard residency. Query clients share the test process, so RSS includes
client work. Memory sampling starts after initial prewarm and uses a 100 ms
interval, not an exact kernel high-water mark. Host available memory is reported,
but an isolated test cannot pass the production host-headroom gate.

For the existing Amsterdam generator, a portable Linux release test executable
can be passed to `ops/scripts/run-transparent-burst.py --test-binary <path>
--source-sha <revision> --out <new-directory>`. Keep the source checkout with the
runner so its manifest can hash the Rust source. The manifest also hashes the
prebuilt executable; preserve build flags and host/cgroup configuration with the
results. The actual canary and fleet gates remain in [deployment](deployment.md).

### Full recent residency

Pass `--fixture <fixture.json>` to load a frozen three-publication fixture with
an assignment for fourteen recent shards. Its JSON contains `worker_id`,
`query_shard` and exactly three `publications`, each with `directory`, `assignment`,
`map_sha256` and `height`; paths resolve relative to the fixture file. Copy public
artifacts to an isolated location, preserve hard links between unchanged revisions,
and keep the source map and assignment hashes. Do not use the mutable live
publication tree as a replay fixture.

This mode uses a 5 GiB RAM cache and a new 10 GiB runtime disk cache per process,
prewarms all 28 runtimes, and verifies full warm readiness on each activation.
Both clients query the changing tail. Cold prewarm has a ten-minute timeout; the
measured burst and verification have a two-minute timeout. Runtime-cache writes
and cleanup affect only the temporary directory, never the frozen input.

On Linux, use `--systemd --test-binary <executable> --source-sha <revision>` for
one fresh cgroup per repetition: CPUs 0–3, the recent-worker MemoryHigh/MemoryMax
settings from [deployment](deployment.md), and zero swap. Kernel peak memory
includes cold startup; cgroup limits, memory events and CPU accounting are saved.
The runner checks a **modeled** 8 GiB host with 512 MiB reserved for non-worker
memory, requiring at least 20% remaining. `--host-overhead-bytes` makes that
assumption explicit; do not lower it to force a pass. This model cannot substitute
for measured live-host headroom. A fixture run without validated kernel counters
and process isolation cannot pass the memory qualification.

The Makefile exposes these options as `TRANSPARENT_BURST_FIXTURE`,
`TRANSPARENT_BURST_BINARY`, `TRANSPARENT_BURST_SOURCE_SHA` and
`TRANSPARENT_BURST_SYSTEMD=1`. Keep `summary.json` and all logs even if only one
setting fails; the runner attempts the complete comparison before exiting.

### Separate clients and preparation diagnostics

Use `--external-clients` with a prebuilt test executable to move the two exact
query clients into another process. With `--systemd`, clients run on CPUs 4–7,
outside the worker's memory cgroup; the generator therefore needs those CPUs
available. Each run retains the client configuration, JSONL query records and
process log beside the worker report. Client failure fails the run even when
worker publication succeeds. Version 4 reports require `client_shutdown_complete`: after requesting stop, the
worker keeps HTTP serving until each client flushes its final record and writes a
done marker. Readers drain to EOF after that marker; missing completion, truncated
records and fatal errors fail qualification. Both clients must complete warmup, and the final
publication must return exact occupied rows for both tables.

`TRANSPARENT_BURST_EXTERNAL_CLIENTS=1` enables this through Make;
`TRANSPARENT_BURST_HOST_OVERHEAD_BYTES` exposes the explicit non-worker reserve.
The default remains the smaller local experiment described above.

Manual burst logs enable worker debug diagnostics. Runtime events identify the
revision/table/segment and time waiting for restore/build slots, disk restoration,
source verification/loading, runtime construction and disk saving. Admission
rejections record the current cgroup usage, held reservations and requested
bytes; prewarm retries are also recorded. Successful prepare responses include
`loading_seconds` and `warming_seconds`, retained in each activation report.
Query events also separate evaluation-slot admission, runtime acquisition,
blocking dispatch and evaluation. A setup request may restore the runtime before
the private query starts; use the complete client completion gap to include that
work. These durations can overlap across runtimes: their sum is work time, not the
publication's wall time. Debug diagnostics are opt-in for normal service runs.

Construction events further split encoding, public setup, hint-column generation,
packing preprocessing and published-parameter serialization. The packing reuse
dependency includes coefficient-for-coefficient comparisons at production
parameters and a frozen reference digest; disk tests check byte compatibility
across buffer boundaries, short writes, restore and corruption. Retain query
retry counts and gaps between each client's exact completions alongside latency
percentiles; successful-query p95 alone omits admission backoff.

Full-residency runs must also prove full warm readiness **before** starting the
publication wave. `cold_ready` and `cold_warm` preserve that check; an incomplete
startup fails the run rather than charging missing startup work to publication.
A successful prewarm task by itself does not prove that every runtime was built.

`--worker-budget-seconds` (Make: `TRANSPARENT_BURST_WORKER_BUDGET_SECONDS`) adds a
stricter screening budget derived from the fleet's remaining work. It cannot
exceed the public 30-second ceiling. Both the original ceiling result and the
stricter result remain visible in the summary; neither is fleet acceptance.


The v6 burst report drains optional snapshot persistence before initial readiness
and after the measured clients finish. Memory sampling continues through the
final drain. Both `cold_persistence_complete` and `persistence_complete` must be
true, so asynchronous writes cannot conceal later memory peaks or unfinished
work. The report includes both drain durations.

For target CPU diagnostics, build the shard server test with
`--features portable-kernel` to force the existing chunked-split backend for both
construction and restoration. The default remains automatic backend selection.
Reports record `kernel_policy` and the runner records CPU information; pinning
four CPUs on the Amsterdam generator does not reproduce the older four-vCPU
worker's performance. Qualification on the generator is a screening step before
the actual loaded canary.
