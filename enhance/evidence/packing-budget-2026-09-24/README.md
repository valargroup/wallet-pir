# Packing-router memory limits: September 24, 2026

Decision: use an **8 GiB packing-router host**, initially targeting four CPUs,
**six retained serving packing objects plus one construction/activation slot**,
and four active packing requests. Reserve 1 GiB for the host and cap the service
at 7 GiB. This is a measured **component memory budget**, not qualification of the
unimplemented router's HTTP path or a deployed capacity change.

The measured maximum at the 7 GiB service ceiling was seven serving objects plus
one concurrent construction, but the cold-cache case peaked around 6.94 GiB.
The lower initial target leaves space within that ceiling for HTTP, routing,
connections and other allocations that this standalone probe does not contain.

## What was measured

The benchmark uses the production `runtime::Packing::new`, `query_coefficients`
and `pack` from frozen wallet-pir commit
`a3c7626a54b6fee95be61a8629567fd29e14cf1e`, with pinned `ipir-sp`
`611a29284264d844bf4dba00de2874c5b762f8c2`. It uses release mode with fat LTO,
Rust 1.91.0, x86-64 Linux and the system allocator. The only source overlay is
[the frozen measured example](packing_budget_measured.rs); the binary and source
hashes are in [host-and-hashes.txt](host-and-hashes.txt).

An existing idle DigitalOcean benchmark VM in ams3 supplied eight Xeon Platinum
8358 vCPUs and 32 GiB physical RAM. Each trial used a fresh systemd cgroup with
swap disabled and the stated hard memory limit. The 8 GiB campaign was pinned
to CPUs 0–3. The earlier 4 GiB campaign used CPUs 0–1. This is a memory-constrained
VM experiment, not a physical 8 GiB SKU certification. No production service was
changed, and no new infrastructure was provisioned.

A separate preparation process built a synthetic database with nonzero,
deterministic 653-byte records, 33 records per row, p16/q48 and 12,288 database
columns. It generated fresh wallet keys, evaluated queries, packed responses and
decrypted/checksummed their answers against the source records. The 32K fixture
uses 1,081,343 records and 16 queries covering positions 0, 32, 33 and the last
record. The 4K, 8K and 16K fixtures each have four such queries. No live chain or
journal is used; `main`/`ironwood`, height zero and a zero anchor hash are fixture
metadata required by the protocol. No private wallet keys are retained.

The measurement process contains no database or client setup. It loads the hint
and builds separate packing objects, then clones each request body and worker
intermediate, decodes coefficients, synchronizes the first request in each thread,
packs and checks every response against its preverified SHA-256. Coefficients,
bodies, intermediates and responses remain live together for an extra 100 ms in
the overlap trials to represent buffered output. This is not an actual slow HTTP
reader or socket backpressure test. Threads cycle through the fixed fixture
corpus; it is not a fresh-query offered-rate throughput test.

A builder thread constructs and retains one additional packing object while
queries use the original objects. Each process is fresh; repeated publications,
expired-session eviction and long-running allocator behavior are not modeled.
All independently allocated copies use the same valid hint. They model the
allocation cost of distinct retained sessions, without asserting multi-session
routing correctness or exercising distinct hint-file cache entries.

## Results and interpretation

[summary.json](summary.json) contains every trial's outcome, command, cgroup peak,
heap observations and completed-answer count. Run `python3 summarize.py` here to
rebuild it from the raw logs. The numerical table is in [results.md](results.md).

Each additional packing object retains approximately **722,124,832 bytes =
688.67 MiB** of live requested heap. The 4K, 8K, 16K and 32K cases all have this
increment under the fixed record/profile geometry. Smaller logical row counts
therefore do not proportionally reduce packing residency. The hint is
201,375,776 bytes, independent of those row counts. A full-size upload is 282,652
bytes and the worker intermediate is 98,304 bytes before transport encoding.

Linux RSS and cgroup peaks exceed live heap because construction temporaries,
allocator-retained pages, thread/runtime state and file cache also consume
memory. Four-CPU preprocessing has a higher construction peak than the two-CPU
run. Use the measured cgroup envelope, not `689 MiB × session count`, to size a
host.

The relevant boundaries were:

- **4 GiB, two CPUs:** four objects could be constructed; constructing the fifth
  was OOM-killed. Four objects with one request passed. Three plus a concurrent
  fourth construction and four packing requests passed at about 3.56 GiB, leaving
  inadequate room for a normal 4 GiB host reserve.
- **8 GiB, four CPUs:** ten objects could be constructed; constructing the
  eleventh was OOM-killed. Nine serving objects plus a concurrent tenth build
  was also OOM-killed. Eight plus a ninth build passed around 7.41 GiB, but does
  not leave the selected 1 GiB host reserve.
- **7 GiB service ceiling:** eight serving objects plus a ninth build was
  OOM-killed. Seven plus an eighth build and four requests passed three repeats,
  each verifying 512 packed responses. A 16-request stress trial also passed,
  but CPU contention increased packing latency; it is not the proposed admission
  count. A cold-cache four-request run passed near 6.94 GiB.
- **Initial six-plus-one target:** tested separately with cold hint/corpus cache,
  four requests and 512 verified responses; see the exact peak in results.md.

These are **session-object slots**, not worker replicas or necessarily domains.
Six retained frontier versions consume the six default serving slots. Adding a
sealed domain requires another assignment or separately qualifying the higher
limit. Incrementing evaluation replication needs no additional packing objects.
Pins count against the slots even after logical expiry. A construction slot must
not be reused until the old object is actually freed. The test does not justify
multiple simultaneous publications, unlimited request buffering or optimistic
admission based only on current RSS.

## Evidence and limitations

- [metadata.json](metadata.json): source, binary, workload and environment identity.
- [raw/](raw/): immutable JSONL phase output, stderr and compressed cgroup samples.
- [fixture32.json.gz](fixture32.json.gz): public query/intermediate corpus with
  expected response hashes. The large hint is reproducible from the deterministic
  fixture generator and identified by hash; it is not committed.
- [run_measured.py](run_measured.py): exact driver used for the measurements.
  [run.py](run.py) fixes a subsequent sampling issue: after systemd removes an
  exited cgroup, the measured driver mistakenly reads root-cgroup counters for
  its final sample. **Ignore that final sample.** All reported peaks come from
  the unit's preserved `MemoryPeak` property, not those root counters;
  [summarize.py](summarize.py) uses that property explicitly.
- [remaining.py](remaining.py) and [cold.py](cold.py): additional trial commands.
  Cold runs request eviction of only the fixture files using `POSIX_FADV_DONTNEED`;
  no global page-cache dropping was used. File-cache charges are visible in the
  raw cgroup samples. Ordinary runs can use previously cached fixture pages.

OOM runs are intentional negative boundary tests, not successful qualifications.
The failed first preparation used an unsupported `synthetic` network label and
produced no valid query corpus; its logs remain. A rebuild briefly picked up
manifest fields from concurrent workspace edits that were absent from the frozen
source; [that compile failure](build-incompatible-workspace.log) is retained.
The measured overlay was corrected and rebuilt before the successful fixtures
and all limit tests.

The reusable example in the live workspace was adjusted for the concurrent
protocol API changes and passed a separate macOS build, four decrypted fixture
checks and eight repeated packing checks. Those are smoke checks only; the
Linux memory qualification remains bound to the frozen source above. The required
`make check` attempt stopped at formatting in concurrently edited protocol/runtime
files. Strict Clippy also initially stopped on an existing identical-branches
warning in `control.rs`; the example passed with that one unrelated lint allowed.
The supporting logs are retained with `workspace-` and `current-workspace-` names.

Before deployment, qualify the extracted router on the actual host with fresh
HTTP requests, bounded upload/output queues, slow clients, cancellation, sustained
rates, worker failures, publication/retention churn and recovery fencing. Add
memory reservations for all such allocations; use another router assignment or
reduce admitted sessions if the measured combined path exceeds the budget. No
fleet throughput, availability or wallet-protocol claim follows from this probe.

## Reproduction

Use an isolated checkout of the recorded commit. Copy `packing_budget_measured.rs`
to `enhance/services/enhance-pir-server/examples/packing_budget.rs`, then build:

```sh
cargo build --locked --release -p enhance-pir-server --example packing_budget
```

Generate a fixture in a new directory, outside the router memory cgroup:

```sh
target/release/examples/packing_budget prepare \
  --output /absolute/new/fixture --rows 32768 --queries 16
```

Run the corrected driver as an operator allowed to create systemd units:

```sh
python3 run.py --binary /absolute/path/to/packing_budget \
  --out /absolute/new/results --name six-plus-one --limit 7G --cpus 0-3 \
  measure --fixture /absolute/new/fixture --copies 6 --concurrency 4 \
  --iterations 128 --hold-ms 100 --overlap
```

Never run a limit sweep without a memory cap and swap limit. The intentional OOM
boundary tests are confined to their own cgroup and time-limited to 900 seconds.
