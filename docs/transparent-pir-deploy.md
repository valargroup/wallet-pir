# Transparent PIR deployment

What is deployed, how to change it, and the measurements the shape rests on.
The Enhance fleet is a separate product with its own runbook in
[enhance-pir-deploy.md](enhance-pir-deploy.md); the two share a coordinator host
and nothing else.

## What is running

| | |
|---|---|
| Public name | `transparent-pir.valargroup.dev` |
| Worker | `transparent-pir-worker-01`, `s-4vcpu-16gb-amd`, ams3 |
| Addresses | `164.90.205.186` public, `10.142.0.4` private |
| Service | `transparent-shard-server`, `0.0.0.0:8093`, `MemoryMax=12G` |
| Shard set | `/srv/transparent-pir/shards` on the worker |
| Published set | `/srv/zakura/transparent-shards-v6` on the coordinator |
| Coverage | 3 shards, heights 3,428,143–3,473,474, 169 MB |
| Serving schema | `transparent-shard-v6` |

**This build serves `transparent-shard-v7`, which the deployed set is not.**
A v7 binary refuses a v6 set at load and a v6 binary refuses a v7 one, so
deploying this build against `/srv/zakura/transparent-shards-v6` is an outage
rather than a degraded service. Publish a v7 set into a directory of its own
first — `shard-publish` refuses a `shards.json` of another schema — and deploy
with `shard_dir` pointing at it. The v6 set and the running binary stay as the
rollback baseline until that has verified.

The sections below describe what this build does. Everything marked as new
against the live worker — the geometry registry, the bounded runtime cache, the
revision-addressed routes, `/metrics` and `/v1/ready` — arrives with that
deploy, not before it.

TLS terminates on the worker, not behind the Enhance coordinator's Caddy. The
two never share a Caddyfile: the coordinator's is rendered and installed by
`deploy-enhance-pir.sh`, and a token that script does not substitute would fail
`caddy validate` on its next deploy — breaking a live service over an unrelated
change. When the coordinator daemon lands and there is more than one worker,
fronting moves there and the DNS record follows it.

The worker's firewall opens 8093 to the coordinator tag alone, plus 80 and 443
to the world for Caddy. The retrieval port is not public; only the routed paths
are.

## The two surfaces, and why they are on different hosts

A wallet syncs filter-first: take the shard map, download each shard's range
filter, test its own scripts locally, and issue a private query only for the
shards that matched. Without the filters a wallet would have to query every
shard to find its own history, which is the cost the design exists to avoid.

| surface | host | paths |
|---|---|---|
| Public filters | **both hosts** | `/v1/filters/shards`, `/v1/filters/shards/{id}/filter` |
| Per-block filters | `enhance-pir.valargroup.dev` | the rest of `/v1/filters/*` |
| Private retrieval | `transparent-pir.valargroup.dev` | `/v1/shards*` |

Private retrieval is addressed by **revision**, not by shard alone:

```
GET  /v1/shards/init
GET  /v1/shards/{id}/revisions/{manifest_digest}/setup/{table}/{segment}
POST /v1/shards/{id}/revisions/{manifest_digest}/query/{table}
```

The digest is the one the map publishes for that shard, so this is public and
already in the wallet's hands. What it buys is that a tail republished mid-sync
cannot answer a query prepared against the revision it replaced. A digest the
worker no longer holds returns **409** with the current map digest, which tells
the wallet to refresh the map and re-derive that range rather than accept rows
for a range it did not ask about. The worker keeps three superseded revisions
per shard by default (`--retain-revisions`), so an ordinary republication does
not strand a wallet mid-sync.

`/v1/shards/init` now publishes **one entry per geometry the worker holds**
rather than one pair of schemes for the fleet, because a set may mix them. Each
entry carries the row counts beside the derived scheme, so a client checks the
pair instead of adopting either.

The shard map and range filters answer at the same paths on both hosts, so a
wallet points at either origin by changing only the base URL. The transparent
host serves them from `transparent-shard-server`, which already loads and
digest-verifies every `filter.bin`; retaining the bytes costs about a hundred
kilobytes per shard, negligible beside a 257 MiB runtime.

`/v1/health`, `/v1/ready` and `/metrics` are **not** routed publicly on either
host. They report shard counts, retained revisions, cache occupancy, and build
and eviction rates — operator information, and a running commentary on how much
traffic the worker is taking. The deploy verifies them over the VPC and asserts
each returns 404 through the edge, because the way that goes wrong is a route
wider than intended and the failure is otherwise silent.

An earlier revision kept public bytes off the retrieval origin entirely, so a
filter download and a private query could not be correlated. That rule bought
less than it appeared to: the shard id of a private query is public in its own
URL, and one operator runs both services, so the correlation was available from
logs regardless. What it did preserve is a wallet's ability to reach the two over
*different network paths* — separate circuits, or a third-party filter mirror.

That option is deliberately kept. The filter service still serves the same bytes
at the same paths on its own host, so a wallet that wants two origins has them
and one that wants a single endpoint has that too. Do not remove either copy
without deciding which of those two wallets you are dropping.

## Deploying

Two workflows, both gated on `ref` being the current `main` with a **completed**
successful `ci.yml` run for that exact SHA. A green PR is not enough: CI re-runs
on main after a squash merge and takes about thirteen minutes, and the deploy
will refuse until it finishes.

The `production` GitHub Environment permits deployments from `main` only, so a
branch cannot be deployed even for a test.

### The worker: `deploy-transparent-shard.yml`

Inputs: `ref`, `shard_dir` (the published set on the coordinator), and `mode`
(`preflight` or `deploy`). Preflight stages the binary, unit and Caddyfile, runs
`caddy validate`, and checks host capacity without activating anything.

The deploy validates the shard set **before** stopping the service. An
incomplete set is refused at load by `ShardSet::open`, but by then the service
is down, so a bad input would become an outage rather than a failed
precondition.

Shard bytes reach the worker by `rsync` from the coordinator, which is also the
deploy runner, so the set is local to it. That stands in for the coordinator
push that multi-worker will add.

### The filter service: `deploy-enhance-pir.yml`

The filter service runs on the coordinator and is built and activated as part of
the **Enhance** artifact. There is no path that ships it alone, so changing it
means an Enhance coordinated rollout: stop the Enhance coordinator, activate
both Enhance workers, reactivate the coordinator. It has verification and a
rollback closure, but the blast radius is the live Enhance service.

`deploy-enhance-pir.sh` has a `preflight` mode; `deploy-enhance-pir.yml`
hardcodes `deploy`, so that mode is unreachable through CI. Until a `mode` input
is added, verify a risky change out of band — for a shard-set change, confirm
every filter matches the digest its map publishes, since the service refuses to
start otherwise and would roll the whole Enhance deploy back.

**The schema bump reaches this service too, and it is the sharper edge of the
two.** `ShardFilters::open` deserializes `shards.json` into the same `ShardMap`
the wallet uses, so a v7 build cannot read the v6 map: no `geometry` on an
entry, and `seal` is now keyed by geometry name rather than a single record.
The unit's `--shard-dir` now points at `/srv/zakura/transparent-shards-v7` for
exactly this reason, and the service refuses to start on a set it cannot read — inside the Enhance
coordinated rollout, whose blast radius is the live Enhance service.

So the filter service's `--shard-dir` must be repointed to the v7 set **in the
same change that ships the v7 binary**, not after it. Publish the v7 set first,
update `transparent-filter-server.service`, and only then run
`deploy-enhance-pir.yml`. Deploying the Enhance artifact from this build with
the unit still pointing at the v6 set will fail at startup and roll Enhance
back.

## Publishing a shard set

`backfill-transparent-events.yml`, action `publish`, with `data_dir` (the event
journal) and `shard_dir` (a new directory). A set is published under one schema
and `shard-publish` refuses a `shards.json` from another, so a build that
changes the schema must publish into a directory of its own.

The Ironwood-range journal is at
`/srv/zakura/transparent-event-data/superseded-v1`; the genesis-to-tip journal
is `/srv/zakura/transparent-event-data`. Publishing reads a journal read-only
through `EventStore::open_existing`, which errors on a version mismatch rather
than renaming files aside, so it cannot damage the journal.

The build and the publish are niced and run in the idle IO class. They share the
coordinator with the event ingester, which runs at a reduced `CPUWeight` and, since
the state-backed reader landed, is CPU- and IO-bound on local RocksDB rather
than waiting on RPC — so contention costs real blocks per second.

**The unit pins `--shard-dir` to a specific published set.** The filter service
reads it once at startup, so publishing a new set is a unit change and a
restart, not a hot swap.

## Geometry is per shard, not per fleet

A shard names its geometry from a closed registry, and a set may mix them —
archive geometry for old, dense history, a narrower one for the recent window
wallets synchronise constantly. The registry is in
`pir/transparent-shard/src/layout.rs`:

| Name | Directory rows | Page rows | Directory query |
|---|---:|---:|---:|
| `recent-8k` | 8,192 | 8,192 | 128,008 B |
| `recent-4k` | 4,096 | 4,096 | 106,504 B |
| `archive-32k` | 32,768 | 32,768 | 258,056 B |
| `archive-wide` | 32,768 | 65,536 | 258,056 B |

Every entry keeps 3,584-byte rows, so only the row *counts* vary. That is what
keeps the record codec, `EVENTS_PER_PAGE` (36) and `DIRECTORY_SLOTS` (14) fixed
across the registry — widening a row is a schema change and a republication, not
a new entry, and `Geometry::validate_publishable` refuses one.

Seal thresholds are derived from the geometry rather than given
(`SealPolicy::for_geometry`): the directory keeps a seventh in reserve for
two-choice placement, the page table a thirty-second for one more block. A
mixed set therefore publishes one set of thresholds per geometry, keyed by name.

`shard-publish --archive-geometry X --recent-from H` publishes the two-tier set;
the cutoff is a forced shard boundary, because a shard's rows are addressed at
one row count and none may span the change. Derive `H` from the pinned anchor's
chain timestamp, not from a block count — a height standing in for "six months"
drifts with the interval, and re-deriving it later re-shards the chain.

## Runtimes are bounded, not kept

Table plaintext is verified at startup and released; what is retained is the
path and the digest, and the bytes are re-read and re-verified when a runtime is
built from them. Runtimes live in a byte-bounded LRU with single-flight
construction: one build per identity, everyone else waits, and a request that
cannot make room gets a `503` with `Retry-After` rather than pushing the process
past its budget.

Room is reserved *before* a build starts, from the size the geometry implies,
because the build is what allocates. The reservation counts the encoded database
and the pack matrices and nothing else, so `--cache-bytes` is set below the
host's real headroom: 8 GiB against `MemoryMax=12G` leaves 4 GiB for allocator
fragmentation, the transient plaintext each build reads, the shared parameters,
and in-flight requests. Re-measure with `shard-residency` before raising it.

`--build-slots` defaults to 1 and `--query-slots` to 2, the latter because
`shard-scaling` measured evaluation saturating at two threads. The unit file
passes `--cache-bytes 8589934592` with those two defaults.

## What the fleet costs, measured

Per-runtime resident memory, measured with `shard-residency` at the pinned
geometry, decomposing exactly: a 32 MiB `u16` database plus three 32 MiB pack
matrices, the three being `t_exp_left`. `runtime::reserved_bytes` computes the
same two terms, and a unit test pins it to this measurement.

| | |
|---|---|
| Per table segment | **128.5 MiB** |
| Per shard (both tables) | ~257 MiB, ~290 MiB with allocator fragmentation |
| Build time | 0.67 s per segment, ~1.4 s per shard |

`db_cols` is 2,048 at every candidate row count, so the pack matrices cost
96 MiB whatever the geometry and only the database follows the row count. Table
width is therefore nearly free in memory while it divides the shard count.

Full chain, censused genesis-to-tip over 352,873,356 events in 3,473,687 blocks:

| rows | shards | fleet plaintext | live share | fleet RAM | hosts | $/mo |
|---:|---:|---:|---:|---:|---:|---:|
| **8,192** (pinned) | **1,091** | 64.1 GB | 64.0% | 273 GiB | 21 | 1,764 |
| 32,768 | 314 | 73.8 GB | 51.2% | 137 GiB | 11 | 924 |
| 65,536 | 162 | 76.1 GB | 48.4% | 111 GiB | 9 | 756 |

Note that fleet *plaintext grows* as resident memory falls: wider tables pad
more, and the fixed pack matrices amortise over fewer shards. Disk is not the
constraint; RAM is.

Evaluation is memory-bandwidth-bound and **saturates at two threads** — one core
takes 66 of the ~87 GiB/s the kernel reaches, sixteen return 1.39x one core.
Capacity therefore scales with host count, not core count, and `$/GB` is flat at
$5.25 from 16 GB to 256 GB, so one 256 GB host costs the same as eight 32 GB
hosts and has one memory path instead of eight. Buy more small hosts.

## Growing to the full chain

Contiguous ascending shard ranges, one per worker, append-only. Shard ids are
assigned by the sealer in height order and stable once sealed, so a new shard
appends to the last worker until it fills and then a new worker is appended.

The tail needs no rule and no host of its own: it is the last shard, so
contiguous assignment already places it on the last worker. A per-block
republish is ~1.4 s out of 75 — 1.9% of one core. Retain an hour of revision
*bytes* (2.8 GB) with an LRU of built runtimes rather than an hour of resident
runtimes, which would be 13.6 GiB.

**Recommended shape:** two geometry classes — archive at 32,768, the recent
window at the pinned 8,192 — on 11–12 `s-4vcpu-16gb-amd`, about $950/mo. The
recent class costs ~9 GiB over an all-archive fleet and buys a 2x smaller query
on every incremental sync, which is the traffic every wallet pays constantly.

## Geometry decision

The archive tier's geometry is **decided**: `archive-wide`, 32,768 directory
rows and 65,536 page rows, with the recent tier unchanged at `recent-8k`. The
reasoning, the evidence and the one measurement it still rests on are in
[transparent_pir_geometry_decision.md](transparent_pir_geometry_decision.md).
Nothing is published at that geometry yet.

## What is not settled

- ~~**`g` is unmeasured at wide geometry.**~~ **Measured 2026-09-07** over the
  complete journal at `archive-wide`: mean 2.58, p50 1, p90 3, p95 9, p99 35,
  max 156, across 9,264,547 distinct indexable scripts. The coverage-matched
  8,192 baseline is now measured too, and it **contradicts the figure this doc
  used to carry**: real full-chain 8,192 is mean `g` 6.55 and mean restoration
  2.03 MB, not 27.4 and 3.75 MB. So `archive-wide` cuts `g` 2.54-fold, not
  10.6-fold, and on restoration bytes it is a **trade rather than a win** —
  p50 0.52 MB against 0.26, p90 2.41 against 1.54, but p99 33.46 against 44.42.
  The cause is the page query, 430,088 bytes at 65,536 page rows against
  128,008 at 8,192; directory bytes do favour the wider shape. Evidence in
  [fullchain-archive-wide-notes.md](transparent-pir-evaluation/shard-utilisation/fullchain-archive-wide-notes.md)
  and [fullchain-8192-matches.txt](transparent-pir-evaluation/shard-utilisation/fullchain-8192-matches.txt).
- **Several figures quoted in this repo come from the partial `genesis-*`
  census covering 9.4% of chain height.** The 27.4/3.75 MB pair above was one.
  `layout.rs` is another, and confirmed rather than suspected: it cites
  `genesis-geometry-notes.md` directly, and its "511 shards" is the exact
  figure the README identifies as the partial result against a real 1,091. The `shard-utilisation` README was written to warn about exactly
  this after it caused a 55% error in shard count, and it has now happened
  twice. Re-measure before quoting.
- **No two-tier set has been published or served, and nothing has been measured
  on a droplet at archive geometry.** The registry, the mixed publisher, the
  bounded cache and the revision routes are implemented, tested, and now live at
  `recent-8k`. But the 91.1 GiB fleet-RAM figure is `reserved_bytes` arithmetic
  rather than RSS; `shard-residency` and `shard-scaling` have never run at
  32,768/65,536; and the "saturates at two threads" result behind
  `--query-slots 2` came from a 16-core Apple Silicon machine. The census
  settles what a geometry *contains*, not what it costs to serve.
- ~~**Which limit binds is not uniform along the chain.**~~ **At `archive-wide`
  it is uniform**: all 161 sealed shards closed on page rows and not one reached
  the 393,216-script target. The non-uniformity is a property of 8,192, where
  the directory is tight enough to bind sometimes; a directory that is never the
  constraint cannot vary in whether it constrains. Still true at 8,192, where
  1,042 shards close on page rows and 48 on scripts.
- ~~**The directory is slack at wide geometry and could be narrowed.**~~
  **Confirmed by the real placer**: 162 of 162 shards fit one directory segment,
  fullest row 11 of 14 slots, busiest shard 284,221 scripts against 458,752
  capacity — 62.0%, exactly what this section predicted. Fleet plaintext came in
  at 57.1 GB against the 76.1 GB projected for 65,536/65,536, because shard
  count is set by the page table and a narrower directory pins half the bytes
  for free. Do **not** pair a 16,384 directory with 32,768 pages: 223,711 observed
  against 229,376 capacity is 2.5% headroom, and one shard overflowing into a
  second segment costs every wallet an extra directory query.
- **Concurrency under restoration load is untested.** Cold shards are packed
  denser and get less bandwidth each; a script appears in 27.4 shards on average
  and 563 at p99, so restorations land across all history rather than in the
  recent window.
- The scaling measurement ran on a 16-core Apple Silicon machine, not a droplet.
  The saturation *shape* should hold, but re-run `shard-scaling` on a droplet
  before buying the fleet.

## Known drift and hazards

- **A v7 build of `transparent-filter-server` cannot read the v6 shard map**,
  and that binary is activated by the Enhance coordinated rollout rather than by
  the transparent one. See "The filter service" above: repoint its `--shard-dir`
  to the v7 set in the same change, or the Enhance deploy fails at startup and
  rolls back.

- The live `enhance-pir-coordinator` firewall has **SSH open to `0.0.0.0/0`**,
  which is drift from Terraform. A full `terraform apply` reverts it and locks
  out anyone outside the two configured CIDRs.
- `ops/infra/digitalocean/production/terraform.tfstate` is local and untracked,
  and wants to create a `transparent_spend_worker` nobody decided to provision.
  Use `-target=` for unrelated changes.
- `cloud-init-coordinator.yaml.tftpl` looks for a volume by an id that does not
  match the volume's actual name, so a rebuilt coordinator would fail to mount
  `/srv/zakura`.
- DigitalOcean reused the private address `10.142.0.4`, previously the memo
  worker's. The coordinator's `/root/.ssh/known_hosts` still carries the old key
  for it, so interactive SSH from the coordinator to the worker fails a host-key
  check. The deploy is unaffected: it writes its own `known_hosts`.
