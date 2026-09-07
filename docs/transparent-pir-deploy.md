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

The shard map and range filters answer at the same paths on both hosts, so a
wallet points at either origin by changing only the base URL. The transparent
host serves them from `transparent-shard-server`, which already loads and
digest-verifies every `filter.bin`; retaining the bytes costs about a hundred
kilobytes per shard, negligible beside a 257 MiB runtime.

`/v1/health` is **not** routed publicly on either host. It reports shard counts
and how many runtimes are built, which is operator information; the deploy
verifies it over the VPC and asserts it returns 404 through the edge, because
the way that goes wrong is a route wider than intended and the failure is
otherwise silent.

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

## What the fleet costs, measured

Per-runtime resident memory, measured with `shard-residency` at the pinned
geometry, decomposing exactly: a 32 MiB `u16` database plus three 32 MiB pack
matrices, the three being `t_exp_left`.

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

## What is not settled

- **`g`, the shards a script appears in, is unmeasured at wide geometry.**
  Restoration cost at 32,768 is extrapolated from the 0–330,000 census, and the
  same extrapolation was already 55% wrong for shard count. `census
  --shard-matches` computes it directly over the full journal; run it before
  provisioning eleven hosts.
- **Which limit binds is not uniform along the chain.** At 8,192 over the full
  journal, 1,042 shards close on page rows and 48 on scripts. `census
  --per-shard` emits heights and the closing limit per shard, so the mid-range
  behaviour can be checked rather than assumed — a geometry chosen on the
  aggregate is chosen on a mixture.
- **The directory is slack at wide geometry and could be narrowed.** Observed max
  scripts per shard against directory capacity: 86% at 8,192, 49% at 32,768, 31%
  at 65,536. A 65,536-page table with a 32,768 directory would hold at 62% and
  cut fleet RAM to ~91 GiB while dropping directory queries from 430 KB to
  258 KB. Do **not** pair a 16,384 directory with 32,768 pages: 223,711 observed
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
