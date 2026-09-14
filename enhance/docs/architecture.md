# Architecture

Enhance is a position-indexed database of Ironwood output records. A coordinator
builds the database from canonical blocks and publishes consistent generations.
Workers evaluate encrypted queries over their assigned shards; the coordinator
combines their answers. The wallet selects the record within the decrypted row.

```text
Data publication

  Zakura archive RPC
          |
          v
  Canonical ingest and record journal
          |
          v
  Coordinator: prepare and publish generations
          |
          v
  Worker groups: replicated shards

Query round trip

  Wallet compact scan
          |
          v
       Client -- encrypted query --> Coordinator -- evaluate --> Worker groups
              <-- encrypted row ---             <-- partials ---
          |
          v
  Row decoding, record selection and wallet validation
```

## Repository layout

Paths below are relative to the repository root. Cargo package names remain the
names used by build and run commands, even when directory names differ.

| Path | Responsibility |
|---|---|
| `enhance/crates/enhance-pir` | Public types, record validation, query preparation/decoding, HTTP client and `enhance-pir-cli` |
| `enhance/services/enhance-pir-server` | Canonical ingestion, journal, coordinator, workers and qualification utility |
| `enhance/services/pir-apm` | Metrics dashboard and operational alerts |
| `enhance/tools/loadtest` | `enhance-pir-load-test`: closed-loop encrypted query measurements |
| `enhance/ops` | Deployment scripts, service units, expansion controller and operations tests |
| `enhance/docs`, `enhance/evidence` | Current documentation and retained measurements |
| `enhance/crates/transparent-spend-pir` | Retained outpoint-keyed spend types/client, not an active service |
| `ops/infra/digitalocean/production`, `ops/deploy/coordinator` | Shared infrastructure, archive-node and ingress configuration |
| `.github/workflows/deploy-enhance-pir.yml` | Tested-revision artifact build, preparation and deployment |

The root Cargo workspace builds active products. `demos/legacy-spendability` is
an independent preserved workspace, not an Enhance dependency or deployment.

## Ingestion and storage

The server reads canonical transactions through Zakura RPC starting at Ironwood
activation height 3,428,143. It extracts one record per output position in chain
order, checking continuity against the block's Ironwood tree size. The active
journal stores fixed-width records in `enhance/records.bin` and committed block
metadata in `enhance/manifest.json` beneath the configured data directory.
The journal supports restart and rewind; it is not a finality guarantee.

Each record is 737 bytes. Nine records occupy a 6,633-byte row, which fits two
PIR instances; a tenth record would require a third. A shard contains 8,192 rows,
or 73,728 positions. Full shards are sealed; the partly filled frontier changes
as blocks arrive. Row digests identify shard content for preparation and reuse.
The logical database is padded to a power-of-two row count, at least 8,192.
See the [protocol](protocol.md) for field offsets and public geometry.

A reorg rewinds the journal to the common ancestor and appends the replacement
chain. Publication validates its anchor before exposing the candidate. Generation
IDs increase even across same-height reorgs, so they must not be treated as block
heights. A wallet must still reconcile its scan context with the published anchor.

## Publication and queries

Initialization returns a generation, PIR parameters and published public material
in one response. This atomic snapshot prevents clients from mixing parameters
from one generation with another. Each query names its generation; responses bind
the generation and public-parameter epoch.

The coordinator prepares the shards and combines their public hints. It activates
replicas that completed their full assignment, then publishes the generation once
at least one replica in every used group is ready. A failed candidate leaves the
previous generation available. The coordinator retains eight generations;
workers also pin the unpublished candidate so preparation cannot evict a still
published snapshot. Retention is a count, not a fixed session lifetime.

For a query, the coordinator sends encrypted coefficients to one ready replica
in each populated group. Groups evaluate in parallel. Within a group, replicas
are alternatives for load balancing and retry: only one answer contributes to
the result. The coordinator combines these partial answers and returns one
encrypted row. The client decodes it and extracts the requested slot locally.

## Placement and capacity

Ordered groups own consecutive ranges of 16 shards. A group therefore covers
1,179,648 positions, with two production replicas holding the same assignment.
Group order fixes shard ownership and is append-only; replicas can be replaced
within a group. Readiness is tracked per generation, so a recovering replica is
not selected for data it does not yet hold.

Adding groups extends position coverage. It does not make each request cheaper:
every query still evaluates every populated group. At range exhaustion, publication
stops before allocating out-of-range shards. Retained generations remain answerable,
but coverage stops advancing and health reports failure. Online append can make
room for the backlog without restarting existing processes.

The c-4 hardware limits, qualification gates and expansion controller are described
in [capacity expansion](capacity-expansion.md). The [status record](status.md)
distinguishes this implemented design from verified deployment facts.

## Boundaries

The active server exposes only the Enhance table. The outpoint-keyed
transparent-spend implementation remains in the tree but is not registered,
ingested or routed. It must not be confused with the active transparent
script-history recovery product.

PIR hides the selected position within the advertised database under the scheme's
security assumptions. The service still observes network connections, request
sizes, timing and generation IDs. Encoding checks and parameter hashes do not
prove that a server's records came from the canonical chain. The wallet performs
note authentication and chain-context checks; fee, expiry and transaction-shape
metadata remain indexer assertions. See [integration](integration.md).
