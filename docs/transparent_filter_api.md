# Transparent activity filter API, version 1

Status: the public wallet-facing surface of `server/transparent-filter-server`.
Served over TLS on the Enhance PIR host under `/v1/filters/`. The wire format of
a range response is specified separately in
[the range envelope format](transparent_filter_envelope.md), which is versioned
independently: the envelope may change without any filter byte changing.

## What a request may contain

Chain identity, a profile, and a height range. Nothing else. There is no
parameter in which a script, address, outpoint, transaction identifier, match,
or address-derived partition could be expressed, and the service has no endpoint
that accepts one.

A range request does reveal **which interval is being synchronized, and when**.
This profile does not hide that. A wallet asks for the contiguous range from its
own durable checkpoint to a block it has accepted; it must not ask only for the
blocks that matched, which would leak exactly what the filters exist to keep
local.

## Endpoints

### `GET /v1/filters/info`

Service identity and limits: `genesis_hash`, `network`, `profile`,
`envelope_version`, `start_height`, `covered_through`, `covered_block_hash`,
`max_records_per_batch`, `max_filter_bytes`.

A client must check `genesis_hash`, `network` and `profile` against what it
expects before using anything else. `covered_through` is the highest height with
durable coverage; it is not a claim about the chain tip.

### `GET /v1/filters/chain?start_height=&count=`

Heights and block hashes this service has coverage for, so a wallet can confirm
the service is on the branch it accepted before requesting filters. Returns the
covered prefix when the range runs past coverage, rather than an error. `count`
is 1..=10000.

### `GET /v1/filters/range?start_height=&stop_block_hash=`

One bounded batch of filters, in the binary envelope format. `stop_block_hash`
is display hex, as a block explorer shows it. The batch is capped at 1,000
records; a wallet catching up over a longer range issues successive batches.
Batching does not alter filter bytes.

The client must validate the response before using any result from it, including
an early positive: exact record count and order, every height and block hash
against its own accepted chain, the profile, the network, and the terminal hash.
Missing, duplicate, truncated, wrong-fork and excess records are all rejections.

### `GET /v1/filters/digests?start_height=&count=`

The double-SHA-256 digest of each filter in the range, with its height and block
hash. `count` is 1..=1000; the covered prefix is returned when the range runs
past coverage.

A wallet that just downloaded the filters can compute these itself. The endpoint
exists for the case where it has not: comparing what independent operators
publish for the same blocks without downloading both sets of filters.

### `GET /v1/filters/shards`

The published height-to-shard map: the ordered, gapless list of shards, each
with its height range, terminal block hash, filter digest, occupancy counts,
segment counts, manifest digest, revision and sealed flag. The response carries
`x-shard-map-sha256`, the digest of the map as served, so a wallet can pin the
revision it synced against without hashing the body itself.

This is protocol data, not a convenience index. Shard boundaries are derived
from chain content, so a wallet cannot recompute them arithmetically and an
independent operator must reproduce these boundaries rather than derive its own
— which is what keeps a disagreement about one event localised to one shard's
digests instead of shifting every later boundary.

Returns 404 when the service was not given a published set. Per-block filters
and shard filters are independent products, and this service serves the former
with or without the latter.

### `GET /v1/filters/shards/{shard_id}/filter`

One shard's range filter, as published. Globally identical bytes for every
wallet, immutable once published, and served with a one-year `immutable`
cache-control for that reason.

This is the byte string that makes private retrieval affordable. A wallet tests
its own scripts against it locally and issues a private query only for the
shards that matched; without it a wallet would have to query every shard to find
the ones holding its history, which is the cost the design exists to avoid.

Asking for a shard is not a disclosure: which shards exist is public, and a
wallet downloads the filters of every shard across the range it is syncing, not
only the ones it will go on to query.

**These live on a different origin from private retrieval.** Filters are served
here, on the Enhance PIR host; private directory and page queries are served by
`transparent-shard-server` at `transparent-pir.valargroup.dev`. That service
loads every `filter.bin` to verify it and deliberately refuses to serve it, so
that a wallet never fetches public bytes from the origin it makes private
requests to, which would correlate the two by construction.

## What is not served publicly

`/metrics`, `/ready` and the service's `/v1/health` are operator surfaces. They
are not under the `/v1/filters/` prefix and are not routed from the internet.

## What the digests do and do not establish

A digest detects corruption of content relative to what was previously accepted.
It is not evidence of honest or complete construction: a digest supplied
alongside a false filter simply commits to the false filter. Matching a filter
against the wallet's own accepted block hash binds *where* the filter claims to
be, not *what* it contains.

This is the boundary the design's
[trusted-service profile](transparent_pir_architecture.md#trust-and-completeness)
states for private retrieval as well: digests and structural checks are
consistency checks against corruption and stale data, not evidence that the
index matches the chain.

This matters because of how a wallet uses a filter. A negative result advances
coverage — it is a positive claim that the wallet had no activity in that block.
An operator that omits one script from one filter therefore causes a silently
wrong balance rather than a visible failure, and no amount of privacy in a
follow-up retrieval detects it.

The mitigation this API supports is **comparing digests across independent
operators**. Agreement between operators that do not share an indexer is
evidence about construction that no single operator can produce about itself.
A wallet relying on one operator is trusting that operator's completeness.

**Current position: single-operator trust is accepted.** One operator serves
these filters today, so the comparison above is enabled by the API and is not
performed by anyone. A wallet using this deployment trusts our indexer to have
built every filter over the whole block. This is a deliberate decision for the
present stage, not an oversight, and it is the assumption to revisit first if
these filters are ever relied on for balances shown to users, or served to
wallets outside our own. Standing up a second independent operator is what would
change it; nothing in the client needs to change to take advantage.

The crate implements the BIP 157 filter-header chain construction
(`filter_header` in `pir/transparent-filter/src/digest.rs`), but this service
does not publish a header chain. Its coverage starts at Ironwood activation
(3,428,143), not at genesis, and an all-zero predecessor part-way up the chain
is not a genesis-derived header chain; calling it one would overstate what it
anchors. There is no BIP 157 peer verification and no service signalling here.

## Coverage boundary

Filters begin at Ironwood activation, height 3,428,143. This is a deliberate
boundary, not a backlog item: a wallet whose birthday is earlier is not served
by this deployment and must cover the earlier range another way. A client must
read `start_height` from `/v1/filters/info` and treat anything below it as
uncovered, rather than inferring absence of activity there.

Reindexing from genesis was measured before deciding this. It is affordable —
roughly one to three days of wall clock on comparable hardware and under a
gigabyte of filters — because throughput is bound by previous-output resolution,
and the early chain carries far more transparent inputs per block than the
modern one: 16.4 blocks/s at height 200,000 against 110 blocks/s after
activation, on the same host. The boundary stands because pre-activation
coverage is not needed by the wallets this deployment targets, not because
building it would be expensive. Revisit it if that changes.

The fallback a wallet uses below the boundary is observable to whoever serves
it, which is the reason to state the boundary here rather than let a client
discover it as a gap.
