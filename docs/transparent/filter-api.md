# Transparent shard filter API

Part of the active [transparent PIR documentation](README.md). This reference defines the filter surface/format; fleet parameters and deployment status live in the linked index.

Status: the public half of a published shard set, served by the Transparent
history query hosts (`pir-query-server`, `transparent_history`) under
`/transparent/v1/filters/shards` and its deployed `/v2/filters/shards` alias.
`https://transparent-pir.valargroup.dev` serves the alias and
`https://wallet-pir.valargroup.dev` the canonical path. The publisher prepares
the map and filters; `pir-publisher verify-filters --shard-dir DIR` checks a
staged set offline.

The per-block activity filter service (`/v1/filters/{info,chain,digests,range}`)
and its range envelope were removed: no wallet client used them. See the
[last revision that documented them](https://github.com/valargroup/wallet-pir/blob/002e82e3/docs/transparent/filter-api.md).

## What a request may contain

A shard id, or nothing. There is no parameter in which a script, address,
outpoint, transaction identifier, match, or address-derived partition could be
expressed, and the service has no endpoint that accepts one. Which shards exist
is public, and a wallet downloads the filter of every shard across the range it
is syncing, not only the ones it will go on to query privately.

## Endpoints

### `GET /v2/filters/shards`

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

### `GET /v2/filters/shards/{shard_id}/filter`

One shard's range filter, as published. Globally identical bytes for every
wallet, immutable once published, and served with a one-year `immutable`
cache-control for that reason.

This is the byte string that makes private retrieval affordable. A wallet tests
its own scripts against it locally and issues a private query only for the
shards that matched; without it a wallet would have to query every shard to find
the ones holding its history, which is the cost the design exists to avoid.

A wallet may fetch these from a different origin or network path than its
private queries — separate circuits, or a third-party filter mirror. One
operator runs both today, and the shard id of a private query is public in its
own URL, so this separation limits network-path correlation rather than
operator correlation.

## What is not served publicly

Health, readiness and metrics are operator surfaces on the query host's private
listener and are not routed from the internet.

## What the digests do and do not establish

Each map entry's filter digest detects corruption of a filter relative to the
published map; `verify-filters` refuses a staged set whose filters do not match
their digests. A digest is not evidence of honest or complete construction: a
digest published alongside a false filter simply commits to the false filter.

This matters because of how a wallet uses a filter. A negative result advances
coverage — it is a positive claim that the wallet had no activity in that shard.
An operator that omits one script from one filter therefore causes a silently
wrong balance rather than a visible failure, and no amount of privacy in a
follow-up retrieval detects it. Comparing map and filter digests across
independent operators is the mitigation; one operator serves these filters
today, so a wallet trusts that operator's completeness, as the design's
[trusted-service profile](architecture.md) states for private retrieval too.
