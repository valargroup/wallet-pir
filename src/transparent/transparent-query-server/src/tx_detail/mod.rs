//! Serving the time-tiered, bucketed txid display publication (proof of
//! concept).
//!
//! A display publication is not a history shard set: it has its own map
//! (`txid-shards.json`), its own manifests and its own revisions, and the
//! history loader would refuse every one of them. It is therefore served by a
//! separate process and a separate set of routes under `/v2/txid/`, which
//! reuse the history worker's machinery unchanged: the runtime cache, the
//! shared native parameters, admission, work-memory accounting, metrics and
//! the disk cache.
//!
//! **Roles.** An archive owner holds the sealed archive shards, a recent
//! replica holds the recent shard, and a combined worker holds both, so one
//! host can serve the whole publication. A worker reads every manifest of a
//! publication, so any worker can answer the map and manifest routes, but it
//! verifies, builds and serves only the tables of its own role's revisions. A
//! request for another role's revision is refused with 421, as a history
//! worker refuses an unassigned shard.
//!
//! **The split map.** Besides the full map (`/v2/txid/shards`, kept for
//! clients built before the split), every worker serves the recent map
//! (`/v2/txid/map`) and the archive index chunks
//! (`/v2/txid/map/{base}/{sha256}`) derived from the same map; see
//! `transparent_shard::display::split`.
//!
//! **Tiers in the path.** Data-plane routes name the tier,
//! `/v2/txid/{archive|recent}/...`, so a static edge route can send archive
//! traffic to its owner without knowing which shard ids are sealed. The tier
//! must equal the revision's sealed flag, which is what the map already
//! publishes, so naming it discloses nothing beyond the shard id.
//!
//! **Buckets are tables.** Every bucket of a shard is its own native table,
//! `directory-{b}`, sharing the native parameters of its geometry's
//! `txdirectory` table with every other bucket and shard. Runtimes are cached
//! under `"{digest}/directory-{b}"`, so two buckets of one revision never
//! share a cache entry.

pub mod live;
pub mod manager;
pub mod process;
pub mod service;
