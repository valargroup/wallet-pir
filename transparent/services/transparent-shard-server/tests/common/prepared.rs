//! Bounded, process-local reuse of deterministic publication bytes. Each caller
//! still writes a fresh publication and opens fresh service and wallet stores.
use std::collections::VecDeque;
use std::sync::{Arc, Mutex, OnceLock};
use transparent_events::TransparentEvent;
use transparent_filter::{BlockHash, ScriptBytes};
use transparent_shard::build::{build_directory_shard, Block, BuiltChild, BuiltDirectory};
use transparent_shard::layout::Geometry;

const CACHE_BYTES: usize = 128 << 20;
/// A directory shard and its page shards, as built.
pub type Prepared = (BuiltDirectory, Vec<BuiltChild>);
type Cache = VecDeque<(String, Arc<Prepared>, usize)>;
static CACHE: OnceLock<Mutex<Cache>> = OnceLock::new();

/// Builds the directory shard `id` over `start..=end` with `children` page
/// shards of equal span, the last ending at `end`.
#[allow(clippy::too_many_arguments)]
pub fn prepared_shard(
    id: u64,
    start: u64,
    end: u64,
    genesis: BlockHash,
    hash: &dyn Fn(u64) -> BlockHash,
    profile: &str,
    geometry: &'static Geometry,
    children: u32,
    sealed: bool,
    events: &[(ScriptBytes, TransparentEvent)],
) -> Arc<Prepared> {
    prepared_in(
        CACHE.get_or_init(Default::default),
        id,
        start,
        end,
        genesis,
        hash,
        profile,
        geometry,
        children,
        sealed,
        events,
    )
}

/// The heights `children` equal page shards over `start..=end` end at.
pub fn child_ends(start: u64, end: u64, children: u32) -> Vec<u64> {
    let span = end - start + 1;
    let children = u64::from(children.max(1)).min(span);
    (1..=children)
        .map(|i| start + span * i / children - 1)
        .collect()
}

/// The same, over a given cache: the process-wide one is shared by every test
/// in the binary, which may evict anything between two calls.
#[allow(clippy::too_many_arguments)]
fn prepared_in(
    cache: &Mutex<Cache>,
    id: u64,
    start: u64,
    end: u64,
    genesis: BlockHash,
    hash: &dyn Fn(u64) -> BlockHash,
    profile: &str,
    geometry: &'static Geometry,
    children: u32,
    sealed: bool,
    events: &[(ScriptBytes, TransparentEvent)],
) -> Arc<Prepared> {
    use sha2::{Digest, Sha256};
    let ends = child_ends(start, end, children);
    let terminals: Vec<[u8; 32]> = ends.iter().map(|h| hash(*h).0).collect();
    let key = hex::encode(Sha256::digest(
        serde_json::to_vec(&(
            id,
            start,
            end,
            genesis.0,
            terminals,
            &ends,
            sealed,
            profile,
            format!("{geometry:?}"),
            format!("{events:?}"),
        ))
        .unwrap(),
    ));
    let mut cache = cache.lock().unwrap();
    if let Some(index) = cache.iter().position(|(k, _, _)| k == &key) {
        let item = cache.remove(index).unwrap();
        let result = item.1.clone();
        cache.push_back(item);
        return result;
    }
    let blocks: Vec<Block> = (start..=end)
        .map(|height| {
            (
                height,
                events
                    .iter()
                    .filter(|(_, event)| u64::from(event.height()) == height)
                    .cloned()
                    .collect(),
            )
        })
        .collect();
    let built = Arc::new(
        build_directory_shard(id, geometry, genesis, profile, blocks, &ends, hash, sealed)
            .expect("build"),
    );
    let bytes = built.0.filter.as_slice().len()
        + built.0.directory.iter().map(Vec::len).sum::<usize>()
        + built.1.iter().map(|child| child.pages.len()).sum::<usize>();
    if bytes <= CACHE_BYTES {
        while cache.iter().map(|(_, _, size)| size).sum::<usize>() + bytes > CACHE_BYTES {
            cache.pop_front();
        }
        cache.push_back((key, built.clone(), bytes));
    }
    built
}

#[cfg(test)]
mod tests {
    use super::*;
    use transparent_shard::layout::DECOUPLED_2K_8K;

    #[test]
    fn identical_inputs_reuse_bytes_but_different_anchors_do_not() {
        // Its own cache: other tests in the binary build through the shared
        // one concurrently and may evict this entry between two calls.
        let cache = Mutex::new(Cache::new());
        let anchor = |_| BlockHash::from_internal_bytes([1; 32]);
        let other = |_| BlockHash::from_internal_bytes([2; 32]);
        let build = |hash: &dyn Fn(u64) -> BlockHash| {
            prepared_in(
                &cache,
                0,
                1,
                2,
                BlockHash::from_internal_bytes([1; 32]),
                hash,
                transparent_filter::RANGE_PROFILE,
                &DECOUPLED_2K_8K,
                1,
                true,
                &[],
            )
        };
        let a = build(&anchor);
        let b = build(&anchor);
        assert!(Arc::ptr_eq(&a, &b));
        let c = build(&other);
        assert!(!Arc::ptr_eq(&a, &c));
        // A caller can corrupt its copy without modifying any cached source.
        let mut bytes = a.0.directory[0].clone();
        bytes[0] ^= 1;
        assert_ne!(bytes, b.0.directory[0]);
    }

    #[test]
    fn child_ends_tile_the_range() {
        assert_eq!(child_ends(10, 19, 1), vec![19]);
        assert_eq!(child_ends(10, 19, 2), vec![14, 19]);
        assert_eq!(child_ends(10, 19, 3), vec![12, 15, 19]);
        assert_eq!(child_ends(10, 11, 5), vec![10, 11]);
    }
}
