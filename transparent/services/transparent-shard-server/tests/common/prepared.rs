//! Bounded, process-local reuse of deterministic publication bytes. Each caller
//! still writes a fresh publication and opens fresh service and wallet stores.
use std::collections::VecDeque;
use std::sync::{Arc, Mutex, OnceLock};
use transparent_events::TransparentEvent;
use transparent_filter::{BlockHash, ScriptBytes};
use transparent_shard::build::{build_shard, BuiltShard};
use transparent_shard::layout::Geometry;

const CACHE_BYTES: usize = 128 << 20;
type Cache = VecDeque<(String, Arc<BuiltShard>, usize)>;
static CACHE: OnceLock<Mutex<Cache>> = OnceLock::new();

#[allow(clippy::too_many_arguments)]
pub fn prepared_shard(
    id: u64,
    start: u64,
    end: u64,
    genesis: BlockHash,
    terminal: BlockHash,
    profile: &str,
    geometry: &'static Geometry,
    events: &[(ScriptBytes, TransparentEvent)],
) -> Arc<BuiltShard> {
    use sha2::{Digest, Sha256};
    let key = hex::encode(Sha256::digest(
        serde_json::to_vec(&(
            id,
            start,
            end,
            genesis.0,
            terminal.0,
            profile,
            format!("{geometry:?}"),
            format!("{events:?}"),
        ))
        .unwrap(),
    ));
    let mut cache = CACHE.get_or_init(Default::default).lock().unwrap();
    if let Some(index) = cache.iter().position(|(k, _, _)| k == &key) {
        let item = cache.remove(index).unwrap();
        let result = item.1.clone();
        cache.push_back(item);
        return result;
    }
    let built = Arc::new(
        build_shard(id, start, end, genesis, terminal, profile, geometry, events).expect("build"),
    );
    let bytes = built.filter.as_slice().len()
        + built.directory.iter().map(Vec::len).sum::<usize>()
        + built.pages.iter().map(Vec::len).sum::<usize>();
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
    use transparent_shard::layout::RECENT_4K;

    #[test]
    fn identical_inputs_reuse_bytes_but_different_anchors_do_not() {
        let anchor = BlockHash::from_internal_bytes([1; 32]);
        let a = prepared_shard(
            0,
            1,
            2,
            anchor,
            anchor,
            transparent_filter::RANGE_PROFILE,
            &RECENT_4K,
            &[],
        );
        let b = prepared_shard(
            0,
            1,
            2,
            anchor,
            anchor,
            transparent_filter::RANGE_PROFILE,
            &RECENT_4K,
            &[],
        );
        assert!(Arc::ptr_eq(&a, &b));
        let c = prepared_shard(
            0,
            1,
            2,
            anchor,
            BlockHash::from_internal_bytes([2; 32]),
            transparent_filter::RANGE_PROFILE,
            &RECENT_4K,
            &[],
        );
        assert!(!Arc::ptr_eq(&a, &c));
        // A caller can corrupt its copy without modifying any cached source.
        let mut bytes = a.directory[0].clone();
        bytes[0] ^= 1;
        assert_ne!(bytes, b.directory[0]);
    }
}
