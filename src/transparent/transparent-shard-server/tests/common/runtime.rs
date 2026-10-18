//! Reuse only verified public preprocessing inside an owned test-run directory.
//! Every caller still opens a fresh service/cache and independent mutable state.
use transparent_shard_server::{
    runtime::disk::DiskCache,
    service::{ServiceConfig, ServiceState},
    shardset::ShardSet,
};

pub fn state(set: ShardSet) -> ServiceState {
    let disk = std::env::var_os("WALLET_PIR_TEST_RUNTIME_CACHE").map(|directory| {
        let mut disk = DiskCache::new(directory.into(), 4 << 30).expect("fixture runtime cache");
        disk.restore_slots = 1;
        disk
    });
    ServiceState::build_with_disk(set, ServiceConfig::default(), disk).expect("fixture state")
}
