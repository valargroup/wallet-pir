//! Read-only live observation (`observe-live`) against a canonical node.
use super::{require, AnyError};
use crate::zakura::ZakuraClient;
use serde_json::json;
use status_material::protocol::*;
use std::{path::PathBuf, time::Duration};

pub async fn observe_live(
    rpc_url: String,
    cookie: PathBuf,
    salt_hex: String,
    window_blocks: u32,
    samples: usize,
    interval_ms: u64,
    state_dir: Option<PathBuf>,
) -> Result<(), AnyError> {
    require(
        samples > 0 && interval_ms > 0,
        "invalid observation schedule",
    )?;
    let salt: Hash = hex::decode(salt_hex)?
        .try_into()
        .map_err(|_| "salt must be exactly 32 bytes")?;
    let rpc = ZakuraClient::from_cookie_file(rpc_url, cookie)?;
    let mut window = match state_dir {
        Some(path) => crate::source::RollingWindow::open(path, salt, window_blocks)?,
        None => crate::source::RollingWindow::new(salt, window_blocks)?,
    };
    let mut previous = None;
    for sample in 0..samples {
        if sample > 0 {
            tokio::time::sleep(Duration::from_millis(interval_ms)).await;
        }
        let observed = window.observe(&rpc).await?;
        let changed_rows = previous
            .as_ref()
            .map(|old| observed.snapshot.changed_rows(old));
        println!(
            "{}",
            json!({"phase":"live_observation","published":false,"sample":sample,
                "observed_ms":observed.observed_ms,"collect_ms":observed.collect_ms,
                "source_blocks":observed.blocks,"source_mempool":observed.mempool,
                "retained_forks":observed.retained_forks,"changed_rows":changed_rows,
                "anchor_height":observed.snapshot.height,
                "coverage_start":observed.snapshot.start,
                "indexed_entries":observed.snapshot.entries,
                "evicted_blocks":observed.snapshot.evicted_blocks,
                "rows_digest":hex::encode(observed.snapshot.digest)})
        );
        previous = Some(observed.snapshot);
    }
    Ok(())
}
