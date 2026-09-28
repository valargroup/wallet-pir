//! Compare full and cached witness preparation on an ISOLATED copy of a public index.
//! This rewinds and re-appends the final block. Never point it at a serving database.
use receiver_directory::{
    store::{Config, IndexedBlock, Store},
    witness::WitnessCache,
    Hash, Record,
};
use rusqlite::{Connection, OpenFlags};
use sha2::{Digest, Sha256};
use std::{path::PathBuf, time::Instant};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
fn main() -> Result<()> {
    let path = PathBuf::from(
        std::env::args()
            .nth(1)
            .ok_or("expected isolated database path")?,
    );
    if std::env::args().nth(2).as_deref() != Some("--isolated-copy") {
        return Err("explicit --isolated-copy acknowledgement required".into());
    }
    let db = Connection::open_with_flags(&path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    let config: Config = serde_json::from_str(&db.query_row(
        "SELECT value FROM config WHERE id=1",
        [],
        |r| r.get::<_, String>(0),
    )?)?;
    let mut store = Store::open(&path, config)?;
    let tip = store.tip()?;
    let parent = store.checkpoint(tip.height - 1)?;
    let commitments = db
        .prepare("SELECT cmx FROM commitments WHERE height=?1 ORDER BY position")?
        .query_map([tip.height], |r| r.get::<_, Vec<u8>>(0))?
        .map(|r| -> Result<Hash> { Ok(r?.try_into().map_err(|_| "invalid commitment")?) })
        .collect::<Result<Vec<_>>>()?;
    let payments = db
        .prepare("SELECT record FROM payments WHERE height=?1 ORDER BY position")?
        .query_map([tip.height], |r| r.get::<_, Vec<u8>>(0))?
        .map(|r| -> Result<_> {
            let r = Record::decode(&r?)?.ok_or("missing record")?;
            Ok((r.receiver, r.payment))
        })
        .collect::<Result<Vec<_>>>()?;
    let block = IndexedBlock {
        height: tip.height,
        hash: tip.hash,
        parent: parent.hash,
        start_position: parent.position,
        end_position: tip.position,
        coinbase_actions: db.query_row(
            "SELECT coinbase_actions FROM blocks WHERE height=?1",
            [tip.height],
            |r| r.get(0),
        )?,
        payments,
        commitments,
    };
    drop(db);
    for trial in 0..3 {
        store.rewind(parent.height, parent.hash)?;
        let prior = store.snapshot(8192)?;
        let mut cache = WitnessCache::default();
        let cold = Instant::now();
        store.witnesses_cached(&prior.manifest, &mut cache)?;
        let cold_us = cold.elapsed().as_micros();
        let ingest = Instant::now();
        store.append(&block)?;
        let ingest_us = ingest.elapsed().as_micros();
        let directory = Instant::now();
        let current = store.snapshot(8192)?;
        let directory_us = directory.elapsed().as_micros();
        let mut full = Vec::new();
        let mut cached = Vec::new();
        let mut full_us = 0;
        let mut cached_us = 0;
        // Alternate order to avoid attributing SQLite/page-cache warming to tree reuse.
        for cached_first in [trial % 2 == 0, trial % 2 != 0] {
            let start = Instant::now();
            if cached_first {
                cached = store
                    .witnesses_cached(&current.manifest, &mut cache)?
                    .encode();
                cached_us = start.elapsed().as_micros();
            } else {
                full = store.witnesses(&current.manifest)?.encode();
                full_us = start.elapsed().as_micros();
            }
        }
        assert_eq!(cached, full, "publication bytes changed");
        println!(
            "{}",
            serde_json::json!({
                "trial":trial, "height":tip.height, "block_hash":hex::encode(tip.hash),
                "commitments":tip.position, "new_commitments":block.commitments.len(),
                "records":current.manifest.records, "revision":hex::encode(current.manifest.revision()?),
                "cold_cache_us":cold_us, "append_us":ingest_us, "directory_us":directory_us,
                "full_witness_us":full_us, "cached_witness_us":cached_us,
                "witness_bytes":full.len(), "witness_sha256":hex::encode(Sha256::digest(&full)),
                "equal":true
            })
        );
    }
    Ok(())
}
