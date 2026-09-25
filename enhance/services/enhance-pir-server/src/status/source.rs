//! Read-only canonical Status observation. It does not publish or persist state.
use super::{
    index::{Block, Snapshot},
    now_ms,
};
use crate::zakura::{ZakuraClient, ZakuraError};
use enhance_pir::status::{Error as StatusError, Hash, Record};
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    time::Instant,
};

#[derive(Debug, thiserror::Error)]
pub enum SourceError {
    #[error(transparent)]
    Rpc(#[from] ZakuraError),
    #[error(transparent)]
    Index(#[from] StatusError),
    #[error("canonical tip changed during Status collection")]
    TipChanged,
    #[error("invalid Status observation window")]
    Window,
    #[error("Status index salt must be nonzero")]
    Salt,
    #[error("invalid network genesis hash")]
    Genesis,
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("corrupt or incompatible Status source checkpoint")]
    Checkpoint,
}

const MAX_INSPECTION_BLOCKS: u32 = 4_096;
const CHECKPOINT_MAGIC: &[u8; 8] = b"STSOBS01";
const MAX_CHECKPOINT_TXIDS: usize = 4_000_000;

pub struct Observation {
    pub snapshot: Snapshot,
    pub observed_ms: u64,
    pub collect_ms: f64,
    pub blocks: usize,
    pub mempool: usize,
    pub retained_forks: usize,
}

/// A bounded in-memory source cache for repeated read-only observations. It
/// detects disconnected blocks and retains their txids as fork observations.
/// Durability and serving authority still belong to a future controller.
pub struct RollingWindow {
    network: Option<Hash>,
    salt: Hash,
    window_blocks: u32,
    blocks: Vec<Block>,
    forks: Vec<Record>,
    dir: Option<PathBuf>,
    _lock: Option<File>,
}

impl RollingWindow {
    pub fn new(salt: Hash, window_blocks: u32) -> Result<Self, SourceError> {
        if window_blocks == 0 || window_blocks > MAX_INSPECTION_BLOCKS {
            return Err(SourceError::Window);
        }
        if salt == [0; 32] {
            return Err(SourceError::Salt);
        }
        Ok(Self {
            network: None,
            salt,
            window_blocks,
            blocks: Vec::new(),
            forks: Vec::new(),
            dir: None,
            _lock: None,
        })
    }

    /// Recover cached blocks and fork observations. The next `observe` still
    /// checks canonical hashes and collects a fresh mempool before use.
    pub fn open(
        path: impl AsRef<Path>,
        salt: Hash,
        window_blocks: u32,
    ) -> Result<Self, SourceError> {
        let mut window = Self::new(salt, window_blocks)?;
        fs::create_dir_all(path.as_ref())?;
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path.as_ref().join("source.lock"))?;
        lock.try_lock()
            .map_err(|e| std::io::Error::other(e.to_string()))?;
        let checkpoint = path.as_ref().join("source.bin");
        if checkpoint.exists() {
            window.load_checkpoint(&checkpoint)?;
        }
        window.dir = Some(path.as_ref().to_path_buf());
        window._lock = Some(lock);
        Ok(window)
    }

    fn load_checkpoint(&mut self, path: &Path) -> Result<(), SourceError> {
        let mut input = File::open(path)?;
        let mut magic = [0; 8];
        input
            .read_exact(&mut magic)
            .map_err(|_| SourceError::Checkpoint)?;
        if &magic != CHECKPOINT_MAGIC
            || read_u32(&mut input)? != self.window_blocks
            || read_hash(&mut input)? != self.salt
        {
            return Err(SourceError::Checkpoint);
        }
        let network = read_hash(&mut input)?;
        let count = read_u32(&mut input)? as usize;
        if count > self.window_blocks as usize {
            return Err(SourceError::Checkpoint);
        }
        let mut blocks = Vec::with_capacity(count);
        let mut total_txids = 0usize;
        for _ in 0..count {
            let height = read_u32(&mut input)?;
            let hash = read_hash(&mut input)?;
            let parent = read_hash(&mut input)?;
            let tx_count = read_u32(&mut input)? as usize;
            total_txids = total_txids
                .checked_add(tx_count)
                .ok_or(SourceError::Checkpoint)?;
            if total_txids > MAX_CHECKPOINT_TXIDS
                || height == 0
                || hash == [0; 32]
                || blocks.last().is_some_and(|old: &Block| {
                    old.height.checked_add(1) != Some(height) || old.hash != parent
                })
            {
                return Err(SourceError::Checkpoint);
            }
            let mut txids = Vec::with_capacity(tx_count);
            for _ in 0..tx_count {
                txids.push(read_hash(&mut input)?);
            }
            blocks.push(Block {
                height,
                hash,
                parent,
                txids,
            });
        }
        let fork_count = read_u32(&mut input)? as usize;
        if fork_count > MAX_CHECKPOINT_TXIDS {
            return Err(SourceError::Checkpoint);
        }
        let mut forks = Vec::with_capacity(fork_count);
        for _ in 0..fork_count {
            let fork = Record {
                txid: read_hash(&mut input)?,
                tag: 3,
                height: read_u32(&mut input)?,
                block: read_hash(&mut input)?,
            };
            fork.validate().map_err(|_| SourceError::Checkpoint)?;
            forks.push(fork);
        }
        let mut trailing = [0];
        if input.read(&mut trailing)? != 0
            || (network == [0; 32] && (count != 0 || fork_count != 0))
        {
            return Err(SourceError::Checkpoint);
        }
        self.network = (network != [0; 32]).then_some(network);
        self.blocks = blocks;
        self.forks = forks;
        Ok(())
    }

    fn persist(
        &self,
        network: Hash,
        blocks: &[Block],
        forks: &[Record],
    ) -> Result<(), SourceError> {
        let Some(dir) = &self.dir else {
            return Ok(());
        };
        if self.network == Some(network) && self.blocks == blocks && self.forks == forks {
            return Ok(());
        }
        let temp = dir.join("source.bin.tmp");
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&temp)?;
        file.write_all(CHECKPOINT_MAGIC)?;
        file.write_all(&self.window_blocks.to_le_bytes())?;
        file.write_all(&self.salt)?;
        file.write_all(&network)?;
        write_len(&mut file, blocks.len())?;
        for block in blocks {
            file.write_all(&block.height.to_le_bytes())?;
            file.write_all(&block.hash)?;
            file.write_all(&block.parent)?;
            write_len(&mut file, block.txids.len())?;
            for txid in &block.txids {
                file.write_all(txid)?;
            }
        }
        write_len(&mut file, forks.len())?;
        for fork in forks {
            file.write_all(&fork.txid)?;
            file.write_all(&fork.height.to_le_bytes())?;
            file.write_all(&fork.block)?;
        }
        file.sync_all()?;
        fs::rename(temp, dir.join("source.bin"))?;
        File::open(dir)?.sync_all()?;
        Ok(())
    }

    pub async fn observe(&mut self, rpc: &ZakuraClient) -> Result<Observation, SourceError> {
        let began = Instant::now();
        let tip = rpc.tip_height().await?;
        let tip_height = u32::try_from(tip).map_err(|_| SourceError::Window)?;
        if tip_height == 0 {
            return Err(SourceError::Window);
        }
        let anchor: zakura_chain::block::Hash = rpc
            .block_hash(tip)
            .await?
            .parse()
            .map_err(|_| SourceError::TipChanged)?;
        let genesis: zakura_chain::block::Hash = rpc
            .block_hash(0)
            .await?
            .parse()
            .map_err(|_| SourceError::Genesis)?;
        if self.network.is_some_and(|network| network != genesis.0) {
            return Err(SourceError::Genesis);
        }

        let mut keep = 0;
        for (index, block) in self.blocks.iter().enumerate().rev() {
            if block.height <= tip_height {
                let canonical: zakura_chain::block::Hash = rpc
                    .block_hash(u64::from(block.height))
                    .await?
                    .parse()
                    .map_err(|_| SourceError::TipChanged)?;
                if canonical.0 == block.hash {
                    keep = index + 1;
                    break;
                }
            }
        }
        let mut blocks = self.blocks[..keep].to_vec();
        let mut forks = self.forks.clone();
        for disconnected in &self.blocks[keep..] {
            for txid in &disconnected.txids {
                forks.push(Record {
                    txid: *txid,
                    tag: 3,
                    height: disconnected.height,
                    block: disconnected.hash,
                });
            }
        }
        let mut unique_forks = BTreeMap::<Hash, Record>::new();
        for fork in forks {
            let replace = unique_forks
                .get(&fork.txid)
                .is_none_or(|old| (fork.height, fork.block) > (old.height, old.block));
            if replace {
                unique_forks.insert(fork.txid, fork);
            }
        }
        let mut forks: Vec<Record> = unique_forks.into_values().collect();
        if blocks.last().is_none_or(|block| block.height < tip_height) {
            let next = blocks.last().map_or(
                tip_height.saturating_sub(self.window_blocks - 1).max(1),
                |block| block.height + 1,
            );
            for height in next..=tip_height {
                blocks.push(rpc.status_block(u64::from(height)).await?);
            }
        }
        if blocks.len() > self.window_blocks as usize {
            blocks.drain(..blocks.len() - self.window_blocks as usize);
        }
        let mempool = rpc.status_mempool().await?;
        let final_anchor: zakura_chain::block::Hash = rpc
            .block_hash(tip)
            .await?
            .parse()
            .map_err(|_| SourceError::TipChanged)?;
        if rpc.tip_height().await? != tip || final_anchor != anchor {
            return Err(SourceError::TipChanged);
        }
        let observed_ms = now_ms();
        let collect_ms = began.elapsed().as_secs_f64() * 1000.;
        let snapshot = Snapshot::build(genesis.0, self.salt, &blocks, &mempool, &forks)?;
        blocks.retain(|block| block.height >= snapshot.start);
        forks.retain(|fork| fork.height >= snapshot.start);
        let retained_forks = forks.len();
        self.persist(genesis.0, &blocks, &forks)?;
        self.network = Some(genesis.0);
        self.blocks = blocks;
        self.forks = forks;
        Ok(Observation {
            snapshot,
            observed_ms,
            collect_ms,
            blocks: self.blocks.len(),
            mempool: mempool.len(),
            retained_forks,
        })
    }
}

fn read_u32(input: &mut File) -> Result<u32, SourceError> {
    let mut bytes = [0; 4];
    input
        .read_exact(&mut bytes)
        .map_err(|_| SourceError::Checkpoint)?;
    Ok(u32::from_le_bytes(bytes))
}

fn read_hash(input: &mut File) -> Result<Hash, SourceError> {
    let mut hash = [0; 32];
    input
        .read_exact(&mut hash)
        .map_err(|_| SourceError::Checkpoint)?;
    Ok(hash)
}

fn write_len(file: &mut File, len: usize) -> Result<(), SourceError> {
    let len = u32::try_from(len).map_err(|_| SourceError::Checkpoint)?;
    file.write_all(&len.to_le_bytes())?;
    Ok(())
}

/// One-shot collection for the diagnostic CLI.
pub async fn observe(
    rpc: &ZakuraClient,
    salt: Hash,
    window_blocks: u32,
) -> Result<Observation, SourceError> {
    RollingWindow::new(salt, window_blocks)?.observe(rpc).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{extract::State, routing::post, Json, Router};
    use serde_json::{json, Value};
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    use zakura_chain::serialization::ZcashSerialize;

    #[derive(Clone)]
    struct Mock {
        block_hex: String,
        hash: String,
        mempool_id: String,
        tip_reads: Arc<AtomicUsize>,
        move_tip: bool,
        bad_block_hash: bool,
    }

    async fn rpc(State(mock): State<Mock>, Json(input): Json<Value>) -> Json<Value> {
        let result = match input["method"].as_str().unwrap() {
            "getblockcount" => {
                let n = mock.tip_reads.fetch_add(1, Ordering::SeqCst);
                json!(if mock.move_tip && n > 0 { 2 } else { 1 })
            }
            "getblockhash" => {
                if mock.bad_block_hash && input["params"][0].as_u64() == Some(1) {
                    json!("00".repeat(32))
                } else {
                    json!(mock.hash)
                }
            }
            "getblock" => json!(mock.block_hex),
            "getrawmempool" => json!([mock.mempool_id]),
            method => panic!("unexpected RPC {method}"),
        };
        Json(json!({"result":result,"error":null,"id":"enhance-pir"}))
    }

    async fn fixture(
        move_tip: bool,
        bad_block_hash: bool,
    ) -> (ZakuraClient, tokio::task::JoinHandle<()>) {
        let block = zakura_chain::block::genesis::regtest_genesis_block();
        let mut raw = Vec::new();
        block.zcash_serialize(&mut raw).unwrap();
        let mock = Mock {
            block_hex: hex::encode(raw),
            hash: block.hash().to_string(),
            mempool_id: block.transactions[0].hash().to_string(),
            tip_reads: Arc::new(AtomicUsize::new(0)),
            move_tip,
            bad_block_hash,
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let job = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new().route("/", post(rpc)).with_state(mock),
            )
            .await
            .unwrap();
        });
        let dir = tempfile::tempdir().unwrap();
        let cookie = dir.path().join("cookie");
        std::fs::write(&cookie, "user:password").unwrap();
        let client = ZakuraClient::from_cookie_file(format!("http://{addr}"), cookie).unwrap();
        (client, job)
    }

    #[tokio::test]
    async fn canonical_observation_indexes_every_transaction_type() {
        let (client, job) = fixture(false, false).await;
        let expected = zakura_chain::block::genesis::regtest_genesis_block().transactions[0]
            .hash()
            .0;
        assert_eq!(client.status_mempool().await.unwrap(), vec![expected]);
        let observed = observe(&client, [1; 32], 1).await.unwrap();
        assert_eq!(observed.snapshot.height, 1);
        assert_eq!(observed.snapshot.start, 1);
        assert_eq!(observed.blocks, 1);
        assert_eq!(observed.mempool, 1);
        assert!(observed.snapshot.entries > 0);
        job.abort();
    }

    #[tokio::test]
    async fn checkpoint_recovers_only_as_a_cache_and_reobserves_canonical_source() {
        let dir = tempfile::tempdir().unwrap();
        let (client, job) = fixture(false, false).await;
        let first_digest;
        {
            let mut window = RollingWindow::open(dir.path(), [1; 32], 1).unwrap();
            first_digest = window.observe(&client).await.unwrap().snapshot.digest;
            assert!(dir.path().join("source.bin").exists());
        }
        let mut recovered = RollingWindow::open(dir.path(), [1; 32], 1).unwrap();
        assert_eq!(recovered.blocks.len(), 1);
        assert!(recovered.network.is_some());
        // No observation timestamp or served generation is recovered.
        assert_eq!(
            recovered.observe(&client).await.unwrap().snapshot.digest,
            first_digest
        );
        job.abort();
    }

    #[tokio::test]
    async fn live_rpc_candidate_revalidates_before_durable_publication() {
        use crate::status::{authority::Authority, Controller, Generation};
        use ipir_sp::server::MatvecBackend;

        let dir = tempfile::tempdir().unwrap();
        let (client, job) = fixture(false, false).await;
        let mut source = RollingWindow::open(dir.path().join("source"), [1; 32], 1).unwrap();
        let observed = source.observe(&client).await.unwrap();
        let mut authority = Authority::open(
            dir.path().join("authority"),
            observed.snapshot.network,
            [1; 32],
        )
        .unwrap();
        let (number, epoch) = authority.next_identity().unwrap();
        let (mut candidate, _) = Generation::prepare(
            &observed.snapshot,
            number,
            epoch,
            observed.observed_ms,
            None,
            MatvecBackend::Cpu,
        )
        .unwrap();
        let verified = source.observe(&client).await.unwrap();
        assert_eq!(verified.snapshot.digest, candidate.manifest.rows_digest);
        assert_eq!(verified.snapshot.anchor, candidate.manifest.anchor_hash);
        candidate.manifest.observed_ms = verified.observed_ms;
        authority.commit(&candidate.manifest).unwrap();
        let controller = Controller::new(candidate);
        assert_eq!(controller.current().manifest.generation, 1);
        assert_eq!(controller.current().manifest.recovery_epoch, 1);
        drop(authority);
        let restarted = Authority::open(
            dir.path().join("authority"),
            verified.snapshot.network,
            [1; 32],
        )
        .unwrap();
        assert_eq!(restarted.next_identity().unwrap(), (2, 2));
        job.abort();
    }

    #[test]
    fn damaged_or_incompatible_checkpoint_fails_closed() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("source.bin"), b"incomplete").unwrap();
        assert!(matches!(
            RollingWindow::open(dir.path(), [1; 32], 1),
            Err(SourceError::Checkpoint)
        ));
    }

    #[tokio::test]
    async fn changing_tip_discards_candidate() {
        let (client, job) = fixture(true, false).await;
        let mut window = RollingWindow::new([1; 32], 1).unwrap();
        window.blocks.push(Block {
            height: 1,
            hash: [9; 32],
            parent: [0; 32],
            txids: vec![[8; 32]],
        });
        assert!(matches!(
            window.observe(&client).await,
            Err(SourceError::TipChanged)
        ));
        assert_eq!(window.blocks[0].hash, [9; 32]);
        assert!(window.forks.is_empty());
        job.abort();
    }

    #[tokio::test]
    async fn disconnected_block_becomes_fork_observation() {
        let (client, job) = fixture(false, false).await;
        let mut window = RollingWindow::new([1; 32], 1).unwrap();
        window.blocks.push(Block {
            height: 1,
            hash: [9; 32],
            parent: [0; 32],
            txids: vec![[8; 32]],
        });
        window.forks.push(Record {
            txid: [8; 32],
            tag: 3,
            height: 1,
            block: [7; 32],
        });
        let observed = window.observe(&client).await.unwrap();
        assert_eq!(observed.retained_forks, 1);
        assert_eq!(window.forks[0].txid, [8; 32]);
        assert_eq!(window.forks[0].block, [9; 32]);
        assert_eq!(observed.snapshot.entries, 2);
        job.abort();
    }

    #[tokio::test]
    async fn noncanonical_raw_block_is_rejected() {
        let (client, job) = fixture(false, true).await;
        assert!(matches!(
            observe(&client, [1; 32], 1).await,
            Err(SourceError::Rpc(_))
        ));
        job.abort();
    }
}
