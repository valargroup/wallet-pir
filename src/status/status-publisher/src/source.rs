//! Read-only canonical Status observation. It does not publish or persist state.
use super::{
    index::{Block, ForkRecord, Snapshot},
    now_ms,
};
use crate::zakura::{ZakuraClient, ZakuraError};
use status_material::protocol::{Error as StatusError, Hash};
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::Read,
    path::{Path, PathBuf},
    time::Instant,
};

/// The canonical block hash at `height`, or `error` when the node's answer is not a
/// hash.
async fn hash_at(
    rpc: &ZakuraClient,
    height: u64,
    error: impl FnOnce() -> SourceError,
) -> Result<Hash, SourceError> {
    let displayed = rpc.block_hash_display(height).await?;
    zakura_server_chain::hash::parse_display(&displayed).map_err(|_| error())
}

/// The canonical block at `height` as a Status index block; see
/// [`ZakuraClient::status_block`].
pub async fn status_block(rpc: &ZakuraClient, height: u64) -> Result<Block, ZakuraError> {
    let block = rpc.status_block(height).await?;
    Ok(Block {
        height: block.height,
        hash: block.hash,
        parent: block.parent,
        txids: block.txids,
    })
}

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
    forks: Vec<ForkRecord>,
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
            let fork = ForkRecord {
                txid: read_hash(&mut input)?,
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
        forks: &[ForkRecord],
    ) -> Result<(), SourceError> {
        let Some(dir) = &self.dir else {
            return Ok(());
        };
        if self.network == Some(network) && self.blocks == blocks && self.forks == forks {
            return Ok(());
        }
        // Every count is a u32 in the checkpoint; refuse one that is not
        // before anything is written.
        let len = |len: usize| {
            u32::try_from(len)
                .map(u32::to_le_bytes)
                .map_err(|_| SourceError::Checkpoint)
        };
        len(blocks.len())?;
        len(forks.len())?;
        for block in blocks {
            len(block.txids.len())?;
        }
        let len = |n: usize| (n as u32).to_le_bytes();
        pir_control::durable::write_atomic(dir, "source.bin", |file| {
            file.write_all(CHECKPOINT_MAGIC)?;
            file.write_all(&self.window_blocks.to_le_bytes())?;
            file.write_all(&self.salt)?;
            file.write_all(&network)?;
            file.write_all(&len(blocks.len()))?;
            for block in blocks {
                file.write_all(&block.height.to_le_bytes())?;
                file.write_all(&block.hash)?;
                file.write_all(&block.parent)?;
                file.write_all(&len(block.txids.len()))?;
                for txid in &block.txids {
                    file.write_all(txid)?;
                }
            }
            file.write_all(&len(forks.len()))?;
            for fork in forks {
                file.write_all(&fork.txid)?;
                file.write_all(&fork.height.to_le_bytes())?;
                file.write_all(&fork.block)?;
            }
            Ok(())
        })?;
        pir_control::durable::sync_dir(dir)?;
        Ok(())
    }

    pub async fn observe(&mut self, rpc: &ZakuraClient) -> Result<Observation, SourceError> {
        let began = Instant::now();
        let tip = rpc.tip_height().await?;
        let tip_height = u32::try_from(tip).map_err(|_| SourceError::Window)?;
        if tip_height == 0 {
            return Err(SourceError::Window);
        }
        let anchor = hash_at(rpc, tip, || SourceError::TipChanged).await?;
        let genesis = hash_at(rpc, 0, || SourceError::Genesis).await?;
        if self.network.is_some_and(|network| network != genesis) {
            return Err(SourceError::Genesis);
        }

        let mut keep = 0;
        for (index, block) in self.blocks.iter().enumerate().rev() {
            if block.height <= tip_height {
                let canonical =
                    hash_at(rpc, u64::from(block.height), || SourceError::TipChanged).await?;
                if canonical == block.hash {
                    keep = index + 1;
                    break;
                }
            }
        }
        let mut blocks = self.blocks[..keep].to_vec();
        let mut forks = self.forks.clone();
        for disconnected in &self.blocks[keep..] {
            for txid in &disconnected.txids {
                forks.push(ForkRecord {
                    txid: *txid,
                    height: disconnected.height,
                    block: disconnected.hash,
                });
            }
        }
        let mut unique_forks = BTreeMap::<Hash, ForkRecord>::new();
        for fork in forks {
            let replace = unique_forks
                .get(&fork.txid)
                .is_none_or(|old| (fork.height, fork.block) > (old.height, old.block));
            if replace {
                unique_forks.insert(fork.txid, fork);
            }
        }
        let mut forks: Vec<ForkRecord> = unique_forks.into_values().collect();
        if blocks.last().is_none_or(|block| block.height < tip_height) {
            let next = blocks.last().map_or(
                tip_height.saturating_sub(self.window_blocks - 1).max(1),
                |block| block.height + 1,
            );
            for height in next..=tip_height {
                blocks.push(status_block(rpc, u64::from(height)).await?);
            }
        }
        if blocks.len() > self.window_blocks as usize {
            blocks.drain(..blocks.len() - self.window_blocks as usize);
        }
        let mempool = rpc.status_mempool().await?;
        let final_anchor = hash_at(rpc, tip, || SourceError::TipChanged).await?;
        if rpc.tip_height().await? != tip || final_anchor != anchor {
            return Err(SourceError::TipChanged);
        }
        let observed_ms = now_ms();
        let collect_ms = began.elapsed().as_secs_f64() * 1000.;
        let snapshot = Snapshot::build(genesis, self.salt, &blocks, &mempool, &forks)?;
        blocks.retain(|block| block.height >= snapshot.start);
        forks.retain(|fork| fork.height >= snapshot.start);
        let retained_forks = forks.len();
        self.persist(genesis, &blocks, &forks)?;
        self.network = Some(genesis);
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

/// One-shot collection for the diagnostic CLI.
pub async fn observe(
    rpc: &ZakuraClient,
    salt: Hash,
    window_blocks: u32,
) -> Result<Observation, SourceError> {
    RollingWindow::new(salt, window_blocks)?.observe(rpc).await
}

#[cfg(all(test, feature = "fixtures"))]
#[path = "../../../pir-publisher/tests/included/status_source.rs"]
pub(super) mod tests;
