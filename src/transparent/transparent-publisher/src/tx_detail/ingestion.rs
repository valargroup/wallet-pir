//! The display journal as a root-scheduled writer.
//!
//! The root follower observes the node; this writer extracts each block with
//! its display sidecars and commits it. A rollback is refused below the
//! sealed floor (the controller halts), and otherwise withdraws HTTP
//! authority and records the invalidation durably before the journal changes.
//! The controller only reads committed blocks ([`super::source::CommittedSource`]).
use super::controller::RollbackFence;
use super::now_ms;
use super::source::WriterView;
use crate::ingest::blocks::build_fetched_block_events;
use crate::ingest::prevout::{OutputCache, DEFAULT_CACHE_OUTPUTS};
use crate::ingest::zakura::NodeClient;
use pir_pipeline::ingestion::{BlockId, ChainObservation, Ingestion};
use std::sync::{Arc, Mutex};
use tokio::sync::watch;
use transparent_shared::journal::events::{EventStore, EventStoreError};
use zakura_pir_publisher::transparent::filter::BlockHash;

#[derive(Debug, thiserror::Error)]
pub enum IngestError {
    /// The chain forked below the sealed floor; the journal is untouched.
    #[error("reorg ancestor {0} is below the sealed floor {1}")]
    SealedReorg(i128, u64),
    /// The journal could not be written; restart from its checkpoint.
    #[error(transparent)]
    Store(#[from] EventStoreError),
    /// Nothing advanced; retry on a later observation.
    #[error("{0}")]
    Transient(String),
}

fn transient(error: impl std::fmt::Display) -> IngestError {
    IngestError::Transient(error.to_string())
}

pub struct DetailIngestion {
    rpc: NodeClient,
    store: EventStore,
    outputs: OutputCache,
    fence: Arc<RollbackFence>,
    view: Arc<Mutex<WriterView>>,
    notify: watch::Sender<u64>,
}

impl DetailIngestion {
    /// The only writer of `store`; the controller sharing `fence` reads the
    /// committed journal and wakes on `notify`.
    pub fn new(
        rpc: NodeClient,
        store: EventStore,
        fence: Arc<RollbackFence>,
        view: Arc<Mutex<WriterView>>,
        notify: watch::Sender<u64>,
    ) -> Self {
        Self {
            rpc,
            store,
            outputs: OutputCache::new(DEFAULT_CACHE_OUTPUTS),
            fence,
            view,
            notify,
        }
    }

    fn block(&self, height: u64) -> Option<BlockId> {
        Some(BlockId {
            height,
            hash: self.store.block_at(height)?.block_hash.to_display_hex(),
        })
    }

    fn changed(&self) {
        self.notify.send_modify(|n| *n += 1);
    }

    /// Fetches, extracts and commits one block, as the History writer does.
    async fn ingest_one(&mut self, height: u64) -> Result<(), IngestError> {
        let fetched = self.rpc.fetch_block(height).await.map_err(transient)?;
        let parent = BlockHash::from_internal_bytes(fetched.parent()).to_display_hex();
        let expected = height
            .checked_sub(1)
            .and_then(|parent| self.block(parent))
            .ok_or_else(|| transient("missing journal parent"))?
            .hash;
        if parent != expected {
            return Err(transient("node reorganized while fetching block; retry"));
        }
        let built = build_fetched_block_events(&self.rpc, &mut self.outputs, height, fetched, true)
            .await
            .map_err(transient)?;
        if self
            .rpc
            .block_hash_display(height)
            .await
            .map_err(transient)?
            != built.block_hash.to_display_hex()
        {
            return Err(transient("block became noncanonical during extraction"));
        }
        self.store.append_block_with_display(
            height,
            built.block_hash,
            &built.events,
            &built.display,
        )?;
        self.store.commit()?;
        Ok(())
    }
}

impl Ingestion for DetailIngestion {
    type Error = IngestError;
    fn start_height(&self, _tip: u64) -> u64 {
        self.store.start_height()
    }
    /// Halt on another network, and note when each new height was first seen.
    async fn observed(&mut self, observation: &ChainObservation) -> Result<(), IngestError> {
        if observation.genesis != self.store.genesis_hash() {
            // Another network shares no block with the journal: its fork
            // ancestor lies below genesis, which the sealed floor always
            // refuses, so the controller halts and the journal is untouched.
            let (ancestor, floor) = self
                .fence
                .check(None)
                .expect_err("no rollback below genesis passes the sealed floor");
            return Err(IngestError::SealedReorg(ancestor, floor));
        }
        let tip = observation.tip.height;
        let next = self.store.covered_through().map_or(0, |h| h + 1);
        let now = now_ms();
        let mut view = self.view.lock().unwrap();
        view.node_tip = Some(tip);
        for height in next..=tip {
            view.first_seen.entry(height).or_insert(now);
        }
        Ok(())
    }
    fn committed(&self) -> Option<BlockId> {
        self.block(self.store.covered_through()?)
    }
    /// Checkpoints stop at the sealed floor: a fork below it is never searched
    /// past, and reconciliation ends in a refused rollback.
    async fn checkpoint_at_or_below(&self, height: u64) -> Option<BlockId> {
        if height < self.fence.floor() {
            return None;
        }
        self.block(self.store.covered_through()?.min(height))
    }
    async fn withdraw_above(&mut self, keep: Option<&BlockId>) -> Result<(), IngestError> {
        self.fence
            .check(keep.map(|keep| keep.height))
            .map_err(|(ancestor, floor)| IngestError::SealedReorg(ancestor, floor))?;
        let keep = keep.expect("a checked rollback keeps a block");
        let hash = self
            .store
            .block_at(keep.height)
            .ok_or_else(|| transient("missing journal ancestor"))?
            .block_hash;
        self.fence
            .before_rollback(keep.height, hash)
            .map_err(IngestError::Transient)
    }
    async fn rewind(&mut self, keep: Option<&BlockId>) -> Result<(), IngestError> {
        let keep = keep.map(|keep| keep.height);
        self.store.rollback_to(keep)?;
        self.outputs = OutputCache::new(DEFAULT_CACHE_OUTPUTS);
        // Replacement blocks are observed from the next pass, not when the
        // orphans were.
        let above = keep.map_or(0, |h| h + 1);
        self.view.lock().unwrap().first_seen.split_off(&above);
        self.changed();
        Ok(())
    }
    async fn ingest(&mut self, from: u64, through: u64) -> Result<(), IngestError> {
        self.view.lock().unwrap().current = false;
        for height in from..=through {
            self.ingest_one(height).await?;
            self.changed();
        }
        Ok(())
    }
    async fn caught_up(&mut self, _: &ChainObservation) -> Result<(), IngestError> {
        let mut view = self.view.lock().unwrap();
        if !view.current {
            view.current = true;
            drop(view);
            self.changed();
        }
        Ok(())
    }
    fn failed(&mut self, error: &IngestError) -> pir_pipeline::FailurePolicy {
        match error {
            IngestError::Transient(error) => {
                tracing::warn!(%error, "display journal ingestion retries next pass");
                self.view.lock().unwrap().error = Some(error.clone());
                pir_pipeline::FailurePolicy::NextPoll
            }
            // The controller halts on the refused rollback the fence holds,
            // or the service restarts from the journal's checkpoint.
            IngestError::SealedReorg(..) | IngestError::Store(_) => {
                self.changed();
                pir_pipeline::FailurePolicy::Stop
            }
        }
    }
}

#[cfg(test)]
mod tests {
    //! A journal laid down byte by byte in the deployed version-2 layout, not
    //! written by this binary, opened in place by the root-scheduled writer.
    use super::*;
    use pir_pipeline::ingestion::{catch_up, CanonicalChain, ChainFuture, Outcome};
    use std::collections::BTreeMap;
    use std::path::Path;
    use zakura_pir_protocol::transparent::events::{ReceiveEvent, TransparentEvent, Txid};

    const GENESIS: &str = "00000000000000000000000000000000000000000000000000000000000000aa";
    const START: u64 = 100;
    const TIP: u64 = 102;

    fn hash(height: u64) -> BlockHash {
        let mut bytes = [0u8; 32];
        bytes[..8].copy_from_slice(&height.to_le_bytes());
        BlockHash::from_internal_bytes(bytes)
    }

    /// The chain the journal was ingested from.
    struct Canonical;
    impl CanonicalChain for Canonical {
        fn hash_at(&self, height: u64) -> ChainFuture<'_, String> {
            Box::pin(async move { Ok(hash(height).to_display_hex()) })
        }
    }

    /// `meta.json`, `events.bin`, `blocks.bin` and `checkpoint.bin` as a
    /// version-2 writer committed them: one fixed-size receive per block.
    fn write_v2_journal(dir: &Path) {
        std::fs::create_dir_all(dir).unwrap();
        let (mut events, mut blocks) = (vec![], vec![]);
        for height in START..=TIP {
            let script = [0x76, 0xa9, 0x14, height as u8];
            let event = TransparentEvent::Receive(ReceiveEvent {
                metadata: None,
                height: height as u32,
                txid: Txid([height as u8; 32]),
                transaction_index: 0,
                output_index: 0,
                value: 1_000,
                coinbase: false,
            });
            blocks.extend_from_slice(hash(height).internal_bytes());
            blocks.extend_from_slice(&(events.len() as u64).to_le_bytes());
            blocks.extend_from_slice(&1u64.to_le_bytes());
            events.extend_from_slice(&(script.len() as u16).to_le_bytes());
            events.extend_from_slice(&script);
            events.extend_from_slice(&event.to_legacy_bytes());
        }
        let meta = format!(r#"{{"version":2,"genesis_hash":"{GENESIS}","start_height":{START}}}"#);
        std::fs::write(dir.join("meta.json"), meta).unwrap();
        std::fs::write(dir.join("events.bin"), &events).unwrap();
        std::fs::write(dir.join("blocks.bin"), &blocks).unwrap();
        let checkpoint = [events.len() as u64, blocks.len() as u64]
            .into_iter()
            .flat_map(u64::to_le_bytes)
            .collect::<Vec<_>>();
        std::fs::write(dir.join("checkpoint.bin"), checkpoint).unwrap();
    }

    /// Every committed file's bytes; the writer's lock file holds none.
    fn files(dir: &Path) -> BTreeMap<String, Vec<u8>> {
        std::fs::read_dir(dir)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| path.file_name().unwrap() != "writer.lock")
            .map(|path| {
                let name = path.file_name().unwrap().to_string_lossy().into_owned();
                (name, std::fs::read(&path).unwrap())
            })
            .collect()
    }

    fn writer(journal: &Path, root: &Path) -> (DetailIngestion, watch::Receiver<u64>) {
        let cookie = root.join("cookie");
        std::fs::write(&cookie, "fixture:fixture").unwrap();
        // Nothing here may reach the node: an unchanged tip reads no blocks.
        let rpc = crate::ingest::zakura::connect("http://127.0.0.1:9", &cookie).unwrap();
        let store = super::super::source::open_writer(journal).unwrap();
        let fence = Arc::new(RollbackFence::detached(root, START + 1));
        let (notify, updates) = watch::channel(0);
        let ingestion = DetailIngestion::new(rpc, store, fence, Arc::default(), notify);
        (ingestion, updates)
    }

    fn observation(genesis: &str, pass: u64) -> ChainObservation {
        ChainObservation {
            genesis: genesis.into(),
            tip: BlockId {
                height: TIP,
                hash: hash(TIP).to_display_hex(),
            },
            pass,
        }
    }

    #[tokio::test]
    async fn an_existing_version_two_journal_resumes_in_place_at_an_unchanged_tip() {
        let temp = tempfile::tempdir().unwrap();
        let (journal, root) = (temp.path().join("journal"), temp.path().join("root"));
        std::fs::create_dir_all(&root).unwrap();
        write_v2_journal(&journal);
        let before = files(&journal);

        let (mut ingestion, updates) = writer(&journal, &root);
        assert_eq!(ingestion.committed(), Some(observation(GENESIS, 0).tip));
        assert_eq!(ingestion.start_height(TIP), START);
        // The journal has one writer.
        assert!(super::super::source::open_writer(&journal).is_err());
        for pass in 1..=2 {
            catch_up(&mut ingestion, &Canonical, &observation(GENESIS, pass))
                .await
                .unwrap();
        }
        assert!(ingestion.view.lock().unwrap().current);
        assert_eq!(*updates.borrow(), 1, "only becoming current is announced");
        assert_eq!(files(&journal), before, "no committed byte was rewritten");

        // Another network halts under the sealed floor; the journal is untouched.
        let refused = catch_up(
            &mut ingestion,
            &Canonical,
            &observation(&"bb".repeat(32), 3),
        )
        .await;
        assert!(matches!(
            refused,
            Err(Outcome::Writer(IngestError::SealedReorg(-1, floor))) if floor == START + 1
        ));
        assert_eq!(ingestion.fence.sealed(), Some((-1, START + 1)));
        drop(ingestion);
        assert_eq!(files(&journal), before);

        // A restart reopens the same journal with its version and identity.
        let (mut reopened, _) = writer(&journal, &root);
        assert_eq!(reopened.store.version(), 2);
        assert_eq!(reopened.store.genesis_hash(), GENESIS);
        catch_up(&mut reopened, &Canonical, &observation(GENESIS, 1))
            .await
            .unwrap();
        assert_eq!(files(&journal), before);
    }
}
