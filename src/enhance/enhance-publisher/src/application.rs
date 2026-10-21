//! Enhance ingestion application: collect/commit before preparing/activating.
//! Serving and chain decoding remain in their existing owners.
use crate::{
    capacity,
    control::{Group, Ledger, PlacementPolicy, Replica},
    coordinator::Coordinator,
    ingest::EnhanceJournal,
    zakura::{Prevouts, ZakuraClient},
};
use pir_pipeline::{FailurePolicy, Input, Phase};
use serde::Deserialize;
use std::{path::PathBuf, time::Duration};
pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// The two existing source modes. A fixture directory never becomes canonical.
pub enum Source {
    Canonical {
        rpc: ZakuraClient,
        prevouts: Box<Prevouts>,
    },
    Fixture {
        records: u64,
        append_records: u64,
    },
}

/// Polling and fleet policy, independent of CLI parsing.
pub struct Config {
    pub worker_config: PathBuf,
    pub placement_policy: PlacementPolicy,
    pub pool_placement: bool,
    pub pool_policy: Option<PathBuf>,
    pub frontier_replicas: usize,
    pub capacity_policy: capacity::Policy,
    pub poll: Duration,
}

/// Reconcile durable publication state and inventory, then run one collection and
/// publication pass per poll. Inventory failures retain the registered fleet.
pub async fn run(
    config: Config,
    coordinator: Coordinator,
    journal: EnhanceJournal,
    source: Source,
    task: tokio::task::JoinHandle<std::io::Result<()>>,
) -> Result<()> {
    run_until(
        config,
        coordinator,
        journal,
        source,
        task,
        pir_pipeline::service::Shutdown::default(),
    )
    .await
}

pub async fn run_until(
    config: Config,
    coordinator: Coordinator,
    journal: EnhanceJournal,
    source: Source,
    task: tokio::task::JoinHandle<std::io::Result<()>>,
    shutdown: pir_pipeline::service::Shutdown,
) -> Result<()> {
    let poll = config.poll;
    pir_pipeline::supervise(
        &mut Ingestion {
            config,
            coordinator,
            journal,
            source,
            task,
        },
        poll,
        shutdown.cancelled(),
    )
    .await
}

struct Ingestion {
    config: Config,
    coordinator: Coordinator,
    journal: EnhanceJournal,
    source: Source,
    task: tokio::task::JoinHandle<std::io::Result<()>>,
}
impl pir_pipeline::Task for Ingestion {
    type Error = Box<dyn std::error::Error + Send + Sync>;
    async fn run(&mut self) -> Result<()> {
        let Self {
            config,
            coordinator,
            journal,
            source,
            task,
        } = self;
        if let Err(error) =
            pir_pipeline::phase(Phase::Reconcile, coordinator.reconcile(), &mut ()).await
        {
            tracing::error!(%error, "publication recovery blocked");
            return Ok(());
        }
        // Capacity registration is independent of journal advancement. A bad
        // or unavailable addition must not stop serving/publishing on the
        // already registered fleet. The infrastructure writer replaces this
        // file atomically only after provisioning its replica pair.
        let inventory_result = match load_inventory(&config.worker_config) {
            Ok(mut groups) => {
                for group in &mut groups {
                    group.placement_policy = config.placement_policy;
                }
                coordinator.reconcile_inventory(groups).await
            }
            Err(error) => Err(error.to_string()),
        };
        if let Err(error) = inventory_result {
            tracing::error!(%error, "inventory reconciliation deferred");
        }
        if config.pool_placement {
            let policy = config
                .pool_policy
                .as_ref()
                .map(|path| {
                    std::fs::read(path)
                        .map_err(|e| e.to_string())
                        .and_then(|bytes| serde_json::from_slice(&bytes).map_err(|e| e.to_string()))
                })
                .transpose();
            if let Err(error) = policy.and_then(|p| {
                coordinator.configure_pool(config.frontier_replicas, p.unwrap_or_default())
            }) {
                tracing::error!(%error,"pool policy waiting for inventory or durable decisions");
                return Ok(());
            }
        }
        let result = async {
            collect(journal, coordinator, source)
                .await?
                .publish(config.capacity_policy)
                .await
        }
        .await;
        if task.is_finished() {
            return Err("HTTP server stopped".into());
        }
        result
    }
    fn failed(&mut self, error: &Self::Error) -> FailurePolicy {
        if self.task.is_finished() {
            FailurePolicy::Stop
        } else {
            tracing::error!(%error, "ingestion/publication blocked");
            FailurePolicy::NextPoll
        }
    }
}

/// One committed anchor and its actual source, store and publication owner.
/// Borrowing the journal keeps its contents fixed until this pass is published.
pub struct Collected<'a> {
    journal: &'a EnhanceJournal,
    coordinator: &'a Coordinator,
    source: &'a Source,
    height: u64,
    hash: String,
}

/// Revoke before rewinding, then append only verified source blocks. Collection
/// commits independently of preparation, allowing retry/restart without rereading.
pub async fn collect<'a>(
    journal: &'a mut EnhanceJournal,
    coordinator: &'a Coordinator,
    source: &'a Source,
) -> Result<Collected<'a>> {
    match pir_pipeline::acquire(
        async {
            collect_inner(journal, coordinator, source)
                .await
                .map(Input::Work)
        },
        &mut (),
    )
    .await?
    {
        Input::Work(work) => Ok(work),
        Input::Unchanged | Input::Deferred => {
            unreachable!("Enhance collection always commits an anchor")
        }
    }
}

async fn collect_inner<'a>(
    journal: &'a mut EnhanceJournal,
    coordinator: &'a Coordinator,
    source: &'a Source,
) -> Result<Collected<'a>> {
    let (height, hash) = if let Source::Canonical { rpc, prevouts } = source {
        let tip = rpc.tip_height().await?;
        let mut ancestor = None;
        for block in journal.records().blocks().iter().rev() {
            if block.height <= tip && rpc.block_hash_display(block.height).await? == block.hash {
                ancestor = Some((block.height, block.first_position + block.action_count));
                break;
            }
        }
        if journal.committed_height() != ancestor.map(|a| a.0) {
            coordinator
                .revoke_after(ancestor.map_or(0, |a| a.1))
                .await?;
            journal.rewind_to_height(ancestor.map(|a| a.0))?;
        }
        let next = journal
            .committed_height()
            .map_or(enhance_query_server::ACTIVATION_HEIGHT, |h| h + 1);
        for height in next..=tip {
            journal.append_block(&crate::zakura::block(rpc, height, prevouts).await?)?;
        }
        journal
            .highest_committed()
            .ok_or("empty canonical journal")?
    } else {
        let Source::Fixture {
            records: fixture_records,
            append_records: fixture_append_records,
        } = source
        else {
            unreachable!()
        };
        let target = if journal.tree_size() == 0 {
            *fixture_records
        } else {
            journal
                .tree_size()
                .checked_add(*fixture_append_records)
                .ok_or("fixture overflow")?
        };
        if target == 0 || target > 23 * 32768 * 33 {
            return Err("fixture outside fleet coverage".into());
        }
        while journal.tree_size() < target {
            let start = journal.tree_size();
            let count = (target - start).min(4096);
            let records: Vec<_> = (start..start + count)
                .map(|p| {
                    let mut r = vec![0u8; enhance_query_server::RECORD_BYTES];
                    r[..8].copy_from_slice(&p.to_le_bytes());
                    r
                })
                .collect();
            journal.append_fixture_block(&records)?;
        }
        journal.highest_committed().ok_or("empty fixture")?
    };
    Ok(Collected {
        journal,
        coordinator,
        source,
        height,
        hash,
    })
}

impl Collected<'_> {
    /// Observe capacity and prepare/activate through the coordinator's existing
    /// checked publication API, revalidating the canonical anchor before commit.
    pub async fn publish(self, capacity_policy: capacity::Policy) -> Result<()> {
        pir_pipeline::advance(
            Input::Work(self),
            |work| work.prepare(capacity_policy),
            |candidate| candidate.activate(),
            &mut (),
        )
        .await?;
        Ok(())
    }

    async fn prepare(self, capacity_policy: capacity::Policy) -> Result<Input<Self>> {
        let Self {
            journal,
            coordinator,
            height,
            ref hash,
            ..
        } = self;
        coordinator.observe_capacity(
            journal.tree_size(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_secs(),
            capacity_policy,
        )?;
        if coordinator
            .manifest()
            .await
            .is_some_and(|m| m.anchor_height == height && m.anchor_block_hash == *hash)
        {
            coordinator.publish_distribution(journal.records()).await?;
            return Ok(Input::Unchanged);
        }
        Ok(Input::Work(self))
    }

    // Distributed preparation and activation deliberately remain one owner call:
    // the coordinator holds its publication permit and durable operation throughout.
    async fn activate(self) -> Result<()> {
        let Self {
            journal,
            coordinator,
            source,
            height,
            hash,
        } = self;
        let verify_hash = hash.clone();
        coordinator
            .publish_checked(journal.records(), height, hash, || async {
                if let Source::Canonical { rpc, .. } = source {
                    if rpc
                        .block_hash_display(height)
                        .await
                        .map_err(|e| e.to_string())?
                        != verify_hash
                    {
                        return Err("canonical anchor changed during preparation".into());
                    }
                }
                Ok(())
            })
            .await?;
        coordinator.publish_distribution(journal.records()).await?;
        Ok(())
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Inventory {
    #[serde(default)]
    groups: Vec<InventoryGroup>,
    #[serde(default)]
    workers: Vec<InventoryReplica>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct InventoryGroup {
    name: String,
    replicas: Vec<InventoryReplica>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct InventoryReplica {
    name: String,
    url: String,
}

pub fn load_inventory(path: &std::path::Path) -> Result<Vec<Group>> {
    let config: Inventory = serde_json::from_slice(&std::fs::read(path)?)?;
    Ok(config
        .groups
        .into_iter()
        .chain(config.workers.into_iter().map(|r| InventoryGroup {
            name: format!("pool:{}", r.name),
            replicas: vec![r],
        }))
        .enumerate()
        .map(|(sequence, g)| Group {
            placement_policy: Default::default(),
            id: g.name,
            sequence: sequence as u64,
            replicas: g
                .replicas
                .into_iter()
                .map(|r| Replica {
                    name: r.name,
                    url: r.url.trim_end_matches('/').into(),
                    incarnation: String::new(),
                    ledger: Ledger::default(),
                })
                .collect(),
        })
        .collect())
}
