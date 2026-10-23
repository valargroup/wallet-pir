//! `receiver-directory serve-publication --config FILE`: serve receiver directory
//! publications an operator stages, without indexing.
//!
//! The operator stages a complete publication directory, in the files a one-shot
//! indexer run writes: a `<layout>.inventory.json` naming every shard's segments and
//! filter, each segment's `<revision>.json` and `.rows` and its companion's own
//! `<witness revision>.witness.json` and `.witness`,
//! and each sealed shard's `<digest>.filter`. It then atomically replaces a small
//! record naming the layout: `{"directory": "/abs/path", "revision": "<layout hex>"}`.
//! The record is a private, local control input: there is no update route. This mode
//! opens no index database, ingests no blocks and reads no partner key.
//!
//! Each record is loaded and verified in full before it can be served: the layout must
//! digest to the expected identity and be for mainnet, every payment and witness
//! manifest must hold its listed revision and commit to its `.rows` or `.witness` file
//! beside it, which the Receiver server library re-validates while preparing PIR,
//! every filter must hold its digest, and the shards must form one contiguous
//! inventory from Ironwood activation. Its anchor must then be canonical on the
//! freshest node. Throughout, an independent guard checks every served anchor and
//! revokes on a confirmed reorg, as the indexer's serving mode does. [`Publications`]
//! owns epochs, monotonic coverage and retention of displaced inventories.
//!
//! Updates are serial. A newer record supersedes one still preparing; a record that
//! does not verify is refused and the valid publication already served stays.
//! A validated higher history revision revokes old answers before loading its rows,
//! even if replacement preparation fails, and is recorded in the state directory's
//! [`HISTORY_FILE`] first, so a restart never serves older, uncorrected history again
//! even if the record still names it. A restart reads the record again and verifies
//! everything before serving: the status file in the state directory is a report for
//! operators, never an input.
//!
//! The publisher remains trusted for completeness: verification proves the files are
//! the publication the record names and that it is internally consistent and anchored
//! on the canonical chain, not that it holds every payment.
use crate::application::Layout;
use crate::canonical::{freshest, tree_mismatch, TreeAnchor, TreeMismatch};
use crate::revision::Refused;
use receiver_directory::Hash;
use receiver_directory::{
    shards::{Coverage, ShardFilter, MAX_FILTER_BYTES, MAX_RECEIVERS},
    snapshot::{Manifest, Snapshot, ROW_BYTES},
    witness,
};
use receiver_pir_server::{
    tables::{FilterSource, Loaded},
    Publication, Publications,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    io::{Read, Write},
    net::{IpAddr, SocketAddr},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};
use tracing::{info, warn};
use zakura_server_chain::Network;
use zakura_server_chain::NodeClient as ZakuraClient;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// How often the publication record is read for a replacement.
const RECORD_POLL: Duration = Duration::from_secs(1);
/// The largest publication record read.
const MAX_RECORD_BYTES: u64 = 4096;
/// The largest configuration file read.
const MAX_CONFIG_BYTES: u64 = 64 * 1024;
/// The report this mode writes into its state directory.
pub const STATUS_FILE: &str = "serve-publication.json";
/// The highest history revision this service has accepted, in its state directory: a
/// record below it is refused. Deleting it is an explicit operator rollback.
pub const HISTORY_FILE: &str = "history-revision.json";

#[derive(clap::Parser)]
#[command(name = "receiver-directory serve-publication")]
pub struct Args {
    /// The JSON configuration; see [`Config`].
    #[arg(long)]
    pub config: PathBuf,
    /// Validate the configuration and the publication the record names, as the
    /// service would before serving, then exit without serving or activating.
    #[arg(long)]
    pub check: bool,
}

/// The operator's configuration. Every path is absolute; the record and its
/// publication are read-only inputs, and the state directory is the only path
/// written.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// The operator-owned record naming the publication to serve.
    pub publication_record: PathBuf,
    /// The private listener: loopback, private IPv4 or unique-local IPv6.
    pub bind: SocketAddr,
    /// Writable worker state: only the status report is written there.
    pub state_dir: PathBuf,
    /// Node RPC endpoints, each checked for mainnet before it is used.
    pub rpc_urls: Vec<String>,
    /// The node's RPC cookie file, read on every request.
    #[serde(default)]
    pub cookie: Option<PathBuf>,
    /// Set instead of `cookie` for a node whose RPC needs no authentication.
    #[serde(default)]
    pub no_auth: bool,
    /// How often served anchors are checked against the node, in seconds.
    #[serde(default = "default_poll_seconds")]
    pub poll_seconds: u64,
    /// The fewest rows a served publication may have.
    #[serde(default = "default_min_rows")]
    pub min_rows: u32,
}

fn default_poll_seconds() -> u64 {
    10
}

fn default_min_rows() -> u32 {
    receiver_pir::MIN_ROWS
}

impl Config {
    /// Reads and checks a configuration file.
    pub fn load(path: &Path) -> Result<Self> {
        let bytes = read_bounded(path, MAX_CONFIG_BYTES)?;
        let config: Self = serde_json::from_slice(&bytes)?;
        config.check()?;
        Ok(config)
    }

    fn check(&self) -> Result<()> {
        for (name, path) in [
            ("publication_record", &self.publication_record),
            ("state_dir", &self.state_dir),
        ] {
            if !path.is_absolute() {
                return Err(format!("{name} must be an absolute path").into());
            }
        }
        if self.publication_record.starts_with(&self.state_dir) {
            return Err("the publication record is an input; keep it out of state_dir".into());
        }
        if self.rpc_urls.is_empty() {
            return Err("rpc_urls names no node".into());
        }
        if self.cookie.is_some() == self.no_auth {
            return Err("set exactly one of cookie and no_auth".into());
        }
        if self
            .cookie
            .as_deref()
            .is_some_and(|cookie| !cookie.is_absolute())
        {
            return Err("cookie must be an absolute path".into());
        }
        if self.poll_seconds == 0 {
            return Err("poll_seconds must be at least 1".into());
        }
        receiver_pir::validate_rows(self.min_rows)?;
        let private = match self.bind.ip() {
            IpAddr::V4(ip) => ip.is_loopback() || ip.is_private(),
            IpAddr::V6(ip) => ip.is_loopback() || ip.is_unique_local(),
        };
        if !private {
            return Err("serving requires a loopback or private bind".into());
        }
        Ok(())
    }
}

/// What the operator's record names: an immutable publication directory and the
/// revision it must hold.
///
/// Built only by [`PublicationRecord::read`], which checks the record's
/// structure (an absolute directory and a 64-digit lowercase hex revision).
/// It does not verify the publication or its chain anchor; activation does.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PublicationRecord {
    directory: PathBuf,
    revision: String,
}

impl PublicationRecord {
    pub fn directory(&self) -> &Path {
        &self.directory
    }

    pub fn revision(&self) -> &str {
        &self.revision
    }

    /// Reads the record. A record that is missing, oversized or malformed is a
    /// refusal of that record, never of the publication already served.
    pub fn read(path: &Path) -> std::result::Result<Self, Refusal> {
        let bytes = read_bounded(path, MAX_RECORD_BYTES)
            .map_err(|e| Refusal::InvalidSource(format!("publication record: {e}")))?;
        let record: Self = serde_json::from_slice(&bytes)
            .map_err(|e| Refusal::InvalidSource(format!("publication record: {e}")))?;
        if !record.directory.is_absolute() {
            return Err(Refusal::InvalidSource(
                "publication record: directory must be an absolute path".into(),
            ));
        }
        if record.revision.len() != 64
            || !record
                .revision
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(Refusal::InvalidSource(
                "publication record: revision must be 64 lowercase hex digits".into(),
            ));
        }
        Ok(record)
    }
}

/// Why a publication was not activated, by what the operator must do about it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", content = "message", rename_all = "snake_case")]
pub enum Refusal {
    /// The record or the publication it names does not verify. Retrying the same
    /// record cannot succeed; stage a correct one.
    InvalidSource(String),
    /// The node could not confirm the anchor (unreachable, behind it, or not on
    /// mainnet). Retried while the record stands; the served publication stays.
    Unavailable(String),
    /// The publication's anchor is not on the node's canonical chain.
    Noncanonical(String),
    /// A newer record, or a revocation, overtook this one.
    Superseded(String),
}

impl std::error::Error for Refusal {}

impl std::fmt::Display for Refusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (Self::InvalidSource(message)
        | Self::Unavailable(message)
        | Self::Noncanonical(message)
        | Self::Superseded(message)) = self;
        f.write_str(message)
    }
}

/// A publication the loader verified against the record that named it.
///
/// Only [`load`] constructs one, and only after every file has been checked, so a
/// path and a digest are never enough to obtain one.
pub struct VerifiedPublication {
    record: PublicationRecord,
    anchor: (u32, [u8; 32]),
    publication: Publication,
}

impl VerifiedPublication {
    pub fn record(&self) -> &PublicationRecord {
        &self.record
    }

    /// The inventory it serves once activated.
    pub fn publication(&self) -> &Publication {
        &self.publication
    }

    /// Its terminal height and block hash.
    pub fn anchor(&self) -> (u32, [u8; 32]) {
        self.anchor
    }

    /// The head of the inventory it serves.
    pub fn head(&self) -> &receiver_pir::inventory::Head {
        self.publication.head()
    }

    /// Every block it depends on, with the Ironwood tree size it claims after it, which
    /// must all match the node before it activates: every segment's end and every
    /// companion's construction block.
    pub fn anchors(&self) -> Vec<TreeAnchor> {
        let mut anchors = Vec::new();
        for segment in self.publication.segments() {
            let payments = &segment
                .payments
                .payment_manifest()
                .expect("a payment source");
            let witnesses = &segment
                .witnesses
                .witness_manifest()
                .expect("a witness source");
            for anchor in [
                TreeAnchor::payments(&payments.directory),
                TreeAnchor::witnesses(&witnesses.table),
            ] {
                if !anchors.contains(&anchor) {
                    anchors.push(anchor);
                }
            }
        }
        anchors
    }
}

/// The largest staged layout read.
const MAX_LAYOUT_BYTES: u64 = 4 * 1024 * 1024;

/// Loads and verifies the inventory `record` names: its layout, then every segment's
/// rows and companion witness table and every sealed shard's filter. Blocking: it reads
/// up to several gigabytes and prepares PIR. When serving, a validated correction
/// layout revokes older sessions before any row loading or preparation.
pub fn load(
    record: &PublicationRecord,
    config: &Config,
    serving: Option<&Publications>,
) -> std::result::Result<VerifiedPublication, Refusal> {
    let invalid = |message: String| Refusal::InvalidSource(message);
    let file = |name: &str| record.directory.join(name);
    let raw = read_bounded(
        &file(&format!("{}.inventory.json", record.revision)),
        MAX_LAYOUT_BYTES,
    )
    .map_err(|e| invalid(format!("layout: {e}")))?;
    let digest = hex::encode(Sha256::digest(&raw));
    if digest != record.revision {
        return Err(invalid(format!(
            "the directory holds layout {digest}, the record expects {}",
            record.revision
        )));
    }
    let layout: Layout =
        serde_json::from_slice(&raw).map_err(|e| invalid(format!("layout: {e}")))?;
    if layout.genesis != Network::Mainnet.genesis_hash() {
        return Err(invalid("the publication is not for mainnet".into()));
    }
    // A trusted operator correction invalidates old answers immediately, even if
    // replacement rows are missing or preparation later fails. A check only compares
    // with the floor; it records nothing.
    let mut floor = history_floor(&config.state_dir);
    let accepted = match serving {
        Some(publications) => {
            crate::revision::advance(&mut floor, Some(publications), layout.history_revision)
        }
        None => match crate::revision::Floor::floor(&mut floor) {
            Ok(recorded) if layout.history_revision < recorded => Err(Refused::Lower {
                revision: layout.history_revision,
                floor: recorded,
            }),
            Ok(_) => Ok(()),
            Err(e) => Err(Refused::Floor(e)),
        },
    };
    match accepted {
        Ok(()) => {}
        Err(Refused::Lower { .. }) => {
            return Err(invalid("history revision moved backwards".into()))
        }
        Err(Refused::Floor(e)) => return Err(Refusal::Unavailable(format!("{HISTORY_FILE}: {e}"))),
    }
    let mut shards = Vec::new();
    for shard in &layout.shards {
        let mut segments = Vec::new();
        // A sealed shard's distinct receivers, which its staged filter must hold exactly.
        // They are 43 bytes each, at most a filter's capacity, for one shard at a time.
        let mut receivers = Vec::new();
        for segment in &shard.segments {
            let (revision, name) = (&segment.payments, hex::encode(segment.payments));
            let companion = hex::encode(segment.witnesses);
            let raw = read_bounded(
                &file(&format!("{name}.json")),
                receiver_pir::MAX_MANIFEST_BYTES as u64,
            )
            .map_err(|e| invalid(format!("manifest: {e}")))?;
            let manifest: Manifest =
                serde_json::from_slice(&raw).map_err(|e| invalid(format!("manifest: {e}")))?;
            if manifest.revision().ok().as_ref() != Some(revision) {
                return Err(invalid(format!(
                    "manifest {name} does not hold its revision"
                )));
            }
            if manifest.genesis != layout.genesis
                || manifest.history_revision != layout.history_revision
            {
                return Err(invalid(format!("manifest {name} is for another layout")));
            }
            if manifest.rows < config.min_rows {
                return Err(invalid(format!(
                    "the publication has {} rows, fewer than min_rows {}",
                    manifest.rows, config.min_rows
                )));
            }
            let rows_bytes = u64::from(manifest.rows) * ROW_BYTES as u64;
            let data = read_exact_len(&file(&format!("{name}.rows")), rows_bytes)
                .map_err(|e| invalid(format!("rows: {e}")))?;
            let raw = read_bounded(
                &file(&format!("{companion}.witness.json")),
                receiver_pir::MAX_MANIFEST_BYTES as u64,
            )
            .map_err(|e| invalid(format!("witness manifest: {e}")))?;
            let table: witness::Manifest = serde_json::from_slice(&raw)
                .map_err(|e| invalid(format!("witness manifest: {e}")))?;
            if table.revision().ok() != Some(segment.witnesses) {
                return Err(invalid(format!(
                    "witness manifest {companion} does not hold its revision"
                )));
            }
            let witness_bytes = u64::from(table.rows) * ROW_BYTES as u64;
            let witnesses = witness::Table {
                manifest: table,
                data: read_exact_len(&file(&format!("{companion}.witness")), witness_bytes)
                    .map_err(|e| invalid(format!("witness: {e}")))?,
            };
            let staged = Staged {
                payments: file(&format!("{name}.rows")),
                witnesses: file(&format!("{companion}.witness")),
            };
            if shard.filter.is_some() {
                crate::verify::receivers(&data, &mut receivers)
                    .map_err(|e| invalid(format!("publication: {e}")))?;
                receivers.sort_unstable();
                receivers.dedup();
                if receivers.len() > MAX_RECEIVERS as usize {
                    return Err(invalid(
                        "a shard with more receivers than a filter holds".into(),
                    ));
                }
            }
            // Each segment is verified and prepared, then released before the next, so
            // only one segment's tables are ever resident outside the table budget.
            let (server, witnesses) =
                crate::verify::prepare_servers(Snapshot { manifest, data }, witnesses)
                    .map_err(|e| invalid(format!("publication/witness: {e}")))?;
            let segment = staged
                .sources(&server, &witnesses)
                .map_err(|e| invalid(format!("publication/witness: {e}")))?;
            if let Some(publications) = serving {
                let tables = publications.tables();
                tables.insert(&segment.payments, Loaded::Payments(server));
                tables.insert(&segment.witnesses, Loaded::Witnesses(witnesses));
            }
            segments.push(segment);
        }
        let filter = match &shard.filter {
            None => None,
            Some(digest) => {
                let name = format!("{}.filter", hex::encode(digest));
                let path = file(&name);
                let bytes = read_bounded(&path, MAX_FILTER_BYTES as u64)
                    .map_err(|e| invalid(format!("filter: {e}")))?;
                let m = &segments
                    .first()
                    .and_then(|s| s.payments.payment_manifest())
                    .ok_or_else(|| invalid("a sealed shard without segments".into()))?
                    .directory;
                let coverage = Coverage {
                    start_height: m.start_height,
                    end_height: m.end_height,
                    end_hash: m.end_hash,
                };
                let shard = (layout.genesis, layout.history_revision, coverage);
                let source = staged_filter(path, bytes.into(), digest, shard, &receivers)
                    .map_err(|e| invalid(format!("filter {name}: {e}")))?;
                Some(Arc::new(source))
            }
        };
        shards.push((shard.sealed, segments, filter));
    }
    // Wallets refuse served coverage that starts after activation, and witnesses
    // need every commitment from it.
    let start = shards
        .first()
        .and_then(|s| s.1.first())
        .and_then(|s| s.payments.payment_manifest())
        .map(|m| m.directory.start_height);
    if start != Some(crate::blocks::ironwood_activation()) {
        return Err(invalid(
            "served coverage must start at Ironwood activation".into(),
        ));
    }
    let publication = Publication::assemble(layout.genesis, layout.history_revision, shards)
        .map_err(|e| invalid(format!("publication: {e}")))?;
    let report = serde_json::json!({
        "mode": "serve-publication",
        "directory": record.directory,
        "revision": record.revision,
    });
    let head = publication.head();
    Ok(VerifiedPublication {
        record: record.clone(),
        anchor: (head.end_height, head.end_hash),
        publication: publication.with_report(report),
    })
}

/// Verifies `bytes`, read from the staged filter file `path`, as the filter `digest`
/// names for `shard`, its network, correction revision and coverage, as a wallet checks
/// it, and as exactly the filter the indexer builds over `receivers`, the shard's
/// verified distinct receivers: a wallet skips a shard its filter excludes, so a missing
/// receiver would hide a payment. Returns a source that serves it from that file within
/// the table budget. The source keeps the path and length only, never `bytes`.
fn staged_filter(
    path: PathBuf,
    bytes: Arc<[u8]>,
    digest: &Hash,
    shard: (Hash, u64, Coverage),
    receivers: &[receiver_directory::ReceiverBytes],
) -> std::result::Result<FilterSource, String> {
    let filter = ShardFilter::decode(&bytes).map_err(|e| e.to_string())?;
    if filter.digest() != *digest {
        return Err("it does not hold its digest".into());
    }
    let (genesis, revision, coverage) = shard;
    filter
        .check(&genesis, revision, &coverage)
        .map_err(|e| format!("it is for another shard, network or revision: {e}"))?;
    let rebuilt = ShardFilter::build(genesis, revision, coverage, receivers)
        .map_err(|e| format!("the shard's receivers: {e}"))?;
    if rebuilt.digest() != *digest {
        return Err("it does not hold exactly the shard's receivers".into());
    }
    let len = bytes.len();
    Ok(FilterSource::stored(
        filter.digest(),
        len as u32,
        filter.receivers(),
        move || {
            read_exact_len(&path, len as u64)
                .map_err(|e| receiver_pir::Error::Transport(e.to_string()))
        },
    ))
}

/// A staged segment's payment and companion row files, which its sources reload from.
struct Staged {
    payments: PathBuf,
    witnesses: PathBuf,
}

impl Staged {
    /// Sources for the verified, prepared `server` and its companion `witnesses` that
    /// reload them from these files, so staged history stays within the table budget as
    /// indexed history does, and a check retains none of it.
    fn sources(
        self,
        server: &receiver_pir::server::Server,
        witnesses: &receiver_pir::server::WitnessServer,
    ) -> std::result::Result<receiver_pir_server::publication::Segment, receiver_pir::Error> {
        use receiver_pir_server::tables::Source;
        // A reload prepares the rows under the manifest verified here, never a restaged
        // one, and reads exactly the rows its geometry holds, so a replaced file cannot
        // change the geometry or grow the allocation.
        let read = |path: &Path, rows: u32| {
            read_exact_len(path, u64::from(rows) * ROW_BYTES as u64)
                .map_err(|e| receiver_pir::Error::Transport(e.to_string()))
        };
        let (manifest, path) = (server.manifest().directory.clone(), self.payments);
        let payments = Arc::new(Source::payments(
            server.manifest().clone(),
            server.public().to_vec(),
            move || {
                receiver_pir::server::Server::new(Snapshot {
                    manifest: manifest.clone(),
                    data: read(&path, manifest.rows)?,
                })
            },
        )?);
        let (table, path) = (witnesses.manifest().table.clone(), self.witnesses);
        let witnesses = Arc::new(Source::witnesses(
            witnesses.manifest().clone(),
            witnesses.public().to_vec(),
            payments.clone(),
            move || {
                Ok(witness::Table {
                    manifest: table.clone(),
                    data: read(&path, table.rows)?,
                })
            },
        )?);
        Ok(receiver_pir_server::publication::Segment {
            payments,
            witnesses,
        })
    }
}

/// Reads at most `limit` bytes of `path`, refusing a longer file.
pub(crate) fn read_bounded(path: &Path, limit: u64) -> std::io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    std::fs::File::open(path)?
        .take(limit + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > limit {
        return Err(std::io::Error::other(format!(
            "{} is larger than {limit} bytes",
            path.display()
        )));
    }
    Ok(bytes)
}

/// Reads `path`, which must be exactly `len` bytes.
pub(crate) fn read_exact_len(path: &Path, len: u64) -> std::io::Result<Vec<u8>> {
    let bytes = read_bounded(path, len)?;
    if bytes.len() as u64 != len {
        return Err(std::io::Error::other(format!(
            "{} is {} bytes, the manifest says {len}",
            path.display(),
            bytes.len()
        )));
    }
    Ok(bytes)
}

/// What the worker reports about its publications: on health and in the state
/// directory. Never read back as authority.
#[derive(Clone, Debug, Default, Serialize)]
pub struct Status {
    /// The record as last read, if it parsed.
    pub assigned: Option<PublicationRecord>,
    /// The record whose publication is served now.
    pub loaded: Option<PublicationRecord>,
    /// The record being loaded or awaiting activation.
    pub preparing: Option<PublicationRecord>,
    /// Why the latest record was not activated, while that stands.
    pub refusal: Option<Refusal>,
}

#[derive(Clone)]
struct Reporter {
    status: Arc<Mutex<Status>>,
    file: PathBuf,
}

impl Reporter {
    fn update(&self, change: impl FnOnce(&mut Status)) {
        let snapshot = {
            let mut status = self.status.lock().unwrap();
            change(&mut status);
            status.clone()
        };
        if let Err(error) = write_atomic(&self.file, &snapshot) {
            warn!(%error, "status report not written");
        }
    }

    fn json(&self) -> serde_json::Value {
        serde_json::to_value(&*self.status.lock().unwrap()).unwrap_or_default()
    }
}

/// The correction floor in `state_dir`; see [`HISTORY_FILE`].
fn history_floor(state_dir: &Path) -> crate::revision::StateFile {
    crate::revision::StateFile::new(state_dir, HISTORY_FILE)
}

fn write_atomic(path: &Path, value: &impl Serialize) -> Result<()> {
    let parent = path.parent().ok_or("status file has no directory")?;
    let mut temp = tempfile::NamedTempFile::new_in(parent)?;
    temp.write_all(&serde_json::to_vec_pretty(value)?)?;
    temp.as_file().sync_all()?;
    temp.persist(path)?;
    Ok(())
}

/// One candidate being brought to activation.
struct Pending {
    record: PublicationRecord,
    verified: VerifiedPublication,
}

/// The single serial updater: loads what the record names, waits while the retained
/// set is full, confirms the anchor and publishes.
struct Updater {
    config: Config,
    nodes: Vec<ZakuraClient>,
    genesis: Hash,
    publications: Publications,
    reporter: Reporter,
    /// The record last acted on, so an unchanged invalid record is not reloaded.
    settled: Option<PublicationRecord>,
    pending: Option<Pending>,
    /// The epoch the served publication was activated in. A revocation advances
    /// the epoch and serves nothing, so the record is verified and confirmed again.
    activated_epoch: Option<u64>,
}

impl Updater {
    /// Reads the record and moves it one step towards activation.
    async fn step(&mut self) {
        let record = match PublicationRecord::read(&self.config.publication_record) {
            Ok(record) => record,
            Err(refusal) => {
                // A missing or malformed record leaves the served publication alone.
                self.pending = None;
                self.settled = None;
                self.reporter.update(|status| {
                    status.assigned = None;
                    status.preparing = None;
                    status.refusal = Some(refusal);
                });
                return;
            }
        };
        // A record not yet acted on clears the last outcome in the same report
        // that names it, so no reader pairs it with its predecessor's refusal.
        let fresh = self.settled.as_ref() != Some(&record)
            && self.pending.as_ref().is_none_or(|p| p.record != record);
        self.reporter.update(|status| {
            status.assigned = Some(record.clone());
            if fresh {
                status.preparing = Some(record.clone());
                status.refusal = None;
            }
        });
        if self.pending.as_ref().is_some_and(|p| p.record != record) {
            let old = self.pending.take().unwrap();
            info!(revision = %old.record.revision, "pending publication superseded by a newer record");
        }
        if self.settled.as_ref() == Some(&record) && self.pending.is_none() {
            if self
                .activated_epoch
                .is_none_or(|epoch| epoch == self.publications.epoch())
            {
                return;
            }
            info!(revision = %record.revision, "served publication revoked; verifying the record again");
            self.settled = None;
            self.activated_epoch = None;
            self.reporter.update(|status| status.loaded = None);
        }
        if self.pending.is_none() {
            self.reporter.update(|status| {
                status.preparing = Some(record.clone());
                status.refusal = None;
            });
            let started = std::time::Instant::now();
            let config = self.config.clone();
            let loading = record.clone();
            let serving = self.publications.clone();
            let revision = self.publications.history_revision();
            let loaded =
                tokio::task::spawn_blocking(move || load(&loading, &config, Some(&serving)))
                    .await
                    .unwrap_or_else(|e| {
                        Err(Refusal::InvalidSource(format!("preparation failed: {e}")))
                    });
            // A higher correction revision in this record revoked what was served. That
            // is this record's own doing, not a revocation to verify it again after, so
            // the epoch it left is the one its outcome settles in.
            if self.publications.history_revision() > revision {
                self.activated_epoch = Some(self.publications.epoch());
                self.reporter.update(|status| status.loaded = None);
            }
            info!(
                revision = %record.revision,
                elapsed_ms = started.elapsed().as_millis() as u64,
                ok = loaded.is_ok(),
                "publication prepared"
            );
            match loaded {
                Ok(verified) => {
                    self.pending = Some(Pending {
                        record: record.clone(),
                        verified,
                    })
                }
                // A transient failure, such as writing the correction floor, leaves the
                // record unsettled, so the next poll loads it again.
                Err(refusal @ Refusal::Unavailable(_)) => {
                    warn!(revision = %record.revision, %refusal, "publication load deferred");
                    self.reporter.update(|status| {
                        status.preparing = None;
                        status.refusal = Some(refusal);
                    });
                    return;
                }
                Err(refusal) => return self.refuse(record, refusal),
            }
            // A newer record that arrived while this one prepared supersedes it.
            if PublicationRecord::read(&self.config.publication_record)
                .ok()
                .as_ref()
                != Some(&record)
            {
                self.pending = None;
                self.reporter.update(|status| {
                    status.preparing = None;
                    status.refusal = Some(Refusal::Superseded(format!(
                        "revision {} was superseded while it prepared",
                        record.revision
                    )));
                });
                return;
            }
        }
        // Displaced inventories keep their full retention; the step is retried after.
        if self.publications.ready_at().is_some() {
            return;
        }
        self.activate().await;
    }

    async fn activate(&mut self) {
        let Some(pending) = self.pending.take() else {
            return;
        };
        let record = pending.record.clone();
        let (height, _) = pending.verified.anchor();
        let anchors = pending.verified.anchors();
        let epoch = self.publications.epoch();
        let confirmed = async {
            let (tip, rpc) = freshest(&self.nodes, self.genesis).await?;
            let mut off_chain = None;
            for anchor in &anchors {
                if u64::from(anchor.height) > tip {
                    return Ok(Err(anchor.height));
                }
                if off_chain.is_none() {
                    off_chain = tree_mismatch(&rpc, *anchor)
                        .await?
                        .map(|mismatch| (anchor, mismatch));
                }
            }
            // Ranking read the genesis earlier, and an endpoint can be repointed.
            rpc.check_network(self.genesis).await?;
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(Ok(off_chain))
        }
        .await;
        match confirmed {
            Ok(Ok(None)) => {}
            Ok(Ok(Some((anchor, TreeMismatch::OffChain)))) => {
                let height = anchor.height;
                return self.refuse(
                    record,
                    Refusal::Noncanonical(format!(
                        "the anchor at height {height} is not on the node's canonical chain"
                    )),
                );
            }
            Ok(Ok(Some((anchor, TreeMismatch::Size(size))))) => {
                return self.refuse(
                    record,
                    Refusal::InvalidSource(format!(
                        "the publication claims Ironwood tree size {} after block {}, the node has {size}",
                        anchor.size, anchor.height
                    )),
                );
            }
            Ok(Err(height)) => {
                self.pending = Some(pending);
                return self.report_waiting(Refusal::Unavailable(format!(
                    "the node is behind the anchor at height {height}"
                )));
            }
            Err(error) => {
                self.pending = Some(pending);
                return self.report_waiting(Refusal::Unavailable(format!(
                    "canonical check deferred: {error}"
                )));
            }
        }
        // Canonical RPC can take seconds. The operator may replace the record
        // while it is in flight, after the preparation-time check above.
        if PublicationRecord::read(&self.config.publication_record)
            .ok()
            .as_ref()
            != Some(&record)
        {
            self.reporter.update(|status| {
                status.preparing = None;
                status.refusal = Some(Refusal::Superseded(format!(
                    "revision {} was superseded during its canonical check",
                    record.revision
                )));
            });
            return;
        }
        let verified = pending.verified;
        if self.publications.publish(verified.publication, epoch) {
            info!(revision = %record.revision, height, "publication activated");
            self.settled = Some(record.clone());
            self.activated_epoch = Some(epoch);
            self.reporter.update(|status| {
                status.loaded = Some(record);
                status.preparing = None;
                status.refusal = None;
            });
        } else if self.publications.epoch() != epoch {
            // A revocation raced the activation; the record is loaded again.
            self.settled = None;
            self.reporter.update(|status| {
                status.refusal = Some(Refusal::Superseded(
                    "a revocation overtook activation; preparing again".into(),
                ))
            });
        } else {
            self.refuse(
                record,
                Refusal::InvalidSource(format!(
                    "the publication ends at height {height}, below the one served"
                )),
            );
        }
    }

    fn refuse(&mut self, record: PublicationRecord, refusal: Refusal) {
        warn!(revision = %record.revision, %refusal, "publication refused; still serving the last valid one");
        self.pending = None;
        self.settled = Some(record);
        self.reporter.update(|status| {
            status.preparing = None;
            status.refusal = Some(refusal);
        });
    }

    fn report_waiting(&self, refusal: Refusal) {
        self.reporter
            .update(|status| status.refusal = Some(refusal));
    }
}

/// Checks the configuration and fully verifies the publication the record names,
/// exactly as the service does before serving it, without serving or activating.
pub fn check(config: &Config) -> std::result::Result<VerifiedPublication, Refusal> {
    let record = PublicationRecord::read(&config.publication_record)?;
    load(&record, config, None)
}

pub async fn main(args: Args) -> Result<()> {
    let config = Config::load(&args.config)?;
    if args.check {
        let verified = tokio::task::spawn_blocking(move || check(&config)).await??;
        println!(
            "{}",
            serde_json::json!({
                "revision": verified.record().revision,
                "directory": verified.record().directory,
                "end_height": verified.anchor().0,
                "end_hash": hex::encode(verified.anchor().1),
            })
        );
        return Ok(());
    }
    run(config).await
}

/// Serves until SIGTERM or Ctrl-C.
pub async fn run(config: Config) -> Result<()> {
    std::fs::create_dir_all(&config.state_dir)?;
    let reporter = Reporter {
        status: Arc::new(Mutex::new(Status::default())),
        file: config.state_dir.join(STATUS_FILE),
    };
    // Proves the state directory writable before the listener opens.
    reporter.update(|_| {});
    std::fs::metadata(&reporter.file)?;
    let nodes = config
        .rpc_urls
        .iter()
        .map(|url| ZakuraClient::new(url.clone(), config.cookie.as_deref()))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let genesis = Network::Mainnet.genesis_hash();
    let publications = Publications::default();
    // Older sessions stay fenced across a restart; see `HISTORY_FILE`.
    crate::revision::restore(&mut history_floor(&config.state_dir), &publications)?;
    let listener = tokio::net::TcpListener::bind(config.bind).await?;
    let app = receiver_pir_server::router_with_health(publications.clone(), {
        let reporter = reporter.clone();
        Arc::new(move || {
            let mut fields = serde_json::Map::new();
            fields.insert("publication".into(), reporter.json());
            fields
        })
    });
    info!(
        address = %listener.local_addr()?,
        record = %config.publication_record.display(),
        "receiver serve-publication listening; verifying the recorded publication"
    );
    let guard = tokio::spawn(crate::canonical::guard(
        publications.clone(),
        nodes.clone(),
        genesis,
        Duration::from_secs(config.poll_seconds),
    ));
    let http = tokio::spawn(async move { axum::serve(listener, app).await });
    let mut updater = Updater {
        config,
        nodes,
        genesis,
        publications,
        reporter,
        settled: None,
        pending: None,
        activated_epoch: None,
    };
    let updates = async {
        loop {
            updater.step().await;
            tokio::time::sleep(RECORD_POLL).await;
        }
    };
    tokio::select! {
        result = http => { result??; },
        result = guard => { result?; return Err("canonical guard stopped".into()); },
        _ = updates => {},
        _ = shutdown() => {},
    }
    Ok(())
}

/// Resolves on Ctrl-C, or on SIGTERM.
async fn shutdown() {
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("SIGTERM handler");
    tokio::select! { _ = tokio::signal::ctrl_c() => {}, _ = term.recv() => {} }
}

#[cfg(test)]
use crate::probe::common;

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> Config {
        serde_json::from_value(serde_json::json!({
            "publication_record": "/etc/receiver/publication.json",
            "bind": "127.0.0.1:18380",
            "state_dir": "/var/lib/receiver-serve",
            "rpc_urls": ["http://127.0.0.1:8232"],
            "no_auth": true,
        }))
        .unwrap()
    }

    #[test]
    fn configurations_are_checked_before_anything_is_read() {
        config().check().unwrap();
        let refused = |change: fn(&mut Config)| {
            let mut c = config();
            change(&mut c);
            c.check().unwrap_err().to_string()
        };
        assert!(refused(|c| c.bind = "0.0.0.0:1".parse().unwrap()).contains("private"));
        assert!(refused(|c| c.cookie = Some("/run/cookie".into())).contains("exactly one"));
        assert!(refused(|c| c.no_auth = false).contains("exactly one"));
        assert!(refused(|c| c.rpc_urls.clear()).contains("no node"));
        assert!(refused(|c| c.state_dir = "state".into()).contains("absolute"));
        assert!(
            refused(|c| c.publication_record = "/var/lib/receiver-serve/r.json".into())
                .contains("out of state_dir")
        );
        // Unknown fields, such as a partner key, are refused outright.
        let error = serde_json::from_value::<Config>(serde_json::json!({
            "publication_record": "/r", "bind": "127.0.0.1:1", "state_dir": "/s",
            "rpc_urls": ["http://n"], "no_auth": true, "near_key": "x",
        }))
        .unwrap_err();
        assert!(error.to_string().contains("unknown field"));
    }

    /// A validated correction fences existing sessions before reading or preparing rows.
    #[test]
    fn staged_correction_fences_before_row_loading() {
        let dir = tempfile::tempdir().unwrap();
        let mut manifest = common::manifest(receiver_pir::MIN_ROWS);
        manifest.genesis = Network::Mainnet.genesis_hash();
        manifest.start_height = crate::blocks::ironwood_activation();
        manifest.end_height = manifest.start_height;
        let old = receiver_directory::snapshot::Snapshot::build(manifest.clone(), &[]).unwrap();
        let table = crate::witness::table(&old, &[]).unwrap();
        let segment = crate::verify::prepare(old, table).unwrap();
        let old = Publication::assemble(manifest.genesis, 0, vec![(false, vec![segment], None)]);
        let publications = Publications::default();
        assert!(publications.publish(old.unwrap(), 0));
        manifest.history_revision = 1;
        let layout = Layout {
            genesis: manifest.genesis,
            history_revision: 1,
            shards: vec![crate::application::LayoutShard {
                sealed: false,
                segments: vec![crate::application::LayoutSegment {
                    payments: manifest.revision().unwrap(),
                    witnesses: [0; 32],
                }],
                filter: None,
            }],
        };
        let (id, bytes) = layout.encode().unwrap();
        let revision = hex::encode(id);
        std::fs::write(dir.path().join(format!("{revision}.inventory.json")), bytes).unwrap();
        std::fs::write(
            dir.path().join(format!(
                "{}.json",
                hex::encode(manifest.revision().unwrap())
            )),
            serde_json::to_vec(&manifest).unwrap(),
        )
        .unwrap();
        let record = PublicationRecord {
            directory: dir.path().to_owned(),
            revision,
        };
        let state = tempfile::tempdir().unwrap();
        let config = Config {
            state_dir: state.path().to_owned(),
            ..config()
        };
        let failure = load(&record, &config, Some(&publications)).err().unwrap();
        assert!(failure.to_string().contains("rows:"));
        assert!(publications.current_anchor().is_none());
        assert_eq!(publications.epoch(), 1);
        assert!(publications.ready_at().is_none());
    }

    /// A staged filter's source serves from its file and keeps none of the verified
    /// bytes, which would otherwise stay resident outside the table budget.
    #[test]
    fn a_staged_filter_keeps_no_verified_bytes() {
        use receiver_directory::shards::Coverage;
        let coverage = Coverage {
            start_height: 100,
            end_height: 101,
            end_hash: [3; 32],
        };
        let filter = ShardFilter::build([1; 32], 0, coverage, &[common::receiver()]).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("filter");
        std::fs::write(&path, filter.bytes()).unwrap();
        let bytes: Arc<[u8]> = filter.bytes().into();
        let verified = Arc::downgrade(&bytes);
        let shard = ([1; 32], 0, coverage);
        let receivers = [common::receiver()];
        let source = staged_filter(path, bytes, &filter.digest(), shard, &receivers).unwrap();
        assert!(verified.upgrade().is_none());
        let tables = receiver_pir_server::tables::Tables::new(1 << 30);
        let served = tables.filter(&source).unwrap();
        assert_eq!(served.filter().unwrap(), filter.bytes());
        let staged = |bytes: &[u8], shard, receivers: &[_]| {
            staged_filter(
                dir.path().join("filter"),
                bytes.into(),
                &filter.digest(),
                shard,
                receivers,
            )
        };
        assert!(staged(&[0u8; 4], shard, &receivers).is_err());
        // A filter missing one of the shard's verified receivers is refused, since a
        // wallet paid there would skip the shard, and so is one holding an extra.
        let both = [common::receiver(), common::seeded_receiver(1)];
        let refused = staged(filter.bytes(), shard, &both).err().unwrap();
        assert!(
            refused.contains("exactly the shard's receivers"),
            "{refused}"
        );
        let extra = ShardFilter::build([1; 32], 0, coverage, &both).unwrap();
        assert!(staged_filter(
            dir.path().join("filter"),
            extra.bytes().into(),
            &extra.digest(),
            shard,
            &receivers,
        )
        .is_err());
        // A valid filter for another shard, network or revision is refused too.
        let other = Coverage {
            end_hash: [4; 32],
            ..coverage
        };
        for shard in [
            ([1; 32], 0, other),
            ([2; 32], 0, coverage),
            ([1; 32], 1, coverage),
        ] {
            assert!(
                staged(filter.bytes(), shard, &receivers).is_err(),
                "{shard:?}"
            );
        }
    }

    /// A correction's revision outlives a restart: once revision 1 was accepted, a fresh
    /// service, or a check, refuses a record of revision 0 even though nothing serves.
    #[test]
    fn an_accepted_correction_survives_a_restart() {
        let (dir, state) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        let config = Config {
            state_dir: state.path().to_owned(),
            ..config()
        };
        let stage = |history_revision: u64| {
            let layout = Layout {
                genesis: Network::Mainnet.genesis_hash(),
                history_revision,
                shards: vec![crate::application::LayoutShard {
                    sealed: false,
                    segments: vec![crate::application::LayoutSegment {
                        payments: [history_revision as u8; 32],
                        witnesses: [0; 32],
                    }],
                    filter: None,
                }],
            };
            let (id, bytes) = layout.encode().unwrap();
            let revision = hex::encode(id);
            std::fs::write(dir.path().join(format!("{revision}.inventory.json")), bytes).unwrap();
            PublicationRecord {
                directory: dir.path().to_owned(),
                revision,
            }
        };
        let (old, corrected) = (stage(0), stage(1));
        // Accepted, then refused for its missing segment files.
        let failure = load(&corrected, &config, Some(&Publications::default()));
        assert!(failure.err().unwrap().to_string().contains("manifest:"));
        let floor = || history_floor(state.path());
        assert_eq!(crate::revision::Floor::floor(&mut floor()).unwrap(), 1);
        let restarted = Publications::default();
        crate::revision::restore(&mut floor(), &restarted).unwrap();
        for serving in [Some(&restarted), None] {
            let failure = load(&old, &config, serving).err().unwrap();
            assert!(failure.to_string().contains("moved backwards"), "{failure}");
        }
        // An unreadable record refuses rather than guess.
        std::fs::write(state.path().join(HISTORY_FILE), "x").unwrap();
        assert!(matches!(
            load(&old, &config, None),
            Err(Refusal::Unavailable(_))
        ));
    }

    #[test]
    fn records_are_strict() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("record.json");
        let revision = "ab".repeat(32);
        let write = |value: serde_json::Value| std::fs::write(&path, value.to_string()).unwrap();
        write(serde_json::json!({"directory": "/p", "revision": revision}));
        assert_eq!(PublicationRecord::read(&path).unwrap().revision, revision);
        for bad in [
            serde_json::json!({"directory": "p", "revision": revision}),
            serde_json::json!({"directory": "/p", "revision": "AB".repeat(32)}),
            serde_json::json!({"directory": "/p", "revision": "ab"}),
            serde_json::json!({"directory": "/p", "revision": revision, "extra": 1}),
        ] {
            write(bad);
            assert!(matches!(
                PublicationRecord::read(&path),
                Err(Refusal::InvalidSource(_))
            ));
        }
        std::fs::write(&path, vec![b' '; 5000]).unwrap();
        assert!(PublicationRecord::read(&path).is_err());
    }
}
