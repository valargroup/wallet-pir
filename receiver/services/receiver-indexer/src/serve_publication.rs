//! `receiver-directory serve-publication --config FILE`: serve receiver directory
//! publications an operator stages, without indexing.
//!
//! The operator stages a complete publication directory, in the files a one-shot
//! indexer run writes (`<revision>.json`, `.rows`, `.filters` and optionally
//! `.witness`), and then atomically replaces a small record naming it:
//! `{"directory": "/abs/path", "revision": "<hex>"}`. The record is a private, local
//! control input: there is no update route. This mode opens no index database,
//! ingests no blocks and reads no partner key.
//!
//! Each record is loaded and verified in full before it can be served: the manifest
//! must digest to the expected revision, cover history from Ironwood activation on
//! mainnet, and commit to the row, filter and witness files beside it, which the
//! Receiver server library re-validates record by record while preparing PIR. Its
//! anchor must then be canonical on the freshest node. Throughout, an independent
//! guard checks every served anchor and revokes on a confirmed reorg, as the
//! indexer's serving mode does. [`Publications`] owns epochs, monotonic coverage and
//! the predecessor's grace.
//!
//! Updates are serial. A newer record supersedes one still preparing; a record that
//! does not verify is refused and the valid publication already served stays. A
//! restart reads the record again and verifies everything before serving: the
//! status file in the state directory is a report for operators, never an input.
//!
//! The publisher remains trusted for completeness: verification proves the files are
//! the publication the record names and that it is internally consistent and anchored
//! on the canonical chain, not that it holds every payment.
use crate::canonical::{freshest, is_canonical};
use crate::zakura::ZakuraClient;
use receiver_directory::{
    filter::MAX_FILTERS_BYTES,
    snapshot::{Manifest, Snapshot, ROW_BYTES},
    witness::MAX_WITNESS_BYTES,
};
use receiver_pir::server::Server;
use receiver_pir_server::{Publication, Publications};
use serde::{Deserialize, Serialize};
use std::{
    io::{Read, Write},
    net::{IpAddr, SocketAddr},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};
use tracing::{info, warn};
use zakura_chain::{block::Hash, parameters::Network};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// How often the publication record is read for a replacement.
const RECORD_POLL: Duration = Duration::from_secs(1);
/// The largest publication record read.
const MAX_RECORD_BYTES: u64 = 4096;
/// The largest configuration file read.
const MAX_CONFIG_BYTES: u64 = 64 * 1024;
/// The report this mode writes into its state directory.
pub const STATUS_FILE: &str = "serve-publication.json";

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
    /// Refuse a publication without a witness file.
    #[serde(default)]
    pub witnesses: bool,
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
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PublicationRecord {
    pub directory: PathBuf,
    pub revision: String,
}

impl PublicationRecord {
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

    /// Its terminal height and block hash.
    pub fn anchor(&self) -> (u32, [u8; 32]) {
        self.anchor
    }

    pub fn manifest(&self) -> &Manifest {
        self.publication.manifest()
    }
}

/// Loads and verifies the publication `record` names. Blocking: it reads up to a few
/// hundred megabytes and prepares PIR.
pub fn load(
    record: &PublicationRecord,
    config: &Config,
) -> std::result::Result<VerifiedPublication, Refusal> {
    let invalid = |message: String| Refusal::InvalidSource(message);
    let file = |extension: &str| {
        record
            .directory
            .join(format!("{}.{extension}", record.revision))
    };
    let raw = read_bounded(&file("json"), receiver_pir::MAX_MANIFEST_BYTES as u64)
        .map_err(|e| invalid(format!("manifest: {e}")))?;
    let manifest: Manifest =
        serde_json::from_slice(&raw).map_err(|e| invalid(format!("manifest: {e}")))?;
    let revision = manifest
        .revision()
        .map_err(|e| invalid(format!("manifest: {e}")))?;
    if hex::encode(revision) != record.revision {
        return Err(invalid(format!(
            "the directory holds revision {}, the record expects {}",
            hex::encode(revision),
            record.revision
        )));
    }
    if manifest.genesis != Network::Mainnet.genesis_hash().0 {
        return Err(invalid("the publication is not for mainnet".into()));
    }
    // Wallets refuse served coverage that starts after activation, and witnesses
    // need every commitment from it.
    if manifest.start_height != crate::blocks::ironwood_activation() {
        return Err(invalid(
            "served coverage must start at Ironwood activation".into(),
        ));
    }
    if manifest.rows < config.min_rows {
        return Err(invalid(format!(
            "the publication has {} rows, fewer than min_rows {}",
            manifest.rows, config.min_rows
        )));
    }
    let rows_bytes = u64::from(manifest.rows) * ROW_BYTES as u64;
    let data =
        read_exact_len(&file("rows"), rows_bytes).map_err(|e| invalid(format!("rows: {e}")))?;
    let filters = read_bounded(&file("filters"), MAX_FILTERS_BYTES as u64)
        .map_err(|e| invalid(format!("filters: {e}")))?;
    let witness_path = file("witness");
    let witnesses = match std::fs::symlink_metadata(&witness_path) {
        Ok(_) => Some(
            read_bounded(&witness_path, MAX_WITNESS_BYTES as u64)
                .map_err(|e| invalid(format!("witness: {e}")))?,
        ),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            if config.witnesses {
                return Err(invalid("the publication has no witness file".into()));
            }
            None
        }
        Err(e) => return Err(invalid(format!("witness: {e}"))),
    };
    let anchor = (manifest.end_height, manifest.end_hash);
    let server = Server::new(Snapshot {
        manifest,
        data,
        filters,
    })
    .map_err(|e| invalid(format!("publication: {e}")))?;
    let publication =
        Publication::new(server, witnesses).map_err(|e| invalid(format!("witness: {e}")))?;
    let report = serde_json::json!({
        "mode": "serve-publication",
        "directory": record.directory,
        "revision": record.revision,
    });
    Ok(VerifiedPublication {
        record: record.clone(),
        anchor,
        publication: publication.with_report(report),
    })
}

/// Reads at most `limit` bytes of `path`, refusing a longer file.
fn read_bounded(path: &Path, limit: u64) -> std::io::Result<Vec<u8>> {
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
fn read_exact_len(path: &Path, len: u64) -> std::io::Result<Vec<u8>> {
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

/// The single serial updater: loads what the record names, waits out any
/// predecessor's grace, confirms the anchor and publishes.
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
            let loaded = tokio::task::spawn_blocking(move || load(&loading, &config))
                .await
                .unwrap_or_else(|e| {
                    Err(Refusal::InvalidSource(format!("preparation failed: {e}")))
                });
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
        // The predecessor keeps its full grace; the step is retried after it.
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
        let (height, hash) = pending.verified.anchor();
        let epoch = self.publications.epoch();
        let confirmed = async {
            let (tip, rpc) = freshest(&self.nodes, self.genesis).await?;
            if u64::from(height) > tip {
                return Ok(None);
            }
            let canonical = is_canonical(&rpc, height, hash).await?;
            // Ranking read the genesis earlier, and an endpoint can be repointed.
            rpc.check_network(self.genesis).await?;
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(Some(canonical))
        }
        .await;
        match confirmed {
            Ok(Some(true)) => {}
            Ok(Some(false)) => {
                return self.refuse(
                    record,
                    Refusal::Noncanonical(format!(
                        "the anchor at height {height} is not on the node's canonical chain"
                    )),
                )
            }
            Ok(None) => {
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
    load(&record, config)
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
