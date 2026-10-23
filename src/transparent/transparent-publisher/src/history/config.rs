//! History source, shard policy, and publication storage settings.
use crate::history::publication;
use serde::Deserialize;
use std::path::{Path, PathBuf};
use zakura_pir_publisher::transparent::filter as transparent_filter;

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub data_dir: PathBuf,
    pub publication_root: PathBuf,
    pub initial_publication: PathBuf,
    pub recent_from: u64,
    pub recent_geometry: String,
    pub archive_geometry: String,
    pub rpc_url: String,
    pub rpc_cookie: PathBuf,
    pub listen: std::net::SocketAddr,
    pub source_sha: String,
    /// Which newly built shards publish a directory choice table. Absent means
    /// `off`, which is what every configuration before this field publishes.
    #[serde(default)]
    pub directory_choice: publication::DirectoryChoice,
    /// Range-filter profile to publish under. Absent means the original
    /// profile, which is what every configuration before this field publishes.
    #[serde(default = "default_range_profile")]
    pub range_profile: String,
    /// Follow a journal another process writes instead of ingesting into it.
    ///
    /// The journal at `data_dir` is then opened read-only: its writer keeps
    /// the lock, this controller re-reads its checkpoints, and a rollback the
    /// writer commits invalidates publication exactly as one seen by ingest
    /// would. Nothing is written beside the journal.
    #[serde(default)]
    pub follow_journal: bool,
    /// Where journal snapshots are spooled. Absent means the publication
    /// root; never the journal's directory when following.
    #[serde(default)]
    pub snapshot_dir: Option<PathBuf>,
}

impl Config {
    pub(crate) fn snapshot_dir(&self) -> &Path {
        self.snapshot_dir
            .as_deref()
            .unwrap_or(&self.publication_root)
    }
}

fn default_range_profile() -> String {
    transparent_filter::RANGE_PROFILE.to_string()
}
