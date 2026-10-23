//! Explicit preparation beside a live predecessor; never ingest or activate.
use crate::{
    storage::{
        self,
        preparation::{self, Ownership},
    },
    BoxError,
};
use clap::{Parser, ValueEnum};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

pub const RECORD: &str = "prepared-storage.json";
#[derive(Clone, Copy, Debug, ValueEnum)]
enum Product {
    History,
    TransactionDetails,
}
impl Product {
    fn name(self) -> &'static str {
        match self {
            Self::History => "transparent_history",
            Self::TransactionDetails => "transaction_details",
        }
    }
}
#[derive(Parser)]
struct Cli {
    #[arg(long)]
    config: PathBuf,
    #[arg(long, value_enum)]
    product: Product,
    #[arg(long)]
    source_root: PathBuf,
    #[arg(long)]
    target_root: PathBuf,
    /// Check completed file identities and the current node anchor without activation.
    #[arg(long)]
    verify_only: bool,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Prepared {
    pub schema: String,
    pub ownership: Ownership,
    pub source_active_sha256: String,
    pub configuration_sha256: String,
    pub map_sha256: String,
    pub anchor_height: u64,
    pub anchor_hash: String,
    pub revision_floor: u64,
    pub files: BTreeMap<PathBuf, preparation::Entry>,
}
impl Prepared {
    pub fn verify(&self) -> Result<(), BoxError> {
        if self.schema != "transparent-prepared-storage/1"
            || preparation::fingerprint(&self.ownership.target)? != self.files
        {
            return Err("prepared storage changed after verification".into());
        }
        storage::layout::require(&self.ownership.target, &self.ownership.product)
    }
}

pub async fn run(args: Vec<std::ffi::OsString>) -> Result<(), BoxError> {
    let cli = Cli::parse_from(args);
    let target = cli.target_root.clone();
    let product = cli.product;
    let configuration: serde_json::Value = serde_json::from_slice(&std::fs::read(&cli.config)?)?;
    tokio::task::spawn_blocking(move || prepare(cli)).await??;
    {
        // Recheck the selected snapshot, including at the stopped-owner handoff.
        // A newer source publication does not invalidate these independent bytes.
        let prepared: Prepared = serde_json::from_slice(&std::fs::read(target.join(RECORD))?)?;
        let settings = &configuration[product.name()]["settings"];
        let cookie = PathBuf::from(
            settings["rpc_cookie"]
                .as_str()
                .ok_or("missing node cookie path")?,
        );
        let rpc = crate::ingest::zakura::connect(
            settings["rpc_url"].as_str().ok_or("missing node URL")?,
            &cookie,
        )?;
        if rpc.block_hash_display(prepared.anchor_height).await? != prepared.anchor_hash {
            return Err(
                "prepared anchor is no longer canonical; predecessor stays unchanged".into(),
            );
        }
    }
    Ok(())
}
fn prepare(cli: Cli) -> Result<(), BoxError> {
    prepare_with(cli, &preparation::Cloner::default())
}
fn prepare_with(cli: Cli, clone: &preparation::Cloner) -> Result<(), BoxError> {
    if cli.verify_only {
        let prepared: Prepared =
            serde_json::from_slice(&std::fs::read(cli.target_root.join(RECORD))?)?;
        if prepared.ownership.target != cli.target_root
            || prepared.ownership.source != cli.source_root
            || prepared.ownership.product != cli.product.name()
            || prepared.configuration_sha256 != preparation::digest(&std::fs::read(&cli.config)?)
        {
            return Err("prepared storage belongs to another request".into());
        }
        prepared.verify()?;
        println!(
            "{}",
            serde_json::json!({"verified":true,"map_sha256":prepared.map_sha256})
        );
        return Ok(());
    }
    let (owner, _lock) = preparation::open(&cli.source_root, &cli.target_root, cli.product.name())?;
    let configuration = std::fs::read(&cli.config)?;
    let configuration_sha256 = preparation::digest(&configuration);
    let active = std::fs::read(owner.source.join("active.json"))?;
    let active_digest = preparation::digest(&active);
    let record = owner.target.join(RECORD);
    require_resolved(&owner.source)?;
    if record.exists() {
        let prior: Prepared = serde_json::from_slice(&std::fs::read(&record)?)?;
        let current = preparation::fingerprint(&owner.target)?;
        for (path, entry) in &prior.files {
            if path.components().count() > 1 && current.get(path) != Some(entry) {
                return Err("previously prepared immutable material changed".into());
            }
        }
        if prior.ownership != owner || prior.configuration_sha256 != configuration_sha256 {
            return Err("prepared storage belongs to another request".into());
        }
        if prior.files == current {
            prior.verify()?;
            println!(
                "{}",
                serde_json::json!({"prepared":true,"map_sha256":prior.map_sha256})
            );
            return Ok(());
        }
    }
    let raw: serde_json::Value = serde_json::from_slice(&configuration)?;
    let settings = raw
        .get(cli.product.name())
        .and_then(|p| p.get("settings"))
        .ok_or("missing product settings")?;
    let active_value: serde_json::Value = serde_json::from_slice(&active)?;
    let source = PathBuf::from(
        active_value["directory"]
            .as_str()
            .ok_or("active publication has no directory")?,
    );
    if source.parent() != Some(owner.source.as_path()) {
        return Err("active publication is outside its source root".into());
    }
    let target = preparation::destination(&owner, &source)?;
    // Pin independent bytes while this source directory still exists. Once
    // cloned, later publications and source collection cannot change them.
    clone.tree(&source, &target, &owner.target.join(".objects"))?;
    let (map_sha256, anchor_height, anchor_hash) = match cli.product {
        Product::History => history(&owner, &target, settings)?,
        Product::TransactionDetails => detail(&owner, &target, settings, &active, clone)?,
    };
    let (height_field, hash_field) = match cli.product {
        Product::History => ("height", "hash"),
        Product::TransactionDetails => ("tip", "tip_hash"),
    };
    if active_value["map_sha256"].as_str() != Some(map_sha256.as_str())
        || active_value[height_field].as_u64() != Some(anchor_height)
        || active_value[hash_field].as_str() != Some(anchor_hash.as_str())
    {
        return Err("source pointer disagrees with its verified publication".into());
    }
    require_resolved(&owner.source)?;
    let mut rebased = active_value;
    rebased["directory"] = serde_json::to_value(&target)?;
    storage::records::write_durable(
        &owner.target.join("active.json"),
        &serde_json::to_vec(&rebased)?,
    )?;
    let floor = match std::fs::read(owner.source.join("distribution-revision.json")) {
        Ok(raw) => serde_json::from_slice::<u64>(&raw)?,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => 0,
        Err(e) => return Err(e.into()),
    };
    storage::records::write_durable(
        &owner.target.join("distribution-revision.json"),
        &serde_json::to_vec(&floor)?,
    )?;
    storage::layout::complete(&owner.target, cli.product.name())?;
    let prepared = Prepared {
        schema: "transparent-prepared-storage/1".into(),
        ownership: owner,
        source_active_sha256: active_digest,
        configuration_sha256,
        map_sha256,
        anchor_height,
        anchor_hash,
        revision_floor: floor,
        files: preparation::fingerprint(&cli.target_root)?,
    };
    storage::records::write_durable(&record, &serde_json::to_vec(&prepared)?)?;
    println!(
        "{}",
        serde_json::json!({"prepared":true,"map_sha256":prepared.map_sha256})
    );
    Ok(())
}

fn require_resolved(source: &Path) -> Result<(), BoxError> {
    for intent in ["activation.json", "withdrawn.json", "halted.json"] {
        if source.join(intent).exists() {
            return Err(format!("source has unresolved state: {intent}").into());
        }
    }
    for (name, empty) in [
        ("invalidate.json", serde_json::json!({"rollback":null})),
        ("integrity-failures.json", serde_json::json!([])),
    ] {
        match std::fs::read(source.join(name)) {
            Ok(raw) => {
                let value = serde_json::from_slice::<serde_json::Value>(&raw)?;
                // The predecessor also saved an empty worker-acknowledgement
                // queue. It carries no unresolved work and is not migrated.
                let retired_empty = name == "invalidate.json"
                    && value == serde_json::json!({"pending":{},"rollback":null});
                if value != empty && !retired_empty {
                    return Err(format!("source has unresolved state: {name}").into());
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

fn history(
    owner: &Ownership,
    target: &Path,
    settings: &serde_json::Value,
) -> Result<(String, u64, String), BoxError> {
    let config: crate::history::controller::Config = serde_json::from_value(settings.clone())?;
    if config.publication_root != owner.target {
        return Err("History configuration names another target root".into());
    }
    storage::adopt_segments(target, &|| false)?;
    storage::adopt_runtimes(target, &|| false)?;
    let set = transparent_shared::history::shardset::ShardSet::open(
        target,
        transparent_shared::history::shardset::DEFAULT_RETAIN_REVISIONS,
    )?;
    let finalized =
        transparent_shared::publication::Finalized::prepare_full(target, &set, config.recent_from)?;
    let end = set.map.shards.last().ok_or("empty History publication")?;
    let record = finalized.restore_record(
        true,
        config.recent_from,
        end.end_height,
        &end.terminal_block_hash,
    )?;
    storage::records::write_durable(
        &target.join("history-restore.json"),
        &serde_json::to_vec(&record)?,
    )?;
    Ok((record.map_sha256, record.anchor_height, record.anchor_hash))
}
fn detail(
    owner: &Ownership,
    target: &Path,
    settings: &serde_json::Value,
    active: &[u8],
    clone: &preparation::Cloner,
) -> Result<(String, u64, String), BoxError> {
    use crate::tx_detail::publisher::{ActiveRecord, DisplayRoot, ROOT_FILE};
    let config: crate::tx_detail::service::Config = serde_json::from_value(settings.clone())?;
    if config.root != owner.target {
        return Err("transaction-detail configuration names another target root".into());
    }
    let layout = DisplayRoot::load(&owner.source)?;
    layout.validate()?;
    clone.file(&owner.source.join(ROOT_FILE), &owner.target.join(ROOT_FILE))?;
    let current: ActiveRecord = serde_json::from_slice(active)?;
    // Carry the derived lookup prefix too: rebuilding it from genesis belongs
    // in offline preparation, never in the stopped-owner startup interval.
    // Clone into an owned temporary directory so a failed/resumed attempt can
    // replace its earlier derived copy without touching immutable objects.
    {
        use crate::tx_detail::timeline::{HeightIndex, HEIGHTS_FILE};
        let temporary = tempfile::Builder::new()
            .prefix(".index-preparation-")
            .tempdir_in(&owner.target)?;
        clone.file(
            &owner.source.join(HEIGHTS_FILE),
            &temporary.path().join(HEIGHTS_FILE),
        )?;
        let mut index = HeightIndex::open(temporary.path())?;
        index.truncate_after(current.tip)?;
        std::fs::File::open(index.path())?.sync_all()?;
        let destination = owner.target.join(HEIGHTS_FILE);
        let parent = destination.parent().ok_or("derived index has no parent")?;
        std::fs::create_dir_all(parent)?;
        if destination.is_symlink() {
            return Err("derived index has foreign ownership".into());
        }
        std::fs::rename(index.path(), &destination)?;
        std::fs::File::open(parent)?.sync_all()?;
    }
    let map = crate::tx_detail::publisher::read_map(target, &current.map_sha256)?;
    for entry in &map.shards {
        let tier = if entry.sealed { "sealed" } else { "recent" };
        let destination = owner.target.join(tier).join(&entry.manifest_digest);
        let from = owner.source.join(tier).join(&entry.manifest_digest);
        clone.tree_from(
            from.parent().ok_or("revision has no tier")?,
            &from,
            &destination,
            &owner.target.join(".objects"),
        )?;
    }
    for tier in ["sealed", "recent"] {
        std::fs::create_dir_all(owner.target.join(tier))?;
        storage::adopt_segments(&owner.target.join(tier), &|| false)?;
    }
    storage::adopt_segments(target, &|| false)?;
    storage::adopt_indexes(target, &|| false)?;
    storage::adopt_runtimes(target, &|| false)?;
    let set = transparent_shared::tx_detail::set::DisplaySet::open(
        target,
        0,
        transparent_shared::tx_detail::DisplayRole::Combined,
    )?;
    let finalized =
        transparent_shared::tx_detail::distribution::Finalized::prepare_set(target, &set)?;
    Ok((
        finalized.inventory.map_sha256.clone(),
        finalized.anchor_height,
        finalized.anchor_hash,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tx_detail::{fixture, publisher};
    use std::os::unix::fs::MetadataExt;

    /// Materialize the new producer's fixture references as predecessor files.
    fn legacy(directory: &Path) {
        for entry in std::fs::read_dir(directory).unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_dir() {
                if entry.file_name() != ".objects" {
                    legacy(&entry.path());
                }
            } else if entry.file_type().unwrap().is_symlink() {
                let bytes = std::fs::read(entry.path()).unwrap();
                std::fs::remove_file(entry.path()).unwrap();
                std::fs::write(entry.path(), bytes).unwrap();
            }
        }
    }
    #[test]
    fn prepared_details_resume_verify_and_leave_predecessor_readable() {
        use transparent_shared::tx_detail::{
            prebuild::build_shipped_verified_until, set::DisplaySet, DisplayRole,
        };
        let temp = tempfile::tempdir().unwrap();
        let base = std::fs::canonicalize(temp.path()).unwrap();
        let journal = base.join("journal");
        fixture::write_journal(&journal, 100, 112, 0);
        let store =
            transparent_shared::journal::events::EventStore::open_existing(&journal).unwrap();
        let source = base.join("source");
        let layout = fixture::layout(&store, 101, 4);
        let done = publisher::bootstrap(&store, &source, &layout, 112).unwrap();
        let set = DisplaySet::open(&done.candidate, 0, DisplayRole::Combined).unwrap();
        for revision in set.current() {
            build_shipped_verified_until(revision, &done.candidate, &[], &|| false).unwrap();
        }
        // The old writer can index a block before it publishes that block.
        let mut source_index = crate::tx_detail::timeline::HeightIndex::open(&source).unwrap();
        source_index
            .append(113, &[fixture::record(999, 1)])
            .unwrap();
        let expected_index: Vec<_> = source_index
            .read_all()
            .unwrap()
            .into_iter()
            .filter(|(_, height)| *height <= 112)
            .collect();
        legacy(&source);
        let before = preparation::fingerprint(&source).unwrap();
        let active = std::fs::read(source.join("active.json")).unwrap();
        let target = base.join("prepared");
        let config = base.join("publisher.json");
        std::fs::write(
            &config,
            serde_json::to_vec(&serde_json::json!({"transaction_details":{"settings":{
                "root": target, "journal": journal, "rpc_cookie":"/unused", "poll_ms":1,
                "status_listen":"127.0.0.1:8098", "max_recent_records": null
            }}}))
            .unwrap(),
        )
        .unwrap();
        let cli = |verify_only| Cli {
            config: config.clone(),
            product: Product::TransactionDetails,
            source_root: source.clone(),
            target_root: target.clone(),
            verify_only,
        };
        let clone = preparation::Cloner::for_test();
        prepare_with(cli(false), &clone).unwrap();
        prepare_with(cli(false), &clone).unwrap();
        prepare_with(cli(true), &clone).unwrap();
        assert_eq!(std::fs::read(source.join("active.json")).unwrap(), active);
        assert_eq!(preparation::fingerprint(&source).unwrap(), before);
        // The predecessor's strict regular-file reader can still verify it.
        for revision in set.current() {
            publisher::verify_dir(&revision.dir, &revision.digest).unwrap();
        }
        let prepared: Prepared =
            serde_json::from_slice(&std::fs::read(target.join(RECORD)).unwrap()).unwrap();
        prepared.verify().unwrap();
        let saved_files = preparation::fingerprint(&target).unwrap();
        let mut advanced: publisher::ActiveRecord = serde_json::from_slice(&active).unwrap();
        advanced.cycle += 1;
        advanced.store(&source).unwrap();
        let advanced_source = preparation::fingerprint(&source).unwrap();
        prepare_with(cli(false), &clone).unwrap();
        assert_eq!(preparation::fingerprint(&target).unwrap(), saved_files);

        let saved: publisher::ActiveRecord =
            serde_json::from_slice(&std::fs::read(target.join("active.json")).unwrap()).unwrap();
        let next = DisplaySet::open(&saved.directory, 0, DisplayRole::Combined).unwrap();
        assert_eq!(next.map_digest, done.map_sha256);
        let index_file = crate::tx_detail::timeline::HEIGHTS_FILE;
        let reader = tempfile::tempdir().unwrap();
        let reader_path = reader.path().join(index_file);
        std::fs::create_dir_all(reader_path.parent().unwrap()).unwrap();
        std::fs::copy(target.join(index_file), &reader_path).unwrap();
        let copied_index = crate::tx_detail::timeline::HeightIndex::open(reader.path()).unwrap();
        assert_eq!(copied_index.read_all().unwrap(), expected_index);
        assert_ne!(
            std::fs::metadata(target.join(index_file)).unwrap().ino(),
            std::fs::metadata(source.join(index_file)).unwrap().ino()
        );
        for revision in next.current() {
            let name = revision.manifest.tables()[0].file_name(0);
            let new = revision.dir.join(&name);
            let old = done.candidate.join(&revision.digest).join(&name);
            assert_ne!(
                std::fs::metadata(new).unwrap().ino(),
                std::fs::metadata(old).unwrap().ino()
            );
        }
        // Replacement with identical bytes still invalidates the receipt.
        let object = std::fs::read_dir(target.join(".objects"))
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        let replacement = target.join("replacement");
        std::fs::write(&replacement, std::fs::read(&object).unwrap()).unwrap();
        std::fs::rename(replacement, object).unwrap();
        assert!(prepare_with(cli(true), &clone).is_err());
        assert!(prepare_with(cli(false), &clone).is_err());
        assert_eq!(preparation::fingerprint(&source).unwrap(), advanced_source);
    }
    #[test]
    fn prepared_history_clones_and_resumes_predecessor_table_and_runtime_references() {
        use crate::history::publication::{publish, DirectoryChoice, PublishOptions};
        use transparent_shared::{
            history::shardset::{ShardSet, DEFAULT_RETAIN_REVISIONS},
            runtime::prebuild::prepare_publication,
        };
        use zakura_pir_publisher::transparent::filter::{BlockHash, RANGE_PROFILE};
        let temp = tempfile::tempdir().unwrap();
        let base = std::fs::canonicalize(temp.path()).unwrap();
        let journal = base.join("journal");
        fixture::write_journal(&journal, 0, 2, 0);
        let store =
            transparent_shared::journal::events::EventStore::open_existing(&journal).unwrap();
        let source = base.join("source");
        let directory = source.join("initial");
        let map = publish(
            &PublishOptions {
                data_dir: journal.clone(),
                output: directory.clone(),
                previous: None,
                recent_geometry: "decoupled-2k-8k".into(),
                archive_geometry: None,
                recent_from: None,
                zakura_rpc_url: String::new(),
                zakura_cookie: PathBuf::new(),
                through: None,
                record: None,
                source_sha: None,
                directory_choice: DirectoryChoice::Off,
                range_profile: RANGE_PROFILE.into(),
            },
            &store,
            BlockHash::from_internal_bytes([0; 32]),
        )
        .unwrap();
        let set = ShardSet::open(&directory, DEFAULT_RETAIN_REVISIONS).unwrap();
        prepare_publication(&set, &directory, &[], &|| false).unwrap();
        storage::adopt_segments(&directory, &|| false).unwrap();
        storage::adopt_runtimes(&directory, &|| false).unwrap();
        assert!(std::fs::read_dir(&directory).unwrap().any(|entry| {
            let entry = entry.unwrap();
            entry.path().extension().is_some_and(|ext| ext == "runtime")
                && entry.file_type().unwrap().is_symlink()
        }));
        let end = map.shards.last().unwrap();
        std::fs::write(source.join("active.json"), serde_json::to_vec(&serde_json::json!({
            "directory":directory, "map_sha256":set.map_digest, "height":end.end_height, "hash":end.terminal_block_hash,
        })).unwrap()).unwrap();
        let before = preparation::fingerprint(&source).unwrap();
        let target = base.join("prepared");
        let config = base.join("publisher.json");
        std::fs::write(&config, serde_json::to_vec(&serde_json::json!({"transparent_history":{"settings":{
            "data_dir":journal, "publication_root":target, "initial_publication":target.join("initial"),
            "recent_from":0, "recent_geometry":"decoupled-2k-8k", "archive_geometry":"decoupled-2k-8k",
            "rpc_url":"http://127.0.0.1:8232", "rpc_cookie":"/unused", "listen":"127.0.0.1:8096", "source_sha":"fixture"
        }}})).unwrap()).unwrap();
        let cli = |verify_only| Cli {
            config: config.clone(),
            product: Product::History,
            source_root: source.clone(),
            target_root: target.clone(),
            verify_only,
        };
        let clone = preparation::Cloner::for_test();
        prepare_with(cli(false), &clone).unwrap();
        // A crash can leave adopted references before the final receipt exists.
        std::fs::remove_file(target.join(RECORD)).unwrap();
        prepare_with(cli(false), &clone).unwrap();
        prepare_with(cli(true), &clone).unwrap();
        assert_eq!(preparation::fingerprint(&source).unwrap(), before);
        assert!(target.join("initial/history-restore.json").is_file());
        assert_eq!(
            ShardSet::open(&target.join("initial"), DEFAULT_RETAIN_REVISIONS)
                .unwrap()
                .map_digest,
            set.map_digest
        );
        let before = preparation::fingerprint(&target).unwrap();
        std::fs::write(source.join("withdrawn.json"), b"unresolved").unwrap();
        // Even an unchanged active pointer cannot hide new withdrawal state.
        assert!(prepare_with(cli(false), &clone).is_err());
        assert_eq!(preparation::fingerprint(&target).unwrap(), before);
    }
    #[test]
    fn empty_retired_acknowledgements_are_resolved_but_pending_work_is_not() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("invalidate.json");
        std::fs::write(&path, br#"{"pending":{},"rollback":null}"#).unwrap();
        require_resolved(root.path()).unwrap();
        std::fs::write(
            &path,
            br#"{"pending":{"worker":"unacknowledged"},"rollback":null}"#,
        )
        .unwrap();
        assert!(require_resolved(root.path()).is_err());
    }

    #[tokio::test]
    async fn read_only_handoff_rechecks_the_anchor_with_the_current_node() {
        use axum::{extract::State, routing::post, Json, Router};
        use std::sync::{Arc, RwLock};
        let anchor = "33".repeat(32);
        let node = Arc::new(RwLock::new(anchor.clone()));
        async fn answer(
            State(node): State<Arc<RwLock<String>>>,
            Json(request): Json<serde_json::Value>,
        ) -> Json<serde_json::Value> {
            assert_eq!(request["method"], "getblockhash");
            assert_eq!(request["params"], serde_json::json!([12]));
            Json(
                serde_json::json!({"id":request["id"],"result":node.read().unwrap().clone(),"error":null}),
            )
        }
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let router = Router::new()
            .route("/", post(answer))
            .with_state(node.clone());
        let server = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        let temp = tempfile::tempdir().unwrap();
        let source = temp.path().join("source");
        let target = temp.path().join("prepared");
        std::fs::create_dir(&target).unwrap();
        storage::layout::complete(&target, "transparent_history").unwrap();
        let cookie = temp.path().join("node-cookie");
        std::fs::write(&cookie, b"fixture:fixture").unwrap();
        let config = temp.path().join("publisher.json");
        let bytes = serde_json::to_vec(&serde_json::json!({"transparent_history":{"settings":{
            "rpc_url":url,"rpc_cookie":cookie
        }}}))
        .unwrap();
        std::fs::write(&config, &bytes).unwrap();
        let record = Prepared {
            schema: "transparent-prepared-storage/1".into(),
            ownership: Ownership {
                schema: "transparent-storage-preparation/1".into(),
                product: "transparent_history".into(),
                source: source.clone(),
                target: target.clone(),
            },
            source_active_sha256: "11".repeat(32),
            configuration_sha256: preparation::digest(&bytes),
            map_sha256: "22".repeat(32),
            anchor_height: 12,
            anchor_hash: anchor,
            revision_floor: 0,
            files: preparation::fingerprint(&target).unwrap(),
        };
        std::fs::write(target.join(RECORD), serde_json::to_vec(&record).unwrap()).unwrap();
        let args = || {
            vec![
                "prepare-transparent-storage".into(),
                "--config".into(),
                config.clone().into_os_string(),
                "--source-root".into(),
                source.clone().into_os_string(),
                "--target-root".into(),
                target.clone().into_os_string(),
                "--product".into(),
                "history".into(),
                "--verify-only".into(),
            ]
        };
        let before = preparation::fingerprint(&target).unwrap();
        run(args()).await.unwrap();
        *node.write().unwrap() = "44".repeat(32);
        assert!(run(args())
            .await
            .unwrap_err()
            .to_string()
            .contains("no longer canonical"));
        assert_eq!(preparation::fingerprint(&target).unwrap(), before);
        server.abort();
    }
}
