//! Durable copies of served PIR tables in the data directory.
//!
//! Each prepared table is written once beside its session manifest and public setup,
//! named by its directory or witness revision. Inventories then hold only
//! [`Source`]s, and the serving cache prepares a table from its files when a query needs
//! it and the budget allows, so sealed history no longer stays resident. A restart finds
//! every unchanged table by revision and prepares none of them until queried.
//!
//! Sealed shards' receiver filters are written here too, named by digest, and served
//! from these files through the same cache, so inventories share one source per filter
//! and hold none of its bytes.
use receiver_pir_server::{
    publication::Segment,
    tables::{FilterSource, Loaded, Source, Tables},
};
use std::{
    collections::{HashMap, HashSet},
    io::Write,
    path::{Path, PathBuf},
    sync::Arc,
};
use zakura_pir_publisher::receiver::session::Error;
use zakura_pir_publisher::receiver::session::Manifest;
use zakura_pir_publisher::receiver::session::WitnessManifest;
use zakura_pir_publisher::receiver::{shards::ShardFilter, snapshot::Snapshot, witness, Hash};
use zakura_pir_query_server::receiver::server::{Server, WitnessServer};

/// The data directory's subdirectory holding table files.
pub const TABLES_DIR: &str = "tables";

/// What a stored file set holds.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
enum Kind {
    Payments,
    Witnesses,
    Filter,
}

impl Kind {
    /// Its files' extensions; the last written marks it stored.
    fn extensions(self) -> &'static [&'static str] {
        match self {
            Kind::Payments | Kind::Witnesses => &["rows", "public", "json"],
            Kind::Filter => &["filter"],
        }
    }
}

/// One table's files by revision, payment (`p`) or witness (`w`), or a filter's (`f`)
/// by digest.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
struct Name {
    kind: Kind,
    revision: Hash,
}

impl Name {
    /// The file holding its rows or filter, which reuse compares with rebuilt data.
    fn data_file(&self) -> String {
        let extension = match self.kind {
            Kind::Payments | Kind::Witnesses => "rows",
            Kind::Filter => "filter",
        };
        format!("{}.{extension}", self.stem())
    }

    /// The file stem `<kind>-<revision>`.
    fn stem(&self) -> String {
        let kind = match self.kind {
            Kind::Payments => "p",
            Kind::Witnesses => "w",
            Kind::Filter => "f",
        };
        format!("{kind}-{}", hex::encode(self.revision))
    }

    /// The name a file stem encodes, if it is one of ours.
    fn parse(stem: &str) -> Option<Self> {
        let (kind, revision) = stem.split_once('-')?;
        Some(Self {
            kind: match kind {
                "p" => Kind::Payments,
                "w" => Kind::Witnesses,
                "f" => Kind::Filter,
                _ => return None,
            },
            revision: hex::decode(revision).ok()?.try_into().ok()?,
        })
    }
}

/// The table files of one data directory and the cache serving them.
pub struct Durable {
    dir: PathBuf,
    tables: Arc<Tables>,
    /// Every stored table's session ID, and every stored filter's digest.
    stored: HashMap<Name, Hash>,
    /// Tables this handle prepared because no stored copy existed.
    prepared: u64,
}

impl Durable {
    /// The table files under `data_dir`, served through `tables`; leftovers of an
    /// interrupted write are removed.
    pub fn open(data_dir: &Path, tables: Arc<Tables>) -> std::io::Result<Self> {
        let dir = data_dir.join(TABLES_DIR);
        std::fs::create_dir_all(&dir)?;
        let mut stored = HashMap::new();
        for entry in std::fs::read_dir(&dir)? {
            let path = entry?.path();
            let file = path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or_default();
            if file.ends_with(".tmp") {
                std::fs::remove_file(&path)?;
            } else if let Some(name) = file.strip_suffix(".filter").and_then(Name::parse) {
                // Written whole under its digest, which every load checks.
                if name.kind == Kind::Filter {
                    stored.insert(name, name.revision);
                }
            } else if let Some(name) = file.strip_suffix(".json").and_then(Name::parse) {
                // A manifest that is oversized or does not parse is dropped with its
                // table below as a leftover, and its revision is prepared afresh.
                let Ok(bytes) = pir_publication::records::read_bounded(
                    &path,
                    zakura_pir_publisher::receiver::session::MAX_MANIFEST_BYTES as u64,
                ) else {
                    continue;
                };
                let session = match name.kind {
                    Kind::Payments => serde_json::from_slice::<Manifest>(&bytes)
                        .ok()
                        .and_then(|m| m.id().ok()),
                    Kind::Witnesses => serde_json::from_slice::<WitnessManifest>(&bytes)
                        .ok()
                        .and_then(|m| m.id().ok()),
                    Kind::Filter => None,
                };
                if let Some(session) = session {
                    stored.insert(name, session);
                }
            }
        }
        // Files of a table whose manifest was never written are leftovers.
        for entry in std::fs::read_dir(&dir)? {
            let path = entry?.path();
            let stem = path
                .file_stem()
                .and_then(|n| n.to_str())
                .and_then(Name::parse);
            if stem.is_none_or(|name| !stored.contains_key(&name)) {
                std::fs::remove_file(&path)?;
            }
        }
        Ok(Self {
            dir,
            tables,
            stored,
            prepared: 0,
        })
    }

    /// How many tables this handle prepared because no stored copy existed.
    pub fn prepared(&self) -> u64 {
        self.prepared
    }

    /// The cache this directory's tables are served through.
    pub fn tables(&self) -> &Arc<Tables> {
        &self.tables
    }

    /// The segment serving freshly built `snapshot` and its companion `witnesses`, both
    /// already checked: each table reuses its stored copy when one exists, and otherwise
    /// is prepared, written and cached. Blocking.
    pub fn segment(
        &mut self,
        snapshot: Snapshot,
        witnesses: witness::Table,
    ) -> Result<Segment, Error> {
        let name = Name {
            kind: Kind::Payments,
            revision: snapshot.manifest.revision()?,
        };
        let stored = match self.stored.contains_key(&name) {
            true => self.reuse(name, &snapshot.data, |d| d.payment_source(name))?,
            false => None,
        };
        let (payments, server) = match stored {
            Some(source) => (source, None),
            None => {
                // Rows are written before preparation consumes them; the manifest, written
                // last, marks the table stored.
                self.write_file(name, "rows", &snapshot.data)?;
                let server = prepare_payments(snapshot)?;
                self.prepared += 1;
                self.write_file(name, "public", server.public())?;
                self.write_file(name, "json", &serde_json::to_vec(server.manifest())?)?;
                self.stored.insert(name, server.id());
                let source = self.payment_source(name)?;
                self.tables
                    .insert(&source, Loaded::Payments(server.clone()));
                (source, Some(server))
            }
        };
        let witnesses = self.companion(&payments, server.as_ref(), witnesses)?;
        Ok(Segment {
            payments,
            witnesses,
        })
    }

    /// `segment`'s payment table with a rebuilt companion `witnesses`, already checked.
    /// Blocking.
    pub fn rewitness(
        &mut self,
        payments: &Arc<Source>,
        witnesses: witness::Table,
    ) -> Result<Segment, Error> {
        let witnesses = self.companion(payments, None, witnesses)?;
        Ok(Segment {
            payments: payments.clone(),
            witnesses,
        })
    }

    /// The stored rows of payment table `payments`, validated against its manifest.
    /// Blocking.
    pub fn snapshot(&self, payments: &Source) -> Result<Snapshot, Error> {
        let directory = &payments
            .payment_manifest()
            .ok_or(Error::Malformed)?
            .directory;
        let name = Name {
            kind: Kind::Payments,
            revision: directory.revision()?,
        };
        let snapshot = Snapshot {
            manifest: directory.clone(),
            data: read(&self.path(name, "rows"), rows_bytes(directory.rows))?,
        };
        snapshot.validate()?;
        Ok(snapshot)
    }

    /// The source of sealed shard filter `filter`, written here once by digest. Its
    /// bytes are read back only when a request serves them. Blocking.
    pub fn filter(&mut self, filter: &ShardFilter) -> Result<Arc<FilterSource>, Error> {
        let name = Name {
            kind: Kind::Filter,
            revision: filter.digest(),
        };
        let reused = match self.stored.contains_key(&name) {
            true => self.reuse(name, filter.bytes(), |_| Ok(()))?,
            false => None,
        };
        if reused.is_none() {
            self.write_file(name, "filter", filter.bytes())?;
            self.stored.insert(name, name.revision);
        }
        let (path, len) = (self.path(name, "filter"), filter.bytes().len());
        Ok(Arc::new(FilterSource::stored(
            filter.digest(),
            len as u32,
            filter.receivers(),
            move || read(&path, len),
        )))
    }

    /// Removes every stored table whose session, and every filter whose digest, `live`
    /// does not hold.
    pub fn retain(&mut self, live: &HashSet<Hash>) -> std::io::Result<()> {
        let dropped: Vec<Name> = self
            .stored
            .iter()
            .filter(|(_, session)| !live.contains(*session))
            .map(|(name, _)| *name)
            .collect();
        for name in dropped {
            self.forget(name)?;
        }
        Ok(())
    }

    /// The source `source` builds over stored entry `name`, whose rows or filter must be
    /// exactly `data`, the copy just rebuilt from the index. Every reuse of a stored copy
    /// goes through this check. If the stored data differs or is missing, or the files do
    /// not form a valid source, such as a public setup that is not the one its manifest
    /// binds, the entry's files are removed and `None` returned, so the caller writes
    /// and prepares it again instead of serving it.
    fn reuse<T>(
        &mut self,
        name: Name,
        data: &[u8],
        source: impl FnOnce(&Self) -> Result<T, Error>,
    ) -> Result<Option<T>, Error> {
        let built = match self.holds(name, data) {
            true => source(self),
            false => Err(Error::Transport(format!(
                "{} differs from the rebuilt data",
                name.data_file()
            ))),
        };
        match built {
            Ok(source) => Ok(Some(source)),
            Err(error) => {
                tracing::warn!(table = %name.stem(), %error, "stored copy invalid; writing it again");
                self.forget(name)
                    .map_err(|e| Error::Transport(format!("table {}: {e}", name.stem())))?;
                Ok(None)
            }
        }
    }

    /// Whether the stored rows or filter of `name` are exactly `data`, compared in bounded
    /// chunks without reading the file into memory.
    fn holds(&self, name: Name, data: &[u8]) -> bool {
        use std::io::Read;
        let path = self.dir.join(name.data_file());
        let Ok(mut file) = std::fs::File::open(path) else {
            return false;
        };
        if file.metadata().map(|m| m.len()).ok() != Some(data.len() as u64) {
            return false;
        }
        let mut chunk = vec![0; 1 << 20];
        let mut offset = 0;
        loop {
            match file.read(&mut chunk) {
                Ok(0) => return offset == data.len(),
                Ok(n) if data.get(offset..offset + n) == Some(&chunk[..n]) => offset += n,
                _ => return false,
            }
        }
    }

    /// Removes stored entry `name` and its files.
    fn forget(&mut self, name: Name) -> std::io::Result<()> {
        for extension in name.kind.extensions() {
            match std::fs::remove_file(self.path(name, extension)) {
                Err(e) if e.kind() != std::io::ErrorKind::NotFound => return Err(e),
                _ => {}
            }
        }
        self.stored.remove(&name);
        Ok(())
    }

    /// The source of `witnesses` beside the payment table `payments`, whose prepared
    /// server is `server` when the caller has it.
    fn companion(
        &mut self,
        payments: &Arc<Source>,
        server: Option<&Server>,
        witnesses: witness::Table,
    ) -> Result<Arc<Source>, Error> {
        let name = Name {
            kind: Kind::Witnesses,
            revision: witnesses.manifest.revision()?,
        };
        if self.stored.contains_key(&name) {
            let reused = self.reuse(name, &witnesses.data, |d| {
                d.witness_source(name, payments.clone())
            })?;
            if let Some(source) = reused {
                return Ok(source);
            }
        }
        let resident;
        let server = match server {
            Some(server) => server,
            None => {
                resident = self.tables.get(payments)?;
                resident.payments().ok_or(Error::Malformed)?
            }
        };
        self.write_file(name, "rows", &witnesses.data)?;
        let prepared = prepare_witnesses(witnesses, server)?;
        self.prepared += 1;
        self.write_file(name, "public", prepared.public())?;
        self.write_file(name, "json", &serde_json::to_vec(prepared.manifest())?)?;
        self.stored.insert(name, prepared.id());
        let source = self.witness_source(name, payments.clone())?;
        self.tables.insert(&source, Loaded::Witnesses(prepared));
        Ok(source)
    }

    /// A stored payment table's source, which reloads it from its files.
    /// A reload prepares the rows under the manifest read here, never a later one, and
    /// reads exactly the rows that manifest's geometry holds, so a replaced file cannot
    /// change the geometry or grow the allocation.
    fn payment_source(&self, name: Name) -> Result<Arc<Source>, Error> {
        let manifest: Manifest = serde_json::from_slice(&self.manifest(name)?)?;
        if manifest.directory.revision()? != name.revision {
            return Err(Error::Malformed);
        }
        let rows = manifest.directory.rows;
        let public = read(
            &self.path(name, "public"),
            zakura_pir_publisher::receiver::native::public_bytes(rows)?,
        )?;
        let (directory, path) = (manifest.directory.clone(), self.path(name, "rows"));
        let source = Source::payments(manifest, public, move || {
            prepare_payments(Snapshot {
                manifest: directory.clone(),
                data: read(&path, rows_bytes(rows))?,
            })
        })?;
        Ok(Arc::new(source))
    }

    /// A stored companion's source beside `payments`, which reloads it from its files as
    /// [`Self::payment_source`] does.
    fn witness_source(&self, name: Name, payments: Arc<Source>) -> Result<Arc<Source>, Error> {
        let manifest: WitnessManifest = serde_json::from_slice(&self.manifest(name)?)?;
        if manifest.table.revision()? != name.revision {
            return Err(Error::Malformed);
        }
        let rows = manifest.table.rows;
        let public = read(
            &self.path(name, "public"),
            zakura_pir_publisher::receiver::native::public_bytes(rows)?,
        )?;
        let (table, path) = (manifest.table.clone(), self.path(name, "rows"));
        let source = Source::witnesses(manifest, public, payments, move || {
            Ok(witness::Table {
                manifest: table.clone(),
                data: read(&path, rows_bytes(rows))?,
            })
        })?;
        Ok(Arc::new(source))
    }

    /// Durably writes one of a table's files.
    fn write_file(&self, name: Name, extension: &str, bytes: &[u8]) -> Result<(), Error> {
        let path = self.path(name, extension);
        let temp = path.with_extension(format!("{extension}.tmp"));
        let written = std::fs::File::create(&temp)
            .and_then(|mut file| {
                file.write_all(bytes)?;
                file.sync_all()
            })
            .and_then(|_| std::fs::rename(&temp, &path))
            .and_then(|_| std::fs::File::open(&self.dir)?.sync_all());
        written.map_err(|e| Error::Transport(format!("table file {}: {e}", path.display())))
    }

    /// One of a table's files.
    fn path(&self, name: Name, extension: &str) -> PathBuf {
        self.dir.join(format!("{}.{extension}", name.stem()))
    }

    /// A table's session manifest file, within the manifest size bound.
    fn manifest(&self, name: Name) -> Result<Vec<u8>, Error> {
        let path = self.path(name, "json");
        pir_publication::records::read_bounded(
            &path,
            zakura_pir_publisher::receiver::session::MAX_MANIFEST_BYTES as u64,
        )
        .map_err(|e| Error::Transport(format!("table file {}: {e}", path.display())))
    }
}

/// A file that must hold exactly `len` bytes, refused without reading more, a failure as
/// a transport error the cache reports.
fn read(path: &Path, len: usize) -> Result<Vec<u8>, Error> {
    pir_publication::records::read_exact_len(path, len as u64)
        .map_err(|e| Error::Transport(format!("table file {}: {e}", path.display())))
}

/// The bytes of a `rows`-row table's rows.
fn rows_bytes(rows: u32) -> usize {
    rows as usize * zakura_pir_publisher::receiver::snapshot::ROW_BYTES
}

/// Prepares a payment table, reusing identical fixtures in tests.
fn prepare_payments(snapshot: Snapshot) -> Result<Server, Error> {
    #[cfg(test)]
    {
        crate::prepared::payments(snapshot)
    }
    #[cfg(not(test))]
    {
        Server::new(snapshot)
    }
}

/// Prepares a companion beside `payments`, reusing identical fixtures in tests.
fn prepare_witnesses(table: witness::Table, payments: &Server) -> Result<WitnessServer, Error> {
    #[cfg(test)]
    {
        crate::prepared::companion(table, payments)
    }
    #[cfg(not(test))]
    {
        WitnessServer::new(table, payments)
    }
}

