//! Durable copies of served PIR tables in the data directory.
//!
//! Each prepared table is written once beside its session and public setup, named by
//! its directory or companion revision; a directory's choice object is written beside
//! its rows. Inventories then hold only sources, and the serving cache prepares a
//! table from its files when a query needs it and the budget allows, so sealed history
//! does not stay resident. A restart finds every unchanged table by revision and
//! prepares none of them until queried. Files of any other name, such as an earlier
//! format's tables, are removed on open.
//!
//! Sealed shards' receiver filters are written here too, named by digest, and served
//! from these files through the same cache.
use receiver_pir_server::{
    publication::Segment,
    tables::{Loaded, ObjectSource, Reservation, Source, Tables},
};
use std::{
    collections::{HashMap, HashSet},
    io::Write,
    path::{Path, PathBuf},
    sync::Arc,
};
use zakura_pir_publisher::receiver::session::{Error, MAX_MANIFEST_BYTES};
use zakura_pir_publisher::receiver::{
    native::public_bytes, Companion, CompanionTable, Directory, DirectoryTable, Entry,
    Kind as TableKind, Session, Table, DIRECTORY_ROWS, ROW_BYTES,
};
use zakura_pir_publisher::receiver::{shards::ShardFilter, Hash};
use zakura_pir_query_server::receiver::server::Server;

/// How long a preparation waits for queries in flight to leave room for its peak.
const PREPARE_WAIT: std::time::Duration = std::time::Duration::from_secs(60);

/// The data directory's subdirectory holding table files.
pub const TABLES_DIR: &str = "tables";

/// What a stored file set holds.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
enum Kind {
    Directory,
    Companion,
    Filter,
}

impl Kind {
    /// Its files' extensions; the last written marks it stored.
    fn extensions(self) -> &'static [&'static str] {
        match self {
            Kind::Directory => &["rows", "choice", "public", "json"],
            Kind::Companion => &["rows", "public", "json"],
            Kind::Filter => &["filter"],
        }
    }
}

/// One table's files by revision, directory (`d`) or companion (`c`), or a filter's
/// (`f`) by digest.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
struct Name {
    kind: Kind,
    revision: Hash,
}

impl Name {
    /// The file holding its rows or filter, which reuse compares with rebuilt data.
    fn data_file(&self) -> String {
        let extension = match self.kind {
            Kind::Directory | Kind::Companion => "rows",
            Kind::Filter => "filter",
        };
        format!("{}.{extension}", self.stem())
    }

    /// The file stem `<kind>-<revision>`.
    fn stem(&self) -> String {
        let kind = match self.kind {
            Kind::Directory => "d",
            Kind::Companion => "c",
            Kind::Filter => "f",
        };
        format!("{kind}-{}", hex::encode(self.revision))
    }

    /// The name a file stem encodes, if it is one of ours.
    fn parse(stem: &str) -> Option<Self> {
        let (kind, revision) = stem.split_once('-')?;
        Some(Self {
            kind: match kind {
                "d" => Kind::Directory,
                "c" => Kind::Companion,
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
    #[cfg(test)]
    prepared: u64,
}

impl Durable {
    /// The table files under `data_dir`, served through `tables`; leftovers of an
    /// interrupted write and files of another format are removed.
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
                // A session that is oversized or does not parse is dropped with its
                // table below as a leftover, and its revision is prepared afresh.
                let Ok(bytes) =
                    pir_publication::records::read_bounded(&path, MAX_MANIFEST_BYTES as u64)
                else {
                    continue;
                };
                let session = match name.kind {
                    Kind::Directory => session_id::<Directory>(&bytes, &name.revision),
                    Kind::Companion => session_id::<Companion>(&bytes, &name.revision),
                    Kind::Filter => None,
                };
                if let Some(session) = session {
                    stored.insert(name, session);
                }
            }
        }
        // Files of a table whose session was never written are leftovers.
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
            #[cfg(test)]
            prepared: 0,
        })
    }

    /// How many tables this handle prepared because no stored copy existed.
    #[cfg(test)]
    pub fn prepared(&self) -> u64 {
        self.prepared
    }

    /// The segment serving freshly built and checked `tables`, whose directory has
    /// `entries`: each table reuses its stored copy when one exists, and otherwise is
    /// prepared, written and cached. Blocking.
    pub fn segment(
        &mut self,
        tables: zakura_pir_publisher::receiver::Segment,
        entries: &[Entry],
    ) -> Result<Segment, Error> {
        let manifest = tables.directory.manifest.clone();
        let name = Name {
            kind: Kind::Directory,
            revision: manifest.revision()?,
        };
        let stored = match self.stored.contains_key(&name) {
            // The choice object is checked as the rows are, since serving reads it.
            true => self.reuse(name, &tables.directory.data, |d| {
                match d.holds(&d.path(name, "choice"), &tables.directory.choice) {
                    true => d.directory_source(name),
                    false => Err(Error::Transport(
                        "choice differs from the rebuilt one".into(),
                    )),
                }
            })?,
            false => None,
        };
        let directory = match stored {
            Some(source) => source,
            None => {
                // Rows and choice are written before preparation consumes them; the
                // session, written last, marks the table stored.
                self.write_file(name, "rows", &tables.directory.data)?;
                self.write_file(name, "choice", &tables.directory.choice)?;
                let reserved = self.reserve(TableKind::Directory, DIRECTORY_ROWS)?;
                let server = prepare_directory(tables.directory)?;
                #[cfg(test)]
                {
                    self.prepared += 1;
                }
                self.write_file(name, "public", server.public())?;
                self.write_file(name, "json", &serde_json::to_vec(server.session())?)?;
                self.stored.insert(name, server.id());
                let source = self.directory_source(name)?;
                self.tables
                    .insert(&source, Loaded::Directory(server), reserved);
                source
            }
        };
        let choice = self.choice_source(name, &manifest)?;
        let companion = self.companion(&manifest, entries, tables.companion)?;
        Ok(Segment {
            directory,
            companion,
            choice,
        })
    }

    /// `kept`'s directory with a rebuilt and checked companion `table`, whose
    /// directory has `entries`. Blocking.
    pub fn rebuilt(
        &mut self,
        kept: &Segment,
        entries: &[Entry],
        table: CompanionTable,
    ) -> Result<Segment, Error> {
        let directory = &kept
            .directory
            .directory_session()
            .ok_or(Error::Malformed)?
            .manifest;
        Ok(Segment {
            companion: self.companion(directory, entries, table)?,
            ..kept.clone()
        })
    }

    /// The source of sealed shard filter `filter`, written here once by digest. Its
    /// bytes are read back only when a request serves them. Blocking.
    pub fn filter(&mut self, filter: &ShardFilter) -> Result<Arc<ObjectSource>, Error> {
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
        Ok(Arc::new(ObjectSource::filter(
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
    /// not form a valid source, such as a public setup that is not the one its session
    /// binds, the entry's files are removed and `None` returned, so the caller writes
    /// and prepares it again instead of serving it.
    fn reuse<T>(
        &mut self,
        name: Name,
        data: &[u8],
        source: impl FnOnce(&Self) -> Result<T, Error>,
    ) -> Result<Option<T>, Error> {
        let built = match self.holds(&self.dir.join(name.data_file()), data) {
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

    /// Whether the file at `path` holds exactly `data`, compared in bounded chunks
    /// without reading the file into memory.
    fn holds(&self, path: &Path, data: &[u8]) -> bool {
        use std::io::Read;
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

    /// The source of checked companion `table`, serving `directory` with `entries`:
    /// its stored copy, or prepared, written and cached.
    fn companion(
        &mut self,
        directory: &Directory,
        entries: &[Entry],
        table: CompanionTable,
    ) -> Result<Arc<Source>, Error> {
        let name = Name {
            kind: Kind::Companion,
            revision: table.manifest.revision()?,
        };
        let directory_name = Name {
            kind: Kind::Directory,
            revision: directory.revision()?,
        };
        if self.stored.contains_key(&name) {
            let reused = self.reuse(name, &table.data, |d| {
                d.companion_source(name, directory_name)
            })?;
            if let Some(source) = reused {
                return Ok(source);
            }
        }
        self.write_file(name, "rows", &table.data)?;
        let reserved = self.reserve(TableKind::Companion, table.manifest.rows)?;
        let prepared = prepare_companion(table, directory, entries)?;
        #[cfg(test)]
        {
            self.prepared += 1;
        }
        self.write_file(name, "public", prepared.public())?;
        self.write_file(name, "json", &serde_json::to_vec(prepared.session())?)?;
        self.stored.insert(name, prepared.id());
        let source = self.companion_source(name, directory_name)?;
        self.tables
            .insert(&source, Loaded::Companion(prepared), reserved);
        Ok(source)
    }

    /// A stored directory's session, read within the size bound and bound to `name`.
    fn session<M: Table + serde::de::DeserializeOwned>(
        &self,
        name: Name,
    ) -> Result<Session<M>, Error> {
        let session: Session<M> = serde_json::from_slice(&self.manifest(name)?)?;
        if session.manifest.revision()? != name.revision {
            return Err(Error::Malformed);
        }
        Ok(session)
    }

    /// A loader of stored directory `name`'s whole table under `manifest`, reading
    /// exactly the rows and choice length the manifest fixes.
    fn directory_table(
        &self,
        name: Name,
        manifest: Directory,
    ) -> impl Fn() -> Result<DirectoryTable, Error> + Clone {
        let (rows, choice) = (self.path(name, "rows"), self.path(name, "choice"));
        move || {
            Ok(DirectoryTable {
                data: read(&rows, DIRECTORY_ROWS as usize * ROW_BYTES)?,
                choice: read(&choice, manifest.choice.bytes as usize)?,
                manifest: manifest.clone(),
            })
        }
    }

    /// A stored directory's source, which reloads it from its files under the session
    /// read here, never a later one.
    fn directory_source(&self, name: Name) -> Result<Arc<Source>, Error> {
        let session = self.session::<Directory>(name)?;
        let public = read(&self.path(name, "public"), public_bytes())?;
        let table = self.directory_table(name, session.manifest.clone());
        let source = Source::directory(session, public, move || prepare_directory(table()?))?;
        Ok(Arc::new(source))
    }

    /// The choice object stored beside directory `name` with `manifest`.
    fn choice_source(&self, name: Name, manifest: &Directory) -> Result<Arc<ObjectSource>, Error> {
        let (path, len) = (self.path(name, "choice"), manifest.choice.bytes as usize);
        Ok(Arc::new(ObjectSource::choice(
            &manifest.choice,
            move || read(&path, len),
        )))
    }

    /// A stored companion's source, which reloads it from its files and checks it
    /// against stored directory `directory`, whose files it reads too.
    fn companion_source(&self, name: Name, directory: Name) -> Result<Arc<Source>, Error> {
        let session = self.session::<Companion>(name)?;
        let public = read(&self.path(name, "public"), public_bytes())?;
        let directory_manifest = self.session::<Directory>(directory)?.manifest;
        let directory = self.directory_table(directory, directory_manifest);
        let (manifest, path) = (session.manifest.clone(), self.path(name, "rows"));
        let source = Source::companion(session, public, move || {
            let directory = directory()?;
            let entries = directory.validate()?;
            let table = CompanionTable {
                data: read(&path, manifest.rows as usize * ROW_BYTES)?,
                manifest: manifest.clone(),
            };
            prepare_companion(table, &directory.manifest, &entries)
        })?;
        Ok(Arc::new(source))
    }

    /// The cache's charge for preparing a table here, as a query's cold load is charged;
    /// see `Tables::reserve_peak`. It waits up to [`PREPARE_WAIT`] for tables in use.
    fn reserve(&self, kind: TableKind, rows: u32) -> Result<Reservation, Error> {
        Ok(self.tables.reserve_peak(kind, rows, PREPARE_WAIT)?)
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

    /// A table's session file, within the manifest size bound.
    fn manifest(&self, name: Name) -> Result<Vec<u8>, Error> {
        let path = self.path(name, "json");
        pir_publication::records::read_bounded(&path, MAX_MANIFEST_BYTES as u64)
            .map_err(|e| Error::Transport(format!("table file {}: {e}", path.display())))
    }
}

/// The ID of the session in `bytes`, if it parses and its manifest has `revision`.
fn session_id<M: Table + serde::de::DeserializeOwned>(
    bytes: &[u8],
    revision: &Hash,
) -> Option<Hash> {
    let session: Session<M> = serde_json::from_slice(bytes).ok()?;
    (session.manifest.revision().ok()? == *revision)
        .then(|| session.id().ok())
        .flatten()
}

/// A file that must hold exactly `len` bytes, refused without reading more, a failure as
/// a transport error the cache reports.
fn read(path: &Path, len: usize) -> Result<Vec<u8>, Error> {
    pir_publication::records::read_exact_len(path, len as u64)
        .map_err(|e| Error::Transport(format!("table file {}: {e}", path.display())))
}

/// Prepares a directory, reusing identical fixtures in tests.
fn prepare_directory(table: DirectoryTable) -> Result<Server<Directory>, Error> {
    #[cfg(test)]
    {
        Ok(crate::prepared::directory(table)?.0)
    }
    #[cfg(not(test))]
    {
        Ok(Server::directory(table)?.0)
    }
}

/// Prepares a companion against `directory`'s `entries`, reusing identical fixtures in
/// tests.
fn prepare_companion(
    table: CompanionTable,
    directory: &Directory,
    entries: &[Entry],
) -> Result<Server<Companion>, Error> {
    #[cfg(test)]
    {
        crate::prepared::companion(table, directory, entries)
    }
    #[cfg(not(test))]
    {
        Server::companion(table, directory, entries)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A data directory an earlier release wrote keeps none of its tables: payment and
    /// witness files are not ours and are removed on open, while a filter written by
    /// digest stays.
    #[test]
    fn earlier_tables_are_dropped_on_open() {
        let root = tempfile::tempdir().unwrap();
        let dir = root.path().join(TABLES_DIR);
        std::fs::create_dir_all(&dir).unwrap();
        let hex = "ab".repeat(32);
        let earlier = [
            format!("p-{hex}.rows"),
            format!("p-{hex}.json"),
            format!("w-{hex}.public"),
        ];
        for name in &earlier {
            std::fs::write(dir.join(name), b"earlier").unwrap();
        }
        std::fs::write(dir.join(format!("f-{hex}.filter")), b"filter").unwrap();
        let durable = Durable::open(root.path(), Arc::new(Tables::new(1 << 30))).unwrap();
        assert!(earlier.iter().all(|name| !dir.join(name).exists()));
        assert!(dir.join(format!("f-{hex}.filter")).exists());
        assert_eq!(durable.stored.len(), 1);
    }
}
