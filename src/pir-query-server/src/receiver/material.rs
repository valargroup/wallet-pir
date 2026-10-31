//! Reconstruct lazy evaluator sources from verified cached objects.
use pir_publication::{VerifiedObjectFile, VerifiedObjects};
use receiver_pir_server::{
    blocks::BlockWindow,
    distribution::Inventory,
    publication::{Publication, Segment},
    tables::{self, ObjectSource, Source},
};
use std::{collections::HashSet, sync::Arc};
use zakura_pir_query_server::receiver::{
    blocks::{Range, MAX_RANGE_BLOCKS},
    inventory,
    server::Server,
    session::Error,
    CompanionTable, DirectoryTable, Hash,
};

fn read(file: &VerifiedObjectFile) -> Result<Vec<u8>, Error> {
    file.read_bytes().map_err(|_| Error::Malformed)
}

/// The table sources built for one directory. A later publication that names
/// the same files, such as the one a restarted publisher serves again, reuses
/// them and whatever rows they already hold. A publication that replaces a
/// loaded one prepares its new tables in `cache` before it is served, so the
/// first wallet to query a new shard does not wait for its preparation.
#[derive(Clone)]
pub(super) struct Tables {
    pub(super) key: Vec<u8>,
    segments: Vec<Segment>,
    filters: Vec<Arc<ObjectSource>>,
}

pub(super) fn load(
    inventory: &Inventory,
    objects: &VerifiedObjects,
    owner: Arc<std::fs::File>,
    previous: Option<Tables>,
    cache: &tables::Tables,
) -> crate::Result<(Option<Publication>, Option<BlockWindow>, Option<Tables>)> {
    let mut built = None;
    let directory = inventory
        .directory
        .as_ref()
        .map(|directory| -> crate::Result<Publication> {
            let key =
                serde_json::to_vec(&(inventory.genesis, inventory.history_revision, directory))?;
            let tables = match previous {
                Some(tables) if tables.key == key => tables,
                previous => {
                    let tables = build(directory, objects, owner, key)?;
                    if let Some(previous) = previous {
                        tables.prepare_new(&previous, cache);
                    }
                    tables
                }
            };
            let publication = Publication::verified(
                inventory::Inventory::build(
                    inventory.genesis,
                    inventory.history_revision,
                    directory.shards.clone(),
                )?,
                tables.segments.clone(),
                tables.filters.clone(),
            )?;
            built = Some(tables);
            Ok(publication)
        })
        .transpose()?;
    Ok((directory, load_filters(inventory, objects)?, built))
}

impl Tables {
    /// Prepares in `cache` each table these name that `previous` did not, such as
    /// the shard a new block extended; tables both name stay cached as they are.
    /// Preparing is best effort: a table that does not fit or fails to load is
    /// left to the first query that needs it, which reports the failure.
    fn prepare_new(&self, previous: &Tables, cache: &tables::Tables) {
        let known: HashSet<_> = previous
            .segments
            .iter()
            .flat_map(|s| [s.directory.id(), s.companion.id()])
            .collect();
        for source in self
            .segments
            .iter()
            .flat_map(|s| [&s.directory, &s.companion])
            .filter(|source| !known.contains(&source.id()))
        {
            if let Err(error) = cache.get(source) {
                tracing::warn!(%error, "Receiver table not prepared before serving");
            }
        }
    }
}

fn build(
    directory: &receiver_pir_server::distribution::Directory,
    objects: &VerifiedObjects,
    owner: Arc<std::fs::File>,
    key: Vec<u8>,
) -> crate::Result<Tables> {
    let mut segments = Vec::new();
    for segment in &directory.segments {
        let (rows, choice) = (
            objects.get(&segment.directory)?,
            objects.get(&segment.choice)?,
        );
        let choice_rows = choice.clone();
        let session = segment.publication.directory.clone();
        // Reads and checks the directory: the companion's load needs its entries too.
        let table = {
            let (owner, manifest) = (owner.clone(), session.manifest.clone());
            move || {
                let _owner = &owner;
                Ok::<_, Error>(DirectoryTable {
                    manifest: manifest.clone(),
                    data: read(&rows)?,
                    choice: read(&choice_rows)?,
                })
            }
        };
        let public = objects.read(&segment.directory_public)?;
        let directory_table = table.clone();
        let directory = Arc::new(Source::directory(session.clone(), public, move || {
            Ok(Server::directory(directory_table()?)?.0)
        })?);
        let companion_rows = objects.get(&segment.companion)?;
        let companion_session = segment.publication.companion.clone();
        let public = objects.read(&segment.companion_public)?;
        let (manifest, owner_c) = (companion_session.manifest.clone(), owner.clone());
        let companion = Arc::new(Source::companion(companion_session, public, move || {
            let _owner = &owner_c;
            let directory = table()?;
            let entries = directory.validate()?;
            let table = CompanionTable {
                manifest: manifest.clone(),
                data: read(&companion_rows)?,
            };
            Server::companion(table, &directory.manifest, &entries)
        })?);
        let owner_k = owner.clone();
        let choice = Arc::new(ObjectSource::choice(&session.manifest.choice, move || {
            let _owner = &owner_k;
            read(&choice)
        }));
        segments.push(Segment {
            directory,
            companion,
            choice,
        });
    }
    let filters = directory
        .shards
        .iter()
        .filter_map(|s| s.filter.as_ref())
        .zip(&directory.filters)
        .map(|(named, object)| -> crate::Result<_> {
            let checked = objects.get(object)?;
            let owner = owner.clone();
            Ok(Arc::new(ObjectSource::filter(
                named.digest,
                named.bytes,
                u64::from(named.receivers),
                move || {
                    let _owner = &owner;
                    read(&checked)
                },
            )))
        })
        .collect::<crate::Result<Vec<_>>>()?;
    Ok(Tables {
        key,
        segments,
        filters,
    })
}

pub(super) fn load_filters(
    inventory: &Inventory,
    objects: &VerifiedObjects,
) -> crate::Result<Option<BlockWindow>> {
    inventory
        .filters
        .as_ref()
        .map(|filters| -> crate::Result<BlockWindow> {
            let head = &filters.head;
            let hashes: Vec<Hash> = objects
                .read(&filters.hashes)?
                .chunks_exact(32)
                .map(|hash| hash.try_into().expect("32-byte chunks"))
                .collect();
            let mut sets = Vec::new();
            let mut parent = None;
            let mut previous: Option<Range> = None;
            for object in &filters.ranges {
                let bytes = objects.read(object)?;
                let range = Range::decode(&bytes)?;
                let from = previous.as_ref().map_or(head.first, |p| p.end().height + 1);
                range.check(head, from, MAX_RANGE_BLOCKS)?;
                let end = range.end();
                if previous.as_ref().is_some_and(|p| !range.follows(p))
                    || hashes[(end.height - head.first) as usize] != end.hash
                {
                    return Err("Receiver block hint ranges do not continue".into());
                }
                parent.get_or_insert(range.parent());
                // Checked by the decode above, so only the encoded sets are kept.
                sets.extend(
                    Range::raw_sets(&bytes)?
                        .into_iter()
                        .map(|(height, set)| (height, set.to_vec())),
                );
                previous = Some(range);
            }
            let window = BlockWindow::new(
                inventory.genesis,
                inventory.history_revision,
                head.first,
                parent.ok_or("Receiver block hints are empty")?,
                hashes,
                sets,
            )?;
            if previous.map(|p| p.end()) != Some(head.tip) || window.head() != head {
                return Err("Receiver block hints differ from their head".into());
            }
            Ok(window)
        })
        .transpose()
}
