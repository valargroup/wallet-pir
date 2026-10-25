//! Reconstruct lazy evaluator sources from verified cached objects.
use pir_publication::{ObjectCache, ObjectReference};
use receiver_pir_server::{
    blocks::BlockWindow,
    distribution::Inventory,
    publication::{Publication, Segment},
    tables::{FilterSource, Source},
};
use std::{io::Read, path::Path, sync::Arc};
use zakura_pir_query_server::receiver::{
    blocks::Batch, inventory, server::Server, session::Error, snapshot::Snapshot, witness,
};

fn read(path: &Path, object: &ObjectReference) -> Result<Vec<u8>, Error> {
    let mut bytes = Vec::new();
    std::fs::File::open(path)
        .map_err(|_| Error::Malformed)?
        .take(object.bytes.checked_add(1).ok_or(Error::Malformed)?)
        .read_to_end(&mut bytes)
        .map_err(|_| Error::Malformed)?;
    if bytes.len() as u64 != object.bytes {
        return Err(Error::Malformed);
    }
    Ok(bytes)
}

pub(super) fn load(
    inventory: &Inventory,
    cache: &ObjectCache,
    owner: Arc<std::fs::File>,
) -> crate::Result<(Option<Publication>, Option<BlockWindow>)> {
    let directory = inventory
        .directory
        .as_ref()
        .map(|directory| -> crate::Result<Publication> {
            let mut segments = Vec::new();
            for segment in &directory.segments {
                let rows = segment.payments.clone();
                let path = cache.object_path(&rows)?;
                let manifest = segment.publication.payments.clone();
                let snapshot = manifest.directory.clone();
                let public = read(
                    &cache.object_path(&segment.payment_public)?,
                    &segment.payment_public,
                )?;
                let payment_owner = owner.clone();
                let payments = Arc::new(Source::payments(manifest, public, move || {
                    let _owner = &payment_owner;
                    Server::new(Snapshot {
                        manifest: snapshot.clone(),
                        data: read(&path, &rows)?,
                    })
                })?);
                let rows = segment.witnesses.clone();
                let path = cache.object_path(&rows)?;
                let manifest = segment.publication.witnesses.clone();
                let table = manifest.table.clone();
                let public = read(
                    &cache.object_path(&segment.witness_public)?,
                    &segment.witness_public,
                )?;
                let witness_owner = owner.clone();
                let witnesses = Arc::new(Source::witnesses(
                    manifest,
                    public,
                    payments.clone(),
                    move || {
                        let _owner = &witness_owner;
                        Ok(witness::Table {
                            manifest: table.clone(),
                            data: read(&path, &rows)?,
                        })
                    },
                )?);
                segments.push(Segment {
                    payments,
                    witnesses,
                });
            }
            let filters = directory
                .shards
                .iter()
                .filter_map(|s| s.filter.as_ref())
                .zip(&directory.filters)
                .map(|(named, object)| -> crate::Result<_> {
                    let named = named.clone();
                    let object = object.clone();
                    let path = cache.object_path(&object)?;
                    let owner = owner.clone();
                    Ok(Arc::new(FilterSource::stored(
                        named.digest,
                        named.bytes,
                        u64::from(named.receivers),
                        move || {
                            let _owner = &owner;
                            read(&path, &object)
                        },
                    )))
                })
                .collect::<crate::Result<Vec<_>>>()?;
            Ok(Publication::verified(
                inventory::Inventory::build(
                    inventory.genesis,
                    inventory.history_revision,
                    directory.shards.clone(),
                )?,
                segments,
                filters,
            )?)
        })
        .transpose()?;
    let filters = inventory
        .filters
        .as_ref()
        .map(|filters| -> crate::Result<BlockWindow> {
            let mut from = filters.head.first;
            let mut blocks = Vec::new();
            for object in &filters.batches {
                let batch = Batch::decode(&read(&cache.object_path(object)?, object)?)?;
                batch.check(
                    &filters.head,
                    from,
                    zakura_pir_query_server::receiver::blocks::MAX_BLOCKS,
                )?;
                if batch.filters().is_empty() {
                    return Err("empty Receiver filter batch".into());
                }
                from = from
                    .checked_add(batch.filters().len() as u32)
                    .ok_or("Receiver filter height overflow")?;
                blocks.extend_from_slice(batch.filters());
                if blocks.len() > 24192 {
                    return Err("Receiver filter window exceeds block limit".into());
                }
            }
            let window = BlockWindow::new(inventory.genesis, inventory.history_revision, blocks)?;
            if window.head() != &filters.head {
                return Err("Receiver filter window differs from its head".into());
            }
            Ok(window)
        })
        .transpose()?;
    Ok((directory, filters))
}
