//! Reconstruct lazy evaluator sources from verified cached objects.
use pir_publication::{VerifiedObjectFile, VerifiedObjects};
use receiver_pir_server::{
    blocks::BlockWindow,
    distribution::Inventory,
    publication::{Publication, Segment},
    tables::{FilterSource, Source},
};
use std::sync::Arc;
use zakura_pir_query_server::receiver::{
    blocks::Batch, inventory, server::Server, session::Error, snapshot::Snapshot, witness,
};

fn read(file: &VerifiedObjectFile) -> Result<Vec<u8>, Error> {
    file.read_bytes().map_err(|_| Error::Malformed)
}

pub(super) fn load(
    inventory: &Inventory,
    objects: &VerifiedObjects,
    owner: Arc<std::fs::File>,
) -> crate::Result<(Option<Publication>, Option<BlockWindow>)> {
    let directory = inventory
        .directory
        .as_ref()
        .map(|directory| -> crate::Result<Publication> {
            let mut segments = Vec::new();
            for segment in &directory.segments {
                let rows = segment.payments.clone();
                let checked = objects.get(&rows)?;
                let manifest = segment.publication.payments.clone();
                let snapshot = manifest.directory.clone();
                let public = objects.read(&segment.payment_public)?;
                let payment_owner = owner.clone();
                let payments = Arc::new(Source::payments(manifest, public, move || {
                    let _owner = &payment_owner;
                    Server::new(Snapshot {
                        manifest: snapshot.clone(),
                        data: read(&checked)?,
                    })
                })?);
                let rows = segment.witnesses.clone();
                let checked = objects.get(&rows)?;
                let manifest = segment.publication.witnesses.clone();
                let table = manifest.table.clone();
                let public = objects.read(&segment.witness_public)?;
                let witness_owner = owner.clone();
                let witnesses = Arc::new(Source::witnesses(
                    manifest,
                    public,
                    payments.clone(),
                    move || {
                        let _owner = &witness_owner;
                        Ok(witness::Table {
                            manifest: table.clone(),
                            data: read(&checked)?,
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
                    let checked = objects.get(&object)?;
                    let owner = owner.clone();
                    Ok(Arc::new(FilterSource::stored(
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
    Ok((directory, load_filters(inventory, objects)?))
}

pub(super) fn load_filters(
    inventory: &Inventory,
    objects: &VerifiedObjects,
) -> crate::Result<Option<BlockWindow>> {
    inventory
        .filters
        .as_ref()
        .map(|filters| -> crate::Result<BlockWindow> {
            let mut from = filters.head.first;
            let mut blocks = Vec::new();
            for object in &filters.batches {
                let batch = Batch::decode(&objects.read(object)?)?;
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
        .transpose()
}
