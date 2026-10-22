//! Process-local reuse of verified public fixtures, never wallets or service state.
#![allow(dead_code)]
use receiver_directory::snapshot::{Snapshot, ROW_BYTES};
use receiver_pir::{server::Server, Error, MIN_ROWS};
use receiver_pir_server::Publication;
use std::{
    collections::VecDeque,
    sync::{Mutex, OnceLock},
};

// Only the fixed minimum geometry is retained. Larger geometry campaigns build
// independently; four entries bound retained PIR preparation memory.
const ENTRIES: usize = 4;
static CACHE: OnceLock<Mutex<VecDeque<Server>>> = OnceLock::new();

/// [`prepare_in`] the process-wide cache.
pub fn prepare(snapshot: Snapshot, witnesses: Option<Vec<u8>>) -> Result<Publication, Error> {
    prepare_in(CACHE.get_or_init(Default::default), snapshot, witnesses)
}

/// Prepares `snapshot` as [`Server::new`] would, reusing a server in `cache` prepared
/// from an identical snapshot. Any other snapshot, including one differing only in its
/// manifest, builds a new server.
pub fn prepare_in(
    cache: &Mutex<VecDeque<Server>>,
    snapshot: Snapshot,
    witnesses: Option<Vec<u8>>,
) -> Result<Publication, Error> {
    if snapshot.manifest.rows != MIN_ROWS || snapshot.data.len() != MIN_ROWS as usize * ROW_BYTES {
        return Ok(Publication::verified(Server::new(snapshot)?, witnesses));
    }
    // Validation binds the rows to the manifest's digest, so an equal manifest of a
    // valid snapshot means equal rows; corruption still fails as it would uncached.
    snapshot.validate()?;
    let mut cache = cache.lock().unwrap();
    let found = cache
        .iter()
        .position(|server| server.manifest().directory == snapshot.manifest);
    let server = match found {
        Some(index) => cache.remove(index).unwrap(),
        None => {
            if cache.len() == ENTRIES {
                cache.pop_front();
            }
            Server::new(snapshot)?
        }
    };
    cache.push_back(server.clone());
    Ok(Publication::verified(server, witnesses))
}
