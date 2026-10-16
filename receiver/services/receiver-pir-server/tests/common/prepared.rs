//! Process-local reuse of verified public fixtures, never wallets or service state.
#![allow(dead_code)]
use receiver_directory::snapshot::{Snapshot, ROW_BYTES};
use receiver_pir::{server::Server, Error, MIN_ROWS};
use receiver_pir_server::Publication;
use sha2::{Digest, Sha256};
use std::{
    collections::VecDeque,
    sync::{Mutex, OnceLock},
};

// Only the fixed minimum geometry is retained. Larger geometry campaigns build
// independently; four entries bound retained database and preprocessing memory.
const ENTRIES: usize = 4;
type PreparedServers = VecDeque<([u8; 32], Server)>;
static CACHE: OnceLock<Mutex<PreparedServers>> = OnceLock::new();

pub fn prepare(snapshot: Snapshot, witnesses: Option<Vec<u8>>) -> Result<Publication, Error> {
    if snapshot.manifest.rows != MIN_ROWS || snapshot.data.len() != MIN_ROWS as usize * ROW_BYTES {
        return Ok(Publication::verified(Server::new(snapshot)?, witnesses));
    }
    let mut hash = Sha256::new();
    hash.update(snapshot.manifest.rows.to_le_bytes());
    hash.update(&snapshot.data);
    let key: [u8; 32] = hash.finalize().into();
    let mut cache = CACHE.get_or_init(Default::default).lock().unwrap();
    if let Some(index) = cache.iter().position(|(found, _)| *found == key) {
        let item = cache.remove(index).unwrap();
        // The constructor still validates every new snapshot and binds a new
        // session. The key selects a candidate; byte equality authorizes reuse.
        let server = Server::new_reusing_rows(snapshot, &item.1)?;
        cache.push_back(item);
        return Ok(Publication::verified(server, witnesses));
    }
    let server = Server::new(snapshot)?;
    if cache.len() == ENTRIES {
        cache.pop_front();
    }
    cache.push_back((key, server.clone()));
    Ok(Publication::verified(server, witnesses))
}
