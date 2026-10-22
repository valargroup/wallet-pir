//! Process-local reuse of verified public fixtures, never wallets or service state.
#![allow(dead_code)]
use receiver_directory::{
    snapshot::{Snapshot, ROW_BYTES},
    witness,
};
use receiver_pir::{
    server::{Server, WitnessServer},
    Error, MIN_ROWS,
};
use receiver_pir_server::Publication;
use std::{
    collections::VecDeque,
    sync::{Mutex, OnceLock},
};

// Only the fixed minimum geometry is retained. Larger geometry campaigns build
// independently; eight entries per kind bound retained PIR preparation memory while
// tests preparing in parallel rarely evict one another's fixtures.
const ENTRIES: usize = 8;
static CACHE: OnceLock<Mutex<VecDeque<Server>>> = OnceLock::new();
static WITNESSES: OnceLock<Mutex<VecDeque<WitnessServer>>> = OnceLock::new();

/// Prepares `snapshot` and its companion `witnesses` as production does, reusing a
/// cached server prepared from an identical snapshot or an identical witness table. Any
/// other input, including one differing only in its manifest, builds a new server.
pub fn prepare(snapshot: Snapshot, witnesses: witness::Table) -> Result<Publication, Error> {
    let server = payments(snapshot)?;
    let witnesses = companion(witnesses, &server)?;
    Ok(Publication::verified(server, witnesses))
}

/// A zero-payload companion table for `snapshot`, built at its end, for serving tests
/// whose records have no stored commitments.
pub fn placeholder(snapshot: &Snapshot) -> witness::Table {
    let payloads = vec![[0; witness::PAYLOAD_BYTES]; snapshot.manifest.records as usize];
    companion_table(snapshot, &payloads)
}

/// A minimum-geometry companion table for `snapshot` holding `payloads` in ordinal
/// order, built at its end. The serving crate never interprets payloads.
pub fn companion_table(
    snapshot: &Snapshot,
    payloads: &[[u8; witness::PAYLOAD_BYTES]],
) -> witness::Table {
    let m = &snapshot.manifest;
    witness::Table::build(
        witness::Manifest {
            profile: witness::PROFILE.into(),
            genesis: m.genesis,
            history_revision: m.history_revision,
            payments: m.revision().unwrap(),
            payloads: 0,
            rows: MIN_ROWS,
            tree_height: m.end_height,
            tree_hash: m.end_hash,
            tree_size: m.end_position,
            data_sha256: [0; 32],
        },
        payloads,
    )
    .unwrap()
}

/// [`prepare`] with a [`placeholder`] companion.
pub fn prepare_payments(snapshot: Snapshot) -> Result<Publication, Error> {
    prepare_payments_in(CACHE.get_or_init(Default::default), snapshot)
}

/// [`prepare_payments`] reusing payment servers from `cache` instead of the process-wide one.
pub fn prepare_payments_in(
    cache: &Mutex<VecDeque<Server>>,
    snapshot: Snapshot,
) -> Result<Publication, Error> {
    snapshot.validate()?;
    let witnesses = placeholder(&snapshot);
    let server = payments_in(cache, snapshot)?;
    let witnesses = companion(witnesses, &server)?;
    Ok(Publication::verified(server, witnesses))
}

/// [`payments_in`] the process-wide cache.
fn payments(snapshot: Snapshot) -> Result<Server, Error> {
    payments_in(CACHE.get_or_init(Default::default), snapshot)
}

/// The payment server for `snapshot`, reusing one in `cache` prepared from an identical
/// snapshot.
fn payments_in(cache: &Mutex<VecDeque<Server>>, snapshot: Snapshot) -> Result<Server, Error> {
    if snapshot.manifest.rows != MIN_ROWS || snapshot.data.len() != MIN_ROWS as usize * ROW_BYTES {
        return Server::new(snapshot);
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
    Ok(server)
}

/// The witness server for `table`, reusing one prepared from an identical table, whose
/// manifest binds the same payment table and rows.
fn companion(table: witness::Table, payments: &Server) -> Result<WitnessServer, Error> {
    table.validate()?;
    table
        .manifest
        .check_payments(&payments.manifest().directory)?;
    let mut cache = WITNESSES.get_or_init(Default::default).lock().unwrap();
    let found = cache
        .iter()
        .position(|server| server.manifest().table == table.manifest);
    let server = match found {
        Some(index) => cache.remove(index).unwrap(),
        None => {
            if cache.len() == ENTRIES {
                cache.pop_front();
            }
            WitnessServer::new(table, payments)?
        }
    };
    cache.push_back(server.clone());
    Ok(server)
}
