//! Process-local reuse of verified public fixtures, never wallets or service state.
#![allow(dead_code)]
use receiver_pir_server::{publication::Segment, tables::ObjectSource, Publication};
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex, OnceLock},
};
use zakura_pir_publisher::receiver::{
    build, initial_salt, Coverage, Projection, Tree, OPEN_SEED_ATTEMPTS, QUERY_BLOCK_ROWS,
};
use zakura_pir_query_server::receiver::session::Error;
use zakura_pir_query_server::receiver::shards::ShardFilter;
use zakura_pir_query_server::receiver::{server::Server, Companion, Directory};

// Small geometries only; larger campaigns build independently. Eight entries per kind
// bound retained preparation memory while parallel tests rarely evict each other's.
const ENTRIES: usize = 8;
static DIRECTORIES: OnceLock<Mutex<VecDeque<Server<Directory>>>> = OnceLock::new();
/// A segment's built tables.
pub type Tables = zakura_pir_publisher::receiver::Segment;
pub use zakura_pir_publisher::receiver::Paid;

static COMPANIONS: OnceLock<Mutex<VecDeque<Server<Companion>>>> = OnceLock::new();

/// The coverage of every fixture: heights 100 to 101 and positions 200 to 300 after
/// block `[2; 32]`, ending at block `[end; 32]`.
pub fn coverage(end: u8) -> Coverage {
    Coverage {
        genesis: [1; 32],
        history_revision: 0,
        start_height: 100,
        start_parent: [2; 32],
        start_position: 200,
        end_height: 101,
        end_hash: [end; 32],
        end_position: 300,
    }
}

/// The tree at `coverage`'s end.
pub fn tree(coverage: &Coverage) -> Tree {
    Tree {
        height: coverage.end_height,
        hash: coverage.end_hash,
        size: coverage.end_position,
    }
}

/// The segment tables of `paid` over `coverage`, projected as one block and built with
/// a companion of `rows` rows at the coverage end, seeded by the end block.
pub fn tables(coverage: Coverage, paid: &[Paid], rows: u32) -> Tables {
    let tree = tree(&coverage);
    tables_at(coverage, paid, rows, tree)
}

/// [`tables`] with the companion built at `tree`.
pub fn tables_at(coverage: Coverage, paid: &[Paid], rows: u32, tree: Tree) -> Tables {
    let mut projection = Projection::new(initial_salt(&coverage.start_parent, 0));
    let receivers: Vec<_> = paid.iter().map(|p| p.0).collect();
    projection
        .project(&receivers, &coverage.end_hash)
        .expect("fixture receivers place");
    let seed = coverage.end_hash;
    build(
        &projection,
        coverage,
        &seed,
        OPEN_SEED_ATTEMPTS,
        rows,
        tree,
        paid,
    )
    .unwrap()
}

/// An empty minimum-geometry segment ending at block `[end; 32]`.
pub fn empty(end: u8) -> Tables {
    tables(coverage(end), &[], QUERY_BLOCK_ROWS)
}

/// Prepares `tables` as production does, reusing cached servers prepared from
/// identical tables. Any other input builds new servers.
pub fn segment(tables: Tables) -> Result<Segment, Error> {
    let choice = tables.directory.choice.clone();
    let (directory, entries) = directory(tables.directory)?;
    let manifest = directory.session().manifest.clone();
    let companion = companion(tables.companion, &manifest, &entries)?;
    Ok(Segment::resident(directory, &choice, companion))
}

/// An inventory of one open shard holding the segment [`segment`] prepares.
pub fn prepare(tables: Tables) -> Result<Publication, Error> {
    inventory(vec![(false, vec![tables], None)])
}

/// One fixture shard: whether it is sealed, its segments' tables and its filter.
pub type FixtureShard = (bool, Vec<Tables>, Option<ShardFilter>);

/// An inventory of `shards` in order, each sealed or not, with its segments and filter.
pub fn inventory(shards: Vec<FixtureShard>) -> Result<Publication, Error> {
    let c = shards[0].1[0].directory.manifest.coverage.clone();
    let mut prepared = Vec::new();
    for (sealed, tables, filter) in shards {
        let segments = tables.into_iter().map(segment).collect::<Result<_, _>>()?;
        let filter = filter.map(|f| Arc::new(memory_filter(f)));
        prepared.push((sealed, segments, filter));
    }
    Publication::assemble(c.genesis, c.history_revision, prepared)
}

/// A source over `filter`'s encoding held in memory.
pub fn memory_filter(filter: ShardFilter) -> ObjectSource {
    let bytes: Arc<[u8]> = filter.bytes().into();
    let length = u32::try_from(bytes.len()).expect("fixture filters are small");
    ObjectSource::filter(filter.digest(), length, filter.receivers(), move || {
        Ok(bytes.to_vec())
    })
}

/// The directory server and entries for `table`, reusing one prepared from an
/// identical table.
pub fn directory(
    table: zakura_pir_query_server::receiver::DirectoryTable,
) -> Result<
    (
        Server<Directory>,
        Vec<zakura_pir_query_server::receiver::Entry>,
    ),
    Error,
> {
    let entries = table.validate()?;
    let mut cache = DIRECTORIES.get_or_init(Default::default).lock().unwrap();
    let found = cache
        .iter()
        .position(|s| s.session().manifest == table.manifest);
    let server = match found {
        Some(index) => cache.remove(index).unwrap(),
        None => {
            if cache.len() == ENTRIES {
                cache.pop_front();
            }
            drop(cache);
            let (server, _) = Server::directory(table)?;
            cache = DIRECTORIES.get().unwrap().lock().unwrap();
            server
        }
    };
    cache.push_back(server.clone());
    Ok((server, entries))
}

/// The companion server for `table`, checked against `directory` and its `entries`,
/// reusing one prepared from an identical table.
pub fn companion(
    table: zakura_pir_query_server::receiver::CompanionTable,
    directory: &Directory,
    entries: &[zakura_pir_query_server::receiver::Entry],
) -> Result<Server<Companion>, Error> {
    table.validate(directory, entries)?;
    let mut cache = COMPANIONS.get_or_init(Default::default).lock().unwrap();
    let found = cache
        .iter()
        .position(|s| s.session().manifest == table.manifest);
    let server = match found {
        Some(index) => cache.remove(index).unwrap(),
        None => {
            if cache.len() == ENTRIES {
                cache.pop_front();
            }
            drop(cache);
            let server = Server::companion(table, directory, entries)?;
            cache = COMPANIONS.get().unwrap().lock().unwrap();
            server
        }
    };
    cache.push_back(server.clone());
    Ok(server)
}
