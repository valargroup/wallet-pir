//! Shared exact HTTP query probe for the operator soak and isolated burst test.
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::{
    io::{Read, Seek, SeekFrom},
    path::{Path, PathBuf},
    time::{Duration, Instant},
};
use transparent_wallet::{
    client::{Table, TableClient},
    http::{HttpFilterSource, HttpOptions, HttpShardTransport},
    transport::{BoxError, FilterSource, ShardTransport},
};

#[derive(Deserialize)]
struct Setup {
    public_params: String,
    public_params_sha256: String,
}

#[allow(dead_code)] // Used by the soak executable, not the manual burst test.
pub fn once(
    url: &str,
    publications: &Path,
    shard: usize,
    table: Table,
) -> Result<serde_json::Value, BoxError> {
    once_at(url, publications, shard, table, Some(7))
}

/// Select an occupied row so an empty response cannot pass a burst probe.
#[allow(dead_code)] // Used by the manual burst test, not the soak executable.
pub fn once_nonempty(
    url: &str,
    publications: &Path,
    shard: usize,
    table: Table,
) -> Result<serde_json::Value, BoxError> {
    once_at(url, publications, shard, table, None)
}

fn once_at(
    url: &str,
    publications: &Path,
    shard: usize,
    table: Table,
    row: Option<u64>,
) -> Result<serde_json::Value, BoxError> {
    let options = HttpOptions {
        timeout: Duration::from_secs(60),
        ..Default::default()
    };
    let mut filters = HttpFilterSource::new(url, &options)?;
    let mut transport = HttpShardTransport::new(url, &options)?;
    let (bytes, _) = filters.shard_map()?;
    let map: transparent_filter::ShardMap = serde_json::from_slice(&bytes)?;
    map.check_shape().map_err(|e| format!("map: {e}"))?;
    let entry = map.shards.get(shard).ok_or("shard absent")?;
    let geometry = transport.geometry()?;
    let g = geometry
        .geometries
        .iter()
        .find(|g| g.name == entry.geometry)
        .ok_or("geometry absent")?;
    let (rows, width, scheme, segments, name) = match table {
        Table::Directory => (
            g.directory_rows,
            g.directory_row_bytes,
            &g.directory_scheme,
            entry.directory_segments,
            "directory",
        ),
        Table::Pages => (
            g.page_rows,
            g.page_row_bytes,
            &g.pages_scheme,
            entry.page_segments,
            "pages",
        ),
    };
    let directory = find_revision(publications, &entry.manifest_digest)?;
    let manifest_bytes = std::fs::read(directory.join("manifest.json"))?;
    let manifest: transparent_shard::manifest::ShardManifest =
        serde_json::from_slice(&manifest_bytes)?;
    if manifest.digest() != entry.manifest_digest {
        return Err("local manifest digest mismatch".into());
    }
    let sources = if name == "directory" {
        &manifest.directory_segments
    } else {
        &manifest.page_segments
    };
    let row_number = match row {
        Some(row) => row,
        None => {
            let mut file = std::fs::File::open(directory.join(format!("{name}.0.bin")))?;
            let mut bytes = vec![0; width as usize];
            let mut found = None;
            for index in 0..rows {
                file.read_exact(&mut bytes)?;
                if bytes.iter().any(|b| *b != 0) {
                    found = Some(index);
                    break;
                }
            }
            found.ok_or("fixture table contains no occupied row")?
        }
    };
    let mut expected = Vec::new();
    // Hold the expected bytes before the publisher can collect this directory.
    for (segment, source) in sources.iter().enumerate() {
        let path = directory.join(format!("{name}.{segment}.bin"));
        let mut file = std::fs::File::open(&path)?;
        let mut hash = Sha256::new();
        let mut buffer = [0u8; 65536];
        loop {
            let n = file.read(&mut buffer)?;
            if n == 0 {
                break;
            }
            hash.update(&buffer[..n]);
        }
        if hex::encode(hash.finalize()) != source.sha256 {
            return Err("local table digest mismatch".into());
        }
        file.seek(SeekFrom::Start(row_number * u64::from(width)))?;
        let mut row = vec![0; width as usize];
        file.read_exact(&mut row)?;
        expected.push(row);
    }
    let mut client = TableClient::new(table, &g.name, rows, width, scheme)?;
    for segment in 0..segments {
        let (bytes, _) = transport.setup(entry.shard_id, &entry.manifest_digest, table, segment)?;
        let setup: Setup = serde_json::from_slice(&bytes)?;
        client.open_segment(
            &entry.manifest_digest,
            segment,
            &setup.public_params,
            &setup.public_params_sha256,
        )?;
    }
    let query = client.prepare(&entry.manifest_digest, usize::try_from(row_number)?)?;
    let started = Instant::now();
    let response = transport.query(entry.shard_id, &entry.manifest_digest, table, &query.body)?;
    let decoded = client.decode(&entry.manifest_digest, segments, query, &response)?;
    if decoded != expected {
        return Err("private query differs from published plaintext".into());
    }
    Ok(
        serde_json::json!({"event":"query", "height":entry.end_height,"revision":entry.manifest_digest,"table":name,"seconds":started.elapsed().as_secs_f64(),"exact":true}),
    )
}

fn find_revision(root: &Path, digest: &str) -> Result<PathBuf, BoxError> {
    for item in std::fs::read_dir(root)? {
        let directory = item?.path().join(digest);
        if directory.join("manifest.json").is_file() {
            return Ok(directory);
        }
    }
    Err("published source revision unavailable".into())
}
