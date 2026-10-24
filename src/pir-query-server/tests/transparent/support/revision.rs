//! Writes one directory shard revision, with its page shards, the way the
//! publisher lays one out on disk. Shared by the suites that build sets by
//! hand rather than through the publisher.
#![allow(dead_code)]

use zakura_pir_protocol::transparent::events as transparent_events;
use zakura_pir_protocol::transparent::filter as transparent_filter;
use zakura_pir_publisher::transparent as transparent_shard;

use std::path::Path;
use transparent_events::TransparentEvent;
use transparent_filter::{filter_hash, BlockHash, ScriptBytes, ShardMapEntry};
use transparent_shard::build::{build_directory_shard, Block, BuiltChild, BuiltDirectory};
use transparent_shard::layout::Geometry;
use transparent_shard::manifest::{
    pages_table, ManifestLayout, ManifestOccupancy, ManifestSeal, ShardManifest, TableGeometry,
    SCHEMA,
};

/// What one revision is.
pub struct Revision<'a> {
    pub shard_id: u64,
    pub start: u64,
    pub end: u64,
    pub geometry: &'static Geometry,
    pub sealed: bool,
    pub revision: u32,
    pub supersedes: &'a str,
    pub parent_digest: &'a str,
    /// Page shards of equal span inside the directory shard.
    pub children: u32,
    pub genesis: BlockHash,
    pub hash: &'a dyn Fn(u64) -> BlockHash,
}

/// The heights `children` equal page shards over `start..=end` end at.
pub fn child_ends(start: u64, end: u64, children: u32) -> Vec<u64> {
    let span = end - start + 1;
    let children = u64::from(children.max(1)).min(span);
    (1..=children)
        .map(|i| start + span * i / children - 1)
        .collect()
}

/// Builds the revision's tables from `events` inside its range.
pub fn build(
    spec: &Revision,
    events: &[(ScriptBytes, TransparentEvent)],
) -> (BuiltDirectory, Vec<BuiltChild>) {
    let blocks: Vec<Block> = (spec.start..=spec.end)
        .map(|height| {
            (
                height,
                events
                    .iter()
                    .filter(|(_, event)| u64::from(event.height()) == height)
                    .cloned()
                    .collect(),
            )
        })
        .collect();
    build_directory_shard(
        spec.shard_id,
        spec.geometry,
        spec.genesis,
        transparent_filter::RANGE_PROFILE,
        blocks,
        &child_ends(spec.start, spec.end, spec.children),
        spec.hash,
        spec.sealed,
    )
    .expect("build")
}

/// The manifest of a built revision.
pub fn manifest(spec: &Revision, built: &BuiltDirectory, children: &[BuiltChild]) -> ShardManifest {
    let geometry = spec.geometry;
    let page_children: Vec<_> = children.iter().map(|c| c.describe(geometry)).collect();
    ShardManifest {
        schema: SCHEMA.to_string(),
        profile: transparent_filter::RANGE_PROFILE.to_string(),
        geometry: geometry.name.to_string(),
        network: transparent_filter::NETWORK.to_string(),
        genesis_hash: spec.genesis.to_display_hex(),
        shard_id: spec.shard_id,
        start_height: spec.start,
        end_height: spec.end,
        parent_block_hash: (spec.hash)(spec.start - 1).to_display_hex(),
        terminal_block_hash: (spec.hash)(spec.end).to_display_hex(),
        tag_salt_counter: built.tag_salt_counter,
        parent_manifest_digest: spec.parent_digest.to_string(),
        sealed: spec.sealed,
        revision: spec.revision,
        supersedes: spec.supersedes.to_string(),
        seal: ManifestSeal {
            scripts_target: 8_192,
            scripts_capacity: 16_384,
            directory_bytes_target: 7_000_000,
            directory_bytes_capacity: 8_380_416,
            page_rows_target: 2_048,
            page_rows_capacity: 4_096,
            max_page_children: transparent_shard::MAX_PAGE_CHILDREN,
        },
        layout: ManifestLayout::current(),
        filter_hash: filter_hash(built.filter.as_slice()).to_display_hex(),
        directory_segments: built
            .directory
            .iter()
            .map(|segment| TableGeometry {
                rows: geometry.directory_rows,
                row_bytes: geometry.directory_row_bytes as u32,
                sha256: hex::encode(<sha2::Sha256 as sha2::Digest>::digest(segment)),
            })
            .collect(),
        occupancy: ManifestOccupancy {
            scripts: built.scripts,
            page_rows: page_children.iter().map(|c| c.occupancy.page_rows).sum(),
            fragments: page_children.iter().map(|c| c.occupancy.fragments).sum(),
            events: built.events,
            blocks: spec.end - spec.start + 1,
            txids: 0,
            excluded_scripts: built.excluded_scripts,
        },
        page_children,
        directory_choice: None,
    }
}

/// Writes a built revision's directory and returns its manifest and map entry.
pub fn write_built(
    dir: &Path,
    spec: &Revision,
    built: &BuiltDirectory,
    children: &[BuiltChild],
) -> (ShardManifest, ShardMapEntry) {
    let manifest = manifest(spec, built, children);
    write_manifest(dir, spec, manifest, built, children)
}

/// Writes a built revision under `manifest`, which a caller may have edited
/// from [`manifest`]'s, and returns it with its map entry.
pub fn write_manifest(
    dir: &Path,
    spec: &Revision,
    manifest: ShardManifest,
    built: &BuiltDirectory,
    children: &[BuiltChild],
) -> (ShardManifest, ShardMapEntry) {
    manifest
        .check_children(spec.geometry.page_rows, spec.geometry.page_row_bytes as u32)
        .expect("well-formed page shards");
    let digest = manifest.digest();
    let shard_dir = dir.join(&digest);
    std::fs::create_dir_all(&shard_dir).unwrap();
    std::fs::write(shard_dir.join("manifest.json"), manifest.canonical_bytes()).unwrap();
    std::fs::write(shard_dir.join("filter.bin"), built.filter.as_slice()).unwrap();
    for (index, segment) in built.directory.iter().enumerate() {
        std::fs::write(shard_dir.join(format!("directory.{index}.bin")), segment).unwrap();
    }
    for child in children {
        for segment in 0..child.segments {
            std::fs::write(
                shard_dir.join(format!("{}.{segment}.bin", pages_table(child.index))),
                child.segment(spec.geometry, segment),
            )
            .unwrap();
        }
    }
    let entry = ShardMapEntry {
        shard_id: spec.shard_id,
        geometry: spec.geometry.name.to_string(),
        start_height: spec.start,
        end_height: spec.end,
        parent_block_hash: manifest.parent_block_hash.clone(),
        terminal_block_hash: manifest.terminal_block_hash.clone(),
        filter_hash: manifest.filter_hash.clone(),
        scripts: built.scripts,
        page_rows: manifest.occupancy.page_rows,
        txids: 0,
        directory_segments: built.directory_segments(),
        page_segments: manifest.page_children.len() as u32,
        manifest_digest: digest,
        revision: spec.revision,
        sealed: spec.sealed,
    };
    (manifest, entry)
}

/// Builds and writes one revision.
pub fn write(
    dir: &Path,
    spec: &Revision,
    events: &[(ScriptBytes, TransparentEvent)],
) -> (ShardManifest, ShardMapEntry) {
    let (built, children) = build(spec, events);
    write_built(dir, spec, &built, &children)
}
