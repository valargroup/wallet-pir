//! Planning an assignment over a roster of workers, and rendering the edge
//! that routes to it.
//!
//! Routing is public data only. A request names a shard id, a revision digest
//! and a table in its path; the edge maps the shard id to the workers that
//! hold it and nothing else. The selected script, the row inside the table and
//! the page locator are inside the private body and never reach a routing
//! decision. Every replica of the recent tier holds the same shards, so the
//! newest shard — the one every syncing wallet touches — draws on every
//! replica's bandwidth; archive shards have one owner each, and losing that
//! owner makes its range unavailable, explicitly, until it is rebuilt.

use crate::assignment::{
    Assignment, GeneratedBy, SetIdentity, WorkerAssignment, WorkerRole, ASSIGNMENT_SCHEMA,
};
use crate::runtime::reserved_bytes;
use crate::shardset::Table;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use transparent_filter::{ShardMap, ShardMapEntry};

/// One worker as the operator describes it, before shards are assigned.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RosterEntry {
    pub id: String,
    pub role: WorkerRole,
    #[serde(default)]
    pub replica_group: Option<String>,
    /// How the deploy reaches the host.
    pub ssh_host: String,
    /// How the router reaches the service: `host:port`.
    pub upstream: String,
    pub cache_bytes: u64,
    /// An archive owner's pinned range, first and last shard id inclusive.
    ///
    /// When every owner names one, the planner gives each exactly its range
    /// instead of re-cutting the archive: adding or removing recent replicas,
    /// or re-planning after a restart, never moves an archive cut and never
    /// sends an owner cold tables. Pin all owners or none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub archive_range: Option<[u64; 2]>,
}

#[derive(Debug, thiserror::Error)]
pub enum PlanError {
    #[error("{0}")]
    Invalid(String),
}

/// Bytes every runtime of `entry` reserves: both tables, every segment.
pub fn shard_reserved_bytes(entry: &ShardMapEntry) -> Result<u64, PlanError> {
    let geometry = transparent_shard::layout::by_name(&entry.geometry).ok_or_else(|| {
        PlanError::Invalid(format!(
            "shard {} names geometry {}, which this build does not know",
            entry.shard_id, entry.geometry
        ))
    })?;
    let mut total = 0u64;
    for (table, segments) in [
        (Table::Directory, entry.directory_segments),
        (Table::Pages, entry.page_segments),
    ] {
        total +=
            reserved_bytes(table.rows(geometry), table.row_bytes(geometry)) * u64::from(segments);
    }
    Ok(total)
}

/// Plans an assignment: every recent shard on every replica, the archive
/// split into contiguous ranges balanced by reserved bytes across owners.
///
/// `headroom` is the fraction of each worker's cache that must stay free of
/// assigned runtimes, for retained revisions and the transient cost of a
/// build. A worker that cannot hold its share within that is a refusal, not a
/// plan: an assignment that relies on eviction is a worker that thrashes.
pub fn plan(
    map: &ShardMap,
    map_sha256: &str,
    roster: &[RosterEntry],
    recent_from_shard: u64,
    headroom: f64,
    generated_by: GeneratedBy,
) -> Result<Assignment, PlanError> {
    if !(0.0..1.0).contains(&headroom) {
        return Err(PlanError::Invalid("headroom must be in [0, 1)".into()));
    }
    let shards = map.shards.len() as u64;
    if recent_from_shard > shards {
        return Err(PlanError::Invalid(format!(
            "recent_from_shard {recent_from_shard} is beyond the set's {shards} shards"
        )));
    }
    if roster.is_empty() {
        return Err(PlanError::Invalid("the roster is empty".into()));
    }
    let mut ids = std::collections::BTreeSet::new();
    for entry in roster {
        if !ids.insert(entry.id.as_str()) {
            return Err(PlanError::Invalid(format!(
                "worker {} appears twice",
                entry.id
            )));
        }
    }
    let costs: Vec<u64> = map
        .shards
        .iter()
        .map(shard_reserved_bytes)
        .collect::<Result<_, _>>()?;
    let budget = |entry: &RosterEntry| (entry.cache_bytes as f64 * (1.0 - headroom)) as u64;

    let recent: Vec<u64> = (recent_from_shard..shards).collect();
    let recent_bytes: u64 = recent.iter().map(|id| costs[*id as usize]).sum();
    let archive: Vec<u64> = (0..recent_from_shard).collect();
    let archive_bytes: u64 = archive.iter().map(|id| costs[*id as usize]).sum();

    let owners: Vec<&RosterEntry> = roster
        .iter()
        .filter(|entry| entry.role == WorkerRole::ArchiveOwner)
        .collect();
    let replicas: Vec<&RosterEntry> = roster
        .iter()
        .filter(|entry| entry.role == WorkerRole::RecentReplica)
        .collect();
    if !recent.is_empty() && replicas.is_empty() {
        return Err(PlanError::Invalid(
            "the set has a recent tier but the roster has no recent replica".into(),
        ));
    }
    if !archive.is_empty() && owners.is_empty() {
        return Err(PlanError::Invalid(
            "the set has an archive tier but the roster has no archive owner".into(),
        ));
    }

    let mut workers: Vec<WorkerAssignment> = Vec::new();

    let pinned = owners.iter().filter(|o| o.archive_range.is_some()).count();
    if pinned != 0 && pinned != owners.len() {
        return Err(PlanError::Invalid(
            "pin every archive owner's range or none".into(),
        ));
    }

    // The archive is cut into contiguous ranges. Contiguity keeps an owner's
    // range legible ("shards 0-80") and a rebuild a single copy; the cut
    // points are chosen so each owner's reserved bytes are as close to the
    // per-owner share as a contiguous split allows.
    if pinned != 0 {
        let mut ranges: Vec<[u64; 2]> = owners.iter().filter_map(|o| o.archive_range).collect();
        ranges.sort();
        let mut next = 0u64;
        for [first, last] in &ranges {
            if *first != next || last < first {
                return Err(PlanError::Invalid(format!(
                    "pinned archive ranges must be contiguous from shard 0; {first}-{last} \
                     does not start at {next}"
                )));
            }
            next = last + 1;
        }
        if next != recent_from_shard {
            return Err(PlanError::Invalid(format!(
                "pinned archive ranges cover shards 0-{}, the archive is 0-{}",
                next.saturating_sub(1),
                recent_from_shard.saturating_sub(1)
            )));
        }
        for owner in &owners {
            let [first, last] = owner.archive_range.expect("every owner is pinned");
            let range: Vec<u64> = (first..=last).collect();
            let bytes: u64 = range.iter().map(|id| costs[*id as usize]).sum();
            if bytes > budget(owner) {
                return Err(PlanError::Invalid(format!(
                    "archive owner {} would hold {} bytes of runtimes ({} shards) against a \
                     budget of {} after {:.0}% headroom; its pinned range needs more memory",
                    owner.id,
                    bytes,
                    range.len(),
                    budget(owner),
                    headroom * 100.0
                )));
            }
            workers.push(WorkerAssignment {
                id: owner.id.clone(),
                role: WorkerRole::ArchiveOwner,
                replica_group: None,
                upstream: owner.upstream.clone(),
                cache_bytes: owner.cache_bytes,
                shards: range,
                estimated_resident_bytes: bytes,
            });
        }
    } else if !archive.is_empty() {
        let share = archive_bytes as f64 / owners.len() as f64;
        let mut ranges: Vec<Vec<u64>> = Vec::new();
        let mut current: Vec<u64> = Vec::new();
        let mut current_bytes = 0u64;
        let mut cumulative = 0f64;
        for &id in &archive {
            let cost = costs[id as usize];
            // Close the range when adding this shard would carry it past the
            // next cut point, unless this is the last owner's range.
            let remaining_owners = owners.len() - ranges.len();
            let target = share * (ranges.len() + 1) as f64;
            if remaining_owners > 1
                && !current.is_empty()
                && cumulative + cost as f64 > target
                && (cumulative + cost as f64 - target) > (target - cumulative)
            {
                ranges.push(std::mem::take(&mut current));
                current_bytes = 0;
            }
            current.push(id);
            current_bytes += cost;
            cumulative += cost as f64;
            let _ = current_bytes;
        }
        ranges.push(current);
        while ranges.len() < owners.len() {
            ranges.push(Vec::new());
        }
        for (owner, range) in owners.iter().zip(ranges) {
            let bytes: u64 = range.iter().map(|id| costs[*id as usize]).sum();
            if bytes > budget(owner) {
                return Err(PlanError::Invalid(format!(
                    "archive owner {} would hold {} bytes of runtimes ({} shards) against a \
                     budget of {} after {:.0}% headroom; add owners or memory rather than rely \
                     on eviction",
                    owner.id,
                    bytes,
                    range.len(),
                    budget(owner),
                    headroom * 100.0
                )));
            }
            workers.push(WorkerAssignment {
                id: owner.id.clone(),
                role: WorkerRole::ArchiveOwner,
                replica_group: None,
                upstream: owner.upstream.clone(),
                cache_bytes: owner.cache_bytes,
                shards: range,
                estimated_resident_bytes: bytes,
            });
        }
    } else {
        for owner in &owners {
            workers.push(WorkerAssignment {
                id: owner.id.clone(),
                role: WorkerRole::ArchiveOwner,
                replica_group: None,
                upstream: owner.upstream.clone(),
                cache_bytes: owner.cache_bytes,
                shards: Vec::new(),
                estimated_resident_bytes: 0,
            });
        }
    }

    for replica in &replicas {
        if recent_bytes > budget(replica) {
            return Err(PlanError::Invalid(format!(
                "recent replica {} would hold {} bytes of runtimes ({} shards) against a budget \
                 of {} after {:.0}% headroom; the recent tier has outgrown its replicas",
                replica.id,
                recent_bytes,
                recent.len(),
                budget(replica),
                headroom * 100.0
            )));
        }
        workers.push(WorkerAssignment {
            id: replica.id.clone(),
            role: WorkerRole::RecentReplica,
            replica_group: Some(
                replica
                    .replica_group
                    .clone()
                    .unwrap_or_else(|| "recent".to_string()),
            ),
            upstream: replica.upstream.clone(),
            cache_bytes: replica.cache_bytes,
            shards: recent.clone(),
            estimated_resident_bytes: recent_bytes,
        });
    }

    let assignment = Assignment {
        schema: ASSIGNMENT_SCHEMA.to_string(),
        set: SetIdentity {
            shard_schema: transparent_shard::manifest::SCHEMA.to_string(),
            map_sha256: map_sha256.to_string(),
            network: map.network.clone(),
            genesis_hash: map.genesis_hash.clone(),
            shards,
            start_height: map.start_height,
            covered_through: map.shards.last().map(|e| e.end_height).unwrap_or(0),
            recent_from_shard,
        },
        generated_by,
        workers,
        unassigned: Vec::new(),
    };
    assignment
        .check_shape()
        .map_err(|error| PlanError::Invalid(error.to_string()))?;
    Ok(assignment)
}

/// Renders the router's Caddyfile for an assignment.
///
/// Private routes are matched on the shard id in the path and sent to that
/// shard's owners: the recent pool round-robin with health checks, each
/// archive owner alone. Set-wide public bytes — map, init, filters, manifests
/// — go to the recent pool, since every worker holds them. Operator routes
/// are not routed at all.
pub fn render_caddyfile(assignment: &Assignment, public_host: &str) -> String {
    render_caddyfile_with(assignment, public_host, None)
}

/// As [`render_caddyfile`], with an optional second site: the same routes on
/// a plain-HTTP listener (an address such as `10.0.0.5:8080`), for the load
/// harness and deploy verification from inside the VPC before the public
/// name points at the router. The firewall, not this file, keeps it private.
pub fn render_caddyfile_with(
    assignment: &Assignment,
    public_host: &str,
    internal_listen: Option<&str>,
) -> String {
    let mut out = String::new();
    out.push_str("{\n\tservers {\n\t\tmetrics\n\t}\n}\n\n");
    out.push_str(&format!(
        "# Rendered by shard-assign for assignment {}.\n\
         # Do not edit: the assignment is the source, and every worker reports\n\
         # the assignment digest it runs under.\n",
        assignment.digest()
    ));
    out.push_str(&format!("{public_host} {{\n"));
    out.push_str(
        "\ttls {\n\t\tissuer acme {\n\t\t\tdir https://acme-v02.api.letsencrypt.org/directory\n\t\t}\n\t}\n\n",
    );
    out.push_str("\trequest_body {\n\t\tmax_size 1MB\n\t}\n\n");
    out.push_str(&render_routes(assignment));
    out.push_str("\thandle {\n\t\trespond 404\n\t}\n");
    out.push_str(PROXY_ERRORS);
    out.push_str("}\n");
    if let Some(listen) = internal_listen {
        out.push_str(&format!(
            "\n# Internal plain-HTTP listener, VPC only: the same routes without TLS.\nhttp://{listen} {{\n"
        ));
        out.push_str("\trequest_body {\n\t\tmax_size 1MB\n\t}\n\n");
        out.push_str(&render_routes(assignment));
        out.push_str("\thandle {\n\t\trespond 404\n\t}\n");
        out.push_str(PROXY_ERRORS);
        out.push_str("}\n");
    }
    out
}

/// Health checking for every worker pool the router renders.
///
/// The failure this is shaped against is a saturated pool, not a dead one.
/// On 2026-09-27 a four-worker bench fleet at 128 wallets had every worker
/// ejected at once while each kept passing its active check: the previous
/// passive policy (`fail_duration 30s`, Caddy's default `max_fails 1`) took a
/// worker out of rotation for 30 s on any single proxy error, and a saturated
/// worker produces a few — a refused upload torn down mid-body, a keep-alive
/// connection closed under the proxy. The remaining workers took the whole
/// load, failed the same way, and the router then refused everything.
///
/// - Active: `/v1/ready` every second, a worker leaves after three
///   consecutive failures (about 3 s for a worker that is restarting, warming
///   or invalidated, all of which answer 503 at once or refuse the
///   connection) and returns after one pass. The 5 s timeout is far above
///   what the handler needs, since it answers from counters without waiting
///   on query work, so only a worker that has stopped answering at all is
///   timed out.
/// - Passive: a worker leaves after three proxy errors within 10 s. A
///   restarting worker refuses every connection and reaches that within
///   milliseconds under traffic; connection refusals are retried on another
///   worker within `lb_try_duration`, so no wallet sees them. A worker
///   answering 503 is not counted at all: that is the worker's own
///   capacity refusal, and it carries the delay the wallet backs off by.
///
/// Only directives Caddy 2.6.2 accepts: the production router runs that
/// version, so there is no `health_fails` and an active readiness failure
/// removes a worker until its next successful check, one second later.
pub const HEALTH: &str = "\t\t\thealth_uri /v1/ready\n\t\t\thealth_interval 1s\n\t\t\thealth_timeout 5s\n\t\t\tfail_duration 10s\n\t\t\tmax_fails 3\n";

/// Errors the proxy itself produces — no worker available (503), none
/// reachable (502), none answering in time (504) — carry a retry delay, as
/// the worker's own capacity refusal does. Without it a wallet reads the
/// router's 503 as a terminal failure, abandons the sync, and a load client
/// resubmits at once: the resubmissions are what kept the bench fleet's
/// router refusing. The status is the proxy's own.
pub const PROXY_ERRORS: &str = "\n\thandle_errors {\n\t\theader Retry-After 1\n\t\trespond \"no worker could take the request; retry shortly\"\n\t}\n";

/// The route handlers one site carries, shared by the public and the
/// internal listener.
fn render_routes(assignment: &Assignment) -> String {
    let mut out = String::new();
    let health = HEALTH;

    // Replica groups first, then owners; both keyed by shard id.
    let mut groups: BTreeMap<&str, (Vec<&str>, &[u64])> = BTreeMap::new();
    let mut owners: Vec<&WorkerAssignment> = Vec::new();
    for worker in &assignment.workers {
        match worker.role {
            WorkerRole::RecentReplica => {
                let group = worker.replica_group.as_deref().unwrap_or("recent");
                let entry = groups.entry(group).or_insert((Vec::new(), &worker.shards));
                entry.0.push(&worker.upstream);
            }
            WorkerRole::ArchiveOwner => owners.push(worker),
        }
    }
    let ids_pattern = |ids: &[u64]| {
        ids.iter()
            .map(|id| id.to_string())
            .collect::<Vec<_>>()
            .join("|")
    };
    for (group, (upstreams, shards)) in &groups {
        if shards.is_empty() {
            continue;
        }
        out.push_str(&format!(
            "\t@{group} path_regexp ^/v1/shards/({})/revisions/[0-9a-f]{{64}}/(setup|query)/\n",
            ids_pattern(shards)
        ));
        out.push_str(&format!(
            "\thandle @{group} {{\n\t\treverse_proxy {} {{\n\t\t\tlb_policy round_robin\n\t\t\tlb_try_duration 2s\n{health}\t\t}}\n\t}}\n\n",
            upstreams.join(" ")
        ));
    }
    for owner in &owners {
        if owner.shards.is_empty() {
            continue;
        }
        let name = owner.id.replace(|c: char| !c.is_ascii_alphanumeric(), "_");
        out.push_str(&format!(
            "\t@owner_{name} path_regexp ^/v1/shards/({})/revisions/[0-9a-f]{{64}}/(setup|query)/\n",
            ids_pattern(&owner.shards)
        ));
        out.push_str(&format!(
            "\thandle @owner_{name} {{\n\t\treverse_proxy {} {{\n{health}\t\t}}\n\t}}\n\n",
            owner.upstream
        ));
    }

    // Set-wide public bytes from whichever pool has the most members: every
    // worker holds every manifest and filter.
    let public_pool: Vec<&str> = groups
        .values()
        .max_by_key(|(upstreams, _)| upstreams.len())
        .map(|(upstreams, _)| upstreams.clone())
        .unwrap_or_else(|| owners.iter().map(|o| o.upstream.as_str()).collect());
    let pool = public_pool.join(" ");
    out.push_str(&format!(
        "\t@manifest path_regexp ^/v1/shards/[0-9]+/revisions/[0-9a-f]{{64}}/manifest$\n\
         \thandle @manifest {{\n\t\treverse_proxy {pool} {{\n\t\t\tlb_policy round_robin\n{health}\t\t}}\n\t}}\n\n"
    ));
    out.push_str(&format!(
        "\thandle /v1/shards/init {{\n\t\treverse_proxy {pool} {{\n\t\t\tlb_policy round_robin\n{health}\t\t}}\n\t}}\n\n"
    ));
    out.push_str(&format!(
        "\thandle /v1/shards {{\n\t\treverse_proxy {pool} {{\n\t\t\tlb_policy round_robin\n{health}\t\t}}\n\t}}\n\n"
    ));
    out.push_str(&format!(
        "\thandle /v1/filters/* {{\n\t\treverse_proxy {pool} {{\n\t\t\tlb_policy round_robin\n{health}\t\t}}\n\t}}\n\n"
    ));
    // Everything else, including any shard id no worker holds and every
    // operator route, is a 404 at the edge.
    out
}

/// The files a worker needs from a published set, relative to the set's
/// root: the map, every current revision's manifest and filter, and the
/// whole directory of every assigned shard's current and retained revisions.
///
/// Superseded revisions are included only for assigned shards, and only
/// those on disk at the source; the worker's own retention bound decides
/// what it keeps.
pub fn files_for(
    set_dir: &std::path::Path,
    map: &ShardMap,
    assignment: &Assignment,
    worker_id: &str,
) -> Result<Vec<String>, PlanError> {
    let scope = assignment
        .scope_for(worker_id)
        .map_err(|error| PlanError::Invalid(error.to_string()))?;
    let mut files = vec!["shards.json".to_string()];
    let current: std::collections::BTreeSet<&str> = map
        .shards
        .iter()
        .map(|e| e.manifest_digest.as_str())
        .collect();
    let mut by_shard: BTreeMap<u64, Vec<String>> = BTreeMap::new();
    for entry in std::fs::read_dir(set_dir)
        .map_err(|error| PlanError::Invalid(format!("{}: {error}", set_dir.display())))?
    {
        let entry = entry.map_err(|error| PlanError::Invalid(error.to_string()))?;
        if !entry.path().is_dir() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().into_owned();
        let manifest: transparent_shard::manifest::ShardManifest =
            match std::fs::read(entry.path().join("manifest.json"))
                .ok()
                .and_then(|raw| serde_json::from_slice(&raw).ok())
            {
                Some(manifest) => manifest,
                None => continue,
            };
        if current.contains(name.as_str()) {
            files.push(format!("{name}/manifest.json"));
            files.push(format!("{name}/filter.bin"));
        }
        by_shard.entry(manifest.shard_id).or_default().push(name);
    }
    for id in &scope.assigned {
        for name in by_shard.get(id).into_iter().flatten() {
            files.push(format!("{name}/"));
        }
    }
    files.sort();
    files.dedup();
    Ok(files)
}

#[cfg(test)]
mod tests {
    use super::*;
    use transparent_filter::SealParameters;

    fn entry(shard_id: u64, geometry: &str) -> ShardMapEntry {
        ShardMapEntry {
            shard_id,
            geometry: geometry.into(),
            start_height: shard_id * 100,
            end_height: shard_id * 100 + 99,
            parent_block_hash: "00".repeat(32),
            terminal_block_hash: "11".repeat(32),
            filter_hash: "22".repeat(32),
            scripts: 1,
            page_rows: 1,
            txids: 0,
            directory_segments: 1,
            page_segments: 1,
            manifest_digest: format!("{:064x}", shard_id),
            revision: 0,
            sealed: true,
        }
    }

    fn map(archive: u64, recent: u64) -> ShardMap {
        let mut shards = Vec::new();
        for id in 0..archive {
            shards.push(entry(id, "archive-wide"));
        }
        for id in archive..archive + recent {
            shards.push(entry(id, "recent-8k"));
        }
        ShardMap {
            genesis_hash: "33".repeat(32),
            network: "main".into(),
            profile: "zcash-transparent-range-v1".into(),
            range_envelope_version: 1,
            start_height: 0,
            seal: BTreeMap::from([(
                "recent-8k".to_string(),
                SealParameters {
                    max_scripts: 1,
                    max_page_rows: 1,
                    max_txids: 0,
                },
            )]),
            shards,
        }
    }

    fn roster(owners: usize, replicas: usize, cache: u64) -> Vec<RosterEntry> {
        let mut roster = Vec::new();
        for i in 0..owners {
            roster.push(RosterEntry {
                id: format!("archive-{i}"),
                role: WorkerRole::ArchiveOwner,
                replica_group: None,
                ssh_host: format!("10.0.1.{i}"),
                upstream: format!("10.0.1.{i}:8093"),
                cache_bytes: cache,
                archive_range: None,
            });
        }
        for i in 0..replicas {
            roster.push(RosterEntry {
                id: format!("recent-{i}"),
                role: WorkerRole::RecentReplica,
                replica_group: None,
                ssh_host: format!("10.0.2.{i}"),
                upstream: format!("10.0.2.{i}:8093"),
                cache_bytes: cache,
                archive_range: None,
            });
        }
        roster
    }

    fn generated() -> GeneratedBy {
        GeneratedBy {
            tool: "test".into(),
            source_sha: None,
            generated_at: "now".into(),
        }
    }

    fn pin(roster: &mut [RosterEntry], ranges: &[[u64; 2]]) {
        for (owner, range) in roster
            .iter_mut()
            .filter(|e| e.role == WorkerRole::ArchiveOwner)
            .zip(ranges)
        {
            owner.archive_range = Some(*range);
        }
    }

    #[test]
    fn pinned_ranges_reproduce_the_balanced_plan_byte_for_byte() {
        let map = map(10, 4);
        let free = plan(&map, "ab", &roster(2, 4, 64 << 30), 10, 0.1, generated()).unwrap();
        let mut pinned_roster = roster(2, 4, 64 << 30);
        pin(&mut pinned_roster, &[[0, 4], [5, 9]]);
        let pinned = plan(&map, "ab", &pinned_roster, 10, 0.1, generated()).unwrap();
        assert_eq!(pinned.canonical_bytes(), free.canonical_bytes());
    }

    #[test]
    fn recent_replicas_come_and_go_without_moving_an_archive_cut() {
        let map = map(10, 4);
        let mut pinned_roster = roster(2, 6, 64 << 30);
        // A deliberately unbalanced cut: pinning keeps it, re-cutting would not.
        pin(&mut pinned_roster, &[[0, 2], [3, 9]]);
        let six = plan(&map, "ab", &pinned_roster, 10, 0.1, generated()).unwrap();
        pinned_roster.truncate(3);
        let one = plan(&map, "ab", &pinned_roster, 10, 0.1, generated()).unwrap();
        for id in ["archive-0", "archive-1"] {
            assert_eq!(
                six.worker_digest(id).unwrap(),
                one.worker_digest(id).unwrap()
            );
        }
        assert_eq!(six.worker("archive-0").unwrap().shards, vec![0, 1, 2]);
        assert_eq!(
            six.worker("archive-1").unwrap().shards,
            (3..10).collect::<Vec<_>>()
        );
    }

    #[test]
    fn pinned_ranges_must_cover_the_archive_exactly_and_all_owners() {
        let map = map(10, 4);
        for ranges in [
            vec![[0, 4], [6, 9]],
            vec![[0, 5], [5, 9]],
            vec![[1, 4], [5, 9]],
            vec![[0, 4], [5, 8]],
            vec![[0, 4], [5, 10]],
        ] {
            let mut roster = roster(2, 1, 64 << 30);
            pin(&mut roster, &ranges);
            assert!(
                plan(&map, "ab", &roster, 10, 0.1, generated()).is_err(),
                "{ranges:?} must be refused"
            );
        }
        let mut partial = roster(2, 1, 64 << 30);
        partial[0].archive_range = Some([0, 9]);
        let error = plan(&map, "ab", &partial, 10, 0.1, generated()).unwrap_err();
        assert!(error.to_string().contains("pin every archive owner"));
    }

    #[test]
    fn a_pinned_owner_over_budget_is_refused() {
        let map = map(10, 4);
        let mut roster = roster(2, 1, 64 << 30);
        roster[0].cache_bytes = 1;
        pin(&mut roster, &[[0, 4], [5, 9]]);
        assert!(plan(&map, "ab", &roster, 10, 0.1, generated())
            .unwrap_err()
            .to_string()
            .contains("pinned range"));
    }

    /// The production cache of an m-8vcpu-64gb archive owner.
    const OWNER_CACHE: u64 = 51_539_607_552;

    /// The production shape with one archive owner: shards 0-76 archive,
    /// 77-85 recent, the whole archive pinned to a single owner.
    fn single_owner_roster(replicas: usize, cache: u64) -> Vec<RosterEntry> {
        let mut roster = roster(1, replicas, cache);
        pin(&mut roster, &[[0, 76]]);
        roster
    }

    fn archive_bytes(map: &ShardMap, recent_from: u64) -> u64 {
        map.shards[..recent_from as usize]
            .iter()
            .map(|entry| shard_reserved_bytes(entry).unwrap())
            .sum()
    }

    #[test]
    fn a_single_pinned_owner_holds_the_whole_archive() {
        let map = map(77, 9);
        let pinned = single_owner_roster(2, OWNER_CACHE);
        let assignment = plan(&map, "ab", &pinned, 77, 0.05, generated()).unwrap();
        assignment.check_shape().unwrap();
        let owners: Vec<&WorkerAssignment> = assignment
            .workers
            .iter()
            .filter(|w| w.role == WorkerRole::ArchiveOwner)
            .collect();
        assert_eq!(owners.len(), 1);
        assert_eq!(owners[0].id, "archive-0");
        assert_eq!(owners[0].shards, (0..77).collect::<Vec<_>>());
        assert_eq!(owners[0].estimated_resident_bytes, archive_bytes(&map, 77));
        let replicas: Vec<&WorkerAssignment> = assignment
            .workers
            .iter()
            .filter(|w| w.role == WorkerRole::RecentReplica)
            .collect();
        assert_eq!(replicas.len(), 2);
        for replica in replicas {
            assert_eq!(replica.shards, (77..86).collect::<Vec<_>>());
            assert_eq!(replica.replica_group.as_deref(), Some("recent"));
        }
        assert!(assignment.unassigned.is_empty());
        // Pinning the one range and leaving it free cut the same archive.
        let unpinned = roster(1, 2, OWNER_CACHE);
        let free = plan(&map, "ab", &unpinned, 77, 0.05, generated()).unwrap();
        assert_eq!(assignment.canonical_bytes(), free.canonical_bytes());
    }

    #[test]
    fn a_single_pinned_owner_is_refused_when_its_headroom_does_not_fit() {
        let map = map(77, 9);
        // Scale the cache to the prototype's measurement: 41.35 GiB reserved
        // against 48 GiB, which fits at 5% headroom and not at 15%.
        let bytes = archive_bytes(&map, 77);
        let cache = (bytes as f64 * 48.0 / 41.35) as u64;
        let mut roster = single_owner_roster(2, OWNER_CACHE);
        roster[0].cache_bytes = cache;
        plan(&map, "ab", &roster, 77, 0.05, generated()).unwrap();
        let error = plan(&map, "ab", &roster, 77, 0.15, generated()).unwrap_err();
        assert!(error.to_string().contains("pinned range"), "{error}");
        assert!(error.to_string().contains("archive-0"), "{error}");

        // A single owner cannot fall back on a second owner's memory.
        let mut small = single_owner_roster(2, OWNER_CACHE);
        small[0].cache_bytes = bytes;
        let error = plan(&map, "ab", &small, 77, 0.05, generated()).unwrap_err();
        assert!(error.to_string().contains("pinned range"), "{error}");
    }

    #[test]
    fn the_archive_is_split_contiguously_and_balanced_by_reserved_bytes() {
        let map = map(10, 4);
        let assignment = plan(&map, "ab", &roster(2, 4, 64 << 30), 10, 0.1, generated()).unwrap();
        let owners: Vec<&WorkerAssignment> = assignment
            .workers
            .iter()
            .filter(|w| w.role == WorkerRole::ArchiveOwner)
            .collect();
        assert_eq!(owners.len(), 2);
        assert_eq!(owners[0].shards, vec![0, 1, 2, 3, 4]);
        assert_eq!(owners[1].shards, vec![5, 6, 7, 8, 9]);
        assert_eq!(
            owners[0].estimated_resident_bytes,
            owners[1].estimated_resident_bytes
        );
        let replicas: Vec<&WorkerAssignment> = assignment
            .workers
            .iter()
            .filter(|w| w.role == WorkerRole::RecentReplica)
            .collect();
        assert_eq!(replicas.len(), 4);
        for replica in replicas {
            assert_eq!(replica.shards, vec![10, 11, 12, 13]);
            assert_eq!(replica.replica_group.as_deref(), Some("recent"));
        }
        assert!(assignment.unassigned.is_empty());
    }

    #[test]
    fn a_roster_that_cannot_hold_its_share_is_refused() {
        let map = map(10, 4);
        let error = plan(&map, "ab", &roster(2, 4, 1 << 30), 10, 0.1, generated()).unwrap_err();
        assert!(error.to_string().contains("rely on eviction"), "{error}");
    }

    #[test]
    fn the_caddyfile_routes_each_shard_to_its_owners_and_nothing_else() {
        let map = map(4, 2);
        let assignment = plan(&map, "ab", &roster(2, 2, 64 << 30), 4, 0.1, generated()).unwrap();
        let rendered = render_caddyfile(&assignment, "transparent.example");
        assert!(rendered.contains("transparent.example {"));
        assert!(rendered.contains("@recent path_regexp ^/v1/shards/(4|5)/revisions/"));
        assert!(rendered.contains("reverse_proxy 10.0.2.0:8093 10.0.2.1:8093 {"));
        assert!(rendered.contains("@owner_archive_0 path_regexp ^/v1/shards/(0|1)/revisions/"));
        assert!(rendered.contains("@owner_archive_1 path_regexp ^/v1/shards/(2|3)/revisions/"));
        assert!(rendered.contains("health_uri /v1/ready"));
        assert!(!rendered.contains("/metrics"));
        assert!(rendered.ends_with(&format!(
            "\thandle {{\n\t\trespond 404\n\t}}\n{PROXY_ERRORS}}}\n"
        )));
    }

    #[test]
    fn a_saturated_pool_is_not_ejected_on_a_single_proxy_error() {
        let map = map(4, 2);
        let assignment = plan(&map, "ab", &roster(2, 2, 64 << 30), 4, 0.1, generated()).unwrap();
        let rendered =
            render_caddyfile_with(&assignment, "transparent.example", Some("10.0.0.5:8080"));
        // Every pool on both sites, archive owners included, carries the same
        // policy: one recent group, two owners, four public routes.
        let pools = rendered.matches("reverse_proxy ").count();
        assert_eq!(pools, 2 * (1 + 2 + 4));
        assert_eq!(rendered.matches(HEALTH).count(), pools);
        // Caddy's default `max_fails` is 1: with it, one proxy error took a
        // saturated worker out for the whole `fail_duration`.
        // The production router runs Caddy 2.6.2, which has no `health_fails`
        // and no status list on `handle_errors`; a rendered file using either
        // fails to load and withdraws routing (2026-09-28).
        assert!(!rendered.contains("health_fails"));
        assert!(!rendered.contains("handle_errors 5"));
        let max_fails: u32 = HEALTH
            .lines()
            .find_map(|line| line.trim().strip_prefix("max_fails"))
            .expect("max_fails is set")
            .trim()
            .parse()
            .unwrap();
        assert!(max_fails > 1, "max_fails {max_fails}");
        assert!(!rendered.contains("fail_duration 30s"));
        // Both sites give the proxy's own refusals a retry delay.
        assert_eq!(rendered.matches(PROXY_ERRORS).count(), 2);
        assert!(PROXY_ERRORS.contains("header Retry-After"));
    }
}
