use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use transparent_wallet::{
    client::Table,
    transport::{BoxError, FilterSource, ShardTransport},
};
/// Bytes and time per stage, as one client saw them.
#[derive(Default, Clone, serde::Serialize)]
pub struct StageTotals {
    calls: u64,
    routes: Vec<String>,
    up: u64,
    down: u64,
    micros: u64,
    http_409: u64,
    http_503: u64,
    failures: u64,
}

/// The bytes a (payload, cost) reply carried, or its error.
fn sized(result: &Result<(Vec<u8>, u64), BoxError>) -> Result<u64, &BoxError> {
    result.as_ref().map(|(bytes, _)| bytes.len() as u64)
}

/// A transport wrapper that records every request's stage, size, time and
/// refusal, without changing what the wallet does.
pub struct Counting<T> {
    pub inner: T,
    pub stages: Arc<Mutex<BTreeMap<&'static str, StageTotals>>>,
}

impl<T> Counting<T> {
    fn route(&self, stage: &'static str, route: String) {
        self.stages
            .lock()
            .unwrap()
            .entry(stage)
            .or_default()
            .routes
            .push(route);
    }
    fn record(
        &self,
        stage: &'static str,
        up: u64,
        result: Result<u64, &BoxError>,
        elapsed: Duration,
    ) {
        let mut stages = self.stages.lock().unwrap();
        let entry = stages.entry(stage).or_default();
        entry.calls += 1;
        entry.up += up;
        entry.micros += elapsed.as_micros() as u64;
        match result {
            Ok(down) => entry.down += down,
            Err(error) => {
                let text = error.to_string();
                if transparent_wallet::StaleRevision::found_in(error).is_some() {
                    entry.http_409 += 1;
                } else if transparent_wallet::Overloaded::found_in(error).is_some()
                    || text.contains("503")
                {
                    entry.http_503 += 1;
                } else {
                    entry.failures += 1;
                }
            }
        }
    }
}

impl<T: ShardTransport> ShardTransport for Counting<T> {
    fn init(&mut self) -> Result<(Vec<u8>, u64), BoxError> {
        let started = Instant::now();
        self.route("init", "/v1/shards/init".into());
        let result = self.inner.init();
        self.record("init", 0, sized(&result), started.elapsed());
        result
    }
    fn manifest(&mut self, shard_id: u64, revision: &str) -> Result<(Vec<u8>, u64), BoxError> {
        let started = Instant::now();
        self.route(
            "manifest",
            format!("/v1/shards/{shard_id}/revisions/{revision}/manifest"),
        );
        let result = self.inner.manifest(shard_id, revision);
        self.record("manifest", 0, sized(&result), started.elapsed());
        result
    }
    fn setup(
        &mut self,
        shard_id: u64,
        revision: &str,
        table: Table,
        segment: u32,
    ) -> Result<(Vec<u8>, u64), BoxError> {
        let started = Instant::now();
        self.route(
            match table {
                Table::Directory => "setup_directory",
                Table::Pages => "setup_pages",
            },
            format!(
                "/v1/shards/{shard_id}/revisions/{revision}/setup/{}/{segment}",
                table.as_str()
            ),
        );
        let result = self.inner.setup(shard_id, revision, table, segment);
        self.record(
            match table {
                Table::Directory => "setup_directory",
                Table::Pages => "setup_pages",
            },
            0,
            sized(&result),
            started.elapsed(),
        );
        result
    }
    fn query(
        &mut self,
        shard_id: u64,
        revision: &str,
        table: Table,
        body: &[u8],
    ) -> Result<Vec<u8>, BoxError> {
        let started = Instant::now();
        self.route(
            match table {
                Table::Directory => "query_directory",
                Table::Pages => "query_pages",
            },
            format!(
                "/v1/shards/{shard_id}/revisions/{revision}/query/{}",
                table.as_str()
            ),
        );
        let result = self.inner.query(shard_id, revision, table, body);
        self.record(
            match table {
                Table::Directory => "query_directory",
                Table::Pages => "query_pages",
            },
            body.len() as u64,
            result.as_ref().map(|b| b.len() as u64),
            started.elapsed(),
        );
        result
    }
}

pub struct CountingFilters<F> {
    pub inner: F,
    pub stages: Arc<Mutex<BTreeMap<&'static str, StageTotals>>>,
}

impl<F: FilterSource> FilterSource for CountingFilters<F> {
    fn shard_map(&mut self) -> Result<(Vec<u8>, u64), BoxError> {
        let started = Instant::now();
        let result = self.inner.shard_map();
        let mut stages = self.stages.lock().unwrap();
        let entry = stages.entry("map").or_default();
        entry.routes.push("/v1/filters/shards".into());
        entry.calls += 1;
        entry.micros += started.elapsed().as_micros() as u64;
        match sized(&result) {
            Ok(down) => entry.down += down,
            Err(_) => entry.failures += 1,
        }
        result
    }
    fn filter(&mut self, shard_id: u64) -> Result<(Vec<u8>, u64), BoxError> {
        let started = Instant::now();
        let result = self.inner.filter(shard_id);
        let mut stages = self.stages.lock().unwrap();
        let entry = stages.entry("filters").or_default();
        entry
            .routes
            .push(format!("/v1/filters/shards/{shard_id}/filter"));
        entry.calls += 1;
        entry.micros += started.elapsed().as_micros() as u64;
        match sized(&result) {
            Ok(down) => entry.down += down,
            Err(_) => entry.failures += 1,
        }
        result
    }
}
