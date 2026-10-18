//! Offline evidence and report generation. Request traces stay on disk; only
//! per-wallet aggregates and one-second bins are retained by the supervisor.
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::scenario::{now, Config, Mode};

#[derive(Serialize, Deserialize)]
pub struct Report {
    pub schema: String,
    pub run_id: String,
    pub config: Config,
    pub sample_sha256: String,
    pub created_at: f64,
    pub started_at: Option<f64>,
    pub execution_finished_at: Option<f64>,
    pub finished_at: Option<f64>,
    pub publication: Value,
    pub publication_stable: Option<bool>,
    #[serde(default)]
    pub publication_compatible: Option<bool>,
    pub interrupted: bool,
    pub success: bool,
    pub errors: Vec<String>,
    pub users: Vec<Value>,
    #[serde(default)]
    pub preparation: Vec<Value>,
    #[serde(default)]
    pub preparation_status: Value,
    #[serde(default)]
    pub phase: String,
    pub metrics: Vec<Value>,
    pub seconds: BTreeMap<u64, Value>,
    pub summary: Value,
    pub provenance: Value,
}
fn git(args: &[&str]) -> Option<String> {
    std::process::Command::new("git")
        .args(args)
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
}
fn add(value: &mut Value, key: &str, n: f64) {
    value[key] = json!(value[key].as_f64().unwrap_or(0.0) + n);
}
fn number(value: &Value, key: &str) -> f64 {
    value[key].as_f64().unwrap_or(0.0)
}
fn add_statuses(target: &mut Value, source: &Value) {
    if let Some(statuses) = source["http_status_counts"].as_object() {
        if !target["http_status_counts"].is_object() {
            target["http_status_counts"] = json!({});
        }
        for (status, n) in statuses {
            add(
                &mut target["http_status_counts"],
                status,
                n.as_f64().unwrap_or(0.0),
            );
        }
    }
}

fn distribution(mut seconds: Vec<f64>) -> Value {
    seconds.sort_by(f64::total_cmp);
    let percentile = |p: f64| {
        if seconds.is_empty() {
            None
        } else {
            Some(seconds[((seconds.len() as f64 * p).ceil() as usize).saturating_sub(1)])
        }
    };
    json!({"n":seconds.len(),"sparse":seconds.len()<100,"p50":percentile(0.5),"p95":percentile(0.95),"p99":percentile(0.99),"max":seconds.last()})
}
impl Report {
    pub fn new(config: &Config, sample_sha256: String) -> Self {
        let mut system = sysinfo::System::new();
        system.refresh_cpu_all();
        system.refresh_memory();
        Self {
            schema: "transparent-simulation-v1".into(),
            run_id: format!(
                "simulation-{}-{}",
                (now() * 1000.0) as u64,
                std::process::id()
            ),
            config: config.clone(),
            sample_sha256,
            created_at: now(),
            started_at: None,
            execution_finished_at: None,
            finished_at: None,
            publication: Value::Null,
            publication_stable: None,
            publication_compatible: None,
            interrupted: false,
            success: false,
            errors: Vec::new(),
            users: Vec::new(),
            preparation: Vec::new(),
            preparation_status: Value::Null,
            phase: "preparing".into(),
            metrics: Vec::new(),
            seconds: BTreeMap::new(),
            summary: Value::Null,
            provenance: json!({"source_sha":env!("SIMULATION_SOURCE_SHA"),"build_working_tree_dirty":env!("SIMULATION_SOURCE_DIRTY"),"build_profile":env!("SIMULATION_PROFILE"),"build_opt_level":env!("SIMULATION_OPT_LEVEL"),"build_target":env!("SIMULATION_TARGET"),"invocation_checkout_sha":git(&["rev-parse","HEAD"]),"command":std::env::args().collect::<Vec<_>>(),"os":sysinfo::System::long_os_version(),"kernel":sysinfo::System::kernel_version(),"host":sysinfo::System::host_name(),"cpu":system.cpus().first().map(|c|c.brand()),"logical_cpus":system.cpus().len(),"memory_bytes":system.total_memory(),"build_optimized":!cfg!(debug_assertions)}),
        }
    }
    pub fn event(&mut self, event: &Value) {
        let Some(id) = event["id"].as_u64().map(|n| n as usize) else {
            return;
        };
        match event["type"].as_str() {
            Some("scheduled") => {
                let mut user = event.clone();
                user["scheduled_at"] = event["at"].clone();
                user["outcome"] = json!("running");
                user["stages"] = json!({});
                self.users.push(user);
            }
            Some("started") => {
                if let Some(user) = self.users.get_mut(id) {
                    user["started_at"] = event["at"].clone();
                }
            }
            Some("outcome") => {
                if let Some(user) = self.users.get_mut(id) {
                    if user["outcome"] != "running" {
                        return;
                    }
                    for (key, value) in event.as_object().unwrap() {
                        user[key] = value.clone();
                    }
                    user["finished_at"] = event["at"].clone();
                    user["seconds"] = json!((number(event, "at")
                        - user["started_at"]
                            .as_f64()
                            .unwrap_or(number(user, "scheduled_at")))
                    .max(0.0));
                }
            }
            _ => {}
        }
    }
    pub fn request(&mut self, event: &Value) {
        let Some(user) = event["id"]
            .as_u64()
            .and_then(|id| self.users.get_mut(id as usize))
        else {
            return;
        };
        let stage = event["stage"].as_str().unwrap_or("unknown");
        if !user["stages"][stage].is_object() {
            user["stages"][stage] = json!({});
        }
        let totals = &mut user["stages"][stage];
        add(totals, "calls", 1.0);
        for key in ["bytes_up", "bytes_down", "seconds"] {
            add(totals, key, number(event, key));
        }
        if event["failed"] == true {
            add(totals, "failures", 1.0);
        }
        if event["status"] == 503 {
            add(totals, "http_503", 1.0);
        }
        if event["status"] == 409 {
            add(totals, "http_409", 1.0);
        }
        if let Some(status) = event["status"].as_u64() {
            if !totals["http_status_counts"].is_object() {
                totals["http_status_counts"] = json!({});
            }
            add(&mut totals["http_status_counts"], &status.to_string(), 1.0);
        }
        if event["attempt"].as_u64().unwrap_or(1) > 1 {
            add(totals, "retry_attempts", 1.0);
        }
        if event["attempt"] == 2 {
            add(totals, "retried_requests", 1.0);
        }
        let end = number(event, "at");
        let begin = end - number(event, "seconds");
        if totals["first_at"].as_f64().is_none_or(|old| begin < old) {
            totals["first_at"] = json!(begin);
        }
        totals["last_at"] = json!(totals["last_at"].as_f64().map_or(end, |old| old.max(end)));
        let bin = (end - self.started_at.unwrap_or(self.created_at))
            .max(0.0)
            .floor() as u64;
        let totals = self.seconds.entry(bin).or_insert_with(|| json!({}));
        add(totals, "requests", 1.0);
        add(totals, "request_seconds", number(event, "seconds"));
        add(totals, "bytes_up", number(event, "bytes_up"));
        add(totals, "bytes_down", number(event, "bytes_down"));
        if event["failed"] == true {
            add(totals, "failures", 1.0);
        }
    }
    pub fn cancel_unfinished(&mut self) {
        for user in &mut self.users {
            if user["outcome"] == "running" {
                user["outcome"] = json!("cancelled");
                user["finished_at"] = json!(now());
                user["seconds"] = json!(
                    now()
                        - user["started_at"]
                            .as_f64()
                            .unwrap_or(number(user, "scheduled_at"))
                );
            }
        }
    }
    pub fn finalize(&mut self) {
        self.cancel_unfinished();
        for user in &mut self.users {
            let mut totals = json!({"calls":0.0,"bytes_up":0.0,"bytes_down":0.0,"seconds":0.0,"failures":0.0,"http_503":0.0,"http_409":0.0});
            if let Some(stages) = user["stages"].as_object() {
                for stage in stages.values() {
                    for key in [
                        "calls",
                        "bytes_up",
                        "bytes_down",
                        "seconds",
                        "failures",
                        "http_503",
                        "http_409",
                        "retry_attempts",
                        "retried_requests",
                    ] {
                        add(&mut totals, key, number(stage, key));
                    }
                }
            }
            if let Some(stages) = user["stages"].as_object() {
                for stage in stages.values() {
                    add_statuses(&mut totals, stage);
                }
            }
            user["exact_after_retry"] =
                json!(user["outcome"] == "exact" && number(&totals, "retry_attempts") > 0.0);
            user["http_totals"] = totals;
        }
        let started = self.started_at.unwrap_or(self.created_at);
        let end = self
            .execution_finished_at
            .or(self.finished_at)
            .unwrap_or_else(now);
        let duration = (end - started).max(0.001);
        let load_seconds = if self.config.mode == Mode::Sustained {
            duration.min(self.config.duration_seconds as f64)
        } else {
            duration
        };
        let load_end = started + load_seconds;
        let mut outcomes: BTreeMap<String, u64> = BTreeMap::new();
        let mut profiles = serde_json::Map::new();
        let mut stages = json!({});
        for user in &self.users {
            *outcomes
                .entry(user["outcome"].as_str().unwrap_or("unknown").into())
                .or_default() += 1;
            if let Some(map) = user["stages"].as_object() {
                for (stage, totals) in map {
                    if !stages[stage].is_object() {
                        stages[stage] = json!({});
                    }
                    add_statuses(&mut stages[stage], totals);
                    for key in [
                        "calls",
                        "bytes_up",
                        "bytes_down",
                        "seconds",
                        "failures",
                        "http_503",
                        "http_409",
                        "retry_attempts",
                        "retried_requests",
                    ] {
                        add(&mut stages[stage], key, number(totals, key));
                    }
                }
            }
        }
        for profile in self.config.profiles.keys() {
            let users: Vec<_> = self
                .users
                .iter()
                .filter(|u| u["profile"] == *profile)
                .collect();
            let exact: Vec<_> = users
                .iter()
                .filter(|u| u["outcome"] == "exact")
                .map(|u| number(u, "seconds"))
                .collect();
            let unsuccessful: Vec<_> = users
                .iter()
                .filter(|u| u["outcome"] != "exact")
                .map(|u| number(u, "seconds"))
                .collect();
            let mut profile_stages = json!({});
            for user in &users {
                if let Some(stages) = user["stages"].as_object() {
                    for (stage, totals) in stages {
                        if !profile_stages[stage].is_object() {
                            profile_stages[stage] = json!({});
                        }
                        add_statuses(&mut profile_stages[stage], totals);
                        for key in [
                            "calls",
                            "bytes_up",
                            "bytes_down",
                            "seconds",
                            "failures",
                            "http_503",
                            "http_409",
                            "retry_attempts",
                            "retried_requests",
                        ] {
                            add(&mut profile_stages[stage], key, number(totals, key));
                        }
                    }
                }
            }
            profiles.insert(profile.clone(),json!({"started":users.len(),"exact_seconds":distribution(exact),"unsuccessful_seconds":distribution(unsuccessful),"stages":profile_stages}));
        }
        let exact = outcomes.get("exact").copied().unwrap_or(0);
        let exact_during_load = self
            .users
            .iter()
            .filter(|u| u["outcome"] == "exact" && number(u, "finished_at") <= load_end)
            .count();
        self.summary = json!({"started":self.users.len(),"outcomes":outcomes,"exact_after_retry":self.users.iter().filter(|u|u["exact_after_retry"]==true).count(),"execution_seconds":duration,"load_seconds":load_seconds,"drain_seconds":(duration-load_seconds).max(0.0),"exact_per_second":exact as f64/duration,"exact_during_load":exact_during_load,"exact_during_drain":exact-exact_during_load as u64,"exact_per_load_second":exact_during_load as f64/load_seconds.max(0.001),"profiles":profiles,"stages":stages});
        let mut wallets: BTreeMap<String, Vec<&Value>> = BTreeMap::new();
        for user in &self.users {
            wallets
                .entry(user["sample_index"].to_string())
                .or_default()
                .push(user);
        }
        self.summary["wallets"] = json!(wallets.into_iter().map(|(sample, users)| {
            let mut totals = json!({});
            let mut outcomes: BTreeMap<String, usize> = BTreeMap::new();
            for user in &users {
                add_statuses(&mut totals,&user["http_totals"]);
                *outcomes.entry(user["outcome"].as_str().unwrap_or("unknown").to_owned()).or_default() += 1;
                for key in ["calls", "bytes_up", "bytes_down", "seconds", "failures", "http_503", "http_409", "retry_attempts", "retried_requests"] {
                    add(&mut totals, key, number(&user["http_totals"], key));
                }
            }
            (sample, json!({"profile":users[0]["profile"], "script_count":users[0]["script_count"],
                "recoveries":users.len(), "outcomes":outcomes,"exact_after_retry":users.iter().filter(|u|u["exact_after_retry"]==true).count(), "http_totals":totals,
                "exact_seconds":distribution(users.iter().filter(|u|u["outcome"] == "exact").map(|u|number(u,"seconds")).collect()),
                "unsuccessful_seconds":distribution(users.iter().filter(|u|u["outcome"] != "exact").map(|u|number(u,"seconds")).collect())}))
        }).collect::<BTreeMap<_,_>>());
        self.summary["client_peak_rss_bytes"] = json!(self
            .metrics
            .iter()
            .filter(|m| m["target"] == "load-client")
            .filter_map(|m| m["processes"].as_array())
            .map(|ps| ps
                .iter()
                .filter_map(|p| p["rss_bytes"].as_u64())
                .sum::<u64>())
            .max());
        self.success = !self.users.is_empty()
            && exact as usize == self.users.len()
            && (self.publication_stable == Some(true)
                || (self.config.allow_advancing_publication
                    && self.publication_compatible == Some(true)))
            && !self.interrupted
            && self.errors.is_empty();
        if let Some(limit) = self.config.max_p99_exact_seconds {
            for (profile, stats) in self.summary["profiles"].as_object().unwrap() {
                if stats["exact_seconds"]["p99"]
                    .as_f64()
                    .is_some_and(|p| p > limit)
                {
                    self.errors
                        .push(format!("{profile}: exact p99 exceeds {limit}s"));
                    self.success = false;
                }
            }
        }
    }
}

pub fn checkpoint(out: &Path, report: &Report) -> Result<()> {
    // Atomic replacement keeps the previous readable checkpoint on interruption.
    fs::write(
        out.join("report.json.next"),
        serde_json::to_vec_pretty(report)?,
    )?;
    fs::rename(out.join("report.json.next"), out.join("report.json"))?;
    Ok(())
}

pub fn write(out: &Path, report: &Report) -> Result<()> {
    checkpoint(out, report)?;
    // A JSON data island must escape '<' to prevent a value closing its script.
    let data = serde_json::to_string(report)?
        .replace('<', "\\u003c")
        .replace('>', "\\u003e")
        .replace('&', "\\u0026");
    fs::write(
        out.join("report.html"),
        include_str!("simulation_report.html")
            .replace("__APM_SCRIPT__", include_str!("simulation_apm.js"))
            .replace("__REPORT_DATA__", &data),
    )?;
    fs::write(
        out.join("manifest.json"),
        serde_json::to_vec_pretty(&json!({
            "schema":report.schema,"run_id":report.run_id,"scenario":report.config,"sample_sha256":report.sample_sha256,"publication":report.publication,"created_at":report.created_at,"finished_at":report.finished_at,"provenance":report.provenance,
            "artifacts":["report.html","report.json","wallets.ndjson","requests.ndjson","metrics.ndjson","preflight.ndjson","postflight.ndjson","scenario.json","map.json","logs/","stores/"],
            "limitations":["Synthetic public-script groupings, not a user population. Windowed recoveries import prior ledger events from separately verified full recoveries, with cold client filter/setup caches.","HTTP payloads only. Upload bytes are submitted body sizes; failed or interrupted requests may have delivered fewer bytes. Partial response bodies from failed reads are not counted.","All requests observed by each server contribute to its metrics, including background traffic. CPU/RSS are process measurements, not whole-host measurements.","Preparation may warm server caches; server caches are never reset by the harness. Parent and worker CPU compete with other work on the client host.","Hard termination cancels the client; an already running server evaluation can continue until its server-held slot is released.","Abrupt supervisor termination can leave report.json unfinished; recover evidence from append-only NDJSON. Missing artifacts after preflight failure are intentional."]
        }))?,
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn report() -> Report {
        let config: Config = serde_json::from_value(json!({"schema":"transparent-scenario-v1","name":"test </script>","mode":"wave","sample":"sample.json","shard_url":"http://localhost:1","profiles":{"small-active":2}})).unwrap();
        Report::new(&config, String::new())
    }
    #[test]
    fn failed_and_incomplete_work_is_in_denominator_not_success_latency() {
        let mut r = report();
        r.started_at = Some(100.0);
        r.execution_finished_at = Some(110.0);
        r.publication_stable = Some(true);
        for id in 0..3 {
            r.event(&json!({"type":"scheduled","id":id,"at":100.0,"profile":"small-active"}));
        }
        r.event(&json!({"type":"outcome","id":0,"at":102.0,"outcome":"exact"}));
        r.event(&json!({"type":"outcome","id":1,"at":105.0,"outcome":"incomplete"}));
        r.finalize();
        assert!(!r.success);
        assert_eq!(r.summary["started"], 3);
        assert_eq!(r.summary["outcomes"]["cancelled"], 1);
        assert_eq!(r.summary["exact_per_second"], 0.1);
        assert_eq!(
            r.summary["profiles"]["small-active"]["exact_seconds"]["p99"],
            2.0
        );
        assert_eq!(
            r.summary["profiles"]["small-active"]["exact_seconds"]["sparse"],
            true
        );
    }
    #[test]
    fn wallet_payload_totals_include_repeated_and_unsuccessful_recoveries() {
        let mut r = report();
        r.started_at = Some(100.0);
        for id in 0..2 {
            r.event(&json!({"type":"scheduled","id":id,"at":100.0,"profile":"small-active","sample_index":42}));
            r.request(&json!({"id":id,"at":101.0,"stage":"query_pages","status":200,"seconds":0.5,"bytes_up":100,"bytes_down":20}));
            r.request(&json!({"id":id,"at":102.0,"stage":"filters","status":503,"failed":true,"seconds":0.2,"bytes_up":0,"bytes_down":7}));
            r.event(&json!({"type":"outcome","id":id,"at":103.0,"outcome":if id == 0 {"exact"} else {"incomplete"}}));
        }
        r.finished_at = Some(104.0);
        r.finalize();
        assert_eq!(r.users[0]["http_totals"]["bytes_up"], 100.0);
        assert_eq!(r.users[0]["http_totals"]["bytes_down"], 27.0);
        let wallet = &r.summary["wallets"]["42"];
        assert_eq!(wallet["recoveries"], 2);
        assert_eq!(wallet["http_totals"]["bytes_up"], 200.0);
        assert_eq!(wallet["http_totals"]["bytes_down"], 54.0);
        assert_eq!(wallet["http_totals"]["calls"], 4.0);
        assert_eq!(wallet["http_totals"]["failures"], 2.0);
    }
    #[test]
    fn stages_capture_error_payloads_and_report_is_offline_and_escaped() {
        let mut r = report();
        r.started_at = Some(100.0);
        r.event(&json!({"type":"scheduled","id":0,"at":100.0,"profile":"small-active"}));
        r.request(&json!({"id":0,"at":101.0,"stage":"filters","status":503,"failed":true,"seconds":0.5,"bytes_down":12,"bytes_up":0}));
        assert_eq!(r.users[0]["stages"]["filters"]["http_503"], 1.0);
        let dir = tempfile::tempdir().unwrap();
        write(dir.path(), &r).unwrap();
        let html = fs::read_to_string(dir.path().join("report.html")).unwrap();
        assert!(html.contains("test \\u003c/script\\u003e"));
        assert!(!html.contains("<script src="));
    }
}
