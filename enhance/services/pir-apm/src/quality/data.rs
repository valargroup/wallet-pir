//! Validated, bounded metric snapshots and restart-aware deltas.
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Histogram {
    /// None is the open-ended final bucket, never an invented finite latency.
    pub bounds: Vec<Option<f64>>,
    pub counts: Vec<f64>,
}
impl Histogram {
    pub fn valid(&self) -> bool {
        !self.bounds.is_empty()
            && self.bounds.len() == self.counts.len()
            && self.bounds.last() == Some(&None)
            && self.bounds[..self.bounds.len() - 1]
                .iter()
                .all(|b| b.is_some_and(|v| v.is_finite() && v >= 0.))
            && self
                .bounds
                .windows(2)
                .all(|b| b[1].is_none() || b[0] < b[1])
            && self.counts.iter().all(|v| v.is_finite() && *v >= 0.)
            && self.counts.windows(2).all(|n| n[0] <= n[1])
    }
    pub fn quantile(&self, q: f64) -> Option<f64> {
        if !self.valid() {
            return None;
        }
        let count = *self.counts.last()?;
        if count == 0. {
            return None;
        }
        let target = count * q;
        let (mut lower, mut before) = (0., 0.);
        for (bound, count) in self.bounds.iter().zip(&self.counts) {
            if *count >= target {
                let upper = (*bound)?;
                return Some(if *count == before {
                    upper
                } else {
                    lower + (upper - lower) * (target - before) / (count - before)
                });
            }
            lower = bound.unwrap_or(lower);
            before = *count;
        }
        None
    }
    fn difference(&self, old: &Self) -> Option<Self> {
        if self.bounds != old.bounds || !self.valid() || !old.valid() {
            return None;
        }
        let result = Self {
            bounds: self.bounds.clone(),
            counts: self
                .counts
                .iter()
                .zip(&old.counts)
                .map(|(a, b)| a - b)
                .collect(),
        };
        result.valid().then_some(result)
    }
    pub fn merge(&mut self, other: &Self) -> bool {
        if self.bounds != other.bounds {
            return false;
        }
        for (a, b) in self.counts.iter_mut().zip(&other.counts) {
            *a += b;
        }
        true
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Values {
    pub counters: BTreeMap<String, f64>,
    pub gauges: BTreeMap<String, f64>,
    pub histograms: BTreeMap<String, Histogram>,
}
#[derive(Clone, Debug, Default)]
pub struct Reading {
    pub at: u64,
    pub incarnation: Option<String>,
    pub values: Values,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Point {
    pub at: u64,
    pub seconds: u64,
    pub observed_seconds: u64,
    pub discontinuities: u64,
    #[serde(default)]
    pub invalid_histograms: std::collections::BTreeSet<String>,
    pub values: Values,
}
impl Reading {
    pub fn delta(&self, old: Option<&Self>) -> Point {
        let mut p = Point {
            at: self.at,
            values: Values {
                gauges: self.values.gauges.clone(),
                ..Default::default()
            },
            ..Default::default()
        };
        let Some(old) = old else {
            p.discontinuities = 1;
            return p;
        };
        p.seconds = self.at.saturating_sub(old.at);
        if p.seconds == 0
            || p.seconds > 45
            || self.incarnation.is_none()
            || self.incarnation != old.incarnation
        {
            p.discontinuities = 1;
            return p;
        }
        for (key, value) in &self.values.counters {
            let Some(previous) = old.values.counters.get(key) else {
                continue;
            };
            if value < previous {
                p.discontinuities = 1;
                return p;
            }
        }
        let mut values = self.values.clone();
        values.counters = self
            .values
            .counters
            .iter()
            .filter_map(|(k, v)| old.values.counters.get(k).map(|p| (k.clone(), v - p)))
            .collect();
        values.histograms.clear();
        for (key, h) in &self.values.histograms {
            if let Some(previous) = old.values.histograms.get(key) {
                let Some(delta) = h.difference(previous) else {
                    p.discontinuities = 1;
                    return p;
                };
                values.histograms.insert(key.clone(), delta);
            }
        }
        p.observed_seconds = p.seconds;
        p.values = values;
        p
    }
}
impl Point {
    pub fn merge(&mut self, other: &Self) {
        self.seconds += other.seconds;
        self.observed_seconds += other.observed_seconds;
        self.discontinuities += other.discontinuities;
        self.values.gauges.extend(other.values.gauges.clone());
        for (k, v) in &other.values.counters {
            *self.values.counters.entry(k.clone()).or_default() += v;
        }
        self.invalid_histograms
            .extend(other.invalid_histograms.iter().cloned());
        self.values
            .histograms
            .retain(|k, _| !self.invalid_histograms.contains(k));
        for (k, v) in &other.values.histograms {
            if self.invalid_histograms.contains(k) {
                continue;
            }
            if let Some(old) = self.values.histograms.get_mut(k) {
                if !old.merge(v) {
                    self.values.histograms.remove(k);
                    self.invalid_histograms.insert(k.clone());
                    self.discontinuities += 1;
                }
            } else {
                self.values.histograms.insert(k.clone(), v.clone());
            }
        }
    }
}

/// Fixed categories only; identities and public revision paths are deliberately dropped.
fn label_allowed(key: &str, value: &str) -> bool {
    match key {
        "endpoint" => matches!(
            value,
            "init"
                | "query"
                | "map"
                | "filter"
                | "manifest"
                | "setup"
                | "query_directory"
                | "query_pages"
                | "query_other"
        ),
        "code" | "status" => {
            value.len() == 3
                && (value.bytes().all(|c| c.is_ascii_digit())
                    || matches!(value, "1xx" | "2xx" | "3xx" | "4xx" | "5xx"))
        }
        "outcome" | "result" => matches!(value, "success" | "error" | "cancelled" | "failure"),
        "stage" => matches!(value, "total" | "worker" | "packing" | "queue"),
        "table" => matches!(value, "directory" | "pages"),
        "server" => matches!(value, "srv0" | "srv1" | "srv2" | "srv3"),
        "method" => matches!(
            value,
            "GET"
                | "POST"
                | "HEAD"
                | "OPTIONS"
                | "PUT"
                | "DELETE"
                | "PATCH"
                | "CONNECT"
                | "TRACE"
                | "OTHER"
        ),
        _ => false,
    }
}
fn allowed(name: &str) -> bool {
    [
        "pir_http_",
        "transparent_shard_",
        "transparent_publication_",
        "enhance_http_",
        "enhance_query_stage_",
        "enhance_worker_",
        "enhance_packing_",
        "pir_host_",
    ]
    .iter()
    .any(|p| name.starts_with(p))
        || matches!(
            name,
            "process_start_time_seconds"
                | "process_resident_memory_bytes"
                | "caddy_config_last_reload_successful"
                | "caddy_config_last_reload_success_timestamp_seconds"
        )
        || name.starts_with("caddy_http_")
        || name == "pir_observation_timestamp_seconds"
}
pub fn parse(text: &str, at: u64) -> Result<Reading, &'static str> {
    let mut result = Reading {
        at,
        ..Default::default()
    };
    let mut buckets: BTreeMap<String, Vec<(Option<f64>, f64)>> = BTreeMap::new();
    for line in text
        .lines()
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
    {
        let s = crate::metrics::parse_line(line).map_err(|_| "malformed metrics")?;
        if !allowed(&s.name) {
            continue;
        }
        if !s.value.is_finite() || s.value < 0. {
            return Err("invalid metric value");
        }
        if s.name == "pir_observation_timestamp_seconds" {
            if s.value > at as f64 || at.saturating_sub(s.value as u64) > 45 {
                return Err("stale snapshot");
            }
            result.at = s.value as u64;
        }
        // Caddy instruments nested handlers. Exactly one reverse_proxy boundary is used.
        if s.name.starts_with("caddy_http_")
            && s.labels.get("handler").map(String::as_str) != Some("reverse_proxy")
        {
            continue;
        }
        if s.labels.contains_key("endpoint") && !label_allowed("endpoint", &s.labels["endpoint"]) {
            continue;
        }
        let suffix = s
            .labels
            .iter()
            .filter(|(k, v)| label_allowed(k, v))
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join(",");
        let key = |name: &str| {
            if suffix.is_empty() {
                name.to_owned()
            } else {
                format!("{name}|{suffix}")
            }
        };
        if s.name.ends_with("process_start_time_seconds") || s.name == "process_start_time_seconds"
        {
            result.incarnation = Some(s.value.to_string());
        }
        if let Some(name) = s.name.strip_suffix("_bucket") {
            let le = s.labels.get("le").ok_or("missing histogram bound")?;
            let bound = if le == "+Inf" {
                None
            } else {
                Some(le.parse::<f64>().map_err(|_| "invalid histogram bound")?)
            };
            buckets.entry(key(name)).or_default().push((bound, s.value));
        } else if s.name.ends_with("_total")
            || s.name.ends_with("_count")
            || s.name.ends_with("_sum")
        {
            if result
                .values
                .counters
                .insert(key(&s.name), s.value)
                .is_some()
            {
                return Err("ambiguous metric dimensions");
            }
        } else if result.values.gauges.insert(key(&s.name), s.value).is_some() {
            return Err("ambiguous metric dimensions");
        }
        if result.values.counters.len() + result.values.gauges.len() + buckets.len() > 512 {
            return Err("too many series");
        }
    }
    for (name, mut pairs) in buckets {
        pairs.sort_by(|a, b| {
            a.0.unwrap_or(f64::INFINITY)
                .total_cmp(&b.0.unwrap_or(f64::INFINITY))
        });
        let h = Histogram {
            bounds: pairs.iter().map(|p| p.0).collect(),
            counts: pairs.iter().map(|p| p.1).collect(),
        };
        if !h.valid() {
            return Err("inconsistent histogram");
        }
        let (base, labels) = name.split_once('|').unwrap_or((&name, ""));
        let count_key = if labels.is_empty() {
            format!("{base}_count")
        } else {
            format!("{base}_count|{labels}")
        };
        if result
            .values
            .counters
            .get(&count_key)
            .is_none_or(|count| h.counts.last() != Some(count))
        {
            return Err("histogram count differs from buckets");
        }
        result.values.histograms.insert(name, h);
    }
    if result.values.counters.is_empty() && result.values.gauges.is_empty() {
        return Err("no supported telemetry");
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn real_serving_metrics_are_accepted_without_unbounded_labels() {
        for (name, text) in [
            (
                "worker",
                include_str!("../../fixtures/quality/transparent-pir-recent-01.txt"),
            ),
            (
                "publisher",
                include_str!("../../fixtures/quality/publisher.txt"),
            ),
            (
                "ingress",
                include_str!("../../fixtures/quality/enhance_query.txt"),
            ),
            (
                "packing",
                include_str!("../../fixtures/quality/enhance-pir-packing-01.txt"),
            ),
        ] {
            let parsed = parse(text, 1790669000).unwrap_or_else(|e| panic!("{name}: {e}"));
            assert!(!serde_json::to_string(&parsed.values)
                .unwrap()
                .contains("assignment_sha256"));
        }
    }
    #[test]
    fn rejects_count_mismatch_and_keeps_incompatible_aggregate_unknown() {
        assert!(parse("pir_http_duration_seconds_bucket{le=\"1\"} 1\npir_http_duration_seconds_bucket{le=\"+Inf\"} 2\npir_http_duration_seconds_count 3",100).is_err());
        let mut a = Point::default();
        a.values.histograms.insert(
            "latency".into(),
            Histogram {
                bounds: vec![Some(1.), None],
                counts: vec![1., 1.],
            },
        );
        let mut b = a.clone();
        b.values.histograms.get_mut("latency").unwrap().bounds[0] = Some(2.);
        let mut merged = Point::default();
        merged.merge(&a);
        merged.merge(&b);
        merged.merge(&a);
        assert!(!merged.values.histograms.contains_key("latency"));
        assert!(merged.invalid_histograms.contains("latency"));
    }
    #[test]
    fn resets_gaps_and_missing_incarnations_never_become_zero_errors() {
        let mut a = parse(
            "process_start_time_seconds 1\nenhance_http_arrivals_total{endpoint=\"query\"} 10",
            100,
        )
        .unwrap();
        let mut b = a.clone();
        b.at = 105;
        b.values.counters.values_mut().for_each(|v| *v += 5.);
        assert_eq!(b.delta(Some(&a)).observed_seconds, 5);
        b.incarnation = Some("2".into());
        assert_eq!(b.delta(Some(&a)).observed_seconds, 0);
        b.incarnation = a.incarnation.clone();
        b.at = 200;
        assert_eq!(b.delta(Some(&a)).observed_seconds, 0);
        a.incarnation = None;
        assert_eq!(a.delta(Some(&a)).observed_seconds, 0);
    }
    #[test]
    fn quantiles_merge_buckets_and_preserve_overflow() {
        let mut h = Histogram {
            bounds: vec![Some(1.), Some(5.), None],
            counts: vec![8., 9., 10.],
        };
        assert!(h.quantile(0.99).is_none());
        assert!(h.merge(&Histogram {
            bounds: h.bounds.clone(),
            counts: vec![90., 90., 90.]
        }));
        assert!(h.quantile(0.95).unwrap() < 1.);
        assert!(!Histogram {
            bounds: vec![Some(1.), None],
            counts: vec![3., 2.]
        }
        .valid());
    }
}
