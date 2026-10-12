//! Transparent RAM capacity: daily memory peaks per host and per systemd unit,
//! straight-line forecasts of when each runs out, the next DigitalOcean size,
//! and the `transparent_capacity` alert family.
//!
//! The host sampler reports, per source, the host's available and total memory
//! and each unit's current, peak and maximum memory. Every observation is folded
//! into a daily row (kept 180 days). A least-squares line through the daily peaks
//! of the last 14 and 30 days says how many days remain until a host's available
//! memory falls below 20% of its total, or a unit's peak passes 90% of its
//! MemoryMax. Fewer than three days of rows is unknown, not safe.
use super::rules::Policy;
use crate::dashboard::SharedDashboard;
use axum::{extract::State, response::Html};
use pir_apm::incidents::{unix_time, Condition};
use serde::Serialize;
use serde_json::Value;
use std::collections::{BTreeMap, VecDeque};

pub const DAY: u64 = 86400;
pub const KEY_PREFIX: &str = "transparent_capacity_";
/// DigitalOcean sizes in upgrade order with their monthly USD price.
pub const LADDER: [(&str, u32); 7] = [
    ("s-2vcpu-4gb", 24),
    ("s-4vcpu-8gb", 48),
    ("s-8vcpu-16gb", 96),
    ("m-4vcpu-32gb", 168),
    ("m-8vcpu-64gb", 336),
    ("m-16vcpu-128gb", 672),
    ("m-32vcpu-256gb", 1344),
];

fn tier(slug: &str) -> Option<usize> {
    let base = slug.strip_suffix("-intel").unwrap_or(slug);
    LADDER.iter().position(|(s, _)| *s == base)
}
/// The size after `slug` on the ladder; an `-intel` size maps to the same next tier.
pub fn next_size(slug: &str) -> Option<(&'static str, u32)> {
    LADDER.get(tier(slug)? + 1).copied()
}

fn valid_id(s: &str) -> bool {
    (1..=80).contains(&s.len())
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'@'))
}
/// The stable host name of a snapshot entry, else the source it is filed under.
pub fn host_id<'a>(entry: &'a Value, source: &'a str) -> &'a str {
    entry["host_id"]
        .as_str()
        .filter(|h| valid_id(h))
        .unwrap_or(source)
}

/// One host (`unit` empty) or unit observation.
#[derive(Clone, Debug, Default)]
pub struct Sample {
    pub host: String,
    pub unit: String,
    pub at: u64,
    pub size: Option<String>,
    pub memory_total: Option<f64>,
    pub memory_available: Option<f64>,
    pub unit_current: Option<f64>,
    /// The larger of the unit's recorded peak and its current use.
    pub unit_peak: Option<f64>,
    pub unit_max: Option<f64>,
    pub cache_resident: Option<f64>,
}
impl Sample {
    fn used(&self) -> Option<f64> {
        if self.unit.is_empty() {
            Some(self.memory_total? - self.memory_available?)
        } else {
            self.unit_current
        }
    }
}

/// Fresh host and unit observations from the host snapshot. Absent fields are
/// unknown; malformed names are skipped.
pub fn samples(
    hosts: &BTreeMap<String, Value>,
    cache: impl Fn(&str) -> Option<f64>,
    now: u64,
) -> Vec<Sample> {
    let number = |v: &Value| v.as_f64().filter(|n| n.is_finite() && *n >= 0.);
    let mut out = Vec::new();
    for (name, entry) in hosts {
        let Some(at) = entry["at"]
            .as_u64()
            .filter(|at| *at <= now && now - at <= 45)
        else {
            continue;
        };
        if !valid_id(name) {
            continue;
        }
        let host = host_id(entry, name).to_owned();
        let size = entry["size"]
            .as_str()
            .filter(|s| valid_id(s))
            .map(String::from);
        out.push(Sample {
            host: host.clone(),
            at,
            size: size.clone(),
            memory_total: number(&entry["memory_total_bytes"]),
            memory_available: number(&entry["memory_available_bytes"]),
            cache_resident: cache(name),
            ..Default::default()
        });
        for (unit, u) in entry["units"].as_object().into_iter().flatten().take(16) {
            if !valid_id(unit) {
                continue;
            }
            let current = number(&u["memory_current_bytes"]);
            let peak = number(&u["memory_peak_bytes"]);
            out.push(Sample {
                host: host.clone(),
                unit: unit.clone(),
                at,
                size: size.clone(),
                unit_current: current,
                unit_peak: match (peak, current) {
                    (Some(a), Some(b)) => Some(a.max(b)),
                    (a, b) => a.or(b),
                },
                unit_max: number(&u["memory_max_bytes"]),
                ..Default::default()
            });
        }
    }
    out
}

/// One UTC day of a host or unit, as stored.
#[derive(Clone, Debug, Default)]
pub struct Daily {
    pub host: String,
    pub unit: String,
    pub day: u64,
    pub at: u64,
    pub size: Option<String>,
    pub memory_total: Option<f64>,
    pub min_available: Option<f64>,
    pub unit_peak: Option<f64>,
    pub unit_max: Option<f64>,
    pub cache_peak: Option<f64>,
}

/// Per-minute peak use over the last 24 hours and the latest observation, in
/// memory only: a restart shortens the 24-hour peak until a day has passed.
#[derive(Default)]
pub struct Recent {
    peaks: BTreeMap<(String, String), VecDeque<(u64, f64)>>,
    live: BTreeMap<(String, String), Sample>,
}
impl Recent {
    pub fn observe(&mut self, s: &Sample) {
        let key = (s.host.clone(), s.unit.clone());
        if let Some(used) = s.used() {
            let minute = s.at / 60;
            let q = self.peaks.entry(key.clone()).or_default();
            match q.back_mut() {
                Some((m, v)) if *m == minute => *v = v.max(used),
                _ => q.push_back((minute, used)),
            }
            while q.front().is_some_and(|(m, _)| m + 1440 <= minute) {
                q.pop_front();
            }
        }
        self.live.insert(key, s.clone());
    }
    fn peak(&self, key: &(String, String), now: u64) -> Option<f64> {
        self.peaks
            .get(key)?
            .iter()
            .filter(|(m, _)| m + 1440 > now / 60)
            .map(|(_, v)| *v)
            .reduce(f64::max)
    }
}

/// A forecast is `known` with at least three days of rows in its window;
/// `days` is then None when use is flat or falling.
#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct Forecast {
    pub known: bool,
    pub days: Option<f64>,
    pub growth_per_week: Option<f64>,
}

/// Least-squares (intercept, slope); None without two distinct x values.
pub fn fit(points: &[(f64, f64)]) -> Option<(f64, f64)> {
    if points.len() < 2 {
        return None;
    }
    let n = points.len() as f64;
    let mean_x = points.iter().map(|p| p.0).sum::<f64>() / n;
    let mean_y = points.iter().map(|p| p.1).sum::<f64>() / n;
    let sxx = points.iter().map(|p| (p.0 - mean_x).powi(2)).sum::<f64>();
    if sxx <= 0. {
        return None;
    }
    let slope = points
        .iter()
        .map(|p| (p.0 - mean_x) * (p.1 - mean_y))
        .sum::<f64>()
        / sxx;
    Some((mean_y - slope * mean_x, slope))
}

/// Days from today until the trend of daily peaks `(day, bytes)` within the last
/// `window` days reaches `threshold` bytes; 0 once the latest peak is there.
pub fn forecast(points: &[(u64, f64)], threshold: f64, now: u64, window: u64) -> Forecast {
    let today = now / DAY;
    let points: Vec<(f64, f64)> = points
        .iter()
        .filter(|(day, y)| *day <= today && day + window > today && y.is_finite())
        .map(|(day, y)| (*day as f64, *y))
        .collect();
    let Some((intercept, slope)) = fit(&points).filter(|_| points.len() >= 3) else {
        return Forecast::default();
    };
    if !(threshold.is_finite() && threshold > 0.) {
        return Forecast::default();
    }
    let days = if points.last().is_some_and(|p| p.1 >= threshold) {
        Some(0.)
    } else if slope <= 0. {
        None
    } else {
        Some(((threshold - intercept) / slope - today as f64).max(0.))
    };
    Forecast {
        known: true,
        days,
        growth_per_week: Some(slope * 7.),
    }
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct Row {
    pub host: String,
    pub unit: Option<String>,
    pub size: Option<String>,
    pub memory_total: Option<f64>,
    pub unit_max: Option<f64>,
    /// Latest daily unit peak (recorded peak or current, whichever is larger).
    pub unit_peak: Option<f64>,
    pub cache_peak: Option<f64>,
    /// Highest use in the last 24 hours: host memory in use, or unit memory.
    pub peak_24h: Option<f64>,
    /// Current use and when it was observed.
    pub current: Option<f64>,
    pub live_at: u64,
    /// Bytes in use at which the row reaches its floor.
    pub threshold: Option<f64>,
    pub sample: u64,
    pub forecast_14: Forecast,
    pub forecast_30: Forecast,
    pub reprovision: bool,
}
impl Row {
    /// None while both forecasts are unknown; Some(None) when neither is growing;
    /// otherwise the sooner of the two.
    pub fn days(&self) -> Option<Option<f64>> {
        let known: Vec<_> = [&self.forecast_14, &self.forecast_30]
            .into_iter()
            .filter(|f| f.known)
            .collect();
        (!known.is_empty()).then(|| {
            known
                .iter()
                .filter_map(|f| f.days)
                .min_by(|a, b| a.total_cmp(b))
        })
    }
    fn id(&self) -> String {
        match &self.unit {
            Some(unit) => format!("{}_{unit}", self.host),
            None => self.host.clone(),
        }
    }
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct View {
    pub computed_at: u64,
    pub shadow: bool,
    pub error: bool,
    pub rows: Vec<Row>,
}

/// Rows for every host and unit with stored days, merged with live observations.
pub fn build(daily: &[Daily], recent: &Recent, now: u64, p: &Policy) -> View {
    let mut groups: BTreeMap<(String, String), Vec<&Daily>> = BTreeMap::new();
    for d in daily {
        groups
            .entry((d.host.clone(), d.unit.clone()))
            .or_default()
            .push(d);
    }
    let mut rows = Vec::new();
    for (key, days) in groups {
        let Some(last) = days.last() else {
            continue;
        };
        let live = recent.live.get(&key);
        let host_row = key.1.is_empty();
        let (threshold, points): (Option<f64>, Vec<(u64, f64)>) = if host_row {
            let total = live.and_then(|l| l.memory_total).or(last.memory_total);
            (
                total.map(|t| (1. - p.host_floor_ratio) * t),
                days.iter()
                    .filter_map(|d| Some((d.day, d.memory_total? - d.min_available?)))
                    .collect(),
            )
        } else {
            let max = live.and_then(|l| l.unit_max).or(last.unit_max);
            (
                max.map(|m| p.unit_forecast_ratio * m),
                days.iter()
                    .filter_map(|d| Some((d.day, d.unit_peak?)))
                    .collect(),
            )
        };
        let window = |n| {
            threshold
                .map(|t| forecast(&points, t, now, n))
                .unwrap_or_default()
        };
        let mut row = Row {
            host: key.0.clone(),
            unit: (!host_row).then(|| key.1.clone()),
            size: live
                .and_then(|l| l.size.clone())
                .or_else(|| days.iter().rev().find_map(|d| d.size.clone())),
            memory_total: if host_row {
                live.and_then(|l| l.memory_total).or(last.memory_total)
            } else {
                None
            },
            unit_max: live.and_then(|l| l.unit_max).or(last.unit_max),
            unit_peak: last.unit_peak,
            cache_peak: last.cache_peak,
            peak_24h: recent.peak(&key, now),
            current: live.and_then(Sample::used),
            live_at: live.map(|l| l.at).unwrap_or(0),
            threshold,
            sample: last.at.max(live.map(|l| l.at).unwrap_or(0)),
            forecast_14: window(14),
            forecast_30: window(30),
            reprovision: false,
        };
        row.reprovision = row
            .days()
            .flatten()
            .is_some_and(|d| d < p.capacity_warning_days);
        rows.push(row);
    }
    View {
        computed_at: now,
        rows,
        ..Default::default()
    }
}

fn days_text(days: Option<Option<f64>>) -> String {
    match days {
        None => "unknown (fewer than three days of data)".into(),
        Some(None) => "not growing".into(),
        Some(Some(d)) => format!("{d:.0} days"),
    }
}

pub fn conditions(view: &View, now: u64, p: &Policy) -> Vec<Condition> {
    let mut out = Vec::new();
    let current =
        !view.error && view.computed_at > 0 && now.saturating_sub(view.computed_at) <= 900;
    for row in &view.rows {
        let id = row.id();
        let (what, floor) = match &row.unit {
            Some(_) => (
                "unit",
                format!(
                    "peak passes {:.0}% of MemoryMax",
                    p.unit_forecast_ratio * 100.
                ),
            ),
            None => (
                "floor",
                format!(
                    "available memory falls below {:.0}% of total",
                    p.host_floor_ratio * 100.
                ),
            ),
        };
        let days = row
            .days()
            .filter(|_| current && now.saturating_sub(row.sample) <= 900);
        for (level, limit) in [
            ("warning", p.capacity_warning_days),
            ("critical", p.capacity_critical_days),
        ] {
            out.push(Condition {
                retired: false,
                key: format!("{KEY_PREFIX}{what}_{level}_{id}"),
                resource: row.host.clone(),
                severity: level.into(),
                firing: days.map(|d| d.is_some_and(|d| d < limit)),
                observed: format!("{} until {floor}", days_text(days)),
                threshold: format!("fewer than {limit} days until {floor} (14- or 30-day trend)"),
                hold_seconds: 1800,
                sample: row.sample,
            });
        }
        if row.unit.is_some() {
            let ratio = row
                .current
                .zip(row.unit_max)
                .filter(|(_, max)| *max > 0.)
                .map(|(c, max)| c / max)
                .filter(|_| row.live_at > 0 && now.saturating_sub(row.live_at) <= 45);
            out.push(Condition {
                retired: false,
                key: format!("{KEY_PREFIX}unit_memory_{id}"),
                resource: row.host.clone(),
                severity: "warning".into(),
                firing: ratio.map(|r| r > p.unit_memory_ratio),
                observed: ratio
                    .map(|r| format!("{:.1}% of MemoryMax", r * 100.))
                    .unwrap_or_else(|| "unavailable".into()),
                threshold: format!(
                    "unit memory >{:.0}% of MemoryMax for {}s",
                    p.unit_memory_ratio * 100.,
                    p.unit_memory_hold_seconds
                ),
                hold_seconds: p.unit_memory_hold_seconds,
                sample: row.live_at,
            });
        }
    }
    out
}

fn gib(v: Option<f64>) -> String {
    v.map(|b| format!("{:.1} GiB", b / 1073741824.))
        .unwrap_or_else(|| "—".into())
}
fn percent(a: Option<f64>, b: Option<f64>) -> String {
    a.zip(b)
        .filter(|(_, b)| *b > 0.)
        .map(|(a, b)| format!(" ({:.0}%)", a / b * 100.))
        .unwrap_or_default()
}
fn forecast_cell(f: &Forecast) -> String {
    match (f.known, f.days) {
        (false, _) => "unknown".into(),
        (true, None) => "not growing".into(),
        (true, Some(d)) => format!("{d:.0}"),
    }
}

pub fn render(view: &super::View) -> String {
    let escape = crate::dashboard::escape;
    let c = &view.capacity;
    let now = unix_time();
    let mut out = format!("<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\"><meta http-equiv=\"refresh\" content=\"60\"><title>Transparent capacity · PIR APM</title><style>{} main{{max-width:1280px;margin:auto;padding:24px}} a{{color:var(--gold)}} .quality-nav{{display:flex;gap:20px;flex-wrap:wrap;margin:24px 0}} th{{text-align:left}} td,th{{padding:8px;white-space:nowrap}} .table-wrap{{overflow-x:auto}} h1{{font-size:36px}}</style></head><body><main>{}<h1>Transparent RAM capacity</h1><p>Daily memory peaks per host and per service unit, kept 180 days. Days to floor follow a straight-line fit of the daily peaks over the last 14 and 30 days; fewer than three days is unknown. A host's floor is 20% of its memory available; a unit's is 90% of its MemoryMax. New capacity alerts: <strong>{}</strong>.</p>",crate::dashboard::STYLE,super::view::nav(view),if c.shadow {"shadow"} else {"active"});
    if c.error {
        out.push_str(
            "<p class=\"warn\">Capacity history is unavailable. Rows below may be stale.</p>",
        );
    }
    if c.rows.is_empty() {
        out.push_str("<p>No host memory observations yet. The host sampler writes them to the configured host snapshot.</p>");
    }
    out.push_str("<div class=\"table-wrap\"><table><thead><tr><th>Host / unit</th><th>Size</th><th>RAM / MemoryMax</th><th>24 h peak use</th><th>Unit peak vs MemoryMax</th><th>Headroom to floor</th><th>Growth / week</th><th>Days to floor (14 d)</th><th>Days to floor (30 d)</th><th>Next size</th><th>Action</th></tr></thead><tbody>");
    for row in &c.rows {
        let limit = if row.unit.is_some() {
            row.unit_max
        } else {
            row.memory_total
        };
        let growth = row
            .forecast_14
            .growth_per_week
            .or(row.forecast_30.growth_per_week);
        let next = row
            .unit
            .is_none()
            .then(|| row.size.as_deref().and_then(next_size))
            .flatten()
            .map(|(slug, usd)| format!("{slug} (${usd}/month)"))
            .unwrap_or_else(|| "—".into());
        let stale = now.saturating_sub(row.sample) > 900;
        out.push_str(&format!(
            "<tr><th>{}{}</th><td>{}</td><td>{}</td><td>{}{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td class=\"{}\">{}</td></tr>",
            escape(&row.host),
            row.unit
                .as_ref()
                .map(|u| format!(" · {}", escape(u)))
                .unwrap_or_default(),
            row.size.as_deref().map(escape).unwrap_or_else(|| "—".into()),
            gib(limit),
            gib(row.peak_24h),
            percent(row.peak_24h, limit),
            if row.unit.is_some() {
                format!("{}{}", gib(row.unit_peak), percent(row.unit_peak, row.unit_max))
            } else {
                "—".into()
            },
            gib(row.threshold.zip(row.peak_24h).map(|(t, p)| t - p)),
            growth
                .map(|g| format!("{:+.2} GiB", g / 1073741824.))
                .unwrap_or_else(|| "—".into()),
            forecast_cell(&row.forecast_14),
            forecast_cell(&row.forecast_30),
            next,
            if row.reprovision { "warn" } else { "" },
            if stale {
                "stale"
            } else if row.reprovision {
                "reprovision"
            } else {
                ""
            }
        ));
    }
    out.push_str("</tbody></table></div><p>Peak use is memory in use (total minus available) for a host and the unit's own memory for a unit. Headroom is how much more can be used before the floor, measured from the 24-hour peak. Next size follows the DigitalOcean ladder s-2vcpu-4gb → s-4vcpu-8gb → s-8vcpu-16gb → m-4vcpu-32gb → m-8vcpu-64gb → m-16vcpu-128gb → m-32vcpu-256gb; Intel variants move to the same next tier. A row is marked reprovision when either forecast reaches the floor within 30 days.</p></main></body></html>");
    out
}

pub async fn page(State(state): State<SharedDashboard>) -> Html<String> {
    Html(render(&state.read().await.quality))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn daily(day: u64, used: f64) -> Daily {
        Daily {
            host: "h".into(),
            day,
            at: day * DAY + 3500,
            memory_total: Some(100.),
            min_available: Some(100. - used),
            size: Some("s-4vcpu-8gb-intel".into()),
            ..Default::default()
        }
    }

    #[test]
    fn forecast_math() {
        let now = 100 * DAY + 3600;
        // Flat: known, never reaches the floor.
        let flat: Vec<_> = (90..=100).map(|d| (d, 50.)).collect();
        let f = forecast(&flat, 80., now, 14);
        assert!(f.known);
        assert_eq!(f.days, None);
        assert_eq!(f.growth_per_week, Some(0.));
        // Rising 1 per day from 50 today: 30 days to 80.
        let rising: Vec<_> = (90..=100).map(|d| (d, 50. + (d as f64 - 100.))).collect();
        let f = forecast(&rising, 80., now, 14);
        assert!((f.days.unwrap() - 30.).abs() < 1e-9);
        assert!((f.growth_per_week.unwrap() - 7.).abs() < 1e-9);
        // Noisy rise: the fit stays near the trend.
        let noisy: Vec<_> = (87..=100)
            .map(|d| {
                (
                    d,
                    50. + (d as f64 - 100.) + if d % 2 == 0 { 2. } else { -2. },
                )
            })
            .collect();
        let days = forecast(&noisy, 80., now, 14).days.unwrap();
        assert!((25. ..35.).contains(&days), "{days}");
        // Already past the floor.
        assert_eq!(
            forecast(&[(98, 70.), (99, 75.), (100, 85.)], 80., now, 14).days,
            Some(0.)
        );
        // Fewer than three days is unknown.
        assert_eq!(
            forecast(&[(99, 50.), (100, 60.)], 80., now, 14),
            Forecast::default()
        );
        // Days outside the window do not count.
        assert!(!forecast(&[(70, 1.), (71, 2.), (100, 3.)], 80., now, 14).known);
        assert!(forecast(&[(80, 1.), (81, 2.), (100, 3.)], 80., now, 30).known);
    }

    #[test]
    fn next_size_ladder() {
        assert_eq!(next_size("s-2vcpu-4gb"), Some(("s-4vcpu-8gb", 48)));
        assert_eq!(next_size("s-8vcpu-16gb"), Some(("m-4vcpu-32gb", 168)));
        assert_eq!(
            next_size("m-8vcpu-64gb-intel"),
            Some(("m-16vcpu-128gb", 672))
        );
        assert_eq!(next_size("m-32vcpu-256gb"), None);
        assert_eq!(next_size("c-2"), None);
    }

    #[test]
    fn snapshot_fields_parse_leniently() {
        let hosts: BTreeMap<String, Value> = serde_json::from_str(
            r#"{
            "transparent-pir-txid-k8-01": {"at": 100, "host_id": "txid-host", "size": "m-8vcpu-64gb-intel",
              "memory_total_bytes": 1000, "memory_available_bytes": 400,
              "units": {"transparent-display.service": {"memory_current_bytes": 300, "memory_peak_bytes": 350, "memory_max_bytes": null, "oom_kills": 0}}},
            "transparent-coordinator": {"at": 100, "memory_total_bytes": 500},
            "stale": {"at": 10, "memory_total_bytes": 1},
            "bad name!": {"at": 100}
        }"#,
        )
        .unwrap();
        let s = samples(
            &hosts,
            |n| (n == "transparent-pir-txid-k8-01").then_some(7.),
            110,
        );
        assert_eq!(s.len(), 3);
        let host = s
            .iter()
            .find(|s| s.host == "txid-host" && s.unit.is_empty())
            .unwrap();
        assert_eq!(host.used(), Some(600.));
        assert_eq!(host.cache_resident, Some(7.));
        let unit = s.iter().find(|s| !s.unit.is_empty()).unwrap();
        assert_eq!(
            (unit.unit_peak, unit.unit_max, unit.unit_current),
            (Some(350.), None, Some(300.))
        );
        let coordinator = s
            .iter()
            .find(|s| s.host == "transparent-coordinator")
            .unwrap();
        assert_eq!(coordinator.used(), None);
    }

    #[test]
    fn capacity_rows_and_alerts() {
        let p = Policy::default();
        let now = 100 * DAY + 3600;
        // 1 unit/day of growth, 75 in use today: floor at 80 in 5 days.
        let mut rows: Vec<_> = (90..=100)
            .map(|d| daily(d, 65. + (d as f64 - 90.)))
            .collect();
        rows.push(Daily {
            host: "h".into(),
            unit: "worker.service".into(),
            day: 100,
            at: now - 10,
            unit_peak: Some(50.),
            unit_max: Some(100.),
            ..Default::default()
        });
        let mut recent = Recent::default();
        recent.observe(&Sample {
            host: "h".into(),
            unit: "worker.service".into(),
            at: now - 5,
            unit_current: Some(90.),
            unit_peak: Some(90.),
            unit_max: Some(100.),
            ..Default::default()
        });
        let view = build(&rows, &recent, now, &p);
        assert_eq!(view.rows.len(), 2);
        let host = &view.rows[0];
        assert!(host.reprovision);
        assert_eq!(host.size.as_deref(), Some("s-4vcpu-8gb-intel"));
        assert!((host.days().unwrap().unwrap() - 5.).abs() < 1e-6);
        let unit = &view.rows[1];
        assert_eq!(unit.days(), None);
        assert_eq!(unit.peak_24h, Some(90.));
        let c = conditions(&view, now, &p);
        let firing = |key: &str| c.iter().find(|c| c.key == key).unwrap().firing;
        assert_eq!(firing("transparent_capacity_floor_warning_h"), Some(true));
        assert_eq!(firing("transparent_capacity_floor_critical_h"), Some(true));
        assert_eq!(
            firing("transparent_capacity_unit_warning_h_worker.service"),
            None
        );
        assert_eq!(
            firing("transparent_capacity_unit_memory_h_worker.service"),
            Some(true)
        );
        // A stale view knows nothing.
        let c = conditions(&view, now + 3600, &p);
        assert!(c.iter().all(|c| c.firing.is_none()));
        // Flat use recovers.
        let flat: Vec<_> = (90..=100).map(|d| daily(d, 30.)).collect();
        let c = conditions(&build(&flat, &Recent::default(), now, &p), now, &p);
        assert_eq!(
            c.iter()
                .find(|c| c.key == "transparent_capacity_floor_warning_h")
                .unwrap()
                .firing,
            Some(false)
        );
    }

    #[test]
    fn page_renders_with_partial_data() {
        let p = Policy::default();
        let now = unix_time();
        let rows = vec![
            Daily {
                host: "router".into(),
                day: now / DAY,
                at: now,
                ..Default::default()
            },
            daily(now / DAY, 40.),
        ];
        let view = super::super::View {
            capacity: build(&rows, &Recent::default(), now, &p),
            ..Default::default()
        };
        let html = render(&view);
        assert!(html.contains("router"));
        assert!(html.contains("s-8vcpu-16gb ($96/month)"));
        assert!(html.contains("unknown"));
        assert!(render(&super::super::View::default()).contains("No host memory observations yet"));
    }
}
