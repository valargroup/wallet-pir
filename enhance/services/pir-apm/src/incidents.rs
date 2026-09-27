use anyhow::{Context, Result};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use std::{
    path::Path,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

pub fn unix_time() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Condition {
    #[serde(default)]
    pub retired: bool,
    pub key: String,
    pub resource: String,
    pub severity: String,
    /// None means unknown, never a recovery.
    pub firing: Option<bool>,
    pub observed: String,
    pub threshold: String,
    pub hold_seconds: u64,
    /// Timestamp of the observation, not the evaluator tick.
    pub sample: u64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Incident {
    pub id: String,
    pub active: bool,
    pub since: Option<u64>,
    pub healthy_samples: u8,
    pub last_sample: u64,
    pub last_evaluated: u64,
    pub last_reminder: u64,
    pub condition: Option<Condition>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct DeliveryHealth {
    pub configured: bool,
    pub pending: u64,
    pub oldest_pending_age: u64,
    pub last_success: Option<u64>,
    pub failure: Option<String>,
    pub worker_at: u64,
}

const RUNBOOK: &str =
    "https://github.com/valargroup/wallet-pir/blob/main/enhance/docs/observability-alerting.md";

/// Escape the three characters Slack mrkdwn treats as control sequences.
fn escape(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

fn duration(seconds: u64) -> String {
    match seconds {
        s if s < 60 => format!("{s}s"),
        s if s < 3600 => format!("{}m {}s", s / 60, s % 60),
        s => format!("{}h {}m", s / 3600, s % 3600 / 60),
    }
}

/// Context line with a Slack date token (viewer's time zone, UTC fallback) and links.
fn footer(id: &str, now: u64, dashboard: &str) -> String {
    let utc = chrono::DateTime::from_timestamp(now as i64, 0)
        .map(|t| t.format("%Y-%m-%d %H:%M:%S UTC").to_string())
        .unwrap_or_else(|| now.to_string());
    let mut links = format!("<{RUNBOOK}|Runbook>");
    if !dashboard.is_empty() {
        links = format!("<{dashboard}|Dashboard> · {links}");
    }
    format!(
        "`{}` · <!date^{now}^{{date_short_pretty}} {{time_secs}}|{utc}> · {links}",
        escape(id)
    )
}

/// Slack mrkdwn body for one incident transition; stored verbatim in the outbox.
fn slack_message(
    event: &str,
    c: &Condition,
    id: &str,
    now: u64,
    shadow: bool,
    environment: &str,
    dashboard: &str,
) -> String {
    let icon = match (event, c.severity.as_str()) {
        ("RECOVERED", _) => ":large_green_circle:",
        ("RETIRED", _) => ":white_circle:",
        ("REMINDER", _) => ":bell:",
        (_, "critical") => ":red_circle:",
        _ => ":large_orange_circle:",
    };
    // Incident ids end in their firing time.
    let open_for = id
        .rsplit_once('-')
        .and_then(|(_, t)| t.parse::<u64>().ok())
        .filter(|_| event != "FIRED")
        .map(|fired| format!(" after {}", duration(now.saturating_sub(fired))))
        .unwrap_or_default();
    format!(
        "{icon} *{}{event}{open_for} · {}* · `{}` on `{}` · {}\n>*Observed:* {}\n>*Threshold:* {}\n{}",
        if shadow { "SHADOW " } else { "" },
        escape(&c.severity),
        escape(&c.key),
        escape(&c.resource),
        escape(environment),
        escape(&c.observed),
        escape(&c.threshold),
        footer(id, now, dashboard),
    )
}

pub struct Store {
    db: Connection,
}

impl Store {
    pub fn open(path: &Path) -> Result<Self> {
        let db = Connection::open(path).context("opening incident database")?;
        db.busy_timeout(Duration::from_secs(2))?;
        db.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;
            CREATE TABLE IF NOT EXISTS incidents (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS outbox (
                id INTEGER PRIMARY KEY, incident TEXT NOT NULL, body TEXT NOT NULL,
                created INTEGER NOT NULL, due INTEGER NOT NULL, attempts INTEGER NOT NULL DEFAULT 0);
            CREATE TABLE IF NOT EXISTS events (id INTEGER PRIMARY KEY, at INTEGER NOT NULL, shadow INTEGER NOT NULL, body TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);")?;
        Ok(Self { db })
    }

    /// Commit incident transitions and their delivery records in one transaction.
    /// Unknown observations preserve active incidents and reset pending dwell/recovery.
    pub fn evaluate(
        &mut self,
        conditions: &[Condition],
        now: u64,
        shadow: bool,
        environment: &str,
        dashboard: &str,
    ) -> Result<()> {
        let tx = self.db.transaction()?;
        let previous_mode: Option<String> = tx
            .query_row("SELECT value FROM metadata WHERE key='mode'", [], |r| {
                r.get(0)
            })
            .optional()?;
        let promoting = !shadow && previous_mode.as_deref() == Some("shadow");
        for c in conditions {
            let value: Option<String> = tx
                .query_row("SELECT value FROM incidents WHERE key=?", [&c.key], |r| {
                    r.get(0)
                })
                .optional()?;
            let mut i: Incident = value
                .map(|s| serde_json::from_str(&s))
                .transpose()?
                .unwrap_or_default();
            let previous_sample = i.last_sample;
            if now < i.last_evaluated || now.saturating_sub(i.last_evaluated) > 60 {
                i.since = None;
                i.healthy_samples = 0;
            }
            i.last_evaluated = now;
            i.condition = Some(c.clone());
            let fresh = c.sample > previous_sample;
            i.last_sample = i.last_sample.max(c.sample);
            // Promotion announces existing active state without clearing an incident
            // whose current input is unknown or whose recovery is not yet confirmed.
            let mut event = (promoting && i.active && !c.retired).then_some("FIRED");
            if c.retired {
                if i.active {
                    event = Some("RETIRED");
                }
                i.active = false;
                i.since = None;
                i.healthy_samples = 0;
            } else {
                match c.firing {
                    None => {
                        i.since = None;
                        i.healthy_samples = 0;
                    }
                    Some(true) => {
                        i.healthy_samples = 0;
                        let since = *i.since.get_or_insert(now);
                        if !i.active && fresh && now.saturating_sub(since) >= c.hold_seconds {
                            i.active = true;
                            i.id = format!("{}-{now}", c.key);
                            i.last_reminder = now;
                            event = Some("FIRED");
                        } else if event.is_none()
                            && i.active
                            && c.severity == "critical"
                            && now.saturating_sub(i.last_reminder) >= 1800
                        {
                            i.last_reminder = now;
                            event = Some("REMINDER");
                        }
                    }
                    Some(false) => {
                        i.since = None;
                        if fresh {
                            i.healthy_samples = i.healthy_samples.saturating_add(1);
                        }
                        if i.active && i.healthy_samples >= 2 {
                            i.active = false;
                            event = Some("RECOVERED");
                        }
                    }
                }
            }
            if let Some(event) = event {
                let body = slack_message(event, c, &i.id, now, shadow, environment, dashboard);
                tx.execute("INSERT INTO metadata(key,value) VALUES ('last_event',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", [&body])?;
                tx.execute(
                    "INSERT INTO events(at,shadow,body) VALUES (?,?,?)",
                    params![now, shadow, body],
                )?;
                if !shadow {
                    tx.execute(
                        "INSERT INTO outbox(incident,body,created,due) VALUES (?,?,?,?)",
                        params![i.id, body, now, now],
                    )?;
                }
            }
            tx.execute("INSERT INTO incidents(key,value) VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", params![c.key,serde_json::to_string(&i)?])?;
        }
        tx.execute("INSERT INTO metadata(key,value) VALUES ('mode',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",[if shadow {"shadow"} else {"active"}])?;
        tx.commit()?;
        Ok(())
    }

    /// Explicit operator verification uses the real outbox without changing rule state.
    pub fn enqueue_test(&mut self, now: u64, environment: &str, dashboard: &str) -> Result<()> {
        let tx = self.db.transaction()?;
        let id = format!("operator-test-{now}");
        for event in ["FIRED", "RECOVERED"] {
            let body = format!(
                ":test_tube: *PIR test · {event}* · {}\nNo service failure: durable delivery verification.\n{}",
                escape(environment),
                footer(&id, now, dashboard)
            );
            tx.execute(
                "INSERT INTO outbox(incident,body,created,due) VALUES (?,?,?,?)",
                params![id, body, now, now],
            )?;
            tx.execute(
                "INSERT INTO events(at,shadow,body) VALUES (?,0,?)",
                params![now, body],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    pub fn incidents(&self) -> Result<Vec<Incident>> {
        let mut stmt = self
            .db
            .prepare("SELECT value FROM incidents ORDER BY key")?;
        let values = stmt
            .query_map([], |r| r.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        values
            .into_iter()
            .map(|s| serde_json::from_str(&s).map_err(Into::into))
            .collect()
    }

    pub fn health(&self, configured: bool, now: u64) -> Result<DeliveryHealth> {
        let (pending, oldest): (u64, Option<u64>) =
            self.db
                .query_row("SELECT COUNT(*),MIN(created) FROM outbox", [], |r| {
                    Ok((r.get(0)?, r.get(1)?))
                })?;
        let get = |key: &str| -> Result<Option<String>> {
            Ok(self
                .db
                .query_row("SELECT value FROM metadata WHERE key=?", [key], |r| {
                    r.get(0)
                })
                .optional()?)
        };
        Ok(DeliveryHealth {
            configured,
            pending,
            oldest_pending_age: oldest.map(|t| now.saturating_sub(t)).unwrap_or(0),
            last_success: get("last_success")?.and_then(|s| s.parse().ok()),
            failure: get("failure")?.filter(|s| !s.is_empty()),
            worker_at: get("worker_at")?.and_then(|s| s.parse().ok()).unwrap_or(0),
        })
    }

    pub fn metadata(&self, key: &str, value: &str) -> Result<()> {
        self.db.execute("INSERT INTO metadata(key,value) VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", [key,value])?;
        Ok(())
    }

    fn next(&self, now: u64) -> Result<Option<(i64, String, u32)>> {
        Ok(self
            .db
            .query_row(
                "SELECT id,body,attempts FROM outbox q WHERE due<=? AND NOT EXISTS
            (SELECT 1 FROM outbox p WHERE p.incident=q.incident AND p.id<q.id) ORDER BY id LIMIT 1",
                [now],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .optional()?)
    }

    fn delivered(&mut self, id: i64, now: u64) -> Result<()> {
        let tx = self.db.transaction()?;
        tx.execute("DELETE FROM outbox WHERE id=?", [id])?;
        for (key, value) in [
            ("last_success", now.to_string()),
            ("failure", String::new()),
        ] {
            tx.execute("INSERT INTO metadata(key,value) VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", [key,&value])?;
        }
        tx.commit()?;
        Ok(())
    }

    fn retry(&self, id: i64, now: u64, delay: u64, reason: &str) -> Result<()> {
        self.db.execute(
            "UPDATE outbox SET attempts=attempts+1,due=? WHERE id=?",
            params![now.saturating_add(delay), id],
        )?;
        self.metadata("failure", reason)
    }
}

/// The caller supervises this task. Errors contain no request URL or credentials.
pub async fn deliver(path: std::path::PathBuf, webhook: Option<String>) -> Result<()> {
    let mut store = Store::open(&path)?;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .redirect(reqwest::redirect::Policy::none())
        .build()?;
    loop {
        let now = unix_time();
        store.metadata("worker_at", &now.to_string())?;
        if let Some(webhook) = &webhook {
            if let Some((id, body, attempts)) = store.next(now)? {
                let result = client
                    .post(webhook)
                    .json(&serde_json::json!({"text":body}))
                    .send()
                    .await;
                let backoff = (2u64.saturating_pow(attempts.min(8))
                    + u64::from(rand::random::<u8>() % 5))
                .min(300);
                match result {
                    Ok(response) if response.status().is_success() => {
                        store.delivered(id, unix_time())?
                    }
                    Ok(response) => {
                        let status = response.status();
                        let retry_after = response
                            .headers()
                            .get("retry-after")
                            .and_then(|v| v.to_str().ok())
                            .and_then(|v| v.parse::<u64>().ok());
                        let delay = if status.is_client_error() && status.as_u16() != 429 {
                            300
                        } else {
                            backoff
                        };
                        store.retry(
                            id,
                            unix_time(),
                            retry_after.unwrap_or(delay).max(1),
                            &format!("Slack HTTP {}", status.as_u16()),
                        )?;
                    }
                    Err(_) => store.retry(
                        id,
                        unix_time(),
                        backoff,
                        "Slack request failed or timed out",
                    )?,
                }
            }
        } else {
            store.metadata("failure", "Slack webhook missing")?;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn condition(firing: Option<bool>, sample: u64) -> Condition {
        Condition {
            retired: false,
            key: "query_5xx".into(),
            resource: "query".into(),
            severity: "critical".into(),
            firing,
            observed: "10/20".into(),
            threshold: ">5%".into(),
            hold_seconds: 10,
            sample,
        }
    }
    #[test]
    fn slack_message_is_escaped_mrkdwn() {
        let mut c = condition(Some(true), 100);
        c.observed = "p99 <5s & >2s".into();
        let fired = slack_message(
            "FIRED",
            &c,
            "query_5xx-100",
            100,
            false,
            "production",
            "https://x/apm/",
        );
        assert!(fired.starts_with(
            ":red_circle: *FIRED · critical* · `query_5xx` on `query` · production\n"
        ));
        assert!(fired.contains(">*Observed:* p99 &lt;5s &amp; &gt;2s\n"));
        assert!(fired.contains("<!date^100^"));
        assert!(fired.ends_with("<https://x/apm/|Dashboard> · <https://github.com/valargroup/wallet-pir/blob/main/enhance/docs/observability-alerting.md|Runbook>"));
        let recovered = slack_message(
            "RECOVERED",
            &c,
            "query_5xx-100",
            231,
            true,
            "production",
            "",
        );
        assert!(recovered
            .starts_with(":large_green_circle: *SHADOW RECOVERED after 2m 11s · critical*"));
        assert!(!recovered.contains("Dashboard"));
    }
    #[test]
    fn unknown_cannot_recover_and_recovery_needs_distinct_samples() {
        let mut s = Store::open(Path::new(":memory:")).unwrap();
        for t in [100, 110] {
            s.evaluate(
                &[condition(Some(true), t)],
                t,
                false,
                "test",
                "https://example.test/apm",
            )
            .unwrap();
        }
        assert_eq!(s.health(true, 110).unwrap().pending, 1);
        s.evaluate(&[condition(None, 120)], 120, false, "test", "")
            .unwrap();
        for _ in 0..3 {
            s.evaluate(&[condition(Some(false), 130)], 130, false, "test", "")
                .unwrap();
        }
        assert!(s.incidents().unwrap()[0].active);
        s.evaluate(&[condition(Some(false), 140)], 140, false, "test", "")
            .unwrap();
        assert_eq!(s.health(true, 140).unwrap().pending, 2);
        assert!(!s.incidents().unwrap()[0].active);
        let first = s.next(140).unwrap().unwrap().0;
        s.retry(first, 140, 300, "timeout").unwrap();
        assert!(s.next(141).unwrap().is_none());
        s.delivered(first, 150).unwrap();
        assert!(s.next(150).unwrap().is_some());
    }
    #[test]
    fn promotion_preserves_active_unknown_and_requires_confirmed_recovery() {
        let mut s = Store::open(Path::new(":memory:")).unwrap();
        for t in [100, 110] {
            s.evaluate(&[condition(Some(true), t)], t, true, "test", "")
                .unwrap();
        }
        let id = s.incidents().unwrap()[0].id.clone();
        s.evaluate(&[condition(None, 120)], 120, false, "test", "")
            .unwrap();
        assert!(s.incidents().unwrap()[0].active);
        assert_eq!(s.incidents().unwrap()[0].id, id);
        assert_eq!(s.health(true, 120).unwrap().pending, 1);
        s.evaluate(&[condition(Some(false), 130)], 130, false, "test", "")
            .unwrap();
        assert!(s.incidents().unwrap()[0].active);
        s.evaluate(&[condition(Some(false), 140)], 140, false, "test", "")
            .unwrap();
        assert!(!s.incidents().unwrap()[0].active);
        assert_eq!(s.health(true, 140).unwrap().pending, 2);
    }
    #[test]
    fn restart_preserves_outbox_and_does_not_refire() {
        let path =
            std::env::temp_dir().join(format!("pir-alert-test-{}.sqlite", rand::random::<u64>()));
        {
            let mut s = Store::open(&path).unwrap();
            for t in [100, 110] {
                s.evaluate(&[condition(Some(true), t)], t, false, "test", "")
                    .unwrap();
            }
        }
        {
            let mut s = Store::open(&path).unwrap();
            s.evaluate(&[condition(Some(true), 120)], 120, false, "test", "")
                .unwrap();
            assert_eq!(s.health(true, 120).unwrap().pending, 1);
        }
        std::fs::remove_file(path).unwrap();
    }
}

#[cfg(test)]
mod delivery_tests {
    use super::*;
    use axum::{http::StatusCode, routing::post, Router};
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    #[tokio::test]
    async fn failed_delivery_retries_while_evaluation_and_recovery_continue() {
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = calls.clone();
        let app = Router::new().route(
            "/",
            post(move || {
                let c = observed.clone();
                async move {
                    if c.fetch_add(1, Ordering::SeqCst) == 0 {
                        (StatusCode::TOO_MANY_REQUESTS, [("retry-after", "1")])
                    } else {
                        (StatusCode::OK, [("retry-after", "1")])
                    }
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let path = std::env::temp_dir().join(format!(
            "pir-delivery-test-{}.sqlite",
            rand::random::<u64>()
        ));
        let mut s = Store::open(&path).unwrap();
        let now = unix_time();
        let mut c = Condition {
            retired: false,
            key: "test".into(),
            resource: "test".into(),
            severity: "critical".into(),
            firing: Some(true),
            observed: "test".into(),
            threshold: "test".into(),
            hold_seconds: 0,
            sample: now,
        };
        s.evaluate(&[c.clone()], now, false, "test", "").unwrap();
        let worker = tokio::spawn(deliver(path.clone(), Some(format!("http://{addr}/"))));
        c.firing = Some(false);
        for t in [now + 1, now + 2] {
            c.sample = t;
            s.evaluate(&[c.clone()], t, false, "test", "").unwrap();
        }
        tokio::time::timeout(Duration::from_secs(12), async {
            loop {
                if s.health(true, unix_time()).unwrap().pending == 0 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        worker.abort();
        server.abort();
        drop(s);
        std::fs::remove_file(path).unwrap();
    }
    #[test]
    fn promotion_enqueues_existing_shadow_incident_once_and_retirement_is_explicit() {
        let mut s = Store::open(Path::new(":memory:")).unwrap();
        let mut c = Condition {
            retired: false,
            key: "domain_1".into(),
            resource: "domain_1".into(),
            severity: "critical".into(),
            firing: Some(true),
            observed: "none".into(),
            threshold: "one".into(),
            hold_seconds: 0,
            sample: 100,
        };
        s.evaluate(&[c.clone()], 100, true, "test", "").unwrap();
        assert_eq!(s.health(true, 100).unwrap().pending, 0);
        c.sample = 110;
        s.evaluate(&[c.clone()], 110, false, "test", "").unwrap();
        c.sample = 120;
        s.evaluate(&[c.clone()], 120, false, "test", "").unwrap();
        assert_eq!(s.health(true, 120).unwrap().pending, 1);
        c.retired = true;
        c.sample = 130;
        s.evaluate(&[c], 130, false, "test", "").unwrap();
        assert!(!s.incidents().unwrap()[0].active);
        assert_eq!(s.health(true, 130).unwrap().pending, 2);
    }
}
