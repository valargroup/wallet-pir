//! One fleet-wide notification window, with a fixed deadline owned by APM.
use crate::incidents::Store;
use anyhow::{ensure, Result};
use rusqlite::{params, OptionalExtension, Transaction};
use serde::{Deserialize, Serialize};

const KEY: &str = "deployment";
pub const MAX_SECONDS: u64 = 3600;
/// A deploy that does not name its own length pauses alerts this long.
pub const DEFAULT_SECONDS: u64 = 600;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Deployment {
    pub id: String,
    pub services: Vec<String>,
    pub started_at: u64,
    pub expires_at: u64,
    pub outcome: Option<String>,
    /// Copied from the APM that owns the window, which alone announces it.
    #[serde(default)]
    pub mirrored: bool,
}

fn read(tx: &Transaction<'_>) -> Result<Option<Deployment>> {
    let value: Option<String> = tx
        .query_row("SELECT value FROM metadata WHERE key=?", [KEY], |r| {
            r.get(0)
        })
        .optional()?;
    value
        .map(|v| serde_json::from_str(&v).map_err(Into::into))
        .transpose()
}

fn save(tx: &Transaction<'_>, deployment: &Deployment) -> Result<()> {
    tx.execute("INSERT INTO metadata(key,value) VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        params![KEY, serde_json::to_string(deployment)?])?;
    Ok(())
}

/// Records the deployment event; only `post` ones reach Slack, so a deploy that
/// passes adds nothing to the alert channel.
fn announce(
    tx: &Transaction<'_>,
    deployment: &Deployment,
    now: u64,
    message: &str,
    post: bool,
) -> Result<()> {
    let services = deployment
        .services
        .iter()
        .map(|v| crate::incidents::escape(v))
        .collect::<Vec<_>>()
        .join(", ");
    let body = format!(
        "{message}: {services}. `{}`",
        crate::incidents::escape(&deployment.id)
    );
    tx.execute(
        "INSERT INTO events(at,shadow,body) VALUES (?,?,?)",
        params![now, !post, body],
    )?;
    if post {
        tx.execute(
            "INSERT INTO outbox(incident,body,created,due) VALUES (?,?,?,?)",
            params![format!("deployment:{}", deployment.id), body, now, now],
        )?;
    }
    Ok(())
}

fn resume(
    tx: &Transaction<'_>,
    deployment: &mut Deployment,
    now: u64,
    outcome: &str,
) -> Result<()> {
    deployment.outcome = Some(outcome.into());
    save(tx, deployment)?;
    tx.execute("INSERT INTO metadata(key,value) VALUES ('deployment_resumed_at',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", [now.to_string()])?;
    Ok(())
}

/// Called by evaluation and delivery, so expiry does not depend on the deploy runner.
pub(crate) fn muted(tx: &Transaction<'_>, now: u64) -> Result<bool> {
    let Some(mut deployment) = read(tx)? else {
        return Ok(false);
    };
    if deployment.outcome.is_some() {
        return Ok(false);
    }
    if now >= deployment.expires_at || now < deployment.started_at {
        resume(tx, &mut deployment, now, "expired")?;
        announce(
            tx,
            &deployment,
            now,
            "⚠️ Deployment window expired; completion unconfirmed; normal alerts enabled",
            !deployment.mirrored,
        )?;
        return Ok(false);
    }
    Ok(true)
}

pub(crate) fn resumed_at(tx: &Transaction<'_>) -> Result<u64> {
    Ok(tx
        .query_row(
            "SELECT value FROM metadata WHERE key='deployment_resumed_at'",
            [],
            |r| r.get::<_, String>(0),
        )
        .optional()?
        .and_then(|v| v.parse().ok())
        .unwrap_or(0))
}

/// Pause alerts: save the window and drop queued notifications. Events remain in
/// history; notifications from before the boundary must not leak into it. An
/// incident whose notification is dropped here is announced after the
/// deployment; one Slack already heard about is not announced again.
fn open_window(tx: &Transaction<'_>, deployment: &Deployment) -> Result<()> {
    save(tx, deployment)?;
    let unsent = {
        let mut statement = tx.prepare(
            "SELECT DISTINCT incident FROM outbox WHERE incident NOT LIKE 'deployment:%'",
        )?;
        let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
        rows.collect::<rusqlite::Result<std::collections::BTreeSet<_>>>()?
    };
    tx.execute(
        "DELETE FROM outbox WHERE incident NOT LIKE 'deployment:%'",
        [],
    )?;
    let incidents = {
        let mut statement = tx.prepare("SELECT key,value FROM incidents")?;
        let rows = statement.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()?
    };
    for (key, value) in incidents {
        let mut incident: crate::incidents::Incident = serde_json::from_str(&value)?;
        if incident.active && (!incident.notified || unsent.contains(&incident.id)) {
            incident.announce_pending = true;
            incident.notified = false;
            tx.execute(
                "UPDATE incidents SET value=? WHERE key=?",
                params![serde_json::to_string(&incident)?, key],
            )?;
        }
    }
    Ok(())
}

impl Store {
    /// Retries never extend the deadline or reopen a completed deployment.
    /// Alerts stay paused for `seconds`, at most [`MAX_SECONDS`].
    pub fn begin_deployment(
        &mut self,
        id: &str,
        services: Vec<String>,
        seconds: u64,
        now: u64,
    ) -> Result<Deployment> {
        let simple = |s: &str| {
            !s.is_empty()
                && s.len() <= 128
                && s.bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"-_.@".contains(&b))
        };
        ensure!(
            simple(id)
                && !services.is_empty()
                && services.len() <= 32
                && services.iter().all(|s| simple(s))
                && (1..=MAX_SECONDS).contains(&seconds),
            "invalid deployment id, service names or length"
        );
        let tx = self
            .db
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let active = muted(&tx, now)?;
        if let Some(previous) = read(&tx)? {
            if previous.id == id {
                ensure!(
                    previous.services == services,
                    "deployment service names changed"
                );
                tx.commit()?;
                return Ok(previous);
            }
            ensure!(!active, "another deployment notification window is active");
        }
        let deployment = Deployment {
            id: id.into(),
            services,
            started_at: now,
            expires_at: now.saturating_add(seconds),
            outcome: None,
            mirrored: false,
        };
        open_window(&tx, &deployment)?;
        announce(
            &tx,
            &deployment,
            now,
            &format!(
                "🔧 Deployment starting; normal alerts paused for up to {} minutes",
                seconds.div_ceil(60)
            ),
            false,
        )?;
        tx.commit()?;
        Ok(deployment)
    }

    /// The window as this store holds it, if any.
    pub fn deployment(&mut self, now: u64) -> Result<Option<Deployment>> {
        let tx = self.db.transaction()?;
        muted(&tx, now)?;
        let deployment = read(&tx)?;
        tx.commit()?;
        Ok(deployment)
    }

    /// Follow the window of the APM that owns deployments, so this store's
    /// alerts pause and resume with it. Nothing is announced here.
    pub fn mirror_deployment(&mut self, owner: Option<&Deployment>, now: u64) -> Result<()> {
        let tx = self
            .db
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let open = muted(&tx, now)?;
        let local = read(&tx)?;
        match owner.filter(|o| o.outcome.is_none() && now < o.expires_at) {
            Some(owner) => {
                if local.as_ref().is_none_or(|l| l.id != owner.id) {
                    let mirrored = Deployment {
                        mirrored: true,
                        ..owner.clone()
                    };
                    open_window(&tx, &mirrored)?;
                }
            }
            None => {
                if let Some(mut local) = local.filter(|l| open && l.mirrored) {
                    let outcome = owner
                        .filter(|o| o.id == local.id)
                        .and_then(|o| o.outcome.clone())
                        .unwrap_or_else(|| "passed".into());
                    resume(&tx, &mut local, now, &outcome)?;
                }
            }
        }
        tx.commit()?;
        Ok(())
    }

    pub fn finish_deployment(&mut self, id: &str, failed: bool, now: u64) -> Result<Deployment> {
        let tx = self
            .db
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        muted(&tx, now)?;
        let mut deployment =
            read(&tx)?.ok_or_else(|| anyhow::anyhow!("no deployment notification window"))?;
        ensure!(
            deployment.id == id,
            "deployment id does not own this notification window"
        );
        if deployment.outcome.is_none() {
            resume(
                &tx,
                &mut deployment,
                now,
                if failed { "failed" } else { "passed" },
            )?;
            announce(
                &tx,
                &deployment,
                now,
                if failed {
                    "🚨 Deployment failed; deployment or validation did not pass; normal alerts enabled"
                } else {
                    "✅ Deployment finished; validation passed; normal alerts enabled"
                },
                failed,
            )?;
        }
        tx.commit()?;
        Ok(deployment)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::incidents::Condition;

    fn store() -> Store {
        Store::open(std::path::Path::new(":memory:")).unwrap()
    }
    fn messages(store: &Store) -> Vec<String> {
        store
            .db
            .prepare("SELECT body FROM outbox ORDER BY id")
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap()
    }
    fn evaluate(store: &mut Store, now: u64, firing: bool) {
        let condition = Condition {
            retired: false,
            key: "unavailable".into(),
            resource: "fleet".into(),
            severity: "critical".into(),
            firing: Some(firing),
            observed: "probe".into(),
            threshold: "must pass".into(),
            hold_seconds: 0,
            window_seconds: 0,
            sample: now,
        };
        store
            .evaluate(&[condition], now, false, "test", "")
            .unwrap();
    }

    #[test]
    fn retry_keeps_deadline_and_a_completed_window_cannot_reopen() {
        let mut s = store();
        let first = s
            .begin_deployment("one", vec!["publisher".into()], MAX_SECONDS, 100)
            .unwrap();
        let retried = s
            .begin_deployment("one", vec!["publisher".into()], MAX_SECONDS, 200)
            .unwrap();
        assert_eq!(first.expires_at, 3700);
        assert_eq!(retried.expires_at, first.expires_at);
        // A deploy that starts or passes adds nothing to the alert channel.
        assert!(messages(&s).is_empty());
        assert!(s
            .begin_deployment("other", vec!["status".into()], MAX_SECONDS, 201)
            .is_err());
        assert!(s.finish_deployment("other", false, 202).is_err());
        s.finish_deployment("one", false, 203).unwrap();
        assert_eq!(
            s.begin_deployment("one", vec!["publisher".into()], MAX_SECONDS, 204)
                .unwrap()
                .outcome
                .as_deref(),
            Some("passed")
        );
        assert!(messages(&s).is_empty());
    }

    #[test]
    fn a_deploy_chooses_how_long_alerts_stay_paused() {
        let mut s = store();
        let window = s
            .begin_deployment("one", vec!["publisher".into()], DEFAULT_SECONDS, 100)
            .unwrap();
        assert_eq!(window.expires_at, 100 + DEFAULT_SECONDS);
        assert!(s
            .begin_deployment("two", vec!["publisher".into()], MAX_SECONDS + 1, 100)
            .is_err());
        evaluate(&mut s, 100 + DEFAULT_SECONDS, true);
        assert!(messages(&s)[0].contains("window expired"));
    }

    #[test]
    fn a_mirrored_window_pauses_and_resumes_without_announcing() {
        let mut owner = store();
        let window = owner
            .begin_deployment("one", vec!["publisher".into()], DEFAULT_SECONDS, 100)
            .unwrap();
        let mut monitor = store();
        monitor.mirror_deployment(Some(&window), 100).unwrap();
        evaluate(&mut monitor, 101, true);
        evaluate(&mut monitor, 102, true);
        assert!(messages(&monitor).is_empty());
        let finished = owner.finish_deployment("one", false, 103).unwrap();
        monitor.mirror_deployment(Some(&finished), 103).unwrap();
        evaluate(&mut monitor, 104, true);
        // Only the incident that began during the pause is announced afterwards.
        assert_eq!(messages(&monitor).len(), 1);
        assert!(messages(&monitor)[0].contains("*FIRED"));
    }

    #[test]
    fn an_incident_slack_already_heard_about_is_not_announced_again() {
        let mut s = store();
        evaluate(&mut s, 99, true);
        // The FIRED notification is delivered before the deployment.
        let (id, body, _) = s.next(99).unwrap().unwrap();
        s.delivered(id, &body, 99).unwrap();
        s.begin_deployment("one", vec!["publisher".into()], MAX_SECONDS, 100)
            .unwrap();
        evaluate(&mut s, 101, true);
        s.finish_deployment("one", false, 102).unwrap();
        evaluate(&mut s, 103, true);
        evaluate(&mut s, 104, true);
        assert!(messages(&s).is_empty());
    }

    #[test]
    fn a_five_minute_rate_waits_until_its_window_starts_after_the_deploy() {
        let mut s = store();
        s.begin_deployment("one", vec!["publisher".into()], MAX_SECONDS, 100)
            .unwrap();
        s.finish_deployment("one", false, 110).unwrap();
        let rate = |sample| Condition {
            retired: false,
            key: "init_5xx".into(),
            resource: "init".into(),
            severity: "critical".into(),
            firing: Some(true),
            observed: "errors during the deploy".into(),
            threshold: "> 5% over 5m".into(),
            hold_seconds: 0,
            window_seconds: 300,
            sample,
        };
        s.evaluate(&[rate(200)], 200, false, "test", "").unwrap();
        s.evaluate(&[rate(410)], 410, false, "test", "").unwrap();
        assert!(messages(&s).is_empty());
        s.evaluate(&[rate(411)], 411, false, "test", "").unwrap();
        assert!(messages(&s)[0].contains("init_5xx"));
    }

    #[test]
    fn recovered_deployment_failures_are_not_replayed() {
        let mut s = store();
        s.begin_deployment("one", vec!["publisher".into()], MAX_SECONDS, 100)
            .unwrap();
        evaluate(&mut s, 101, true);
        evaluate(&mut s, 102, true);
        evaluate(&mut s, 103, false);
        evaluate(&mut s, 104, false);
        assert!(messages(&s).is_empty());
        s.finish_deployment("one", false, 105).unwrap();
        evaluate(&mut s, 106, false);
        assert!(messages(&s).is_empty());
    }

    #[test]
    fn validation_failure_bypasses_suppression_and_failing_incidents_resume_once() {
        let mut s = store();
        s.begin_deployment("one", vec!["publisher".into()], MAX_SECONDS, 100)
            .unwrap();
        evaluate(&mut s, 101, true);
        s.finish_deployment("one", true, 102).unwrap();
        assert!(messages(&s)[0].contains("Deployment failed"));
        evaluate(&mut s, 103, true);
        evaluate(&mut s, 104, true);
        assert_eq!(
            messages(&s).iter().filter(|m| m.contains("*FIRED")).count(),
            1
        );
        evaluate(&mut s, 105, false);
        evaluate(&mut s, 106, false);
        assert!(messages(&s).last().unwrap().contains("RECOVERED"));
    }

    #[test]
    fn expiry_survives_reopening_and_is_announced_once() {
        let path =
            std::env::temp_dir().join(format!("pir-apm-deploy-{}.sqlite", rand::random::<u64>()));
        {
            let mut s = Store::open(&path).unwrap();
            s.begin_deployment("one", vec!["publisher".into()], MAX_SECONDS, 100)
                .unwrap();
            evaluate(&mut s, 101, true);
        }
        let mut s = Store::open(&path).unwrap();
        evaluate(&mut s, 3700, true);
        evaluate(&mut s, 3701, true);
        assert_eq!(
            messages(&s)
                .iter()
                .filter(|m| m.contains("window expired"))
                .count(),
            1
        );
        assert_eq!(
            messages(&s).iter().filter(|m| m.contains("*FIRED")).count(),
            1
        );
        drop(s);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn in_flight_acknowledgement_cannot_remove_the_deployment_announcement() {
        let mut s = store();
        evaluate(&mut s, 99, true);
        let (selected, sent, _) = s.next(99).unwrap().unwrap();
        s.begin_deployment("one", vec!["publisher".into()], MAX_SECONDS, 100)
            .unwrap();
        // The failure announcement may reuse the row of the send selected before
        // the deployment began; that send's outcome must not touch it.
        s.finish_deployment("one", true, 101).unwrap();
        s.retry(selected, &sent, 101, 300, "timeout").unwrap();
        s.delivered(selected, &sent, 101).unwrap();
        assert_eq!(messages(&s).len(), 1);
        assert!(messages(&s)[0].contains("Deployment failed"));
        let (attempts, due): (u64, u64) =
            s.db.query_row("SELECT attempts,due FROM outbox", [], |r| {
                Ok((r.get(0)?, r.get(1)?))
            })
            .unwrap();
        assert_eq!((attempts, due), (0, 101));
    }

    #[test]
    fn queued_notifications_are_removed_but_incident_history_is_preserved() {
        let mut s = store();
        evaluate(&mut s, 99, true);
        s.begin_deployment("one", vec!["publisher".into()], MAX_SECONDS, 100)
            .unwrap();
        // The queued FIRED is dropped, so it is announced after the deployment.
        assert!(messages(&s).is_empty());
        assert!(s.incidents().unwrap()[0].active);
        evaluate(&mut s, 101, true);
        s.finish_deployment("one", false, 102).unwrap();
        evaluate(&mut s, 103, true);
        assert_eq!(
            messages(&s).iter().filter(|m| m.contains("*FIRED")).count(),
            1
        );
    }
}
