//! One fleet-wide notification window, with a fixed deadline owned by APM.
use crate::incidents::Store;
use anyhow::{ensure, Result};
use rusqlite::{params, OptionalExtension, Transaction};
use serde::{Deserialize, Serialize};

const KEY: &str = "deployment";
pub const MAX_SECONDS: u64 = 3600;

#[derive(Debug, Serialize, Deserialize)]
pub struct Deployment {
    pub id: String,
    pub services: Vec<String>,
    pub started_at: u64,
    pub expires_at: u64,
    pub outcome: Option<String>,
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

fn announce(tx: &Transaction<'_>, deployment: &Deployment, now: u64, message: &str) -> Result<()> {
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
        "INSERT INTO events(at,shadow,body) VALUES (?,0,?)",
        params![now, body],
    )?;
    tx.execute(
        "INSERT INTO outbox(incident,body,created,due) VALUES (?,?,?,?)",
        params![format!("deployment:{}", deployment.id), body, now, now],
    )?;
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

impl Store {
    /// Retries never extend the deadline or reopen a completed deployment.
    pub fn begin_deployment(
        &mut self,
        id: &str,
        services: Vec<String>,
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
                && services.iter().all(|s| simple(s)),
            "invalid deployment id or service names"
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
            expires_at: now.saturating_add(MAX_SECONDS),
            outcome: None,
        };
        save(&tx, &deployment)?;
        // Events remain in history; notifications from before the boundary must not leak into it.
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
            if incident.active {
                incident.announce_pending = true;
                tx.execute(
                    "UPDATE incidents SET value=? WHERE key=?",
                    params![serde_json::to_string(&incident)?, key],
                )?;
            }
        }
        announce(
            &tx,
            &deployment,
            now,
            "🔧 Deployment starting; normal alerts paused for up to one hour",
        )?;
        tx.commit()?;
        Ok(deployment)
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
            .begin_deployment("one", vec!["publisher".into()], 100)
            .unwrap();
        let retried = s
            .begin_deployment("one", vec!["publisher".into()], 200)
            .unwrap();
        assert_eq!(first.expires_at, 3700);
        assert_eq!(retried.expires_at, first.expires_at);
        assert_eq!(messages(&s).len(), 1);
        assert!(s
            .begin_deployment("other", vec!["status".into()], 201)
            .is_err());
        assert!(s.finish_deployment("other", false, 202).is_err());
        s.finish_deployment("one", false, 203).unwrap();
        assert_eq!(
            s.begin_deployment("one", vec!["publisher".into()], 204)
                .unwrap()
                .outcome
                .as_deref(),
            Some("passed")
        );
        assert_eq!(messages(&s).len(), 2);
    }

    #[test]
    fn recovered_deployment_failures_are_not_replayed() {
        let mut s = store();
        s.begin_deployment("one", vec!["publisher".into()], 100)
            .unwrap();
        evaluate(&mut s, 101, true);
        evaluate(&mut s, 102, true);
        evaluate(&mut s, 103, false);
        evaluate(&mut s, 104, false);
        assert_eq!(messages(&s).len(), 1);
        s.finish_deployment("one", false, 105).unwrap();
        evaluate(&mut s, 106, false);
        assert_eq!(messages(&s).len(), 2);
        assert!(messages(&s)[1].contains("Deployment finished"));
    }

    #[test]
    fn validation_failure_bypasses_suppression_and_failing_incidents_resume_once() {
        let mut s = store();
        s.begin_deployment("one", vec!["publisher".into()], 100)
            .unwrap();
        evaluate(&mut s, 101, true);
        s.finish_deployment("one", true, 102).unwrap();
        assert!(messages(&s)[1].contains("Deployment failed"));
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
            s.begin_deployment("one", vec!["publisher".into()], 100)
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
    fn queued_notifications_are_removed_but_incident_history_is_preserved() {
        let mut s = store();
        evaluate(&mut s, 99, true);
        s.begin_deployment("one", vec!["publisher".into()], 100)
            .unwrap();
        assert_eq!(messages(&s).len(), 1);
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
