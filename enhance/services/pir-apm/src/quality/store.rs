use super::data::Point;
use anyhow::Result;
use rusqlite::{params, Connection, OptionalExtension};
use std::{collections::BTreeMap, path::Path, time::Duration};

pub const RETENTION: u64 = 7 * 24 * 3600;
pub struct Store {
    db: Connection,
}
impl Store {
    pub fn open(path: &Path) -> Result<Self> {
        let db = Connection::open(path)?;
        db.busy_timeout(Duration::from_secs(2))?;
        db.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;
            CREATE TABLE IF NOT EXISTS quality_points(service TEXT NOT NULL,source TEXT NOT NULL,minute INTEGER NOT NULL,last_sample INTEGER NOT NULL,value TEXT NOT NULL,PRIMARY KEY(service,source,minute));
            CREATE INDEX IF NOT EXISTS quality_retention ON quality_points(minute);
            CREATE TABLE IF NOT EXISTS quality_events(at INTEGER NOT NULL,service TEXT NOT NULL,source TEXT NOT NULL,kind TEXT NOT NULL);
            CREATE INDEX IF NOT EXISTS quality_event_retention ON quality_events(at);")?;
        Ok(Self { db })
    }
    pub fn record(&mut self, service: &str, source: &str, point: &Point) -> Result<()> {
        let tx = self.db.transaction()?;
        let minute = point.at / 60 * 60;
        let old:Option<(u64,String)>=tx.query_row("SELECT last_sample,value FROM quality_points WHERE service=? AND source=? AND minute=?",params![service,source,minute],|r|Ok((r.get(0)?,r.get(1)?))).optional()?;
        let mut value = if let Some((last, text)) = old {
            if last >= point.at {
                return Ok(());
            }
            serde_json::from_str::<Point>(&text)?
        } else {
            Point {
                at: minute,
                ..Default::default()
            }
        };
        value.merge(point);
        tx.execute("INSERT INTO quality_points VALUES(?,?,?,?,?) ON CONFLICT(service,source,minute) DO UPDATE SET last_sample=excluded.last_sample,value=excluded.value",params![service,source,minute,point.at,serde_json::to_string(&value)?])?;
        if point.discontinuities > 0 {
            tx.execute(
                "INSERT INTO quality_events VALUES(?,?,?,?)",
                params![point.at, service, source, "observation_discontinuity"],
            )?;
        }
        tx.commit()?;
        Ok(())
    }
    pub fn prune(&self, now: u64) -> Result<()> {
        self.db.execute(
            "DELETE FROM quality_points WHERE minute < ?",
            [now.saturating_sub(RETENTION)],
        )?;
        self.db.execute(
            "DELETE FROM quality_events WHERE at < ?",
            [now.saturating_sub(RETENTION)],
        )?;
        self.db.execute_batch("PRAGMA wal_checkpoint(PASSIVE)")?;
        Ok(())
    }
    /// At most 420 points per source; quantiles are computed after bucket merging.
    pub fn history(
        &self,
        service: &str,
        now: u64,
        seconds: u64,
    ) -> Result<BTreeMap<String, Vec<Point>>> {
        let width = match seconds {
            0..=3600 => 60,
            3601..=86400 => 300,
            _ => 1800,
        };
        let mut stmt = self.db.prepare(
            "SELECT source,value FROM quality_points WHERE service=? AND minute>=? ORDER BY minute",
        )?;
        let mut rows = stmt.query(params![service, now.saturating_sub(seconds)])?;
        let mut result: BTreeMap<String, Vec<Point>> = BTreeMap::new();
        while let Some(row) = rows.next()? {
            let source: String = row.get(0)?;
            let point: Point = serde_json::from_str(&row.get::<_, String>(1)?)?;
            let at = point.at / width * width;
            let points = result.entry(source).or_default();
            if points.last().is_none_or(|p| p.at != at) {
                points.push(Point {
                    at,
                    ..Default::default()
                });
            }
            points.last_mut().unwrap().merge(&point);
        }
        Ok(result)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn restart_retention_and_idempotency() {
        let path = std::env::temp_dir().join(format!(
            "pir-quality-{}-{}.sqlite",
            std::process::id(),
            pir_apm::incidents::unix_time()
        ));
        let mut store = Store::open(&path).unwrap();
        let mut p = Point {
            at: 120,
            seconds: 5,
            observed_seconds: 5,
            ..Default::default()
        };
        p.values.counters.insert("requests".into(), 3.);
        store.record("transparent", "worker", &p).unwrap();
        store.record("transparent", "worker", &p).unwrap();
        drop(store);
        let store = Store::open(&path).unwrap();
        let h = store.history("transparent", 130, 3600).unwrap();
        assert_eq!(h["worker"][0].values.counters["requests"], 3.);
        store.prune(RETENTION + 200).unwrap();
        assert!(store
            .history("transparent", RETENTION + 200, RETENTION)
            .unwrap()
            .is_empty());
        drop(store);
        let _ = std::fs::remove_file(path);
    }
}
