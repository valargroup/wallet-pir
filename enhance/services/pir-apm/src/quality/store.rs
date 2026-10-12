use super::capacity::{Daily, Sample, DAY};
use super::data::Point;
use anyhow::Result;
use rusqlite::{params, Connection, OptionalExtension};
use std::{collections::BTreeMap, path::Path, time::Duration};

pub const RETENTION: u64 = 7 * 24 * 3600;
/// Daily memory rollups outlive minute history so forecasts see months of growth.
pub const CAPACITY_RETENTION: u64 = 180 * DAY;
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
            CREATE INDEX IF NOT EXISTS quality_event_retention ON quality_events(at);
            CREATE TABLE IF NOT EXISTS capacity_daily(host TEXT NOT NULL,unit TEXT NOT NULL,day INTEGER NOT NULL,at INTEGER NOT NULL,size TEXT,memory_total REAL,min_available REAL,unit_peak REAL,unit_max REAL,cache_peak REAL,PRIMARY KEY(host,unit,day));")?;
        Ok(Self { db })
    }
    /// Folds one host or unit observation into its UTC day: the lowest available
    /// memory, the highest unit use (anonymous memory) and cache residency, and
    /// the latest size and limits.
    pub fn record_capacity(&mut self, s: &Sample) -> Result<()> {
        self.db.execute(
            "INSERT INTO capacity_daily VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT(host,unit,day) DO UPDATE SET
             at=MAX(at,excluded.at),
             size=COALESCE(excluded.size,size),
             memory_total=COALESCE(excluded.memory_total,memory_total),
             min_available=MIN(COALESCE(min_available,excluded.min_available),COALESCE(excluded.min_available,min_available)),
             unit_peak=MAX(COALESCE(unit_peak,excluded.unit_peak),COALESCE(excluded.unit_peak,unit_peak)),
             unit_max=COALESCE(excluded.unit_max,unit_max),
             cache_peak=MAX(COALESCE(cache_peak,excluded.cache_peak),COALESCE(excluded.cache_peak,cache_peak))",
            params![
                s.host,
                s.unit,
                s.at / DAY,
                s.at,
                s.size,
                s.memory_total,
                s.memory_available,
                if s.unit.is_empty() { None } else { s.used() },
                s.unit_max,
                s.cache_resident
            ],
        )?;
        Ok(())
    }
    /// Daily rows from `since_day` on, ordered by host, unit and day.
    pub fn capacity(&self, since_day: u64) -> Result<Vec<Daily>> {
        let mut stmt = self.db.prepare(
            "SELECT host,unit,day,at,size,memory_total,min_available,unit_peak,unit_max,cache_peak
             FROM capacity_daily WHERE day>=? ORDER BY host,unit,day",
        )?;
        let rows = stmt
            .query_map([since_day], |r| {
                Ok(Daily {
                    host: r.get(0)?,
                    unit: r.get(1)?,
                    day: r.get(2)?,
                    at: r.get(3)?,
                    size: r.get(4)?,
                    memory_total: r.get(5)?,
                    min_available: r.get(6)?,
                    unit_peak: r.get(7)?,
                    unit_max: r.get(8)?,
                    cache_peak: r.get(9)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
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
        self.db.execute(
            "DELETE FROM capacity_daily WHERE day < ?",
            [now.saturating_sub(CAPACITY_RETENTION) / DAY],
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
    #[test]
    fn capacity_rollup_keeps_daily_extremes_for_180_days() {
        let path = std::env::temp_dir().join(format!(
            "pir-capacity-{}-{}.sqlite",
            std::process::id(),
            pir_apm::incidents::unix_time()
        ));
        let mut store = Store::open(&path).unwrap();
        let sample = |at, available| Sample {
            host: "h".into(),
            unit: String::new(),
            at,
            size: Some("s-4vcpu-8gb".into()),
            memory_total: Some(100.),
            memory_available: Some(available),
            ..Default::default()
        };
        // A unit's daily peak is the day's highest anonymous memory, never its
        // current use (page cache) or its recorded lifetime peak.
        let unit = |at, anon: Option<f64>| Sample {
            host: "h".into(),
            unit: "worker.service".into(),
            at,
            unit_current: Some(90.),
            unit_lifetime_peak: Some(95.),
            unit_anon: anon,
            ..Default::default()
        };
        store.record_capacity(&sample(DAY + 10, 60.)).unwrap();
        store.record_capacity(&sample(DAY + 20, 40.)).unwrap();
        store.record_capacity(&sample(DAY + 30, 50.)).unwrap();
        store.record_capacity(&sample(2 * DAY + 5, 70.)).unwrap();
        for (at, anon) in [
            (DAY + 10, 5.),
            (DAY + 20, 12.),
            (DAY + 30, 3.),
            (2 * DAY + 5, 4.),
        ] {
            store.record_capacity(&unit(at, Some(anon))).unwrap();
        }
        let rows = store.capacity(0).unwrap();
        assert_eq!(rows.len(), 4);
        assert_eq!(rows[0].min_available, Some(40.));
        assert_eq!(rows[0].unit_peak, None);
        assert_eq!(rows[0].at, DAY + 30);
        assert_eq!(rows[1].min_available, Some(70.));
        assert_eq!(
            (rows[2].unit.as_str(), rows[2].unit_peak),
            ("worker.service", Some(12.))
        );
        assert_eq!(rows[3].unit_peak, Some(4.));
        // Without anonymous memory the current use stands in.
        store.record_capacity(&unit(2 * DAY + 6, None)).unwrap();
        assert_eq!(store.capacity(0).unwrap()[3].unit_peak, Some(90.));
        store.prune(CAPACITY_RETENTION + DAY + 10).unwrap();
        assert_eq!(store.capacity(0).unwrap().len(), 4);
        store.prune(CAPACITY_RETENTION + 2 * DAY + 10).unwrap();
        assert_eq!(store.capacity(0).unwrap().len(), 2);
        drop(store);
        let _ = std::fs::remove_file(path);
    }
}
