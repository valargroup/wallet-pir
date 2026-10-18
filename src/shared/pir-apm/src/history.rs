//! Bounded, in-memory chart points; a restart intentionally starts a new hour.
use crate::metrics::LatencyWindow;
use std::{
    collections::{BTreeMap, VecDeque},
    time::SystemTime,
};
#[derive(Clone, Debug)]
pub struct Point {
    pub at: SystemTime,
    pub latencies: BTreeMap<String, LatencyWindow>,
    pub arrivals: Option<f64>,
}
pub fn push(history: &mut VecDeque<Point>, point: Point) {
    let now = point.at;
    history.push_back(point);
    while history.len() > 61
        || history
            .front()
            .is_some_and(|p| now.duration_since(p.at).unwrap_or_default().as_secs() >= 3600)
    {
        history.pop_front();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn retains_only_last_hour_and_bounds_dense_samples() {
        let mut history = VecDeque::new();
        for n in 0..120 {
            push(
                &mut history,
                Point {
                    at: SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(n * 60),
                    latencies: BTreeMap::new(),
                    arrivals: Some(n as f64),
                },
            );
        }
        assert_eq!(history.len(), 60);
        assert_eq!(history.front().unwrap().arrivals, Some(60.0));
        let at = history.back().unwrap().at;
        for _ in 0..100 {
            push(
                &mut history,
                Point {
                    at,
                    latencies: BTreeMap::new(),
                    arrivals: None,
                },
            );
        }
        assert_eq!(history.len(), 61);
    }
}
