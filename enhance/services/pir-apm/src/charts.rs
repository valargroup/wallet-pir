//! Small SVG charts use only bounded aggregate samples, never request data.
use crate::history::Point;
use std::{
    collections::VecDeque,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

fn stamp(at: SystemTime) -> String {
    chrono::DateTime::<chrono::Utc>::from(at)
        .format("%H:%M:%S UTC")
        .to_string()
}
fn svg(
    points: &VecDeque<Point>,
    now: SystemTime,
    series: &[(&str, &str, &str, bool)],
    traffic: bool,
) -> String {
    let start = now
        .checked_sub(Duration::from_secs(3600))
        .unwrap_or(UNIX_EPOCH);
    let value = |p: &Point, stage: &str, p99: bool| -> Option<f64> {
        if traffic {
            p.arrivals
        } else {
            let w = p.latencies.get(stage)?;
            if p99 { w.p99 } else { w.p50 }.map(|s| s * 1000.0)
        }
    };
    let maximum = points
        .iter()
        .flat_map(|p| {
            series
                .iter()
                .filter_map(|(key, _, _, p99)| value(p, key, *p99))
        })
        .filter(|v| v.is_finite())
        .fold(0.0, f64::max)
        .max(1.0);
    let mut out = format!("<svg viewBox=\"0 0 500 130\" role=\"img\" aria-label=\"{} over the past hour\"><text x=\"0\" y=\"12\" fill=\"currentColor\" font-size=\"10\">{maximum:.1} {}</text><path d=\"M42 20V108H492\" fill=\"none\" stroke=\"currentColor\" opacity=\".25\"/><text x=\"0\" y=\"110\" fill=\"currentColor\" font-size=\"10\">0</text><text x=\"42\" y=\"127\" fill=\"currentColor\" font-size=\"10\">−60m</text><text x=\"252\" y=\"127\" fill=\"currentColor\" font-size=\"10\">−30m</text><text x=\"470\" y=\"127\" fill=\"currentColor\" font-size=\"10\">now</text>", if traffic {"Request arrivals"} else {"Latency"}, if traffic {"requests/min"} else {"ms"});
    let mut any = false;
    for (stage, label, color, p99) in series {
        let mut path = String::new();
        let mut dots = String::new();
        let mut previous = None;
        for point in points.iter().filter(|p| p.at >= start && p.at <= now) {
            let Some(n) = value(point, stage, *p99).filter(|n| n.is_finite() && *n >= 0.0) else {
                previous = None;
                continue;
            };
            any = true;
            let x = 42.0
                + point
                    .at
                    .duration_since(start)
                    .unwrap_or_default()
                    .as_secs_f64()
                    / 3600.0
                    * 450.0;
            let y = 108.0 - n / maximum * 84.0;
            let continuous = previous
                .is_some_and(|at| point.at.duration_since(at).unwrap_or_default().as_secs() <= 90);
            path.push_str(&format!(
                "{} {x:.2} {y:.2} ",
                if continuous { "L" } else { "M" }
            ));
            dots.push_str(&format!("<circle cx=\"{x:.2}\" cy=\"{y:.2}\" r=\"2\"><title>{label} · {} · {n:.2} {}</title></circle>",stamp(point.at),if traffic {"requests"}else{"ms"}));
            previous = Some(point.at);
        }
        out.push_str(&format!("<g class=\"series-{}\" stroke=\"{color}\" fill=\"{color}\"><path d=\"{path}\" fill=\"none\" stroke-width=\"1.6\"/>{dots}</g>",if *p99 {"p99"}else{"p50"}));
    }
    if !any {
        out.push_str("<text x=\"165\" y=\"68\" fill=\"currentColor\" font-size=\"12\">Awaiting one-minute samples</text>");
    }
    let visible: Vec<_> = points
        .iter()
        .filter(|p| p.at >= start && p.at <= now)
        .collect();
    let x_at = |p: &Point| {
        42.0 + p.at.duration_since(start).unwrap_or_default().as_secs_f64() / 3600.0 * 450.0
    };
    for (index, point) in visible.iter().enumerate() {
        let x = x_at(point);
        let left = if index == 0 {
            42.0
        } else {
            (x_at(visible[index - 1]) + x) / 2.0
        };
        let right = visible
            .get(index + 1)
            .map(|p| (x_at(p) + x) / 2.0)
            .unwrap_or(492.0);
        let readout = |percentile: bool| {
            series
                .iter()
                .filter(|(_, _, _, p99)| traffic || series.len() == 2 || *p99 == percentile)
                .map(|(stage, label, _, p99)| {
                    let number = value(point, stage, *p99)
                        .filter(|n| n.is_finite() && *n >= 0.0)
                        .map(|n| {
                            if traffic {
                                format!("{n:.0} requests")
                            } else {
                                format!("{n:.2} ms")
                            }
                        })
                        .unwrap_or_else(|| "Unavailable".into());
                    format!("{label}: {number}")
                })
                .collect::<Vec<_>>()
                .join("&#10;")
        };
        out.push_str(&format!("<g class=\"chart-hit\" data-time=\"{}\" data-p50=\"{}\" data-p99=\"{}\"><line x1=\"{x:.2}\" x2=\"{x:.2}\" y1=\"20\" y2=\"108\" stroke=\"currentColor\" stroke-dasharray=\"3 3\"/><rect x=\"{left:.2}\" y=\"20\" width=\"{:.2}\" height=\"88\" fill=\"transparent\" tabindex=\"0\" aria-label=\"{}; {}\"/></g>", stamp(point.at), readout(false), readout(true), right-left, stamp(point.at), readout(true)));
    }
    out.push_str("</svg>");
    out
}

pub fn render(endpoint: &str, points: &VecDeque<Point>, now: SystemTime) -> String {
    let query = endpoint == "query";
    let mut out = format!("<div class=\"mini-charts\"><div class=\"latency-chart {}\" data-chart-id=\"{endpoint}\" data-percentile=\"p99\"><div class=\"chart-heading\"><span>Latency · past hour</span>", if query {"select-percentile"} else {""});
    if query {
        out.push_str("<span class=\"percentile-buttons\"><button type=\"button\" data-percentile-choice=\"p50\">p50</button><button type=\"button\" data-percentile-choice=\"p99\">p99</button></span>");
    }
    out.push_str("</div>");
    let series: Vec<_> = if query {
        vec![
            ("total", "Total p50", "#c6a15b", false),
            ("worker", "Worker p50", "#3a95dc", false),
            ("packing", "Packing p50", "#8fb573", false),
            ("total", "Total p99", "#c6a15b", true),
            ("worker", "Worker p99", "#3a95dc", true),
            ("packing", "Packing p99", "#8fb573", true),
        ]
    } else {
        vec![
            ("total", "p50", "#3a95dc", false),
            ("total", "p99", "#c6a15b", true),
        ]
    };
    out.push_str(&svg(points, now, &series, false));
    out.push_str(if query {"<p class=\"chart-legend\"><span style=\"color:#c6a15b\">Total</span> · <span style=\"color:#3a95dc\">Worker</span> · <span style=\"color:#8fb573\">Packing</span></p>"} else {"<p class=\"chart-legend\"><span style=\"color:#3a95dc\">p50</span> · <span style=\"color:#c6a15b\">p99</span></p>"});
    out.push_str("</div><div><div class=\"chart-heading\">Arriving requests · past hour</div>");
    out.push_str(&svg(
        points,
        now,
        &[("", "Arrivals", "#3a95dc", false)],
        true,
    ));
    out.push_str(
        "<p class=\"chart-legend\">One-minute samples · gaps mean unavailable data</p></div></div>",
    );
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn hover_readout_converts_seconds_and_distinguishes_percentiles() {
        let now = UNIX_EPOCH + Duration::from_secs(7200);
        let points = VecDeque::from([Point {
            at: now,
            latencies: std::collections::BTreeMap::from([(
                "total".into(),
                crate::metrics::LatencyWindow {
                    p50: Some(0.01234),
                    p99: Some(0.05678),
                    ..Default::default()
                },
            )]),
            arrivals: Some(120.0),
        }]);
        let chart = render("query", &points, now);
        assert!(chart.contains("data-p50=\"Total p50: 12.34 ms&#10;Worker p50: Unavailable"));
        assert!(chart.contains("data-p99=\"Total p99: 56.78 ms&#10;Worker p99: Unavailable"));
        assert!(chart.contains("Arrivals: 120 requests"));
        assert!(chart.contains("tabindex=\"0\""));
    }
    #[test]
    fn missing_samples_break_paths_and_query_has_toggle() {
        let now = UNIX_EPOCH + Duration::from_secs(7200);
        let mut points = VecDeque::new();
        for (age, arrivals) in [
            (180, Some(1.0)),
            (120, None),
            (60, Some(2.0)),
            (0, Some(3.0)),
        ] {
            points.push_back(Point {
                at: now - Duration::from_secs(age),
                latencies: Default::default(),
                arrivals,
            });
        }
        let chart = svg(&points, now, &[("", "Arrivals", "blue", false)], true);
        assert!(chart.contains("M 469.50"));
        assert!(chart.contains("M 484.50"));
        assert!(chart.contains("L 492.00"));
        let query = render("query", &VecDeque::new(), now);
        assert!(query.contains("data-percentile=\"p99\""));
        assert!(query.contains("data-percentile-choice=\"p50\""));
        assert!(query.contains("Awaiting one-minute samples"));
    }
}
