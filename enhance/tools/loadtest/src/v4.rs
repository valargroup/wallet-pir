//! Bounded v4 load driver with exact-answer checking and expiry refresh.
use crate::Args;
use anyhow::{ensure, Context};
use enhance_pir::{client::ClientError, v4_client::EnhancePirClient, RECORD_BYTES};
use hdrhistogram::Histogram;
use rand::{rngs::StdRng, Rng, SeedableRng};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

#[derive(Clone, Deserialize)]
struct OracleRecord {
    position: u64,
    record_hex: String,
}
#[derive(Serialize)]
struct Report {
    protocol: &'static str,
    seconds: f64,
    concurrency: usize,
    offered_qps: Option<f64>,
    completed: u64,
    succeeded: u64,
    incorrect_answers: u64,
    errors: BTreeMap<String, u64>,
    unstarted_arrivals: u64,
    correct_queries_per_second: f64,
    p50_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
    scheduled_p99_ms: f64,
    exact_answer_oracle: bool,
    warmup_errors: BTreeMap<String, u64>,
    warmup_correct_answers: u64,
    warmup_incorrect_answers: u64,
}

struct Samples {
    latency: Histogram<u64>,
    scheduled: Histogram<u64>,
    correct: u64,
    wrong: u64,
    errors: BTreeMap<String, u64>,
    completed: u64,
}

#[derive(Default)]
struct WarmupSamples {
    errors: BTreeMap<String, u64>,
    correct: u64,
    wrong: u64,
}

impl WarmupSamples {
    // Return whether the caller should back off. Protocol failures are never
    // converted into transient overload; incorrect answers remain fatal at report validation.
    fn observe(
        &mut self,
        result: Result<&[u8], ClientError>,
        expected: &[u8],
    ) -> Result<bool, ClientError> {
        match result {
            Ok(record) => {
                if record == expected {
                    self.correct += 1;
                } else {
                    self.wrong += 1;
                }
                Ok(false)
            }
            Err(error @ (ClientError::HttpStatus(429 | 503) | ClientError::Http(_))) => {
                let class = match error {
                    ClientError::HttpStatus(status) => format!("http_{status}"),
                    _ => "transport".into(),
                };
                *self.errors.entry(class).or_default() += 1;
                Ok(true)
            }
            Err(error) => Err(error),
        }
    }
}

fn check_answers(measured_wrong: u64, warmup_wrong: u64) -> anyhow::Result<()> {
    ensure!(
        measured_wrong == 0 && warmup_wrong == 0,
        "incorrect PIR answers"
    );
    Ok(())
}

pub async fn run(args: Args) -> anyhow::Result<()> {
    ensure!(
        args.parallelism > 0 && !args.duration.is_zero(),
        "positive concurrency and duration required"
    );
    ensure!(
        args.rate.is_none_or(|r| r.is_finite() && r > 0.0),
        "offered rate must be positive and finite"
    );
    ensure!(
        args.max_error_rate.is_finite() && (0.0..=1.0).contains(&args.max_error_rate),
        "invalid error rate"
    );
    ensure!(
        args.slo_p99_ms.is_none_or(|s| s.is_finite() && s > 0.0),
        "invalid latency SLO"
    );
    ensure!(
        args.fixture_oracle || args.oracle.is_some(),
        "v4 qualification requires --fixture-oracle or --oracle"
    );
    let mut oracle = BTreeMap::new();
    if let Some(path) = &args.oracle {
        let entries: Vec<OracleRecord> = serde_json::from_slice(&std::fs::read(path)?)?;
        for entry in entries {
            let bytes = decode_hex(&entry.record_hex)?;
            ensure!(
                bytes.len() == RECORD_BYTES && oracle.insert(entry.position, bytes).is_none(),
                "invalid or duplicate oracle record"
            );
        }
        ensure!(!oracle.is_empty(), "oracle is empty");
    }
    let oracle = Arc::new(oracle);
    let positions: Arc<Vec<u64>> = Arc::new(oracle.keys().copied().collect());
    let mut clients = Vec::new();
    for _ in 0..args.parallelism {
        clients.push(EnhancePirClient::connect(&args.server).await?);
    }
    // Warm every generator independently before starting the measurement clock.
    let mut warmup = tokio::task::JoinSet::new();
    for mut client in clients {
        let duration = args.warmup;
        let position = positions.first().copied().unwrap_or(0);
        let expected = if args.fixture_oracle {
            let mut bytes = vec![0; RECORD_BYTES];
            bytes[..8].copy_from_slice(&position.to_le_bytes());
            bytes
        } else {
            oracle[&position].clone()
        };
        warmup.spawn(async move {
            let mut samples = WarmupSamples::default();
            let until = Instant::now() + duration;
            loop {
                let result = client.query_position_with_timing(position).await;
                let backoff = match result {
                    Ok((record, _)) => samples.observe(Ok(record.as_ref()), &expected)?,
                    Err(error) => samples.observe(Err(error), &expected)?,
                };
                if backoff {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                if Instant::now() >= until {
                    break;
                }
            }
            Ok::<_, ClientError>((client, samples.errors, samples.correct, samples.wrong))
        });
    }
    let mut clients = Vec::new();
    let mut warmup_errors = BTreeMap::new();
    let (mut warmup_correct_answers, mut warmup_incorrect_answers) = (0, 0);
    while let Some(client) = warmup.join_next().await {
        let (client, errors, correct, wrong) = client??;
        clients.push(client);
        warmup_correct_answers += correct;
        warmup_incorrect_answers += wrong;
        for (class, count) in errors {
            *warmup_errors.entry(class).or_default() += count;
        }
    }
    let start = Instant::now();
    let end = start + args.duration;
    let next = Arc::new(AtomicU64::new(0));
    let started = Arc::new(AtomicU64::new(0));
    let offered = args
        .rate
        .map(|rate| (rate * args.duration.as_secs_f64()).floor() as u64);
    let mut tasks = tokio::task::JoinSet::new();
    for (index, mut client) in clients.into_iter().enumerate() {
        let oracle = oracle.clone();
        let positions = positions.clone();
        let next = next.clone();
        let started = started.clone();
        let rate = args.rate;
        let fixture = args.fixture_oracle;
        let seed = args.seed.unwrap_or(0) + index as u64;
        tasks.spawn(async move {
            let mut rng = StdRng::seed_from_u64(seed);
            let mut sample = Samples {
                latency: Histogram::new(3).unwrap(),
                scheduled: Histogram::new(3).unwrap(),
                correct: 0,
                wrong: 0,
                errors: BTreeMap::new(),
                completed: 0,
            };
            while Instant::now() < end {
                let scheduled = if let Some(rate) = rate {
                    let index = next.fetch_add(1, Ordering::Relaxed);
                    if index >= offered.unwrap() {
                        break;
                    }
                    let at = start + Duration::from_secs_f64(index as f64 / rate);
                    tokio::time::sleep_until(tokio::time::Instant::from_std(at)).await;
                    at
                } else {
                    Instant::now()
                };
                if Instant::now() >= end {
                    break;
                }
                started.fetch_add(1, Ordering::Relaxed);
                let position = if fixture {
                    rng.gen_range(0..client.manifest().coverage.records)
                } else {
                    positions[rng.gen_range(0..positions.len())]
                };
                let began = Instant::now();
                let result = client.query_position_with_timing(position).await;
                sample.completed += 1;
                sample
                    .latency
                    .record(began.elapsed().as_micros().max(1) as u64)
                    .unwrap();
                sample
                    .scheduled
                    .record(scheduled.elapsed().as_micros().max(1) as u64)
                    .unwrap();
                match result {
                    Ok((record, _)) => {
                        let expected = if fixture {
                            let mut r = vec![0; RECORD_BYTES];
                            r[..8].copy_from_slice(&position.to_le_bytes());
                            r
                        } else {
                            oracle[&position].clone()
                        };
                        if record.as_ref() == expected {
                            sample.correct += 1;
                        } else {
                            sample.wrong += 1;
                        }
                    }
                    Err(error) => {
                        if matches!(error, ClientError::HttpStatus(429 | 503)) {
                            tokio::time::sleep(Duration::from_millis(50)).await;
                        }
                        let class = match error {
                            ClientError::HttpStatus(s) => format!("http_{s}"),
                            ClientError::Http(_) => "transport".into(),
                            ClientError::OutsideCoverage(_) => "coverage".into(),
                            ClientError::Response(_) => "invalid_response".into(),
                            _ => "client".into(),
                        };
                        *sample.errors.entry(class).or_default() += 1;
                    }
                }
            }
            sample
        });
    }
    let mut total = Samples {
        latency: Histogram::new(3)?,
        scheduled: Histogram::new(3)?,
        correct: 0,
        wrong: 0,
        errors: BTreeMap::new(),
        completed: 0,
    };
    while let Some(sample) = tasks.join_next().await {
        let sample = sample?;
        total.latency.add(sample.latency)?;
        total.scheduled.add(sample.scheduled)?;
        total.correct += sample.correct;
        total.wrong += sample.wrong;
        total.completed += sample.completed;
        for (class, n) in sample.errors {
            *total.errors.entry(class).or_default() += n;
        }
    }
    let seconds = start.elapsed().as_secs_f64();
    let report = Report {
        protocol: enhance_pir::v4::PROTOCOL_REVISION,
        seconds,
        concurrency: args.parallelism,
        offered_qps: args.rate,
        completed: total.completed,
        succeeded: total.correct,
        incorrect_answers: total.wrong,
        errors: total.errors,
        unstarted_arrivals: offered
            .unwrap_or(0)
            .saturating_sub(started.load(Ordering::Relaxed)),
        correct_queries_per_second: total.correct as f64 / seconds,
        p50_ms: total.latency.value_at_quantile(0.5) as f64 / 1000.,
        p95_ms: total.latency.value_at_quantile(0.95) as f64 / 1000.,
        p99_ms: total.latency.value_at_quantile(0.99) as f64 / 1000.,
        scheduled_p99_ms: total.scheduled.value_at_quantile(0.99) as f64 / 1000.,
        exact_answer_oracle: true,
        warmup_errors,
        warmup_correct_answers,
        warmup_incorrect_answers,
    };
    let json = serde_json::to_string_pretty(&report)?;
    println!("{json}");
    if let Some(path) = args.json_out {
        std::fs::write(path, format!("{json}\n"))?;
    }
    ensure!(report.completed > 0, "no measured queries completed");
    check_answers(report.incorrect_answers, report.warmup_incorrect_answers)?;
    let failed = report.errors.values().sum::<u64>() + report.unstarted_arrivals;
    ensure!(
        failed as f64 / (report.completed + report.unstarted_arrivals) as f64
            <= args.max_error_rate,
        "error/unsent-arrival threshold exceeded"
    );
    if let Some(slo) = args.slo_p99_ms {
        let latency = if args.rate.is_some() {
            report.scheduled_p99_ms
        } else {
            report.p99_ms
        };
        ensure!(latency <= slo, "end-to-end p99 {latency}ms exceeds {slo}ms");
    }
    Ok(())
}

fn decode_hex(text: &str) -> anyhow::Result<Vec<u8>> {
    ensure!(
        text.len().is_multiple_of(2) && text.is_ascii(),
        "invalid oracle hex"
    );
    text.as_bytes()
        .chunks_exact(2)
        .map(|chunk| {
            u8::from_str_radix(std::str::from_utf8(chunk)?, 16).context("invalid oracle hex")
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn warmup_preserves_transient_failures_and_exact_answer_failures() {
        let mut samples = WarmupSamples::default();
        // Invalid URL constructs a real reqwest transport error without relying
        // on an external host or a race to close a local TCP listener.
        let transport = match EnhancePirClient::connect("not a URL").await {
            Ok(_) => panic!("invalid URL unexpectedly connected"),
            Err(error) => error,
        };
        assert!(matches!(transport, ClientError::Http(_)));
        assert!(samples.observe(Err(transport), b"expected").unwrap());
        for status in [429, 503, 429] {
            assert!(samples
                .observe(Err(ClientError::HttpStatus(status)), b"expected")
                .unwrap());
        }
        assert!(!samples.observe(Ok(b"expected"), b"expected").unwrap());
        assert!(!samples.observe(Ok(b"corrupt"), b"expected").unwrap());
        assert_eq!(
            samples.errors,
            [
                ("transport".into(), 1),
                ("http_429".into(), 2),
                ("http_503".into(), 1)
            ]
            .into()
        );
        assert_eq!((samples.correct, samples.wrong), (1, 1));
        assert!(check_answers(0, samples.wrong).is_err());
        assert!(check_answers(1, 0).is_err());
        assert!(check_answers(0, 0).is_ok());
    }

    #[test]
    fn warmup_does_not_hide_protocol_or_authorization_failures() {
        let mut samples = WarmupSamples::default();
        for error in [
            ClientError::HttpStatus(401),
            ClientError::HttpStatus(500),
            ClientError::Response("malformed".into()),
            ClientError::Generation("incompatible".into()),
            ClientError::OutsideCoverage(10),
        ] {
            assert!(samples.observe(Err(error), b"expected").is_err());
        }
        assert!(samples.errors.is_empty());
        assert_eq!((samples.correct, samples.wrong), (0, 0));
    }
}
