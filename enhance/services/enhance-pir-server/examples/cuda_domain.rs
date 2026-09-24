//! Correctness-checked benchmark of wallet-pir's immutable-unit evaluation path.
use enhance_pir::protocol::{Geometry, Lifecycle};
use enhance_pir::RECORD_BYTES;
use enhance_pir_server::{
    matvec::{Backend, MatvecConfig},
    runtime::{plan, Engine},
};
use std::{sync::Arc, time::Instant};
fn records(start: u64, count: usize) -> Result<Vec<u8>, String> {
    let mut bytes = vec![0; count * RECORD_BYTES];
    for (i, row) in bytes.chunks_exact_mut(RECORD_BYTES).enumerate() {
        row[..8].copy_from_slice(&(start + i as u64).to_le_bytes());
    }
    Ok(bytes)
}
fn main() {
    let directory = tempfile::tempdir().unwrap();
    let coverage = Lifecycle::default()
        .coverage(32768 * 33, Geometry::default())
        .unwrap();
    let domain = plan(coverage.shards[0].clone(), records).unwrap();
    assert_eq!(domain.shard.logical_rows, 32768);
    let start = Instant::now();
    let mut cpu = Engine::new(directory.path());
    let cpu_eval = cpu.prepare(domain.clone(), records).unwrap();
    println!("cpu_build_ms={:.3}", start.elapsed().as_secs_f64() * 1000.0);
    let backend = MatvecConfig {
        matvec_backend: Backend::Cuda,
        cuda_device: Some(0),
    };
    backend.validate().unwrap();
    let start = Instant::now();
    let mut gpu = Engine::with_backend(directory.path(), backend);
    let gpu_eval = gpu
        .prepare(domain.clone(), |_, _| panic!("load existing artifacts"))
        .unwrap();
    println!(
        "gpu_reload_prepare_ms={:.3}, units={}, host_database_bytes={}",
        start.elapsed().as_secs_f64() * 1000.0,
        gpu_eval.units.len(),
        gpu.live_bytes()
    );
    let reused = gpu
        .prepare(domain, |_, _| panic!("reuse immutable units"))
        .unwrap();
    for (a, b) in gpu_eval.units.iter().zip(&reused.units) {
        assert!(Arc::ptr_eq(a, b));
    }
    drop(reused);
    let q = 72_057_594_037_641_217;
    let queries: Vec<Vec<u64>> = (0..100)
        .map(|i| {
            (0..32768)
                .map(|r| ((r as u64).wrapping_mul(6364136223846793005).wrapping_add(i)) % q)
                .collect()
        })
        .collect();
    let expected: Vec<_> = queries
        .iter()
        .map(|q| cpu_eval.evaluate(q).unwrap())
        .collect();
    for q in queries.iter().take(10) {
        gpu_eval.evaluate(q).unwrap();
    }
    for repetition in 0..3 {
        let start = Instant::now();
        for (q, expected) in queries.iter().zip(&expected) {
            assert_eq!(&cpu_eval.evaluate(q).unwrap(), expected);
        }
        println!(
            "repeat={repetition}, cpu_ms_per_query={:.3}",
            start.elapsed().as_secs_f64() * 10.0
        );
        for callers in [1, 2] {
            let start = Instant::now();
            std::thread::scope(|scope| {
                for caller in 0..callers {
                    let (gpu_eval, queries, expected) = (&gpu_eval, &queries, &expected);
                    scope.spawn(move || {
                        for i in (caller..100).step_by(callers) {
                            assert_eq!(gpu_eval.evaluate(&queries[i]).unwrap(), expected[i]);
                        }
                    });
                }
            });
            println!(
                "repeat={repetition}, callers={callers}, gpu_ms_per_query={:.3}",
                start.elapsed().as_secs_f64() * 10.0
            );
        }
    }
    drop(cpu_eval);
    drop(gpu_eval);
    gpu.collect_unused().unwrap();
    assert_eq!(gpu.live_bytes(), 0);
    println!("all results matched; runtime references released");
}
