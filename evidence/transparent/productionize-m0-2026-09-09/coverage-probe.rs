
// M0 diagnostic only: appended to the isolated baseline, not the wallet checkout.
#[test]
fn m0_measure_checkpoint_writes() {
    for prior in [0u32, 10, 100, 1000] {
        let (mut db, _) = wallet();
        let script = vec![0x51];
        for i in 0..prior {
            db.commit_transparent_shard(&commit(u64::from(i), "m0", true, (i, i), vec![script.clone()])).unwrap();
        }
        let before: u64 = db.connection().query_row("SELECT total_changes()", [], |r| r.get(0)).unwrap();
        let started = std::time::Instant::now();
        db.commit_transparent_shard(&commit(u64::from(prior), "m0", true, (prior, prior), vec![script.clone()])).unwrap();
        let elapsed = started.elapsed().as_nanos();
        let after: u64 = db.connection().query_row("SELECT total_changes()", [], |r| r.get(0)).unwrap();
        let count = db.transparent_coverage_of(&script).unwrap().len();
        assert_eq!(count, prior as usize + 1);
        println!("M0_COVERAGE {{\"prior_checkpoints\":{},\"sql_row_changes\":{},\"elapsed_ns\":{},\"coverage_rows\":{}}}", prior, after-before, elapsed, count);
    }
}
