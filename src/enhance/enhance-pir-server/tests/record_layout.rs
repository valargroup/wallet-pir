//! The 653-byte Enhance record is the one layout that can never change cheaply:
//! widening it later rebuilds every sealed shard. Pin every offset here, and
//! pin that a record survives the journal and the padded shard read unchanged.

use enhance_pir::types::{
    EnhanceRecord, EnhanceRecordParts, RECORDS_PER_ROW, RECORD_BYTES, ROW_BYTES, SHARD_ROWS,
};
use enhance_pir_server::store::RecordJournal;
use enhance_pir_server::types::{DatabaseId, ENHANCE_LAYOUT};

fn sample(seed: u8) -> EnhanceRecord {
    EnhanceRecord::from_parts(EnhanceRecordParts {
        enc_ciphertext_suffix: [seed.wrapping_add(2); 528],
        cv_net: [seed.wrapping_add(3); 32],
        out_ciphertext: [seed.wrapping_add(4); 80],
        has_transparent_inputs: true,
        has_transparent_outputs: false,
        metadata: enhance_pir::EnhanceTransactionMetadata::new(0, Some(0)).unwrap(),
    })
}

#[test]
fn field_offsets_are_pinned() {
    let record = sample(10);
    let bytes = record.as_bytes();
    assert_eq!(bytes.len(), 653);
    assert_eq!(&bytes[0..528], &[12; 528][..]);
    assert_eq!(&bytes[528..560], &[13; 32]);
    assert_eq!(&bytes[560..640], &[14; 80][..]);
    assert_eq!(bytes[640], 5);
    assert_eq!(&bytes[641..645], &0u32.to_le_bytes());
    assert_eq!(&bytes[645..653], &0u64.to_le_bytes());
}

#[test]
fn records_survive_the_journal_and_padded_shard_read() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut store =
        RecordJournal::open(dir.path(), DatabaseId::Enhance, ENHANCE_LAYOUT).expect("open");
    let records: Vec<EnhanceRecord> = (0..(RECORDS_PER_ROW as u8 + 3)).map(sample).collect();
    store
        .append_block(3_428_143, "hash".to_string(), &records)
        .expect("append");
    drop(store);

    let store =
        RecordJournal::open(dir.path(), DatabaseId::Enhance, ENHANCE_LAYOUT).expect("reopen");
    let shard = store.read_shard_rows(0).expect("shard");
    assert_eq!(shard.len(), SHARD_ROWS * ROW_BYTES);
    for (position, expected) in records.iter().enumerate() {
        let row = position / RECORDS_PER_ROW;
        let slot = position % RECORDS_PER_ROW;
        let start = row * ROW_BYTES + slot * RECORD_BYTES;
        assert_eq!(&shard[start..start + RECORD_BYTES], expected.as_bytes());
    }
    let padding_start = records.len() * RECORD_BYTES;
    assert!(shard[padding_start..].iter().all(|byte| *byte == 0));
}
