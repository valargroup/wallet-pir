//! Raw mainnet fixture from zakura-test at 1faf150fc3648aae22c55a6b30f8f5a9b9ce934e.
//! protoc supplies an independent implementation of the pinned protobuf schema.
use std::{
    io::Write,
    process::{Command, Stdio},
};
use transparent_blocks::{
    dataset::{decode, encode},
    proto::CompactBlock,
};
use transparent_filter_server::compact::compact_block;
use zakura_chain::{block::Block, serialization::ZcashDeserialize};
fn protoc(mode: &str, input: &[u8]) -> Vec<u8> {
    let mut child = Command::new("protoc")
        .args([
            format!("--{mode}=cash.z.wallet.sdk.rpc.CompactBlock"),
            "-I../../crates/transparent-blocks".into(),
            "compact_formats.proto".into(),
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    child.stdin.take().unwrap().write_all(input).unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(output.status.success());
    output.stdout
}
#[test]
fn raw_shielded_and_transparent_fields_round_trip_through_protoc() {
    let raw = hex::decode(include_str!("fixtures/compact-mainnet-1687106.hex").trim()).unwrap();
    let block = Block::zcash_deserialize(raw.as_slice()).unwrap();
    let compact = compact_block(&block, 1687106, &mut [0; 3]).unwrap();
    let combined = encode(std::slice::from_ref(&compact), "combined").unwrap();
    let prefix = combined.iter().position(|b| b & 128 == 0).unwrap() + 1;
    let text = protoc("decode", &combined[prefix..]);
    assert!(String::from_utf8_lossy(&text).contains("height: 1687106"));
    let bytes = protoc("encode", &text);
    let mut frame = Vec::new();
    let mut len = bytes.len();
    while len >= 128 {
        frame.push((len as u8 & 127) | 128);
        len >>= 7;
    }
    frame.push(len as u8);
    frame.extend(bytes);
    let independent: CompactBlock = decode(&frame, "identity").unwrap().remove(0);
    assert_eq!(independent, compact);
    assert_eq!(
        compact.vtx.iter().map(|t| t.vin.len()).sum::<usize>(),
        block
            .transactions
            .iter()
            .map(|t| t
                .inputs()
                .iter()
                .filter(|i| !matches!(i, zakura_chain::transparent::Input::Coinbase { .. }))
                .count())
            .sum::<usize>()
    );
    assert_eq!(
        compact.vtx.iter().map(|t| t.vout.len()).sum::<usize>(),
        block
            .transactions
            .iter()
            .map(|t| t.outputs().len())
            .sum::<usize>()
    );
    assert!(compact
        .vtx
        .iter()
        .any(|t| !t.outputs.is_empty() || !t.actions.is_empty()));
    for tx in &compact.vtx {
        for output in &tx.outputs {
            assert_eq!(output.ciphertext.len(), 52);
        }
        for action in &tx.actions {
            assert_eq!(action.ciphertext.len(), 52);
        }
    }
    let without = decode(&encode(&[compact], "shielded").unwrap(), "identity").unwrap();
    assert!(without[0]
        .vtx
        .iter()
        .all(|t| t.vin.is_empty() && t.vout.is_empty()));
}
