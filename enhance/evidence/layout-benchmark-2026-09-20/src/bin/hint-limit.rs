#[allow(dead_code)]
#[path="../../../../services/enhance-pir-server/src/wire.rs"] mod wire;
use ipir_sp::server::CrsBlock;
fn main() {
 let blocks=vec![CrsBlock{rows:vec![vec![0u64;2048];2048]};12];
 let bytes=wire::encode_crs_blocks(&blocks);
 let small=58*737*2048*4usize; let larger=58*737*4096*4usize;
 assert!(bytes.len()>small);assert!(bytes.len()<=larger);
 println!("{}",serde_json::json!({"actual_encoded_hint_bytes":bytes.len(),"records_per_row":58,"instances":12,"shard2048_hint_limit":small,"shard4096_hint_limit":larger,"shard2048_exceeds_limit":true,"shard4096_within_limit":true}));
}
