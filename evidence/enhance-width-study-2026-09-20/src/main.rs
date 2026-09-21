use ipir_sp::{params_for_simplepir,IPIRClient};
use ipir_sp::serialize::serialize_packing_keys;
use ipir_sp::modulus_switch::response_body_len;
fn main(){
 let records=450163u64;
 for k in [9u64,10,14,18,19,29,36,38,58,72,77] {
  let used=records.div_ceil(k);let rows=used.max(8192).next_power_of_two();
  let (rlwe,p)=params_for_simplepir(rows,k*737*8).unwrap();
  let client=IPIRClient::new(&rlwe,&p);
  let mut seed=[0u8;32];seed[..8].copy_from_slice(&0xa4d6_9bc2_317e_085fu64.to_le_bytes());
  let setup=client.generate_public_query_setup_simplepir_from_seed(seed);
  let (q,keys,_)=client.generate_fresh_query_simplepir(&setup,0);
  let key_bytes=serialize_packing_keys(&rlwe,&keys).unwrap().len();
  let coeff_bytes=q.to_switched_bytes(rlwe.q,p.query_bits).len();
  let response_bytes=16+p.instances*response_body_len(rlwe.d,p.q_prime_1);
  println!("{}",serde_json::json!({"records":records,"records_per_row":k,"plaintext_row_bytes":k*737,"used_rows":used,"logical_rows":rows,"instances":p.instances,"db_cols":p.db_cols,"query_bits":p.query_bits,"packing_key_bytes":key_bytes,"coefficient_bytes":coeff_bytes,"constructed_upload_bytes":8+key_bytes+coeff_bytes,"calculated_response_bytes":response_bytes,"combined_bytes":8+key_bytes+coeff_bytes+response_bytes,"populated_shards":used.div_ceil(8192),"encoded_physical_database_bytes":used.div_ceil(8192)*8192*p.db_cols as u64*2}));
 }
}
