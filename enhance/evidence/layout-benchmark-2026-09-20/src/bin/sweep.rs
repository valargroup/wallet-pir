use ipir_sp::{params_for_simplepir,serialize::serialized_packing_keys_len,modulus_switch::{published_c1_len,response_body_len}};
use serde_json::json;
fn main(){
 for n in [450163u64,466986,475137,589824,1000000,4700000] {
  for queries in [1u64,2,5,10,11,20,100,1000000] {
   for shard_rows in [8192u64,4096,2048] {
    let mut best=None;
    for k in 5u64..=512 {
     let used=n.div_ceil(k);let rows=used.max(shard_rows).next_power_of_two();let(r,p)=params_for_simplepir(rows,k*737*8).unwrap();
     let upload=8+serialized_packing_keys_len(&r)+(p.db_rows*p.query_bits).div_ceil(8);let download=16+p.instances*response_body_len(r.d,p.q_prime_1);
     let public=p.instances*published_c1_len(r.d,r.q);let base64=4*public.div_ceil(3);let total=(upload+download) as u64*queries+base64 as u64;
     if best.as_ref().is_none_or(|(cost,old_k,_,_,_,_,_,_)|total<*cost || (total==*cost && k>*old_k)){best=Some((total,k,rows,p.instances,upload,download,base64,used.div_ceil(shard_rows)));}
    }
    let(total,k,rows,instances,upload,download,setup,shards)=best.unwrap();
    println!("{}",json!({"records":n,"queries_per_setup":queries,"shard_rows":shard_rows,"records_per_row":k,"instances":instances,"logical_rows":rows,"shards":shards,"upload":upload,"download":download,"base64_public_setup":setup,"total_bytes":total,"amortized_bytes":total as f64/queries as f64,"scope":"Parameter arithmetic; excludes small init JSON metadata and HTTP/TLS; no latency estimate."}));
   }
  }
 }
}
