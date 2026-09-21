#![allow(dead_code)]
use std::{time::Instant,fs};
use serde_json::json;
use base64::{Engine,engine::general_purpose::STANDARD};
use inspiring::TopKeyImages;
use ipir_sp::{IPIRClient,server::{build_pack_preprocessed_blocks,pack_intermediate_blocks,published_c1_rows},modulus_switch::{recover_published_c1,serialize_rlwe_response_bodies},serialize::{serialize_packing_keys,deserialize_packing_keys}};
#[path="../../../services/enhance-pir-server/src/ipir.rs"] mod ipir;
#[path="../../../services/enhance-pir-server/src/wire.rs"] mod wire;
// Minimal type adapter for the unchanged production ipir.rs implementation.
mod types {
 #[derive(Clone,Copy)] pub struct DatabaseLayout {pub record_bytes:usize,pub records_per_row:usize,pub shard_rows:usize}
 impl DatabaseLayout {pub fn row_bytes(self)->usize{self.record_bytes*self.records_per_row}pub fn item_size_bits(self)->u64{self.row_bytes() as u64*8}pub fn shard_bytes(self)->usize{self.row_bytes()*self.shard_rows}}
 #[derive(Clone,Copy)] pub enum DatabaseId{Enhance} impl DatabaseId{pub fn as_str(&self)->&'static str{"enhance"}}
}
fn memory()->serde_json::Value{
 let s=fs::read_to_string("/proc/self/status").unwrap_or_default();let mut v=serde_json::Map::new();
 for l in s.lines(){if l.starts_with("VmRSS:")||l.starts_with("VmHWM:"){let a:Vec<_>=l.split_whitespace().collect();v.insert(a[0].trim_end_matches(':').to_string(),json!(a[1].parse::<u64>().unwrap()*1024));}}
 json!(v)
}
fn fill(start:usize,positions:usize,bytes:&mut[u8]){
 for (i,r) in bytes.chunks_mut(737).enumerate(){let position=start+i;if position>=positions{break;}for(j,b)in r.iter_mut().enumerate(){*b=((position.wrapping_mul(31)^j.wrapping_mul(17)^(position>>8))%256) as u8;}r[..8].copy_from_slice(&(position as u64).to_le_bytes());}
}
fn main()->Result<(),Box<dyn std::error::Error>>{
 let a:Vec<String>=std::env::args().collect();let k:usize=a[1].parse()?;let positions:usize=a[2].parse()?;let queries:usize=a.get(3).map(|s|s.parse().unwrap()).unwrap_or(30);let retained:usize=a.get(4).map(|s|s.parse().unwrap()).unwrap_or(0);
 let shard_rows:usize=a.get(5).map(|s|s.parse().unwrap()).unwrap_or(8192);
 let layout=types::DatabaseLayout{record_bytes:737,records_per_row:k,shard_rows};let used=positions.div_ceil(k);let rows=used.max(shard_rows).next_power_of_two();let count=used.div_ceil(shard_rows);
 let (rlwe,params)=ipir::global_parameters(rows as u64,&layout)?;let client=IPIRClient::new(&rlwe,&params);
 let mut seed=[0u8;32];seed[..8].copy_from_slice(&0xa4d6_9bc2_317e_085fu64.to_le_bytes());let setup=client.generate_public_query_setup_simplepir_from_seed(seed);
 println!("{}",json!({"event":"start","records_per_row":k,"records":positions,"shard_rows":shard_rows,"logical_rows":rows,"used_rows":used,"instances":params.instances,"shards":count,"query_bits":params.query_bits,"threads":rayon::current_num_threads(),"memory":memory()}));
 let start=Instant::now();let mut runtimes=Vec::new();let mut aggregate:Option<Vec<ipir_sp::server::CrsBlock>>=None;
 for shard in 0..count {
  let mut data=vec![0;layout.shard_bytes()];fill(shard*shard_rows*k,positions,&mut data);let t=Instant::now();
  let runtime=ipir::ShardRuntime::build(&layout,shard as u64,shard*shard_rows,String::new(),&data,&rlwe,&setup)?;
  println!("{}",json!({"event":"shard_build","shard":shard,"seconds":t.elapsed().as_secs_f64(),"memory":memory()}));
  if let Some(ref mut total)=aggregate{ipir::add_crs_blocks_assign_mod(total,&runtime.crs_blocks,&rlwe)?;}else{aggregate=Some(runtime.crs_blocks.clone());}
  runtimes.push(runtime);
 }
 let built=start.elapsed().as_secs_f64();println!("{}",json!({"event":"workers_ready","seconds":built,"memory":memory()}));
 if retained>0 {
  drop(aggregate);let tail=count-1;let mut data=vec![0;layout.shard_bytes()];fill(tail*shard_rows*k,positions,&mut data);
  let baseline=memory();println!("{}",json!({"event":"retention_baseline","memory":baseline}));
  for n in 0..retained {data[100]=(n+1) as u8;let t=Instant::now();runtimes.push(ipir::ShardRuntime::build(&layout,tail as u64,tail*shard_rows,String::new(),&data,&rlwe,&setup)?);println!("{}",json!({"event":"retained_revision","extra":n+1,"seconds":t.elapsed().as_secs_f64(),"memory":memory()}));}
  return Ok(())
 }
 let t=Instant::now();let pre=build_pack_preprocessed_blocks(&rlwe,&aggregate.take().unwrap())?;let top=TopKeyImages::build(&rlwe);let pubbytes=published_c1_rows(&pre,rlwe.q);let c1=recover_published_c1(&pubbytes,rlwe.d,params.instances,rlwe.q);
 let packing_build=t.elapsed().as_secs_f64();
 let template=std::env::var("BENCH_INIT_TEMPLATE")?;let mut init:serde_json::Value=serde_json::from_slice(&fs::read(template)?)?;
 init["params"]=serde_json::to_value(&params)?;init["public_params_base64"]=json!(STANDARD.encode(&pubbytes));
 let g=init["generation"].as_object_mut().unwrap();g.insert("records_per_row".into(),json!(k));g.insert("row_bytes".into(),json!(k*737));g.insert("shard_rows".into(),json!(shard_rows));g.insert("logical_rows".into(),json!(rows));g.insert("used_rows".into(),json!(used));g.insert("ironwood_tree_size".into(),json!(positions));g.insert("parameter_id".into(),json!(format!("ironwood-enhance-pir-v2-enhance-d2048-p16384-rows{}-cols{}",rows,params.db_cols)));
 g.insert("shards".into(),json!((0..count).map(|shard|json!({"shard_id":shard,"global_row_start":shard*shard_rows,"populated_positions":(positions-shard*shard_rows*k).min(shard_rows*k),"sealed":shard+1<count,"rows_sha256":"0".repeat(64),"worker":"shard-group-01"})).collect::<Vec<_>>()));
 let init_bytes=serde_json::to_vec(&init)?.len();
 println!("{}",json!({"event":"packing_ready","seconds":packing_build,"public_params_bytes":pubbytes.len(),"base64_public_params_bytes":STANDARD.encode(&pubbytes).len(),"serialized_init_bytes":init_bytes,"memory":memory()}));
 for i in 0..queries+3 {
  let target=match i%8 {0=>0,1=>used-1,2=>(shard_rows-1).min(used-1),3=>shard_rows.min(used-1),4=>2047.min(used-1),5=>2048.min(used-1),_=>i.wrapping_mul(7919)%used};
  let t=Instant::now();let(q,keys,secret)=client.generate_fresh_query_simplepir(&setup,target);let keywire=serialize_packing_keys(&rlwe,&keys)?;let qwire=q.to_switched_bytes(rlwe.q,params.query_bits);let prep=t.elapsed().as_secs_f64();
  let server=Instant::now();let t=Instant::now();let parsed_keys=deserialize_packing_keys(&rlwe,&keywire)?;let coeffs=ipir::deserialize_first_dim_query(&rlwe,&params,&qwire)?;let parsing=t.elapsed().as_secs_f64();
  let t=Instant::now();let mut intermediate=vec![0u64;params.db_cols];for r in &runtimes{let partial=r.evaluate(&rlwe,&coeffs[r.query_row_start..r.query_row_start+shard_rows])?;ipir::add_intermediate_assign_mod(&mut intermediate,&partial,rlwe.q)?;}let scan=t.elapsed().as_secs_f64();
  let t=Instant::now();let packed=pack_intermediate_blocks(&intermediate,&parsed_keys,&top,&pre)?;let response=serialize_rlwe_response_bodies(&packed,params.q_prime_1);let packing=t.elapsed().as_secs_f64();let serving=server.elapsed().as_secs_f64();
  let t=Instant::now();let(decoded,error)=client.decode_response_simplepir_with_margin(secret,&c1,&response);let decode=t.elapsed().as_secs_f64();
  let mut expected=vec![0;layout.row_bytes()];fill(target*k,positions,&mut expected);let expected:Vec<u64>=ipir::RowPlaintextIter::new(&expected,layout.row_bytes(),1,params.db_cols,14).map(u64::from).collect();let exact=decoded==expected;
  println!("{}",json!({"event":"query","i":i,"warmup":i<3,"row":target,"exact":exact,"upload_bytes":8+keywire.len()+qwire.len(),"response_bytes":16+response.len(),"prepare_ms":prep*1000.0,"parse_ms":parsing*1000.0,"scan_ms":scan*1000.0,"pack_ms":packing*1000.0,"server_ms":serving*1000.0,"decode_ms":decode*1000.0,"noise_error":error,"noise_threshold":rlwe.delta/2}));
  if !exact||error>=rlwe.delta/8{return Err("incorrect answer or insufficient measured noise margin".into())}
 }
 println!("{}",json!({"event":"complete","memory":memory()}));Ok(())
}
