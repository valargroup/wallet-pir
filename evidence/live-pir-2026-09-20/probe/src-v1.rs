use std::{time::{Instant,Duration},collections::BTreeMap,fs};
use serde_json::{Value,json};
use sha2::{Sha256,Digest};
use enhance_pir::{EnhanceSession,client::QuerySession};
use transparent_wallet::{client::{Table,TableClient},http::{HttpShardTransport,HttpOptions,parse_init},transport::ShardTransport};
type E=Box<dyn std::error::Error+Send+Sync>;
fn metrics(h:&reqwest::blocking::Client,urls:&[String])->Result<Value,E>{
 let results=std::thread::scope(|s| {
  let jobs:Vec<_>=urls.iter().map(|u|s.spawn(move||->Result<String,reqwest::Error>{h.get(u).send()?.error_for_status()?.text()})).collect();
  jobs.into_iter().map(|j|j.join().unwrap()).collect::<Vec<_>>()
 });
 let mut parsed=BTreeMap::<String,f64>::new();
 for (i,r) in results.into_iter().enumerate(){let text=r?;for l in text.lines().filter(|l|!l.starts_with('#')){if let Some((key,val))=l.rsplit_once(' '){if let Ok(n)=val.parse(){parsed.insert(format!("{i}:{key}"),n);}}}}
 Ok(serde_json::to_value(parsed)?)
}
fn main()->Result<(),E>{
 let cfg:Value=serde_json::from_slice(&fs::read(std::env::args().nth(1).ok_or("config")?)?)?;
 let urls:Vec<String>=serde_json::from_value(cfg["metrics"].clone())?;
 let h=reqwest::blocking::Client::builder().timeout(Duration::from_secs(15)).build()?;
 let origin=cfg["url"].as_str().ok_or("url")?;
 let n=cfg["count"].as_u64().unwrap_or(30) as usize;
 if cfg["mode"]=="enhance" {
  let raw=h.get(format!("{origin}/v1/enhance/init")).send()?.error_for_status()?.bytes()?;
  let v:EnhanceSession=serde_json::from_slice(&raw)?;
  println!("{}",json!({"event":"init","bytes":raw.len(),"session":serde_json::from_slice::<Value>(&raw)?}));
  let s=QuerySession::from_session(v)?;
  for i in 0..n+3 {
   let row=i%30;
   let a=Instant::now();let q=s.prepare_row(row)?;let prep=a.elapsed().as_secs_f64();let up=q.body().len();
   let before=metrics(&h,&urls)?;let a=Instant::now();
   let res=h.post(format!("{origin}/v1/enhance/query")).body(q.body().to_vec()).send()?.error_for_status()?.bytes()?;let http=a.elapsed().as_secs_f64();
   let a=Instant::now();let decoded=s.decode(q,&res)?;let dec=a.elapsed().as_secs_f64();let after=metrics(&h,&urls)?;
   let exact=hex::encode(Sha256::digest(&decoded))==cfg["hashes"][row].as_str().ok_or("hash")?;
   println!("{}",json!({"event":"query","i":i,"warmup":i<3,"row":row,"upload":up,"download":res.len(),"prepare_s":prep,"http_s":http,"decode_s":dec,"total_s":prep+http+dec,"exact":exact,"before":before,"after":after}));
   if !exact{return Err("exact answer mismatch".into())}
  }
 } else {
  let shard=cfg["shard"].as_u64().ok_or("shard")?;let rev=cfg["revision"].as_str().ok_or("revision")?;
  let name=cfg["table"].as_str().ok_or("table")?;let table=if name=="directory"{Table::Directory}else{Table::Pages};
  let raw=fs::read(cfg["init_file"].as_str().ok_or("init_file")?)?;let geom=parse_init(&raw)?;
  let g=geom.geometries.iter().find(|g|g.name==cfg["geometry"].as_str().unwrap()).ok_or("geometry")?;
  let (rows,width,seed,scheme)=if name=="directory"{(g.directory_rows,g.directory_row_bytes,g.directory_setup_seed,&g.directory_scheme)}else{(g.page_rows,g.page_row_bytes,g.pages_setup_seed,&g.pages_scheme)};
  let mut c=TableClient::new(table,rows,width,seed,scheme)?;
  let mut tr=HttpShardTransport::new(origin,&HttpOptions::default())?;
  let segments=cfg["segments"].as_u64().unwrap() as u32;let mut setupbytes=0;
  for seg in 0..segments {let (raw,_)=tr.setup(shard,rev,table,seg)?;setupbytes+=raw.len();let v:Value=serde_json::from_slice(&raw)?;c.open_segment(rev,seg,v["public_params"].as_str().unwrap(),v["public_params_sha256"].as_str().unwrap())?;}
  println!("{}",json!({"event":"setup","bytes":setupbytes,"rows":rows,"width":width,"shard":shard,"revision":rev}));
  for i in 0..n+3 {
   let row=i%30;let a=Instant::now();let q=c.prepare(rev,row)?;let prep=a.elapsed().as_secs_f64();let up=q.body.len();
   let before=metrics(&h,&urls)?;let a=Instant::now();let res=tr.query(shard,rev,table,&q.body)?;let http=a.elapsed().as_secs_f64();
   let a=Instant::now();let decoded=c.decode(rev,segments,q,&res)?;let dec=a.elapsed().as_secs_f64();let after=metrics(&h,&urls)?;
   let exact=decoded.iter().enumerate().all(|(seg,b)|Some(hex::encode(Sha256::digest(b)).as_str())==cfg["hashes"][seg][row].as_str());
   println!("{}",json!({"event":"query","i":i,"warmup":i<3,"row":row,"upload":up,"download":res.len(),"prepare_s":prep,"http_s":http,"decode_s":dec,"total_s":prep+http+dec,"exact":exact,"before":before,"after":after}));
   if !exact{return Err("exact answer mismatch".into())}
  }
 }
 Ok(())
}
