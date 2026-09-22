//! Schema-9 worker-only primitive qualification. This does not implement architecture_2 routing.
#![allow(dead_code)]
use std::{collections::BTreeMap, sync::{Arc, atomic::{AtomicBool, Ordering}}, time::Instant};
use serde_json::json;
use sha2::{Digest, Sha256};
use tokio_stream::StreamExt;
use tower::ServiceExt;
#[path="../../../services/enhance-pir-server/src/ipir.rs"] mod ipir;
#[path="../../../services/enhance-pir-server/src/wire.rs"] mod wire;
#[path="../../../services/enhance-pir-server/src/artifact.rs"] mod artifact;
#[path="../../../services/enhance-pir-server/src/worker.rs"] mod worker;
mod types {
    #[derive(Clone, Copy)] pub struct DatabaseLayout {pub record_bytes:usize,pub records_per_row:usize,pub shard_rows:usize,pub pir_profile:ipir_sp::SimplePirProfile}
    impl DatabaseLayout {pub fn row_bytes(self)->usize{self.record_bytes*self.records_per_row} pub fn item_size_bits(self)->u64{self.row_bytes() as u64*8} pub fn shard_bytes(self)->usize{self.row_bytes()*self.shard_rows}}
    #[derive(Clone,Copy,Debug,PartialEq,Eq,PartialOrd,Ord,Hash,serde::Serialize,serde::Deserialize)]
    #[serde(rename_all="lowercase")] pub enum DatabaseId{Enhance}
    impl DatabaseId {
        pub const ALL:[Self;1]=[Self::Enhance];
        pub fn as_str(&self)->&'static str{"enhance"}
        pub fn layout(self)->DatabaseLayout{DatabaseLayout{record_bytes:737,records_per_row:33,shard_rows:8192,pir_profile:ipir_sp::SimplePirProfile::P16Q46}}
        pub fn setup_seed_bytes(self)->[u8;32]{let mut seed=[0;32];seed[..8].copy_from_slice(&0xa4d6_9bc2_317e_085fu64.to_le_bytes());seed}
    }
    impl std::fmt::Display for DatabaseId {fn fmt(&self,f:&mut std::fmt::Formatter<'_>)->std::fmt::Result{f.write_str(self.as_str())}}
    impl std::str::FromStr for DatabaseId {type Err=String;fn from_str(s:&str)->Result<Self,Self::Err>{if s=="enhance"{Ok(Self::Enhance)}else{Err("unknown table".into())}}}
}
mod store {
    pub struct RecordJournal;
    impl RecordJournal {pub fn rows_digest(rows:&[u8])->String {use sha2::{Digest,Sha256};hex::encode(Sha256::digest(rows))}}
}
const RECORDS_PER_ROW:usize=33;
const ROW_BYTES:usize=RECORDS_PER_ROW*737;
const UNIT_ROWS:usize=8192;
fn fill_row(unit:usize, revision:u64, row:usize, bytes:&mut[u8]) {
    for (i,record) in bytes.chunks_mut(737).enumerate() {
        let position=unit*UNIT_ROWS*RECORDS_PER_ROW+row*RECORDS_PER_ROW+i;
        for(j,b) in record.iter_mut().enumerate(){*b=((position.wrapping_mul(31)^j.wrapping_mul(17)^(position>>8))%256) as u8;}
        record[..8].copy_from_slice(&(position as u64).to_le_bytes());
        record[8..16].copy_from_slice(&revision.to_le_bytes());
    }
}
fn expected(unit:usize, revision:u64)->Vec<u64> {
    let mut row=vec![0;ROW_BYTES];fill_row(unit,revision,0,&mut row);
    ipir::RowPlaintextIter::new(&row,ROW_BYTES,1,12288,16).map(u64::from).collect()
}
fn memory()->serde_json::Value {
    let (total,available,rss)=worker::host_memory();
    let group=std::fs::read_to_string("/proc/self/cgroup").ok().and_then(|text|text.lines().find_map(|line|line.strip_prefix("0::").map(str::to_owned)));
    let read=|name:&str|group.as_ref().and_then(|path|std::fs::read_to_string(format!("/sys/fs/cgroup{path}/{name}")).ok()).map(|s|s.trim().to_string());
    json!({"rss_bytes":rss,"host_available_bytes":available,"host_total_bytes":total,
        "cgroup_current":read("memory.current"),"cgroup_peak":read("memory.peak")})
}
async fn prepare(state:&worker::WorkerState, unit:usize, revision:u64)->Result<worker::ActivateShard,String> {
    let start=Instant::now();
    let mut rows=vec![0;ROW_BYTES*UNIT_ROWS];
    for (row,bytes) in rows.chunks_mut(ROW_BYTES).enumerate(){fill_row(unit,revision,row,bytes);}
    let digest=hex::encode(Sha256::digest(&rows));
    // Exercise the actual HTTP handler's Bytes-to-Vec copy, body cap and digest check.
    // Local offsets give each consecutive four units the setup of a 32K domain.
    let request=axum::http::Request::builder().method("PUT")
        .uri(format!("/internal/enhance/shards/{unit}?query_row_start={}&logical_rows=32768&rows_sha256={digest}",(unit%4)*UNIT_ROWS))
        .body(axum::body::Body::from(rows)).map_err(|e|e.to_string())?;
    let response=worker::router(state.clone()).oneshot(request).await.map_err(|e|e.to_string())?;
    let status=response.status();
    let body=axum::body::to_bytes(response.into_body(),1024*1024).await.map_err(|e|e.to_string())?;
    if !status.is_success(){return Err(format!("prepare {unit}/{revision} failed {status}: {}",String::from_utf8_lossy(&body)));}
    println!("{}",json!({"event":"prepared","unit":unit,"revision":revision,"seconds":start.elapsed().as_secs_f64(),"cached_units":state.cached_shard_count().await,"memory":memory()}));
    Ok(worker::ActivateShard{shard_id:unit as u64,rows_sha256:digest})
}
async fn activate(state:&worker::WorkerState,generation:u64,shards:Vec<worker::ActivateShard>,pins:Vec<u64>)->Result<(),String> {
    state.activate_local(worker::ActivateRequest{generation,retained_generations:pins,tables:BTreeMap::from([(types::DatabaseId::Enhance,shards)])}).await
}
async fn evaluate(state:&worker::WorkerState,generation:u64,units:usize,target:usize,revision:u64)->Result<(),String> {
    let request=wire::EvaluateRequest{generation,shards:(0..units).map(|unit|{
        let mut coefficients=vec![0;UNIT_ROWS];if unit==target{coefficients[0]=1;}
        wire::ShardQuery{shard_id:unit as u64,coefficients}
    }).collect()};
    let result=state.evaluate_local(types::DatabaseId::Enhance,request).await?;
    if result!=expected(target,revision){return Err(format!("wrong intermediate generation={generation} target={target} revision={revision}"));}
    Ok(())
}
async fn streaming(state:worker::WorkerState,unit:usize)->Result<u64,String> {
    let artifact=state.crs_local(types::DatabaseId::Enhance,unit as u64).await?;
    let mut stream=artifact.stream();let mut bytes=0;
    while let Some(chunk)=stream.next().await{bytes+=chunk.map_err(|e|e.to_string())?.len() as u64;}
    Ok(bytes)
}
#[tokio::main(flavor="multi_thread",worker_threads=4)]
async fn main()->Result<(),Box<dyn std::error::Error>> {
    let args:Vec<String>=std::env::args().collect();
    let domains:usize=args.get(1).ok_or("usage: binary domains active|sealed [publications=12] [scratch-parent] [transition-extra-units=0]")?.parse()?;
    let active=match args.get(2).map(String::as_str){Some("active")=>true,Some("sealed")=>false,_=>return Err("mode must be active or sealed".into())};
    let publications:u64=args.get(3).map(|s|s.parse().unwrap()).unwrap_or(12);
    let transition_extra_units:usize=args.get(5).map(|s|s.parse().unwrap()).unwrap_or(0);
    assert!((1..=8).contains(&domains) && publications>=9);
    assert!(transition_extra_units<=8 && (active || transition_extra_units==0));
    let scratch=if let Some(parent)=args.get(4){tempfile::Builder::new().prefix("enhance-worker-capacity-").tempdir_in(parent)?}else{tempfile::Builder::new().prefix("enhance-worker-capacity-").tempdir()?};
    let state=worker::WorkerState::new(scratch.path().to_path_buf())?;
    let (_,profile)=ipir::global_parameters(32768,&types::DatabaseId::Enhance.layout())?;
    assert_eq!(profile.p,65536);
    assert_eq!(profile.db_cols,12288);
    assert_eq!(profile.query_bits,46);
    assert_eq!(profile.instances,6);
    let units=domains*4;let stable=units-usize::from(active);
    println!("{}",json!({"event":"start","schema_version":9,"source_revision":"05337af410dd0f49214dc36fc4f053d3b126fb6c","pir_profile":"simplepir-p16-q46-v1","records_per_row":RECORDS_PER_ROW,"record_bytes":737,"row_bytes":ROW_BYTES,"plaintext_modulus":profile.p,"query_bits":profile.query_bits,"db_cols":profile.db_cols,"instances":profile.instances,"domains":domains,"active":active,"publications":publications,"transition_extra_units":transition_extra_units,"stable_units":stable,"unit_rows":UNIT_ROWS,"threads":rayon::current_num_threads(),"scratch":scratch.path(),"memory":memory()}));
    let started=Instant::now();
    let mut base=Vec::new();
    for unit in 0..stable{base.push(prepare(&state,unit,0).await?);}
    if !active {
        activate(&state,1,base,vec![]).await?;
        let (a,b,c)=tokio::join!(
            tokio::spawn({let state=state.clone();async move{for _ in 0..25{evaluate(&state,1,units,0,0).await?;}Ok::<(),String>(())}}),
            tokio::spawn({let state=state.clone();async move{for _ in 0..25{evaluate(&state,1,units,units-1,0).await?;}Ok::<(),String>(())}}),
            streaming(state.clone(),units-1));
        a??;b??;let bytes=c?;
        println!("{}",json!({"event":"sealed_serving","evaluations":50,"streamed_bytes":bytes,"cached_units":state.cached_shard_count().await,"memory":memory()}));
    } else {
        let frontier=units-1;
        for generation in 1..=publications {
            let pins:Vec<u64>=(generation.saturating_sub(8).max(1)..generation).collect();
            let oldest=pins.first().copied();
            let stop=Arc::new(AtomicBool::new(false));
            let mut tasks=Vec::new();
            if let Some(old)=oldest {
                // Two admitted evaluations continuously exercise retained generations while preparing.
                for requested in [old,generation-1] {
                    let state=state.clone();let stop=stop.clone();
                    tasks.push(tokio::spawn(async move {
                        let mut count=0u64;
                        while !stop.load(Ordering::Relaxed) {
                            evaluate(&state,requested,units,frontier,requested).await?;
                            count+=1;tokio::task::yield_now().await;
                        }
                        Ok::<u64,String>(count)
                    }));
                }
            }
            let previous_stream=if generation>1 {Some(tokio::spawn(streaming(state.clone(),frontier)))}else{None};
            let candidate=prepare(&state,frontier,generation).await?;
            let mut assignment=base.clone();assignment.push(candidate);
            activate(&state,generation,assignment.clone(),pins.clone()).await?;
            stop.store(true,Ordering::Relaxed);
            let mut evaluated=0;for task in tasks{evaluated+=task.await??;}
            let mut streamed=if let Some(task)=previous_stream{task.await??}else{0};
            // Candidate plus eight pinned generations coexist at this point.
            evaluate(&state,generation,units,frontier,generation).await?;
            if let Some(old)=oldest{evaluate(&state,old,units,frontier,old).await?;}
            streamed+=streaming(state.clone(),frontier).await?;
            println!("{}",json!({"event":"candidate_overlap","generation":generation,"pins":pins,"cached_units":state.cached_shard_count().await,"query_database_bytes":state.cached_shard_count().await*UNIT_ROWS*12288*2,"concurrent_evaluations":evaluated,"streamed_bytes":streamed,"memory":memory()}));
            if generation==publications && transition_extra_units>0 {
                // Evidence-only surcharge: hold extra obsolete/candidate runtimes beside all
                // eight published generations plus the candidate. This is not loan routing.
                let stop=Arc::new(AtomicBool::new(false));
                let mut tasks=Vec::new();
                for requested in [oldest.expect("full retention"),generation] {
                    let state=state.clone();let stop=stop.clone();
                    tasks.push(tokio::spawn(async move {
                        let mut count=0u64;
                        while !stop.load(Ordering::Relaxed) {
                            evaluate(&state,requested,units,frontier,requested).await?;
                            count+=1;tokio::task::yield_now().await;
                        }
                        Ok::<u64,String>(count)
                    }));
                }
                let stream=tokio::spawn(streaming(state.clone(),frontier));
                for offset in 0..transition_extra_units {prepare(&state,units+offset,generation).await?;}
                stop.store(true,Ordering::Relaxed);
                let mut evaluated=0;for task in tasks{evaluated+=task.await??;}
                let streamed=stream.await??;
                let cached=state.cached_shard_count().await;
                assert_eq!(cached,stable+9+transition_extra_units);
                evaluate(&state,generation,units,frontier,generation).await?;
                println!("{}",json!({"event":"transition_surcharge_overlap","generation":generation,"extra_units":transition_extra_units,"extra_database_bytes":transition_extra_units*UNIT_ROWS*12288*2,"cached_units":cached,"query_database_bytes":cached*UNIT_ROWS*12288*2,"concurrent_evaluations":evaluated,"streamed_bytes":streamed,"memory":memory()}));
            }
            let committed:Vec<u64>=(generation.saturating_sub(7).max(1)..=generation).collect();
            activate(&state,generation,assignment,committed).await?;
            println!("{}",json!({"event":"committed","generation":generation,"cached_units":state.cached_shard_count().await,"memory":memory()}));
        }
    }
    println!("{}",json!({"event":"complete","seconds":started.elapsed().as_secs_f64(),"cached_units":state.cached_shard_count().await,"memory":memory()}));
    // Drop open artifact pins before removing this benchmark's unique scratch directory.
    drop(state);scratch.close()?;
    Ok(())
}
