#![allow(dead_code)]
#[path="baseline_ipir.rs"] mod baseline;
#[path="baseline_wire.rs"] mod wire;
use enhance_pir_server::{ipir, types};
use types::{DatabaseId, ENHANCE_LAYOUT};
use serde_json::json;
use std::{path::PathBuf, time::Instant};
use tokio_stream::StreamExt;
fn report(mode: &str, phase: &str, start: Instant, revision: usize) {
    let (_, _, rss) = enhance_pir_server::worker::host_memory();
    println!("{}", json!({"pid":std::process::id(),"mode":mode,"phase":phase,"seconds":start.elapsed().as_secs_f64(),"revision":revision,"rss_bytes":rss}));
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().collect();
    let mode=&args[1]; let scenario=&args[2]; let root=PathBuf::from(&args[3]);
    std::fs::create_dir_all(&root)?;
    let layout=ENHANCE_LAYOUT;
    let (rlwe,params)=ipir::shard_parameters(&layout)?;
    let start=Instant::now();
    report(mode,"start", start,0);
    if scenario=="load" {
        let start=Instant::now();
        if mode=="baseline" {
            let runtime=baseline::ShardRuntime::load_cached(&root,DatabaseId::Enhance,&layout,0,0,"revision-0",&rlwe)?;
            report(mode,"loaded",start,0); std::hint::black_box(&runtime);
        } else {
            let runtime=ipir::ShardRuntime::load_cached(&root,DatabaseId::Enhance,&layout,0,0,"revision-0",&rlwe)?;
            report(mode,"loaded",start,0); std::hint::black_box(&runtime);
        }
        return Ok(());
    }
    if scenario=="hint" {
        if mode=="baseline" {
            let runtime=baseline::ShardRuntime::load_cached(&root,DatabaseId::Enhance,&layout,0,0,"revision-0",&rlwe)?;
            report(mode,"hint_ready",start,0);
            let start=Instant::now();
            let bytes=wire::encode_crs_blocks(&runtime.crs_blocks);
            let mut sink=sha2::Sha256::default(); use sha2::Digest; sink.update(&bytes);
            report(mode,"hint_fetched",start,0); println!("{}",json!({"hint_bytes":bytes.len(),"sha256":hex::encode(sink.finalize())}));
        } else {
            let runtime=ipir::ShardRuntime::load_cached(&root,DatabaseId::Enhance,&layout,0,0,"revision-0",&rlwe)?;
            report(mode,"hint_ready",start,0);
            let start=Instant::now(); let mut stream=runtime.publication.stream();
            let mut sink=sha2::Sha256::default(); use sha2::Digest; let mut n=0;
            while let Some(chunk)=stream.next().await {let chunk=chunk?;n+=chunk.len();sink.update(&chunk);}
            report(mode,"hint_fetched",start,0);println!("{}",json!({"hint_bytes":n,"sha256":hex::encode(sink.finalize())}));
        }
        return Ok(());
    }
    let setup=ipir_sp::IPIRClient::new(&rlwe,&params).generate_public_query_setup_simplepir_from_seed(DatabaseId::Enhance.setup_seed_bytes());
    let mut rows=vec![0;layout.shard_bytes()];
    for (i,b) in rows.iter_mut().enumerate(){*b=(i.wrapping_mul(17)^(i>>8)) as u8;}
    let mut old=Vec::new();let mut new=Vec::new();
    let revisions=if scenario=="retained" {8} else {1};
    for revision in 0..revisions {
        rows[0]=revision as u8;let start=Instant::now();
        let digest=format!("revision-{revision}");
        if mode=="baseline" {
            let (runtime,built)=baseline::ShardRuntime::load_or_build(&root,DatabaseId::Enhance,&layout,0,0,digest,&rows,&rlwe,&setup)?;
            assert!(built); old.push(runtime);
        } else {
            let (runtime,built)=ipir::ShardRuntime::load_or_build(&root,DatabaseId::Enhance,&layout,0,0,digest,&rows,&rlwe,&setup)?;
            assert!(built); new.push(runtime);
        }
        report(mode,"prepared",start,revision);
    }
    let (_,_,rss)=enhance_pir_server::worker::host_memory();
    println!("{}",json!({"pid":std::process::id(),"phase":"retained","mode":mode,"revisions":revisions,"rss_bytes":rss}));
    if std::env::var_os("BENCH_HOLD").is_some() {
        let mut line=String::new(); std::io::stdin().read_line(&mut line)?;
    }
    std::hint::black_box((&old,&new));
    Ok(())
}
