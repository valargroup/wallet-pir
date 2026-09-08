//! Pin provenance to the built executable rather than the invocation directory.
use std::process::Command;

fn git(args: &[&str]) -> Option<String> {
    Command::new("git")
        .args(args)
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

fn main() {
    println!("cargo:rerun-if-changed=src");
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=Cargo.toml");
    // The wallet implementation and dependency pins are part of the tool too.
    println!("cargo:rerun-if-changed=../../pir");
    println!("cargo:rerun-if-changed=../../Cargo.lock");
    for path in ["HEAD".to_owned(), "index".to_owned()]
        .into_iter()
        .chain(git(&["symbolic-ref", "-q", "HEAD"]))
    {
        if let Some(path) = git(&["rev-parse", "--git-path", &path]) {
            println!("cargo:rerun-if-changed={path}");
        }
    }
    let sha = git(&["rev-parse", "HEAD"]).unwrap_or_else(|| "unknown".into());
    let dirty = git(&["status", "--porcelain"]).map_or("unknown", |s| {
        if s.is_empty() {
            "false"
        } else {
            "true"
        }
    });
    println!("cargo:rustc-env=SIMULATION_SOURCE_SHA={sha}");
    println!("cargo:rustc-env=SIMULATION_SOURCE_DIRTY={dirty}");
    for key in ["PROFILE", "OPT_LEVEL", "TARGET"] {
        println!(
            "cargo:rustc-env=SIMULATION_{key}={}",
            std::env::var(key).unwrap_or_default()
        );
    }
}
