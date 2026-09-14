//! Pin provenance to the built executable rather than the invocation directory.
use sha2::{Digest, Sha256};
use std::path::Path;
use std::process::Command;

fn fingerprint_files(path: &Path, hash: &mut Sha256) {
    if path.is_dir() {
        let mut files: Vec<_> = std::fs::read_dir(path)
            .unwrap()
            .map(|e| e.unwrap().path())
            .collect();
        files.sort();
        for file in files {
            if file.file_name().unwrap() != "target" {
                fingerprint_files(&file, hash);
            }
        }
    } else if matches!(
        path.extension().and_then(|e| e.to_str()),
        Some("rs" | "toml" | "lock")
    ) {
        let bytes = std::fs::read(path).unwrap();
        hash.update(path.to_string_lossy().as_bytes());
        hash.update((bytes.len() as u64).to_le_bytes());
        hash.update(bytes);
    }
}

fn git(args: &[&str]) -> Option<String> {
    Command::new("git")
        .args(args)
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

fn main() {
    let mut hash = Sha256::new();
    for path in [
        "src",
        "build.rs",
        "Cargo.toml",
        "../../crates",
        "../../../enhance/crates",
        "../../../Cargo.toml",
        "../../../Cargo.lock",
    ] {
        fingerprint_files(Path::new(path), &mut hash);
    }
    println!(
        "cargo:rustc-env=SIMULATION_PREPARATION_FINGERPRINT={:x}",
        hash.finalize()
    );
    println!("cargo:rerun-if-changed=src");
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=Cargo.toml");
    // The wallet implementation and dependency pins are part of the tool too.
    println!("cargo:rerun-if-changed=../../crates");
    println!("cargo:rerun-if-changed=../../../enhance/crates");
    println!("cargo:rerun-if-changed=../../../Cargo.lock");
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
