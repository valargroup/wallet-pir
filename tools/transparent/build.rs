//! Compile the pinned compact-block schema and pin load-generation provenance
//! to the built executable rather than the invocation directory. Both belong to
//! the `benchmarks` feature; ordinary tool builds run neither.
#[cfg(feature = "benchmarks")]
use sha2::{Digest, Sha256};
#[cfg(feature = "benchmarks")]
use std::path::Path;
#[cfg(feature = "benchmarks")]
use std::process::Command;

#[cfg(feature = "benchmarks")]
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

#[cfg(feature = "benchmarks")]
fn git(args: &[&str]) -> Option<String> {
    Command::new("git")
        .args(args)
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

fn main() {
    #[cfg(feature = "benchmarks")]
    {
        compact_schema();
        provenance();
    }
}

#[cfg(feature = "benchmarks")]
fn compact_schema() {
    println!("cargo:rerun-if-changed=src/compact_blocks/compact_formats.proto");
    prost_build::compile_protos(
        &["src/compact_blocks/compact_formats.proto"],
        &["src/compact_blocks"],
    )
    .expect("compile pinned compact schema");
}

#[cfg(feature = "benchmarks")]
fn provenance() {
    let mut hash = Sha256::new();
    for path in [
        "src",
        "build.rs",
        "Cargo.toml",
        "../../src/transparent",
        "../../src/shared",
        "../../Cargo.toml",
        "../../Cargo.lock",
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
    // The served implementation and dependency pins are part of the tool too.
    println!("cargo:rerun-if-changed=../../src/transparent");
    println!("cargo:rerun-if-changed=../../src/shared");
    println!("cargo:rerun-if-changed=../../Cargo.toml");
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
