#[path = "build_support/identity.rs"]
mod identity;

fn main() {
    println!("cargo:rerun-if-changed=Cargo.lock");
    println!("cargo:rerun-if-changed=build_support/identity.rs");
    let manifest_dir = std::path::PathBuf::from(
        std::env::var_os("CARGO_MANIFEST_DIR").expect("Cargo manifest directory"),
    );
    // Cargo scans a directory recursively: any edit to, or new file in, the
    // local chain reader re-embeds its source-tree identity.
    println!(
        "cargo:rerun-if-changed={}",
        manifest_dir.join(identity::ADAPTER_DIR).display()
    );
    let identity = identity::compiled(&manifest_dir).expect("resolve compiled parser identity");
    let output =
        std::path::PathBuf::from(std::env::var_os("OUT_DIR").expect("Cargo output directory"));
    std::fs::write(output.join("source_identity.json"), identity.to_string())
        .expect("embed compiled parser identity");
}
