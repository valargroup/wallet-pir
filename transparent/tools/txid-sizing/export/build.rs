#[path = "build_support/identity.rs"]
mod identity;

fn main() {
    println!("cargo:rerun-if-changed=Cargo.lock");
    println!("cargo:rerun-if-changed=build_support/identity.rs");
    let bytes = std::fs::read("Cargo.lock").expect("read export's compilation lock");
    let identity = identity::from_lock(&bytes).expect("resolve compiled parser identity");
    let output =
        std::path::PathBuf::from(std::env::var_os("OUT_DIR").expect("Cargo output directory"));
    std::fs::write(output.join("source_identity.json"), identity.to_string())
        .expect("embed compiled parser identity");
}
