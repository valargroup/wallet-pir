fn main() {
    println!("cargo:rerun-if-changed=compact_formats.proto");
    prost_build::compile_protos(&["compact_formats.proto"], &["."])
        .expect("compile pinned compact schema");
}
