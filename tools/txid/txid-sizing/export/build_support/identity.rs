use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};

/// The repository's chain reader, relative to this package's manifest directory.
pub const ADAPTER_DIR: &str = "../../../../src/shared/pir-chain";

/// The identity compiled into this executable: the lock beside `manifest_dir`
/// and the local chain reader's source tree.
pub fn compiled(manifest_dir: &Path) -> Result<Value, String> {
    let lock = std::fs::read(manifest_dir.join("Cargo.lock")).map_err(|e| e.to_string())?;
    from_lock(&lock, &source_tree_sha256(&manifest_dir.join(ADAPTER_DIR))?)
}

/// Every file of a package's source tree, sorted by relative path. Build outputs
/// (`target` directories) are excluded; anything else that is not a regular file
/// or directory is refused rather than skipped.
pub fn source_files(root: &Path) -> Result<Vec<(String, PathBuf)>, String> {
    let mut files = Vec::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(directory) = pending.pop() {
        let entries =
            std::fs::read_dir(&directory).map_err(|e| format!("{}: {e}", directory.display()))?;
        for entry in entries {
            let path = entry.map_err(|e| e.to_string())?.path();
            let kind = std::fs::symlink_metadata(&path)
                .map_err(|e| e.to_string())?
                .file_type();
            if kind.is_dir() {
                if path.file_name().is_some_and(|name| name != "target") {
                    pending.push(path);
                }
            } else if kind.is_file() {
                let relative = path
                    .strip_prefix(root)
                    .map_err(|e| e.to_string())?
                    .components()
                    .map(|c| c.as_os_str().to_str().ok_or("non-UTF-8 source path"))
                    .collect::<Result<Vec<_>, _>>()?
                    .join("/");
                files.push((relative, path));
            } else {
                return Err(format!("{}: not a regular source file", path.display()));
            }
        }
    }
    files.sort();
    if !files.iter().any(|(relative, _)| relative == "Cargo.toml") {
        return Err(format!("{}: missing package manifest", root.display()));
    }
    Ok(files)
}

/// A deterministic digest of a package's source tree: each file's relative path
/// and bytes, length-prefixed, in path order.
pub fn source_tree_sha256(root: &Path) -> Result<String, String> {
    let mut digest = Sha256::new();
    for (relative, path) in source_files(root)? {
        let bytes = std::fs::read(&path).map_err(|e| format!("{}: {e}", path.display()))?;
        digest.update((relative.len() as u64).to_le_bytes());
        digest.update(relative.as_bytes());
        digest.update((bytes.len() as u64).to_le_bytes());
        digest.update(&bytes);
    }
    Ok(format!("{:x}", digest.finalize()))
}

/// Resolve identities from the lock used to compile this executable. The node
/// parser and event codec must have exact Git sources; the server adapter must
/// be the repository's local package, identified by `adapter_tree_sha256`, and
/// is never attributed a Git source. Ambiguous or duplicate packages are refused.
pub fn from_lock(bytes: &[u8], adapter_tree_sha256: &str) -> Result<Value, String> {
    let lock: toml::Value = std::str::from_utf8(bytes)
        .map_err(|e| e.to_string())?
        .parse::<toml::Value>()
        .map_err(|e| e.to_string())?;
    let packages = lock
        .get("package")
        .and_then(toml::Value::as_array)
        .ok_or("missing lock packages")?;
    let mut identities = std::collections::BTreeSet::new();
    for package in packages {
        let field = |key| package.get(key).and_then(toml::Value::as_str);
        if !identities.insert((field("name"), field("version"), field("source"))) {
            return Err(format!(
                "duplicate locked package identity {}",
                field("name").unwrap_or("<unnamed>")
            ));
        }
    }
    let package = |name: &str| -> Result<&toml::Value, String> {
        let matches: Vec<_> = packages
            .iter()
            .filter(|p| p.get("name").and_then(toml::Value::as_str) == Some(name))
            .collect();
        if matches.len() != 1 {
            return Err(format!("expected exactly one locked {name}"));
        }
        Ok(matches[0])
    };
    let version = |name: &str| -> Result<String, String> {
        package(name)?
            .get("version")
            .and_then(toml::Value::as_str)
            .filter(|s| !s.is_empty())
            .map(str::to_owned)
            .ok_or_else(|| format!("{name}: missing package version"))
    };
    let identify = |name: &str| -> Result<Value, String> {
        let version = version(name)?;
        let source = package(name)?
            .get("source")
            .and_then(toml::Value::as_str)
            .ok_or("missing package Git source")?;
        let (url, revision) = source
            .rsplit_once('#')
            .ok_or("missing resolved Git revision")?;
        if !url.starts_with("git+")
            || revision.len() != 40
            || !revision.bytes().all(|b| b.is_ascii_hexdigit())
        {
            return Err(format!("{name} has no exact resolved Git source"));
        }
        Ok(json!({"package":name,"version":version,"git_source":source,"git_revision":revision}))
    };
    let adapter = package("zakura-server-chain")?;
    if adapter.get("source").is_some() {
        return Err("server adapter must be the repository's local package".into());
    }
    if adapter_tree_sha256.len() != 64
        || !adapter_tree_sha256
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err("server adapter source tree digest must be lowercase SHA-256".into());
    }
    if !adapter
        .get("dependencies")
        .and_then(toml::Value::as_array)
        .is_some_and(|deps| {
            deps.iter().any(|d| {
                d.as_str()
                    .is_some_and(|s| s == "zakura-chain" || s.starts_with("zakura-chain "))
            })
        })
    {
        return Err("server adapter does not depend on the locked node parser".into());
    }
    Ok(json!({"schema":"txid-sizing-export-source-v2",
        "node_parser":identify("zakura-chain")?,
        "server_adapter":{"package":"zakura-server-chain","version":version("zakura-server-chain")?,
            "source":"local","source_tree_sha256":adapter_tree_sha256},
        "event_codec":identify("zakura-pir-protocol")?,
        "cargo_lock_sha256":format!("{:x}", Sha256::digest(bytes))}))
}
