use serde_json::{json, Value};
use sha2::{Digest, Sha256};

/// Resolve identities from the lock used to compile this executable, refusing
/// ambiguous or local packages rather than attributing a guessed Git revision.
pub fn from_lock(bytes: &[u8]) -> Result<Value, String> {
    let lock: toml::Value = std::str::from_utf8(bytes)
        .map_err(|e| e.to_string())?
        .parse::<toml::Value>()
        .map_err(|e| e.to_string())?;
    let packages = lock
        .get("package")
        .and_then(toml::Value::as_array)
        .ok_or("missing lock packages")?;
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
    let identify = |name: &str| -> Result<Value, String> {
        let entry = package(name)?;
        let version = entry
            .get("version")
            .and_then(toml::Value::as_str)
            .filter(|s| !s.is_empty())
            .ok_or("missing package version")?;
        let source = entry
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
    Ok(json!({"schema":"txid-sizing-export-source-v1",
        "node_parser":identify("zakura-chain")?,
        "server_adapter":identify("zakura-server-chain")?,
        "event_codec":identify("zakura-pir-protocol")?,
        "cargo_lock_sha256":format!("{:x}", Sha256::digest(bytes))}))
}
