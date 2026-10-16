//! Identity of this compiled reader, separately from any journal's ingest.
use crate::AnyError;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

pub fn compiled() -> Value {
    serde_json::from_str(include_str!(concat!(
        env!("OUT_DIR"),
        "/source_identity.json"
    )))
    .expect("build script embeds valid source identity JSON")
}

/// The identity of the image this process executes, captured once. Capture
/// it before reading any input and report only this value: a later read of
/// the executable's pathname may name a rebuilt or removed file.
pub struct Executing(Value);

impl Executing {
    pub fn capture() -> Result<Self, AnyError> {
        let mut identity = compiled();
        identity["compiled_source_file_sha256"] = json!({
            "extractor":hex::encode(Sha256::digest(include_bytes!("../../../../services/transparent-filter-server/src/extract.rs"))),
            "census":hex::encode(Sha256::digest(include_bytes!("census.rs"))),
        });
        // The pinned event codec's extents, as compiled into this reader.
        identity["event_codec_bounds"] = json!({
            "event_bytes":zakura_transparent_private_shared::events::EVENT_BYTES,
            "max_event_bytes":zakura_transparent_private_shared::events::MAX_EVENT_BYTES,
        });
        identity["binary_sha256"] = json!(running_image_sha256()?);
        Ok(Self(identity))
    }

    pub fn identity(&self) -> &Value {
        &self.0
    }

    pub fn binary_sha256(&self) -> &Value {
        &self.0["binary_sha256"]
    }

    pub fn into_identity(self) -> Value {
        self.0
    }
}

/// `/proc/self/exe` opens the inode this process executes, even after its
/// pathname is replaced or unlinked; `current_exe()` only names a path.
#[cfg(target_os = "linux")]
fn running_image_sha256() -> Result<String, AnyError> {
    let mut image = std::fs::File::open("/proc/self/exe")?;
    let mut digest = Sha256::new();
    std::io::copy(&mut image, &mut digest)?;
    Ok(hex::encode(digest.finalize()))
}

/// No verified way to open the executing image here; hashing the current
/// pathname could attribute output to bytes that never ran.
#[cfg(not(target_os = "linux"))]
fn running_image_sha256() -> Result<String, AnyError> {
    Err("executing-image identity requires Linux /proc/self/exe".into())
}

/// Test synchronization only: with `TXID_SIZING_EXPORT_TEST_BARRIER` set to a
/// directory, announce `<dir>/<name>.reached` and wait for `<dir>/<name>.go`.
/// Unset, this does nothing.
pub fn test_barrier(name: &str) -> Result<(), AnyError> {
    let Some(dir) = std::env::var_os("TXID_SIZING_EXPORT_TEST_BARRIER") else {
        return Ok(());
    };
    let dir = std::path::Path::new(&dir);
    std::fs::write(dir.join(format!("{name}.reached")), b"")?;
    let go = dir.join(format!("{name}.go"));
    let start = std::time::Instant::now();
    while !go.exists() {
        if start.elapsed() > std::time::Duration::from_secs(120) {
            return Err(format!("test barrier {name} timed out").into());
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    Ok(())
}
