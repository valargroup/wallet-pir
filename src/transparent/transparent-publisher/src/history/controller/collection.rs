//! Retain live publication owners before collecting immutable objects.
use super::*;

pub(super) fn collect_candidates(config: &Config, authority: &Authority) -> Result<(), BoxError> {
    let active = authority
        .0
        .active
        .read()
        .unwrap()
        .publication
        .directory
        .clone();
    let pending = match std::fs::read(config.publication_root.join("activation.json")) {
        Ok(bytes) => Some(serde_json::from_slice::<ActivePublication>(&bytes)?.directory),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(error.into()),
    };
    let validating = authority.0.validation.held();
    let ownership = crate::storage::collecting();
    let mut unused: Vec<_> = std::fs::read_dir(&config.publication_root)?
        .filter_map(Result::ok)
        .filter(|e| {
            e.file_type().is_ok_and(|t| t.is_dir())
                && e.file_name()
                    .to_str()
                    .is_some_and(|s| s.starts_with("candidate-"))
                && !ownership.holds(&e.path())
                && e.path() != active
                && pending.as_ref() != Some(&e.path())
                && !validating.contains(&e.path())
        })
        .collect();
    unused.sort_by_key(|e| std::cmp::Reverse(e.metadata().and_then(|m| m.modified()).ok()));
    for entry in unused.into_iter().skip(3) {
        std::fs::remove_dir_all(entry.path())?;
    }
    // Stored segments and runtimes no remaining publication links.
    let collected = crate::history::store::collect(&config.publication_root)?;
    if collected != Default::default() {
        tracing::info!(?collected, "collected unreferenced stored objects");
    }
    Ok(())
}
